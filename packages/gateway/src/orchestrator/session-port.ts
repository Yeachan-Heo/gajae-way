import { createHash } from "node:crypto";
import {
	assertControlAllowed,
	assertValidOpRef,
	TranscriptIncompleteError,
	GjcCliError,
	fetchOpState,
	envelopeErrorCode,
	inspectSession,
	isTerminalStatus,
	OpRefRejectedError,
	parseEnvelope,
	sendPrompt,
	type BrokerSession,
	type CliRunner,
	type ControllerOptions,
	type CliResult,
	type LastAssistantResult,
	type SendReceipt,
	type StatusReport,
} from "@gajaeway/subsession";
import type { GjcModelSelection } from "../config";
import type { GatewayDatabase } from "../store/db";
import { sanitizeDiagnostic } from "./rebind";
import { type TailAttachInput, type TailHandle, TailRunner } from "./tail-runner";

/**
 * Generic broker-backed session surface. Callers own prompt composition,
 * operation-reference selection, and recovery policy; this port only binds,
 * sends, observes, and reads terminal output through the SDK CLI.
 */
export interface SessionPort {
	bind(input: SessionBindInput): Promise<SessionBinding>;
	inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined>;
	/** Restores a saved, non-deleted session through `session.resume`; it never creates a replacement. */
	resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding>;
	send(input: SessionSendInput): Promise<SendReceipt>;
	steer(input: SessionSteerInput): Promise<void>;
	setModel(input: { sessionId: string; repo: string; selection: GjcModelSelection }): Promise<{ readonly changed: boolean }>;
	status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport>;
	fetchLastAssistant(input: { sessionId: string; repo: string }): Promise<LastAssistantResult>;
	attachTail(input: TailAttachInput): Promise<TailHandle>;
	runCompaction(input: SessionCompactionInput): Promise<{ readonly status: SessionCompactionStatus }>;
	/** Presentation/recovery tick; it never kills a running SDK turn. */
	checkStalls(now?: number): void;
	/** Applies the live tail-stall alarm policy; it never aborts a running turn. */
	setStallTimeoutMs(timeoutMs: number): void;
	/** Serializes caller-owned workflows without putting chat policy in this port. */
	runExclusive<T>(key: string, work: () => Promise<T>): Promise<T>;
	/** Request/response helper for callers that already own their op-ref and serialization. */
	request(input: SessionRequestInput): Promise<SessionRequestResult>;
}

export type SessionCompactionStatus = "succeeded" | "failed" | "skipped" | "unavailable";

export interface SessionCompactionInput {
	readonly sessionId: string;
	readonly repo: string;
	readonly originKey: string;
}

export interface SessionBindInput {
	readonly originKey: string;
	readonly epoch: number;
	readonly repo: string;
	/** The SDK host's default coding register is retained when true. */
	readonly codingRegister?: boolean;
}

export interface SessionBinding {
	readonly sessionId: string;
	readonly originKey: string;
	readonly epoch: number;
	readonly repo: string;
}

export interface SessionSendInput {
	readonly sessionId: string;
	readonly repo: string;
	readonly text: string;
	readonly opRef: string;
	/** Trusted bootstrap/policy material attached to this exact turn. */
	readonly systemPreamble?: string;
	readonly model?: GjcModelSelection;
	readonly codingRegister?: boolean;
}

export interface SessionSteerInput {
	readonly sessionId: string;
	readonly repo: string;
	readonly text: string;
	readonly clientRef: string;
}

export interface SessionRequestInput extends SessionSendInput {
	/** Stable caller identity carried into tail observability. */
	readonly originKey?: string;
	readonly waitTimeoutMs?: number;
	readonly pollMs?: number;
}

export interface SessionRequestResult {
	readonly receipt: SendReceipt;
	readonly status: StatusReport;
	readonly assistant: LastAssistantResult;
}

export class SessionRequestTimeoutError extends Error {
	readonly sessionId: string;
	readonly opRef: string;
	readonly lastStatus: StatusReport;

	constructor(sessionId: string, opRef: string, lastStatus: StatusReport) {
		super(`session operation ${opRef} did not reach a terminal status before the bounded request wait elapsed`);
		this.name = "SessionRequestTimeoutError";
		this.sessionId = sessionId;
		this.opRef = opRef;
		this.lastStatus = lastStatus;
	}
}

export class SessionTerminalError extends Error {
	readonly status: StatusReport;

	constructor(status: StatusReport) {
		const detail = status.status.error?.message ?? status.status.error?.code ?? status.status.status;
		super(`session operation ${status.operationRef} ended ${status.status.status}: ${detail}`);
		this.name = "SessionTerminalError";
		this.status = status;
	}
}

export interface BrokerSessionPortOptions {
	readonly database: GatewayDatabase;
	readonly cli: CliRunner;
	readonly instanceId: string;
	readonly tailRunner: TailRunner;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_REQUEST_WAIT_MS = 30 * 60_000;
const DEFAULT_STATUS_POLL_MS = 500;
const SESSION_CREATE_ATTEMPTS = 5;
const SESSION_CREATE_RETRY_MS = 1_000;

/**
 * Production SessionPort implementation. The broker-bound CliRunner is the sole
 * transport: it neither discovers an endpoint nor opens an authenticated socket.
 */
export class BrokerSessionPort implements SessionPort {
	readonly #database: GatewayDatabase;
	readonly #cli: CliRunner;
	readonly #instanceId: string;
	readonly #tailRunner: TailRunner;
	readonly #now: () => number;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly #chains = new Map<string, Promise<void>>();

	constructor(options: BrokerSessionPortOptions) {
		this.#database = options.database;
		this.#cli = async (args, commandOptions) => normalizeSdkEnvelopeFailure(await options.cli(args, commandOptions));
		this.#instanceId = options.instanceId;
		this.#tailRunner = options.tailRunner;
		this.#now = options.now ?? (() => Date.now());
		this.#sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
	}

	async #safe<T>(work: () => Promise<T>): Promise<T> {
		try {
			return await work();
		} catch (error) {
			throw sanitizeSdkFailure(error);
		}
	}

	async bind(input: SessionBindInput): Promise<SessionBinding> {
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0) throw new Error("session epoch must be a non-negative integer");
		const existing = this.#database.getSessionRecord(input.originKey);
		if (existing?.epoch === input.epoch && existing.sessionId) {
			// A persisted binding is only reusable if the broker still indexes it. A
			// binding written against a store the current runtime cannot read
			// (pre-cutover session ids) must be rebound, not handed to a send that
			// will fail with session_unavailable forever. Inspect failure (transport
			// outage) keeps the binding: that is not evidence the session is gone.
			let indexed = true;
			try {
				// Judged on the raw envelope: gjc >= 0.16.0 reports a live session with a
				// locator that lacks `repo`, which the subsession normalizer treats as
				// absent. "Not indexed" is the broker disowning the id, nothing else.
				const result = await this.#cli(["sdk", "session", "inspect", existing.sessionId, "--repo", input.repo]);
				const envelope = JSON.parse(result.stdout) as { ok?: unknown; error?: { code?: unknown } };
				if (envelope.ok === false) indexed = envelope.error?.code !== "session_unavailable";
			} catch {
				indexed = true;
			}
			if (indexed) return { sessionId: existing.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
			const rebound = this.#database.rebindEpoch(input.originKey);
			console.error(`session_rebound origin=${input.originKey} epoch=${input.epoch} nextEpoch=${rebound} session=${existing.sessionId} reason=not_indexed_by_broker`);
			return await this.bind({ ...input, epoch: rebound });
		}
		const idempotencyKey = sessionCreateRef(this.#instanceId, input.originKey, input.epoch, input.repo);
		const created = await this.#createSession(input.repo, idempotencyKey);
		if (typeof created.sessionId !== "string" || created.sessionId.length === 0) {
			throw new Error("session.create succeeded without a sessionId");
		}
		const persistedEpoch = this.#database.getSessionRecord(input.originKey)?.epoch;
		if (persistedEpoch !== undefined && persistedEpoch > input.epoch) {
			throw new Error(`session bind for ${input.originKey} epoch ${input.epoch} lost to epoch ${persistedEpoch}`);
		}
		if (!this.#database.putSessionAtEpoch(input.originKey, created.sessionId, input.epoch)) {
			throw new Error(`session bind for ${input.originKey} epoch ${input.epoch} lost to a concurrent durable epoch change`);
		}
		return { sessionId: created.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
	}

	#createChain: Promise<unknown> = Promise.resolve();

	/** Cold creates are serialized per agent dir: parallel launches starve gjc's lifecycle launcher. */
	#createSession(repo: string, idempotencyKey: string): Promise<{ readonly sessionId?: unknown }> {
		const run = this.#createChain.then(async () => await this.#createSessionUnserialized(repo, idempotencyKey));
		this.#createChain = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
	}

	async #createSessionUnserialized(repo: string, idempotencyKey: string): Promise<{ readonly sessionId?: unknown }> {
		let lastFailure: unknown;
		for (let attempt = 1; attempt <= SESSION_CREATE_ATTEMPTS; attempt++) {
			try {
				return parseEnvelope<{ sessionId?: unknown }>(
					await this.#cli([
						"sdk",
						"session",
						"raw",
						"global",
						"--op",
						"session.create",
						"--idempotency-key",
						idempotencyKey,
						"--json-input",
						JSON.stringify({ cwd: repo }),
					]),
					"session.create",
				);
			} catch (error) {
				lastFailure = error;
				if (!isTransientCreateFailure(error) || attempt === SESSION_CREATE_ATTEMPTS) throw sanitizeSdkFailure(error);
				await this.#sleep(SESSION_CREATE_RETRY_MS);
			}
		}
		throw sanitizeSdkFailure(lastFailure ?? new Error("session.create did not produce a result"));
	}

	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		return await this.#safe(async () => await inspectSession(this.#controller(input.repo), input.sessionId));
	}

	async resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding> {
		const existing = await this.inspect(input);
		if (!existing || existing.deleted || existing.repo !== input.repo)
			throw new Error(`cannot resume session ${input.sessionId}: saved authority is unavailable`);
		if (!existing.live) {
			parseEnvelope(
				await this.#cli([
					"sdk",
					"session",
					"raw",
					"control",
					input.sessionId,
					"--op",
					"session.resume",
					"--json-input",
					"{}",
				]),
				"session.resume",
			);
			const resumed = await this.inspect(input);
			if (!resumed || resumed.deleted || !resumed.live || resumed.repo !== input.repo)
				throw new Error(`session.resume did not restore live authority for ${input.sessionId}`);
		}
		return { sessionId: input.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
	}

	async send(input: SessionSendInput): Promise<SendReceipt> {
		assertValidOpRef(input.opRef);
		if (input.model) await this.setModel({ sessionId: input.sessionId, repo: input.repo, selection: input.model });
		return await sendPrompt(this.#controller(input.repo), {
			sessionId: input.sessionId,
			text: renderPrompt(input.systemPreamble, input.text),
			taskKey: "gateway",
			opRef: input.opRef,
		});
	}

	async steer(input: SessionSteerInput): Promise<void> {
		assertControlAllowed("turn.steer", { operatorApproval: true });
		parseEnvelope(
			await this.#cli([
				"sdk",
				"session",
				"raw",
				"control",
				input.sessionId,
				"--op",
				"turn.steer",
				"--json-input",
				JSON.stringify({ text: input.text, clientRef: input.clientRef }),
			]),
			"turn.steer",
		);
	}

	async setModel(input: { sessionId: string; repo: string; selection: GjcModelSelection }): Promise<{ readonly changed: boolean }> {
		const payload = typeof input.selection === "string" ? { id: input.selection } : { preset: input.selection.preset };
		const result = parseEnvelope<{ changed?: unknown }>(
			await this.#cli([
				"sdk",
				"session",
				"raw",
				"control",
				input.sessionId,
				"--op",
				"model.set",
				"--json-input",
				JSON.stringify(payload),
			]),
			"model.set",
		);
		if (typeof result.changed !== "boolean") throw new Error("model.set succeeded without a changed receipt");
		return { changed: result.changed };
	}

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		return await fetchOpState(this.#controller(input.repo), input.sessionId, input.opRef);
	}

	async fetchLastAssistant(input: { sessionId: string; repo: string }): Promise<LastAssistantResult> {
		const maxPages = 50;
		const chunks: string[] = [];
		let cursor: string | undefined;
		for (let pages = 1; pages <= maxPages; pages++) {
			const page = parseLastAssistantPage(
				await this.#cli([
					"sdk",
					"session",
					"raw",
					"query",
					input.sessionId,
					"--query",
					"session.last_assistant",
					"--repo",
					input.repo,
					...(cursor ? ["--cursor", cursor] : []),
				]),
			);
			chunks.push(page.text);
			if (page.complete) return { text: chunks.join(""), pages, complete: true };
			if (!page.cursor)
				throw new TranscriptIncompleteError(
					`session.last_assistant page ${pages} is incomplete but returned no continuation cursor`,
					pages,
				);
			cursor = page.cursor;
		}
		throw new TranscriptIncompleteError(`session.last_assistant did not complete within ${maxPages} pages`, maxPages);
	}

	async attachTail(input: TailAttachInput): Promise<TailHandle> {
		return await this.#tailRunner.attach(input);
	}

	async runCompaction(input: SessionCompactionInput): Promise<{ readonly status: SessionCompactionStatus }> {
		try {
			const result = parseEnvelope<Record<string, unknown>>(
				await this.#cli([
					"sdk",
					"session",
					"raw",
					"control",
					input.sessionId,
					"--op",
					"compaction.run",
					"--json-input",
					"{}",
				]),
				"compaction.run",
			);
			this.#tailRunner.recordCompactionReceipt({ sessionId: input.sessionId, originKey: input.originKey, result });
			if (result.started === true || result.status === "started" || result.status === "completed") return { status: "succeeded" };
			if (result.skipped === true || result.status === "skipped") return { status: "skipped" };
			return { status: "failed" };
		} catch (error) {
			const code = sdkErrorCode(error);
			if (code === "unsupported_operation" || code === "not_supported" || code === "unknown_operation") return { status: "unavailable" };
			return { status: "failed" };
		}
	}

	checkStalls(now = this.#now()): void {
		this.#tailRunner.checkStalls(now);
	}

	setStallTimeoutMs(timeoutMs: number): void {
		this.#tailRunner.setStallTimeoutMs(timeoutMs);
	}

	async runExclusive<T>(key: string, work: () => Promise<T>): Promise<T> {
		const previous = this.#chains.get(key);
		if (!previous) {
			const task = work();
			const settled = task.then(
				() => undefined,
				() => undefined,
			);
			this.#chains.set(key, settled);
			try {
				return await task;
			} finally {
				if (this.#chains.get(key) === settled) this.#chains.delete(key);
			}
		}
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const chain = previous.catch(() => undefined).then(() => gate);
		this.#chains.set(key, chain);
		await previous.catch(() => undefined);
		try {
			return await work();
		} finally {
			release();
			if (this.#chains.get(key) === chain) this.#chains.delete(key);
		}
	}

	async request(input: SessionRequestInput): Promise<SessionRequestResult> {
		// Attach before the send receipt. Status remains terminal authority, while the
		// logical tail owns liveness/cursor observation for every generic caller.
		const tail = await this.attachTail({
			sessionId: input.sessionId,
			brokerGeneration: 0,
			repo: input.repo,
			...(input.originKey ? { originKey: input.originKey } : {}),
			onStall: ({ elapsedMs }) => console.error(`session stall sessionId=${input.sessionId} opRef=${input.opRef} silentMs=${elapsedMs}`),
		});
		tail.setTurnRunning(true);
		try {
			const receipt = await this.send(input);
			tail.markAccepted(input.opRef);
			const deadline = this.#now() + (input.waitTimeoutMs ?? DEFAULT_REQUEST_WAIT_MS);
			const pollMs = input.pollMs ?? DEFAULT_STATUS_POLL_MS;
			let status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			while (!isTerminalStatus(status.status.status) && this.#now() < deadline) {
				this.checkStalls();
				await this.#sleep(pollMs);
				status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			}
			if (!isTerminalStatus(status.status.status)) throw new SessionRequestTimeoutError(input.sessionId, input.opRef, status);
			if (status.status.status !== "terminal_ok") throw new SessionTerminalError(status);
			return {
				receipt,
				status,
				assistant: await this.fetchLastAssistant({ sessionId: input.sessionId, repo: input.repo }),
			};
		} finally {
			tail.setTurnRunning(false);
			await tail.close();
		}
	}

	#controller(repo: string): ControllerOptions {
		return { run: this.#cli, repo };
	}
}

function renderPrompt(systemPreamble: string | undefined, text: string): string {
	if (!systemPreamble) return text;
	// SDK `session send` has no unproven system-prompt flag. Keep the trusted
	// bootstrap in the same accepted turn instead of inventing a raw control API.
	return `${systemPreamble}\n\n${text}`;
}

type LastAssistantPage = {
	readonly text: string;
	readonly complete: boolean;
	readonly cursor?: string;
};

/**
 * The installed SDK emits raw-query replies as `{ type: "query_response", page }`,
 * not a generic `{ result }` envelope. Keep that runtime-shape adaptation at the
 * gateway transport boundary; subsession remains the owner of generic contracts.
 */
function parseLastAssistantPage(result: CliResult): LastAssistantPage {
	if (result.exitCode !== 0) {
		parseEnvelope<unknown>(result, "session raw query session.last_assistant");
		throw new Error("unreachable session.last_assistant command result");
	}
	let envelope: Record<string, unknown>;
	try {
		const parsed: unknown = JSON.parse(result.stdout);
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("not an object");
		envelope = parsed as Record<string, unknown>;
	} catch {
		throw new Error("session.last_assistant did not print a JSON query response");
	}
	if (envelope.ok !== true) {
		parseEnvelope<unknown>(result, "session raw query session.last_assistant");
		throw new Error("unreachable failed session.last_assistant query response");
	}
	const page = envelope.page;
	if (typeof page !== "object" || page === null || Array.isArray(page))
		throw new Error("session.last_assistant succeeded without a query page");
	const items = (page as Record<string, unknown>).items;
	if (!Array.isArray(items) || items.some((item) => typeof item !== "string"))
		throw new Error("session.last_assistant query page contained non-text items");
	const cursor = (page as Record<string, unknown>).cursor;
	return {
		text: items.join(""),
		complete: (page as Record<string, unknown>).complete === true,
		...(typeof cursor === "string" && cursor.length > 0 ? { cursor } : {}),
	};
}

/**
 * Current `gjc sdk session` commands return a declared `{ ok: false, error }`
 * envelope with a non-zero process status. Subsession's parser receives the
 * envelope as its structured-error authority, so preserve that payload while
 * leaving unstructured process failures untouched.
 */
function normalizeSdkEnvelopeFailure(result: CliResult): CliResult {
	if (result.exitCode === 0) return result;
	try {
		const parsed: unknown = JSON.parse(result.stdout);
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed) &&
			(parsed as { ok?: unknown }).ok === false &&
			typeof (parsed as { error?: unknown }).error === "object" &&
			(parsed as { error?: unknown }).error !== null
		)
			return { ...result, exitCode: 0 };
	} catch {
		// A process failure without a complete JSON error envelope remains one.
	}
	return result;
}

function sdkErrorCode(error: unknown): string | undefined {
	if (error instanceof OpRefRejectedError) return stableErrorCode(error.code);
	if (error instanceof GjcCliError) return stableErrorCode(envelopeErrorCode(error.details));
	return undefined;
}

function stableErrorCode(value: unknown): string | undefined {
	if (typeof value !== "string" || !/^[a-z0-9_.-]{1,64}$/i.test(value)) return undefined;
	return value;
}

function sanitizedDetails(details: unknown): unknown {
	const code = stableErrorCode(envelopeErrorCode(details));
	const message =
		typeof details === "object" && details !== null && typeof (details as { message?: unknown }).message === "string"
			? sanitizeDiagnostic((details as { message: string }).message)
			: undefined;
	return { ...(code ? { code } : {}), ...(message ? { message } : {}) };
}

function sanitizeSdkFailure(error: unknown): Error {
	if (error instanceof OpRefRejectedError)
		return new OpRefRejectedError(error.opRef, stableErrorCode(error.code) ?? "sdk_error", sanitizedDetails(error.details));
	if (error instanceof GjcCliError) {
		const code = sdkErrorCode(error) ?? "sdk_error";
		return new GjcCliError(`gjc sdk request failed: ${code}`, error.exitCode, sanitizeDiagnostic(error.stderr), sanitizedDetails(error.details));
	}
	return new Error(sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error");
}

function sanitizeStatusReport(report: StatusReport): StatusReport {
	const failure = report.status.error;
	if (!failure) return report;
	const code = stableErrorCode(failure.code);
	const message = typeof failure.message === "string" ? sanitizeDiagnostic(failure.message) : undefined;
	return {
		...report,
		status: {
			...report.status,
			error: { ...(code ? { code } : {}), ...(message ? { message } : {}) },
		},
	};
}

/**
 * Create failures that are safe to retry under the SAME idempotency key: the
 * runtime either has not created the session (spawn_failed) or cannot yet
 * prove what it created (terminal_uncertain, uncertain_after_send). The key
 * guarantees the retry resolves to the same session, never a second one.
 * Measured: five simultaneous cold binds on one agent dir starve gjc's
 * lifecycle launcher and surface all three codes.
 */
function isTransientCreateFailure(error: unknown): boolean {
	if (!(error instanceof GjcCliError)) return false;
	const code = envelopeErrorCode(error.details);
	return code === "terminal_uncertain" || code === "uncertain_after_send" || code === "spawn_failed";
}

function sessionCreateRef(instanceId: string, originKey: string, epoch: number, repo: string): string {
	return `gw-bind-${createHash("sha256").update(`${instanceId}|${originKey}|${epoch}|${repo}`).digest("hex").slice(0, 32)}`;
}
