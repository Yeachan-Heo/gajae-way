import { createHash } from "node:crypto";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import {
	assertControlAllowed,
	assertValidOpRef,
	type BrokerSession,
	type CliResult,
	type CliRunner,
	type ControllerOptions,
	envelopeErrorCode,
	fetchOpState,
	GjcCliError,
	inspectSession,
	isTerminalStatus,
	type LastAssistantResult,
	OpRefRejectedError,
	parseEnvelope,
	type SendReceipt,
	type StatusReport,
	sendPrompt,
	TranscriptIncompleteError,
} from "@gajaeway/subsession";
import type { GjcModelSelection, GjcServiceTier } from "../config";
import type { GatewayDatabase } from "../store/db";
import { type FailedTurnEvidence, type FailedTurnEvidenceInput, readFailedTurnEvidence } from "./failed-turn-evidence";
import { sanitizeDiagnostic } from "./rebind";
import type { TailAttachInput, TailHandle, TailRunner } from "./tail-runner";

/**
 * Generic broker-backed session surface. Callers own prompt composition,
 * operation-reference selection, and recovery policy; this port only binds,
 * sends, observes, and reads terminal output through the SDK CLI.
 */
export interface SessionPort {
	bind(input: SessionBindInput): Promise<SessionBinding>;
	inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined>;
	liveness?(input: {
		sessionId: string;
		repo: string;
	}): Promise<{ readonly live: boolean | undefined; readonly disowned: boolean }>;
	/** True when the session's prompt queue has no pending messages (queue.messages.list empty). */
	queueEmpty?(input: { sessionId: string; repo: string }): Promise<boolean>;
	/** Recognized current-session provider failure, never authorization to replay an operation. */
	failedTurnEvidence?(input: FailedTurnEvidenceInput): Promise<FailedTurnEvidence | undefined>;
	/** Restores a saved, non-deleted session through `session.resume`; it never creates a replacement. */
	resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding>;
	send(input: SessionSendInput): Promise<SendReceipt>;
	steer(input: SessionSteerInput): Promise<void>;
	setModel(input: {
		sessionId: string;
		repo: string;
		selection: GjcModelSelection;
	}): Promise<{ readonly changed: boolean }>;
	setServiceTier(input: {
		sessionId: string;
		repo: string;
		tier: GjcServiceTier;
	}): Promise<{ readonly changed: boolean }>;
	status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport>;
	/** Exact invocation-owned original output; never falls back to a latest-assistant heuristic. */
	fetchWorkerOutput(input: WorkerOutputInput): Promise<WorkerOutputResult>;
	fetchLastAssistant(input: { sessionId: string; repo: string }): Promise<LastAssistantResult>;
	/**
	 * Last assistant row NOT older than `notBeforeMs`. Callers pass the op's
	 * reported startedAt when present, otherwise the turn's `dispatched_at`
	 * (stamped at bind, before the send, and cleared on requeue). Turn-scoped
	 * by wall clock, independent of gjc ring
	 * coordinates (gajae-code#5200); undefined when the newest row predates
	 * the turn.
	 */
	fetchAssistantSince?(input: {
		sessionId: string;
		repo: string;
		notBeforeMs: number;
	}): Promise<LastAssistantResult | undefined>;
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
	/**
	 * Every session the broker indexes for this agent directory, live or saved.
	 * Recovery-only surface: the persona actor never lists, it inspects by id.
	 */
	listSessions?(): Promise<readonly IndexedSession[]>;
	/**
	 * Removes a saved (non-live) session from the broker's index and disk. The
	 * broker's own guards still apply: a live session or one with pending
	 * cleanup is refused, and the refusal is returned, never thrown.
	 */
	deleteSession?(input: { sessionId: string; cwd: string; sessionPath: string }): Promise<SessionDeleteOutcome>;
	/**
	 * Closes a live session (`session.close`). Recoverable on the broker side and
	 * never `session.delete`: gjc fences every later lifecycle op on one refused
	 * delete, so the gateway retires lanes by closing and rebinding instead.
	 */
	close(input: { sessionId: string; repo: string }): Promise<void>;
}

export interface IndexedSession {
	readonly sessionId: string;
	readonly live: boolean;
	readonly cwd: string | undefined;
	/** Absolute path of the saved session file, when the broker reports one. */
	readonly sessionPath: string | undefined;
	/** Broker-reported last activity, epoch ms; undefined when it reports none. */
	readonly lastActivityMs: number | undefined;
}

export type SessionDeleteOutcome =
	| { readonly deleted: true }
	| { readonly deleted: false; readonly code: string; readonly message: string };
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
	/** Startup selection; presets must be activated by session.create, not model.set. */
	readonly model?: GjcModelSelection;
	/** The SDK host's default coding register is retained when true. */
	readonly codingRegister?: boolean;
	/** Internal recursion fence: one poisoned create key may advance to one fresh epoch per bind call. */
	readonly epochRecovery?: boolean;
}

export interface SessionBinding {
	readonly sessionId: string;
	readonly originKey: string;
	readonly epoch: number;
	readonly repo: string;
	readonly startupModelApplied?: boolean;
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

export interface WorkerOutputInput {
	readonly sessionId: string;
	readonly repo: string;
	readonly opRef: string;
	/** Exact pre-send floor, optionally tightened by the broker's startedAt. No clock tolerance. */
	readonly notBeforeMs: number;
	/** Only actual receipt/status identities, never locally synthesized identifiers. */
	readonly terminalIdentity?: { readonly commandId?: string; readonly turnId?: string };
	readonly signal?: AbortSignal;
	/** Generation/attempt fence, checked before and after transport I/O. */
	readonly isCurrent?: () => boolean;
}

export type WorkerOutputResult =
	| {
			readonly status: "proven";
			readonly text: string;
			readonly observedAtMs: number;
			readonly provenance: {
				readonly source: "turn.result";
				readonly fullness: "original";
				readonly sessionId: string;
				readonly repo: string;
				readonly opRef: string;
				readonly clientRef: string;
				readonly commandId?: string;
				readonly turnId?: string;
				readonly terminalAt: number;
				readonly contentVersion: 1;
				readonly byteLength: number;
			};
	  }
	| { readonly status: "absent"; readonly code: "output_pending" | "transport_error" }
	| {
			readonly status: "unavailable";
			readonly code: "output_unavailable" | "invalid_evidence" | "identity_mismatch" | "incomplete_body" | "cancelled";
	  };

export interface SessionRequestInput extends SessionSendInput {
	/** Stable caller identity carried into tail observability. */
	readonly originKey?: string;
	readonly waitTimeoutMs?: number;
	readonly pollMs?: number;
	/** Monitor/batch authoring needs only terminal status + final answer, not live tail replay. */
	readonly observeTail?: boolean;
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
	/** The private gjc agent directory; needed to locate saved session files for deletion. */
	readonly agentDir?: string;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_REQUEST_WAIT_MS = 30 * 60_000;
const DEFAULT_STATUS_POLL_MS = 500;
const SESSION_CREATE_ATTEMPTS = 5;
const SESSION_CREATE_READINESS_MS = 60_000;
const SESSION_READY_TIMEOUT_MS = 60_000;
const SESSION_READY_POLL_MS = 250;
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
	readonly #agentDir: string | undefined;

	constructor(options: BrokerSessionPortOptions) {
		this.#database = options.database;
		this.#agentDir = options.agentDir;
		this.#cli = async (args, commandOptions) => {
			const result = await options.cli(args, commandOptions);
			// Steering requires an unambiguous control receipt: do not promote a
			// failed transport to a definitive rejection merely because stdout is JSON.
			return args.includes("turn.steer") ? result : normalizeSdkEnvelopeFailure(result);
		};
		this.#instanceId = options.instanceId;
		this.#tailRunner = options.tailRunner;
		this.#now = options.now ?? (() => Date.now());
		this.#sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
	}

	failedTurnEvidence(input: FailedTurnEvidenceInput): Promise<FailedTurnEvidence | undefined> {
		return readFailedTurnEvidence(this.#agentDir, input);
	}

	async #safe<T>(work: () => Promise<T>): Promise<T> {
		try {
			return await work();
		} catch (error) {
			throw sanitizeSdkFailure(error);
		}
	}

	async bind(input: SessionBindInput): Promise<SessionBinding> {
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("session epoch must be a non-negative integer");
		const existing = this.#database.getSessionRecord(input.originKey);
		if (existing?.epoch === input.epoch && existing.sessionId) {
			// A persisted binding is only reusable if the broker still indexes it. A
			// binding written against a store the current runtime cannot read
			// (pre-cutover session ids) must be rebound, not handed to a send that
			// will fail with session_unavailable forever. Inspect failure (transport
			// outage) keeps the binding: that is not evidence the session is gone.
			let indexed = true;
			try {
				// Judge the raw envelope: a persisted id is reusable only when the
				// A persisted id is reusable if live, and resumable if it still has
				// saved authority. Monitor authoring reaches SessionPort directly, so
				// resume here before paying for a cold replacement session.
				const result = await this.#cli(["sdk", "session", "inspect", existing.sessionId, "--repo", input.repo]);
				const envelope = JSON.parse(result.stdout) as {
					ok?: unknown;
					result?: { session?: { live?: unknown; deleted?: unknown } };
					error?: { code?: unknown };
				};
				if (envelope.ok === false) indexed = envelope.error?.code !== "session_unavailable";
				if (envelope.ok === true && envelope.result?.session?.live === false) {
					if (envelope.result.session.deleted !== true) {
						try {
							return await this.resume({
								sessionId: existing.sessionId,
								repo: input.repo,
								originKey: input.originKey,
								epoch: input.epoch,
							});
						} catch {
							// Saved authority cannot be resumed: replace it below.
						}
					}
					indexed = false;
				}
			} catch {
				indexed = true;
			}
			if (indexed)
				return { sessionId: existing.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
			const rebound = this.#database.rebindEpoch(input.originKey);
			console.error(
				`session_rebound origin=${input.originKey} epoch=${input.epoch} nextEpoch=${rebound} session=${existing.sessionId} reason=not_live_or_disowned_by_broker`,
			);
			return await this.bind({ ...input, epoch: rebound });
		}
		const idempotencyKey = sessionCreateRef(this.#instanceId, input.originKey, input.epoch, input.repo);
		let created: { readonly sessionId?: unknown };
		try {
			created = await this.#createSession(input.repo, idempotencyKey, input.model);
		} catch (error) {
			if (input.epochRecovery === false) throw error;
			const nextEpoch = this.#database.rebindEpoch(input.originKey);
			console.error(
				`session_create_epoch_rotated origin=${input.originKey} epoch=${input.epoch} nextEpoch=${nextEpoch} reason=poisoned_create_key`,
			);
			return await this.bind({ ...input, epoch: nextEpoch, epochRecovery: false });
		}
		if (typeof created.sessionId !== "string" || created.sessionId.length === 0) {
			throw new Error("session.create succeeded without a sessionId");
		}
		const persistedEpoch = this.#database.getSessionRecord(input.originKey)?.epoch;
		if (persistedEpoch !== undefined && persistedEpoch > input.epoch) {
			throw new Error(`session bind for ${input.originKey} epoch ${input.epoch} lost to epoch ${persistedEpoch}`);
		}
		if (!this.#database.putSessionAtEpoch(input.originKey, created.sessionId, input.epoch)) {
			throw new Error(
				`session bind for ${input.originKey} epoch ${input.epoch} lost to a concurrent durable epoch change`,
			);
		}
		// session.create returns once the host is admitted; the Router indexes it
		// a moment later. A tail/send before that answers session_unavailable, so
		// wait until the broker reports the id live before handing the binding out.
		await this.#awaitIndexed(created.sessionId, input.repo);
		return {
			sessionId: created.sessionId,
			originKey: input.originKey,
			epoch: input.epoch,
			repo: input.repo,
			...(input.model ? { startupModelApplied: true } : {}),
		};
	}

	#createChain: Promise<unknown> = Promise.resolve();

	/** Judged on the raw inspect envelope (gjc >= 0.16.0 omits locator.repo, which the subsession normalizer requires). */
	async #awaitIndexed(sessionId: string, repo: string): Promise<void> {
		const deadline = Date.now() + SESSION_READY_TIMEOUT_MS;
		let lastCode: string | undefined;
		for (;;) {
			try {
				const result = await this.#cli(["sdk", "session", "inspect", sessionId, "--repo", repo], { timeoutMs: 10_000 });
				const envelope = JSON.parse(result.stdout) as {
					ok?: unknown;
					result?: { session?: { live?: unknown } };
					error?: { code?: unknown };
				};
				// Only a broker that explicitly reports the id as not indexed / not
				// live keeps us waiting; anything else is treated as ready (the send
				// path still has its own recovery if that turns out to be wrong).
				const disowned = envelope.ok === false && envelope.error?.code === "session_unavailable";
				const notLive =
					envelope.ok === true && envelope.result?.session !== undefined && envelope.result.session.live === false;
				if (!disowned && !notLive) return;
				lastCode = disowned ? "session_unavailable" : "not_live";
			} catch (error) {
				const code = sdkErrorCode(error);
				if (code !== "session_unavailable") return;
				lastCode = code;
			}
			if (Date.now() >= deadline)
				throw new Error(`session ${sessionId} was created but never became live (${lastCode})`);
			await this.#sleep(SESSION_READY_POLL_MS);
		}
	}

	/** Cold creates are serialized per agent dir: parallel launches starve gjc's lifecycle launcher. */
	#createSession(
		repo: string,
		idempotencyKey: string,
		model: GjcModelSelection | undefined,
	): Promise<{ readonly sessionId?: unknown }> {
		const run = this.#createChain.then(async () => await this.#createSessionUnserialized(repo, idempotencyKey, model));
		this.#createChain = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
	}

	async #createSessionUnserialized(
		repo: string,
		idempotencyKey: string,
		model: GjcModelSelection | undefined,
	): Promise<{ readonly sessionId?: unknown }> {
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
						// A fresh session host boots the full agent (~10s measured on the
						// persona host); the runtime's 10s default readiness cutoff turns a
						// slow-but-healthy cold start into spawn_failed. Use the maximum
						// budget: create is idempotent under this key either way.
						JSON.stringify({
							cwd: repo,
							readinessTimeoutMs: SESSION_CREATE_READINESS_MS,
							...(typeof model === "string" ? { modelId: model } : model ? { modelPreset: model.preset } : {}),
						}),
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

	/**
	 * Raw liveness judged on the broker envelope: gjc >= 0.16.0 omits
	 * `locator.repo`, which makes the subsession normalizer return undefined for a
	 * perfectly well-known session. `disowned` = the broker rejects the id.
	 */
	async liveness(input: {
		sessionId: string;
		repo: string;
	}): Promise<{ readonly live: boolean | undefined; readonly disowned: boolean }> {
		try {
			const result = await this.#cli(["sdk", "session", "inspect", input.sessionId, "--repo", input.repo], {
				timeoutMs: 10_000,
			});
			const envelope = JSON.parse(result.stdout) as {
				ok?: unknown;
				result?: { session?: { live?: unknown } };
				error?: { code?: unknown };
			};
			if (envelope.ok === false) return { live: undefined, disowned: envelope.error?.code === "session_unavailable" };
			const live = envelope.result?.session?.live;
			return { live: typeof live === "boolean" ? live : undefined, disowned: false };
		} catch (error) {
			return { live: undefined, disowned: sdkErrorCode(error) === "session_unavailable" };
		}
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
		const raw = await this.#cli([
			"sdk",
			"session",
			"raw",
			"control",
			input.sessionId,
			"--op",
			"turn.steer",
			"--json-input",
			JSON.stringify({ text: input.text, clientRef: input.clientRef }),
		]);
		let receipt: unknown;
		try {
			receipt = parseEnvelope<unknown>(raw, "turn.steer");
		} catch (error) {
			// Recognized outer control refusals are decisions; malformed output and
			// transport/authority failures retain their uncertain error contract.
			const code = sdkErrorCode(error);
			let envelope: { ok?: unknown };
			try {
				envelope = JSON.parse(raw.stdout);
			} catch {
				throw error;
			}
			if (
				error instanceof GjcCliError &&
				error.exitCode === 0 &&
				envelope?.ok === false &&
				code &&
				[
					"busy",
					"steer_refused",
					"invalid_params",
					"not_running",
					"no_active_turn",
					"client_ref_conflict",
					"session_not_found",
				].includes(code)
			)
				throw new GjcCliError("gjc sdk turn.steer refused acceptance", 0, "", { code, refused: true });
			throw error;
		}
		const body = workerRecord(receipt);
		// Synthetic negative receipts are not authoritative control rejections.
		if (body?.clientRef !== input.clientRef)
			throw new GjcCliError("gjc sdk turn.steer identity mismatch", 0, "", { code: "receipt_identity_mismatch" });
		if (body?.clientRef === input.clientRef && body.accepted === false && body.status === "rejected") {
			throw new GjcCliError("gjc sdk turn.steer rejected acceptance", 0, "", { code: "steer_refused", refused: true });
		}
		if (body?.accepted !== true || body.ok === false || (body.status !== undefined && body.status !== "accepted"))
			throw new GjcCliError("gjc sdk turn.steer acceptance unavailable", 0, "", { code: "receipt_identity_mismatch" });
	}

	async setModel(input: {
		sessionId: string;
		repo: string;
		selection: GjcModelSelection;
	}): Promise<{ readonly changed: boolean }> {
		if (typeof input.selection !== "string") {
			const activation = parseEnvelope<boolean | { changed?: unknown; id?: unknown }>(
				await this.#cli([
					"sdk",
					"session",
					"raw",
					"control",
					input.sessionId,
					"--op",
					"model.profile.set",
					"--json-input",
					JSON.stringify({ id: input.selection.preset }),
				]),
				"model.profile.set",
			);
			const changed = typeof activation === "boolean" ? activation : activation.changed;
			if (typeof changed !== "boolean") throw new Error("model.profile.set succeeded without a changed receipt");
			return { changed };
		}
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
				JSON.stringify({ id: input.selection }),
			]),
			"model.set",
		);
		if (typeof result.changed !== "boolean") throw new Error("model.set succeeded without a changed receipt");
		return { changed: result.changed };
	}

	async setServiceTier(input: {
		sessionId: string;
		repo: string;
		tier: GjcServiceTier;
	}): Promise<{ readonly changed: boolean }> {
		const result = parseEnvelope<{ changed?: unknown }>(
			await this.#cli([
				"sdk",
				"session",
				"raw",
				"control",
				input.sessionId,
				"--op",
				"service_tier.set",
				"--json-input",
				JSON.stringify({ tier: input.tier }),
			]),
			"service_tier.set",
		);
		if (typeof result.changed !== "boolean") throw new Error("service_tier.set succeeded without a changed receipt");
		return { changed: result.changed };
	}

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		return await fetchOpState(this.#controller(input.repo), input.sessionId, input.opRef);
	}

	async fetchWorkerOutput(input: WorkerOutputInput): Promise<WorkerOutputResult> {
		if (workerOutputCancelled(input)) return { status: "unavailable", code: "cancelled" };
		if (!Number.isFinite(input.notBeforeMs) || !input.opRef) return { status: "unavailable", code: "invalid_evidence" };
		// Verified SDK Q26: turn.result carries invocation-owned content, not a
		// transcript summary. It is capped at 16 KiB and explicitly disallows
		// cursors. One bounded query replaces an unbounded transcript traversal;
		// truncated results are NOT silently promoted to complete original text.
		const read = async (): Promise<WorkerOutputResult> => {
			if (workerOutputCancelled(input)) return { status: "unavailable", code: "cancelled" };
			try {
				const raw = await this.#cli(
					[
						"sdk",
						"session",
						"raw",
						"query",
						input.sessionId,
						"--query",
						"turn.result",
						"--repo",
						input.repo,
						"--json-input",
						JSON.stringify({ kind: "prompt", clientRef: input.opRef }),
					],
					{ timeoutMs: 15_000 },
				);
				return parseWorkerOutputResponse(input, raw, this.#now());
			} catch {
				return workerOutputCancelled(input)
					? { status: "unavailable", code: "cancelled" }
					: { status: "absent", code: "transport_error" };
			}
		};
		// CliRunner has no AbortSignal transport contract. Cancel the consumer
		// immediately; the read-only subprocess retains its finite timeout and
		// its late result has no effects. Never invent an SDK cancellation flag.
		if (!input.signal) return await read();
		let onAbort!: () => void;
		const cancelled = new Promise<WorkerOutputResult>((resolve) => {
			onAbort = () => resolve({ status: "unavailable", code: "cancelled" });
			input.signal!.addEventListener("abort", onAbort, { once: true });
			if (input.signal!.aborted) onAbort();
		});
		try {
			return await Promise.race([read(), cancelled]);
		} finally {
			input.signal.removeEventListener("abort", onAbort);
		}
	}

	async queueEmpty(input: { sessionId: string; repo: string }): Promise<boolean> {
		const result = await this.#cli(
			[
				"sdk",
				"session",
				"raw",
				"query",
				input.sessionId,
				"--query",
				"queue.messages.list",
				"--repo",
				input.repo,
				"--json-input",
				"{}",
			],
			{ timeoutMs: 10_000 },
		);
		const page = (JSON.parse(result.stdout) as { ok?: unknown; page?: { items?: unknown[]; complete?: unknown } }).page;
		return page !== undefined && Array.isArray(page.items) && page.items.length === 0 && page.complete === true;
	}

	/**
	 * One `session.list --scope all` per page. The broker caps continuation
	 * cursors at 32 per 15 minutes and leaks one per traversal that stops early,
	 * so a large index makes every id-resolving CLI call fail with
	 * `session.list cursor capacity is exhausted` (live: jip, 149 sessions,
	 * 2026-09-06). This listing exists so the GC can shrink the index below one
	 * page; it drains every page so the broker releases its own cursor.
	 */
	async listSessions(): Promise<readonly IndexedSession[]> {
		const sessions: IndexedSession[] = [];
		let cursor: string | undefined;
		const seen = new Set<string>();
		for (let pages = 0; pages < 100; pages++) {
			// Raw global op, not `session list --scope all`: the scoped form needs a
			// git repository at the cwd (p25's workspace is none) and the health probe
			// already established `raw global session.list` as the portable path.
			// Largest page the broker allows: fewer cursors per traversal, and a
			// traversal that drains to the end is the only one that frees its cursor.
			const result = await this.#cli(
				[
					"sdk",
					"session",
					"raw",
					"global",
					"--op",
					"session.list",
					"--json-input",
					JSON.stringify({ limit: 100, ...(cursor ? { cursor } : {}) }),
				],
				{ timeoutMs: 30_000 },
			);
			const page = parseEnvelope<{ sessions?: unknown[]; continuationCursor?: unknown }>(result, "session.list");
			for (const raw of Array.isArray(page.sessions) ? page.sessions : []) {
				const row = raw as {
					sessionId?: unknown;
					live?: unknown;
					deleted?: unknown;
					locator?: { cwd?: unknown };
					lastHeartbeatAt?: unknown;
					activity?: { at?: unknown; updatedAt?: unknown };
				};
				if (typeof row.sessionId !== "string" || row.deleted === true) continue;
				const heartbeat = typeof row.lastHeartbeatAt === "number" ? row.lastHeartbeatAt : undefined;
				sessions.push({
					sessionId: row.sessionId,
					live: row.live === true,
					cwd: typeof row.locator?.cwd === "string" ? row.locator.cwd : undefined,
					sessionPath: await this.#savedSessionPath(row.sessionId),
					lastActivityMs: heartbeat,
				});
			}
			const next = typeof page.continuationCursor === "string" ? page.continuationCursor : undefined;
			if (!next || seen.has(next)) return sessions;
			seen.add(next);
			cursor = next;
		}
		return sessions;
	}

	/** `<agentDir>/sessions/<bucket>/<timestamp>_<sessionId>.jsonl`, the file the broker's delete wants named. */
	async #savedSessionPath(sessionId: string): Promise<string | undefined> {
		if (!this.#agentDir) return undefined;
		const root = join(this.#agentDir, "sessions");
		let buckets: string[];
		try {
			buckets = await readdir(root);
		} catch {
			return undefined;
		}
		for (const bucket of buckets) {
			let names: string[];
			try {
				names = await readdir(join(root, bucket));
			} catch {
				continue;
			}
			const hit = names.find((name) => name.endsWith(`_${sessionId}.jsonl`));
			if (hit) return join(root, bucket, hit);
		}
		return undefined;
	}

	async deleteSession(input: { sessionId: string; cwd: string; sessionPath: string }): Promise<SessionDeleteOutcome> {
		assertControlAllowed("session.delete", { operatorApproval: true });
		const result = await this.#cli(
			[
				"sdk",
				"session",
				"raw",
				"global",
				"--op",
				"session.delete",
				"--idempotency-key",
				`gw-gc-${this.#instanceId}-${input.sessionId}-${this.#now()}`,
				"--json-input",
				JSON.stringify({ sessionId: input.sessionId, cwd: input.cwd, sessionPath: input.sessionPath }),
			],
			{ timeoutMs: 30_000 },
		);
		let envelope: { ok?: unknown; error?: { code?: unknown; message?: unknown } };
		try {
			envelope = JSON.parse(result.stdout) as typeof envelope;
		} catch {
			return { deleted: false, code: "malformed_envelope", message: `exit ${result.exitCode}` };
		}
		if (envelope.ok === true) return { deleted: true };
		return {
			deleted: false,
			code: typeof envelope.error?.code === "string" ? envelope.error.code : "unknown",
			message: sanitizeDiagnostic(typeof envelope.error?.message === "string" ? envelope.error.message : ""),
		};
	}

	async close(input: { sessionId: string; repo: string }): Promise<void> {
		assertControlAllowed("session.close", { operatorApproval: true });
		// Lifecycle op: the per-session `control` route prohibits it for the
		// daemon CLI (adapter_operation_prohibited on gjc 0.16.x); only the
		// `global` route carries it, like session.create and session.delete.
		parseEnvelope(
			await this.#cli(
				[
					"sdk",
					"session",
					"raw",
					"global",
					"--op",
					"session.close",
					"--idempotency-key",
					`gw-close-${this.#instanceId}-${input.sessionId}-${this.#now()}`,
					"--json-input",
					JSON.stringify({ sessionId: input.sessionId }),
				],
				{ timeoutMs: 30_000 },
			),
			"session.close",
		);
	}

	async fetchAssistantSince(input: {
		sessionId: string;
		repo: string;
		notBeforeMs: number;
	}): Promise<LastAssistantResult | undefined> {
		let cursor: string | undefined;
		let latest: { role?: string; ts?: string; textSummary?: string; body?: string } | undefined;
		const seenCursors = new Set<string>();
		for (let pages = 1; pages <= 1_000; pages++) {
			const result = await this.#cli(
				[
					"sdk",
					"session",
					"raw",
					"query",
					input.sessionId,
					"--query",
					"transcript.list",
					"--repo",
					input.repo,
					"--json-input",
					"{}",
					...(cursor ? ["--cursor", cursor] : []),
				],
				{ timeoutMs: 15_000 },
			);
			const page = (
				JSON.parse(result.stdout) as {
					page?: {
						items?: Array<{ role?: string; ts?: string; textSummary?: string; body?: string }>;
						complete?: unknown;
						continuationCursor?: unknown;
					};
				}
			).page;
			if (!page || !Array.isArray(page.items)) throw new Error("transcript.list returned no page items");
			for (const row of page.items) {
				if (row.role !== "assistant") continue;
				const at = typeof row.ts === "string" ? Date.parse(row.ts) : Number.NaN;
				if (Number.isFinite(at) && at + 2_000 >= input.notBeforeMs) latest = row;
			}
			if (page.complete === true) {
				if (!latest) return undefined;
				const text =
					(typeof latest.body === "string" && latest.body) ||
					(typeof latest.textSummary === "string" ? latest.textSummary : "");
				return { text, pages, complete: true };
			}
			const next = typeof page.continuationCursor === "string" ? page.continuationCursor : undefined;
			if (!next || seenCursors.has(next))
				throw new TranscriptIncompleteError(
					"transcript.list returned an incomplete page without a fresh continuation cursor",
					pages,
				);
			seenCursors.add(next);
			cursor = next;
		}
		throw new TranscriptIncompleteError("transcript.list exceeded 1000 recovery pages", 1_000);
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
			if (result.started === true || result.status === "started" || result.status === "completed")
				return { status: "succeeded" };
			if (result.skipped === true || result.status === "skipped") return { status: "skipped" };
			return { status: "failed" };
		} catch (error) {
			const code = sdkErrorCode(error);
			if (code === "unsupported_operation" || code === "not_supported" || code === "unknown_operation")
				return { status: "unavailable" };
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
		// Chat-like callers attach before send for live output. Monitor authoring
		// sets observeTail=false: it consumes no intermediate frames, and old
		// session history must not be able to fail an otherwise valid final-result
		// request merely because its tail revision metadata predates the provider.
		const tail =
			input.observeTail === false
				? undefined
				: await this.attachTail({
						sessionId: input.sessionId,
						brokerGeneration: 0,
						repo: input.repo,
						...(input.originKey ? { originKey: input.originKey } : {}),
						onStall: ({ elapsedMs }) =>
							console.error(`session stall sessionId=${input.sessionId} opRef=${input.opRef} silentMs=${elapsedMs}`),
					});
		tail?.setTurnRunning(true);
		try {
			let receipt: SendReceipt;
			let status: StatusReport | undefined;
			try {
				receipt = await this.send(input);
				tail?.markAccepted(input.opRef);
			} catch (sendError) {
				// A transport/control failure may occur after the runtime accepted the
				// prompt. Query the SAME clientRef before retrying; monitor authoring
				// otherwise ran the event, produced a final answer, then executed it
				// again because the torn send was treated as definitive failure.
				try {
					status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
				} catch {
					throw sendError;
				}
				if (status.status.status === "unknown") throw sendError;
				receipt = { sessionId: input.sessionId, operationRef: input.opRef } as SendReceipt;
				tail?.markAccepted(input.opRef);
			}
			const deadline = this.#now() + (input.waitTimeoutMs ?? DEFAULT_REQUEST_WAIT_MS);
			const pollMs = input.pollMs ?? DEFAULT_STATUS_POLL_MS;
			status ??= await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			while (!isTerminalStatus(status.status.status) && this.#now() < deadline) {
				this.checkStalls();
				await this.#sleep(pollMs);
				status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			}
			if (!isTerminalStatus(status.status.status))
				throw new SessionRequestTimeoutError(input.sessionId, input.opRef, status);
			if (status.status.status !== "terminal_ok") throw new SessionTerminalError(status);
			return {
				receipt,
				status,
				assistant: await this.fetchLastAssistant({ sessionId: input.sessionId, repo: input.repo }),
			};
		} finally {
			tail?.setTurnRunning(false);
			await tail?.close();
		}
	}

	#controller(repo: string): ControllerOptions {
		return { run: this.#cli, repo };
	}
}

/**
 * Parse the installed SDK's canonical TurnResultPage/TurnResultContent DTOs.
 * Kept at the transport boundary so raw fixtures exercise exactly production
 * attribution/fullness checks. Neither a transcript row nor page.complete is
 * invocation-owned final-body evidence.
 */
export function parseWorkerOutputResponse(
	input: WorkerOutputInput,
	response: CliResult,
	observedAtMs: number,
): WorkerOutputResult {
	if (workerOutputCancelled(input)) return { status: "unavailable", code: "cancelled" };
	if (!Number.isFinite(input.notBeforeMs) || !Number.isFinite(observedAtMs) || !input.opRef)
		return { status: "unavailable", code: "invalid_evidence" };
	let envelope: Record<string, unknown> | undefined;
	try {
		envelope = workerRecord(JSON.parse(response.stdout));
	} catch {
		return response.exitCode === 0
			? { status: "unavailable", code: "invalid_evidence" }
			: { status: "absent", code: "transport_error" };
	}
	if (envelope?.ok === false) {
		const code = workerRecord(envelope.error)?.code;
		if (
			typeof code === "string" &&
			[
				"session_unavailable",
				"resource_gone",
				"unavailable",
				"unsupported_operation",
				"unknown_operation",
				"unknown_query",
				"unsupported_query",
				"not_supported",
				"operation_not_session_owned",
			].includes(code)
		)
			return { status: "unavailable", code: "output_unavailable" };
		return { status: "absent", code: "transport_error" };
	}
	if (response.exitCode !== 0) return { status: "absent", code: "transport_error" };
	const result = workerRecord(envelope?.result);
	if (envelope?.ok !== true || !result) return { status: "unavailable", code: "invalid_evidence" };
	if (result.status === "unknown") return { status: "absent", code: "output_pending" };
	if (result.kind !== "prompt" || result.clientRef !== input.opRef)
		return { status: "unavailable", code: "identity_mismatch" };
	for (const key of ["commandId", "turnId"] as const) {
		const expected = input.terminalIdentity?.[key];
		if (
			(expected !== undefined && result[key] !== expected) ||
			(result[key] !== undefined && (typeof result[key] !== "string" || result[key] === ""))
		)
			return { status: "unavailable", code: "identity_mismatch" };
	}
	if (result.status === "accepted" || result.status === "in_flight")
		return { status: "absent", code: "output_pending" };
	if (
		result.status !== "terminal_ok" ||
		typeof result.terminalAt !== "number" ||
		!Number.isFinite(result.terminalAt) ||
		result.terminalAt < input.notBeforeMs
	)
		return { status: "unavailable", code: "invalid_evidence" };
	if (
		result.startedAt !== undefined &&
		(typeof result.startedAt !== "number" ||
			!Number.isFinite(result.startedAt) ||
			result.startedAt < input.notBeforeMs ||
			result.startedAt > result.terminalAt)
	)
		return { status: "unavailable", code: "invalid_evidence" };
	const content = workerRecord(result.content);
	if (!content) {
		if (result.content !== undefined || result.textSummary !== undefined)
			return { status: "unavailable", code: "invalid_evidence" };
		return result.receiptState === "missing" || result.receiptState === "absent"
			? { status: "unavailable", code: "output_unavailable" }
			: { status: "absent", code: "output_pending" };
	}
	if (content.truncated === true) return { status: "unavailable", code: "incomplete_body" };
	if (
		content.version !== 1 ||
		content.type !== "text" ||
		typeof content.text !== "string" ||
		content.truncated !== false ||
		content.byteLength !== new TextEncoder().encode(content.text).length ||
		(result.receiptState !== undefined && result.receiptState !== "present")
	)
		return { status: "unavailable", code: "invalid_evidence" };
	return {
		status: "proven",
		text: content.text,
		observedAtMs,
		provenance: {
			source: "turn.result",
			fullness: "original",
			sessionId: input.sessionId,
			repo: input.repo,
			opRef: input.opRef,
			clientRef: result.clientRef,
			...(typeof result.commandId === "string" ? { commandId: result.commandId } : {}),
			...(typeof result.turnId === "string" ? { turnId: result.turnId } : {}),
			terminalAt: result.terminalAt,
			contentVersion: 1,
			byteLength: content.byteLength as number,
		},
	};
}

function workerRecord(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function workerOutputCancelled(input: WorkerOutputInput): boolean {
	try {
		return input.signal?.aborted === true || input.isCurrent?.() === false;
	} catch {
		// An unreadable fence is not authority to publish a late answer.
		return true;
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
		return new OpRefRejectedError(
			error.opRef,
			stableErrorCode(error.code) ?? "sdk_error",
			sanitizedDetails(error.details),
		);
	if (error instanceof GjcCliError) {
		const code = sdkErrorCode(error) ?? "sdk_error";
		return new GjcCliError(
			`gjc sdk request failed: ${code}`,
			error.exitCode,
			sanitizeDiagnostic(error.stderr),
			sanitizedDetails(error.details),
		);
	}
	return new Error(sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error");
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
