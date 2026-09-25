import { createHash } from "node:crypto";
import {
	assertControlAllowed,
	assertValidOpRef,
	type BrokerSession,
	CLIENT_REF_CONFLICT_CODE,
	type CliResult,
	type CliRunner,
	type ControllerOptions,
	envelopeErrorCode,
	fetchOpState,
	GjcCliError,
	inspectSession,
	isTerminalStatus,
	type LastAssistantResult,
	OpRefError,
	OpRefRejectedError,
	parseEnvelope,
	parseStatusReport,
	type SendReceipt,
	type StatusReport,
	TranscriptIncompleteError,
} from "@gajae-gateway/subsession";
import type { GjcModelSelection, GjcServiceTier } from "../config";
import { type BrokerAuthority, BrokerAuthorityError, type GatewayDatabase } from "../store/db";
import { type FailedTurnEvidence, type FailedTurnEvidenceInput, readFailedTurnEvidence } from "./failed-turn-evidence";
import { isRebindableCode, sanitizeDiagnostic } from "./rebind";
import {
	isRelayTransportFailure,
	type RelayResponse,
	type TailAttachInput,
	type TailHandle,
	type TailRunner,
} from "./tail-runner";

/**
 * Generic broker-backed session surface. Callers own prompt composition,
 * operation-reference selection, and recovery policy; this port only binds,
 * sends, observes, and reads terminal output through the SDK CLI.
 */
export type TerminateHostOutcome =
	| { readonly outcome: "terminated"; readonly pid: number }
	| { readonly outcome: "already_gone" }
	| { readonly outcome: "not_a_host"; readonly pid: number; readonly command: string }
	| { readonly outcome: "refused"; readonly reason: string };

export interface SessionPort {
	bind(input: SessionBindInput): Promise<SessionBinding>;
	inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined>;
	liveness?(input: {
		sessionId: string;
		repo: string;
	}): Promise<{ readonly live: boolean | undefined; readonly disowned: boolean }>;
	/** True when the session's prompt queue has no pending messages (queue.messages.list empty). */
	queueEmpty?(input: { sessionId: string; repo: string }): Promise<boolean>;
	/**
	 * Ends the host process of a session this gateway created and has retired.
	 * Ownership is the point: the shared GJC daemon and broker are never touched,
	 * only a `session-host-internal` whose pid the broker reports for THIS
	 * session id. Returns what it did so the caller can log it; never throws.
	 */
	terminateHost?(input: { sessionId: string; repo: string }): Promise<TerminateHostOutcome>;
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
	status(input: { sessionId: string; repo: string; opRef: string; relay?: TailHandle }): Promise<StatusReport>;
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
	 * Closes a live session (`session.close`). Recoverable on the broker side and
	 * never `session.delete`: gjc fences every later lifecycle op on one refused
	 * delete, so the gateway retires lanes by closing and rebinding instead.
	 */
	close(input: { sessionId: string; repo: string }): Promise<void>;
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
	/** How long a `busy` refusal is retried before it surfaces as a failure. */
	readonly busyWaitMs?: number;
	/**
	 * The session's resident relay. The prompt is submitted on it so the host
	 * streams the turn's content to this handle; without one the port opens a
	 * throwaway relay for the send (the turn then has no live observer).
	 */
	readonly relay?: TailHandle;
}

export interface SessionSteerInput {
	readonly sessionId: string;
	readonly repo: string;
	readonly text: string;
	readonly clientRef: string;
	readonly relay?: TailHandle;
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
	/**
	 * Inactivity lease, not a wall-clock cap: the wait gives up only after this
	 * long without a frame attributed to the turn. A turn that keeps emitting
	 * (tool calls, deltas) is observed for as long as it progresses (issue #9).
	 */
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
		super(`session operation ${opRef} emitted no attributable activity within the bounded request wait`);
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
	readonly authority: BrokerAuthority;
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
/** Maximum poisoned-create-key epoch rotations permitted per origin. */
export const MAX_POISONED_CREATE_ROTATIONS = 3;

function createRotationMetaKey(originKey: string): string {
	return `create_rotation:${originKey}`;
}
/**
 * The runtime refuses `turn.prompt` with `busy` while a previous turn on the
 * same session is still running. That is occupancy, not failure: the prompt
 * was never accepted and the op-ref is still unused. Wait for the session to
 * go idle (bounded) and resend under the same op-ref before giving up.
 */
export const SESSION_BUSY_CODE = "busy";
const DEFAULT_BUSY_WAIT_MS = 10 * 60_000;
const BUSY_POLL_MS = 2_000;

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
	readonly #authority: BrokerAuthority;

	constructor(options: BrokerSessionPortOptions) {
		this.#database = options.database;
		this.#authority = { ...options.authority };
		this.#database.assertBrokerAuthority(this.#authority);
		this.#cli = async (args, commandOptions) => {
			this.#database.assertBrokerAuthority(this.#authority);
			const result = await options.cli(args, commandOptions);
			this.#database.assertBrokerAuthority(this.#authority);
			// Steering requires an unambiguous control receipt: do not promote a
			// failed transport to a definitive rejection merely because stdout is JSON.
			return args.includes("turn.steer") ? result : normalizeSdkEnvelopeFailure(result);
		};
		this.#instanceId = options.instanceId;
		this.#tailRunner = options.tailRunner;
		this.#now = options.now ?? (() => Date.now());
		this.#sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
	}

	async failedTurnEvidence(input: FailedTurnEvidenceInput): Promise<FailedTurnEvidence | undefined> {
		this.#assertOwned(input);
		const evidence = await readFailedTurnEvidence(this.#authority.canonicalAgentDir, input);
		this.#assertOwned(input);
		return evidence;
	}

	async #safe<T>(work: () => Promise<T>): Promise<T> {
		try {
			return await work();
		} catch (error) {
			throw sanitizeSdkFailure(error);
		}
	}

	async bind(input: SessionBindInput): Promise<SessionBinding> {
		this.#database.assertBrokerAuthority(this.#authority);
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("session epoch must be a non-negative integer");
		const existing = this.#database.getSessionRecord(input.originKey);
		if (existing?.epoch === input.epoch && existing.sessionId) {
			this.#assertOwned({ sessionId: existing.sessionId, repo: input.repo });
			// Only owned bindings may be inspected, resumed, or replaced.
			let indexed = true;
			try {
				// Judge the raw envelope: a persisted id is reusable only when the
				// A persisted id is reusable if live, and resumable if it still has
				// saved authority. Monitor authoring reaches SessionPort directly, so
				// resume here before paying for a cold replacement session.
				const result = await this.#cli(["sdk", "session", "inspect", existing.sessionId]);
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
						} catch (error) {
							if (error instanceof BrokerAuthorityError) throw error;
							// Saved authority cannot be resumed: replace it below.
						}
					}
					indexed = false;
				}
			} catch (error) {
				if (error instanceof BrokerAuthorityError) throw error;
				indexed = true;
			}
			if (indexed) {
				this.#resetCreateRotations(input.originKey);
				return { sessionId: existing.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
			}
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
			if (error instanceof BrokerAuthorityError) throw error;
			if (input.epochRecovery === false || !isRebindableCode(sdkErrorCode(error))) throw error;
			const rotations = this.#createRotations(input.originKey);
			if (rotations >= MAX_POISONED_CREATE_ROTATIONS) {
				console.error(
					`session_create_rotation_capped origin=${input.originKey} rotations=${rotations} reason=poisoned_create_key_capped`,
				);
				throw error;
			}
			this.#database.metaSet(createRotationMetaKey(input.originKey), String(rotations + 1));
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
		if (
			!this.#database.recordOwnedBinding({
				authority: this.#authority,
				sessionId: created.sessionId,
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
			})
		) {
			throw new Error(
				`session bind for ${input.originKey} epoch ${input.epoch} lost to a concurrent durable epoch change`,
			);
		}
		// session.create returns once the host is admitted; the Router indexes it
		// a moment later. A tail/send before that answers session_unavailable, so
		// wait until the broker reports the id live before handing the binding out.
		await this.#awaitIndexed(created.sessionId, input.repo);
		this.#resetCreateRotations(input.originKey);
		return {
			sessionId: created.sessionId,
			originKey: input.originKey,
			epoch: input.epoch,
			repo: input.repo,
			...(input.model ? { startupModelApplied: true } : {}),
		};
	}

	#createRotations(originKey: string): number {
		const raw = this.#database.metaGet(createRotationMetaKey(originKey));
		const value = raw === undefined ? Number.NaN : Number(raw);
		return Number.isSafeInteger(value) && value >= 0 ? value : 0;
	}

	#resetCreateRotations(originKey: string): void {
		this.#database.metaSet(createRotationMetaKey(originKey), "0");
	}

	#createChain: Promise<unknown> = Promise.resolve();

	/** Judged on the raw inspect envelope (gjc >= 0.16.0 omits locator.repo, which the subsession normalizer requires). */
	async #awaitIndexed(sessionId: string, repo: string): Promise<void> {
		this.#assertOwned({ sessionId, repo });
		const deadline = Date.now() + SESSION_READY_TIMEOUT_MS;
		let lastCode: string | undefined;
		for (;;) {
			try {
				const result = await this.#cli(["sdk", "session", "inspect", sessionId], { timeoutMs: 10_000 });
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
				if (error instanceof BrokerAuthorityError) throw error;
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
		this.#assertOwned(input);
		try {
			const result = await this.#cli(["sdk", "session", "inspect", input.sessionId], {
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
			if (error instanceof BrokerAuthorityError) throw error;
			return { live: undefined, disowned: sdkErrorCode(error) === "session_unavailable" };
		}
	}

	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		this.#assertOwned(input);
		return await this.#safe(async () => await inspectSession(this.#controller(input.repo), input.sessionId));
	}

	/**
	 * A retired session's host is this gateway's to end: the gateway created the
	 * session, rebound away from it, and nothing else will ever prompt it again.
	 * Left alone, every `/new`, `/reset` and session-gone rebind leaked one live
	 * `session-host-internal` that kept polling the model provider (live,
	 * 2026-09-18: 5 orphaned hosts of 31 live sessions sharing one API key,
	 * every turn queued behind them). The SDK offers no close op
	 * (`session.close` is prohibited through the session CLI), so the pid the
	 * broker reports for the session is signalled directly - after proving it is
	 * a session host and not the shared daemon/broker.
	 */
	async terminateHost(input: { sessionId: string; repo: string }): Promise<TerminateHostOutcome> {
		this.#assertOwned(input);
		let pid: number | undefined;
		let live: boolean | undefined;
		try {
			const result = await this.#cli(["sdk", "session", "inspect", input.sessionId], {
				timeoutMs: 10_000,
			});
			const envelope = JSON.parse(result.stdout) as {
				ok?: unknown;
				result?: { session?: { pid?: unknown; live?: unknown } };
			};
			if (envelope.ok !== true) return { outcome: "already_gone" };
			const session = envelope.result?.session;
			pid =
				typeof session?.pid === "number" && Number.isSafeInteger(session.pid) && session.pid > 1
					? session.pid
					: undefined;
			live = typeof session?.live === "boolean" ? session.live : undefined;
		} catch (error) {
			if (error instanceof BrokerAuthorityError) throw error;
			return sdkErrorCode(error) === "session_unavailable"
				? { outcome: "already_gone" }
				: { outcome: "refused", reason: `inspect_failed:${sanitizeDiagnostic(String(error))}` };
		}
		if (pid === undefined || live === false) return { outcome: "already_gone" };
		const command = await processCommand(pid);
		if (command === undefined) return { outcome: "already_gone" };
		// The only process shape this gateway may end. Broker (`broker-internal`),
		// daemon (`daemon-internal`) and anything else are someone else's.
		if (!/\bsdk session-host-internal\b/.test(command) || /\b(broker|daemon)-internal\b/.test(command))
			return { outcome: "not_a_host", pid, command: command.slice(0, 160) };
		try {
			process.kill(pid, "SIGTERM");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") return { outcome: "already_gone" };
			return { outcome: "refused", reason: `kill_failed:${(error as NodeJS.ErrnoException).code ?? "unknown"}` };
		}
		return { outcome: "terminated", pid };
	}

	async resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding> {
		this.#assertOwned(input);
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
		this.#resetCreateRotations(input.originKey);
		return { sessionId: input.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
	}

	async send(input: SessionSendInput): Promise<SendReceipt> {
		this.#assertOwned(input);
		assertValidOpRef(input.opRef);
		if (input.text.trim().length === 0) throw new OpRefError("prompt text must not be empty");
		if (input.model) await this.setModel({ sessionId: input.sessionId, repo: input.repo, selection: input.model });
		const text = renderPrompt(input.systemPreamble, input.text);
		// The prompt is submitted on the session's resident relay so that this
		// connection OWNS the turn: the host streams the turn's content only to
		// the connection that submitted it. A caller without a relay gets one
		// for the duration of the send.
		const relay =
			input.relay ?? (await this.attachTail({ sessionId: input.sessionId, brokerGeneration: 0, repo: input.repo }));
		try {
			const deadline = this.#now() + (input.busyWaitMs ?? DEFAULT_BUSY_WAIT_MS);
			let waited = false;
			for (;;) {
				this.#database.assertBrokerAuthority(this.#authority);
				const response = await relay.control("turn.prompt", { text, clientRef: input.opRef });
				this.#database.assertBrokerAuthority(this.#authority);
				if (response.ok) {
					const result = response.result ?? {};
					const receipt = recordOf(result.receipt) ?? result;
					return {
						sessionId: input.sessionId,
						operationRef: input.opRef,
						...(typeof receipt.commandId === "string" ? { commandId: receipt.commandId } : {}),
						...(typeof receipt.turnId === "string" ? { turnId: receipt.turnId } : {}),
						acceptedAt: new Date(this.#now()).toISOString(),
						taskKey: "gateway",
					};
				}
				const error = relayFailure("turn.prompt", response);
				if (envelopeErrorCode(error.details) === CLIENT_REF_CONFLICT_CODE)
					throw new OpRefRejectedError(input.opRef, CLIENT_REF_CONFLICT_CODE, error.details);
				if (!isSessionBusy(error)) throw error;
				// `busy` is occupancy, not failure: the prompt was never accepted and
				// the op-ref is still unused. Wait for the session to go idle
				// (bounded) and resend under the same op-ref before giving up.
				if (this.#now() >= deadline) throw error;
				if (!waited) {
					waited = true;
					console.error(`session_busy_wait session=${input.sessionId} opRef=${input.opRef}`);
				}
				await this.#sleep(BUSY_POLL_MS);
			}
		} finally {
			if (!input.relay) await relay.close();
		}
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.#assertOwned(input);
		assertControlAllowed("turn.steer", { operatorApproval: true });
		if (input.text.trim().length === 0) throw new OpRefError("steer text must not be empty");
		const relay =
			input.relay ?? (await this.attachTail({ sessionId: input.sessionId, brokerGeneration: 0, repo: input.repo }));
		let response: RelayResponse;
		try {
			this.#database.assertBrokerAuthority(this.#authority);
			response = await relay.control("turn.steer", { text: input.text, clientRef: input.clientRef });
			this.#database.assertBrokerAuthority(this.#authority);
		} finally {
			if (!input.relay) await relay.close();
		}
		if (!response.ok) {
			// Recognized control refusals are decisions; anything else keeps its
			// uncertain error contract (the steer may or may not have landed).
			const code = response.error?.code;
			if (
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
			throw relayFailure("turn.steer", response);
		}
		const body = workerRecord(response.result);
		// Synthetic negative receipts are not authoritative control rejections.
		if (body?.clientRef !== input.clientRef)
			throw new GjcCliError("gjc sdk turn.steer identity mismatch", 0, "", { code: "receipt_identity_mismatch" });
		if (body?.clientRef === input.clientRef && body.accepted === false && body.status === "rejected") {
			throw new GjcCliError("gjc sdk turn.steer rejected acceptance", 0, "", { code: "steer_refused", refused: true });
		}
		if (!isSteerAccepted(body))
			throw new GjcCliError("gjc sdk turn.steer acceptance unavailable", 0, "", { code: "receipt_identity_mismatch" });
	}

	async setModel(input: {
		sessionId: string;
		repo: string;
		selection: GjcModelSelection;
	}): Promise<{ readonly changed: boolean }> {
		this.#assertOwned(input);
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
		this.#assertOwned(input);
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

	async status(input: { sessionId: string; repo: string; opRef: string; relay?: TailHandle }): Promise<StatusReport> {
		this.#assertOwned(input);
		// With a live relay the read is one round-trip on the owned connection;
		// without one (retired holds, work lanes, terminal recovery) the CLI's
		// `session status` performs the identical `turn.result` query.
		if (!input.relay) return await fetchOpState(this.#controller(input.repo), input.sessionId, input.opRef);
		this.#database.assertBrokerAuthority(this.#authority);
		const response = await input.relay.query("turn.result", { kind: "prompt", clientRef: input.opRef });
		this.#database.assertBrokerAuthority(this.#authority);
		if (!response.ok) throw relayFailure("turn.result", response);
		const status = response.result ?? {};
		const raw = typeof status.status === "string" ? status.status : "unknown";
		return parseStatusReport({
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				result: {
					operationRef: input.opRef,
					status,
					summary: { completed: raw === "terminal_ok" || raw === "failed" },
				},
			}),
			stderr: "",
		});
	}

	async fetchWorkerOutput(input: WorkerOutputInput): Promise<WorkerOutputResult> {
		this.#assertOwned(input);
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
						"--json-input",
						JSON.stringify({ kind: "prompt", clientRef: input.opRef }),
					],
					{ timeoutMs: 15_000 },
				);
				return parseWorkerOutputResponse(input, raw, this.#now());
			} catch (error) {
				if (error instanceof BrokerAuthorityError) throw error;
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
		this.#assertOwned(input);
		const result = await this.#cli(
			["sdk", "session", "raw", "query", input.sessionId, "--query", "queue.messages.list", "--json-input", "{}"],
			{ timeoutMs: 10_000 },
		);
		const page = (JSON.parse(result.stdout) as { ok?: unknown; page?: { items?: unknown[]; complete?: unknown } }).page;
		return page !== undefined && Array.isArray(page.items) && page.items.length === 0 && page.complete === true;
	}

	async close(input: { sessionId: string; repo: string }): Promise<void> {
		this.#assertOwned(input);
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
		this.#assertOwned(input);
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
		this.#assertOwned(input);
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
		this.#assertOwned(input);
		return await this.#tailRunner.attach(input);
	}

	async runCompaction(input: SessionCompactionInput): Promise<{ readonly status: SessionCompactionStatus }> {
		this.#assertOwned(input);
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
			if (error instanceof BrokerAuthorityError) throw error;
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
		this.#assertOwned(input);
		// One relay owns the whole request: the send, the terminal wait, and the
		// final read. Monitor/batch authoring consumes no mid-turn content, so
		// the handle's frames only feed the stall alarm and the activity lease.
		let lastActivityAt = this.#now();
		const relay = await this.attachTail({
			sessionId: input.sessionId,
			brokerGeneration: 0,
			repo: input.repo,
			...(input.originKey ? { originKey: input.originKey } : {}),
			onFrame: () => {
				lastActivityAt = this.#now();
			},
			onStall: ({ elapsedMs }) =>
				console.error(`session stall sessionId=${input.sessionId} opRef=${input.opRef} silentMs=${elapsedMs}`),
		});
		relay.beginTurn(input.opRef);
		relay.setTurnRunning(true);
		// `turn.result` on the relay while it is up; on the CLI (the authoritative
		// recovery transport) when the relay tore. A torn relay is never a verdict
		// about the operation: the accepted op keeps being observed under the SAME
		// clientRef, so a monitor event is not re-authored under a fresh one.
		const target = { sessionId: input.sessionId, repo: input.repo, opRef: input.opRef };
		const readStatus = async (): Promise<StatusReport> => {
			try {
				return await this.status({ ...target, relay });
			} catch (error) {
				if (!isRelayTransportFailure(error)) throw error;
				console.error(`status_relay_unavailable session=${input.sessionId} opRef=${input.opRef}`);
				return await this.status(target);
			}
		};
		try {
			let receipt: SendReceipt;
			let status: StatusReport | undefined;
			try {
				receipt = await this.send({ ...input, relay });
				relay.correlate(input.opRef, receipt);
			} catch (sendError) {
				if (sendError instanceof BrokerAuthorityError) throw sendError;
				// A transport/control failure may occur after the runtime accepted the
				// prompt. Query the SAME clientRef before retrying; monitor authoring
				// otherwise ran the event, produced a final answer, then executed it
				// again because the torn send was treated as definitive failure.
				try {
					status = await readStatus();
				} catch (error) {
					if (error instanceof BrokerAuthorityError) throw error;
					throw sendError;
				}
				if (status.status.status === "unknown") throw sendError;
				receipt = { sessionId: input.sessionId, operationRef: input.opRef } as SendReceipt;
			}
			// The wait leases on the turn's own activity: every frame the relay
			// attributes to this op moves the deadline forward. A fixed 1800 s cap
			// sat inside the ordinary duration of long monitor turns and killed the
			// request record while the work was still landing (issue #9).
			const leaseMs = input.waitTimeoutMs ?? DEFAULT_REQUEST_WAIT_MS;
			const pollMs = input.pollMs ?? DEFAULT_STATUS_POLL_MS;
			status ??= await readStatus();
			while (!isTerminalStatus(status.status.status) && this.#now() < lastActivityAt + leaseMs) {
				this.checkStalls();
				await this.#sleep(pollMs);
				status = await readStatus();
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
			relay.setTurnRunning(false);
			await relay.close();
		}
	}

	#assertOwned(input: { sessionId: string; repo: string }): void {
		this.#database.assertOwnedSession(input.sessionId, input.repo, this.#authority);
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
		// The SDK's receipt state is monotonic missing -> present: a late agent_end
		// can still attach the final body to this terminal op (#248), so `missing`
		// is retryable within the bounded read budget. A terminal op whose receipt
		// is `absent` contradicts the SDK contract and is not.
		return result.receiptState === "absent"
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

/** Affirmative SDK steer evidence, excluding contradictory or malformed acceptance. */
export function isSteerAccepted(value: unknown): boolean {
	const receipt = workerRecord(value);
	return (
		receipt !== undefined &&
		receipt.ok !== false &&
		(receipt.accepted === undefined || receipt.accepted === true) &&
		(receipt.status === undefined || receipt.status === "accepted") &&
		(receipt.accepted === true || receipt.status === "accepted")
	);
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

/** The command line of a live pid, or undefined when it is gone. `ps` is the portable read on macOS and Linux. */
async function processCommand(pid: number): Promise<string | undefined> {
	try {
		const proc = Bun.spawn(["ps", "-o", "command=", "-p", String(pid)], { stdout: "pipe", stderr: "ignore" });
		const out = (await new Response(proc.stdout).text()).trim();
		await proc.exited;
		return out.length > 0 ? out : undefined;
	} catch {
		return undefined;
	}
}

/**
 * The SDK router's own verdict that it serves no host for the session. Broker
 * transport failures (`broker_unavailable`, timeouts, unparsable output) carry
 * no envelope code and never match.
 */
export function isSessionUnavailable(error: unknown): boolean {
	return sdkErrorCode(error) === "session_unavailable";
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
	if (error instanceof BrokerAuthorityError) return error;
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

/** Exact `busy` envelope code: the runtime refused the prompt because a turn is still running. */
export function isSessionBusy(error: unknown): boolean {
	return error instanceof GjcCliError && envelopeErrorCode(error.details) === SESSION_BUSY_CODE;
}

/** A refused relay control/query as the same GjcCliError the CLI transport raised, so callers classify once. */
function relayFailure(operation: string, response: RelayResponse): GjcCliError {
	const details = response.error ?? {};
	return new GjcCliError(`gjc sdk ${operation} reported failure: ${JSON.stringify(details)}`, 0, "", details);
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
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
