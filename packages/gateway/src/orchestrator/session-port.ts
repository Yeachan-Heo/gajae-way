import { createHash, randomUUID } from "node:crypto";
import { stat } from "node:fs/promises";
import { containsSilenceToken, isSilenceToken } from "@gajae-gateway/protocol";
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
	type PromptOutcomeBody,
	type PromptStatusBody,
	parseEnvelope,
	parseStatusReport,
	projectOpState,
	type SendReceipt,
	type StatusReport,
	TranscriptIncompleteError,
} from "@gajae-gateway/subsession";
import { DEFAULT_SESSIONS_BUSY_STALL_MS, type GjcModelSelection, type GjcServiceTier } from "../config";
import { BrokerAuthorityError, type Store } from "../store";
import {
	BROKER_PROMPT_BUSY_REFUSAL,
	type BrokerAuthority,
	type BrokerOperationKind,
	type BrokerOperationRecord,
	isBrokerPromptBusyRefusal,
} from "../store/groups/bindings";
import { GjcCliUnavailableError, isBrokerUnavailable } from "./broker";
import {
	type FailedTransportCause,
	type FailedTurnEvidence,
	type FailedTurnEvidenceInput,
	locateSavedTranscript,
	readFailedTransportCause,
	readFailedTurnEvidence,
} from "./failed-turn-evidence";
import { isSessionGoneCode } from "./gjc-contract";
import { isRebindableCode, sanitizeDiagnostic } from "./rebind";
import {
	isRelayTransportFailure,
	isUnwrittenRelayRequest,
	type RelayResponse,
	type TailAttachInput,
	type TailHandle,
	type TailRunner,
} from "./tail-runner";

const THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

/** `anthropic/claude-opus-5-5:xhigh` → model id + thinking level; a string without a known level suffix is returned whole. */
export function splitThinkingSuffix(selection: string): { readonly model: string; readonly thinking?: string } {
	const at = selection.lastIndexOf(":");
	if (at <= 0) return { model: selection };
	const level = selection.slice(at + 1);
	if (!THINKING_LEVELS.has(level)) return { model: selection };
	return { model: selection.slice(0, at), thinking: level };
}

/**
 * Generic broker-backed session surface. Callers own prompt composition,
 * operation-reference selection, and recovery policy; this port only binds,
 * sends, observes, and reads terminal output through the SDK CLI.
 */
export type TerminateHostOutcome =
	| { readonly outcome: "terminated"; readonly pid: number }
	/** SIGTERM/SIGKILL was accepted, but the inspected process identity remains live or unverifiable. */
	| { readonly outcome: "terminating"; readonly pid: number }
	/** Positive process evidence that the host is gone; broker session absence alone is `refused`. */
	| { readonly outcome: "already_gone" }
	| { readonly outcome: "not_a_host"; readonly pid: number; readonly command: string }
	| { readonly outcome: "refused"; readonly reason: string };

/**
 * What the session host is doing, as `gjc sdk session inspect` reports it:
 * `state` ("idle", or whatever the host is busy with) since `at` (epoch ms).
 */
export interface SessionActivity {
	readonly state: string;
	readonly at: number;
}

/** Raw broker liveness; `activity` is present only when inspect reported a well-formed one. */
export interface SessionLiveness {
	readonly live: boolean | undefined;
	readonly disowned: boolean;
	readonly activity?: SessionActivity;
	/** The broker still holds the saved session but marks it deleted. */
	readonly deleted?: boolean;
	/** The broker's canonical `locator.cwd`. */
	readonly workspace?: string;
	/** The (last) host process identity; a closed session keeps its dead host's. */
	readonly hostIncarnation?: string;
}

/**
 * The parts of a reply the delivery path would post: split on `[BREAK]`,
 * `[REPLY:id]` markers stripped, and every part that is or carries a silence
 * token dropped whole.
 */
export function spokenParts(text: string): string[] {
	return text
		.split(/\n\s*\[BREAK\]\s*\n?/)
		.map((part) => part.trim())
		.filter((part) => part.length > 0 && !isSilenceToken(part) && !containsSilenceToken(part))
		.map((part) => part.replace(/\[REPLY:[^\]\s]+\]/g, "").trim())
		.filter((part) => part.length > 0);
}

/** True when nothing in `text` would be posted. */
export function isSilentReply(text: string): boolean {
	return spokenParts(text).length === 0;
}

export interface SessionPort {
	/** Positive local non-admission evidence, not native execution-end proof. */
	promptRefusal?(input: { sessionId: string; repo: string; opRef: string }): "busy" | undefined;
	bind(input: SessionBindInput): Promise<SessionBinding>;
	inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined>;
	liveness?(input: { sessionId: string; repo: string }): Promise<SessionLiveness>;
	/** True when the session's prompt queue has no pending messages (queue.messages.list empty). */
	queueEmpty?(input: { sessionId: string; repo: string; relay?: TailHandle }): Promise<boolean>;
	/** Live execution/queued-turn state from context.get; undefined is not idle proof. */
	turnRunning?(input: { sessionId: string; repo: string; relay?: TailHandle }): Promise<boolean | undefined>;
	/**
	 * Background jobs (async `task` sub-lanes, async bash) still running inside the
	 * session host (`runtime.jobs.list`). Ending the host ends them, so a caller
	 * about to do that reads this first. Throws when the host cannot answer.
	 */
	runningJobs?(input: { sessionId: string; repo: string }): Promise<readonly RunningHostJob[]>;
	/**
	 * Context-window usage of the session (`context.get`), read on the turn's
	 * owned relay. Undefined when the host does not report it (gjc reports none
	 * between a compaction and the next model response).
	 */
	contextUsage?(input: {
		sessionId: string;
		repo: string;
		relay: TailHandle;
	}): Promise<SessionContextUsage | undefined>;
	/**
	 * Ends a gateway-owned session host; the optional final guard revalidates
	 * operation status before signaling. If it cancels SIGKILL after SIGTERM,
	 * a later unguarded reconciliation may escalate after a grace period so a
	 * host that ignores SIGTERM cannot hold capacity indefinitely. A process is
	 * reported terminated only after its inspected identity is confirmed absent.
	 */
	terminateHost?(input: {
		sessionId: string;
		repo: string;
		shouldTerminate?: () => boolean | Promise<boolean>;
	}): Promise<TerminateHostOutcome>;
	/** Recognized current-session provider failure, never authorization to replay an operation. */
	failedTurnEvidence?(input: FailedTurnEvidenceInput): Promise<FailedTurnEvidence | undefined>;
	/** Transport failure cause from the same session transcript. */
	failedTransportCause?(input: FailedTurnEvidenceInput): Promise<FailedTransportCause | undefined>;
	/** Restores a saved, non-deleted session through `session.resume`; it never creates a replacement. */
	resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding>;
	send(input: SessionSendInput): Promise<SendReceipt>;
	steer(input: SessionSteerInput): Promise<void>;
	setModel(input: {
		sessionId: string;
		repo: string;
		selection: GjcModelSelection;
		relay?: TailHandle;
	}): Promise<{ readonly changed: boolean }>;
	setServiceTier(input: {
		sessionId: string;
		repo: string;
		tier: GjcServiceTier;
		relay?: TailHandle;
	}): Promise<{ readonly changed: boolean }>;
	status(input: {
		sessionId: string;
		repo: string;
		opRef: string;
		relay?: TailHandle;
		priority?: "interactive" | "background";
	}): Promise<StatusReport>;
	/** Exact invocation-owned original output; never falls back to a latest-assistant heuristic. */
	fetchWorkerOutput(input: WorkerOutputInput): Promise<WorkerOutputResult>;
	fetchLastAssistant(input: { sessionId: string; repo: string; relay?: TailHandle }): Promise<LastAssistantResult>;
	/**
	 * Last assistant row NOT older than `notBeforeMs`. Callers pass the op's
	 * reported startedAt when present, otherwise the turn's `dispatched_at`
	 * (stamped at bind, before the send, and cleared on requeue). Turn-scoped
	 * by wall clock, independent of gjc ring
	 * coordinates (gajae-code#5200); undefined when the newest row predates
	 * the turn. With `preferSpoken`, the last row that is not a silent reply
	 * wins over a later silent one (a trailing `[SILENT]` after a real answer);
	 * a silent row is returned only when no spoken row qualifies.
	 */
	fetchAssistantSince?(input: {
		sessionId: string;
		repo: string;
		notBeforeMs: number;
		relay?: TailHandle;
		preferSpoken?: boolean;
	}): Promise<LastAssistantResult | undefined>;
	attachTail(input: TailAttachInput): Promise<TailHandle>;
	runCompaction(input: SessionCompactionInput): Promise<{ readonly status: SessionCompactionStatus }>;
	/** Emits live-tail stall signals; the owning actor applies bounded recovery policy. */
	checkStalls(now?: number): void;
	/** Sets the tail-silence threshold that triggers stall handling. */
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

export interface RunningHostJob {
	readonly id: string;
	readonly type: string;
	readonly label: string;
}

export type SessionCompactionStatus = "succeeded" | "failed" | "skipped" | "unavailable";

/** Session context usage: percent of the model's context window and the token count it stands for. */
export interface SessionContextUsage {
	readonly percent: number;
	readonly tokens: number;
}

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
	 * How long a `busy` host may show no progress (activity stamp and transcript
	 * unchanged) before the wait gives up with `SessionBusyStalledError` (#687).
	 */
	readonly busyStallMs?: number;
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
	/** The session's live relay; the read goes over it instead of a CLI spawn. */
	readonly relay?: TailHandle;
}

/**
 * One session-scoped SDK request (`raw control|query`). Answers in the CLI's
 * printed shape so a relay answer and a CLI answer feed the same parsers.
 * `input: undefined` keeps the CLI argv without `--json-input`.
 */
type SdkTransport = (
	kind: "control" | "query",
	name: string,
	input: Record<string, unknown> | undefined,
	options?: { readonly timeoutMs?: number; readonly cursor?: string },
) => Promise<CliResult>;

export type WorkerOutputResult =
	| {
			readonly status: "proven";
			readonly text: string;
			readonly observedAtMs: number;
			/** Correlated turn.result terminal facts, not an inferred success verdict. */
			readonly terminal: PromptStatusBody;
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

export const SESSION_REQUEST_STEPS = ["authority", "attach", "send", "status", "last_assistant", "close"] as const;
export type SessionRequestStep = (typeof SESSION_REQUEST_STEPS)[number];

const SESSION_REQUEST_ERROR_STEPS = new WeakMap<object, SessionRequestStep>();

class SessionRequestStepError extends Error {
	constructor(step: SessionRequestStep, cause: unknown) {
		super(`session request failed during ${step}`, { cause });
		this.name = "SessionRequestStepError";
	}
}

function markSessionRequestStep(error: unknown, step: SessionRequestStep): unknown {
	if ((typeof error === "object" && error !== null) || typeof error === "function") {
		if (!SESSION_REQUEST_ERROR_STEPS.has(error)) SESSION_REQUEST_ERROR_STEPS.set(error, step);
		return error;
	}
	const wrapped = new SessionRequestStepError(step, error);
	SESSION_REQUEST_ERROR_STEPS.set(wrapped, step);
	return wrapped;
}

/** Returns the request stage associated with an error without mutating it. */
export function sessionRequestStep(error: unknown): SessionRequestStep | undefined {
	if ((typeof error !== "object" || error === null) && typeof error !== "function") return undefined;
	return SESSION_REQUEST_ERROR_STEPS.get(error);
}

export type LastAssistantUnreadableCode =
	| "last_assistant_not_json"
	| "last_assistant_no_page"
	| "last_assistant_no_items"
	| "last_assistant_null_item"
	| "last_assistant_non_text_item";

/** A protocol-safe diagnosis for an invalid `session.last_assistant` page. */
export class LastAssistantUnreadableError extends Error {
	readonly code: LastAssistantUnreadableCode;

	constructor(code: LastAssistantUnreadableCode) {
		super(`session.last_assistant response is unreadable (${code})`);
		this.name = "LastAssistantUnreadableError";
		this.code = code;
	}
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
		const detail =
			status.status.error?.message ??
			status.status.error?.code ??
			status.status.outcome?.reason ??
			status.status.status;
		super(`session operation ${status.operationRef} ended ${status.status.status}: ${detail}`);
		this.name = "SessionTerminalError";
		this.status = status;
	}
}

export const MODEL_NOT_SELECTED_CODE = "model_not_selected";

/** Authenticated `turn.prompt` refusal: no model is selected and no turn was accepted. */
export class ModelNotSelectedError extends Error {
	readonly opRef: string;
	readonly code = MODEL_NOT_SELECTED_CODE;

	constructor(opRef: string) {
		super("turn.prompt explicitly refused: model_not_selected");
		this.name = "ModelNotSelectedError";
		this.opRef = opRef;
	}
}

/**
 * The pre-send busy wait saw the host report `busy` with no progress evidence
 * (activity stamp and transcript size/mtime unchanged) for `silentMs`. The
 * busy refusal is already durable, so the prompt was provably never accepted.
 */
export class SessionBusyStalledError extends Error {
	readonly sessionId: string;
	readonly opRef: string;
	readonly silentMs: number;
	readonly evidence: string;

	constructor(sessionId: string, opRef: string, silentMs: number, evidence: string) {
		super(`session ${sessionId} stayed busy without progress for ${silentMs}ms`);
		this.name = "SessionBusyStalledError";
		this.sessionId = sessionId;
		this.opRef = opRef;
		this.silentMs = silentMs;
		this.evidence = evidence;
	}
}

export interface BrokerSessionPortOptions {
	readonly database: Store;
	readonly cli: CliRunner;
	readonly tailRunner: TailRunner;
	readonly authority: BrokerAuthority;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_REQUEST_WAIT_MS = 30 * 60_000;
const DEFAULT_STATUS_POLL_MS = 500;
const SESSION_CREATE_READINESS_MS = 60_000;
const SESSION_READY_TIMEOUT_MS = 60_000;
const SESSION_READY_POLL_MS = 250;
/**
 * How long a completed broker operation answers a retry of the same op ref.
 * Keys are per-attempt, so this bounds the journal's size, not its correctness:
 * no other operation can ever read the row.
 */
const BROKER_OPERATION_RETENTION_MS = 60 * 60_000;
/**
 * Pending close intents this old are pruned. Creates are retained until a
 * binding is recorded, and prompts are retained until their status is reconciled.
 */
const BROKER_INTENT_RETENTION_MS = 24 * 60 * 60_000;
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
/** Progress probes (one `session inspect` plus a transcript stat) run at most this often. */
const BUSY_PROGRESS_SAMPLE_MS = 30_000;
const CONTEXT_QUERY_TIMEOUT_MS = 5_000;
/**
 * `compaction.run` answers only after gjc finished compacting, which takes tens
 * of seconds at a few hundred thousand tokens (measured 27-45s at 270-295K,
 * 2026-09-30); the default 30s command bound reported such runs as failed.
 */
const COMPACTION_TIMEOUT_MS = 5 * 60_000;

type SignaledHost = {
	readonly pid: number;
	readonly identity: string;
	readonly command: string;
	readonly signaledAtMs: number;
	escalationAfterMs?: number;
	killSentAtMs?: number;
};

/**
 * Production SessionPort implementation. The broker-bound CliRunner is the sole
 * transport: it neither discovers an endpoint nor opens an authenticated socket.
 */
export class BrokerSessionPort implements SessionPort {
	readonly #database: Store;
	readonly #cli: CliRunner;
	readonly #tailRunner: TailRunner;
	readonly #now: () => number;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly #chains = new Map<string, Promise<void>>();
	readonly #bindings = new Map<string, Promise<SessionBinding>>();
	readonly #signaledHosts = new Map<string, SignaledHost>();
	readonly #authority: BrokerAuthority;

	constructor(options: BrokerSessionPortOptions) {
		this.#database = options.database;
		this.#authority = { ...options.authority };
		this.#database.bindings.assertAuthority(this.#authority);
		this.#cli = async (args, commandOptions) => {
			this.#database.bindings.assertAuthority(this.#authority);
			const result = await options.cli(args, commandOptions);
			this.#database.bindings.assertAuthority(this.#authority);
			// Steering requires an unambiguous control receipt: do not promote a
			// failed transport to a definitive rejection merely because stdout is JSON.
			return args.includes("turn.steer") ? result : normalizeSdkEnvelopeFailure(result);
		};
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

	bind(input: SessionBindInput): Promise<SessionBinding> {
		const key = `${input.originKey}#${input.epoch}`;
		const existing = this.#bindings.get(key);
		if (existing) return existing;
		const pending = this.#bind(input).finally(() => {
			if (this.#bindings.get(key) === pending) this.#bindings.delete(key);
		});
		this.#bindings.set(key, pending);
		return pending;
	}

	async #bind(input: SessionBindInput): Promise<SessionBinding> {
		this.#database.bindings.assertAuthority(this.#authority);
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("session epoch must be a non-negative integer");
		const existing = this.#database.bindings.getSessionRecord(input.originKey);
		if (existing?.epoch === input.epoch && existing.sessionId) {
			const pendingCreate = this.#database.bindings.brokerOperation(
				this.#authority,
				"session.create",
				createScope(input),
			);
			if (pendingCreate?.state === "pending") {
				let createdRepo: unknown;
				try {
					createdRepo = recordOf(JSON.parse(pendingCreate.requestJson))?.cwd;
				} catch {
					// The stored create request is internal journal state; malformed
					// evidence cannot authorize touching this persisted session.
				}
				if (typeof createdRepo !== "string")
					throw new Error("pending session.create intent does not contain a valid workspace");
				this.#assertOwned({ sessionId: existing.sessionId, repo: createdRepo });
				if (pendingCreate.requestJson !== sessionCreateInput(input.repo, input.model)) {
					assertControlAllowed("session.close", { operatorApproval: true });
					try {
						await this.#closeSessionOperation(existing.sessionId);
					} catch (error) {
						if (!isSessionGoneCode(sdkErrorCode(error))) throw error;
					}
					this.#releaseOperation(pendingCreate);
					const nextEpoch = this.#database.transaction((tx) =>
						this.#database.bindings.rotateEpoch(tx, input.originKey),
					);
					return await this.bind({ ...input, epoch: nextEpoch });
				}
				// The binding was durably recorded from this create, but Router
				// indexing (or startup model configuration) did not finish. Keep
				// waiting on this known cold session instead of treating an
				// unindexed ID as a dead reusable binding and rotating the epoch.
				await this.#awaitIndexed(existing.sessionId, input.repo);
				if (typeof input.model === "string") {
					const { thinking } = splitThinkingSuffix(input.model);
					if (thinking) await this.#setThinking(this.#cliTransport(existing.sessionId), thinking);
				}
				this.#releaseOperation(pendingCreate);
				this.#resetCreateRotations(input.originKey);
				return {
					sessionId: existing.sessionId,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					...(input.model ? { startupModelApplied: true } : {}),
				};
			}
			this.#assertOwned({ sessionId: existing.sessionId, repo: input.repo });
			// Only owned bindings may be inspected, resumed, or replaced.
			let indexed = true;
			try {
				// Judge the raw envelope: a persisted id is reusable if live, and
				// resumable if it still has saved authority. Monitor authoring reaches
				// SessionPort directly, so resume here before paying for a cold
				// replacement session.
				const result = await this.#cli(["sdk", "session", "inspect", existing.sessionId]);
				const envelope = JSON.parse(result.stdout) as {
					ok?: unknown;
					result?: { session?: { live?: unknown; deleted?: unknown } };
					error?: { code?: unknown; message?: unknown };
				};
				if (envelope.ok === false && envelope.error?.code === "broker_unavailable")
					throw new GjcCliUnavailableError(
						sanitizeDiagnostic(typeof envelope.error.message === "string" ? envelope.error.message : "") ||
							"broker unavailable",
					);
				if (envelope.ok === false) indexed = !isSessionGoneCode(envelope.error?.code);
				// A previous gateway may have resumed this host and crashed before
				// invalidating the prior incarnation's close intent.
				if (envelope.ok === true && envelope.result?.session?.live === true)
					this.#releaseCloseOperation(existing.sessionId);
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
							// A broker that went away mid-resume says nothing about the
							// session: the caller waits for it rather than replacing a
							// resumable session with a cold one.
							if (error instanceof BrokerAuthorityError || isBrokerUnavailable(error)) throw error;
							// Saved authority cannot be resumed: replace it below.
							console.error(
								`session_resume_failed origin=${input.originKey} epoch=${input.epoch} session=${existing.sessionId} path=bind detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error))}`,
							);
						}
					}
					indexed = false;
				}
			} catch (error) {
				// An unavailable broker cannot vouch for the persisted session; reusing
				// it would only move the failure to the first send, where it is no
				// longer retryable. Surface it so the caller can wait for readiness.
				if (error instanceof BrokerAuthorityError || isBrokerUnavailable(error)) throw error;
				// Any other inspect failure (unparsable output, timeout) keeps the binding.
				indexed = true;
			}
			if (indexed) {
				this.#resetCreateRotations(input.originKey);
				return { sessionId: existing.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
			}
			const rebound = this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, input.originKey));
			console.info(
				`session_rebound origin=${input.originKey} epoch=${input.epoch} nextEpoch=${rebound} session=${existing.sessionId} reason=not_live_or_disowned_by_broker`,
			);
			return await this.bind({ ...input, epoch: rebound });
		}
		// Intent first: the create is journaled under a fresh per-attempt key
		// before the broker sees it. A crash or broker restart before readiness
		// completes leaves the intent pending; retries replay an ambiguous create
		// or wait on its durably recorded binding. Release it only after the
		// binding is ready, so a later bind never inherits an unfinished create.
		const requestJson = sessionCreateInput(input.repo, input.model);
		let intent = this.#beginOperation(
			"session.create",
			createScope(input),
			`gw-bind-${randomUUID()}`,
			() => requestJson,
		);
		if (intent.requestJson !== requestJson) {
			// Resolve and retire the old create before replacing it. Its request may
			// already have created a session under the old repo/model; reusing the
			// old result or minting a second create without closing it would bind the
			// wrong session or orphan the old one.
			let previousSessionId: string | undefined;
			try {
				const previous = await this.#createSession(intent.operationRef, intent.requestJson);
				if (typeof previous.sessionId !== "string" || previous.sessionId.length === 0)
					throw new Error("pending session.create intent did not return a session id");
				previousSessionId = previous.sessionId;
			} catch (error) {
				// A rejected model or definitively poisoned key cannot have created a
				// session; retire it so the current request can start with its input.
				if (
					unknownModelCreateError(error) === undefined &&
					poisonedCreateKeyCode(error, intent.operationRef) === undefined
				)
					throw error;
				this.#releaseOperation(intent);
			}
			if (previousSessionId) {
				assertControlAllowed("session.close", { operatorApproval: true });
				// The pending create row is authority for this just-created session;
				// it is not yet in the durable bindings table for public close().
				try {
					await this.#closeSessionOperation(previousSessionId);
				} catch (error) {
					// A completed close result may have aged out while the create intent
					// remained pending. A broker-confirmed missing session is already retired.
					if (!isSessionGoneCode(sdkErrorCode(error))) throw error;
				}
				this.#releaseOperation(intent);
			}
			intent = this.#beginOperation("session.create", createScope(input), `gw-bind-${randomUUID()}`, () => requestJson);
			if (intent.requestJson !== requestJson) {
				throw new Error(
					"pending session.create intent changed during replacement; refusing to replay a different request",
				);
			}
		}
		let created: { readonly sessionId?: unknown };
		try {
			created = await this.#createSession(intent.operationRef, intent.requestJson);
		} catch (error) {
			if (error instanceof BrokerAuthorityError) throw error;
			const modelError = unknownModelCreateError(error);
			const poisonedCode = modelError === undefined ? poisonedCreateKeyCode(error, intent.operationRef) : undefined;
			// Neither a rejected model nor a poisoned key can create a session. All
			// other failures may have applied the operation, so keep their key for replay.
			if (modelError !== undefined || poisonedCode !== undefined) this.#releaseOperation(intent);
			if (modelError !== undefined) throw modelError;
			if (input.epochRecovery === false || poisonedCode === undefined) throw error;
			const rotations = this.#createRotations(input.originKey);
			if (rotations >= MAX_POISONED_CREATE_ROTATIONS) {
				console.error(
					`session_create_rotation_capped origin=${input.originKey} rotations=${rotations} reason=poisoned_create_key_capped`,
				);
				throw error;
			}
			this.#database.core.metaSet(createRotationMetaKey(input.originKey), String(rotations + 1));
			const nextEpoch = this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, input.originKey));
			console.info(
				`session_create_epoch_rotated origin=${input.originKey} epoch=${input.epoch} nextEpoch=${nextEpoch} reason=poisoned_create_key code=${poisonedCode}`,
			);
			return await this.bind({ ...input, epoch: nextEpoch, epochRecovery: false });
		}
		const sessionId = created.sessionId;
		if (typeof sessionId !== "string" || sessionId.length === 0) {
			// The create may have succeeded even though its result was malformed;
			// keep the pending key so a retry reconciles instead of creating again.
			throw new Error("session.create succeeded without a sessionId");
		}
		const persistedEpoch = this.#database.bindings.getSessionRecord(input.originKey)?.epoch;
		if (persistedEpoch !== undefined && persistedEpoch > input.epoch) {
			this.#releaseOperation(intent);
			throw new Error(`session bind for ${input.originKey} epoch ${input.epoch} lost to epoch ${persistedEpoch}`);
		}
		// Persist ownership before waiting for Router indexing, but retain the
		// pending create intent until readiness and startup configuration finish.
		// A retry can then distinguish this cold binding from a dead old session.
		const recorded = this.#database.transaction((tx) =>
			this.#database.bindings.recordOwnedBinding(tx, {
				authority: this.#authority,
				sessionId,
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
			}),
		);
		if (!recorded) {
			throw new Error(
				`session bind for ${input.originKey} epoch ${input.epoch} lost to a concurrent durable epoch change`,
			);
		}
		// session.create returns once the host is admitted; the Router indexes it
		// a moment later. A tail/send before that answers session_unavailable, so
		// wait until the broker reports the id live before handing the binding out.
		await this.#awaitIndexed(sessionId, input.repo);
		// Apply a model selector's thinking level while the newly-created host is
		// ready and before the binding escapes; a later turn can hit endpoint_stale.
		if (typeof input.model === "string") {
			const { thinking } = splitThinkingSuffix(input.model);
			if (thinking) await this.#setThinking(this.#cliTransport(sessionId), thinking);
		}
		this.#releaseOperation(intent);
		this.#resetCreateRotations(input.originKey);
		return {
			sessionId,
			originKey: input.originKey,
			epoch: input.epoch,
			repo: input.repo,
			...(input.model ? { startupModelApplied: true } : {}),
		};
	}

	/** Records the intent of a broker mutation (its own committed transaction) or returns the row an earlier attempt left. */
	#beginOperation(
		operation: BrokerOperationKind,
		scope: string,
		operationRef: string,
		request: () => string,
	): BrokerOperationRecord {
		const now = this.#now();
		const cutoff = {
			completedBefore: new Date(now - BROKER_OPERATION_RETENTION_MS).toISOString(),
			pendingBefore: new Date(now - BROKER_INTENT_RETENTION_MS).toISOString(),
		};
		return this.#database.transaction((tx) => {
			this.#database.bindings.pruneBrokerOperations(tx, cutoff);
			return this.#database.bindings.beginBrokerOperation(tx, this.#authority, {
				operation,
				scope,
				operationRef,
				requestJson: request(),
			});
		});
	}

	#completeOperation(intent: BrokerOperationRecord, result: unknown): void {
		this.#database.transaction((tx) =>
			this.#database.bindings.completeBrokerOperation(
				tx,
				this.#authority,
				intent.operation,
				intent.scope,
				intent.operationRef,
				JSON.stringify(result),
			),
		);
	}

	#releaseOperation(intent: BrokerOperationRecord): void {
		this.#database.transaction((tx) =>
			this.#database.bindings.releaseBrokerOperation(
				tx,
				this.#authority,
				intent.operation,
				intent.scope,
				intent.operationRef,
			),
		);
	}
	/** Concurrent status/ACK paths must return the first persisted acceptance. */
	#completePromptOperation(intent: BrokerOperationRecord, receipt: SendReceipt): SendReceipt {
		return this.#database.transaction((tx) => {
			this.#database.bindings.assertAuthority(this.#authority);
			const current = this.#database.bindings.brokerOperation(this.#authority, "turn.prompt", intent.scope);
			if (!current || current.operationRef !== intent.operationRef || current.requestJson !== intent.requestJson)
				throw new OpRefError("prompt journal identity changed before completion");
			if (current.state === "complete") {
				const cached = recordOf(JSON.parse(current.resultJson!));
				const nativeIdentityConflict =
					(cached?.commandId !== undefined &&
						receipt.commandId !== undefined &&
						cached.commandId !== receipt.commandId) ||
					(cached?.turnId !== undefined && receipt.turnId !== undefined && cached.turnId !== receipt.turnId);
				if (
					cached?.sessionId === receipt.sessionId &&
					typeof cached.sessionId === "string" &&
					cached.sessionId.length > 0 &&
					cached.operationRef === intent.operationRef &&
					receipt.operationRef === intent.operationRef &&
					typeof cached.acceptedAt === "string" &&
					cached.taskKey === "gateway" &&
					!nativeIdentityConflict
				)
					return cached as unknown as SendReceipt;
				throw new OpRefError("prompt accepted receipt identity changed");
			}
			this.#database.bindings.completeBrokerOperation(
				tx,
				this.#authority,
				"turn.prompt",
				intent.scope,
				intent.operationRef,
				JSON.stringify(receipt),
			);
			return receipt;
		});
	}

	#releaseCloseOperation(sessionId: string): void {
		const previous = this.#database.bindings.brokerOperation(this.#authority, "session.close", sessionId);
		if (previous) this.#releaseOperation(previous);
	}

	#createRotations(originKey: string): number {
		const raw = this.#database.core.metaGet(createRotationMetaKey(originKey));
		const value = raw === undefined ? Number.NaN : Number(raw);
		return Number.isSafeInteger(value) && value >= 0 ? value : 0;
	}

	#resetCreateRotations(originKey: string): void {
		this.#database.core.metaSet(createRotationMetaKey(originKey), "0");
	}

	#createChain: Promise<unknown> = Promise.resolve();

	/** Wait for the broker to index a created session; broker outages must reach the caller's readiness retry. */
	async #awaitIndexed(sessionId: string, repo: string): Promise<void> {
		this.#assertOwned({ sessionId, repo });
		const deadline = Date.now() + SESSION_READY_TIMEOUT_MS;
		let lastCode: string | undefined;
		for (;;) {
			try {
				const state = await this.#readLiveness({ sessionId, repo });
				if (!state.disowned && state.live !== false) return;
				lastCode = state.disowned ? "session_unavailable" : "not_live";
			} catch (error) {
				if (error instanceof BrokerAuthorityError || isBrokerUnavailable(error)) throw error;
				const code = sdkErrorCode(error);
				if (!isSessionGoneCode(code)) return;
				lastCode = code;
			}
			if (Date.now() >= deadline)
				throw new Error(`session ${sessionId} was created but never became live (${lastCode})`);
			await this.#sleep(SESSION_READY_POLL_MS);
		}
	}

	/** Cold creates are serialized per agent dir: parallel launches starve gjc's lifecycle launcher. */
	#createSession(idempotencyKey: string, requestJson: string): Promise<{ readonly sessionId?: unknown }> {
		const run = this.#createChain.then(async () => await this.#createSessionUnserialized(idempotencyKey, requestJson));
		this.#createChain = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
	}

	async #createSessionUnserialized(
		idempotencyKey: string,
		requestJson: string,
	): Promise<{ readonly sessionId?: unknown }> {
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
					requestJson,
				]),
				"session.create",
			);
		} catch (error) {
			throw sanitizeSdkFailure(error);
		}
	}

	/**
	 * Raw liveness judged on the broker envelope. `disowned` means the broker
	 * rejects the id, rather than merely reporting a host that is not live.
	 * `activity` is the host's own `{state, at}` when inspect reports one.
	 * External probes keep transient failures unknown; resume/readiness use the
	 * strict helper so broker outages remain retryable.
	 */
	async liveness(input: { sessionId: string; repo: string }): Promise<SessionLiveness> {
		try {
			return await this.#readLiveness(input);
		} catch (error) {
			if (error instanceof BrokerAuthorityError) throw error;
			return { live: undefined, disowned: isSessionGoneCode(sdkErrorCode(error)) };
		}
	}

	/** Raw probe for recovery paths that must preserve broker-unavailable errors. */
	async #readLiveness(input: { sessionId: string; repo: string }): Promise<SessionLiveness> {
		this.#assertOwned(input);
		const result = await this.#cli(["sdk", "session", "inspect", input.sessionId], { timeoutMs: 10_000 });
		const envelope = JSON.parse(result.stdout) as {
			ok?: unknown;
			result?: {
				session?: {
					live?: unknown;
					deleted?: unknown;
					locator?: { cwd?: unknown };
					activity?: { state?: unknown; at?: unknown };
					hostIncarnation?: unknown;
				};
			};
			error?: { code?: unknown; message?: unknown };
		};
		if (envelope.ok === false) {
			if (envelope.error?.code === "broker_unavailable")
				throw new GjcCliUnavailableError(
					sanitizeDiagnostic(typeof envelope.error.message === "string" ? envelope.error.message : "") ||
						"broker unavailable",
				);
			return { live: undefined, disowned: isSessionGoneCode(envelope.error?.code) };
		}
		if (result.exitCode !== 0 || envelope.ok !== true)
			throw new GjcCliUnavailableError("session inspect did not provide a successful envelope");
		const session = envelope.result?.session;
		const live = session?.live;
		const state = session?.activity?.state;
		const at = session?.activity?.at;
		const workspace = session?.locator?.cwd;
		const host = session?.hostIncarnation;
		return {
			live: typeof live === "boolean" ? live : undefined,
			disowned: false,
			...(session?.deleted === true ? { deleted: true } : {}),
			...(typeof workspace === "string" ? { workspace } : {}),
			...(typeof host === "string" && /^[A-Za-z0-9:._-]{1,128}$/.test(host) ? { hostIncarnation: host } : {}),
			...(typeof state === "string" && typeof at === "number" && Number.isFinite(at) && at > 0
				? { activity: { state, at } }
				: {}),
		};
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
	async terminateHost(input: {
		sessionId: string;
		repo: string;
		shouldTerminate?: () => boolean | Promise<boolean>;
	}): Promise<TerminateHostOutcome> {
		this.#assertOwned(input);
		const finalGuardRefusal = async () => {
			if (!input.shouldTerminate) return undefined;
			try {
				return (await input.shouldTerminate())
					? undefined
					: ({ outcome: "refused", reason: "termination_guard_rejected" } as const);
			} catch (error) {
				return { outcome: "refused", reason: `termination_guard_failed:${sanitizeDiagnostic(String(error))}` } as const;
			}
		};
		const signaledHost = this.#signaledHosts.get(input.sessionId);
		if (signaledHost) return await this.#checkSignaledHost(input, signaledHost, finalGuardRefusal);
		let pid: number | undefined;
		let live: boolean | undefined;
		try {
			const result = await this.#cli(["sdk", "session", "inspect", input.sessionId], {
				timeoutMs: 10_000,
			});
			const envelope = JSON.parse(result.stdout) as {
				ok?: unknown;
				result?: { session?: { pid?: unknown; live?: unknown } };
				error?: { code?: unknown; message?: unknown };
			};
			if (envelope.ok !== true) {
				const code = stableErrorCode(envelope.error?.code);
				return isSessionGoneCode(code)
					? { outcome: "refused", reason: "inspect_failed:session_gone_without_process_identity" }
					: { outcome: "refused", reason: `inspect_failed:${code ?? "invalid_envelope"}` };
			}
			const session = envelope.result?.session;
			pid =
				typeof session?.pid === "number" && Number.isSafeInteger(session.pid) && session.pid > 1
					? session.pid
					: undefined;
			live = typeof session?.live === "boolean" ? session.live : undefined;
		} catch (error) {
			if (error instanceof BrokerAuthorityError) throw error;
			return isSessionGoneCode(sdkErrorCode(error))
				? { outcome: "refused", reason: "inspect_failed:session_gone_without_process_identity" }
				: { outcome: "refused", reason: `inspect_failed:${sanitizeDiagnostic(String(error))}` };
		}
		if (live === false) {
			if (pid === undefined) return { outcome: "refused", reason: "inspect_incomplete:missing_host_pid" };
			const deadCheck = await processCommand(pid);
			if (deadCheck.outcome === "gone") return { outcome: "already_gone" };
			if (deadCheck.outcome === "unreadable")
				return { outcome: "refused", reason: `process_inspection_failed:${deadCheck.reason}` };
			return { outcome: "refused", reason: "session_not_live_but_process_live" };
		}
		if (pid === undefined || live !== true)
			return { outcome: "refused", reason: "inspect_incomplete:missing_live_host_identity" };
		const inspection = await processCommand(pid);
		if (inspection.outcome === "gone") return { outcome: "already_gone" };
		if (inspection.outcome === "unreadable")
			return { outcome: "refused", reason: `process_inspection_failed:${inspection.reason}` };
		const command = inspection.command;
		// The only process shape this gateway may end. The broker, daemon, and anything else are someone else's.
		if (!/\bsdk session-host-internal\b/.test(command) || /\b(broker|daemon)-internal\b/.test(command))
			return { outcome: "not_a_host", pid, command: command.slice(0, 160) };
		const refusal = await finalGuardRefusal();
		if (refusal) return refusal;
		const revalidated = await processCommand(pid);
		if (revalidated.outcome === "gone") return { outcome: "already_gone" };
		if (revalidated.outcome === "unreadable")
			return { outcome: "refused", reason: `process_inspection_failed:${revalidated.reason}` };
		if (
			revalidated.identity !== inspection.identity ||
			revalidated.context !== inspection.context ||
			revalidated.command !== command
		)
			return { outcome: "refused", reason: "process_identity_changed" };
		try {
			process.kill(pid, "SIGTERM");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") return { outcome: "already_gone" };
			return { outcome: "refused", reason: `kill_failed:${(error as NodeJS.ErrnoException).code ?? "unknown"}` };
		}
		const signaled = {
			pid,
			identity: revalidated.identity,
			command: revalidated.command,
			signaledAtMs: Date.now(),
		};
		this.#signaledHosts.set(input.sessionId, signaled);
		const gracefulExit = await waitForProcessExit(pid, revalidated, HOST_SIGTERM_GRACE_MS);
		if (gracefulExit === "exited") {
			this.#signaledHosts.delete(input.sessionId);
			return { outcome: "terminated", pid };
		}
		if (gracefulExit === "changed" || gracefulExit === "unreadable") return { outcome: "terminating", pid };

		const escalationRefusal = await finalGuardRefusal();
		if (escalationRefusal) {
			this.#signaledHosts.set(input.sessionId, {
				...signaled,
				escalationAfterMs: Date.now() + HOST_SIGTERM_ESCALATION_GRACE_MS,
			});
			return { outcome: "terminating", pid };
		}
		const beforeKill = await processCommand(pid);
		if (beforeKill.outcome === "gone") {
			this.#signaledHosts.delete(input.sessionId);
			return { outcome: "already_gone" };
		}
		if (beforeKill.outcome === "unreadable") return { outcome: "terminating", pid };
		if (
			beforeKill.identity !== inspection.identity ||
			beforeKill.context !== inspection.context ||
			beforeKill.command !== command
		)
			return { outcome: "terminating", pid };
		return await this.#signalKillAndConfirm(input.sessionId, signaled, beforeKill);
	}

	async #checkSignaledHost(
		input: { sessionId: string; shouldTerminate?: () => boolean | Promise<boolean> },
		expected: SignaledHost,
		finalGuardRefusal: () => Promise<{ readonly outcome: "refused"; readonly reason: string } | undefined>,
	): Promise<TerminateHostOutcome> {
		const inspection = await processCommand(expected.pid);
		if (inspection.outcome === "gone") {
			this.#signaledHosts.delete(input.sessionId);
			return { outcome: "already_gone" };
		}
		if (inspection.outcome === "unreadable")
			return { outcome: "refused", reason: `process_inspection_failed:${inspection.reason}` };
		if (inspection.identity !== expected.identity || inspection.command !== expected.command) {
			this.#signaledHosts.delete(input.sessionId);
			return { outcome: "already_gone" };
		}
		if (expected.killSentAtMs !== undefined)
			return await this.#signalKillAndConfirm(input.sessionId, expected, inspection);
		const now = Date.now();
		if (now - expected.signaledAtMs < HOST_SIGTERM_GRACE_MS) return { outcome: "terminating", pid: expected.pid };
		if (expected.escalationAfterMs !== undefined && now < expected.escalationAfterMs)
			return { outcome: "terminating", pid: expected.pid };
		if (expected.escalationAfterMs === undefined) {
			const refusal = await finalGuardRefusal();
			if (refusal) {
				this.#signaledHosts.set(input.sessionId, {
					...expected,
					escalationAfterMs: Date.now() + HOST_SIGTERM_ESCALATION_GRACE_MS,
				});
				return { outcome: "terminating", pid: expected.pid };
			}
		}
		return await this.#signalKillAndConfirm(input.sessionId, expected, inspection);
	}

	async #signalKillAndConfirm(
		sessionId: string,
		expected: SignaledHost,
		inspection: Extract<ProcessCommandInspection, { readonly outcome: "live" }>,
	): Promise<TerminateHostOutcome> {
		try {
			process.kill(expected.pid, "SIGKILL");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") {
				this.#signaledHosts.delete(sessionId);
				return { outcome: "already_gone" };
			}
			this.#signaledHosts.set(sessionId, {
				...expected,
				escalationAfterMs: Date.now() + HOST_SIGTERM_ESCALATION_GRACE_MS,
			});
			return { outcome: "terminating", pid: expected.pid };
		}
		const killSent = { ...expected, killSentAtMs: Date.now() };
		this.#signaledHosts.set(sessionId, killSent);
		const forcedExit = await waitForProcessExit(expected.pid, inspection, HOST_SIGKILL_GRACE_MS);
		if (forcedExit === "exited") {
			this.#signaledHosts.delete(sessionId);
			return { outcome: "terminated", pid: expected.pid };
		}
		return { outcome: "terminating", pid: expected.pid };
	}

	/**
	 * Judged on the raw envelope (`liveness`), like every other liveness read:
	 * the normalized `inspect` is undefined for any session gjc >= 0.16.0
	 * reports, which made every resume fail as "saved authority unavailable".
	 */
	async resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding> {
		this.#assertOwned(input);
		const resumable = (state: SessionLiveness) =>
			!state.disowned &&
			state.live !== undefined &&
			state.deleted !== true &&
			(state.workspace === undefined || state.workspace === input.repo);
		const existing = await this.#readLiveness(input);
		if (!resumable(existing))
			throw new Error(`cannot resume session ${input.sessionId}: saved authority is unavailable`);
		if (!existing.live) {
			// `session.resume` is a global lifecycle op, not a session control
			// (gjc broker/lifecycle.ts launchInput): it needs the workspace `cwd`
			// and the saved transcript `sessionPath`, which gjc itself verifies
			// belongs to that cwd and session id.
			const sessionPath = await locateSavedTranscript(this.#authority.canonicalAgentDir, input.sessionId);
			if (sessionPath === undefined)
				throw new Error(`cannot resume session ${input.sessionId}: no unique saved transcript`);
			this.#assertOwned(input);
			// A completed or pending close belongs to the old host incarnation.
			// Delete it before resume: the broker may create a new live host and
			// then lose the reply, leaving no safe window for post-resume cleanup.
			this.#releaseCloseOperation(input.sessionId);
			let failure: unknown;
			try {
				parseEnvelope(
					await this.#cli([
						"sdk",
						"session",
						"raw",
						"global",
						"--op",
						"session.resume",
						"--idempotency-key",
						sessionResumeKey(input.sessionId, input.epoch, existing.hostIncarnation),
						"--json-input",
						JSON.stringify({
							sessionId: input.sessionId,
							sessionPath,
							cwd: input.repo,
							readinessTimeoutMs: SESSION_CREATE_READINESS_MS,
						}),
					]),
					"session.resume",
				);
			} catch (error) {
				if (error instanceof BrokerAuthorityError) throw error;
				// A refused request never restored anything. Every other failure -
				// including the CLI's `operation_failed` (outcome unknown), which is
				// also how it reports a broker invalid_input - is settled by inspect.
				if (sdkErrorCode(error) === "invalid_input" || sdkErrorCode(error) === "idempotency_conflict")
					throw sanitizeSdkFailure(error);
				failure = error;
			}
			const resumed = await this.#readLiveness(input);
			if (!resumable(resumed) || resumed.live !== true) {
				if (failure !== undefined) throw sanitizeSdkFailure(failure);
				throw new Error(`session.resume did not restore live authority for ${input.sessionId}`);
			}
		}
		this.#resetCreateRotations(input.originKey);
		this.#releaseCloseOperation(input.sessionId);
		return { sessionId: input.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
	}

	async send(input: SessionSendInput): Promise<SendReceipt> {
		this.#assertOwned(input);
		assertValidOpRef(input.opRef);
		if (input.text.trim().length === 0) throw new OpRefError("prompt text must not be empty");
		// `turn.prompt` is not idempotent at the broker (a reused clientRef is
		// refused), so the gateway journals it: intent before the relay call,
		// receipt after. Completed acceptance returns the stored receipt; only
		// a durable busy refusal can be re-armed for another submission. Pending
		// intents reconcile the same clientRef and never resend on unknown.
		// The hash covers every field that shapes the turn, so an op ref reused
		// with the same text but another preamble or model is a different prompt.
		const requestJson = JSON.stringify({
			prompt: createHash("sha256")
				.update(JSON.stringify([input.text, input.systemPreamble ?? null, input.model ?? null]))
				.digest("hex"),
		});
		const intent = this.#beginOperation(
			"turn.prompt",
			JSON.stringify([input.sessionId, input.opRef]),
			input.opRef,
			() => requestJson,
		);
		if (intent.requestJson !== requestJson)
			throw new OpRefError(`op ref ${input.opRef} was already used for a different prompt`);
		if (intent.state === "complete" && !isBrokerPromptBusyRefusal(intent))
			return JSON.parse(intent.resultJson!) as SendReceipt;
		let relay: TailHandle;
		try {
			relay =
				input.relay ?? (await this.attachTail({ sessionId: input.sessionId, brokerGeneration: 0, repo: input.repo }));
		} catch (error) {
			// A fresh intent has not reached turn.prompt when its relay cannot be
			// attached. Remove it so a retry can submit rather than reconciling a
			// clientRef that was never sent.
			if (!intent.resumed) this.#releaseOperation(intent);
			throw error;
		}
		try {
			const receipt =
				intent.resumed && !isBrokerPromptBusyRefusal(intent)
					? await this.#acceptedPrompt(input, relay)
					: await this.#submitPrompt(input, relay, intent);
			return this.#completePromptOperation(intent, receipt);
		} finally {
			if (!input.relay) await relay.close();
		}
	}

	/** The receipt of a prompt the broker already accepted under this clientRef. */
	async #acceptedPrompt(input: SessionSendInput, relay: TailHandle): Promise<SendReceipt> {
		const report = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef, relay });
		if (report.status.status === "unknown")
			throw new GjcCliError(`prompt ${input.opRef} status is unknown; refusing to resend`, 0, "", {
				code: "operation_uncertain",
				opRef: input.opRef,
			});
		console.info(
			`session_prompt_replayed session=${input.sessionId} opRef=${input.opRef} status=${report.status.status}`,
		);
		return {
			sessionId: input.sessionId,
			operationRef: input.opRef,
			...(report.status.commandId ? { commandId: report.status.commandId } : {}),
			...(report.status.turnId ? { turnId: report.status.turnId } : {}),
			acceptedAt: new Date(report.status.acceptedAt ?? this.#now()).toISOString(),
			taskKey: "gateway",
		};
	}

	async #submitPrompt(input: SessionSendInput, relay: TailHandle, intent: BrokerOperationRecord): Promise<SendReceipt> {
		if (input.model) {
			try {
				await this.setModel({ sessionId: input.sessionId, repo: input.repo, selection: input.model });
			} catch (error) {
				// Model selection is preflight: no turn.prompt was issued yet.
				if (!isBrokerPromptBusyRefusal(intent)) this.#releaseOperation(intent);
				throw error;
			}
		}
		const text = renderPrompt(input.systemPreamble, input.text);
		// The prompt is submitted on the session's resident relay so that this
		// connection OWNS the turn: the host streams the turn's content only to
		// the connection that submitted it. A caller without a relay gets one
		// for the duration of the send.
		const stallMs = input.busyStallMs ?? DEFAULT_SESSIONS_BUSY_STALL_MS;
		// A configured stall bound is always reachable: the busy wait outlasts it
		// by one sample, so a wedged host is replaced rather than requeued forever.
		const deadline =
			this.#now() +
			Math.max(
				input.busyWaitMs ?? DEFAULT_BUSY_WAIT_MS,
				input.busyStallMs === undefined ? 0 : input.busyStallMs + BUSY_PROGRESS_SAMPLE_MS,
			);
		let waited = false;
		let submission = intent;
		// Progress evidence while the host refuses with `busy` (#687): a host that
		// claims to be busy but never moves its activity stamp or transcript is
		// wedged, and waiting it out would block the origin until the deadline.
		let progress: string | undefined;
		let progressAt = this.#now();
		let nextSampleAt = 0;
		for (;;) {
			const retryingBusy = isBrokerPromptBusyRefusal(submission);
			if (retryingBusy) {
				submission = this.#database.transaction((tx) =>
					this.#database.bindings.rearmBrokerPromptOperation(tx, this.#authority, submission),
				);
				if (submission.state === "complete") return JSON.parse(submission.resultJson!) as SendReceipt;
				if (submission.resumed) return await this.#acceptedPrompt(input, relay);
			}
			try {
				this.#database.bindings.assertAuthority(this.#authority);
			} catch (error) {
				this.#releaseOperation(intent);
				throw error;
			}
			let response: RelayResponse;
			try {
				response = await relay.control("turn.prompt", { text, clientRef: input.opRef });
			} catch (error) {
				// The retry never reached the host (relay gone before the frame was
				// written, e.g. the host exited while we waited): the authenticated
				// busy refusal still stands, so the op stays provably unadmitted (#689).
				if (retryingBusy && isUnwrittenRelayRequest(error))
					this.#completeOperation(submission, BROKER_PROMPT_BUSY_REFUSAL);
				throw error;
			}
			this.#database.bindings.assertAuthority(this.#authority);
			if (response.ok) {
				const result = recordOf(response.result) ?? {};
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
			const code = envelopeErrorCode(error.details);
			// The broker answered: nothing was accepted under this clientRef, so the
			// intent is dropped. A conflict means the ref is held by an op the broker
			// knows, which only a later reconcile may resolve; it stays pending.
			if (code === CLIENT_REF_CONFLICT_CODE)
				throw new OpRefRejectedError(input.opRef, CLIENT_REF_CONFLICT_CODE, error.details);
			if (code === MODEL_NOT_SELECTED_CODE) {
				this.#releaseOperation(intent);
				throw new ModelNotSelectedError(input.opRef);
			}
			// `busy` is occupancy, not failure: the prompt was never accepted and
			// the op-ref is still unused. Wait for the session to go idle
			// (bounded) and resend under the same op-ref before giving up.
			if (!isSessionBusy(error)) {
				this.#releaseOperation(submission);
				throw error;
			}
			// Preserve the authenticated non-acceptance before deliberately waiting.
			// A crash here must not look like a possibly accepted submission.
			this.#completeOperation(submission, BROKER_PROMPT_BUSY_REFUSAL);
			const sampledAt = this.#now();
			if (sampledAt >= nextSampleAt) {
				nextSampleAt = sampledAt + BUSY_PROGRESS_SAMPLE_MS;
				const sample = await this.#busyProgress(input);
				// An incomplete sample is not evidence of a stall: restart the clock.
				if (sample === undefined || sample !== progress) {
					progress = sample;
					progressAt = sampledAt;
				} else if (sampledAt - progressAt >= stallMs)
					throw new SessionBusyStalledError(input.sessionId, input.opRef, sampledAt - progressAt, sample);
			}
			if (this.#now() >= deadline) throw error;
			submission = { ...submission, state: "complete", resultJson: JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL) };
			if (!waited) {
				waited = true;
				console.info(`session_busy_wait session=${input.sessionId} opRef=${input.opRef}`);
			}
			await this.#sleep(BUSY_POLL_MS);
		}
	}

	/**
	 * Fingerprint of a busy host's progress: the inspect activity stamp AND the
	 * saved transcript's size/mtime. Both are required; with either unreadable
	 * the sample is undefined, so a stall is never concluded from partial
	 * evidence (a quiet long call may move only the part that could not be read).
	 */
	async #busyProgress(input: { sessionId: string; repo: string; opRef: string }): Promise<string | undefined> {
		const unavailable = (source: string, detail: string): undefined => {
			console.warn(
				`session_busy_probe_unavailable session=${input.sessionId} opRef=${input.opRef} source=${source} detail=${sanitizeDiagnostic(detail)}`,
			);
			return undefined;
		};
		let activity: SessionActivity | undefined;
		try {
			activity = (await this.#readLiveness(input)).activity;
		} catch (error) {
			if (error instanceof BrokerAuthorityError) throw error;
			return unavailable("inspect", String(error));
		}
		if (!activity) return unavailable("inspect", "no activity stamp");
		const transcript = await locateSavedTranscript(this.#authority.canonicalAgentDir, input.sessionId);
		if (transcript === undefined) return unavailable("transcript", "saved transcript not located");
		try {
			const info = await stat(transcript);
			return `activity=${activity.state}@${activity.at} transcript=${info.size}@${Math.trunc(info.mtimeMs)}`;
		} catch (error) {
			return unavailable("transcript", String(error));
		}
	}
	promptRefusal(input: { sessionId: string; repo: string; opRef: string }): "busy" | undefined {
		this.#assertOwned(input);
		this.#database.bindings.assertAuthority(this.#authority);
		const intent = this.#database.bindings.brokerOperation(
			this.#authority,
			"turn.prompt",
			JSON.stringify([input.sessionId, input.opRef]),
		);
		return intent?.operationRef === input.opRef && isBrokerPromptBusyRefusal(intent) ? "busy" : undefined;
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.#assertOwned(input);
		assertControlAllowed("turn.steer", { operatorApproval: true });
		if (input.text.trim().length === 0) throw new OpRefError("steer text must not be empty");
		const relay =
			input.relay ?? (await this.attachTail({ sessionId: input.sessionId, brokerGeneration: 0, repo: input.repo }));
		let response: RelayResponse;
		try {
			this.#database.bindings.assertAuthority(this.#authority);
			response = await relay.control("turn.steer", { text: input.text, clientRef: input.clientRef });
			this.#database.bindings.assertAuthority(this.#authority);
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
		relay?: TailHandle;
	}): Promise<{ readonly changed: boolean }> {
		this.#assertOwned(input);
		const selection = input.selection;
		if (typeof selection !== "string") {
			return await this.#overRelay(input, "model.profile.set", async (sdk) => {
				const activation = parseEnvelope<boolean | { changed?: unknown; id?: unknown }>(
					await sdk("control", "model.profile.set", { id: selection.preset }),
					"model.profile.set",
				);
				const changed = typeof activation === "boolean" ? activation : activation?.changed;
				if (typeof changed !== "boolean") throw new Error("model.profile.set succeeded without a changed receipt");
				return { changed };
			});
		}
		// `provider/model:level` is the gjc CLI selector; `model.set` accepts it but keeps the
		// session's thinking at `inherit`, so the level suffix was silently a no-op (2026-09-26:
		// `:xhigh` and `:low` produced identical reasoning budgets). Apply the level explicitly.
		const { model, thinking } = splitThinkingSuffix(selection);
		return await this.#overRelay(input, "model.set", async (sdk) => {
			const result = parseEnvelope<{ changed?: unknown } | undefined>(
				await sdk("control", "model.set", { id: model }),
				"model.set",
			);
			if (typeof result?.changed !== "boolean") throw new Error("model.set succeeded without a changed receipt");
			const thinkingChanged = thinking ? await this.#setThinking(sdk, thinking) : false;
			return { changed: result.changed || thinkingChanged };
		});
	}

	async #setThinking(sdk: SdkTransport, level: string): Promise<boolean> {
		const applied = parseEnvelope<{ changed?: unknown } | undefined>(
			await sdk("control", "thinking.set", { level }),
			"thinking.set",
		);
		if (typeof applied?.changed !== "boolean") throw new Error("thinking.set succeeded without a changed receipt");
		return applied.changed;
	}

	async setServiceTier(input: {
		sessionId: string;
		repo: string;
		tier: GjcServiceTier;
		relay?: TailHandle;
	}): Promise<{ readonly changed: boolean }> {
		this.#assertOwned(input);
		return await this.#overRelay(input, "service_tier.set", async (sdk) => {
			const result = parseEnvelope<{ changed?: unknown } | undefined>(
				await sdk("control", "service_tier.set", { tier: input.tier }),
				"service_tier.set",
			);
			if (typeof result?.changed !== "boolean") throw new Error("service_tier.set succeeded without a changed receipt");
			return { changed: result.changed };
		});
	}

	async status(input: {
		sessionId: string;
		repo: string;
		opRef: string;
		relay?: TailHandle;
		priority?: "interactive" | "background";
	}): Promise<StatusReport> {
		this.#assertOwned(input);
		// With a live relay the read is one round-trip on the owned connection;
		// without one (retired holds, work lanes, terminal recovery) the CLI's
		// `session status` performs the identical `turn.result` query.
		if (!input.relay)
			return await fetchOpState(this.#controller(input.repo, input.priority), input.sessionId, input.opRef);
		this.#database.bindings.assertAuthority(this.#authority);
		const response = await input.relay.query("turn.result", { kind: "prompt", clientRef: input.opRef });
		this.#database.bindings.assertAuthority(this.#authority);
		if (!response.ok) throw relayFailure("turn.result", response);
		const status = recordOf(response.result) ?? {};
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
				const raw = await this.#overRelay(
					input,
					"turn.result",
					async (sdk) =>
						await sdk("query", "turn.result", { kind: "prompt", clientRef: input.opRef }, { timeoutMs: 15_000 }),
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

	async queueEmpty(input: { sessionId: string; repo: string; relay?: TailHandle }): Promise<boolean> {
		this.#assertOwned(input);
		const result = await this.#overRelay(
			input,
			"queue.messages.list",
			async (sdk) => await sdk("query", "queue.messages.list", {}, { timeoutMs: 10_000 }),
		);
		const page = (JSON.parse(result.stdout) as { ok?: unknown; page?: { items?: unknown[]; complete?: unknown } }).page;
		return page !== undefined && Array.isArray(page.items) && page.items.length === 0 && page.complete === true;
	}

	async runningJobs(input: { sessionId: string; repo: string }): Promise<readonly RunningHostJob[]> {
		this.#assertOwned(input);
		const result = await this.#cli(
			["sdk", "session", "raw", "query", input.sessionId, "--query", "runtime.jobs.list", "--json-input", "{}"],
			{ timeoutMs: 10_000 },
		);
		return parseRunningJobs(result.stdout);
	}

	async turnRunning(input: { sessionId: string; repo: string; relay?: TailHandle }): Promise<boolean | undefined> {
		this.#assertOwned(input);
		const result = await this.#overRelay(
			input,
			"context.get",
			async (sdk) => await sdk("query", "context.get", {}, { timeoutMs: 10_000 }),
		);
		if (result.exitCode !== 0) return undefined;
		const response = JSON.parse(result.stdout) as {
			ok?: unknown;
			page?: { items?: unknown[]; complete?: unknown };
		};
		if (
			response.ok !== true ||
			response.page?.complete !== true ||
			!Array.isArray(response.page.items) ||
			response.page.items.length !== 1
		)
			return undefined;
		const snapshot = recordOf(response.page.items[0]);
		if (snapshot?.isStreaming === true) return true;
		if (
			(typeof snapshot?.steeringQueueDepth === "number" && snapshot.steeringQueueDepth > 0) ||
			(typeof snapshot?.followupQueueDepth === "number" && snapshot.followupQueueDepth > 0)
		)
			return true;
		if (snapshot?.isStreaming === false && snapshot.steeringQueueDepth === 0 && snapshot.followupQueueDepth === 0)
			return false;
		return undefined;
	}

	async contextUsage(input: {
		sessionId: string;
		repo: string;
		relay: TailHandle;
	}): Promise<SessionContextUsage | undefined> {
		this.#assertOwned(input);
		this.#database.bindings.assertAuthority(this.#authority);
		const response = await input.relay.query("context.get", {}, { timeoutMs: CONTEXT_QUERY_TIMEOUT_MS });
		if (!response.ok) return undefined;
		const items = response.page?.items;
		const snapshot = Array.isArray(items) ? recordOf(items[0]) : undefined;
		const usage = recordOf(snapshot?.usage);
		const percent = usage?.percent;
		const window = usage?.contextWindow;
		if (typeof percent !== "number" || !Number.isFinite(percent)) return undefined;
		if (typeof window !== "number" || !Number.isFinite(window) || window <= 0) return undefined;
		// The wire usage carries no token count; percent is tokens over the window.
		return { percent, tokens: Math.round((percent / 100) * window) };
	}

	async close(input: { sessionId: string; repo: string }): Promise<void> {
		this.#assertOwned(input);
		assertControlAllowed("session.close", { operatorApproval: true });
		await this.#closeSessionOperation(input.sessionId);
	}

	async #closeSessionOperation(sessionId: string): Promise<void> {
		// Journaled per session: retries replay a possibly applied close under its
		// original idempotency key, including cleanup of a mismatched pending create.
		// A completed close is not issued again.
		const intent = this.#beginOperation("session.close", sessionId, `gw-close-${randomUUID()}`, () =>
			JSON.stringify({ sessionId }),
		);
		if (intent.state === "complete") return;
		// Lifecycle op: the per-session `control` route prohibits it for the
		// daemon CLI (adapter_operation_prohibited on gjc 0.16.x); only the
		// `global` route carries it, like session.create and session.delete.
		let result: unknown;
		try {
			result = parseEnvelope(
				await this.#cli(
					[
						"sdk",
						"session",
						"raw",
						"global",
						"--op",
						"session.close",
						"--idempotency-key",
						intent.operationRef,
						"--json-input",
						intent.requestJson,
					],
					{ timeoutMs: 30_000 },
				),
				"session.close",
			);
		} catch (error) {
			// The broker answered that nothing was or can be closed: the intent is spent.
			if (isRefusedOutcome(error)) this.#releaseOperation(intent);
			throw error;
		}
		this.#completeOperation(intent, result ?? null);
	}

	async fetchAssistantSince(input: {
		sessionId: string;
		repo: string;
		notBeforeMs: number;
		relay?: TailHandle;
		preferSpoken?: boolean;
	}): Promise<LastAssistantResult | undefined> {
		this.#assertOwned(input);
		return await this.#overRelay(input, "transcript.list", async (sdk) => await this.#assistantSince(input, sdk));
	}

	async #assistantSince(
		input: { notBeforeMs: number; preferSpoken?: boolean },
		sdk: SdkTransport,
	): Promise<LastAssistantResult | undefined> {
		let cursor: string | undefined;
		type Row = { role?: string; ts?: string; textSummary?: string; body?: string };
		const rowText = (row: Row): string =>
			(typeof row.body === "string" && row.body) || (typeof row.textSummary === "string" ? row.textSummary : "");
		let latest: Row | undefined;
		let latestSpoken: Row | undefined;
		const seenCursors = new Set<string>();
		for (let pages = 1; pages <= 1_000; pages++) {
			const result = await sdk("query", "transcript.list", {}, { timeoutMs: 15_000, ...(cursor ? { cursor } : {}) });
			const envelope = JSON.parse(result.stdout) as { ok?: unknown };
			// A refusal is the host's answer: surface its code, not a missing page.
			if (envelope.ok === false) parseEnvelope(result, "transcript.list");
			const page = (
				envelope as {
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
				if (!Number.isFinite(at) || at + 2_000 < input.notBeforeMs) continue;
				latest = row;
				const text = rowText(row);
				if (text.trim() && !isSilentReply(text)) latestSpoken = row;
			}
			if (page.complete === true) {
				const chosen = (input.preferSpoken ? latestSpoken : undefined) ?? latest;
				if (!chosen) return undefined;
				return { text: rowText(chosen), pages, complete: true };
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

	async fetchLastAssistant(input: {
		sessionId: string;
		repo: string;
		relay?: TailHandle;
	}): Promise<LastAssistantResult> {
		this.#assertOwned(input);
		return await this.#overRelay(input, "session.last_assistant", async (sdk) => await lastAssistant(sdk));
	}

	async attachTail(input: TailAttachInput): Promise<TailHandle> {
		this.#assertOwned(input);
		return await this.#tailRunner.attach(input);
	}

	async runCompaction(input: SessionCompactionInput): Promise<{ readonly status: SessionCompactionStatus }> {
		this.#assertOwned(input);
		try {
			const result = parseEnvelope<Record<string, unknown>>(
				await this.#cli(
					["sdk", "session", "raw", "control", input.sessionId, "--op", "compaction.run", "--json-input", "{}"],
					{ timeoutMs: COMPACTION_TIMEOUT_MS },
				),
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
		try {
			this.#assertOwned(input);
		} catch (error) {
			throw markSessionRequestStep(error, "authority");
		}
		// One relay owns the whole request: the send, the terminal wait, and the
		// final read. Monitor/batch authoring consumes no mid-turn content, so
		// the handle's frames only feed the stall alarm and the activity lease.
		let lastActivityAt = this.#now();
		let relay: TailHandle;
		try {
			relay = await this.attachTail({
				sessionId: input.sessionId,
				brokerGeneration: 0,
				repo: input.repo,
				...(input.originKey ? { originKey: input.originKey } : {}),
				onFrame: () => {
					lastActivityAt = this.#now();
				},
				onStall: ({ elapsedMs }) =>
					console.warn(`session stall sessionId=${input.sessionId} opRef=${input.opRef} silentMs=${elapsedMs}`),
			});
		} catch (error) {
			throw markSessionRequestStep(error, error instanceof BrokerAuthorityError ? "authority" : "attach");
		}
		// `turn.result` on the relay while it is up; on the CLI (the authoritative
		// recovery transport) when the relay tore. A torn relay is never a verdict
		// about the operation: the accepted op keeps being observed under the SAME
		// clientRef, so a monitor event is not re-authored under a fresh one.
		const target = { sessionId: input.sessionId, repo: input.repo, opRef: input.opRef };
		const readStatus = async (): Promise<StatusReport> => {
			try {
				return await this.status({ ...target, relay });
			} catch (error) {
				if (!isRelayTransportFailure(error)) throw markSessionRequestStep(error, "status");
				console.error(`status_relay_unavailable session=${input.sessionId} opRef=${input.opRef}`);
				try {
					return await this.status(target);
				} catch (fallbackError) {
					throw markSessionRequestStep(fallbackError, "status");
				}
			}
		};
		let result: SessionRequestResult | undefined;
		let requestFailed = false;
		let requestError: unknown;
		try {
			try {
				relay.beginTurn(input.opRef);
				relay.setTurnRunning(true);
			} catch (error) {
				throw markSessionRequestStep(error, "attach");
			}
			let receipt: SendReceipt;
			let status: StatusReport | undefined;
			try {
				receipt = await this.send({ ...input, relay });
				relay.correlate(input.opRef, receipt);
			} catch (sendError) {
				if (sendError instanceof BrokerAuthorityError) throw markSessionRequestStep(sendError, "authority");
				const taggedSendError = markSessionRequestStep(sendError, "send");
				// Preserve local validation/fingerprint or journal-identity errors.
				// Recovery must not disguise them as a prior operation's success.
				if (sendError instanceof OpRefError) throw taggedSendError;
				// A transport/control failure may occur after the runtime accepted the
				// prompt. Query the SAME clientRef before retrying; monitor authoring
				// otherwise ran the event, produced a final answer, then executed it
				// again because the torn send was treated as definitive failure.
				try {
					status = await readStatus();
				} catch (statusError) {
					const taggedStatusError = markSessionRequestStep(statusError, "status");
					if (statusError instanceof BrokerAuthorityError) throw taggedStatusError;
					throw taggedSendError;
				}
				if (status.status.status === "unknown") throw taggedSendError;
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
				throw markSessionRequestStep(new SessionRequestTimeoutError(input.sessionId, input.opRef, status), "status");
			// Native terminal_ok also includes cancellation, refusal and budget
			// stops; missing/unknown receipts remain holds. Reuse the canonical
			// projection before consuming an assistant response as this answer.
			if (projectOpState(status.status) !== "completed")
				throw markSessionRequestStep(new SessionTerminalError(status), "status");
			result = {
				receipt,
				status,
				assistant: await this.fetchLastAssistant({ sessionId: input.sessionId, repo: input.repo, relay }).catch(
					(error: unknown) => {
						throw markSessionRequestStep(error, "last_assistant");
					},
				),
			};
		} catch (error) {
			requestFailed = true;
			requestError = error;
		}
		let cleanupFailed = false;
		let cleanupError: unknown;
		try {
			relay.setTurnRunning(false);
		} catch (error) {
			cleanupFailed = true;
			cleanupError = error;
		}
		try {
			await relay.close();
		} catch (error) {
			if (!cleanupFailed) {
				cleanupFailed = true;
				cleanupError = error;
			}
		}
		if (requestFailed) throw requestError;
		if (cleanupFailed) throw markSessionRequestStep(cleanupError, "close");
		if (result === undefined) throw new Error("session request completed without a result");
		return result;
	}

	#assertOwned(input: { sessionId: string; repo: string }): void {
		this.#database.bindings.assertOwnedSession(input.sessionId, input.repo, this.#authority);
	}

	/**
	 * Runs one session-scoped read or control on the caller's live relay, and on
	 * the CLI only when the relay transport failed (closed, timed out) - the same
	 * rule as `status()`. A relay refusal (`ok: false`) is the host's answer and
	 * surfaces exactly as the CLI's envelope would; it is never retried there.
	 * Without a relay the CLI is the transport: opening a relay (itself a
	 * `serve --stdio` spawn) for one request would buy nothing. A paged read that
	 * tears mid-way restarts on the CLI: relay cursors are grants of the relay
	 * connection and do not carry over.
	 */
	async #overRelay<T>(
		input: { readonly sessionId: string; readonly relay?: TailHandle },
		operation: string,
		work: (sdk: SdkTransport) => Promise<T>,
	): Promise<T> {
		const cli = this.#cliTransport(input.sessionId);
		if (!input.relay) return await work(cli);
		try {
			return await work(this.#relayTransport(input.relay));
		} catch (error) {
			if (!isRelayTransportFailure(error)) throw error;
			console.error(
				`relay_request_unavailable session=${input.sessionId} op=${operation} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error))}`,
			);
			return await work(cli);
		}
	}

	#cliTransport(sessionId: string): SdkTransport {
		return async (kind, name, input, options) =>
			await this.#cli(
				[
					"sdk",
					"session",
					"raw",
					kind,
					sessionId,
					kind === "control" ? "--op" : "--query",
					name,
					...(input === undefined ? [] : ["--json-input", JSON.stringify(input)]),
					...(options?.cursor ? ["--cursor", options.cursor] : []),
				],
				options?.timeoutMs === undefined ? undefined : { timeoutMs: options.timeoutMs },
			);
	}

	/**
	 * The relay answer printed in the CLI's shape (`{ ok, result | page | error }`, exit 0).
	 * The CLI timeouts budget a process spawn plus broker attach; a request on an
	 * open connection keeps the runner's own request timeout.
	 */
	#relayTransport(relay: TailHandle): SdkTransport {
		return async (kind, name, input, options) => {
			const requestOptions = options?.cursor ? { cursor: options.cursor } : {};
			this.#database.bindings.assertAuthority(this.#authority);
			const response =
				kind === "control"
					? await relay.control(name, input ?? {}, requestOptions)
					: await relay.query(name, input ?? {}, requestOptions);
			this.#database.bindings.assertAuthority(this.#authority);
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					...(kind === "query" ? { type: "query_response" } : {}),
					ok: response.ok,
					...(response.result === undefined ? {} : { result: response.result }),
					...(response.page === undefined ? {} : { page: response.page }),
					...(response.ok ? {} : { error: response.error ?? {} }),
				}),
				stderr: "",
			};
		};
	}

	#controller(repo: string, priority?: "interactive" | "background"): ControllerOptions {
		const run: CliRunner = priority ? (args, opts) => this.#cli(args, { ...opts, priority }) : this.#cli;
		return { run, repo };
	}
}

/** `session.last_assistant`, paged by the host's continuation cursor until complete. */
async function lastAssistant(sdk: SdkTransport): Promise<LastAssistantResult> {
	const maxPages = 50;
	const chunks: string[] = [];
	let cursor: string | undefined;
	for (let pages = 1; pages <= maxPages; pages++) {
		const page = parseLastAssistantPage(
			await sdk("query", "session.last_assistant", undefined, cursor ? { cursor } : undefined),
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

/**
 * `runtime.jobs.list` answers one page item `{running, recent, delivery}`. Only
 * `running` matters to a caller about to end the host. Anything unparseable
 * throws: an unreadable job list is not an empty one.
 */
export function parseRunningJobs(stdout: string): readonly RunningHostJob[] {
	const envelope = JSON.parse(stdout) as { ok?: unknown; page?: { items?: unknown } };
	const items = envelope.ok === true ? envelope.page?.items : undefined;
	const running = Array.isArray(items) ? (recordOf(items[0])?.running as unknown) : undefined;
	if (!Array.isArray(running)) throw new Error("runtime.jobs.list returned no running-job list");
	return running.map((entry) => {
		const job = recordOf(entry) ?? {};
		return {
			id: typeof job.id === "string" ? job.id : "unknown",
			type: typeof job.type === "string" ? job.type : "unknown",
			label: typeof job.label === "string" ? job.label : "",
		};
	});
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
			(isSessionGoneCode(code) ||
				[
					"resource_gone",
					"unavailable",
					"unsupported_operation",
					"unknown_operation",
					"unknown_query",
					"unsupported_query",
					"not_supported",
					"operation_not_session_owned",
				].includes(code))
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
	const unresolvedFailure =
		result.status === "failed" &&
		(result.receiptState === undefined ||
			["missing", "unknown", "absent"].some((state) => state === result.receiptState));
	if (
		(result.status !== "terminal_ok" && !unresolvedFailure) ||
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
	// A failed operation's nonpresent receipt is not definitive output loss.
	// Keep validated, content-free responses inside the same bounded retrieval
	// budget; malformed states and unproven content remain fail-closed.
	if (result.status === "failed")
		return result.content === undefined && result.textSummary === undefined
			? { status: "absent", code: "output_pending" }
			: { status: "unavailable", code: "invalid_evidence" };
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
		terminal: {
			status: "terminal_ok",
			clientRef: result.clientRef,
			...(typeof result.commandId === "string" ? { commandId: result.commandId } : {}),
			...(typeof result.turnId === "string" ? { turnId: result.turnId } : {}),
			...(typeof result.startedAt === "number" ? { startedAt: result.startedAt } : {}),
			terminalAt: result.terminalAt,
			receiptState: "present",
			...(workerRecord(result.outcome) ? { outcome: result.outcome as PromptOutcomeBody } : {}),
		},
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
	if (!systemPreamble) return text.toWellFormed();
	// SDK `session send` has no unproven system-prompt flag. Keep the trusted
	// bootstrap in the same accepted turn instead of inventing a raw control API.
	return `${systemPreamble}\n\n${text}`.toWellFormed();
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
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		envelope = parsed as Record<string, unknown>;
	} catch (error) {
		if (error instanceof LastAssistantUnreadableError) throw error;
		throw new LastAssistantUnreadableError("last_assistant_not_json");
	}
	if (envelope.ok === false) {
		parseEnvelope<unknown>(result, "session raw query session.last_assistant");
	}
	if (envelope.ok !== true) throw new LastAssistantUnreadableError("last_assistant_no_page");
	const page = envelope.page;
	if (typeof page !== "object" || page === null || Array.isArray(page))
		throw new LastAssistantUnreadableError("last_assistant_no_page");
	const items = (page as Record<string, unknown>).items;
	if (!Array.isArray(items)) throw new LastAssistantUnreadableError("last_assistant_no_items");
	if (items.some((item) => item === null)) throw new LastAssistantUnreadableError("last_assistant_null_item");
	if (items.some((item) => typeof item !== "string"))
		throw new LastAssistantUnreadableError("last_assistant_non_text_item");
	// gjc hosts grant `continuationCursor`; older CLI prints carried `cursor`.
	const record = page as Record<string, unknown>;
	const cursor = typeof record.continuationCursor === "string" ? record.continuationCursor : record.cursor;
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

const HOST_SIGTERM_GRACE_MS = 1_000;
/** Extra time after terminal status cancels SIGKILL before a signaled host is force-retired. */
const HOST_SIGTERM_ESCALATION_GRACE_MS = 1_000;
const HOST_SIGKILL_GRACE_MS = 1_000;
const HOST_EXIT_POLL_MS = 100;

type ProcessCommandInspection =
	| { readonly outcome: "live"; readonly command: string; readonly identity: string; readonly context: string }
	| { readonly outcome: "gone" }
	| { readonly outcome: "unreadable"; readonly reason: string };

type ProcessExitWait = "exited" | "changed" | "unreadable" | "timeout";

async function waitForProcessExit(
	pid: number,
	expected: Extract<ProcessCommandInspection, { readonly outcome: "live" }>,
	timeoutMs: number,
): Promise<ProcessExitWait> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const current = await processCommand(pid);
		if (current.outcome === "gone") return "exited";
		if (current.outcome === "unreadable") return "unreadable";
		if (current.identity !== expected.identity || current.command !== expected.command) return "changed";
		const remaining = deadline - Date.now();
		if (remaining <= 0) return "timeout";
		await Bun.sleep(Math.min(HOST_EXIT_POLL_MS, remaining));
	}
}

/** `ps` is the portable process read on macOS and Linux; only a clean no-match proves the pid is gone. */
async function processCommand(pid: number): Promise<ProcessCommandInspection> {
	try {
		const proc = Bun.spawn(["ps", "-o", "pid=,ppid=,pgid=,lstart=,command=", "-p", String(pid)], {
			stdout: "pipe",
			stderr: "pipe",
			env: { ...process.env, LC_ALL: "C" },
		});
		const [stdout, stderr, exitCode] = await Promise.all([
			new Response(proc.stdout).text(),
			new Response(proc.stderr).text(),
			proc.exited,
		]);
		const line = stdout.trim();
		if (exitCode === 0 && line.length > 0) {
			const match =
				/^(\d+)\s+(\d+)\s+(\d+)\s+([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})(?:\s+(.*))?$/.exec(
					line,
				);
			if (!match || Number(match[1]) !== pid) return { outcome: "unreadable", reason: "ps_pid_mismatch" };
			return {
				outcome: "live",
				identity: `${match[1]}:${match[4]}`,
				context: `${match[2]}:${match[3]}`,
				command: match[5]?.trim() ?? "",
			};
		}
		if (exitCode === 1 && line.length === 0 && stderr.trim().length === 0) return { outcome: "gone" };
		return { outcome: "unreadable", reason: `ps_exit_${exitCode}` };
	} catch (error) {
		return { outcome: "unreadable", reason: sanitizeDiagnostic(String(error)) };
	}
}

/**
 * The SDK router's own verdict that it serves no host for the session, in any
 * code the supported GJC versions use for it. Broker transport failures
 * (`broker_unavailable`, timeouts, unparsable output) carry no envelope code
 * and never match.
 */
export function isSessionUnavailable(error: unknown): boolean {
	return isSessionGoneCode(sdkErrorCode(error));
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

/**
 * The rebindable code of a session.create failure whose idempotency key can
 * never succeed, or undefined when the same key must be retried.
 *
 * gjc <= 0.17 reported a condemned key by its internal code (`terminal_uncertain`,
 * `spawn_failed`, ...). gjc 0.18's public error contract collapses every
 * internal code to `operation_failed`; the only remaining evidence that the key
 * itself is undecidable is `outcomeCertainty: "unknown"` with that exact key
 * named as a reference. Measured on gaebal-gajae 2026-09-30: 32 creates left
 * `terminal_uncertain` by a broker kill loop answered exactly that envelope,
 * deterministically, on every replay, and six channels stayed mute for hours.
 * A bare `operation_failed` without the key reference stays a same-key retry.
 */
function poisonedCreateKeyCode(error: unknown, idempotencyKey: string): string | undefined {
	// GJC 0.19's legacy-history fence is not a disposable poisoned key. Its
	// public code is also operation_failed/unknown, but changing key or target
	// would bypass the broker's explicit requirement for operator reconciliation.
	if (error instanceof GjcCliError && error.diagnostics.includes("lifecycle_legacy_history_ambiguous"))
		return undefined;
	const code = sdkErrorCode(error);
	if (isRebindableCode(code)) return code;
	if (code !== "operation_failed" || !(error instanceof GjcCliError)) return undefined;
	const details = recordOf(error.details);
	if (details?.outcomeCertainty !== "unknown") return undefined;
	const references = Array.isArray(details.references) ? details.references : [];
	const namesKey = references.some((reference) => {
		const ref = recordOf(reference);
		return ref?.kind === "idempotencyKey" && ref.value === idempotencyKey;
	});
	return namesKey ? code : undefined;
}

/** A missing model is rejected before session.create can have any side effect. */
function unknownModelCreateError(error: unknown): GjcCliError | undefined {
	const details = recordOf(error instanceof GjcCliError ? error.details : undefined);
	const candidates = [
		details?.message,
		error instanceof GjcCliError ? error.stderr : undefined,
		error instanceof Error ? error.message : undefined,
	];
	for (const candidate of candidates) {
		if (typeof candidate !== "string") continue;
		const match =
			/(?:^|\r?\n)(?:\[Uncaught Exception\]\s*)?(?:Error:\s*)?Model "([^"\r\n]+)" not found(?:\r?\n|$)/i.exec(
				candidate,
			);
		if (!match?.[1]) continue;
		const model = sanitizeDiagnostic(match[1]).slice(0, 160);
		const message = `Model "${model}" not found`;
		return new GjcCliError(`gjc sdk session.create rejected unknown model "${model}"`, 0, "", {
			code: "model_not_found",
			message,
		});
	}
	return undefined;
}

const OUTCOME_CERTAINTIES = new Set(["applied", "not-applied", "unknown"]);

function sanitizedDetails(details: unknown): unknown {
	const code = stableErrorCode(envelopeErrorCode(details));
	const record = recordOf(details);
	const message = typeof record?.message === "string" ? sanitizeDiagnostic(record.message) : undefined;
	const outcomeCertainty =
		typeof record?.outcomeCertainty === "string" && OUTCOME_CERTAINTIES.has(record.outcomeCertainty)
			? record.outcomeCertainty
			: undefined;
	// Only idempotency-key references survive: the gateway minted them, and
	// poisonedCreateKeyCode matches them exactly against the key it sent.
	const references = (Array.isArray(record?.references) ? record.references : []).flatMap((reference) => {
		const ref = recordOf(reference);
		const value = stableErrorCode(ref?.value);
		return ref?.kind === "idempotencyKey" && value ? [{ kind: "idempotencyKey", value }] : [];
	});
	return {
		...(code ? { code } : {}),
		...(message ? { message } : {}),
		...(outcomeCertainty ? { outcomeCertainty } : {}),
		...(references.length > 0 ? { references } : {}),
	};
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
			error.diagnostics,
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

/** An authenticated broker answer that the operation was not applied, or that its session is gone. */
function isRefusedOutcome(error: unknown): boolean {
	if (!(error instanceof GjcCliError)) return false;
	return recordOf(error.details)?.outcomeCertainty === "not-applied" || isSessionGoneCode(sdkErrorCode(error));
}

/**
 * One resume attempt per dead host: the closed session keeps its last host's
 * incarnation, and a successful resume replaces it. So a resend of the same
 * attempt (gateway restart mid-resume) reuses the key and gets gjc's ledger
 * replay, while resuming again after that host closes too is a new key - the
 * old one would replay a success whose endpoint is gone.
 */
export function sessionResumeKey(sessionId: string, epoch: number, hostIncarnation: string | undefined): string {
	return `gw-resume-${sessionId}-${epoch}-${hostIncarnation ?? "nohost"}`;
}

/** One in-flight create per origin and epoch: repository/model are request data, not operation identity. */
function createScope(input: SessionBindInput): string {
	return JSON.stringify([input.originKey, input.epoch]);
}

function sessionCreateInput(repo: string, model: GjcModelSelection | undefined): string {
	return JSON.stringify({
		cwd: repo,
		// A fresh session host boots the full agent (~10s measured on the persona
		// host); the runtime's 10s default readiness cutoff turns a slow-but-healthy
		// cold start into spawn_failed. Use the maximum budget: create is idempotent
		// under its key either way.
		readinessTimeoutMs: SESSION_CREATE_READINESS_MS,
		// session.create takes a bare model id; a `:level` suffix is applied with
		// thinking.set once the host is up (see bind()).
		...(typeof model === "string"
			? { modelId: splitThinkingSuffix(model).model }
			: model
				? { modelPreset: model.preset }
				: {}),
	});
}
