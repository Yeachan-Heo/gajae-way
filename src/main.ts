import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { BrokerCli } from "./broker/cli";
import { BrokerReconciler } from "./broker/reconcile";
import { parseWayConfig, type WayConfig } from "./config";
import { ConsoleStartupRefusalError, runWayConsole, sanitizeConsoleText } from "./console/console";
import {
	createMainAdmissionHandler,
	MainAdmissionRecoveryError,
	reconcilePendingMainAdmissions,
} from "./main-session/admission";
import { bootstrapMainSession, recoverBootstrap } from "./main-session/bootstrap";
import {
	ClosureError,
	type ClosureExecutor,
	type ClosureHookContext,
	ClosureMutationReadinessError,
	type ClosureMutationReadinessReason,
	createClosureExecutor,
	runClosureWorker,
} from "./main-session/closure";
import { canonicalJson, createMainGateAnswerHandler } from "./main-session/gates";
import {
	createMainSessionHost,
	type MainSessionHost,
	MainSessionHostError,
	type MainSessionJournal,
	parseMainSessionAdmissionAttributions,
} from "./main-session/host";
import { recallCandidates, restrictionMask } from "./main-session/inject";
import { approveProfile, previewProfileApproval } from "./main-session/profile-approval";
import { ResumeError, strictResumeMainSession } from "./main-session/resume";
import { createScheduler } from "./main-session/scheduler";
import { GatewayStateError, GatewayStateStore } from "./main-session/state";
import { createExternalHostSupervisor } from "./main-session/supervisor";
import { runMcpServer } from "./mcp/serve";
import { loadWayCore, type WayCoreHandle } from "./native-loader";
import { MAX_INJECTION_FILES, ProfileRevisionTracker, SESSION_KINDS } from "./profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "./rpc-bridge";

const usage = `Usage:
  gajaeway [serve] [--state-dir PATH] [--profile PATH] [--fail-closed-linger-ms MS] [--broker-cli PATH] [--reconcile-poll-ms MS]
  gajaeway bootstrap --confirm [--session-id ID] [--state-dir PATH] [--profile PATH] [--broker-cli PATH]
  gajaeway console [--surface-id ID] [--state-dir PATH] [--profile PATH]
  gajaeway mcp serve [--state-dir PATH] [--profile PATH]
  gajaeway profile approve --confirm [--state-dir PATH] [--profile PATH]
  gajaeway --health [--state-dir PATH] | --version`;

class FailedClosedExit extends Error {
	readonly forceExit: boolean;

	constructor(forceExit = false) {
		super("failed_closed");
		this.name = "FailedClosedExit";
		this.forceExit = forceExit;
	}
}

export async function healthPayload(stateDirectory = defaultStateDirectory()): Promise<Record<string, unknown>> {
	try {
		const result = await requestOwnerRpc(path.join(stateDirectory, "rpc.sock"), "way.health", {});
		if (!isRecord(result)) throw new Error("daemon returned a non-object health response");
		return result;
	} catch {
		return {
			status: "unhealthy",
			state: "unavailable",
			reason: "daemon_unreachable",
			version: loadWayCore().healthInfo().version,
		};
	}
}

export function defaultStateDirectory(): string {
	return process.env.GAJAEWAY_STATE_DIR || path.join(os.homedir(), ".local", "state", "gajaeway");
}

/** Starts only the native RPC server; tests and later phases can host their own bridge. */
export function startWayServer(
	stateDirectory = defaultStateDirectory(),
	bridgeHandler?: RpcBridgeHandler,
): WayCoreHandle {
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(path.join(stateDirectory, "rpc.sock"), createRpcBridge(core, bridgeHandler));
	return core;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function failureReason(error: unknown): string {
	if (
		error instanceof ResumeError ||
		error instanceof GatewayStateError ||
		error instanceof MainSessionHostError ||
		error instanceof MainAdmissionRecoveryError
	)
		return error.reason;
	if (error instanceof Error && error.name === "ProfileValidationError") return "profile_invalid";
	return "startup_failed";
}

function createRuntimeSupervisor(config: WayConfig, workspace: string) {
	return createExternalHostSupervisor({
		broker: new BrokerCli({ executable: config.brokerCliPath }),
		workspace,
	});
}

function failBeforeMainHostForE2e(): void {
	if (Bun.env.NODE_ENV === "test" && Bun.env.GAJAEWAY_E2E_FAIL_BEFORE_MAIN_HOST === "1") {
		throw new Error("forced E2E pre-host startup failure");
	}
}

function failAfterMainAdmissionBrokerAcceptedForE2e(): void {
	if (Bun.env.NODE_ENV !== "test") return;
	if (Bun.env.GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED === "1") process.exit(137);
	if (Bun.env.GAJAEWAY_E2E_THROW_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED === "1") {
		throw new Error("forced main admission bridge response failure after broker acceptance");
	}
}

function failAfterMainAdmissionTerminalEvidenceForE2e(): void {
	if (Bun.env.NODE_ENV === "test" && Bun.env.GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_TERMINAL_EVIDENCE === "1") {
		process.exit(137);
	}
}

function failTranscriptProjectionForE2e(): void {
	if (Bun.env.NODE_ENV === "test" && Bun.env.GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION === "1") {
		throw new Error("forced transcript projection failure");
	}
}

function healthFilePath(stateDirectory: string): string {
	return path.join(stateDirectory, "health.json");
}

async function writeHealthFile(stateDirectory: string, payload: Record<string, unknown>): Promise<void> {
	await fsp.mkdir(stateDirectory, { recursive: true, mode: 0o700 });
	const destination = healthFilePath(stateDirectory);
	const temporary = `${destination}.tmp.${process.pid}`;
	await fsp.writeFile(temporary, `${JSON.stringify(payload)}\n`, { encoding: "utf8", mode: 0o600 });
	await fsp.rename(temporary, destination);
}

function createRuntimeMainSessionJournal(
	core: WayCoreHandle,
	stateDirectory: string,
	gatewayState: GatewayStateStore,
): MainSessionJournal {
	return {
		journalAppend: (kind, payloadJson) => core.journalAppend(kind, payloadJson),
		journalAppendAtTailCheckpoint: (kind, payloadJson, expected, checkpoint) => {
			gatewayState.appendTailProjection(expected, checkpoint, kind, payloadJson);
		},
		journalAppendTranscriptProjection: (
			kind,
			payloadJson,
			expectedTail,
			checkpoint,
			expectedDelivery,
			nextDelivery,
		) => {
			failTranscriptProjectionForE2e();
			gatewayState.appendTranscriptProjection(
				expectedTail,
				checkpoint,
				expectedDelivery,
				nextDelivery,
				kind,
				payloadJson,
			);
		},
		setRpcHealth: (state, reason) => {
			let healthState: "degraded" | "running" | "failed_closed" = state;
			let healthReason = reason;
			try {
				const durable = gatewayState.read();
				if (durable.bootstrapState === "FAILED_CLOSED" || durable.failedClosedReason) {
					healthState = "failed_closed";
					healthReason = durable.failedClosedReason ?? reason;
				}
			} catch {
				// The host's explicit degradation remains safer than reporting healthy.
			}
			try {
				core.setRpcHealth(healthState, healthState === "running" ? undefined : healthReason);
				if (healthState === "running") core.sdNotifyReady("gajaeway running");
			} finally {
				const status = healthState === "running" ? "healthy" : "unhealthy";
				void writeHealthFile(stateDirectory, {
					status,
					state: healthState,
					...(healthState === "running" ? {} : { reason: healthReason }),
				}).catch((error) => {
					console.error(
						`Could not write ${healthState} health file: ${error instanceof Error ? error.message : String(error)}`,
					);
				});
			}
		},
		setMainSessionStatus: (turnState, followUpQueueDepth, verificationState) =>
			core.setMainSessionStatus(turnState, followUpQueueDepth, verificationState),
		setJournalDegraded: (degraded) => core.setJournalDegraded(degraded),
	};
}

async function publishHostUnavailableHealth(core: WayCoreHandle): Promise<void> {
	try {
		core.setRpcHealth("degraded", "host_disposed");
	} catch {
		// The state file remains the final observable signal when the RPC listener is already gone.
	}
	try {
		await writeHealthFile(core.stateDir, { status: "unhealthy", state: "degraded", reason: "host_disposed" });
	} catch (error) {
		console.error(`Could not write degraded health file: ${error instanceof Error ? error.message : String(error)}`);
	}
}

async function enterFailedClosed(
	core: WayCoreHandle,
	state: GatewayStateStore,
	config: WayConfig,
	reason: string,
	persist = true,
	linger = true,
): Promise<never> {
	if (persist) {
		try {
			const durable = state.read();
			if (durable.bootstrapState !== "FAILED_CLOSED" || durable.failedClosedReason !== reason) {
				// Folds the alert into the same commit as the state change.
				state.markFailedClosed(reason);
			}
		} catch {
			// Health observability and exit 78 remain mandatory even when a secondary
			// metadata write is unavailable.
		}
	}
	try {
		// Runs regardless of `persist`: the startup recovery path passes
		// persist=false, and that is precisely the restart in which a durable
		// failed-closed state may carry no raised alert.
		state.catchUpFailedClosedAlert();
	} catch {
		// Best-effort: the alert must never prevent the fail-closed exit itself.
	}
	try {
		core.resetMainSessionStatus();
	} catch {
		// Failed-closed state must still be published if the RPC listener is already unavailable.
	}

	const payload = { status: "unhealthy", state: "failed_closed", reason };
	try {
		await writeHealthFile(config.stateDir, payload);
	} catch (error) {
		console.error(
			`Could not write failed-closed health file: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	try {
		core.setRpcHealth("failed_closed", reason);
	} catch {
		// The process can still terminate safely if the listener was already lost.
	}
	try {
		core.sdNotifyStatus(`gajaeway failed_closed: ${reason}`);
	} catch {
		// NOTIFY_SOCKET is optional and status notification is best effort.
	}
	if (linger) await Bun.sleep(config.failClosedLingerMs);
	try {
		core.shutdownRpcServer();
	} catch {
		// Shutdown is idempotent from the process' point of view.
	}
	throw new FailedClosedExit(!linger);
}

function profileBridgeHandler(
	state: GatewayStateStore,
	config: WayConfig,
	profiles: ProfileRevisionTracker,
): RpcBridgeHandler {
	return async (method, params) => {
		if (method !== "profile.approve") throw new RpcBridgeException(-32601, `method not found: ${method}`);
		if (!isRecord(params) || params.confirm !== true || Object.keys(params).some((key) => key !== "confirm")) {
			throw new RpcBridgeException(-32602, "profile.approve requires { confirm: true }.");
		}
		try {
			const profile = profiles.load(config.profilePath);
			const result = approveProfile(state, profile, true);
			return {
				receipt_id: result.receiptId,
				approved_at: result.approvedAt,
				cursor: result.cursor,
				previous_digest: result.previousDigest,
				next_digest: result.nextDigest,
				changes: result.changes,
			};
		} catch (error) {
			throw new RpcBridgeException(
				error instanceof Error && error.name === "ProfileApprovalError" ? -32602 : -32603,
				error instanceof Error ? error.message : String(error),
			);
		}
	};
}
const CORPUS_CLOSURE_IDEMPOTENCY_SCOPE = "main.corpus.close";
const CLOSURE_OPERATION_META_KEY = "gitlock_closure_operation";
const CLOSURE_RECOVERY_META_KEY = "gitlock_closure_recovery";
const CLOSURE_OPERATION_LABEL_SEPARATOR = ":operation:";

type ClosureOperationState = "intent" | "acquired" | "pulled" | "staged" | "committed" | "pushed";

interface CorpusClosureRequest {
	readonly paths: readonly string[];
	readonly commitMessage: string;
	readonly idempotencyKey: string;
	readonly requestJson: string;
	readonly requestHash: string;
}

interface CorpusClosureResponse {
	readonly lease_id: string;
	readonly fencing_token: string;
	readonly committed: boolean;
}

interface InFlightCorpusClosure {
	readonly requestJson: string;
	readonly response: Promise<CorpusClosureResponse>;
}

type MutationReadinessReason = ClosureMutationReadinessReason;

class ClosureRecoveryReadinessRefusal extends Error {
	readonly reason: string;

	constructor(reason: string) {
		super(`Pending corpus closure recovery is fenced: ${reason}`);
		this.name = "ClosureRecoveryReadinessRefusal";
		this.reason = reason;
	}
}

interface ClosureOperationIntent {
	readonly version: 1;
	readonly operationId: string;
	readonly idempotencyKey: string;
	readonly requestHash: string;
	readonly requestJson: string;
	readonly corpusPath: string;
	readonly sessionId: string;
	readonly paths: readonly string[];
	readonly commitMessage: string;
}

interface ClosureOperationEvidence {
	/** The latest execution attempt's Git-lock receipt; atomically replaced after reacquisition. */
	leaseId?: string;
	fencingToken?: string;
	baseHead?: string;
	baseTree?: string;
	stagedTree?: string;
	commitHead?: string;
	committed?: boolean;
	recoveryMarker?: string;
}

interface ClosureOperationRecord {
	readonly version: 1;
	readonly intentJson: string;
	readonly state: ClosureOperationState;
	readonly evidence: ClosureOperationEvidence;
}

interface ClosureRecoveryEvidence {
	readonly sessionId: string;
	readonly corpusPath: string;
	readonly baseHead: string;
	readonly baseTree: string;
	readonly stagedTree: string;
}

function sha256(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

/**
 * `execve` cannot carry a NUL byte, and JavaScript lone surrogate code units
 * would be rewritten during UTF-8 argument encoding. Reject both before any
 * closure intent is made durable so no impossible Git invocation can wedge
 * recovery.
 */
function isExecveSafeString(value: string): boolean {
	if (value.includes("\0")) return false;
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (next < 0xdc00 || next > 0xdfff) return false;
			index += 1;
			continue;
		}
		if (code >= 0xdc00 && code <= 0xdfff) return false;
	}
	return true;
}

function requireExecveSafeClosureString(value: string, field: string): string {
	if (!isExecveSafeString(value)) {
		throw new RpcBridgeException(-32602, `main.corpus.close ${field} must be an execve-safe string.`);
	}
	return value;
}

function normalizeCorpusClosurePaths(corpusPath: string, candidates: readonly string[]): string[] {
	const root = path.resolve(requireExecveSafeClosureString(corpusPath, "corpus_path"));
	const paths = candidates.map((candidate) => {
		const argvSafeCandidate = requireExecveSafeClosureString(candidate, "paths[]");
		if (!argvSafeCandidate || path.isAbsolute(argvSafeCandidate) || argvSafeCandidate.startsWith(":")) {
			throw new RpcBridgeException(-32602, "main.corpus.close paths must be literal corpus-relative paths.");
		}
		const absolute = path.resolve(root, argvSafeCandidate);
		const relative = path.relative(root, absolute);
		if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`)) {
			throw new RpcBridgeException(
				-32602,
				"main.corpus.close paths must remain inside the corpus and cannot name the corpus root.",
			);
		}
		return relative.split(path.sep).join("/");
	});
	if (new Set(paths).size !== paths.length) {
		throw new RpcBridgeException(-32602, "main.corpus.close paths must not contain duplicates.");
	}
	return paths;
}

function parseCorpusClosureRequest(params: unknown, corpusPath: string): CorpusClosureRequest {
	if (!isRecord(params)) throw new RpcBridgeException(-32602, "main.corpus.close params must be an object.");
	const allowed = new Set(["paths", "commit_message", "idempotency_key"]);
	for (const key of Object.keys(params)) {
		if (!allowed.has(key)) throw new RpcBridgeException(-32602, `unknown parameter: ${key}`);
	}
	if (
		!Array.isArray(params.paths) ||
		params.paths.length === 0 ||
		params.paths.some((value) => typeof value !== "string" || value.length === 0)
	) {
		throw new RpcBridgeException(-32602, "main.corpus.close paths must be a non-empty array of non-empty strings.");
	}
	if (typeof params.commit_message !== "string" || params.commit_message.trim().length === 0) {
		throw new RpcBridgeException(-32602, "main.corpus.close commit_message must be a non-empty string.");
	}
	if (typeof params.idempotency_key !== "string" || params.idempotency_key.trim().length === 0) {
		throw new RpcBridgeException(-32602, "main.corpus.close idempotency_key must be a non-empty string.");
	}
	const paths = normalizeCorpusClosurePaths(corpusPath, params.paths as string[]);
	const commitMessage = requireExecveSafeClosureString(params.commit_message, "commit_message");
	const idempotencyKey = requireExecveSafeClosureString(params.idempotency_key, "idempotency_key");
	const requestJson = canonicalJson({ commit_message: commitMessage, idempotency_key: idempotencyKey, paths });
	return { paths, commitMessage, idempotencyKey, requestJson, requestHash: sha256(requestJson) };
}

function closureIdempotencyFailure(error: unknown): never {
	if (error instanceof RpcBridgeException) throw error;
	const message = error instanceof Error ? error.message : String(error);
	if (message.includes("1500 ") || message.includes("idempotency conflict")) {
		throw new RpcBridgeException(1500, "idempotency_conflict");
	}
	throw new RpcBridgeException(-32603, `main.corpus.close idempotency failed: ${message}`);
}

function replayCorpusClosureResponse(responseJson: string | undefined): CorpusClosureResponse {
	if (!responseJson) throw new RpcBridgeException(-32603, "Stored main.corpus.close replay has no response.");
	let response: unknown;
	try {
		response = JSON.parse(responseJson);
	} catch {
		throw new RpcBridgeException(-32603, "Stored main.corpus.close replay is invalid JSON.");
	}
	if (
		!isRecord(response) ||
		typeof response.lease_id !== "string" ||
		typeof response.fencing_token !== "string" ||
		typeof response.committed !== "boolean"
	) {
		throw new RpcBridgeException(-32603, "Stored main.corpus.close replay has an invalid shape.");
	}
	return {
		lease_id: response.lease_id,
		fencing_token: response.fencing_token,
		committed: response.committed,
	};
}

function isClosureOperationState(value: unknown): value is ClosureOperationState {
	return (
		value === "intent" ||
		value === "acquired" ||
		value === "pulled" ||
		value === "staged" ||
		value === "committed" ||
		value === "pushed"
	);
}

function parseClosureOperationIntent(value: unknown): ClosureOperationIntent {
	if (!isRecord(value) || value.version !== 1) throw new Error("closure operation intent has an invalid version");
	if (
		typeof value.operationId !== "string" ||
		typeof value.idempotencyKey !== "string" ||
		typeof value.requestHash !== "string" ||
		typeof value.requestJson !== "string" ||
		typeof value.corpusPath !== "string" ||
		typeof value.sessionId !== "string" ||
		typeof value.commitMessage !== "string" ||
		!Array.isArray(value.paths) ||
		value.paths.some((path) => typeof path !== "string")
	) {
		throw new Error("closure operation intent has an invalid shape");
	}
	if (sha256(value.requestJson) !== value.requestHash)
		throw new Error("closure operation intent request hash does not match");
	return {
		version: 1,
		operationId: value.operationId,
		idempotencyKey: value.idempotencyKey,
		requestHash: value.requestHash,
		requestJson: value.requestJson,
		corpusPath: value.corpusPath,
		sessionId: value.sessionId,
		paths: value.paths,
		commitMessage: value.commitMessage,
	};
}

function parseClosureOperationRecord(raw: string): ClosureOperationRecord {
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch {
		throw new Error("closure operation record is invalid JSON");
	}
	if (
		!isRecord(value) ||
		value.version !== 1 ||
		typeof value.intentJson !== "string" ||
		!isClosureOperationState(value.state)
	) {
		throw new Error("closure operation record has an invalid shape");
	}
	const evidence = value.evidence;
	if (!isRecord(evidence)) throw new Error("closure operation record evidence is invalid");
	for (const key of [
		"leaseId",
		"fencingToken",
		"recoveryMarker",
		"baseHead",
		"baseTree",
		"stagedTree",
		"commitHead",
	] as const) {
		if (evidence[key] !== undefined && typeof evidence[key] !== "string") {
			throw new Error(`closure operation evidence ${key} is invalid`);
		}
	}
	if (evidence.committed !== undefined && typeof evidence.committed !== "boolean") {
		throw new Error("closure operation evidence committed is invalid");
	}
	const parsedEvidence: ClosureOperationEvidence = {};
	if (typeof evidence.leaseId === "string") parsedEvidence.leaseId = evidence.leaseId;
	if (typeof evidence.fencingToken === "string") parsedEvidence.fencingToken = evidence.fencingToken;
	if (typeof evidence.recoveryMarker === "string") parsedEvidence.recoveryMarker = evidence.recoveryMarker;
	if (typeof evidence.baseHead === "string") parsedEvidence.baseHead = evidence.baseHead;
	if (typeof evidence.baseTree === "string") parsedEvidence.baseTree = evidence.baseTree;
	if (typeof evidence.stagedTree === "string") parsedEvidence.stagedTree = evidence.stagedTree;
	if (typeof evidence.commitHead === "string") parsedEvidence.commitHead = evidence.commitHead;
	if (typeof evidence.committed === "boolean") parsedEvidence.committed = evidence.committed;
	return { version: 1, intentJson: value.intentJson, state: value.state, evidence: parsedEvidence };
}

function parseClosureRecoveryEvidence(raw: string): ClosureRecoveryEvidence {
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch {
		throw new Error("closure recovery marker is invalid JSON");
	}
	if (
		!isRecord(value) ||
		typeof value.sessionId !== "string" ||
		typeof value.corpusPath !== "string" ||
		typeof value.baseHead !== "string" ||
		typeof value.baseTree !== "string" ||
		typeof value.stagedTree !== "string"
	) {
		throw new Error("closure recovery marker has an invalid shape");
	}
	return {
		sessionId: value.sessionId,
		corpusPath: value.corpusPath,
		baseHead: value.baseHead,
		baseTree: value.baseTree,
		stagedTree: value.stagedTree,
	};
}

function createClosureOperationIntent(
	request: CorpusClosureRequest,
	corpusPath: string,
	sessionId: string,
): { intent: ClosureOperationIntent; intentJson: string; operationJson: string } {
	const intent: ClosureOperationIntent = {
		version: 1,
		operationId: randomUUID(),
		idempotencyKey: request.idempotencyKey,
		requestHash: request.requestHash,
		requestJson: request.requestJson,
		corpusPath: path.resolve(corpusPath),
		sessionId,
		paths: request.paths,
		commitMessage: request.commitMessage,
	};
	const intentJson = canonicalJson(intent);
	return {
		intent,
		intentJson,
		operationJson: canonicalJson({ version: 1, intentJson, state: "intent", evidence: {} }),
	};
}

function closureOperationLabel(sessionId: string, operationId: string): string {
	return `main-session-closure:${sessionId}${CLOSURE_OPERATION_LABEL_SEPARATOR}${operationId}`;
}

function operationIdFromContext(context: ClosureHookContext): string | undefined {
	const offset = context.request.label.lastIndexOf(CLOSURE_OPERATION_LABEL_SEPARATOR);
	if (offset < 0) return undefined;
	const operationId = context.request.label.slice(offset + CLOSURE_OPERATION_LABEL_SEPARATOR.length);
	return operationId || undefined;
}

function readClosureOperation(
	core: WayCoreHandle,
): { raw: string; record: ClosureOperationRecord; intent: ClosureOperationIntent } | undefined {
	const value = core.gatewayMetaRead([CLOSURE_OPERATION_META_KEY]).entries[0]?.value;
	if (!value) return undefined;
	const record = parseClosureOperationRecord(value);
	return { raw: value, record, intent: parseClosureOperationIntent(JSON.parse(record.intentJson)) };
}

function stateRank(state: ClosureOperationState): number {
	return ["intent", "acquired", "pulled", "staged", "committed", "pushed"].indexOf(state);
}

interface AdvanceClosureOperationOptions {
	/** A recovered execution attempt replaces only its active Git-lock receipt. */
	readonly replaceExecutionLease?: boolean;
}

function advanceClosureOperation(
	core: WayCoreHandle,
	operationId: string,
	state: ClosureOperationState,
	evidence: Partial<ClosureOperationEvidence> = {},
	options: AdvanceClosureOperationOptions = {},
): ClosureOperationRecord | undefined {
	const current = readClosureOperation(core);
	if (!current || current.intent.operationId !== operationId) return undefined;
	// Lease evidence is attempt-scoped and replaces atomically on every acquisition.
	// State and commit graph evidence remain monotonic and write-once.
	const nextEvidence: ClosureOperationEvidence = { ...current.record.evidence };
	for (const [key, value] of Object.entries(evidence) as Array<
		[keyof ClosureOperationEvidence, ClosureOperationEvidence[keyof ClosureOperationEvidence]]
	>) {
		const replaceExecutionLease = options.replaceExecutionLease && (key === "leaseId" || key === "fencingToken");
		if (value !== undefined && (replaceExecutionLease || nextEvidence[key] === undefined))
			Object.assign(nextEvidence, { [key]: value });
	}
	const next: ClosureOperationRecord = {
		version: 1,
		intentJson: current.record.intentJson,
		state: stateRank(state) > stateRank(current.record.state) ? state : current.record.state,
		evidence: nextEvidence,
	};
	const nextRaw = canonicalJson(next);
	const updated = core.gatewayMetaTransaction({
		expected: [{ key: CLOSURE_OPERATION_META_KEY, value: current.raw }],
		puts: [{ key: CLOSURE_OPERATION_META_KEY, value: nextRaw }],
		deletes: [],
	});
	if (!updated.applied) throw new ClosureError("closure operation intent changed concurrently");
	return next;
}

async function gitOutput(corpusPath: string, args: readonly string[]): Promise<string> {
	const process = Bun.spawn({ cmd: ["git", "-C", corpusPath, ...args], stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([
		process.exited,
		new Response(process.stdout).text(),
		new Response(process.stderr).text(),
	]);
	if (exitCode !== 0)
		throw new ClosureError(
			`git ${args[0] ?? "command"} failed: ${stderr.trim() || stdout.trim() || "unknown Git failure"}`,
		);
	return stdout.trim();
}

async function pauseClosureHookForE2e(
	operationId: string,
	step: "after_acquire" | "after_commit",
	markerPath: string | undefined,
	releasePath: string | undefined,
): Promise<void> {
	if (Bun.env.NODE_ENV !== "test" || !markerPath || !releasePath) return;
	fs.writeFileSync(markerPath, `${JSON.stringify({ operation_id: operationId, step })}\n`, {
		encoding: "utf8",
		mode: 0o600,
	});
	while (!fs.existsSync(releasePath)) await Bun.sleep(10);
}

async function pauseAfterPushedClosure(operationId: string, evidence: ClosureOperationEvidence): Promise<void> {
	if (Bun.env.NODE_ENV !== "test") return;
	const markerPath = Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_PUSH_MARKER;
	const releasePath = Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_PUSH_RELEASE;
	if (!markerPath || !releasePath) return;
	if (!evidence.leaseId || !evidence.fencingToken || evidence.committed === undefined) {
		throw new ClosureError("post-push closure evidence is incomplete");
	}
	fs.writeFileSync(
		markerPath,
		`${JSON.stringify({
			operation_id: operationId,
			response: {
				lease_id: evidence.leaseId,
				fencing_token: evidence.fencingToken,
				committed: evidence.committed,
			},
		})}\n`,
		{ encoding: "utf8", mode: 0o600 },
	);
	while (!fs.existsSync(releasePath)) await Bun.sleep(10);
}

function createClosureHooks(core: WayCoreHandle): NonNullable<Parameters<typeof createClosureExecutor>[0]["hooks"]> {
	return {
		after_acquire: async (context) => {
			const operationId = operationIdFromContext(context);
			if (!operationId) return;
			advanceClosureOperation(
				core,
				operationId,
				"acquired",
				{ leaseId: context.leaseId, fencingToken: context.fencingToken },
				{ replaceExecutionLease: true },
			);
			await pauseClosureHookForE2e(
				operationId,
				"after_acquire",
				Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_MARKER,
				Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_RELEASE,
			);
		},
		after_pull: async (context) => {
			const operationId = operationIdFromContext(context);
			if (!operationId) return;
			advanceClosureOperation(core, operationId, "pulled", {
				baseHead: await gitOutput(context.request.corpusPath, ["rev-parse", "HEAD"]),
				baseTree: await gitOutput(context.request.corpusPath, ["rev-parse", "HEAD^{tree}"]),
			});
		},
		after_stage: async (context) => {
			const operationId = operationIdFromContext(context);
			if (!operationId) return;
			const rawMarker = core.gatewayMetaRead([CLOSURE_RECOVERY_META_KEY]).entries[0]?.value;
			if (!rawMarker) throw new ClosureError("closure operation is missing its recovery marker");
			const marker = parseClosureRecoveryEvidence(rawMarker);
			advanceClosureOperation(core, operationId, "staged", {
				recoveryMarker: rawMarker,
				baseHead: marker.baseHead,
				baseTree: marker.baseTree,
				stagedTree: marker.stagedTree,
			});
		},
		after_commit: async (context) => {
			const operationId = operationIdFromContext(context);
			if (!operationId) return;
			const current = readClosureOperation(core);
			if (!current || current.intent.operationId !== operationId) return;
			const head = await gitOutput(context.request.corpusPath, ["rev-parse", "HEAD"]);
			const evidence = current.record.evidence;
			const committed =
				evidence.baseHead !== undefined &&
				evidence.stagedTree !== undefined &&
				head !== evidence.baseHead &&
				(await gitOutput(context.request.corpusPath, ["rev-parse", "HEAD^"])) === evidence.baseHead &&
				(await gitOutput(context.request.corpusPath, ["rev-parse", "HEAD^{tree}"])) === evidence.stagedTree;
			advanceClosureOperation(core, operationId, "committed", { commitHead: head, committed });
			await pauseClosureHookForE2e(
				operationId,
				"after_commit",
				Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_COMMIT_MARKER,
				Bun.env.GAJAEWAY_E2E_CLOSURE_AFTER_COMMIT_RELEASE,
			);
		},
		after_push: async (context) => {
			const operationId = operationIdFromContext(context);
			if (!operationId) return;
			const operation = advanceClosureOperation(core, operationId, "pushed");
			if (!operation) return;
			await pauseAfterPushedClosure(operationId, operation.evidence);
		},
	};
}

async function verifyRecoveredClosureEffect(
	intent: ClosureOperationIntent,
	evidence: ClosureOperationEvidence,
): Promise<void> {
	if (evidence.committed !== true) return;
	if (
		!evidence.baseHead ||
		!evidence.baseTree ||
		!evidence.stagedTree ||
		!evidence.commitHead ||
		!evidence.recoveryMarker
	) {
		throw new ClosureError("committed closure recovery evidence is incomplete");
	}
	const marker = parseClosureRecoveryEvidence(evidence.recoveryMarker);
	if (
		marker.sessionId !== intent.sessionId ||
		marker.corpusPath !== intent.corpusPath ||
		marker.baseHead !== evidence.baseHead ||
		marker.baseTree !== evidence.baseTree ||
		marker.stagedTree !== evidence.stagedTree
	) {
		throw new ClosureError("closure operation intent is not bound to its recovery marker");
	}
	const [head, parent, tree, baseTree] = await Promise.all([
		gitOutput(intent.corpusPath, ["rev-parse", "HEAD"]),
		gitOutput(intent.corpusPath, ["rev-parse", "HEAD^"]),
		gitOutput(intent.corpusPath, ["rev-parse", "HEAD^{tree}"]),
		gitOutput(intent.corpusPath, ["rev-parse", `${evidence.baseHead}^{tree}`]),
	]);
	if (
		head !== evidence.commitHead ||
		parent !== evidence.baseHead ||
		tree !== evidence.stagedTree ||
		baseTree !== evidence.baseTree
	) {
		throw new ClosureError("closure recovery evidence does not match the Git commit graph");
	}
	const branch = await gitOutput(intent.corpusPath, ["symbolic-ref", "--quiet", "--short", "HEAD"]);
	const remote = await gitOutput(intent.corpusPath, ["config", "--get", `branch.${branch}.remote`]);
	const mergeRef = await gitOutput(intent.corpusPath, ["config", "--get", `branch.${branch}.merge`]);
	const remoteHead = (
		await gitOutput(intent.corpusPath, ["ls-remote", "--exit-code", "--heads", remote, mergeRef])
	).split(/\s+/)[0];
	if (remoteHead !== evidence.commitHead)
		throw new ClosureError("closure recovery evidence is not the remote branch head");
}

function closureResponseForOperation(
	record: ClosureOperationRecord,
	closure: Awaited<ReturnType<ClosureExecutor["execute"]>>,
): CorpusClosureResponse {
	const evidence = record.evidence;
	if (evidence.leaseId && evidence.fencingToken && evidence.committed !== undefined) {
		return { lease_id: evidence.leaseId, fencing_token: evidence.fencingToken, committed: evidence.committed };
	}
	return { lease_id: closure.leaseId, fencing_token: closure.fencingToken, committed: closure.committed };
}

async function executeAndFinalizeClosureOperation(
	core: WayCoreHandle,
	closures: ClosureExecutor,
	intent: ClosureOperationIntent,
	intentJson: string,
	mutationReadinessReason: MutationReadinessReason,
): Promise<CorpusClosureResponse> {
	let closure: Awaited<ReturnType<ClosureExecutor["execute"]>>;
	try {
		closure = await closures.execute({
			sessionId: intent.sessionId,
			corpusPath: intent.corpusPath,
			label: closureOperationLabel(intent.sessionId, intent.operationId),
			class: "batch",
			paths: intent.paths,
			commitMessage: intent.commitMessage,
			mutationReadinessReason,
		});
	} catch (error) {
		closureExecutionFailure(error);
	}
	const operation = readClosureOperation(core);
	if (!operation || operation.intent.operationId !== intent.operationId || operation.record.intentJson !== intentJson) {
		throw new RpcBridgeException(-32603, "closure operation intent disappeared before finalization");
	}
	await verifyRecoveredClosureEffect(intent, operation.record.evidence);
	const response = closureResponseForOperation(operation.record, closure);
	let finalized;
	try {
		finalized = core.closureOperationFinalize({
			scope: CORPUS_CLOSURE_IDEMPOTENCY_SCOPE,
			key: intent.idempotencyKey,
			requestJson: intent.requestJson,
			intentJson,
			operationJson: operation.raw,
			responseJson: canonicalJson(response),
		});
	} catch (error) {
		closureIdempotencyFailure(error);
	}
	return replayCorpusClosureResponse(finalized?.responseJson);
}

async function reconcilePendingClosureOperation(
	core: WayCoreHandle,
	closures: ClosureExecutor,
	corpusPath: string,
	sessionId: string,
	mutationReadinessReason: MutationReadinessReason,
): Promise<void> {
	const operation = readClosureOperation(core);
	if (!operation) return;
	const intent = operation.intent;
	if (intent.corpusPath !== path.resolve(corpusPath) || intent.sessionId !== sessionId) {
		throw new Error("pending closure operation does not match the resumed main session");
	}
	const replay = core.idempotencyReplay({
		scope: CORPUS_CLOSURE_IDEMPOTENCY_SCOPE,
		key: intent.idempotencyKey,
		requestJson: intent.requestJson,
	});
	if (!replay.replayed || replay.responseJson !== operation.record.intentJson) {
		throw new Error("pending closure operation is not bound to its idempotency intent");
	}
	try {
		await executeAndFinalizeClosureOperation(
			core,
			closures,
			intent,
			operation.record.intentJson,
			mutationReadinessReason,
		);
	} catch (error) {
		if (error instanceof ClosureMutationReadinessError) {
			throw new ClosureRecoveryReadinessRefusal(error.reason);
		}
		throw error;
	}
}

function closureExecutionFailure(error: unknown): never {
	if (error instanceof ClosureMutationReadinessError || error instanceof RpcBridgeException) throw error;
	if (error instanceof ClosureError && error.code !== undefined) {
		throw new RpcBridgeException(error.code, error.message);
	}
	throw new RpcBridgeException(-32603, error instanceof Error ? error.message : String(error));
}

function closureRpcExecutionFailure(error: unknown): never {
	if (error instanceof ClosureMutationReadinessError) throw new RpcBridgeException(1003, error.reason);
	throw error;
}

function assertMutationReadiness(mutationReadinessReason: MutationReadinessReason): void {
	const reason = mutationReadinessReason();
	if (reason) throw new RpcBridgeException(1003, reason);
}

function assertMainSessionMutationReady(host: MainSessionHost): void {
	assertMutationReadiness(() => host.mutationReadinessReason);
}

function markClosureInFlightWaiterForE2e(idempotencyKey: string): void {
	if (Bun.env.NODE_ENV !== "test") return;
	const markerPath = Bun.env.GAJAEWAY_E2E_CLOSURE_INFLIGHT_WAITER_MARKER;
	if (!markerPath) return;
	fs.writeFileSync(markerPath, `${JSON.stringify({ idempotency_key: idempotencyKey })}\n`, {
		encoding: "utf8",
		mode: 0o600,
	});
}
function closureBridgeHandler(
	core: WayCoreHandle,
	closures: ClosureExecutor,
	corpusPath: string,
	sessionId: string,
	mutationReadinessReason: MutationReadinessReason,
): RpcBridgeHandler {
	const inFlightByKey = new Map<string, InFlightCorpusClosure>();
	return async (method, params) => {
		if (method !== "main.corpus.close") throw new RpcBridgeException(-32601, `method not found: ${method}`);
		assertMutationReadiness(mutationReadinessReason);
		const request = parseCorpusClosureRequest(params, corpusPath);
		const inFlight = inFlightByKey.get(request.idempotencyKey);
		if (inFlight) {
			if (inFlight.requestJson !== request.requestJson) throw new RpcBridgeException(1500, "idempotency_conflict");
			markClosureInFlightWaiterForE2e(request.idempotencyKey);
			try {
				return await inFlight.response;
			} catch (error) {
				closureRpcExecutionFailure(error);
			}
		}

		const created = createClosureOperationIntent(request, corpusPath, sessionId);
		let claim;
		try {
			claim = core.closureOperationClaim({
				scope: CORPUS_CLOSURE_IDEMPOTENCY_SCOPE,
				key: request.idempotencyKey,
				requestJson: request.requestJson,
				intentJson: created.intentJson,
				operationJson: created.operationJson,
			});
		} catch (error) {
			closureIdempotencyFailure(error);
		}
		if (!claim?.claimed) {
			const storedResponse = claim?.responseJson;
			try {
				return replayCorpusClosureResponse(storedResponse);
			} catch (error) {
				if (!(error instanceof RpcBridgeException) || error.code !== -32603) throw error;
			}
			const pendingIntent = parseClosureOperationIntent(JSON.parse(storedResponse ?? "null"));
			if (
				pendingIntent.idempotencyKey !== request.idempotencyKey ||
				pendingIntent.requestJson !== request.requestJson ||
				pendingIntent.requestHash !== request.requestHash
			) {
				throw new RpcBridgeException(1500, "idempotency_conflict");
			}
			const response = executeAndFinalizeClosureOperation(
				core,
				closures,
				pendingIntent,
				storedResponse as string,
				mutationReadinessReason,
			);
			inFlightByKey.set(request.idempotencyKey, { requestJson: request.requestJson, response });
			try {
				return await response;
			} catch (error) {
				closureRpcExecutionFailure(error);
			} finally {
				if (inFlightByKey.get(request.idempotencyKey)?.response === response)
					inFlightByKey.delete(request.idempotencyKey);
			}
		}

		const response = executeAndFinalizeClosureOperation(
			core,
			closures,
			created.intent,
			created.intentJson,
			mutationReadinessReason,
		);
		inFlightByKey.set(request.idempotencyKey, { requestJson: request.requestJson, response });
		try {
			return await response;
		} catch (error) {
			closureRpcExecutionFailure(error);
		} finally {
			if (inFlightByKey.get(request.idempotencyKey)?.response === response)
				inFlightByKey.delete(request.idempotencyKey);
		}
	};
}

async function disposeMainSession(core: WayCoreHandle, host: MainSessionHost): Promise<void> {
	try {
		await host.dispose();
	} finally {
		try {
			core.resetMainSessionStatus();
		} finally {
			if (!host.degraded) await publishHostUnavailableHealth(core);
		}
	}
}

type MainSessionStartupDisposalOutcome =
	| { readonly kind: "not_owned" | "disposed" }
	| { readonly kind: "failed"; readonly error: unknown };

function logMainSessionStartupDisposalFailure(error: unknown): void {
	const message = error instanceof Error ? error.message : String(error);
	const stack = error instanceof Error ? error.stack : undefined;
	console.error(`main session teardown failed before failed-closed shutdown: ${message}${stack ? `\n${stack}` : ""}`);
}

async function disposeMainSessionAfterStartupFailure(
	core: WayCoreHandle,
	host: MainSessionHost | undefined,
): Promise<MainSessionStartupDisposalOutcome> {
	try {
		if (!host) return { kind: "not_owned" };
		await disposeMainSession(core, host);
		return { kind: "disposed" };
	} catch (error) {
		return { kind: "failed", error };
	}
}

async function waitForShutdown(
	core: WayCoreHandle,
	host: MainSessionHost,
	closures: ClosureExecutor,
	reconciler: BrokerReconciler,
	closureRecovery: Promise<void>,
): Promise<void> {
	const closureRecoveryFailure = closureRecovery.then(
		() => new Promise<never>(() => {}),
		(error) => ({ kind: "closure_recovery_failed" as const, error }),
	);
	const outcome = await Promise.race([
		new Promise<{ readonly kind: "signal" }>((resolve) => {
			const stop = () => resolve({ kind: "signal" });
			process.once("SIGINT", stop);
			process.once("SIGTERM", stop);
		}),
		host.waitForFatalFailure().then((error) => ({ kind: "fatal" as const, error })),
		closureRecoveryFailure,
	]);
	if (outcome.kind === "fatal") throw outcome.error;
	if (outcome.kind === "closure_recovery_failed") {
		const detail = outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
		console.error(`Pending corpus closure recovery failed: ${detail}`);
		throw new MainSessionHostError("closure_recovery_failed", detail, { cause: outcome.error });
	}
	reconciler.stop();
	await closures.shutdown();
	await disposeMainSession(core, host);
	core.shutdownRpcServer();
}

async function serveWay(config: WayConfig): Promise<void> {
	const core = loadWayCore().WayCore.open(config.stateDir);
	const closures = createClosureExecutor({ core, hooks: createClosureHooks(core) });
	const state = new GatewayStateStore(core);
	const profiles = new ProfileRevisionTracker();
	const profileHandler = profileBridgeHandler(state, config, profiles);
	let mainSessionHandler: RpcBridgeHandler | undefined;
	const bridgeHandler: RpcBridgeHandler = async (method, params) => {
		if (method === "profile.approve") return await profileHandler(method, params);
		if (!mainSessionHandler) throw new RpcBridgeException(1301, "unknown_session");
		return await mainSessionHandler(method, params);
	};
	core.startRpcServer(path.join(config.stateDir, "rpc.sock"), createRpcBridge(core, bridgeHandler));
	core.setRpcHealth("verifying");
	try {
		core.sdNotifyStatus("gajaeway verifying durable external main session");
	} catch {
		// Status notification is optional.
	}
	let host: MainSessionHost | undefined;
	let reconciler: BrokerReconciler | undefined;
	let supervisor: ReturnType<typeof createRuntimeSupervisor> | undefined;
	try {
		const profile = profiles.load(config.profilePath);
		if (config.sessionId && profile.externalSessionId && config.sessionId !== profile.externalSessionId) {
			throw new ResumeError("session_id_conflict", "--session-id does not match [main_session].session_id.");
		}
		core.registryConfigureSurfaces(
			profile.knownSurfaces.map((surface) => ({
				surfaceId: surface.id,
				platform: surface.platform,
				kind: surface.kind,
				isOwnerSurface: profile.ownerSurfaces.some((owner) => owner.id === surface.id),
			})),
		);
		supervisor = createRuntimeSupervisor(config, profile.workspace);
		const recovery = await recoverBootstrap({ profile, state, supervisor });
		if (recovery.kind === "bootstrap_required") {
			await enterFailedClosed(core, state, config, "bootstrap_required", false);
		}
		if (recovery.kind === "failed_closed") await enterFailedClosed(core, state, config, recovery.reason, false);
		const resumed = await strictResumeMainSession({ profile, state, supervisor });
		await reconcilePendingMainAdmissions(core, supervisor);
		const initialAdmissionAttributions = parseMainSessionAdmissionAttributions(core.mainAdmissionAttributions());
		if (config.sessionId && config.sessionId !== resumed.identity.sessionId) {
			throw new ResumeError(
				"session_id_mismatch",
				"--session-id does not match the durable adopted external identity.",
			);
		}
		failBeforeMainHostForE2e();
		const verificationPending = resumed.verificationState === "pending";
		if (verificationPending) {
			core.setRpcHealth("verifying", "transcript_verification_pending");
			await writeHealthFile(config.stateDir, {
				status: "booting",
				state: "verifying",
				reason: "transcript_verification_pending",
			});
		} else {
			// Write the baseline before tail observation begins. A host may degrade
			// immediately; the later running state publication is monotonic in core.
			await writeHealthFile(config.stateDir, { status: "healthy", state: "running" });
		}
		const resumedHost = createMainSessionHost({
			supervisor,
			identity: resumed.identity,
			state,
			journal: createRuntimeMainSessionJournal(core, config.stateDir, state),
			initialTurnState: resumed.turnState,
			initialFollowUpQueueDepth: resumed.followUpQueueDepth,
			initialVerificationState: resumed.verificationState,
			...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
			...(resumed.growthIntent === undefined ? {} : { recoveredGrowthIntent: resumed.growthIntent }),
			initialAdmissionAttributions,
			afterTerminalEvidenceBeforeAdmissionFinalize: failAfterMainAdmissionTerminalEvidenceForE2e,
		});
		host = resumedHost;
		if (!verificationPending) core.setRpcHealth("running");
		const mutationReadinessReason: MutationReadinessReason = () => resumedHost.mutationReadinessReason;
		const { submitFromRpc: admissionHandler, admitSystemEvent } = createMainAdmissionHandler(host, profile, core, {
			isSurfaceQuarantined: (surface) => {
				try {
					return core.surfaceResolve(surface.id).quarantined;
				} catch {
					return true;
				}
			},
			afterBrokerAcceptedBeforeFinalize: failAfterMainAdmissionBrokerAcceptedForE2e,
			journalHeadCursor: () => core.journalHeadCursor(),
			mutationReadinessReason,
		});
		const gateAnswerHandler = createMainGateAnswerHandler(host, core);
		// The scheduler holds `admitSystemEvent` directly and never opens a UDS
		// client: routing scheduled work back through RPC would create a second
		// writer to the main session.
		const scheduler = createScheduler({
			core,
			profile,
			admitSystemEvent,
			submitFromRpc: admissionHandler,
			turnState: () => resumedHost.turnState,
			mutationReadinessReason: () => resumedHost.mutationReadinessReason,
			isCorpusQuarantined: () => {
				try {
					return core.lockStatus().quarantined;
				} catch {
					return true;
				}
			},
			isSurfaceQuarantined: (surfaceId) => {
				try {
					return core.surfaceResolve(surfaceId).quarantined;
				} catch {
					return true;
				}
			},
			onError: (error) => {
				process.stderr.write(`gajaeway scheduler error: ${error.message}\n`);
			},
		});
		const rawClosureHandler = closureBridgeHandler(
			core,
			closures,
			profile.corpusPath,
			resumed.identity.sessionId,
			mutationReadinessReason,
		);
		const closureRecovery = resumedHost
			.waitForVerifiedTranscript()
			.then(
				async () =>
					await reconcilePendingClosureOperation(
						core,
						closures,
						profile.corpusPath,
						resumed.identity.sessionId,
						mutationReadinessReason,
					),
			)
			.catch((error) => {
				if (error instanceof ClosureRecoveryReadinessRefusal) {
					console.error(`Pending corpus closure recovery refused: ${error.reason}`);
					return;
				}
				throw error;
			});
		const closureHandler: RpcBridgeHandler = async (method, params) => {
			await closureRecovery;
			return await rawClosureHandler(method, params);
		};
		mainSessionHandler = async (method, params) => {
			if (method === "main.submit" || method === "main.corpus.close") assertMainSessionMutationReady(resumedHost);
			if (method.startsWith("schedule.")) return await scheduler.handleRpc(method, params);
			if (method === "main.submit") return await admissionHandler(params);
			if (method === "main.gate.answer") return await gateAnswerHandler(params);
			if (method === "main.corpus.close") return await closureHandler(method, params);
			throw new RpcBridgeException(-32601, `method not found: ${method}`);
		};
		reconciler = new BrokerReconciler({
			core,
			broker: new BrokerCli({ executable: config.brokerCliPath }),
			pollMs: config.reconcilePollMs,
		});
		// Without FTS5 the redaction predicate cannot run inside the query, so a
		// degraded fallback would be a disclosure rather than a missing feature.
		// Fail closed on a named reason instead.
		if (!core.memoryFts5Available()) {
			await enterFailedClosed(core, state, config, "memory_fts5_unavailable");
		}

		// Build the recall index against the ACTIVE profile digest. Masks come
		// from the injector's own exported predicate, so the redaction rule has
		// exactly one implementation and Rust cannot drift from it.
		try {
			// One shared `{date}` expansion for the index and the masks, so a bit
			// index, an indexed document, and an injected file all mean the same
			// position.
			const indexedAt = new Date();
			const candidates = recallCandidates(profile, indexedAt);
			if (candidates.length > MAX_INJECTION_FILES) {
				// The mask cannot address past bit 63, and an unaddressable
				// document must never be indexed as permanently unmaskable.
				throw new Error(
					`expanded injection list has ${candidates.length} entries, exceeding the ${MAX_INJECTION_FILES}-bit restriction mask`,
				);
			}
			const documents = candidates.flatMap((file, fileIndex) => {
				try {
					return [{ fileIndex, path: file, body: fs.readFileSync(path.resolve(profile.corpusPath, file), "utf8") }];
				} catch (error) {
					// A missing daily file is an observable skip in the injector too;
					// it simply contributes no recall document at this position.
					if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
					// Any OTHER I/O failure must not silently produce a partial index
					// that then answers as if it were complete.
					throw error;
				}
			});
			core.memoryIndexRebuild(
				profile.digest.sha256,
				documents,
				SESSION_KINDS.map((sessionKind) => ({
					sessionKind,
					mask: restrictionMask(profile, sessionKind, indexedAt),
				})),
			);
		} catch (error) {
			// Fail closed rather than answering from an index that may not match
			// the corpus: clear the digest pin so memory.search returns 1801
			// instead of serving a stale or partial result set.
			process.stderr.write(
				`gajaeway memory recall index unavailable: ${error instanceof Error ? error.message : String(error)}\n`,
			);
			try {
				core.gatewayMetaTransaction({
					expected: [],
					puts: [{ key: "memory_index_digest", value: "unavailable" }],
					deletes: [],
				});
			} catch (clearError) {
				// If the pin cannot be cleared, a prior MATCHING pin plus stale FTS
				// rows would keep answering under a policy we can no longer vouch
				// for. That is a redaction risk, so refuse to serve at all.
				process.stderr.write(
					`gajaeway could not invalidate the recall index pin: ${clearError instanceof Error ? clearError.message : String(clearError)}\n`,
				);
				await enterFailedClosed(core, state, config, "memory_index_unverifiable");
			}
		}

		// Optional loopback Prometheus endpoint. Disabled unless the profile says
		// otherwise, because it is an unauthenticated surface: enabling it must be
		// an affirmative operator act, not a default.
		const metricsTunables = ((profile.tunables.tunables as Record<string, unknown> | undefined)?.metrics ??
			undefined) as Record<string, unknown> | undefined;
		const metricsHttpEnabled = metricsTunables?.http_enabled === true;
		const metricsPort = typeof metricsTunables?.port === "number" ? metricsTunables.port : 9_465;
		if (metricsHttpEnabled) {
			try {
				const bound = core.metricsHttpStart(metricsPort);
				process.stderr.write(`gajaeway metrics endpoint listening on 127.0.0.1:${bound}\n`);
			} catch (error) {
				// A refused bind is reported and non-fatal: telemetry must never
				// prevent the gateway from serving its actual purpose.
				process.stderr.write(
					`gajaeway metrics endpoint unavailable: ${error instanceof Error ? error.message : String(error)}\n`,
				);
			}
		}

		// Alert evaluation runs on a daemon-owned interval that is deliberately
		// independent of the metrics HTTP listener: an operator who leaves the
		// endpoint disabled must still be told when an adapter goes silent.
		const adapterDisconnectMs = 60_000;
		const alertTimer = setInterval(() => {
			try {
				core.alertsEvaluateAdapterDisconnects(adapterDisconnectMs);
			} catch {
				// Alert evaluation is best-effort per tick; the next tick retries and
				// the durable flag means nothing is lost.
			}
		}, 15_000);
		alertTimer.unref?.();

		// In-flight runs are resolved before any overdue collapse, so a collapse
		// cannot swallow or miscount an occurrence that already holds a claim.
		// This runs after reconcilePendingMainAdmissions so a still-pending
		// admission is owned by that recovery, not raced by the scheduler.
		await scheduler.reconcile();
		scheduler.start();
		reconciler.start();
		try {
			core.sdNotifyReady(verificationPending ? "gajaeway transcript verification pending" : "gajaeway running");
		} catch {
			// Readiness remains observable through RPC and health.json.
		}
		try {
			await waitForShutdown(core, host, closures, reconciler, closureRecovery);
		} finally {
			clearInterval(alertTimer);
			scheduler.stop();
			if (metricsHttpEnabled) {
				try {
					core.metricsHttpStop();
				} catch {
					// Shutdown must not be blocked by a telemetry listener.
				}
			}
		}
	} catch (error) {
		if (error instanceof FailedClosedExit) {
			const disposal = await disposeMainSessionAfterStartupFailure(core, host);
			if (disposal.kind === "failed") logMainSessionStartupDisposalFailure(disposal.error);
			await supervisor?.dispose();
			await closures.shutdown();
			reconciler?.stop();
			throw error;
		}
		const disposal = await disposeMainSessionAfterStartupFailure(core, host);
		if (disposal.kind === "failed") logMainSessionStartupDisposalFailure(disposal.error);
		await supervisor?.dispose();
		reconciler?.stop();
		await closures.shutdown();
		if (disposal.kind === "failed") await enterFailedClosed(core, state, config, failureReason(error), true, false);
		await enterFailedClosed(core, state, config, failureReason(error));
	}
}

function requireConfirm(arguments_: readonly string[], command: string): void {
	if (arguments_.length !== 2 || arguments_[0] !== command || arguments_[1] !== "--confirm") {
		throw new Error(`${command} requires --confirm.\n${usage}`);
	}
}

async function bootstrapCommand(config: WayConfig, arguments_: readonly string[]): Promise<void> {
	requireConfirm(arguments_, "bootstrap");
	const core = loadWayCore().WayCore.open(config.stateDir);
	const state = new GatewayStateStore(core);
	const profile = new ProfileRevisionTracker().load(config.profilePath);
	const supervisor = createRuntimeSupervisor(config, profile.workspace);
	try {
		const recovery = await recoverBootstrap({ profile, state, supervisor });
		if (recovery.kind === "failed_closed") throw new Error(`Bootstrap recovery is failed closed: ${recovery.reason}`);
		if (recovery.kind === "committed") {
			console.log(
				JSON.stringify({
					state: "committed",
					session_id: recovery.identity.sessionId,
					recovered: recovery.nonce !== "",
				}),
			);
			return;
		}
		const sessionId = config.sessionId ?? profile.externalSessionId;
		if (!sessionId) throw new Error(`bootstrap requires --session-id or [main_session].session_id.\n${usage}`);
		if (config.sessionId && profile.externalSessionId && config.sessionId !== profile.externalSessionId) {
			throw new Error("--session-id must match [main_session].session_id.");
		}
		const committed = await bootstrapMainSession({ confirm: true, profile, state, supervisor, sessionId });
		console.log(
			JSON.stringify({ state: "committed", session_id: committed.identity.sessionId, nonce: committed.nonce }),
		);
	} finally {
		await supervisor.dispose();
	}
}

interface RpcResponse {
	readonly result?: unknown;
	readonly error?: { readonly code: number; readonly message: string; readonly data?: unknown };
}

async function requestOwnerRpc(socketPath: string, method: string, params: unknown): Promise<unknown> {
	return await new Promise<unknown>((resolve, reject) => {
		const socket = net.createConnection(socketPath);
		let buffer = "";
		let settled = false;
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			socket.destroy();
			callback();
		};
		const timeout = setTimeout(() => finish(() => reject(new Error("Timed out waiting for owner RPC."))), 5_000);
		socket.setEncoding("utf8");
		socket.once("error", (error) => finish(() => reject(error)));
		socket.on("data", (chunk) => {
			buffer += String(chunk);
			const newline = buffer.indexOf("\n");
			if (newline < 0) return;
			try {
				const response = JSON.parse(buffer.slice(0, newline)) as RpcResponse;
				if (response.error) throw new Error(`${response.error.code} ${response.error.message}`);
				finish(() => resolve(response.result));
			} catch (error) {
				finish(() => reject(error));
			}
		});
		socket.once("connect", () => {
			socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: "profile-approve", method, params })}\n`);
		});
	});
}

async function profileApproveCommand(config: WayConfig, arguments_: readonly string[]): Promise<void> {
	if (
		arguments_.length !== 3 ||
		arguments_[0] !== "profile" ||
		arguments_[1] !== "approve" ||
		arguments_[2] !== "--confirm"
	) {
		throw new Error(`profile approve requires --confirm.\n${usage}`);
	}
	const socketPath = path.join(config.stateDir, "rpc.sock");
	if (fs.existsSync(socketPath)) {
		const result = await requestOwnerRpc(socketPath, "profile.approve", { confirm: true });
		console.log(JSON.stringify(result, null, 2));
		return;
	}
	const core = loadWayCore().WayCore.open(config.stateDir);
	const state = new GatewayStateStore(core);
	const profile = new ProfileRevisionTracker().load(config.profilePath);
	const preview = previewProfileApproval(state, profile);
	console.log(
		JSON.stringify(
			{ previous_digest: preview.previousDigest, next_digest: preview.nextDigest, changes: preview.changes },
			null,
			2,
		),
	);
	const result = approveProfile(state, profile, true);
	console.log(
		JSON.stringify({ receipt_id: result.receiptId, approved_at: result.approvedAt, cursor: result.cursor }, null, 2),
	);
}

/** CLI entrypoint. The daemon never calls bootstrap; it only strict-resumes a committed identity. */
export async function runWay(arguments_ = process.argv.slice(2)): Promise<void> {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return;
	}
	if (arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`gajaeway ${loadWayCore().healthInfo().version}`);
		return;
	}
	const parsed = parseWayConfig(arguments_);
	if (parsed.remaining.length === 1 && parsed.remaining[0] === "--health") {
		const payload = await healthPayload(parsed.config.stateDir);
		console.log(JSON.stringify(payload));
		if (payload.status !== "healthy") process.exitCode = 1;
		return;
	}
	if (parsed.remaining.length === 0 || (parsed.remaining.length === 1 && parsed.remaining[0] === "serve")) {
		await serveWay(parsed.config);
		return;
	}
	if (parsed.remaining[0] === "console") {
		await runWayConsole(parsed.config, parsed.remaining.slice(1));
		return;
	}
	if (parsed.remaining[0] === "bootstrap") {
		await bootstrapCommand(parsed.config, parsed.remaining);
		return;
	}
	if (parsed.remaining[0] === "mcp") {
		if (parsed.remaining[1] !== "serve") throw new Error(`Unknown command: ${parsed.remaining.join(" ")}\n${usage}`);
		// A subcommand of the existing binary: no new compile target, and the
		// same UDS peer-credential gate as every other operator entry point.
		await runMcpServer({
			stateDir: parsed.config.stateDir,
			profilePath: parsed.config.profilePath,
			serverVersion: loadWayCore().healthInfo().version,
		});
		return;
	}
	if (parsed.remaining[0] === "profile") {
		await profileApproveCommand(parsed.config, parsed.remaining);
		return;
	}
	throw new Error(`Unknown command: ${parsed.remaining.join(" ")}\n${usage}`);
}

if (import.meta.main && process.env.GAJAEWAY_INTERNAL_CLOSURE_WORKER === "1") {
	await runClosureWorker();
} else if (import.meta.main) {
	try {
		await runWay();
	} catch (error) {
		if (error instanceof FailedClosedExit) {
			if (error.forceExit) process.exit(78);
			process.exitCode = 78;
		} else if (error instanceof ConsoleStartupRefusalError) {
			process.exitCode = 1;
		} else {
			console.error(sanitizeConsoleText(error instanceof Error ? error.message : String(error)));
			process.exitCode = 1;
		}
	}
}
