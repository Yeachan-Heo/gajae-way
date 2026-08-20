import { spawn } from "node:child_process";

/** The published `gjc sdk session` row DTO version. */
export const SESSION_ROWS_VERSION = 1;
const MAX_CAPTURED_OUTPUT_BYTES = 8 * 1024 * 1024;
const DEFAULT_COMMAND_TIMEOUT_MS = 10_000;

export type SdkSessionActivityState = "active" | "idle";

export interface SdkSessionRowV1 {
	readonly sessionId: string;
	readonly locator: { readonly repo: string; readonly stateRoot: string };
	readonly endpointGeneration: number;
	readonly pid: number;
	readonly live: boolean;
	readonly deleted: boolean;
	readonly indexSeq: number;
	readonly hostIncarnation?: string;
	readonly terminalUncertain?: boolean;
	readonly lifecycleRequestId?: string;
	readonly endpointMtimeMs?: number;
	readonly activity?: { readonly state: SdkSessionActivityState; readonly at: number };
	readonly lastHeartbeatAt?: number;
	readonly identityProvenance?: "composite" | "legacy";
	readonly ambiguous?: boolean;
}

export interface SdkSessionRowsV1 {
	readonly version: typeof SESSION_ROWS_VERSION;
	readonly indexSeq?: number;
	readonly sessions: readonly SdkSessionRowV1[];
}

export interface SessionMetadataV1 {
	readonly sessionId: string;
	readonly name?: string;
	readonly cwd: string;
	readonly kind: string;
}

export interface SdkCheckpointRecordV1 {
	readonly revision: number;
	readonly generation: number;
	readonly seq: number;
}

export interface SdkRetentionGapV1 {
	readonly code: "retention_gap";
	readonly missing?: { readonly from: number; readonly to: number };
	readonly resync?: SdkCheckpointRecordV1;
}

export interface SdkTailItemV1 {
	readonly kind: string;
	readonly id?: string;
	readonly generation?: number;
	readonly seq?: number;
	readonly payload: unknown;
}

export interface SdkTailEnvelopeV1 {
	readonly version: typeof SESSION_ROWS_VERSION;
	readonly source: "session" | "offline";
	readonly session: SdkSessionRowV1;
	readonly checkpoint?: SdkCheckpointRecordV1;
	readonly gap?: SdkRetentionGapV1;
	readonly items: readonly SdkTailItemV1[];
	readonly terminal?: boolean;
}

export type BrokerTurnOperation = "turn.prompt" | "turn.steer" | "turn.follow_up";

/** A broker acknowledgement means the external host accepted this operation, not that its turn completed. */
export interface BrokerOperationReceipt {
	readonly sessionId: string;
	readonly operation: BrokerTurnOperation;
	readonly operationRef: string;
	readonly commandId?: string;
	readonly turnId?: string;
}

export interface BrokerTurnStatus {
	readonly operationRef: string;
	readonly status: string;
	readonly completed: boolean;
	readonly detail: unknown;
}

export class BrokerCliError extends Error {
	readonly code: string;
	readonly stderr?: string;

	constructor(code: string, message: string, options: { readonly stderr?: string; readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "BrokerCliError";
		this.code = code;
		this.stderr = options.stderr;
	}
}

/** A version/schema mismatch at the broker CLI boundary. */
export class BrokerDtoParseError extends BrokerCliError {
	readonly path: string;

	constructor(path: string, message: string) {
		super("broker_dto_drift", `Invalid broker CLI DTO at ${path}: ${message}`);
		this.name = "BrokerDtoParseError";
		this.path = path;
	}
}

export class BrokerMetadataUnavailableError extends BrokerCliError {
	constructor(message: string, options: { readonly stderr?: string; readonly cause?: unknown } = {}) {
		super("metadata_unavailable", message, options);
		this.name = "BrokerMetadataUnavailableError";
	}
}

export interface BrokerCliOptions {
	/** Path override used by deterministic fixtures; production defaults to `gjc`. */
	readonly executable?: string;
	readonly commandTimeoutMs?: number;
	/** Explicit inherited environment for deterministic broker processes. */
	readonly environment?: NodeJS.ProcessEnv;
}

export interface BrokerCommand {
	readonly args: readonly string[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseJson(stdout: string, path: string): unknown {
	try {
		return JSON.parse(stdout) as unknown;
	} catch (error) {
		throw new BrokerDtoParseError(path, `stdout was not JSON (${error instanceof Error ? error.message : String(error)})`);
	}
}

function record(value: unknown, path: string): Record<string, unknown> {
	if (!isRecord(value)) throw new BrokerDtoParseError(path, "must be an object");
	return value;
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[], path: string): void {
	for (const key of Object.keys(value)) {
		if (!keys.includes(key)) throw new BrokerDtoParseError(path, `contains unexpected field ${key}`);
	}
}

function requiredString(value: unknown, path: string): string {
	if (typeof value !== "string" || !value.trim()) throw new BrokerDtoParseError(path, "must be a non-empty string");
	return value;
}

function requiredBoolean(value: unknown, path: string): boolean {
	if (typeof value !== "boolean") throw new BrokerDtoParseError(path, "must be a boolean");
	return value;
}

function safeInteger(value: unknown, path: string, nonNegative = true): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || (nonNegative && value < 0)) {
		throw new BrokerDtoParseError(path, nonNegative ? "must be a non-negative safe integer" : "must be a safe integer");
	}
	return value;
}

function finiteNumber(value: unknown, path: string): number {
	if (typeof value !== "number" || !Number.isFinite(value)) throw new BrokerDtoParseError(path, "must be a finite number");
	return value;
}

function optionalString(value: unknown, path: string): string | undefined {
	if (value === undefined) return undefined;
	return requiredString(value, path);
}

function optionalBoolean(value: unknown, path: string): boolean | undefined {
	if (value === undefined) return undefined;
	return requiredBoolean(value, path);
}

function parseRow(value: unknown, path: string): SdkSessionRowV1 {
	const row = record(value, path);
	exactKeys(
		row,
		[
			"sessionId",
			"locator",
			"endpointGeneration",
			"pid",
			"live",
			"deleted",
			"indexSeq",
			"hostIncarnation",
			"terminalUncertain",
			"lifecycleRequestId",
			"endpointMtimeMs",
			"activity",
			"lastHeartbeatAt",
			"identityProvenance",
			"ambiguous",
		],
		path,
	);
	const locator = record(row.locator, `${path}.locator`);
	exactKeys(locator, ["repo", "stateRoot"], `${path}.locator`);
	const activity = row.activity === undefined ? undefined : record(row.activity, `${path}.activity`);
	if (activity) {
		exactKeys(activity, ["state", "at"], `${path}.activity`);
		if (activity.state !== "active" && activity.state !== "idle") {
			throw new BrokerDtoParseError(`${path}.activity.state`, "must be active or idle");
		}
	}
	const identityProvenance = optionalString(row.identityProvenance, `${path}.identityProvenance`);
	if (identityProvenance !== undefined && identityProvenance !== "composite" && identityProvenance !== "legacy") {
		throw new BrokerDtoParseError(`${path}.identityProvenance`, "must be composite or legacy");
	}
	const terminalUncertain = optionalBoolean(row.terminalUncertain, `${path}.terminalUncertain`);
	const ambiguous = optionalBoolean(row.ambiguous, `${path}.ambiguous`);
	return {
		sessionId: requiredString(row.sessionId, `${path}.sessionId`),
		locator: {
			repo: requiredString(locator.repo, `${path}.locator.repo`),
			stateRoot: requiredString(locator.stateRoot, `${path}.locator.stateRoot`),
		},
		endpointGeneration: safeInteger(row.endpointGeneration, `${path}.endpointGeneration`),
		pid: safeInteger(row.pid, `${path}.pid`),
		live: requiredBoolean(row.live, `${path}.live`),
		deleted: requiredBoolean(row.deleted, `${path}.deleted`),
		indexSeq: safeInteger(row.indexSeq, `${path}.indexSeq`),
		...(row.hostIncarnation === undefined ? {} : { hostIncarnation: optionalString(row.hostIncarnation, `${path}.hostIncarnation`) }),
		...(terminalUncertain === undefined ? {} : { terminalUncertain }),
		...(row.lifecycleRequestId === undefined
			? {}
			: { lifecycleRequestId: optionalString(row.lifecycleRequestId, `${path}.lifecycleRequestId`) }),
		...(row.endpointMtimeMs === undefined ? {} : { endpointMtimeMs: finiteNumber(row.endpointMtimeMs, `${path}.endpointMtimeMs`) }),
		...(activity === undefined
			? {}
			: {
				activity: {
					state: activity.state as SdkSessionActivityState,
					at: finiteNumber(activity.at, `${path}.activity.at`),
				},
			}),
		...(row.lastHeartbeatAt === undefined ? {} : { lastHeartbeatAt: finiteNumber(row.lastHeartbeatAt, `${path}.lastHeartbeatAt`) }),
		...(identityProvenance === undefined ? {} : { identityProvenance }),
		...(ambiguous === undefined ? {} : { ambiguous }),
	};
}

function successResult(stdout: string): Record<string, unknown> {
	const envelope = record(parseJson(stdout, "$"), "$");
	// The broker emits two success envelopes: {ok,result} for typed commands and
	// {type:"query_response",id,ok,page} for raw queries. Decorative envelope
	// fields (type/id) are tolerated; authority fields below stay strict.
	exactKeys(envelope, ["ok", "result", "type", "id", "page"], "$");
	if (envelope.ok !== true) throw new BrokerDtoParseError("$.ok", "must be true");
	if (envelope.result !== undefined) return record(envelope.result, "$.result");
	if (envelope.page !== undefined) return { page: envelope.page };
	throw new BrokerDtoParseError("$", "success envelope carried neither result nor page");
}

/** Parses exactly the published `gjc sdk session list` stdout envelope. */
export function parseSessionRows(stdout: string): SdkSessionRowsV1 {
	const result = successResult(stdout);
	exactKeys(result, ["version", "source", "indexSeq", "sessions", "warnings"], "$.result");
	if (result.version !== SESSION_ROWS_VERSION) {
		throw new BrokerDtoParseError("$.result.version", `expected SESSION_ROWS_VERSION ${SESSION_ROWS_VERSION}`);
	}
	if (result.source !== "broker") throw new BrokerDtoParseError("$.result.source", "must be broker");
	if (!Array.isArray(result.sessions)) throw new BrokerDtoParseError("$.result.sessions", "must be an array");
	if (!Array.isArray(result.warnings)) throw new BrokerDtoParseError("$.result.warnings", "must be an array");
	const sessions = result.sessions.map((row, index) => parseRow(row, `$.result.sessions[${index}]`));
	const ids = new Set<string>();
	for (const session of sessions) {
		if (ids.has(session.sessionId)) throw new BrokerDtoParseError("$.result.sessions", `contains duplicate sessionId ${session.sessionId}`);
		ids.add(session.sessionId);
	}
	return {
		version: SESSION_ROWS_VERSION,
		...(result.indexSeq === undefined ? {} : { indexSeq: safeInteger(result.indexSeq, "$.result.indexSeq") }),
		sessions,
	};
}

export function parseSessionInspect(stdout: string, expectedSessionId: string): SdkSessionRowV1 {
	const result = successResult(stdout);
	exactKeys(result, ["version", "source", "session"], "$.result");
	if (result.version !== SESSION_ROWS_VERSION) {
		throw new BrokerDtoParseError("$.result.version", `expected SESSION_ROWS_VERSION ${SESSION_ROWS_VERSION}`);
	}
	if (result.source !== "broker") throw new BrokerDtoParseError("$.result.source", "must be broker");
	const session = parseRow(result.session, "$.result.session");
	if (session.sessionId !== expectedSessionId) throw new BrokerDtoParseError("$.result.session.sessionId", `expected ${expectedSessionId}`);
	return session;
}

function metadataCandidate(result: Record<string, unknown>): unknown {
	if (isRecord(result.page)) {
		const page = result.page;
		exactKeys(page, ["items", "complete", "nextCursor", "revision"], "$.result.page");
		if (!Array.isArray(page.items) || page.items.length !== 1) {
			throw new BrokerDtoParseError("$.result.page.items", "must contain exactly one metadata item");
		}
		return page.items[0];
	}
	return result;
}

/** Parses `session.metadata` without trusting host-provided optional fields. */
export function parseSessionMetadata(stdout: string, expectedSessionId: string): SessionMetadataV1 {
	const item = record(metadataCandidate(successResult(stdout)), "$.result");
	exactKeys(item, ["sessionId", "name", "cwd", "kind"], "$.result");
	const metadata = {
		sessionId: requiredString(item.sessionId, "$.result.sessionId"),
		...(item.name === undefined ? {} : { name: requiredString(item.name, "$.result.name") }),
		cwd: requiredString(item.cwd, "$.result.cwd"),
		kind: requiredString(item.kind, "$.result.kind"),
	};
	if (metadata.sessionId !== expectedSessionId) {
		throw new BrokerDtoParseError("$.result.sessionId", `expected ${expectedSessionId}`);
	}
	return metadata;
}

function parseCheckpoint(value: unknown, path: string): SdkCheckpointRecordV1 {
	const checkpoint = record(value, path);
	exactKeys(checkpoint, ["revision", "generation", "seq"], path);
	return {
		revision: safeInteger(checkpoint.revision, `${path}.revision`),
		generation: safeInteger(checkpoint.generation, `${path}.generation`),
		seq: safeInteger(checkpoint.seq, `${path}.seq`),
	};
}

/**
 * Parses the immediate `session.checkpoint` raw-query response. The query
 * envelope carries broker-generated revision metadata that is useful for
 * diagnostics but is not authority for adoption. Only the checkpoint tuple is
 * authoritative; tolerate additive decorative fields around it.
 */
export function parseSessionCheckpoint(stdout: string): SdkCheckpointRecordV1 {
	const envelope = record(parseJson(stdout, "$"), "$");
	if (envelope.ok !== true) throw new BrokerDtoParseError("$.ok", "must be true");
	const page = record(envelope.page, "$.page");
	if (page.complete !== true) throw new BrokerDtoParseError("$.page.complete", "must be true");
	if (!Array.isArray(page.items) || page.items.length !== 1) {
		throw new BrokerDtoParseError("$.page.items", "must contain exactly one checkpoint item");
	}
	const item = record(page.items[0], "$.page.items[0]");
	const checkpoint = record(item.checkpoint, "$.page.items[0].checkpoint");
	return {
		revision: safeInteger(checkpoint.revision, "$.page.items[0].checkpoint.revision"),
		generation: safeInteger(checkpoint.generation, "$.page.items[0].checkpoint.generation"),
		seq: safeInteger(checkpoint.seq, "$.page.items[0].checkpoint.seq"),
	};
}

function parseGap(value: unknown, path: string): SdkRetentionGapV1 {
	const gap = record(value, path);
	exactKeys(gap, ["code", "missing", "resync"], path);
	if (gap.code !== "retention_gap") throw new BrokerDtoParseError(`${path}.code`, "must be retention_gap");
	let missing: SdkRetentionGapV1["missing"];
	if (gap.missing !== undefined) {
		const candidate = record(gap.missing, `${path}.missing`);
		exactKeys(candidate, ["from", "to"], `${path}.missing`);
		missing = {
			from: safeInteger(candidate.from, `${path}.missing.from`),
			to: safeInteger(candidate.to, `${path}.missing.to`),
		};
	}
	return {
		code: "retention_gap",
		...(missing === undefined ? {} : { missing }),
		...(gap.resync === undefined ? {} : { resync: parseCheckpoint(gap.resync, `${path}.resync`) }),
	};
}

function parseTailItem(value: unknown, path: string): SdkTailItemV1 {
	const item = record(value, path);
	exactKeys(item, ["kind", "id", "generation", "seq", "payload"], path);
	return {
		kind: requiredString(item.kind, `${path}.kind`),
		...(item.id === undefined ? {} : { id: requiredString(item.id, `${path}.id`) }),
		...(item.generation === undefined ? {} : { generation: safeInteger(item.generation, `${path}.generation`) }),
		...(item.seq === undefined ? {} : { seq: safeInteger(item.seq, `${path}.seq`) }),
		payload: item.payload,
	};
}

export function parseTailEnvelope(stdout: string, expectedSessionId: string): SdkTailEnvelopeV1 {
	const result = successResult(stdout);
	exactKeys(result, ["version", "source", "session", "checkpoint", "gap", "items", "terminal"], "$.result");
	if (result.version !== SESSION_ROWS_VERSION) {
		throw new BrokerDtoParseError("$.result.version", `expected SESSION_ROWS_VERSION ${SESSION_ROWS_VERSION}`);
	}
	if (result.source !== "session" && result.source !== "offline") {
		throw new BrokerDtoParseError("$.result.source", "must be session or offline");
	}
	if (!Array.isArray(result.items)) throw new BrokerDtoParseError("$.result.items", "must be an array");
	const session = parseRow(result.session, "$.result.session");
	if (session.sessionId !== expectedSessionId) throw new BrokerDtoParseError("$.result.session.sessionId", `expected ${expectedSessionId}`);
	return {
		version: SESSION_ROWS_VERSION,
		source: result.source,
		session,
		...(result.checkpoint === undefined ? {} : { checkpoint: parseCheckpoint(result.checkpoint, "$.result.checkpoint") }),
		...(result.gap === undefined ? {} : { gap: parseGap(result.gap, "$.result.gap") }),
		items: result.items.map((item, index) => parseTailItem(item, `$.result.items[${index}]`)),
		...(result.terminal === undefined ? {} : { terminal: requiredBoolean(result.terminal, "$.result.terminal") }),
	};
}

function parseOperationReceipt(
	stdout: string,
	expectedSessionId: string,
	expectedOperation: BrokerTurnOperation,
	expectedOperationRef: string,
	fromSend: boolean,
): BrokerOperationReceipt {
	const result = successResult(stdout);
	const candidate = fromSend ? record(result.receipt, "$.result.receipt") : result;
	if (fromSend) {
		exactKeys(result, ["version", "operationRef", "status", "receipt"], "$.result");
		if (result.version !== SESSION_ROWS_VERSION) {
			throw new BrokerDtoParseError("$.result.version", `expected SESSION_ROWS_VERSION ${SESSION_ROWS_VERSION}`);
		}
		if (requiredString(result.operationRef, "$.result.operationRef") !== expectedOperationRef) {
			throw new BrokerDtoParseError("$.result.operationRef", `expected ${expectedOperationRef}`);
		}
		if (result.status !== "accepted") throw new BrokerDtoParseError("$.result.status", "must be accepted");
	}
	if (candidate.accepted !== true) throw new BrokerDtoParseError(fromSend ? "$.result.receipt.accepted" : "$.result.accepted", "must be true");
	// The real broker's receipt is {commandId, turnId, accepted, clientRef} - it
	// does NOT echo sessionId or operation. Bind identity through clientRef
	// (which must equal our operationRef); tolerate but verify echoes if present.
	const sessionId = typeof candidate.sessionId === "string" ? candidate.sessionId : undefined;
	if (sessionId !== undefined && sessionId !== expectedSessionId) {
		throw new BrokerDtoParseError("$.result.sessionId", `expected ${expectedSessionId}`);
	}
	const operation = typeof candidate.operation === "string" ? candidate.operation : undefined;
	if (operation !== undefined && operation !== expectedOperation) {
		throw new BrokerDtoParseError("$.result.operation", `expected ${expectedOperation}`);
	}
	const clientRef = typeof candidate.clientRef === "string" ? candidate.clientRef : undefined;
	if (fromSend) {
		if (clientRef === undefined) throw new BrokerDtoParseError("$.result.receipt.clientRef", "must be present");
		if (clientRef !== expectedOperationRef) throw new BrokerDtoParseError("$.result.receipt.clientRef", `expected ${expectedOperationRef}`);
	} else if (clientRef !== undefined && clientRef !== expectedOperationRef) {
		throw new BrokerDtoParseError("$.result.clientRef", `expected ${expectedOperationRef}`);
	}
	return {
		sessionId: expectedSessionId,
		operation: expectedOperation,
		operationRef: expectedOperationRef,
		...(typeof candidate.commandId === "string" && candidate.commandId ? { commandId: candidate.commandId } : {}),
		...(typeof candidate.turnId === "string" && candidate.turnId ? { turnId: candidate.turnId } : {}),
	};
}

function parseTurnStatus(stdout: string, expectedOperationRef: string): BrokerTurnStatus {
	const result = successResult(stdout);
	exactKeys(result, ["version", "operationRef", "status", "summary"], "$.result");
	if (result.version !== SESSION_ROWS_VERSION) {
		throw new BrokerDtoParseError("$.result.version", `expected SESSION_ROWS_VERSION ${SESSION_ROWS_VERSION}`);
	}
	if (requiredString(result.operationRef, "$.result.operationRef") !== expectedOperationRef) {
		throw new BrokerDtoParseError("$.result.operationRef", `expected ${expectedOperationRef}`);
	}
	const status = record(result.status, "$.result.status");
	const summary = record(result.summary, "$.result.summary");
	exactKeys(summary, ["completed"], "$.result.summary");
	return {
		operationRef: expectedOperationRef,
		status: requiredString(status.status, "$.result.status.status"),
		completed: requiredBoolean(summary.completed, "$.result.summary.completed"),
		detail: status,
	};
}

function extractBrokerError(stdout: string, stderr: string): BrokerCliError | undefined {
	try {
		const envelope = JSON.parse(stdout) as unknown;
		if (!isRecord(envelope) || envelope.ok !== false || !isRecord(envelope.error)) return undefined;
		const code = typeof envelope.error.code === "string" && envelope.error.code ? envelope.error.code : "broker_command_failed";
		const message =
			typeof envelope.error.message === "string" && envelope.error.message ? envelope.error.message : "Broker CLI rejected the request.";
		return new BrokerCliError(code, message, { stderr });
	} catch {
		return undefined;
	}
}

function timeoutArgument(timeoutMs: number | undefined): string[] {
	return timeoutMs === undefined ? [] : ["--timeout-ms", String(timeoutMs)];
}

/**
 * Spawn-only broker boundary. Commands are passed as argv arrays and never via
 * a shell string so a session id cannot alter process execution.
 */
export class BrokerCli {
	readonly executable: string;
	readonly commandTimeoutMs: number;
	readonly #environment: NodeJS.ProcessEnv | undefined;

	constructor(options: BrokerCliOptions = {}) {
		this.executable = options.executable?.trim() || "gjc";
		this.commandTimeoutMs = options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
		this.#environment = options.environment;
		if (!Number.isSafeInteger(this.commandTimeoutMs) || this.commandTimeoutMs <= 0 || this.commandTimeoutMs > 20_000) {
			throw new BrokerCliError("invalid_broker_timeout", "Broker command timeout must be an integer in 1..=20000 ms.");
		}
	}

	async listSessions(options: { readonly timeoutMs?: number } = {}): Promise<SdkSessionRowsV1> {
		const stdout = await this.run(["sdk", "session", "list"], options.timeoutMs);
		return parseSessionRows(stdout);
	}

	async inspectSession(sessionId: string, options: { readonly timeoutMs?: number } = {}): Promise<SdkSessionRowV1> {
		if (!sessionId.trim()) throw new BrokerCliError("invalid_session_id", "Cannot inspect an empty session id.");
		const stdout = await this.run(["sdk", "session", "inspect", sessionId], options.timeoutMs);
		return parseSessionInspect(stdout, sessionId);
	}

	async sessionMetadata(sessionId: string, options: { readonly timeoutMs?: number } = {}): Promise<SessionMetadataV1> {
		if (!sessionId.trim()) throw new BrokerMetadataUnavailableError("Cannot query metadata for an empty session id.");
		let stdout: string;
		try {
			stdout = await this.run(["sdk", "session", "raw", "query", sessionId, "--query", "session.metadata"], options.timeoutMs);
		} catch (error) {
			if (error instanceof BrokerDtoParseError) throw error;
			if (error instanceof BrokerCliError) {
				throw new BrokerMetadataUnavailableError(`Metadata query for ${sessionId} was unavailable.`, {
					stderr: error.stderr,
					cause: error,
				});
			}
			throw error;
		}
		return parseSessionMetadata(stdout, sessionId);
	}

	/** Returns the broker's immediate adoption watermark without waiting for a tail exit condition. */
	async sessionCheckpoint(sessionId: string, options: { readonly timeoutMs?: number } = {}): Promise<SdkCheckpointRecordV1> {
		if (!sessionId.trim()) throw new BrokerCliError("invalid_session_id", "Cannot query a checkpoint for an empty session id.");
		const stdout = await this.run(
			["sdk", "session", "raw", "query", sessionId, "--query", "session.checkpoint", ...timeoutArgument(options.timeoutMs)],
			options.timeoutMs,
		);
		return parseSessionCheckpoint(stdout);
	}

	async sendPrompt(
		sessionId: string,
		text: string,
		operationRef: string,
		options: { readonly timeoutMs?: number } = {},
	): Promise<BrokerOperationReceipt> {
		this.assertOperationInput(sessionId, text, operationRef);
		const stdout = await this.run(
			["sdk", "session", "send", sessionId, "--text", text, "--op-ref", operationRef, ...timeoutArgument(options.timeoutMs)],
			options.timeoutMs,
		);
		return parseOperationReceipt(stdout, sessionId, "turn.prompt", operationRef, true);
	}

	async controlTurn(
		sessionId: string,
		operation: Exclude<BrokerTurnOperation, "turn.prompt">,
		text: string,
		operationRef: string,
		options: { readonly timeoutMs?: number } = {},
	): Promise<BrokerOperationReceipt> {
		this.assertOperationInput(sessionId, text, operationRef);
		const input = JSON.stringify({ text, clientRef: operationRef });
		const stdout = await this.run(
			[
				"sdk",
				"session",
				"raw",
				"control",
				sessionId,
				"--op",
				operation,
				"--json-input",
				input,
				...timeoutArgument(options.timeoutMs),
			],
			options.timeoutMs,
		);
		return parseOperationReceipt(stdout, sessionId, operation, operationRef, false);
	}

	async turnStatus(
		sessionId: string,
		operationRef: string,
		options: { readonly timeoutMs?: number } = {},
	): Promise<BrokerTurnStatus> {
		if (!sessionId.trim() || !operationRef.trim()) throw new BrokerCliError("invalid_operation_ref", "Session id and operation reference are required.");
		const stdout = await this.run(
			["sdk", "session", "status", sessionId, operationRef, ...timeoutArgument(options.timeoutMs)],
			options.timeoutMs,
		);
		return parseTurnStatus(stdout, operationRef);
	}

	async tailSession(
		sessionId: string,
		options: {
			readonly repo: string;
			readonly cursor?: string;
			readonly untilIdle?: boolean;
			readonly strict?: boolean;
			readonly allEvents?: boolean;
			readonly timeoutMs?: number;
		},
	): Promise<SdkTailEnvelopeV1> {
		if (!sessionId.trim()) throw new BrokerCliError("invalid_session_id", "Cannot tail an empty session id.");
		const args = ["sdk", "session", "tail", sessionId, "--repo", options.repo];
		if (options.cursor) args.push("--cursor", options.cursor);
		if (options.untilIdle === true) args.push("--until-idle");
		if (options.strict === true) args.push("--strict");
		if (options.allEvents === true) args.push("--all-events");
		args.push(...timeoutArgument(options.timeoutMs));
		// --timeout-ms bounds the broker-side tail WAIT WINDOW. The process
		// deadline must additionally cover real CLI startup (~5s cold on this
		// hardware), or every tail dies as broker_command_timeout before the CLI
		// can even reach the broker (observed live: a 3s window with a 3s process
		// deadline degraded the daemon permanently while manual tails succeeded).
		const processDeadlineMs = options.timeoutMs === undefined ? undefined : options.timeoutMs + 30_000;
		const stdout = await this.run(args, processDeadlineMs);
		return parseTailEnvelope(stdout, sessionId);
	}

	async contextState(
		sessionId: string,
		options: { readonly timeoutMs?: number } = {},
	): Promise<{ readonly isStreaming: boolean; readonly followUpQueueDepth: number }> {
		if (!sessionId.trim()) throw new BrokerCliError("invalid_session_id", "Cannot query an empty session id.");
		const stdout = await this.run(
			["sdk", "session", "raw", "query", sessionId, "--query", "context.get", ...timeoutArgument(options.timeoutMs)],
			options.timeoutMs,
		);
		const result = successResult(stdout);
		const candidate = isRecord(result.page)
			? Array.isArray(result.page.items) && result.page.items.length === 1
				? result.page.items[0]
				: undefined
			: result;
		const context = record(candidate, "$.result");
		const followUpQueueDepth = context.followupQueueDepth ?? context.followUpQueueDepth;
		return {
			isStreaming: requiredBoolean(context.isStreaming, "$.result.isStreaming"),
			followUpQueueDepth: safeInteger(followUpQueueDepth, "$.result.followupQueueDepth"),
		};
	}

	private assertOperationInput(sessionId: string, text: string, operationRef: string): void {
		if (!sessionId.trim()) throw new BrokerCliError("invalid_session_id", "A non-empty session id is required.");
		if (!text.trim()) throw new BrokerCliError("invalid_prompt", "A non-empty prompt is required.");
		if (!operationRef.trim()) throw new BrokerCliError("invalid_operation_ref", "A non-empty operation reference is required.");
	}

	private async run(args: readonly string[], requestedTimeoutMs?: number): Promise<string> {
		const timeoutMs = requestedTimeoutMs ?? this.commandTimeoutMs;
		if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
			throw new BrokerCliError("invalid_broker_timeout", "Broker command timeout must be a positive integer.");
		}
		return await new Promise<string>((resolve, reject) => {
			let stdout = "";
			let stderr = "";
			let settled = false;
			let timedOut = false;
			const child = spawn(this.executable, [...args], {
				shell: false,
				stdio: ["ignore", "pipe", "pipe"],
				windowsHide: true,
				...(this.#environment === undefined ? {} : { env: this.#environment }),
			});
			const finish = (callback: () => void) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				callback();
			};
			const overflow = () => {
				try {
					child.kill("SIGKILL");
				} catch {
					// The process can already have exited.
				}
				finish(() => reject(new BrokerCliError("broker_output_too_large", "Broker CLI output exceeded the 8 MiB limit.", { stderr })));
			};
			const timer = setTimeout(() => {
				timedOut = true;
				try {
					child.kill("SIGKILL");
				} catch {
					// The close/error listener supplies the final result.
				}
			}, timeoutMs);
			child.once("error", error => finish(() => reject(new BrokerCliError("broker_spawn_failed", `Could not start ${this.executable}.`, { cause: error }))));
			child.stdout?.on("data", chunk => {
				stdout += String(chunk);
				if (Buffer.byteLength(stdout) > MAX_CAPTURED_OUTPUT_BYTES) overflow();
			});
			child.stderr?.on("data", chunk => {
				stderr += String(chunk);
				if (Buffer.byteLength(stderr) > MAX_CAPTURED_OUTPUT_BYTES) overflow();
			});
			child.once("close", (exitCode, signal) => {
				if (timedOut) {
					finish(() => reject(new BrokerCliError("broker_timeout", `Broker CLI exceeded ${timeoutMs} ms.`, { stderr })));
					return;
				}
				if (exitCode !== 0) {
					finish(() =>
						reject(
							extractBrokerError(stdout, stderr) ??
								new BrokerCliError(
									"broker_command_failed",
									`Broker CLI exited with ${exitCode ?? "signal"}${signal ? ` (${signal})` : ""}.`,
									{ stderr },
								),
						),
					);
					return;
				}
				finish(() => resolve(stdout));
			});
		});
	}
}
