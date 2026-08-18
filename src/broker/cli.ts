import { spawn } from "node:child_process";

/** The published `gjc sdk session` list envelope version. */
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
	readonly name: string;
	readonly cwd: string;
	readonly kind: string;
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
		super("broker_dto_drift", `Invalid SdkSessionRowV1 DTO at ${path}: ${message}`);
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

/** Parses exactly the published `gjc sdk session list` stdout envelope. */
export function parseSessionRows(stdout: string): SdkSessionRowsV1 {
	const envelope = record(parseJson(stdout, "$"), "$");
	exactKeys(envelope, ["ok", "result"], "$");
	if (envelope.ok !== true) throw new BrokerDtoParseError("$.ok", "must be true");
	const result = record(envelope.result, "$.result");
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

function metadataCandidate(result: Record<string, unknown>): unknown {
	if (isRecord(result.page)) {
		const page = result.page;
		exactKeys(page, ["items", "complete", "nextCursor"], "$.result.page");
		if (!Array.isArray(page.items) || page.items.length !== 1) {
			throw new BrokerDtoParseError("$.result.page.items", "must contain exactly one metadata item");
		}
		return page.items[0];
	}
	return result;
}

/** Parses `session.metadata` without trusting host-provided optional fields. */
export function parseSessionMetadata(stdout: string, expectedSessionId: string): SessionMetadataV1 {
	const envelope = record(parseJson(stdout, "$"), "$");
	exactKeys(envelope, ["ok", "result"], "$");
	if (envelope.ok !== true) throw new BrokerDtoParseError("$.ok", "must be true");
	const item = record(metadataCandidate(record(envelope.result, "$.result")), "$.result");
	exactKeys(item, ["sessionId", "name", "cwd", "kind"], "$.result");
	const metadata = {
		sessionId: requiredString(item.sessionId, "$.result.sessionId"),
		name: requiredString(item.name, "$.result.name"),
		cwd: requiredString(item.cwd, "$.result.cwd"),
		kind: requiredString(item.kind, "$.result.kind"),
	};
	if (metadata.sessionId !== expectedSessionId) {
		throw new BrokerDtoParseError("$.result.sessionId", `expected ${expectedSessionId}`);
	}
	return metadata;
}

/**
 * Spawn-only broker boundary. Commands are passed as argv arrays and never via
 * a shell string so a session id cannot alter process execution.
 */
export class BrokerCli {
	readonly executable: string;
	readonly commandTimeoutMs: number;

	constructor(options: BrokerCliOptions = {}) {
		this.executable = options.executable?.trim() || "gjc";
		this.commandTimeoutMs = options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
		if (!Number.isSafeInteger(this.commandTimeoutMs) || this.commandTimeoutMs <= 0 || this.commandTimeoutMs > 20_000) {
			throw new BrokerCliError("invalid_broker_timeout", "Broker command timeout must be an integer in 1..=20000 ms.");
		}
	}

	async listSessions(options: { readonly timeoutMs?: number } = {}): Promise<SdkSessionRowsV1> {
		const stdout = await this.run(["sdk", "session", "list"], options.timeoutMs);
		return parseSessionRows(stdout);
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
