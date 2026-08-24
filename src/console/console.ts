import { randomUUID } from "node:crypto";
import * as path from "node:path";
import { stdout } from "node:process";
import type { WayConfig } from "../config";
import { RpcJournalConsumer, type RpcJournalEvent } from "../journal-consumer";
import { loadWayProfile, type WayProfile } from "../profile";
import { type JsonRpcClient, RpcClient, RpcResponseError, rpcResult } from "../rpc-client";
import type { RawConsoleOutputStream } from "./tui/terminal";
import {
	MAX_RAW_CONSOLE_LINE_BYTES,
	MAX_RAW_CONSOLE_QUEUED_BYTES,
	MAX_RAW_CONSOLE_QUEUED_LINES,
	RawConsoleTerminal,
} from "./tui/terminal";

export type { RawConsoleInputStream, RawConsoleOutputStream, RawConsoleTerminalOptions } from "./tui/terminal";
export {
	MAX_RAW_CONSOLE_LINE_BYTES,
	MAX_RAW_CONSOLE_QUEUED_BYTES,
	MAX_RAW_CONSOLE_QUEUED_LINES,
	RawConsoleTerminal,
} from "./tui/terminal";

export const GAJAEWAY_CONSOLE_CONSUMER_ID = "gajaeway-console";
export const GAJAEWAY_CONSOLE_EVENT_KINDS = [
	"assistant_message",
	"turn_start",
	"turn_end",
	"tail_ring_rotation",
	"transcript_delivery_gap",
	"gate_open",
	"gate_resolved",
	"health_change",
	"lock_event",
	"alert_raised",
	"alert_cleared",
] as const;

export const GAJAEWAY_JOURNAL_EVENT_KINDS = [
	...GAJAEWAY_CONSOLE_EVENT_KINDS,
	"registry_change",
	"follow_up_attempted",
	"follow_up_confirmed",
	"profile_approved",
	"schedule_run",
	"memory_index_rebuilt",
] as const;
export const GAJAEWAY_JOURNAL_DEFAULT_KINDS = GAJAEWAY_JOURNAL_EVENT_KINDS.filter((kind) => kind !== "registry_change");
export const DEFAULT_CONSOLE_CLAIM_TTL_MS = 5_000;
export const DEFAULT_CONSOLE_READ_WAIT_MS = 1_000;
export const DEFAULT_CONSOLE_EXIT_DRAIN_MS = 2_000;
export const DEFAULT_CONSOLE_STATUS_POLL_MS = 1_000;
const DEFAULT_CONSOLE_STATUS_REQUEST_TIMEOUT_MS = 2_000;
const MAX_CONSOLE_JOURNAL_TAIL_EVENTS = 100;
const DEFAULT_CONSOLE_JOURNAL_TAIL_EVENTS = 20;
const MAX_CONSOLE_INPUT_OPERATIONS = 16;
const MAX_CONSOLE_EXIT_DIAGNOSTIC_MS = 250;
const EXTERNAL_HOST_GATE_ANSWER_GUIDANCE =
	"The gateway cannot answer gates in the external-host architecture. The owner must answer each gate in the attached gjc TUI: tmux attach -t <session> when the tmux backend hosts it, or whatever terminal runs gjc.";

export type WayConsoleEventKind = (typeof GAJAEWAY_CONSOLE_EVENT_KINDS)[number];
export type WayJournalEventKind = (typeof GAJAEWAY_JOURNAL_EVENT_KINDS)[number];
export type ConsoleConsumerRunResult = "rendered" | "idle";
type RecordValue = Record<string, unknown>;
type ConsoleWrite = (text: string) => void | Promise<void>;

export interface ConsoleStartupDecision {
	readonly interactive: boolean;
	readonly refusal?: string;
}

export interface ConsoleStartup {
	readonly accepted: boolean;
	readonly health?: RecordValue;
	readonly status?: RecordValue;
	readonly refusal?: string;
}

export interface ConsoleEventFrame {
	readonly seq: string | number;
	readonly ts?: number;
	readonly kind: WayConsoleEventKind;
	readonly payload: unknown;
}

/** Raw-terminal operations reserved for complete console-controlled frames and ANSI. */
export interface ConsoleTerminal {
	writeTrusted(text: string): Promise<void>;
	readLine(prompt: string): Promise<string | undefined>;
	close(): void;
	/** Notifies the input loop that terminal interrupt/EOF was received between reads. */
	onExitRequested?(listener: () => void): () => void;
	/** Allows the terminal status rail to reflect delivery fencing without owning gateway state. */
	setDeliveryState?(state: "fenced" | "ready" | "unavailable" | "stopping"): void;
	/** Lets the terminal update live status before the corresponding journal frame is rendered. */
	observeEvent?(event: ConsoleEventFrame): void;
}

/**
 * Separates console-owned terminal control text from data obtained through RPC.
 * Untrusted writes are escaped before they can reach the raw terminal.
 */
export class ConsoleOutput {
	#write: ConsoleWrite;
	#frameTail: Promise<void> = Promise.resolve();

	constructor(write: ConsoleWrite) {
		this.#write = write;
	}

	setWriter(write: ConsoleWrite): void {
		this.#write = write;
	}

	/** Publishes one complete terminal frame without interleaving another frame. */
	async writeFrame(text: string): Promise<void> {
		const writer = this.#write;
		const frame = this.#frameTail.then(async () => await writer(text));
		this.#frameTail = frame.catch(() => undefined);
		await frame;
	}

	async writeTrusted(text: string): Promise<void> {
		await this.writeFrame(text);
	}

	async writeUntrusted(text: string): Promise<void> {
		await this.writeFrame(sanitizeConsoleText(text));
	}
}

export interface ConsoleEventConsumerOptions {
	readonly rpc: JsonRpcClient;
	readonly render: (event: ConsoleEventFrame) => void | Promise<void>;
	readonly consumerId?: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idleDelayMs?: number;
	readonly now?: () => number;
}

export interface OwnerConsoleOptions {
	readonly rpc: JsonRpcClient;
	readonly ownerSurfaceId: string;
	readonly output: ConsoleOutput;
	readonly consumerId?: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idempotencyKey?: () => string;
	/** Renderer-only notification emitted immediately before a durable event frame is published. */
	readonly onEvent?: (event: ConsoleEventFrame) => void | Promise<void>;
}

export interface RunWayConsoleDependencies {
	readonly rpcConnect?: (socketPath: string) => Promise<JsonRpcClient>;
	readonly profile?: WayProfile;
	readonly terminal?: ConsoleTerminal;
	readonly idempotencyKey?: () => string;
	/** Test seam for bounded graceful exit behavior. */
	readonly exitDrainMs?: number;
	/** Test seam for bounded live gateway-status polling. */
	readonly statusPollMs?: number;
}

interface MainSubmitResult {
	readonly accepted: true;
	readonly opRef: string;
	readonly deliveredAs: string;
}

interface GateAnswerAcceptedResult {
	readonly accepted: true;
	readonly gateState: string;
}

interface GateAnswerUnsupportedResult {
	readonly accepted: false;
	readonly gateState: "unsupported";
}

type GateAnswerResult = GateAnswerAcceptedResult | GateAnswerUnsupportedResult;

export class WayConsoleError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WayConsoleError";
	}
}

/** A startup fence refusal; the CLI must surface it as a non-zero result. */
export class ConsoleStartupRefusalError extends WayConsoleError {
	constructor(message: string) {
		super(message);
		this.name = "ConsoleStartupRefusalError";
	}
}

/** Journal publication is unavailable, so owner mutations must stop. */
export class ConsoleDeliveryUnavailableError extends WayConsoleError {
	constructor(message: string) {
		super(message);
		this.name = "ConsoleDeliveryUnavailableError";
	}
}

function isRecord(value: unknown): value is RecordValue {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function recordValue(value: unknown): RecordValue {
	return isRecord(value) ? value : {};
}

function rawStringValue(value: unknown, fallback = "unknown"): string {
	return typeof value === "string" && value ? value : fallback;
}

function stringValue(value: unknown, fallback = "unknown"): string {
	return boundedConsoleText(rawStringValue(value, fallback));
}

function booleanValue(value: unknown): string {
	return typeof value === "boolean" ? String(value) : "unknown";
}

function integerValue(value: unknown): string {
	return typeof value === "number" && Number.isFinite(value) ? String(value) : "unknown";
}

const MAX_CONSOLE_COMPACT_VALUE_CHARS = 512;
const MAX_CONSOLE_STATUS_CONSUMERS = 8;

function boundedConsoleText(value: string, maximum = MAX_CONSOLE_COMPACT_VALUE_CHARS): string {
	const sanitized = sanitizeConsoleText(value);
	return sanitized.length <= maximum ? sanitized : `${sanitized.slice(0, Math.max(0, maximum - 1))}…`;
}

function compactGatewayValue(value: unknown, maximum = MAX_CONSOLE_COMPACT_VALUE_CHARS): string {
	try {
		return boundedConsoleText(JSON.stringify(value) ?? String(value), maximum);
	} catch {
		return "[unserializable gateway payload]";
	}
}

function renderConsumerCheckpoints(value: unknown): string {
	if (!Array.isArray(value) || value.length === 0) return "none";
	const checkpoints = value.slice(0, MAX_CONSOLE_STATUS_CONSUMERS).map((candidate) => {
		const checkpoint = recordValue(candidate);
		const consumerId = boundedConsoleText(rawStringValue(checkpoint.consumer_id), 64);
		const cursor = boundedConsoleText(rawStringValue(checkpoint.cursor), 64);
		return `${consumerId}@${cursor}${typeof checkpoint.claim_id === "string" ? "(claimed)" : ""}`;
	});
	const remainder = value.length - checkpoints.length;
	return `${checkpoints.join(", ")}${remainder > 0 ? ` (+${remainder} more)` : ""}`;
}

function stableStatusFingerprint(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(stableStatusFingerprint).join(",")}]`;
	if (!isRecord(value)) return JSON.stringify(value) ?? "undefined";
	return `{${Object.keys(value)
		.filter((key) => key !== "uptime_ms")
		.sort()
		.map((key) => `${JSON.stringify(key)}:${stableStatusFingerprint(value[key])}`)
		.join(",")}}`;
}

function statusFingerprint(health: RecordValue, status: RecordValue): string {
	return stableStatusFingerprint({ health, status });
}

function firstString(records: readonly RecordValue[], key: string): string | undefined {
	for (const record of records) {
		const value = record[key];
		if (typeof value === "string" && value) return value;
	}
	return undefined;
}

function firstValue(record: RecordValue, keys: readonly string[]): unknown {
	for (const key of keys) {
		if (record[key] !== undefined) return record[key];
	}
	return undefined;
}

/**
 * Makes all C0, DEL, and C1 controls visible rather than executable. Printable
 * Unicode remains readable, while line/control escapes cannot forge terminal UI.
 */
export function sanitizeConsoleText(value: string): string {
	let output = "";
	for (const character of value) {
		const codePoint = character.codePointAt(0) as number;
		if (codePoint === 0x0a) {
			output += "\\n";
			continue;
		}
		if (codePoint === 0x0d) {
			output += "\\r";
			continue;
		}
		if (codePoint === 0x09) {
			output += "\\t";
			continue;
		}
		if (codePoint === 0x1b) {
			output += "\\x1B";
			continue;
		}
		if ((codePoint >= 0x00 && codePoint <= 0x1f) || (codePoint >= 0x7f && codePoint <= 0x9f)) {
			output += `\\u${codePoint.toString(16).toUpperCase().padStart(4, "0")}`;
			continue;
		}
		output += character;
	}
	return output;
}

/** Decides whether a local owner can safely send input to this daemon. */
export function consoleStartupDecision(healthPayload: unknown, statusPayload: unknown): ConsoleStartupDecision {
	if (!isRecord(healthPayload) || !isRecord(statusPayload)) {
		return {
			interactive: false,
			refusal: "Refusing interactive console: the gateway returned an invalid health or status payload.",
		};
	}
	const records = [healthPayload, statusPayload] as const;
	const states = records.map((record) => rawStringValue(record.state));
	const statuses = records.map((record) => rawStringValue(record.status));
	const renderedStates = states.map(sanitizeConsoleText);
	const renderedStatuses = statuses.map(sanitizeConsoleText);
	const reason = firstString(records, "reason");
	const renderedReason = reason ? sanitizeConsoleText(reason) : undefined;
	if (states.includes("failed_closed")) {
		return {
			interactive: false,
			refusal: `Refusing interactive console: the gateway is failed closed${renderedReason ? ` (${renderedReason})` : ""}. The main persona is fenced; repair or explicitly approve it before sending owner input.`,
		};
	}
	if (statuses.some((status) => status !== "healthy")) {
		return {
			interactive: false,
			refusal: `Refusing interactive console: the gateway is not healthy (health=${renderedStatuses.join(", ")}, state=${renderedStates.join(", ")})${renderedReason ? `: ${renderedReason}` : ""}. No owner input was sent.`,
		};
	}
	return { interactive: true };
}

/** Renders the live gateway state shown by the cockpit status rail. */
export function renderConsoleStatusSummary(healthPayload: unknown, statusPayload: unknown, now = Date.now()): string {
	const health = recordValue(healthPayload);
	const status = recordValue(statusPayload);
	const main = recordValue(status.main ?? health.main);
	const lock = recordValue(status.lock);
	const holder = recordValue(lock.holder);
	const journal = recordValue(status.journal);
	const reconcile = recordValue(status.reconcile);
	const lastOkAt =
		typeof reconcile.last_ok_at === "number" && Number.isFinite(reconcile.last_ok_at)
			? reconcile.last_ok_at
			: undefined;
	const cycleMs =
		typeof reconcile.cycle_ms === "number" && Number.isFinite(reconcile.cycle_ms) ? reconcile.cycle_ms : undefined;
	const reconcileFreshness = renderReconcileFreshness(lastOkAt, cycleMs, now);
	const reason = firstString([health, status], "reason");
	const holderDescription = holder.session_id
		? `session=${stringValue(holder.session_id)}`
		: holder.lease_id
			? `lease=${stringValue(holder.lease_id)}`
			: "none";
	return [
		"Gateway status",
		`  daemon: status=${stringValue(health.status)} state=${stringValue(health.state)}${reason ? ` reason=${boundedConsoleText(reason)}` : ""}`,
		`  main: resumed=${booleanValue(main.resumed)} session_id=${stringValue(main.session_id, "none")} turn_state=${stringValue(status.turn_state)} follow_up_queue_depth=${integerValue(status.follow_up_queue_depth)}`,
		`  journal: head_cursor=${stringValue(journal.head_cursor)} degraded=${booleanValue(journal.degraded)}`,
		`  transcript delivery: verification=${stringValue(status.transcript_verification)} gap_detected=${booleanValue(status.transcript_delivery_gap_detected)} gap_count=${integerValue(status.transcript_delivery_gap_count)}`,
		`  lock: held=${booleanValue(lock.held)} holder=${holderDescription} queue_len=${integerValue(lock.queue_len)} stuck=${booleanValue(lock.stuck)} quarantined=${booleanValue(lock.quarantined)} write_mode=${booleanValue(status.write_mode)}`,
		`  reconcile: ${reconcileFreshness} drift_count=${integerValue(reconcile.drift_count)}`,
		`  consumers: ${renderConsumerCheckpoints(status.consumers)}`,
	].join("\n");
}

function renderReconcileFreshness(lastOkAt: number | undefined, cycleMs: number | undefined, now: number): string {
	if (lastOkAt === undefined) return `freshness=unknown last_ok_at=none cycle_ms=${cycleMs ?? "unknown"}`;
	const ageMs = Math.max(0, now - lastOkAt);
	const staleAfterMs = Math.max((cycleMs ?? 15_000) * 2, 30_000);
	return `freshness=${ageMs <= staleAfterMs ? "fresh" : "stale"} last_ok_at=${lastOkAt} age_ms=${ageMs} cycle_ms=${cycleMs ?? "unknown"}`;
}

interface GatewayStatusSnapshot {
	readonly health: RecordValue;
	readonly status: RecordValue;
}

interface JournalTailRequest {
	readonly kinds: readonly WayJournalEventKind[] | undefined;
	readonly count: number;
}

interface JournalTailEvent {
	readonly seq: string | number;
	readonly kind: WayJournalEventKind;
	readonly payload: unknown;
}

interface JournalTailResult {
	readonly events: readonly JournalTailEvent[];
	readonly nextCursor: string;
	readonly gap?: { readonly missingFrom: string; readonly missingTo: string; readonly resyncCursor: string };
}

type LockCommand =
	| { readonly action: "status" }
	| {
			readonly action: "force-release";
			readonly leaseId: string;
			readonly confirmation: string;
			readonly confirmed: boolean;
	  }
	| {
			readonly action: "clear-quarantine";
			readonly receiptId: string;
			readonly confirmation: string;
			readonly confirmed: boolean;
	  };

function parseJournalTailCommand(command: string): JournalTailRequest {
	const argument = command.slice("/journal".length).trim();
	if (!argument) return { kinds: GAJAEWAY_JOURNAL_DEFAULT_KINDS, count: DEFAULT_CONSOLE_JOURNAL_TAIL_EVENTS };
	const fields = argument.split(/\s+/u);
	if (fields.length > 2) throw new WayConsoleError("Usage: /journal [all|kind[,kind...]] [count]");
	if (/^\d+$/u.test(fields[0] as string)) {
		if (fields.length !== 1) throw new WayConsoleError("Usage: /journal [all|kind[,kind...]] [count]");
		return { kinds: GAJAEWAY_JOURNAL_DEFAULT_KINDS, count: parseJournalTailCount(fields[0] as string) };
	}
	const kinds = parseJournalKinds(fields[0] as string);
	return {
		kinds,
		count: fields[1] === undefined ? DEFAULT_CONSOLE_JOURNAL_TAIL_EVENTS : parseJournalTailCount(fields[1]),
	};
}

function parseJournalTailCount(value: string): number {
	if (!/^[1-9]\d*$/u.test(value))
		throw new WayConsoleError(`Journal count must be an integer in 1..=${MAX_CONSOLE_JOURNAL_TAIL_EVENTS}.`);
	const count = Number(value);
	if (!Number.isSafeInteger(count) || count > MAX_CONSOLE_JOURNAL_TAIL_EVENTS) {
		throw new WayConsoleError(`Journal count must be an integer in 1..=${MAX_CONSOLE_JOURNAL_TAIL_EVENTS}.`);
	}
	return count;
}

function parseJournalKinds(value: string): readonly WayJournalEventKind[] | undefined {
	if (value === "all") return undefined;
	const kinds = value.split(",").map((kind) => kind.trim());
	if (kinds.some((kind) => !GAJAEWAY_JOURNAL_EVENT_KINDS.includes(kind as WayJournalEventKind))) {
		throw new WayConsoleError(
			`Unsupported journal kind. Supported kinds: ${GAJAEWAY_JOURNAL_EVENT_KINDS.join(", ")}, or all.`,
		);
	}
	if (new Set(kinds).size !== kinds.length) throw new WayConsoleError("Journal kinds must not contain duplicates.");
	return kinds as WayJournalEventKind[];
}

function journalTailCursor(headCursor: unknown, count: number): string {
	const match = /^(\d+):(\d+)$/u.exec(rawStringValue(headCursor, ""));
	if (!match) throw new WayConsoleError("way.status returned an invalid journal head_cursor.");
	const generation = match[1] as string;
	const head = BigInt(match[2] as string);
	const start = head > BigInt(count) ? head - BigInt(count) : 0n;
	return `${generation}:${start}`;
}

function parseJournalTailResult(value: unknown): JournalTailResult {
	const result = recordValue(value);
	if (!Array.isArray(result.events) || typeof result.next_cursor !== "string") {
		throw new WayConsoleError("main.events.read returned an invalid journal-tail response.");
	}
	const events = result.events.map((candidate): JournalTailEvent => {
		const event = recordValue(candidate);
		const kind = rawStringValue(event.kind, "");
		if (!GAJAEWAY_JOURNAL_EVENT_KINDS.includes(kind as WayJournalEventKind)) {
			throw new WayConsoleError(
				`main.events.read returned an unsupported journal event kind: ${boundedConsoleText(kind)}.`,
			);
		}
		if (typeof event.seq !== "string" && typeof event.seq !== "number") {
			throw new WayConsoleError("main.events.read returned a journal event without a sequence.");
		}
		sequenceString(event.seq);
		return { seq: event.seq, kind: kind as WayJournalEventKind, payload: event.payload };
	});
	const gap = result.gap === undefined ? undefined : recordValue(result.gap);
	if (
		gap &&
		(typeof gap.missing_from !== "string" ||
			typeof gap.missing_to !== "string" ||
			typeof gap.resync_cursor !== "string")
	) {
		throw new WayConsoleError("main.events.read returned an invalid journal retention gap.");
	}
	return {
		events,
		nextCursor: result.next_cursor,
		...(gap
			? {
					gap: {
						missingFrom: gap.missing_from as string,
						missingTo: gap.missing_to as string,
						resyncCursor: gap.resync_cursor as string,
					},
				}
			: {}),
	};
}

function parseLockCommand(command: string): LockCommand {
	const argument = command.slice("/lock".length).trim();
	if (argument === "status") return { action: "status" };
	const forceRelease = /^force-release\s+(\S+)(?:\s+(.+))?$/u.exec(argument);
	if (forceRelease) {
		const leaseId = forceRelease[1] as string;
		const typed = (forceRelease[2] ?? "").trim();
		const confirmation = `CONFIRM FORCE-RELEASE ${leaseId}`;
		return { action: "force-release", leaseId, confirmation, confirmed: typed === confirmation };
	}
	const clearQuarantine = /^clear-quarantine\s+(\S+)(?:\s+(.+))?$/u.exec(argument);
	if (clearQuarantine) {
		const receiptId = clearQuarantine[1] as string;
		const typed = (clearQuarantine[2] ?? "").trim();
		const confirmation = `CONFIRM CLEAR-QUARANTINE ${receiptId}`;
		return { action: "clear-quarantine", receiptId, confirmation, confirmed: typed === confirmation };
	}
	throw new WayConsoleError(
		"Usage: /lock status | /lock force-release <lease_id> CONFIRM FORCE-RELEASE <lease_id> | /lock clear-quarantine <verification_receipt_id> CONFIRM CLEAR-QUARANTINE <verification_receipt_id>",
	);
}

function parseRegistryRows(value: unknown): { readonly rows: readonly RecordValue[]; readonly total: number } {
	const result = recordValue(value);
	if (!Array.isArray(result.rows) || typeof result.total !== "number" || !Number.isFinite(result.total)) {
		throw new WayConsoleError("registry.list returned an invalid response.");
	}
	return { rows: result.rows.map(recordValue), total: result.total };
}

function parseRegistryRow(value: unknown): RecordValue {
	const result = recordValue(value);
	const row = recordValue(result.row);
	if (typeof row.session_id !== "string" || !row.session_id)
		throw new WayConsoleError("registry.get returned an invalid response.");
	return row;
}

function renderRegistryRow(row: RecordValue): string {
	const locator = recordValue(row.locator);
	return `id=${boundedConsoleText(rawStringValue(row.session_id))} status=${boundedConsoleText(rawStringValue(row.status))} live=${booleanValue(row.live)} quarantined=${booleanValue(row.quarantined)} repo=${boundedConsoleText(rawStringValue(locator.repo, "none"), 160)}`;
}

function renderLockStatus(value: unknown): string {
	const status = recordValue(value);
	const holder = recordValue(status.holder);
	const leaseId = holder.lease_id ? boundedConsoleText(rawStringValue(holder.lease_id), 160) : "none";
	const sessionId = holder.session_id ? boundedConsoleText(rawStringValue(holder.session_id), 160) : "none";
	return [
		"Git lock status",
		`  held=${booleanValue(status.held)} lease_id=${leaseId} session_id=${sessionId} queue_len=${Array.isArray(status.queue) ? status.queue.length : "unknown"}`,
		`  stuck=${booleanValue(status.stuck)} quarantined=${booleanValue(status.quarantined)} fencing_token=${boundedConsoleText(rawStringValue(status.fencing_token, "none"), 160)}`,
	].join("\n");
}

function serverRefusalMessage(error: unknown): string {
	if (error instanceof RpcResponseError)
		return boundedConsoleText(error.message.replace(/^RPC\s+\S+\s+failed:\s+-?\d+\s+/u, ""));
	return boundedConsoleText(asError(error).message);
}

/** Resolves only a configured owner surface; an ambiguous profile needs an explicit selection. */
export function resolveConsoleOwnerSurface(profile: WayProfile, requestedSurfaceId?: string): string {
	if (requestedSurfaceId) {
		const owner = profile.ownerSurfaces.find((surface) => surface.id === requestedSurfaceId);
		if (!owner) throw new WayConsoleError(`Console surface ${requestedSurfaceId} is not a configured owner surface.`);
		return owner.id;
	}
	if (profile.ownerSurfaces.length === 1) return profile.ownerSurfaces[0]?.id as string;
	throw new WayConsoleError(
		"The profile defines multiple owner surfaces; pass gajaeway console --surface-id <configured-owner-surface-id>.",
	);
}

/** Thin console-specific wrapper around the shared durable journal consumer. */
export class ConsoleEventConsumer {
	readonly #consumer: RpcJournalConsumer;
	readonly #idleDelayMs: number;

	constructor(options: ConsoleEventConsumerOptions) {
		const consumerId = options.consumerId ?? GAJAEWAY_CONSOLE_CONSUMER_ID;
		const claimTtlMs = boundedInteger(options.claimTtlMs ?? DEFAULT_CONSOLE_CLAIM_TTL_MS, "claimTtlMs", 5_000, 600_000);
		const readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_CONSOLE_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (readWaitMs >= claimTtlMs) throw new WayConsoleError("readWaitMs must be shorter than claimTtlMs.");
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#consumer = new RpcJournalConsumer({
			rpc: options.rpc,
			consumerId,
			claimTtlMs,
			readWaitMs,
			kinds: GAJAEWAY_CONSOLE_EVENT_KINDS,
			now: options.now,
			releaseOnAbort: true,
			errorFactory: (message) => new ConsoleDeliveryUnavailableError(message),
			gapError: (gap) =>
				new ConsoleDeliveryUnavailableError(
					`Console checkpoint ${gap.checkpoint} is behind journal retention. Interactive delivery is disabled; use the supported gateway journal-repair procedure to resync at ${gap.resyncCursor}, then restart gajaeway console.`,
				),
			publish: async (event) => {
				const consoleEvent = toConsoleEvent(event);
				await options.render(consoleEvent);
				const sequence = sequenceString(consoleEvent.seq);
				return { seq: sequence, dedupe_key: `${consumerId}:${sequence}` };
			},
		});
	}

	async ensureReady(): Promise<void> {
		const result = await this.runOnce();
		if (result === "idle" || result === "rendered") return;
	}

	async runOnce(signal?: AbortSignal): Promise<ConsoleConsumerRunResult> {
		try {
			const result = await this.#consumer.runOnce(signal);
			if (result === "claim_held") {
				throw new ConsoleDeliveryUnavailableError(
					"Another gajaeway console currently owns the gajaeway-console journal claim. Close that console or wait for its server-side claim to expire before retrying; no owner input was accepted.",
				);
			}
			return result === "published" ? "rendered" : "idle";
		} catch (error) {
			if (signal?.aborted) throw error;
			if (error instanceof ConsoleDeliveryUnavailableError) throw error;
			throw new ConsoleDeliveryUnavailableError(`Console delivery is unavailable: ${asError(error).message}`);
		}
	}

	async run(signal: AbortSignal): Promise<void> {
		while (!signal.aborted) {
			const result = await this.runOnce(signal);
			if (result === "idle") await sleep(this.#idleDelayMs, signal);
		}
	}
}

/** Gateway-cockpit operations and rendering; all stateful behavior remains on the gateway. */
export class OwnerConsole {
	readonly #rpc: JsonRpcClient;
	readonly #ownerSurfaceId: string;
	readonly #output: ConsoleOutput;
	readonly #idempotencyKey: () => string;
	readonly #consumer: ConsoleEventConsumer;
	readonly #onEvent: ((event: ConsoleEventFrame) => void | Promise<void>) | undefined;
	#deliveryReady = false;
	#deliveryFailure: ConsoleDeliveryUnavailableError | undefined;
	#lastStatusFingerprint: string | undefined;
	#lastStatusPollFailure: string | undefined;

	constructor(options: OwnerConsoleOptions) {
		if (!options.ownerSurfaceId.trim()) throw new WayConsoleError("Console owner surface id must not be empty.");
		this.#rpc = options.rpc;
		this.#ownerSurfaceId = options.ownerSurfaceId;
		this.#output = options.output;
		this.#idempotencyKey = options.idempotencyKey ?? randomUUID;
		this.#onEvent = options.onEvent;
		this.#consumer = new ConsoleEventConsumer({
			rpc: options.rpc,
			consumerId: options.consumerId,
			claimTtlMs: options.claimTtlMs,
			readWaitMs: options.readWaitMs,
			render: async (event) => await this.renderEvent(event),
		});
	}

	/** Queries the non-mutating health/status fence. It never claims journal delivery. */
	async inspectStartup(): Promise<ConsoleStartup> {
		let health: RecordValue;
		let status: RecordValue;
		try {
			const [healthPayload, statusPayload] = await Promise.all([
				rpcResult<unknown>(await this.#rpc.request("way.health", {}, { timeoutMs: 5_000 }), "way.health"),
				rpcResult<unknown>(await this.#rpc.request("way.status", {}, { timeoutMs: 5_000 }), "way.status"),
			]);
			health = recordValue(healthPayload);
			status = recordValue(statusPayload);
			this.#lastStatusFingerprint = statusFingerprint(health, status);
		} catch (error) {
			const refusal = `Refusing interactive console: unable to inspect the gateway over its owner socket: ${asError(error).message}`;
			await this.writeRefusal(refusal);
			return { accepted: false, refusal };
		}
		const decision = consoleStartupDecision(health, status);
		if (!decision.interactive) {
			await this.writeStatusSummary(health, status);
			await this.writeRefusal(decision.refusal ?? "The gateway refused interactive console startup.");
			return { accepted: false, refusal: decision.refusal };
		}
		return { accepted: true, health, status };
	}

	/** Claims and validates delivery only after the caller has installed a terminal. */
	async establishDeliveryReadiness(startup: ConsoleStartup): Promise<ConsoleStartup> {
		if (!startup.accepted || !startup.health || !startup.status) {
			throw new ConsoleDeliveryUnavailableError(
				"Console health/status inspection did not produce an interactive startup state.",
			);
		}
		try {
			await this.#consumer.ensureReady();
			this.#deliveryReady = true;
			return startup;
		} catch (error) {
			const failure = this.markDeliveryFailure(error);
			const refusal = `Refusing interactive console: ${failure.message}`;
			await this.writeRefusal(refusal);
			return { accepted: false, refusal };
		}
	}

	async start(): Promise<ConsoleStartup> {
		const inspected = await this.inspectStartup();
		if (!inspected.accepted || !inspected.health || !inspected.status) return inspected;
		await this.writeStatusSummary(inspected.health, inspected.status);
		return await this.establishDeliveryReadiness(inspected);
	}

	async submit(text: string): Promise<MainSubmitResult> {
		this.assertDeliveryReady();
		if (!text.trim()) throw new WayConsoleError("Owner input must not be empty.");
		const result = parseMainSubmitResult(
			rpcResult<unknown>(
				await this.#rpc.request("main.submit", {
					text,
					surface_id: this.#ownerSurfaceId,
					idempotency_key: this.#idempotencyKey(),
				}),
				"main.submit",
			),
		);
		await this.#output.writeFrame(`Delivered as: ${sanitizeConsoleText(result.deliveredAs)}\n`);
		return result;
	}

	async answerGate(gateId: string, expectedSessionId: string, answer: unknown): Promise<GateAnswerResult> {
		this.assertDeliveryReady();
		const result = parseGateAnswerResult(
			rpcResult<unknown>(
				await this.#rpc.request("main.gate.answer", {
					gate_id: gateId,
					expected_session_id: expectedSessionId,
					answer,
					idempotency_key: this.#idempotencyKey(),
				}),
				"main.gate.answer",
			),
		);
		if (!result.accepted) {
			await this.#output.writeFrame(`Gate ${sanitizeConsoleText(gateId)}: ${EXTERNAL_HOST_GATE_ANSWER_GUIDANCE}\n`);
			return result;
		}
		await this.#output.writeFrame(`Gate ${sanitizeConsoleText(gateId)}: ${sanitizeConsoleText(result.gateState)}\n`);
		return result;
	}

	async refreshStatus(
		options: { readonly onlyIfChanged?: boolean; readonly signal?: AbortSignal } = {},
	): Promise<boolean> {
		const snapshot = await this.#readStatus(options.signal);
		const fingerprint = statusFingerprint(snapshot.health, snapshot.status);
		this.#lastStatusPollFailure = undefined;
		if (options.onlyIfChanged && fingerprint === this.#lastStatusFingerprint) return false;
		this.#lastStatusFingerprint = fingerprint;
		await this.writeStatusSummary(snapshot.health, snapshot.status);
		return true;
	}

	/** Polls health/status without changing admission or consumer ownership. */
	async pollStatus(signal?: AbortSignal): Promise<void> {
		try {
			await this.refreshStatus({ onlyIfChanged: true, signal });
		} catch (error) {
			if (signal?.aborted) return;
			const reason = boundedConsoleText(asError(error).message);
			if (reason === this.#lastStatusPollFailure) return;
			this.#lastStatusPollFailure = reason;
			await this.#output.writeFrame(`Gateway status poll unavailable: ${reason}\n`);
		}
	}

	async tailJournal(request: JournalTailRequest): Promise<void> {
		this.assertDeliveryReady();
		const snapshot = await this.#readStatus();
		const cursor = journalTailCursor(recordValue(snapshot.status.journal).head_cursor, request.count);
		const read = parseJournalTailResult(
			rpcResult<unknown>(
				await this.#rpc.request("main.events.read", {
					cursor,
					limit: request.count,
					wait_ms: 0,
					...(request.kinds === undefined ? {} : { kinds: request.kinds }),
				}),
				"main.events.read",
			),
		);
		const kinds = request.kinds?.join(",") ?? "all";
		if (read.gap) {
			await this.#output.writeFrame(
				`Journal tail retention gap: missing_from=${boundedConsoleText(read.gap.missingFrom)} missing_to=${boundedConsoleText(read.gap.missingTo)} resync_cursor=${boundedConsoleText(read.gap.resyncCursor)}\n`,
			);
			return;
		}
		const lines = read.events.map(
			(event) => `  ${sequenceString(event.seq)} ${event.kind} ${compactGatewayValue(event.payload)}`,
		);
		await this.#output.writeFrame(
			`Journal tail: kinds=${kinds} requested=${request.count} received=${read.events.length} next_cursor=${boundedConsoleText(read.nextCursor)}\n${lines.length > 0 ? `${lines.join("\n")}\n` : "  (no matching events)\n"}`,
		);
	}

	async listRegistry(): Promise<void> {
		this.assertDeliveryReady();
		const listed = parseRegistryRows(
			rpcResult<unknown>(await this.#rpc.request("registry.list", { limit: 100 }), "registry.list"),
		);
		const rows = listed.rows.map((row) => `  ${renderRegistryRow(row)}`);
		await this.#output.writeFrame(
			`Registry list: total=${integerValue(listed.total)} shown=${rows.length}\n${rows.length > 0 ? `${rows.join("\n")}\n` : "  (no registry rows)\n"}`,
		);
	}

	async inspectRegistry(sessionId: string): Promise<void> {
		this.assertDeliveryReady();
		const row = parseRegistryRow(
			rpcResult<unknown>(await this.#rpc.request("registry.get", { session_id: sessionId }), "registry.get"),
		);
		const locator = recordValue(row.locator);
		await this.#output.writeFrame(
			[
				"Registry inspect",
				`  ${renderRegistryRow(row)}`,
				`  kind=${boundedConsoleText(rawStringValue(row.kind))} surface_id=${boundedConsoleText(rawStringValue(row.surface_id, "none"))} source=${boundedConsoleText(rawStringValue(row.source))}`,
				`  activity_state=${boundedConsoleText(rawStringValue(row.activity_state, "none"))} metadata_state=${boundedConsoleText(rawStringValue(row.metadata_state))} locator=${compactGatewayValue(locator, 512)}`,
			].join("\n") + "\n",
		);
	}

	/**
	 * Read-only scheduler view. The console is an acceptance surface, so it
	 * observes jobs and run history but never creates, edits, or fires them;
	 * mutation stays on the authenticated RPC surface.
	 */
	async listSchedule(): Promise<void> {
		this.assertDeliveryReady();
		const listed = recordValue(
			rpcResult<unknown>(await this.#rpc.request("schedule.list", { limit: 100 }), "schedule.list"),
		);
		const jobs = Array.isArray(listed.jobs) ? listed.jobs : [];
		const rows = jobs.map((candidate) => {
			const job = recordValue(candidate);
			return `  ${boundedConsoleText(rawStringValue(job.jobId))} state=${boundedConsoleText(rawStringValue(job.state))} kind=${boundedConsoleText(rawStringValue(job.kind))} payload=${boundedConsoleText(rawStringValue(job.payloadKind))} next_fire_at_ms=${rawStringValue(job.nextFireAtMs === undefined ? "none" : String(job.nextFireAtMs))} failures=${rawStringValue(String(job.failureCount ?? 0))} name=${boundedConsoleText(rawStringValue(job.name))}`;
		});
		await this.#output.writeFrame(
			`Schedule list: shown=${rows.length}\n${rows.length > 0 ? `${rows.join("\n")}\n` : "  (no schedule jobs)\n"}`,
		);
	}

	async listScheduleRuns(jobId: string): Promise<void> {
		this.assertDeliveryReady();
		const listed = recordValue(
			rpcResult<unknown>(await this.#rpc.request("schedule.runs", { job_id: jobId, limit: 50 }), "schedule.runs"),
		);
		const runs = Array.isArray(listed.runs) ? listed.runs : [];
		const rows = runs.map((candidate) => {
			const run = recordValue(candidate);
			return `  ${boundedConsoleText(rawStringValue(run.runId))} outcome=${boundedConsoleText(rawStringValue(run.outcome, "in_flight"))} trigger=${boundedConsoleText(rawStringValue(run.trigger))} attempt=${rawStringValue(String(run.attempt ?? 0))} missed=${rawStringValue(String(run.missedCount ?? 0))} claimed_at=${rawStringValue(String(run.claimedAt ?? 0))}`;
		});
		await this.#output.writeFrame(
			`Schedule runs for ${boundedConsoleText(jobId)}: shown=${rows.length}\n${rows.length > 0 ? `${rows.join("\n")}\n` : "  (no runs)\n"}`,
		);
	}

	async runLockCommand(command: LockCommand): Promise<void> {
		this.assertDeliveryReady();
		if (command.action === "status") {
			const status = rpcResult<unknown>(await this.#rpc.request("gitlock.status", {}), "gitlock.status");
			await this.#output.writeFrame(`${renderLockStatus(status)}\n`);
			return;
		}
		if (!command.confirmed) {
			await this.#output.writeFrame(`Confirmation required. Type exactly: ${command.confirmation}\n`);
			return;
		}
		if (command.action === "force-release") {
			let released: RecordValue;
			try {
				released = recordValue(
					rpcResult<unknown>(
						await this.#rpc.request("gitlock.force_release", {
							lease_id: command.leaseId,
							confirm: true,
							idempotency_key: this.#idempotencyKey(),
						}),
						"gitlock.force_release",
					),
				);
			} catch (error) {
				await this.#output.writeFrame(`Git lock force-release refused: ${serverRefusalMessage(error)}\n`);
				return;
			}
			await this.#output.writeFrame(
				`Git lock force-release: released=${booleanValue(released.released)} held_ms=${integerValue(released.held_ms)}\n`,
			);
			return;
		}
		let status: unknown;
		try {
			status = rpcResult<unknown>(
				await this.#rpc.request("gitlock.clear_quarantine", {
					verification_receipt_id: command.receiptId,
					confirm: true,
					idempotency_key: this.#idempotencyKey(),
				}),
				"gitlock.clear_quarantine",
			);
		} catch (error) {
			await this.#output.writeFrame(`Git lock clear-quarantine refused: ${serverRefusalMessage(error)}\n`);
			return;
		}
		await this.#output.writeFrame(`Git lock clear-quarantine completed.\n${renderLockStatus(status)}\n`);
	}

	async consumeOnce(signal?: AbortSignal): Promise<ConsoleConsumerRunResult> {
		this.assertDeliveryReady();
		try {
			return await this.#consumer.runOnce(signal);
		} catch (error) {
			if (signal?.aborted) throw error;
			const failure = this.markDeliveryFailure(error);
			await this.reportDeliveryFailure(failure);
			throw failure;
		}
	}

	async consume(signal: AbortSignal): Promise<void> {
		this.assertDeliveryReady();
		try {
			await this.#consumer.run(signal);
		} catch (error) {
			if (signal.aborted) return;
			const failure = this.markDeliveryFailure(error);
			await this.reportDeliveryFailure(failure);
			throw failure;
		}
	}

	async handleInput(line: string): Promise<boolean> {
		const command = line.trim();
		if (!command) return true;
		if (command === "/quit" || command === "/exit") return false;
		if (command === "/help") {
			await this.#output.writeFrame(
				[
					"Gateway cockpit commands:",
					"  /status",
					"  /journal [all|kind[,kind...]] [count] (default excludes registry_change)",
					"  /registry list | /registry inspect <session_id>",
					"  /schedule list | /schedule runs <job_id>",
					"  /lock status",
					"  /lock force-release <lease_id> CONFIRM FORCE-RELEASE <lease_id>",
					"  /lock clear-quarantine <verification_receipt_id> CONFIRM CLEAR-QUARANTINE <verification_receipt_id>",
					"  /gate <gate_id> <expected_session_id> <JSON answer> (capability probe; a future backend may support validated gate receipts)",
					`  ${EXTERNAL_HOST_GATE_ANSWER_GUIDANCE}`,
					"  /quit",
					"  Any other line is submitted to the main session.",
				].join("\n") + "\n",
			);
			return true;
		}
		try {
			if (command === "/status") {
				await this.refreshStatus();
				return true;
			}
			if (command === "/journal" || command.startsWith("/journal ")) {
				await this.tailJournal(parseJournalTailCommand(command));
				return true;
			}
			if (command === "/registry list") {
				await this.listRegistry();
				return true;
			}
			const registryInspect = /^\/registry\s+inspect\s+(\S+)$/u.exec(command);
			if (registryInspect) {
				await this.inspectRegistry(registryInspect[1] as string);
				return true;
			}
			if (command === "/registry" || command.startsWith("/registry ")) {
				throw new WayConsoleError("Usage: /registry list | /registry inspect <session_id>");
			}
			if (command === "/schedule list" || command === "/schedule") {
				await this.listSchedule();
				return true;
			}
			const scheduleRuns = /^\/schedule\s+runs\s+(\S+)$/u.exec(command);
			if (scheduleRuns) {
				await this.listScheduleRuns(scheduleRuns[1] as string);
				return true;
			}
			if (command.startsWith("/schedule ")) {
				throw new WayConsoleError("Usage: /schedule list | /schedule runs <job_id>");
			}
			if (command === "/lock" || command.startsWith("/lock ")) {
				await this.runLockCommand(parseLockCommand(command));
				return true;
			}
			if (command.startsWith("/gate")) {
				const gate = parseGateCommand(command);
				await this.answerGate(gate.gateId, gate.expectedSessionId, gate.answer);
				return true;
			}
			await this.submit(line);
			return true;
		} catch (error) {
			if (error instanceof ConsoleDeliveryUnavailableError) return false;
			await this.#output.writeFrame(`Request failed: ${boundedConsoleText(asError(error).message)}\n`);
			return true;
		}
	}

	private assertDeliveryReady(): void {
		if (this.#deliveryFailure) throw this.#deliveryFailure;
		if (!this.#deliveryReady) {
			throw new ConsoleDeliveryUnavailableError(
				"Console delivery readiness has not been established; no owner input was accepted.",
			);
		}
	}

	private markDeliveryFailure(error: unknown): ConsoleDeliveryUnavailableError {
		if (this.#deliveryFailure) return this.#deliveryFailure;
		const failure =
			error instanceof ConsoleDeliveryUnavailableError
				? error
				: new ConsoleDeliveryUnavailableError(`Console delivery is unavailable: ${asError(error).message}`);
		this.#deliveryReady = false;
		this.#deliveryFailure = failure;
		return failure;
	}

	async #readStatus(signal?: AbortSignal): Promise<GatewayStatusSnapshot> {
		const [health, status] = await Promise.all([
			rpcResult<unknown>(
				await this.#rpc.request("way.health", {}, { signal, timeoutMs: DEFAULT_CONSOLE_STATUS_REQUEST_TIMEOUT_MS }),
				"way.health",
			),
			rpcResult<unknown>(
				await this.#rpc.request("way.status", {}, { signal, timeoutMs: DEFAULT_CONSOLE_STATUS_REQUEST_TIMEOUT_MS }),
				"way.status",
			),
		]);
		return { health: recordValue(health), status: recordValue(status) };
	}

	private async writeStatusSummary(health: RecordValue, status: RecordValue): Promise<void> {
		await this.#output.writeFrame(`${renderConsoleStatusSummary(health, status)}\n`);
	}

	private async writeRefusal(message: string): Promise<void> {
		await this.#output.writeFrame(`${sanitizeConsoleText(message)}\n`);
	}

	private async reportDeliveryFailure(failure: ConsoleDeliveryUnavailableError): Promise<void> {
		try {
			await this.#output.writeFrame(`Delivery unavailable; ending console: ${sanitizeConsoleText(failure.message)}\n`);
		} catch {
			// The original publication failure remains authoritative.
		}
	}

	private async renderEvent(event: ConsoleEventFrame): Promise<void> {
		await this.#onEvent?.(event);
		await this.#output.writeFrame(renderConsoleEventFrame(event));
	}
}

/** Composes each journal delivery into one fully sanitized terminal frame. */
function renderConsoleEventFrame(event: ConsoleEventFrame): string {
	switch (event.kind) {
		case "assistant_message": {
			const payload = recordValue(event.payload);
			const text =
				payload.finalized === true && typeof payload.text === "string"
					? sanitizeConsoleText(payload.text)
					: `Assistant message ${sequenceString(event.seq)} had an invalid finalized payload.`;
			return `Assistant:\n${text}\n`;
		}
		case "turn_start":
			return "Main turn started — busy.\n";
		case "turn_end":
			return "Main turn ended — idle.\n";
		case "tail_ring_rotation": {
			const payload = recordValue(event.payload);
			return `WARNING: Lifecycle event-ring retention advanced; resynced from ${compactGatewayValue(payload.prior_watermark)} to ${compactGatewayValue(payload.resync_point)}. Transcript delivery remains authoritative.\n`;
		}
		case "transcript_delivery_gap":
			return `WARNING: Transcript delivery gap detected. Owner-visible transcript continuity may be incomplete: ${compactGatewayValue(event.payload)}\n`;
		case "gate_open": {
			const payload = recordValue(event.payload);
			const gateId = sanitizeConsoleText(rawStringValue(firstValue(payload, ["gate_id", "gateId"])));
			const sessionId = sanitizeConsoleText(rawStringValue(firstValue(payload, ["session_id", "sessionId"])));
			return `Gate opened: gate_id=${gateId} expected_session_id=${sessionId}. ${EXTERNAL_HOST_GATE_ANSWER_GUIDANCE} /gate ${gateId} ${sessionId} <JSON answer> remains a capability probe for a future backend that supports validated gate receipts.\n`;
		}
		case "gate_resolved": {
			const payload = recordValue(event.payload);
			const gateId = sanitizeConsoleText(rawStringValue(firstValue(payload, ["gate_id", "gateId"])));
			return `Gate resolved: gate_id=${gateId}.\n`;
		}
		case "health_change": {
			const payload = recordValue(event.payload);
			const state = sanitizeConsoleText(rawStringValue(payload.state, rawStringValue(payload.status)));
			const reason = typeof payload.reason === "string" ? ` reason=${sanitizeConsoleText(payload.reason)}` : "";
			return `Gateway health changed: ${state}${reason}.\n`;
		}
		case "lock_event":
			return `Lock state changed: ${compactGatewayValue(event.payload)}\n`;
		case "alert_raised":
		case "alert_cleared": {
			const payload = recordValue(event.payload);
			const condition = sanitizeConsoleText(rawStringValue(payload.condition, "unknown"));
			const reason = sanitizeConsoleText(rawStringValue(payload.reason, "unspecified"));
			const verb = event.kind === "alert_raised" ? "RAISED" : "cleared";
			return `Alert ${verb}: ${condition} (${reason}).\n`;
		}
	}
}

/** Runs the interactive raw-terminal owner surface. */
export async function runWayConsole(
	config: Pick<WayConfig, "stateDir" | "profilePath">,
	arguments_: readonly string[] = [],
	dependencies: RunWayConsoleDependencies = {},
): Promise<void> {
	const requestedSurfaceId = parseConsoleArguments(arguments_);
	const profile = dependencies.profile ?? loadWayProfile(config.profilePath);
	const ownerSurfaceId = resolveConsoleOwnerSurface(profile, requestedSurfaceId);
	const exitDrainMs = boundedInteger(
		dependencies.exitDrainMs ?? DEFAULT_CONSOLE_EXIT_DRAIN_MS,
		"exitDrainMs",
		1,
		60_000,
	);
	const statusPollMs = boundedInteger(
		dependencies.statusPollMs ?? DEFAULT_CONSOLE_STATUS_POLL_MS,
		"statusPollMs",
		25,
		60_000,
	);
	const output = new ConsoleOutput(async (text) => await writeToStream(stdout, text));
	let rpc: JsonRpcClient | undefined;
	let terminal: ConsoleTerminal | undefined;
	try {
		rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(path.join(config.stateDir, "rpc.sock"));
		const consoleSurface = new OwnerConsole({
			rpc,
			ownerSurfaceId,
			output,
			idempotencyKey: dependencies.idempotencyKey,
			onEvent: async (event) => terminal?.observeEvent?.(event),
		});
		const inspected = await consoleSurface.inspectStartup();
		if (!inspected.accepted || !inspected.health || !inspected.status) {
			throw new ConsoleStartupRefusalError(inspected.refusal ?? "Interactive console startup was refused.");
		}
		// RawConsoleTerminal validates stdin/stdout before the first consumer claim,
		// read, publication, or commit. Health/status refusals above remain usable
		// through redirected stdout because they never enter delivery readiness.
		terminal = dependencies.terminal ?? new RawConsoleTerminal();
		const activeTerminal = terminal;
		output.setWriter(async (text) => await activeTerminal.writeTrusted(text));
		activeTerminal.setDeliveryState?.("fenced");
		await output.writeFrame(`${renderConsoleStatusSummary(inspected.health, inspected.status)}\n`);
		const startup = await consoleSurface.establishDeliveryReadiness(inspected);
		if (!startup.accepted) {
			activeTerminal.setDeliveryState?.("unavailable");
			throw new ConsoleStartupRefusalError(startup.refusal ?? "Interactive console delivery readiness was refused.");
		}
		activeTerminal.setDeliveryState?.("ready");
		await output.writeFrame("Gateway cockpit ready. Type /help for commands.\n");
		const events = new AbortController();
		const input = new AbortController();
		const status = new AbortController();
		let deliveryFailure: Error | undefined;
		const eventLoop = consoleSurface.consume(events.signal).catch((error) => {
			if (!events.signal.aborted) {
				deliveryFailure = asError(error);
				activeTerminal.setDeliveryState?.("unavailable");
				input.abort();
				activeTerminal.close();
			}
		});
		const statusLoop = pollConsoleStatus(consoleSurface, status.signal, statusPollMs);
		let inputFailure: Error | undefined;
		try {
			await readConsoleInput(activeTerminal, consoleSurface, {
				exitDrainMs,
				signal: input.signal,
				reportOutstanding: async (count) =>
					await output.writeFrame(
						`Exit requested; ${count} console operation${count === 1 ? " is" : "s are"} still outstanding. Results will remain available through the journal at the gajaeway-console consumer checkpoint.\n`,
					),
			});
		} catch (error) {
			inputFailure = asError(error);
		} finally {
			input.abort();
			status.abort();
			events.abort();
			if (!(await waitForConsoleLoopStop(eventLoop, MAX_CONSOLE_EXIT_DIAGNOSTIC_MS))) activeTerminal.close();
			if (!(await waitForConsoleLoopStop(statusLoop, MAX_CONSOLE_EXIT_DIAGNOSTIC_MS))) activeTerminal.close();
		}
		if (deliveryFailure) throw deliveryFailure;
		if (inputFailure) throw inputFailure;
	} finally {
		rpc?.close();
		terminal?.close();
	}
}

/** Keeps the cockpit rail current without owning state or retrying mutations. */
async function pollConsoleStatus(consoleSurface: OwnerConsole, signal: AbortSignal, intervalMs: number): Promise<void> {
	while (!signal.aborted) {
		await sleep(intervalMs, signal);
		if (signal.aborted) return;
		try {
			await consoleSurface.pollStatus(signal);
		} catch {
			// A terminal publication failure is handled by its owning run loop.
			return;
		}
	}
}
interface ConsoleInputLoopOptions {
	readonly exitDrainMs: number;
	readonly signal?: AbortSignal;
	reportOutstanding(count: number): Promise<void>;
}

/**
 * Starts each owner operation as soon as its line arrives so a busy prompt does
 * not prevent a later /gate or steer from reaching the gateway. Calls are
 * initiated in terminal-line order on the single RPC client; only completion
 * frames may arrive later. The bound prevents untrusted input from building an
 * unbounded local operation backlog.
 */
async function readConsoleInput(
	terminal: ConsoleTerminal,
	consoleSurface: OwnerConsole,
	options: ConsoleInputLoopOptions,
): Promise<void> {
	const inFlight = new Set<Promise<void>>();
	const exit = new AbortController();
	const removeExitListener = terminal.onExitRequested?.(() => exit.abort());
	let stopped = false;
	let teardownImmediately = false;
	let failure: Error | undefined;
	const dispatch = (line: string): void => {
		const operation = consoleSurface
			.handleInput(line)
			.then((keepReading) => {
				if (keepReading) return;
				teardownImmediately = true;
				stopped = true;
				terminal.close();
			})
			.catch((error) => {
				failure = asError(error);
				teardownImmediately = true;
				stopped = true;
				terminal.close();
			});
		inFlight.add(operation);
		void operation.finally(() => inFlight.delete(operation));
	};

	try {
		while (!stopped && !options.signal?.aborted && !exit.signal.aborted) {
			while (
				!stopped &&
				!options.signal?.aborted &&
				!exit.signal.aborted &&
				inFlight.size >= MAX_CONSOLE_INPUT_OPERATIONS
			) {
				await waitForInputSlot(inFlight, [options.signal, exit.signal]);
			}
			if (stopped || options.signal?.aborted || exit.signal.aborted) break;
			const line = await terminal.readLine("gajaeway> ");
			if (line === undefined) break;
			const command = line.trim();
			if (command === "/quit" || command === "/exit") break;
			dispatch(line);
		}

		if (!teardownImmediately && !options.signal?.aborted && inFlight.size > 0) {
			const drainResult = await drainInputOperations(inFlight, options.exitDrainMs, options.signal);
			if (drainResult === "expired" && !options.signal?.aborted && inFlight.size > 0) {
				const published = await publishExitOutstandingFrame(
					options.reportOutstanding(inFlight.size),
					Math.min(options.exitDrainMs, MAX_CONSOLE_EXIT_DIAGNOSTIC_MS),
				);
				if (!published) terminal.close();
			}
		}
		if (failure) throw failure;
	} finally {
		removeExitListener?.();
	}
}

async function drainInputOperations(
	inFlight: ReadonlySet<Promise<void>>,
	graceMs: number,
	signal?: AbortSignal,
): Promise<"settled" | "expired" | "aborted"> {
	if (inFlight.size === 0) return "settled";
	if (signal?.aborted) return "aborted";
	return await new Promise((resolve) => {
		let finished = false;
		let timeout: ReturnType<typeof setTimeout> | undefined;
		const finish = (result: "settled" | "expired" | "aborted"): void => {
			if (finished) return;
			finished = true;
			if (timeout) clearTimeout(timeout);
			signal?.removeEventListener("abort", onAbort);
			resolve(result);
		};
		const onAbort = () => finish("aborted");
		timeout = setTimeout(() => finish("expired"), graceMs);
		signal?.addEventListener("abort", onAbort, { once: true });
		void Promise.allSettled([...inFlight]).then(() => finish("settled"));
	});
}

/** A stalled serialized terminal tail must not turn graceful exit into an unbounded wait. */
async function publishExitOutstandingFrame(publication: Promise<void>, timeoutMs: number): Promise<boolean> {
	return await new Promise((resolve) => {
		let finished = false;
		const finish = (published: boolean): void => {
			if (finished) return;
			finished = true;
			clearTimeout(timeout);
			resolve(published);
		};
		const timeout = setTimeout(() => finish(false), timeoutMs);
		void publication.then(
			() => finish(true),
			() => finish(false),
		);
	});
}

/** A journal publisher stalled behind terminal output cannot block process teardown. */
async function waitForConsoleLoopStop(loop: Promise<void>, timeoutMs: number): Promise<boolean> {
	return await new Promise((resolve) => {
		let finished = false;
		const finish = (stopped: boolean): void => {
			if (finished) return;
			finished = true;
			clearTimeout(timeout);
			resolve(stopped);
		};
		const timeout = setTimeout(() => finish(false), timeoutMs);
		void loop.then(
			() => finish(true),
			() => finish(true),
		);
	});
}

async function waitForInputSlot(
	inFlight: ReadonlySet<Promise<void>>,
	signals: readonly (AbortSignal | undefined)[],
): Promise<void> {
	if (signals.some((signal) => signal?.aborted)) return;
	await new Promise<void>((resolve) => {
		let settled = false;
		const finish = (): void => {
			if (settled) return;
			settled = true;
			for (const signal of signals) signal?.removeEventListener("abort", finish);
			resolve();
		};
		for (const signal of signals) signal?.addEventListener("abort", finish, { once: true });
		void Promise.race(inFlight).then(finish, finish);
	});
}

function parseConsoleArguments(arguments_: readonly string[]): string | undefined {
	let surfaceId: string | undefined;
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index];
		if (argument !== "--surface-id") throw new WayConsoleError(`Unknown gajaeway console argument: ${argument}`);
		if (surfaceId) throw new WayConsoleError("gajaeway console --surface-id may only be supplied once.");
		const value = arguments_[index + 1];
		if (!value?.trim()) throw new WayConsoleError("gajaeway console --surface-id requires a value.");
		surfaceId = value;
		index += 1;
	}
	return surfaceId;
}

function parseMainSubmitResult(value: unknown): MainSubmitResult {
	if (
		!isRecord(value) ||
		value.accepted !== true ||
		typeof value.op_ref !== "string" ||
		typeof value.delivered_as !== "string"
	) {
		throw new WayConsoleError("main.submit returned an invalid response.");
	}
	return { accepted: true, opRef: value.op_ref, deliveredAs: value.delivered_as };
}

function parseGateAnswerResult(value: unknown): GateAnswerResult {
	if (!isRecord(value) || typeof value.gate_state !== "string") {
		throw new WayConsoleError("main.gate.answer returned an invalid response.");
	}
	if (value.accepted === true) return { accepted: true, gateState: value.gate_state };
	if (value.accepted === false && value.gate_state === "unsupported")
		return { accepted: false, gateState: "unsupported" };
	throw new WayConsoleError("main.gate.answer returned an invalid response.");
}

function parseGateCommand(command: string): { gateId: string; expectedSessionId: string; answer: unknown } {
	const match = /^\/gate\s+(\S+)\s+(\S+)\s+(.+)$/s.exec(command);
	if (!match) throw new WayConsoleError("Usage: /gate <gate_id> <expected_session_id> <JSON answer>");
	try {
		return {
			gateId: match[1] as string,
			expectedSessionId: match[2] as string,
			answer: JSON.parse(match[3] as string),
		};
	} catch {
		throw new WayConsoleError("Gate answers must be valid JSON.");
	}
}

function toConsoleEvent(event: RpcJournalEvent): ConsoleEventFrame {
	if (!GAJAEWAY_CONSOLE_EVENT_KINDS.includes(event.kind as WayConsoleEventKind)) {
		throw new ConsoleDeliveryUnavailableError(
			`main.events.read returned an unsupported console event kind: ${event.kind}`,
		);
	}
	return {
		seq: event.seq,
		...(event.ts === undefined ? {} : { ts: event.ts }),
		kind: event.kind as WayConsoleEventKind,
		payload: event.payload,
	};
}

function sequenceString(value: string | number): string {
	if (typeof value === "string" && /^\d+$/.test(value)) return value;
	if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return String(value);
	throw new WayConsoleError("Journal event sequence must be an unsigned safe integer.");
}

function boundedInteger(value: number, name: string, minimum: number, maximum: number): number {
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new WayConsoleError(`${name} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
	if (milliseconds === 0 || signal.aborted) return Promise.resolve();
	return new Promise((resolve) => {
		const timeout = setTimeout(finish, milliseconds);
		const onAbort = () => finish();
		function finish(): void {
			clearTimeout(timeout);
			signal.removeEventListener("abort", onAbort);
			resolve();
		}
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

function writeToStream(stream: RawConsoleOutputStream, text: string): Promise<void> {
	if (!text) return Promise.resolve();
	return new Promise((resolve, reject) => {
		let callbackDone = false;
		let drainDone = true;
		let writeReturned = false;
		let settled = false;
		const finish = () => {
			if (settled || !writeReturned || !callbackDone || !drainDone) return;
			settled = true;
			resolve();
		};
		const fail = (error: Error) => {
			if (settled) return;
			settled = true;
			reject(error);
		};
		const onDrain = () => {
			drainDone = true;
			finish();
		};
		try {
			const accepted = stream.write(text, (error) => {
				if (error) {
					fail(error);
					return;
				}
				callbackDone = true;
				finish();
			});
			drainDone = accepted;
			writeReturned = true;
			if (!accepted) stream.once("drain", onDrain);
			finish();
		} catch (error) {
			fail(asError(error));
		}
	});
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}
