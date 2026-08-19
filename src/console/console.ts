import { randomUUID } from "node:crypto";
import * as path from "node:path";
import { stdin, stdout } from "node:process";
import { RpcJournalConsumer, type RpcJournalEvent } from "../journal-consumer";
import type { WayConfig } from "../config";
import { loadWayProfile, type WayProfile } from "../profile";
import { RpcClient, rpcResult, type JsonRpcClient } from "../rpc-client";

export const GAJAEWAY_CONSOLE_CONSUMER_ID = "gajaeway-console";
export const GAJAEWAY_CONSOLE_EVENT_KINDS = [
	"assistant_message",
	"turn_start",
	"turn_end",
	"gate_open",
	"gate_resolved",
	"health_change",
	"lock_event",
] as const;
export const DEFAULT_CONSOLE_CLAIM_TTL_MS = 5_000;
export const DEFAULT_CONSOLE_READ_WAIT_MS = 1_000;
export const DEFAULT_CONSOLE_EXIT_DRAIN_MS = 2_000;
const MAX_CONSOLE_INPUT_OPERATIONS = 16;
const MAX_CONSOLE_EXIT_DIAGNOSTIC_MS = 250;
export const MAX_RAW_CONSOLE_QUEUED_LINES = 16;
export const MAX_RAW_CONSOLE_QUEUED_BYTES = 64 * 1024;
export const MAX_RAW_CONSOLE_LINE_BYTES = 8 * 1024;

export type WayConsoleEventKind = (typeof GAJAEWAY_CONSOLE_EVENT_KINDS)[number];
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
}

const RAW_CONSOLE_REFUSAL_CAUSES = ["oversized-line", "queue-full-lines", "queue-full-bytes"] as const;

type RawConsoleRefusalCause = (typeof RAW_CONSOLE_REFUSAL_CAUSES)[number];
type RawConsoleRefusalCounts = Record<RawConsoleRefusalCause, number>;

interface RawConsoleRefusal {
	readonly counts: RawConsoleRefusalCounts;
	revision: number;
}
interface RawConsoleInputStream {
	readonly isTTY?: boolean;
	readonly isRaw?: boolean;
	setEncoding(encoding: BufferEncoding): unknown;
	setRawMode?(mode: boolean): unknown;
	resume(): unknown;
	pause(): unknown;
	on(event: "data", listener: (chunk: string | Buffer) => void): unknown;
	off(event: "data", listener: (chunk: string | Buffer) => void): unknown;
}

interface RawConsoleOutputStream {
	readonly isTTY?: boolean;
	write(text: string, callback: (error?: Error | null) => void): boolean;
	once(event: "drain", listener: () => void): unknown;
}

export interface RawConsoleTerminalOptions {
	readonly input?: RawConsoleInputStream;
	readonly output?: RawConsoleOutputStream;
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
}

export interface RunWayConsoleDependencies {
	readonly rpcConnect?: (socketPath: string) => Promise<JsonRpcClient>;
	readonly profile?: WayProfile;
	readonly terminal?: ConsoleTerminal;
	readonly idempotencyKey?: () => string;
	/** Test seam for bounded graceful exit behavior. */
	readonly exitDrainMs?: number;
}

interface MainSubmitResult {
	readonly accepted: true;
	readonly opRef: string;
	readonly deliveredAs: string;
}

interface GateAnswerResult {
	readonly accepted: true;
	readonly gateState: string;
}

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
	return sanitizeConsoleText(rawStringValue(value, fallback));
}

function booleanValue(value: unknown): string {
	return typeof value === "boolean" ? String(value) : "unknown";
}

function integerValue(value: unknown): string {
	return typeof value === "number" && Number.isFinite(value) ? String(value) : "unknown";
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

/** Renders the state required before an owner can trust an interactive prompt. */
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
		`  daemon: status=${stringValue(health.status)} state=${stringValue(health.state)}${reason ? ` reason=${sanitizeConsoleText(reason)}` : ""}`,
		`  main: resumed=${booleanValue(main.resumed)} session_id=${stringValue(main.session_id, "none")} turn_state=${stringValue(status.turn_state)} follow_up_queue_depth=${integerValue(status.follow_up_queue_depth)}`,
		`  journal: head_cursor=${stringValue(journal.head_cursor)} degraded=${booleanValue(journal.degraded)}`,
		`  lock: held=${booleanValue(lock.held)} holder=${holderDescription} queue_len=${integerValue(lock.queue_len)} stuck=${booleanValue(lock.stuck)} quarantined=${booleanValue(lock.quarantined)} write_mode=${booleanValue(status.write_mode)}`,
		`  reconcile: ${reconcileFreshness} drift_count=${integerValue(reconcile.drift_count)}`,
	].join("\n");
}

function renderReconcileFreshness(lastOkAt: number | undefined, cycleMs: number | undefined, now: number): string {
	if (lastOkAt === undefined) return `freshness=unknown last_ok_at=none cycle_ms=${cycleMs ?? "unknown"}`;
	const ageMs = Math.max(0, now - lastOkAt);
	const staleAfterMs = Math.max((cycleMs ?? 15_000) * 2, 30_000);
	return `freshness=${ageMs <= staleAfterMs ? "fresh" : "stale"} last_ok_at=${lastOkAt} age_ms=${ageMs} cycle_ms=${cycleMs ?? "unknown"}`;
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

/** Owner-facing operations and rendering; all stateful behavior remains on the gateway. */
export class OwnerConsole {
	readonly #rpc: JsonRpcClient;
	readonly #ownerSurfaceId: string;
	readonly #output: ConsoleOutput;
	readonly #idempotencyKey: () => string;
	readonly #consumer: ConsoleEventConsumer;
	#deliveryReady = false;
	#deliveryFailure: ConsoleDeliveryUnavailableError | undefined;

	constructor(options: OwnerConsoleOptions) {
		if (!options.ownerSurfaceId.trim()) throw new WayConsoleError("Console owner surface id must not be empty.");
		this.#rpc = options.rpc;
		this.#ownerSurfaceId = options.ownerSurfaceId;
		this.#output = options.output;
		this.#idempotencyKey = options.idempotencyKey ?? randomUUID;
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
		await this.#output.writeFrame(`Gate ${sanitizeConsoleText(gateId)}: ${sanitizeConsoleText(result.gateState)}\n`);
		return result;
	}

	async refreshStatus(): Promise<void> {
		const [health, status] = await Promise.all([
			rpcResult<unknown>(await this.#rpc.request("way.health", {}, { timeoutMs: 5_000 }), "way.health"),
			rpcResult<unknown>(await this.#rpc.request("way.status", {}, { timeoutMs: 5_000 }), "way.status"),
		]);
		await this.#output.writeFrame(`${renderConsoleStatusSummary(health, status)}\n`);
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
				"Commands: /status, /gate <gate_id> <expected_session_id> <JSON answer>, /quit. Any other line is submitted to the main session.\n",
			);
			return true;
		}
		try {
			if (command === "/status") {
				await this.refreshStatus();
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
			await this.#output.writeFrame(`Request failed: ${sanitizeConsoleText(asError(error).message)}\n`);
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
		case "gate_open": {
			const payload = recordValue(event.payload);
			const gateId = sanitizeConsoleText(rawStringValue(firstValue(payload, ["gate_id", "gateId"])));
			const sessionId = sanitizeConsoleText(rawStringValue(firstValue(payload, ["session_id", "sessionId"])));
			return `Gate opened: gate_id=${gateId} expected_session_id=${sessionId}. Answer with /gate ${gateId} ${sessionId} <JSON answer>.\n`;
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
			return `Lock state changed: ${sanitizeConsoleText(describeGatewayValue(event.payload))}\n`;
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
		await output.writeFrame(`${renderConsoleStatusSummary(inspected.health, inspected.status)}\n`);
		const startup = await consoleSurface.establishDeliveryReadiness(inspected);
		if (!startup.accepted) {
			throw new ConsoleStartupRefusalError(startup.refusal ?? "Interactive console delivery readiness was refused.");
		}
		await output.writeFrame("Owner console ready. Type /help for commands.\n");
		const events = new AbortController();
		const input = new AbortController();
		let deliveryFailure: Error | undefined;
		const eventLoop = consoleSurface.consume(events.signal).catch((error) => {
			if (!events.signal.aborted) {
				deliveryFailure = asError(error);
				input.abort();
				activeTerminal.close();
			}
		});
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
			events.abort();
			if (!(await waitForConsoleLoopStop(eventLoop, MAX_CONSOLE_EXIT_DIAGNOSTIC_MS))) activeTerminal.close();
		}
		if (deliveryFailure) throw deliveryFailure;
		if (inputFailure) throw inputFailure;
	} finally {
		rpc?.close();
		terminal?.close();
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

export class RawConsoleTerminal implements ConsoleTerminal {
	readonly #input: RawConsoleInputStream;
	readonly #output: RawConsoleOutputStream;
	#buffer = "";
	#bufferBytes = 0;
	readonly #queuedLines: Array<{ readonly text: string; readonly bytes: number }> = [];
	#queuedBytes = 0;
	#pendingRefusal: RawConsoleRefusal | undefined;
	#discardingOversizeLine = false;
	#discardingRefusedLine = false;
	#prompt = "";
	#resolveLine: ((line: string | undefined) => void) | undefined;
	readonly #exitListeners = new Set<() => void>();
	#exitRequested = false;
	#inputPaused = false;
	#echoDirty = false;
	#echoFrame = "";
	#rawPublicationPending = false;
	#rawPublicationKind: "echo" | "refusal" | undefined;
	#writeTail: Promise<void> = Promise.resolve();
	#closed = false;

	constructor(options: RawConsoleTerminalOptions = {}) {
		this.#input = options.input ?? stdin;
		this.#output = options.output ?? stdout;
		if (!this.#input.isTTY || !this.#output.isTTY || !this.#input.setRawMode) {
			throw new WayConsoleError("gajaeway console requires an interactive TTY on stdin and stdout.");
		}
		this.#input.setEncoding("utf8");
		this.#input.setRawMode(true);
		this.#input.resume();
		this.#input.on("data", this.onData);
	}

	get queuedLineCount(): number {
		return this.#queuedLines.length;
	}

	get queuedInputBytes(): number {
		return this.#queuedBytes;
	}

	/** Bytes retained for the unterminated raw input line. */
	get bufferedInputBytes(): number {
		return this.#bufferBytes;
	}

	get inputPaused(): boolean {
		return this.#inputPaused;
	}

	/** At most one coalesced echo redraw is retained while stdout is busy. */
	get pendingEchoRedrawCount(): number {
		return this.#echoDirty ? 1 : 0;
	}

	/** The raw terminal has at most one queued or active echo/refusal publisher. */
	get rawPublicationPending(): boolean {
		return this.#rawPublicationPending;
	}

	/** At most one refusal diagnostic is retained until publication or episode end. */
	get pendingRefusalPublicationCount(): number {
		return this.#pendingRefusal ? 1 : 0;
	}

	onExitRequested(listener: () => void): () => void {
		this.#exitListeners.add(listener);
		if (this.#exitRequested) listener();
		return () => this.#exitListeners.delete(listener);
	}

	async writeTrusted(text: string): Promise<void> {
		if (this.#closed) throw new WayConsoleError("Console terminal is closed.");
		await this.write(this.formatTrustedFrame(text));
	}

	async readLine(prompt: string): Promise<string | undefined> {
		if (this.#closed || this.#exitRequested) return undefined;
		if (this.#resolveLine) throw new WayConsoleError("Console already has a pending input line.");
		const queued = this.#queuedLines.shift();
		if (queued) {
			this.#queuedBytes -= queued.bytes;
			return queued.text;
		}
		this.#prompt = prompt;
		const line = new Promise<string | undefined>((resolve) => {
			this.#resolveLine = resolve;
		});
		if (this.#buffer) return await line;
		try {
			await this.write(prompt);
		} catch (error) {
			this.finishLine(undefined);
			throw error;
		}
		return await line;
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#inputPaused = true;
		this.#echoDirty = false;
		this.clearPendingRefusal();
		this.#input.off("data", this.onData);
		this.#input.pause();
		if (this.#input.isRaw) this.#input.setRawMode?.(false);
		this.finishLine(undefined);
	}

	private onData = (chunk: string | Buffer): void => {
		for (const character of String(chunk)) {
			if (character === "\u0003" || character === "\u0004") {
				this.requestExit();
				return;
			}
			if (character === "\r" || character === "\n") {
				if (this.#discardingOversizeLine || this.#discardingRefusedLine) {
					this.#discardingOversizeLine = false;
					this.#discardingRefusedLine = false;
					this.#buffer = "";
					this.#bufferBytes = 0;
					continue;
				}
				const line = this.#buffer;
				const prompt = this.#prompt;
				this.#buffer = "";
				this.#bufferBytes = 0;
				if (this.acceptLine(line)) this.requestCompletedLineEcho(prompt, line);
				continue;
			}
			if (character === "\u007f" || character === "\b") {
				if (!this.#buffer || this.#discardingOversizeLine || this.#discardingRefusedLine) continue;
				this.#buffer = [...this.#buffer].slice(0, -1).join("");
				this.#bufferBytes = Buffer.byteLength(this.#buffer);
				if (this.#resolveLine) this.requestInputEcho("\b \b");
				continue;
			}
			if (character < " " || this.#discardingOversizeLine || this.#discardingRefusedLine) continue;
			const queueFullCause = this.#resolveLine ? undefined : this.queueFullRefusalCause(0);
			if (queueFullCause) {
				this.#discardingRefusedLine = true;
				this.#buffer = "";
				this.#bufferBytes = 0;
				this.reportInputRefusal(queueFullCause);
				continue;
			}
			const bytes = Buffer.byteLength(character);
			if (this.#bufferBytes + bytes > MAX_RAW_CONSOLE_LINE_BYTES) {
				this.#discardingOversizeLine = true;
				this.#buffer = "";
				this.#bufferBytes = 0;
				this.#echoDirty = false;
				this.#echoFrame = "";
				this.reportInputRefusal("oversized-line");
				continue;
			}
			this.#buffer += character;
			this.#bufferBytes += bytes;
			if (this.#resolveLine) this.requestInputEcho(character);
		}
	};

	private acceptLine(line: string): boolean {
		if (this.#resolveLine) {
			this.finishLine(line);
			return true;
		}
		const bytes = Buffer.byteLength(line);
		const queueFullCause = this.queueFullRefusalCause(bytes);
		if (queueFullCause) {
			this.reportInputRefusal(queueFullCause);
			return false;
		}
		this.#queuedLines.push({ text: line, bytes });
		this.#queuedBytes += bytes;
		return true;
	}

	private queueFullRefusalCause(additionalBytes: number): "queue-full-lines" | "queue-full-bytes" | undefined {
		if (this.#queuedLines.length >= MAX_RAW_CONSOLE_QUEUED_LINES) return "queue-full-lines";
		if (this.#queuedBytes + additionalBytes > MAX_RAW_CONSOLE_QUEUED_BYTES) return "queue-full-bytes";
		return undefined;
	}

	private requestExit(): void {
		if (this.#exitRequested) return;
		this.#exitRequested = true;
		this.#echoDirty = false;
		this.clearPendingRefusal();
		this.refreshInputFlow();
		this.finishLine(undefined);
		for (const listener of [...this.#exitListeners]) listener();
	}

	private refreshInputFlow(): void {
		const shouldPause = this.#closed || this.#exitRequested;
		if (shouldPause === this.#inputPaused) return;
		this.#inputPaused = shouldPause;
		if (shouldPause) this.#input.pause();
		else this.#input.resume();
	}

	private requestInputEcho(text: string): void {
		const frame = this.#echoDirty || this.#rawPublicationKind === "echo" ? this.inputRedrawFrame() : text;
		this.requestEchoFrame(frame);
	}

	private requestCompletedLineEcho(prompt: string, line: string): void {
		this.requestEchoFrame(`\r\x1b[2K${prompt}${line}\r\n`);
	}

	private inputRedrawFrame(): string {
		return `\r\x1b[2K${this.#prompt}${this.#buffer}`;
	}

	private requestEchoFrame(frame: string): void {
		if (this.#closed || this.#exitRequested || !frame) return;
		this.#echoFrame = frame;
		this.#echoDirty = true;
		this.scheduleRawPublication();
	}

	private reportInputRefusal(cause: RawConsoleRefusalCause): void {
		if (this.#closed || this.#exitRequested) return;
		const refusal = this.#pendingRefusal ?? {
			counts: {
				"oversized-line": 0,
				"queue-full-lines": 0,
				"queue-full-bytes": 0,
			},
			revision: 0,
		};
		refusal.counts[cause] += 1;
		refusal.revision += 1;
		this.#pendingRefusal = refusal;
		this.scheduleRawPublication();
	}

	private clearPendingRefusal(): void {
		this.#pendingRefusal = undefined;
	}

	private snapshotRefusalCounts(counts: RawConsoleRefusalCounts): RawConsoleRefusalCounts {
		return {
			"oversized-line": counts["oversized-line"],
			"queue-full-lines": counts["queue-full-lines"],
			"queue-full-bytes": counts["queue-full-bytes"],
		};
	}

	private renderInputRefusal(counts: RawConsoleRefusalCounts): string {
		const causes: string[] = [];
		if (counts["oversized-line"] > 0) {
			causes.push(
				`oversized-line=${counts["oversized-line"]} (Input line exceeds ${MAX_RAW_CONSOLE_LINE_BYTES} bytes and was refused.)`,
			);
		}
		if (counts["queue-full-lines"] > 0) {
			causes.push(
				`queue-full-lines=${counts["queue-full-lines"]} (Input queue is full (${MAX_RAW_CONSOLE_QUEUED_LINES} lines / ${MAX_RAW_CONSOLE_QUEUED_BYTES} bytes); additional pasted input was refused.)`,
			);
		}
		if (counts["queue-full-bytes"] > 0) {
			causes.push(
				`queue-full-bytes=${counts["queue-full-bytes"]} (Input queue byte capacity is full (${MAX_RAW_CONSOLE_QUEUED_BYTES} bytes); additional pasted input was refused.)`,
			);
		}
		return `Input refused: ${causes.join("; ")}\n`;
	}

	/** Keeps counts merged during a backpressured refusal write for the next bounded frame. */
	private settlePublishedRefusal(
		refusal: RawConsoleRefusal,
		publishedCounts: RawConsoleRefusalCounts,
		publishedRevision: number,
	): void {
		if (this.#pendingRefusal !== refusal) return;
		if (refusal.revision === publishedRevision) {
			this.clearPendingRefusal();
			return;
		}
		for (const cause of RAW_CONSOLE_REFUSAL_CAUSES) {
			refusal.counts[cause] -= publishedCounts[cause];
		}
	}

	private scheduleRawPublication(): void {
		if (this.#closed || this.#rawPublicationPending || (!this.#echoDirty && !this.#pendingRefusal)) return;
		this.#rawPublicationPending = true;
		void this.enqueuePublication(async () => await this.publishRawPublication());
	}

	private async publishRawPublication(): Promise<void> {
		try {
			const refusal = this.#pendingRefusal;
			if (refusal) {
				const counts = this.snapshotRefusalCounts(refusal.counts);
				const revision = refusal.revision;
				this.#rawPublicationKind = "refusal";
				await writeToStream(this.#output, this.formatTrustedFrame(this.renderInputRefusal(counts)));
				this.settlePublishedRefusal(refusal, counts, revision);
				return;
			}
			if (!this.#echoDirty || this.#closed || this.#exitRequested) return;
			const frame = this.#echoFrame;
			this.#echoDirty = false;
			this.#rawPublicationKind = "echo";
			await writeToStream(this.#output, frame);
		} catch (error) {
			this.close();
			throw error;
		} finally {
			this.#rawPublicationKind = undefined;
			this.#rawPublicationPending = false;
			this.scheduleRawPublication();
		}
	}

	private formatTrustedFrame(text: string): string {
		return this.#resolveLine ? `\r\x1b[2K${text}${this.#prompt}${this.#buffer}` : text;
	}

	private enqueuePublication(publish: () => Promise<void>): Promise<void> {
		const publication = this.#writeTail.then(publish);
		this.#writeTail = publication.catch(() => undefined);
		return publication;
	}

	private async write(text: string): Promise<void> {
		await this.enqueuePublication(async () => await writeToStream(this.#output, text));
	}

	private finishLine(line: string | undefined): void {
		const resolve = this.#resolveLine;
		this.#resolveLine = undefined;
		this.#buffer = "";
		this.#bufferBytes = 0;
		this.#discardingOversizeLine = false;
		this.#discardingRefusedLine = false;
		this.#prompt = "";
		resolve?.(line);
	}
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
	if (!isRecord(value) || value.accepted !== true || typeof value.gate_state !== "string") {
		throw new WayConsoleError("main.gate.answer returned an invalid response.");
	}
	return { accepted: true, gateState: value.gate_state };
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

function describeGatewayValue(value: unknown): string {
	try {
		return JSON.stringify(value) ?? String(value);
	} catch {
		return "[unserializable gateway payload]";
	}
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
