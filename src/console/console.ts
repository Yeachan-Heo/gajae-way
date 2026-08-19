import { randomUUID } from "node:crypto";
import * as path from "node:path";
import { stdin, stdout } from "node:process";
import {
	RpcJournalConsumer,
	type JournalDeliveryProof,
	type RpcJournalEvent,
} from "../journal-consumer";
import type { WayConfig } from "../config";
import { loadWayProfile, type WayProfile } from "../profile";
import { RpcClient, rpcResult, type JsonRpcClient } from "../rpc-client";

export const WAY_CONSOLE_CONSUMER_ID = "way-console";
export const WAY_CONSOLE_EVENT_KINDS = [
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

export type WayConsoleEventKind = (typeof WAY_CONSOLE_EVENT_KINDS)[number];
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

/** Raw-terminal operations reserved for console-controlled text and ANSI. */
export interface ConsoleTerminal {
	writeTrusted(text: string): Promise<void>;
	readLine(prompt: string): Promise<string | undefined>;
	close(): void;
}

/**
 * Separates console-owned terminal control text from data obtained through RPC.
 * Untrusted writes are escaped before they can reach the raw terminal.
 */
export class ConsoleOutput {
	#write: ConsoleWrite;

	constructor(write: ConsoleWrite) {
		this.#write = write;
	}

	setWriter(write: ConsoleWrite): void {
		this.#write = write;
	}

	async writeTrusted(text: string): Promise<void> {
		await this.#write(text);
	}

	async writeUntrusted(text: string): Promise<void> {
		await this.writeTrusted(sanitizeConsoleText(text));
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
	const states = records.map(record => rawStringValue(record.state));
	const statuses = records.map(record => rawStringValue(record.status));
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
	if (statuses.some(status => status !== "healthy")) {
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
	const lastOkAt = typeof reconcile.last_ok_at === "number" && Number.isFinite(reconcile.last_ok_at) ? reconcile.last_ok_at : undefined;
	const cycleMs = typeof reconcile.cycle_ms === "number" && Number.isFinite(reconcile.cycle_ms) ? reconcile.cycle_ms : undefined;
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
		const owner = profile.ownerSurfaces.find(surface => surface.id === requestedSurfaceId);
		if (!owner) throw new WayConsoleError(`Console surface ${requestedSurfaceId} is not a configured owner surface.`);
		return owner.id;
	}
	if (profile.ownerSurfaces.length === 1) return profile.ownerSurfaces[0]?.id as string;
	throw new WayConsoleError("The profile defines multiple owner surfaces; pass way console --surface-id <configured-owner-surface-id>.");
}

/** Thin console-specific wrapper around the shared durable journal consumer. */
export class ConsoleEventConsumer {
	readonly #consumer: RpcJournalConsumer;
	readonly #idleDelayMs: number;

	constructor(options: ConsoleEventConsumerOptions) {
		const consumerId = options.consumerId ?? WAY_CONSOLE_CONSUMER_ID;
		const claimTtlMs = boundedInteger(options.claimTtlMs ?? DEFAULT_CONSOLE_CLAIM_TTL_MS, "claimTtlMs", 5_000, 600_000);
		const readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_CONSOLE_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (readWaitMs >= claimTtlMs) throw new WayConsoleError("readWaitMs must be shorter than claimTtlMs.");
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#consumer = new RpcJournalConsumer({
			rpc: options.rpc,
			consumerId,
			claimTtlMs,
			readWaitMs,
			kinds: WAY_CONSOLE_EVENT_KINDS,
			now: options.now,
			releaseOnAbort: true,
			errorFactory: message => new ConsoleDeliveryUnavailableError(message),
			gapError: gap =>
				new ConsoleDeliveryUnavailableError(
					`Console checkpoint ${gap.checkpoint} is behind journal retention. Interactive delivery is disabled; use the supported gateway journal-repair procedure to resync at ${gap.resyncCursor}, then restart way console.`,
				),
			publish: async event => {
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
					"Another way console currently owns the way-console journal claim. Close that console or wait for its server-side claim to expire before retrying; no owner input was accepted.",
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
			render: async event => await this.renderEvent(event),
		});
	}

	async start(): Promise<ConsoleStartup> {
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
			await this.#output.writeTrusted(`${renderConsoleStatusSummary(health, status)}\n`);
			await this.writeRefusal(decision.refusal ?? "The gateway refused interactive console startup.");
			return { accepted: false, refusal: decision.refusal };
		}
		try {
			await this.#consumer.ensureReady();
			this.#deliveryReady = true;
			await this.#output.writeTrusted(`${renderConsoleStatusSummary(health, status)}\n`);
			return { accepted: true, health, status };
		} catch (error) {
			const failure = this.markDeliveryFailure(error);
			const refusal = `Refusing interactive console: ${failure.message}`;
			await this.writeRefusal(refusal);
			return { accepted: false, refusal };
		}
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
		await this.#output.writeTrusted("Delivered as: ");
		await this.#output.writeUntrusted(result.deliveredAs);
		await this.#output.writeTrusted("\n");
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
		await this.#output.writeTrusted("Gate ");
		await this.#output.writeUntrusted(gateId);
		await this.#output.writeTrusted(": ");
		await this.#output.writeUntrusted(result.gateState);
		await this.#output.writeTrusted("\n");
		return result;
	}

	async refreshStatus(): Promise<void> {
		const [health, status] = await Promise.all([
			rpcResult<unknown>(await this.#rpc.request("way.health", {}, { timeoutMs: 5_000 }), "way.health"),
			rpcResult<unknown>(await this.#rpc.request("way.status", {}, { timeoutMs: 5_000 }), "way.status"),
		]);
		await this.#output.writeTrusted(`${renderConsoleStatusSummary(health, status)}\n`);
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
			await this.#output.writeTrusted("Commands: /status, /gate <gate_id> <expected_session_id> <JSON answer>, /quit. Any other line is submitted to the main session.\n");
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
			await this.#output.writeTrusted("Request failed: ");
			await this.#output.writeUntrusted(asError(error).message);
			await this.#output.writeTrusted("\n");
			return true;
		}
	}

	private assertDeliveryReady(): void {
		if (this.#deliveryFailure) throw this.#deliveryFailure;
		if (!this.#deliveryReady) {
			throw new ConsoleDeliveryUnavailableError("Console delivery readiness has not been established; no owner input was accepted.");
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

	private async writeRefusal(message: string): Promise<void> {
		await this.#output.writeUntrusted(message);
		await this.#output.writeTrusted("\n");
	}

	private async reportDeliveryFailure(failure: ConsoleDeliveryUnavailableError): Promise<void> {
		try {
			await this.#output.writeTrusted("Delivery unavailable; ending console: ");
			await this.#output.writeUntrusted(failure.message);
			await this.#output.writeTrusted("\n");
		} catch {
			// The original publication failure remains authoritative.
		}
	}

	private async renderEvent(event: ConsoleEventFrame): Promise<void> {
		switch (event.kind) {
			case "assistant_message": {
				const payload = recordValue(event.payload);
				await this.#output.writeTrusted("Assistant:\n");
				if (payload.finalized === true && typeof payload.text === "string") {
					await this.#output.writeUntrusted(payload.text);
				} else {
					await this.#output.writeUntrusted(`Assistant message ${sequenceString(event.seq)} had an invalid finalized payload.`);
				}
				await this.#output.writeTrusted("\n");
				return;
			}
			case "turn_start":
				await this.#output.writeTrusted("Main turn started — busy.\n");
				return;
			case "turn_end":
				await this.#output.writeTrusted("Main turn ended — idle.\n");
				return;
			case "gate_open": {
				const payload = recordValue(event.payload);
				const gateId = rawStringValue(firstValue(payload, ["gate_id", "gateId"]));
				const sessionId = rawStringValue(firstValue(payload, ["session_id", "sessionId"]));
				await this.#output.writeTrusted("Gate opened: gate_id=");
				await this.#output.writeUntrusted(gateId);
				await this.#output.writeTrusted(" expected_session_id=");
				await this.#output.writeUntrusted(sessionId);
				await this.#output.writeTrusted(". Answer with /gate ");
				await this.#output.writeUntrusted(gateId);
				await this.#output.writeTrusted(" ");
				await this.#output.writeUntrusted(sessionId);
				await this.#output.writeTrusted(" <JSON answer>.\n");
				return;
			}
			case "gate_resolved": {
				const payload = recordValue(event.payload);
				await this.#output.writeTrusted("Gate resolved: gate_id=");
				await this.#output.writeUntrusted(rawStringValue(firstValue(payload, ["gate_id", "gateId"])));
				await this.#output.writeTrusted(".\n");
				return;
			}
			case "health_change": {
				const payload = recordValue(event.payload);
				await this.#output.writeTrusted("Gateway health changed: ");
				await this.#output.writeUntrusted(rawStringValue(payload.state, rawStringValue(payload.status)));
				if (typeof payload.reason === "string") {
					await this.#output.writeTrusted(" reason=");
					await this.#output.writeUntrusted(payload.reason);
				}
				await this.#output.writeTrusted(".\n");
				return;
			}
			case "lock_event":
				await this.#output.writeTrusted("Lock state changed: ");
				await this.#output.writeUntrusted(describeGatewayValue(event.payload));
				await this.#output.writeTrusted("\n");
				return;
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
	const output = new ConsoleOutput(async text => await writeToStream(stdout, text));
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
		const startup = await consoleSurface.start();
		if (!startup.accepted) {
			throw new ConsoleStartupRefusalError(startup.refusal ?? "Interactive console startup was refused.");
		}
		terminal = dependencies.terminal ?? new RawConsoleTerminal();
		const activeTerminal = terminal;
		output.setWriter(async text => await activeTerminal.writeTrusted(text));
		await output.writeTrusted("Owner console ready. Type /help for commands.\n");
		const events = new AbortController();
		let deliveryFailure: Error | undefined;
		const eventLoop = consoleSurface.consume(events.signal).catch(error => {
			if (!events.signal.aborted) {
				deliveryFailure = asError(error);
				activeTerminal.close();
			}
		});
		try {
			for (;;) {
				const line = await activeTerminal.readLine("way> ");
				if (line === undefined) break;
				if (!(await consoleSurface.handleInput(line))) break;
			}
		} finally {
			events.abort();
			await eventLoop;
		}
		if (deliveryFailure) throw deliveryFailure;
	} finally {
		rpc?.close();
		terminal?.close();
	}
}

class RawConsoleTerminal implements ConsoleTerminal {
	readonly #input = stdin;
	readonly #output = stdout;
	#buffer = "";
	#prompt = "";
	#resolveLine: ((line: string | undefined) => void) | undefined;
	#closed = false;

	constructor() {
		if (!this.#input.isTTY || !this.#output.isTTY || !this.#input.setRawMode) {
			throw new WayConsoleError("way console requires an interactive TTY on stdin and stdout.");
		}
		this.#input.setEncoding("utf8");
		this.#input.setRawMode(true);
		this.#input.resume();
		this.#input.on("data", this.onData);
	}

	async writeTrusted(text: string): Promise<void> {
		if (this.#closed) throw new WayConsoleError("Console terminal is closed.");
		if (!this.#resolveLine) {
			await writeToStream(this.#output, text);
			return;
		}
		await writeToStream(this.#output, "\r\x1b[2K");
		await writeToStream(this.#output, text);
		await writeToStream(this.#output, `${this.#prompt}${this.#buffer}`);
	}

	async readLine(prompt: string): Promise<string | undefined> {
		if (this.#closed) return undefined;
		if (this.#resolveLine) throw new WayConsoleError("Console already has a pending input line.");
		this.#prompt = prompt;
		this.#buffer = "";
		await writeToStream(this.#output, prompt);
		return await new Promise(resolve => {
			this.#resolveLine = resolve;
		});
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#input.off("data", this.onData);
		this.#input.pause();
		if (this.#input.isRaw) this.#input.setRawMode(false);
		this.finishLine(undefined);
	}

	private onData = (chunk: string | Buffer): void => {
		for (const character of String(chunk)) {
			if (!this.#resolveLine) return;
			if (character === "\u0003" || character === "\u0004") {
				this.#output.write("\r\n");
				this.finishLine(undefined);
				return;
			}
			if (character === "\r" || character === "\n") {
				const line = this.#buffer;
				this.#output.write("\r\n");
				this.finishLine(line);
				return;
			}
			if (character === "\u007f" || character === "\b") {
				if (!this.#buffer) continue;
				this.#buffer = this.#buffer.slice(0, -1);
				this.#output.write("\b \b");
				continue;
			}
			if (character >= " ") {
				this.#buffer += character;
				this.#output.write(character);
			}
		}
	};

	private finishLine(line: string | undefined): void {
		const resolve = this.#resolveLine;
		this.#resolveLine = undefined;
		this.#buffer = "";
		this.#prompt = "";
		resolve?.(line);
	}
}

function parseConsoleArguments(arguments_: readonly string[]): string | undefined {
	let surfaceId: string | undefined;
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index];
		if (argument !== "--surface-id") throw new WayConsoleError(`Unknown way console argument: ${argument}`);
		if (surfaceId) throw new WayConsoleError("way console --surface-id may only be supplied once.");
		const value = arguments_[index + 1];
		if (!value?.trim()) throw new WayConsoleError("way console --surface-id requires a value.");
		surfaceId = value;
		index += 1;
	}
	return surfaceId;
}

function parseMainSubmitResult(value: unknown): MainSubmitResult {
	if (!isRecord(value) || value.accepted !== true || typeof value.op_ref !== "string" || typeof value.delivered_as !== "string") {
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
		return { gateId: match[1] as string, expectedSessionId: match[2] as string, answer: JSON.parse(match[3] as string) };
	} catch {
		throw new WayConsoleError("Gate answers must be valid JSON.");
	}
}

function toConsoleEvent(event: RpcJournalEvent): ConsoleEventFrame {
	if (!WAY_CONSOLE_EVENT_KINDS.includes(event.kind as WayConsoleEventKind)) {
		throw new ConsoleDeliveryUnavailableError(`main.events.read returned an unsupported console event kind: ${event.kind}`);
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
	return new Promise(resolve => {
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

function writeToStream(stream: NodeJS.WriteStream, text: string): Promise<void> {
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
			const accepted = stream.write(text, error => {
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
