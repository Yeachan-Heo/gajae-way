import { randomUUID } from "node:crypto";
import * as path from "node:path";
import { stdin, stdout } from "node:process";
import type { WayConfig } from "../config";
import { loadWayProfile, type WayProfile } from "../profile";
import { RpcClient, RpcResponseError, rpcResult, type JsonRpcClient } from "../rpc-client";

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
export type ConsoleConsumerRunResult = "rendered" | "idle" | "claim_held";

type RecordValue = Record<string, unknown>;

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

export interface ConsoleTerminal {
	write(text: string): void;
	readLine(prompt: string): Promise<string | undefined>;
	close(): void;
}

export interface ConsoleEventConsumerOptions {
	readonly rpc: JsonRpcClient;
	readonly render: (event: ConsoleEventFrame) => void;
	readonly consumerId?: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idleDelayMs?: number;
	readonly retryDelayMs?: number;
	readonly now?: () => number;
	readonly onError?: (error: Error) => void;
}

export interface OwnerConsoleOptions {
	readonly rpc: JsonRpcClient;
	readonly ownerSurfaceId: string;
	readonly write: (text: string) => void;
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

interface ConsumerClaim {
	readonly claimId: string;
	readonly cursor: string;
	readonly expiresAt: number;
}

interface EventRead {
	readonly events: readonly ConsoleEventFrame[];
	readonly nextCursor: string;
	readonly gap?: {
		readonly missingFrom: string;
		readonly missingTo: string;
		readonly resyncCursor: string;
	};
}

interface DeliveryProof {
	readonly seq: string;
	readonly dedupe_key: string;
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

function isRecord(value: unknown): value is RecordValue {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown, fallback = "unknown"): string {
	return typeof value === "string" && value ? value : fallback;
}

function booleanValue(value: unknown): string {
	return typeof value === "boolean" ? String(value) : "unknown";
}

function integerValue(value: unknown): string {
	return typeof value === "number" && Number.isFinite(value) ? String(value) : "unknown";
}

function recordValue(value: unknown): RecordValue {
	return isRecord(value) ? value : {};
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

/** Decides whether a local owner can safely send input to this daemon. */
export function consoleStartupDecision(healthPayload: unknown, statusPayload: unknown): ConsoleStartupDecision {
	if (!isRecord(healthPayload) || !isRecord(statusPayload)) {
		return {
			interactive: false,
			refusal: "Refusing interactive console: the gateway returned an invalid health or status payload.",
		};
	}
	const records = [healthPayload, statusPayload] as const;
	const states = records.map(record => stringValue(record.state));
	const statuses = records.map(record => stringValue(record.status));
	const reason = firstString(records, "reason");
	if (states.includes("failed_closed")) {
		return {
			interactive: false,
			refusal: `Refusing interactive console: the gateway is failed closed${reason ? ` (${reason})` : ""}. The main persona is fenced; repair or explicitly approve it before sending owner input.`,
		};
	}
	if (statuses.some(status => status !== "healthy")) {
		return {
			interactive: false,
			refusal: `Refusing interactive console: the gateway is not healthy (health=${statuses.join(", ")}, state=${states.join(", ")})${reason ? `: ${reason}` : ""}. No owner input was sent.`,
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
		`  daemon: status=${stringValue(health.status)} state=${stringValue(health.state)}${reason ? ` reason=${reason}` : ""}`,
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

/**
 * Server-checkpointed event consumer. It intentionally stores no cursor locally:
 * a render is settled only by consumer.commit after the terminal has displayed it.
 */
export class ConsoleEventConsumer {
	readonly #rpc: JsonRpcClient;
	readonly #render: (event: ConsoleEventFrame) => void;
	readonly #consumerId: string;
	readonly #claimTtlMs: number;
	readonly #readWaitMs: number;
	readonly #idleDelayMs: number;
	readonly #retryDelayMs: number;
	readonly #now: () => number;
	readonly #onError: (error: Error) => void;

	constructor(options: ConsoleEventConsumerOptions) {
		this.#rpc = options.rpc;
		this.#render = options.render;
		this.#consumerId = options.consumerId ?? WAY_CONSOLE_CONSUMER_ID;
		if (!this.#consumerId.trim()) throw new WayConsoleError("Console consumer_id must not be empty.");
		this.#claimTtlMs = boundedInteger(
			options.claimTtlMs ?? DEFAULT_CONSOLE_CLAIM_TTL_MS,
			"claimTtlMs",
			5_000,
			600_000,
		);
		this.#readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_CONSOLE_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (this.#readWaitMs >= this.#claimTtlMs) throw new WayConsoleError("readWaitMs must be shorter than claimTtlMs.");
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#retryDelayMs = boundedInteger(options.retryDelayMs ?? 250, "retryDelayMs", 0, 60_000);
		this.#now = options.now ?? Date.now;
		this.#onError = options.onError ?? (error => console.error(`way console event delivery failed: ${error.message}`));
	}

	async run(signal: AbortSignal): Promise<void> {
		while (!signal.aborted) {
			try {
				const result = await this.runOnce(signal);
				if (result !== "rendered") await sleep(this.#idleDelayMs, signal);
			} catch (error) {
				if (signal.aborted) return;
				this.#onError(asError(error));
				await sleep(this.#retryDelayMs, signal);
			}
		}
	}

	async runOnce(signal?: AbortSignal): Promise<ConsoleConsumerRunResult> {
		throwIfAborted(signal);
		let claim: ConsumerClaim;
		try {
			claim = parseClaim(
				rpcResult<unknown>(
					await this.#rpc.request(
						"consumer.claim",
						{ consumer_id: this.#consumerId, claim_ttl_ms: this.#claimTtlMs },
						{ signal },
					),
					"consumer.claim",
				),
			);
		} catch (error) {
			if (error instanceof RpcResponseError && error.code === 1601) return "claim_held";
			throw error;
		}
		try {
			let readCursor = claim.cursor;
			let readFromCheckpoint = true;
			for (;;) {
				throwIfAborted(signal);
				const read = parseEventRead(
					rpcResult<unknown>(
						await this.#rpc.request(
							"main.events.read",
							{
								...(readFromCheckpoint ? { consumer_id: this.#consumerId } : { cursor: readCursor }),
								limit: 100,
								wait_ms: this.#readWaitMs,
								kinds: WAY_CONSOLE_EVENT_KINDS,
							},
							{ signal, timeoutMs: this.#readWaitMs + 2_000 },
						),
						"main.events.read",
					),
				);
				readFromCheckpoint = false;
				if (read.gap) {
					throw new WayConsoleError(
						`Console checkpoint ${claim.cursor} is behind journal retention; resync at ${read.gap.resyncCursor} before accepting interactive delivery.`,
					);
				}
				if (read.events.length > 0) {
					for (const event of read.events) {
						throwIfAborted(signal);
						this.#render(event);
					}
					await this.commit(
						claim,
						read.nextCursor,
						read.events.map(event => ({ seq: sequenceString(event.seq), dedupe_key: `${this.#consumerId}:${sequenceString(event.seq)}` })),
						signal,
					);
					return "rendered";
				}
				if (read.nextCursor === readCursor || this.nearClaimExpiry(claim)) {
					// An unchanged checkpoint release does not acknowledge an unrendered event.
					await this.commit(claim, claim.cursor, [], signal);
					return "idle";
				}
				readCursor = read.nextCursor;
			}
		} catch (error) {
			await this.releaseUnadvancedClaim(claim);
			throw error;
		}
	}

	private nearClaimExpiry(claim: ConsumerClaim): boolean {
		return this.#now() + this.#readWaitMs + 250 >= claim.expiresAt;
	}

	private async commit(claim: ConsumerClaim, cursor: string, proofs: readonly DeliveryProof[], signal?: AbortSignal): Promise<void> {
		rpcResult<unknown>(
			await this.#rpc.request(
				"consumer.commit",
				{
					consumer_id: this.#consumerId,
					claim_id: claim.claimId,
					cursor,
					proofs,
				},
				{ signal },
			),
			"consumer.commit",
		);
	}

	private async releaseUnadvancedClaim(claim: ConsumerClaim): Promise<void> {
		try {
			await this.commit(claim, claim.cursor, []);
		} catch {
			// A process crash has the same recovery behavior: the server-side claim expires.
		}
	}
}

/** Owner-facing operations and rendering; all stateful behavior remains on the gateway. */
export class OwnerConsole {
	readonly #rpc: JsonRpcClient;
	readonly #ownerSurfaceId: string;
	readonly #write: (text: string) => void;
	readonly #idempotencyKey: () => string;
	readonly #consumer: ConsoleEventConsumer;

	constructor(options: OwnerConsoleOptions) {
		if (!options.ownerSurfaceId.trim()) throw new WayConsoleError("Console owner surface id must not be empty.");
		this.#rpc = options.rpc;
		this.#ownerSurfaceId = options.ownerSurfaceId;
		this.#write = options.write;
		this.#idempotencyKey = options.idempotencyKey ?? randomUUID;
		this.#consumer = new ConsoleEventConsumer({
			rpc: options.rpc,
			consumerId: options.consumerId,
			claimTtlMs: options.claimTtlMs,
			readWaitMs: options.readWaitMs,
			render: event => this.#write(renderConsoleEvent(event)),
			onError: error => this.#write(`Console event delivery paused: ${error.message}\n`),
		});
	}

	async start(): Promise<ConsoleStartup> {
		try {
			const [healthPayload, statusPayload] = await Promise.all([
				rpcResult<unknown>(await this.#rpc.request("way.health", {}, { timeoutMs: 5_000 }), "way.health"),
				rpcResult<unknown>(await this.#rpc.request("way.status", {}, { timeoutMs: 5_000 }), "way.status"),
			]);
			const health = recordValue(healthPayload);
			const status = recordValue(statusPayload);
			const decision = consoleStartupDecision(health, status);
			this.#write(`${renderConsoleStatusSummary(health, status)}\n`);
			if (!decision.interactive) {
				this.#write(`${decision.refusal}\n`);
				return { accepted: false, refusal: decision.refusal };
			}
			return { accepted: true, health, status };
		} catch (error) {
			const refusal = `Refusing interactive console: unable to inspect the gateway over its owner socket: ${asError(error).message}`;
			this.#write(`${refusal}\n`);
			return { accepted: false, refusal };
		}
	}

	async submit(text: string): Promise<MainSubmitResult> {
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
		this.#write(`Delivered as: ${result.deliveredAs}\n`);
		return result;
	}

	async answerGate(gateId: string, expectedSessionId: string, answer: unknown): Promise<GateAnswerResult> {
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
		this.#write(`Gate ${gateId}: ${result.gateState}\n`);
		return result;
	}

	async refreshStatus(): Promise<void> {
		const [health, status] = await Promise.all([
			rpcResult<unknown>(await this.#rpc.request("way.health", {}, { timeoutMs: 5_000 }), "way.health"),
			rpcResult<unknown>(await this.#rpc.request("way.status", {}, { timeoutMs: 5_000 }), "way.status"),
		]);
		this.#write(`${renderConsoleStatusSummary(health, status)}\n`);
	}

	async consumeOnce(signal?: AbortSignal): Promise<ConsoleConsumerRunResult> {
		return await this.#consumer.runOnce(signal);
	}

	async consume(signal: AbortSignal): Promise<void> {
		await this.#consumer.run(signal);
	}

	async handleInput(line: string): Promise<boolean> {
		const command = line.trim();
		if (!command) return true;
		if (command === "/quit" || command === "/exit") return false;
		if (command === "/help") {
			this.#write("Commands: /status, /gate <gate_id> <expected_session_id> <JSON answer>, /quit. Any other line is submitted to the main session.\n");
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
			this.#write(`Request failed: ${asError(error).message}\n`);
			return true;
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
	const terminal = dependencies.terminal ?? new RawConsoleTerminal();
	let rpc: JsonRpcClient | undefined;
	try {
		rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(path.join(config.stateDir, "rpc.sock"));
		const consoleSurface = new OwnerConsole({
			rpc,
			ownerSurfaceId,
			write: text => terminal.write(text),
			idempotencyKey: dependencies.idempotencyKey,
		});
		const startup = await consoleSurface.start();
		if (!startup.accepted) return;
		terminal.write("Owner console ready. Type /help for commands.\n");
		const events = new AbortController();
		const eventLoop = consoleSurface.consume(events.signal);
		try {
			for (;;) {
				const line = await terminal.readLine("way> ");
				if (line === undefined) break;
				if (!(await consoleSurface.handleInput(line))) break;
			}
		} finally {
			events.abort();
			await eventLoop;
		}
	} finally {
		rpc?.close();
		terminal.close();
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

	write(text: string): void {
		if (this.#closed) return;
		const output = text.endsWith("\n") ? text : `${text}\n`;
		if (!this.#resolveLine) {
			this.#output.write(output);
			return;
		}
		this.#output.write("\r\x1b[2K");
		this.#output.write(output);
		this.#output.write(`${this.#prompt}${this.#buffer}`);
	}

	readLine(prompt: string): Promise<string | undefined> {
		if (this.#closed) return Promise.resolve(undefined);
		if (this.#resolveLine) throw new WayConsoleError("Console already has a pending input line.");
		this.#prompt = prompt;
		this.#buffer = "";
		this.#output.write(prompt);
		return new Promise(resolve => {
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

function renderConsoleEvent(event: ConsoleEventFrame): string {
	switch (event.kind) {
		case "assistant_message": {
			const payload = recordValue(event.payload);
			if (payload.finalized === true && typeof payload.text === "string") return `Assistant:\n${payload.text}\n`;
			return `Assistant message ${sequenceString(event.seq)} had an invalid finalized payload.\n`;
		}
		case "turn_start":
			return "Main turn started — busy.\n";
		case "turn_end":
			return "Main turn ended — idle.\n";
		case "gate_open": {
			const payload = recordValue(event.payload);
			const gateId = stringValue(firstValue(payload, ["gate_id", "gateId"]));
			const sessionId = stringValue(firstValue(payload, ["session_id", "sessionId"]));
			return `Gate opened: gate_id=${gateId} expected_session_id=${sessionId}. Answer with /gate ${gateId} ${sessionId} <JSON answer>.\n`;
		}
		case "gate_resolved": {
			const payload = recordValue(event.payload);
			return `Gate resolved: gate_id=${stringValue(firstValue(payload, ["gate_id", "gateId"]))}.\n`;
		}
		case "health_change": {
			const payload = recordValue(event.payload);
			const state = stringValue(payload.state, stringValue(payload.status));
			const reason = typeof payload.reason === "string" ? ` reason=${payload.reason}` : "";
			return `Gateway health changed: ${state}${reason}.\n`;
		}
		case "lock_event":
			return `Lock state changed: ${JSON.stringify(event.payload)}\n`;
	}
}

function parseClaim(value: unknown): ConsumerClaim {
	if (!isRecord(value) || typeof value.claim_id !== "string" || typeof value.cursor !== "string" || typeof value.expires_at !== "number") {
		throw new WayConsoleError("consumer.claim returned an invalid response.");
	}
	return { claimId: value.claim_id, cursor: value.cursor, expiresAt: value.expires_at };
}

function parseEventRead(value: unknown): EventRead {
	if (!isRecord(value) || !Array.isArray(value.events) || typeof value.next_cursor !== "string") {
		throw new WayConsoleError("main.events.read returned an invalid response.");
	}
	const events: ConsoleEventFrame[] = [];
	for (const event of value.events) {
		if (!isRecord(event) || (typeof event.seq !== "number" && typeof event.seq !== "string") || typeof event.kind !== "string") {
			throw new WayConsoleError("main.events.read returned an invalid event.");
		}
		if (!WAY_CONSOLE_EVENT_KINDS.includes(event.kind as WayConsoleEventKind)) {
			throw new WayConsoleError(`main.events.read returned unsupported console event kind: ${event.kind}`);
		}
		events.push({
			seq: event.seq,
			...(typeof event.ts === "number" ? { ts: event.ts } : {}),
			kind: event.kind as WayConsoleEventKind,
			payload: event.payload,
		});
	}
	if (value.gap === undefined) return { events, nextCursor: value.next_cursor };
	if (!isRecord(value.gap) || typeof value.gap.missing_from !== "string" || typeof value.gap.missing_to !== "string" || typeof value.gap.resync_cursor !== "string") {
		throw new WayConsoleError("main.events.read returned an invalid retention gap.");
	}
	return {
		events,
		nextCursor: value.next_cursor,
		gap: {
			missingFrom: value.gap.missing_from,
			missingTo: value.gap.missing_to,
			resyncCursor: value.gap.resync_cursor,
		},
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

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (!signal?.aborted) return;
	const error = new Error("Console event consumer stopped.");
	error.name = "AbortError";
	throw error;
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

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}
