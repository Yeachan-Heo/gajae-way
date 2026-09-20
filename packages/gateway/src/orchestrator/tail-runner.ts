import { createHash } from "node:crypto";
import type { SessionRelayStream } from "./broker";
import { sanitizeDiagnostic } from "./rebind";

/**
 * Relay-owned session transport.
 *
 * One resident `gjc sdk serve --stdio --session <id>` relay per bound session
 * is the ONLY channel for a persona turn. Commands (`turn.prompt`,
 * `turn.steer`, `turn.result`) go down its stdin as `control_request` /
 * `query_request` frames; the host answers on stdout by request id. Because
 * the prompt was submitted on this connection, the host streams the turn's
 * own content (`event/message_end`, `event/tool_execution_*`, `agent_start`,
 * `agent_end`) directly to it, stamped with the turn's commandId/turnId.
 *
 * Nothing here polls, pages, or cursors. Content arrives once, in order, and
 * is attributed by correlation; the checks that used to reconstruct that from
 * three overlapping snapshot sources (turn floors, ring high-water marks,
 * text-hash dedupe, timestamp fences) do not exist any more.
 */

export const HELLO_CAPABILITIES = ["tool_activity_v2"] as const;

/** Frame kinds the actor keys on (host `AgentSessionEvent` names plus lifecycle). */
export type TailEventKind =
	| "agent_start"
	| "agent_end"
	| "agent_failed"
	| "message_end"
	| "tool_execution_start"
	| "tool_execution_update"
	| "tool_execution_end"
	| "activity"
	| "unknown";

export interface TailFrame {
	readonly kind: TailEventKind;
	readonly rawKind: string;
	/** Host correlation, present on every frame the host attributes to a turn. */
	readonly commandId?: string;
	readonly turnId?: string;
	/** Stable host identity of the underlying message/tool call, when it carries one. */
	readonly eventId?: string;
	readonly payload: Record<string, unknown>;
	/** Assistant speech of a `message_end` frame. */
	readonly assistantText?: string;
	/** True for user/tool/custom rows: attribution evidence, never chat output. */
	readonly steerEcho: boolean;
	readonly idle: boolean;
}

export interface TurnCorrelation {
	readonly commandId?: string;
	readonly turnId?: string;
}

export interface TailAttachInput {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly repo: string;
	/** Caller-owned origin identity for grep-stable observability. */
	readonly originKey?: string;
	/** Current tails may wait for capacity; parked retired holds must not consume it. */
	priority?: "current" | "retired";
	onFrame?: (frame: TailFrame) => void | Promise<void>;
	onStall?: (input: { sessionId: string; brokerGeneration: number; elapsedMs: number }) => void | Promise<void>;
	/** The relay died mid-turn; frames emitted while it was down are lost (best-effort content). It reopens. */
	onRelayLost?: (input: { sessionId: string; brokerGeneration: number }) => void | Promise<void>;
	/** The relay could not be kept open (repeated immediate deaths); the handle is closed and will not reopen. */
	onRelayDead?: (input: { sessionId: string; brokerGeneration: number }) => void | Promise<void>;
	onDiagnostic?: (line: string) => void;
}

export interface RelayRequestOptions {
	readonly timeoutMs?: number;
}

export type RelayResponse = {
	readonly ok: boolean;
	readonly result?: Record<string, unknown>;
	readonly error?: { readonly code?: string; readonly message?: string };
};

export interface TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	/** Resolves once the host hello was exchanged on the relay. */
	readonly ready: Promise<void>;
	/** Sends `control_request` and resolves with its `control_response`. */
	control(operation: string, input: Record<string, unknown>, options?: RelayRequestOptions): Promise<RelayResponse>;
	/** Sends `query_request` and resolves with its `query_response`. */
	query(name: string, input: Record<string, unknown>, options?: RelayRequestOptions): Promise<RelayResponse>;
	/**
	 * Names the turn whose content this handle delivers. Frames with a different
	 * correlation are dropped; uncorrelated frames were never turn content.
	 */
	beginTurn(opRef: string, correlation?: TurnCorrelation): void;
	/** Adds host correlation learned after beginTurn (from the accept receipt or agent_start). */
	correlate(opRef: string, correlation: TurnCorrelation): void;
	setTurnRunning(running: boolean): void;
	close(): Promise<void>;
}

export type TailStream = SessionRelayStream;
export type TailStreamSpawner = (sessionId: string) => TailStream;

export interface TailRunnerOptions {
	readonly stream: TailStreamSpawner;
	/** Default session workspace. */
	readonly repo: string;
	readonly maxTailProcesses?: number;
	readonly idleTtlMs?: number;
	readonly stallTimeoutMs?: number;
	readonly requestTimeoutMs?: number;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
	readonly log?: (line: string) => void;
}

const DEFAULT_MAX_TAIL_PROCESSES = 64;
const DEFAULT_IDLE_TTL_MS = 60_000;
const DEFAULT_STALL_TIMEOUT_MS = 120_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const HELLO_TIMEOUT_MS = 15_000;
const STREAM_REOPEN_GIVE_UP = 6;
const REOPEN_BASE_BACKOFF_MS = 250;
const UNKNOWN_KIND_DIAGNOSTIC_CAP = 20;

export class TailCapacityError extends Error {
	constructor() {
		super("tail capacity is reserved for current-generation work");
		this.name = "TailCapacityError";
	}
}

export class RelayClosedError extends Error {
	readonly code = "relay_closed";
	constructor(sessionId: string, detail: string) {
		super(`relay for ${sessionId} closed before the host answered: ${detail}`);
		this.name = "RelayClosedError";
	}
}

/**
 * `gjc sdk serve` refused to attach: it printed one SDK error envelope and
 * exited. `endpoint_stale` / `not_found` mean the broker no longer serves this
 * session id - the same condition the CLI reports as `session_unavailable`,
 * so callers classify it identically (release the bound turn, rebind).
 */
export class RelayRefusedError extends Error {
	readonly code: string;
	constructor(sessionId: string, code: string, message: string | undefined) {
		super(`relay for ${sessionId} refused: ${code}${message ? ` - ${message}` : ""}`);
		this.name = "RelayRefusedError";
		this.code = code === "endpoint_stale" || code === "not_found" ? "session_unavailable" : code;
	}
}

export class RelayRequestTimeoutError extends Error {
	readonly code = "relay_timeout";
	constructor(sessionId: string, id: string, timeoutMs: number) {
		super(`relay request ${id} on ${sessionId} received no response within ${timeoutMs}ms`);
		this.name = "RelayRequestTimeoutError";
	}
}

/** A relay transport failure: the request may or may not have reached the host; it says nothing about the operation. */
export function isRelayTransportFailure(error: unknown): boolean {
	return error instanceof RelayClosedError || error instanceof RelayRequestTimeoutError;
}

/** Supervises the resident relays: capacity, idle reaping, and stall alarms. */
export class TailRunner {
	readonly #stream: TailStreamSpawner;
	readonly #repo: string;
	readonly #maxTailProcesses: number;
	readonly #idleTtlMs: number;
	#stallTimeoutMs: number;
	readonly #requestTimeoutMs: number;
	readonly #now: () => number;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly #log: (line: string) => void;
	readonly #handles = new Set<ManagedTailHandle>();
	readonly #waiters: Array<() => void> = [];
	#lastSaturationAlertAt: number | undefined;

	constructor(options: TailRunnerOptions) {
		this.#stream = options.stream;
		this.#repo = options.repo;
		this.#maxTailProcesses = positiveInteger(options.maxTailProcesses, DEFAULT_MAX_TAIL_PROCESSES, "maxTailProcesses");
		this.#idleTtlMs = positiveInteger(options.idleTtlMs, DEFAULT_IDLE_TTL_MS, "idleTtlMs");
		this.#stallTimeoutMs = positiveInteger(options.stallTimeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
		this.#requestTimeoutMs = positiveInteger(options.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS, "requestTimeoutMs");
		this.#now = options.now ?? (() => Date.now());
		this.#sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
		this.#log = options.log ?? ((line: string) => console.error(line));
	}

	get activeCount(): number {
		return this.#handles.size;
	}

	now(): number {
		return this.#now();
	}

	get stallTimeoutMs(): number {
		return this.#stallTimeoutMs;
	}

	setStallTimeoutMs(timeoutMs: number): void {
		this.#stallTimeoutMs = positiveInteger(timeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
	}

	get requestTimeoutMs(): number {
		return this.#requestTimeoutMs;
	}

	get streamSpawner(): TailStreamSpawner {
		return this.#stream;
	}

	sleep(ms: number): Promise<void> {
		return this.#sleep(ms);
	}

	/**
	 * Opens the session's relay. When all slots are running, this waits without
	 * changing durable inbound state; callers retain their bound turn.
	 */
	async attach(input: TailAttachInput): Promise<TailHandle> {
		await this.#acquireSlot(input.priority ?? "current");
		const handle = new ManagedTailHandle(this, { ...input, repo: input.repo || this.#repo });
		this.#handles.add(handle);
		handle.start();
		try {
			await handle.ready;
			return handle;
		} catch (error) {
			await handle.close();
			throw error;
		}
	}

	/** Test/loop seam: checks exact threshold, so 119.9 seconds never emits a 120s alarm. */
	checkStalls(now = this.#now()): void {
		for (const handle of this.#handles) handle.checkStall(now);
	}

	/** Reaps only idle handles; an active turn is never evicted for capacity. */
	async reapIdle(now = this.#now()): Promise<number> {
		const idle = [...this.#handles].filter(
			(handle) => handle.idleSince !== undefined && now - handle.idleSince >= this.#idleTtlMs,
		);
		for (const handle of idle) await handle.close();
		return idle.length;
	}

	/** An authenticated control reply is the only affirmative compaction observation. */
	recordCompactionReceipt(input: {
		readonly sessionId: string;
		readonly originKey: string;
		readonly result: unknown;
	}): void {
		const receipt = recordOf(input.result);
		const outcome = receipt?.started === true ? "started" : receipt?.skipped === true ? "skipped" : "received";
		this.#log(
			`compaction_event sessionId=${input.sessionId} originKey=${input.originKey} source=control_receipt result=${outcome}`,
		);
	}

	async #acquireSlot(priority: "current" | "retired"): Promise<void> {
		await this.reapIdle();
		while (this.#handles.size >= this.#maxTailProcesses) {
			const evictable = [...this.#handles]
				.filter((handle) => !handle.running)
				.sort(
					(left, right) => (left.idleSince ?? Number.POSITIVE_INFINITY) - (right.idleSince ?? Number.POSITIVE_INFINITY),
				)[0];
			if (evictable) {
				await evictable.close();
				continue;
			}
			this.#logSaturation();
			if (priority === "retired") throw new TailCapacityError();
			await new Promise<void>((resolve) => this.#waiters.push(resolve));
		}
	}

	release(handle: ManagedTailHandle): void {
		this.#handles.delete(handle);
		this.#waiters.shift()?.();
	}

	#logSaturation(): void {
		const now = this.#now();
		if (this.#lastSaturationAlertAt !== undefined && now - this.#lastSaturationAlertAt < 60_000) return;
		this.#lastSaturationAlertAt = now;
		this.#log(`tail_saturation active=${this.#handles.size} limit=${this.#maxTailProcesses}`);
	}
}

type PendingRequest = {
	readonly kind: "control_response" | "query_response";
	readonly resolve: (response: RelayResponse) => void;
	readonly reject: (error: Error) => void;
	readonly timer: ReturnType<typeof setTimeout>;
};

class ManagedTailHandle implements TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly repo: string;
	readonly #runner: TailRunner;
	readonly #input: TailAttachInput;
	readonly ready: Promise<void>;
	#readyResolve: () => void = () => {};
	#readyReject: (error: unknown) => void = () => {};
	#ready = false;
	#closed = false;
	#running = false;
	#lastEventAt: number;
	#idleSince: number | undefined;
	#stallReported = false;
	#unknownDiagnostics = 0;
	#stream: TailStream | undefined;
	#connectionId: string | undefined;
	#helloResolve: (() => void) | undefined;
	#refusal: RelayRefusedError | undefined;
	#reopenFailures = 0;
	#requestSeq = 0;
	readonly #pending = new Map<string, PendingRequest>();
	/** Ordered delivery: a later frame's side effects wait on the earlier one's. */
	#deliveries: Promise<void> = Promise.resolve();
	#opRef: string | undefined;
	#correlation: TurnCorrelation = {};
	#droppedForeign = 0;

	constructor(runner: TailRunner, input: TailAttachInput) {
		this.#runner = runner;
		this.#input = input;
		this.sessionId = input.sessionId;
		this.brokerGeneration = input.brokerGeneration;
		this.repo = input.repo;
		this.#lastEventAt = runner.now();
		this.ready = new Promise<void>((resolve, reject) => {
			this.#readyResolve = resolve;
			this.#readyReject = reject;
		});
		this.ready.catch(() => {});
	}

	get turnRunning(): boolean {
		return this.#running;
	}

	get running(): boolean {
		return this.#running;
	}

	get idleSince(): number | undefined {
		return this.#idleSince;
	}

	start(): void {
		void this.#run();
	}

	beginTurn(opRef: string, correlation: TurnCorrelation = {}): void {
		this.#opRef = opRef;
		this.#correlation = { ...correlation };
		this.#droppedForeign = 0;
	}

	correlate(opRef: string, correlation: TurnCorrelation): void {
		if (this.#opRef !== opRef) return;
		this.#correlation = {
			...(this.#correlation.commandId ? { commandId: this.#correlation.commandId } : {}),
			...(this.#correlation.turnId ? { turnId: this.#correlation.turnId } : {}),
			...(correlation.commandId ? { commandId: correlation.commandId } : {}),
			...(correlation.turnId ? { turnId: correlation.turnId } : {}),
		};
	}

	setTurnRunning(running: boolean): void {
		if (this.#closed) return;
		this.#running = running;
		this.#stallReported = false;
		// A stall is silence DURING a turn: the clock starts at the turn start.
		this.#lastEventAt = this.#runner.now();
		if (running) this.#idleSince = undefined;
		else this.#idleSince ??= this.#runner.now();
	}

	checkStall(now: number): void {
		if (this.#closed || !this.#running || this.#stallReported) return;
		const elapsedMs = now - this.#lastEventAt;
		if (elapsedMs < this.#runner.stallTimeoutMs) return;
		this.#stallReported = true;
		void this.#input.onStall?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration, elapsedMs });
	}

	control(operation: string, input: Record<string, unknown>, options?: RelayRequestOptions): Promise<RelayResponse> {
		return this.#request("control_request", "control_response", { operation, input, confirm: false }, options);
	}

	query(name: string, input: Record<string, unknown>, options?: RelayRequestOptions): Promise<RelayResponse> {
		return this.#request("query_request", "query_response", { query: name, input }, options);
	}

	async #request(
		type: "control_request" | "query_request",
		responseType: "control_response" | "query_response",
		body: Record<string, unknown>,
		options?: RelayRequestOptions,
	): Promise<RelayResponse> {
		if (this.#closed) throw new RelayClosedError(this.sessionId, "handle closed");
		await this.ready;
		const stream = this.#stream;
		const connectionId = this.#connectionId;
		if (!stream || !connectionId) throw new RelayClosedError(this.sessionId, "relay not open");
		const id = `gw-${this.brokerGeneration}-${++this.#requestSeq}`;
		const timeoutMs = options?.timeoutMs ?? this.#runner.requestTimeoutMs;
		return await new Promise<RelayResponse>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(new RelayRequestTimeoutError(this.sessionId, id, timeoutMs));
			}, timeoutMs);
			this.#pending.set(id, { kind: responseType, resolve, reject, timer });
			try {
				stream.write(JSON.stringify({ type, id, connectionId, ...body }));
			} catch (error) {
				clearTimeout(timer);
				this.#pending.delete(id);
				reject(new RelayClosedError(this.sessionId, sanitizeDiagnostic(messageOf(error)) || "write_failed"));
			}
		});
	}

	#failPending(detail: string): void {
		for (const [id, pending] of this.#pending) {
			clearTimeout(pending.timer);
			this.#pending.delete(id);
			pending.reject(new RelayClosedError(this.sessionId, detail));
		}
	}

	async #run(): Promise<void> {
		while (!this.#closed) {
			let stream: TailStream;
			try {
				stream = this.#runner.streamSpawner(this.sessionId);
			} catch (error) {
				if (!this.#ready) {
					this.#ready = true;
					this.#readyReject(error);
					await this.close();
					return;
				}
				this.#input.onDiagnostic?.(
					`tail_stream_spawn_failed session=${this.sessionId} detail=${sanitizeDiagnostic(messageOf(error)) || "sdk_error"}`,
				);
				this.#reopenFailures += 1;
				if (!(await this.#backoff())) return;
				continue;
			}
			this.#stream = stream;
			this.#refusal = undefined;
			const openedAt = this.#runner.now();
			const hello = new Promise<void>((resolve) => {
				this.#helloResolve = resolve;
			});
			const helloTimer = setTimeout(() => this.#helloResolve?.(), HELLO_TIMEOUT_MS);
			try {
				const consume = (async () => {
					for await (const line of stream.lines) {
						if (this.#closed) break;
						this.#receiveLine(line);
					}
					// The relay ended: whether or not hello arrived, nobody is waiting on it any more.
					this.#helloResolve?.();
				})();
				stream.write(JSON.stringify({ type: "hello", protocolVersion: 3, capabilities: [...HELLO_CAPABILITIES] }));
				await hello;
				clearTimeout(helloTimer);
				if (this.#refusal) throw this.#refusal;
				if (!this.#connectionId) throw new Error("host hello did not arrive");
				if (!this.#ready) {
					this.#ready = true;
					this.#readyResolve();
				}
				// A relay that says hello and then dies within seconds still counts
				// toward give-up: the streak resets only after a relay lived 5 s.
				await consume;
			} catch (error) {
				clearTimeout(helloTimer);
				if (!this.#ready) {
					this.#ready = true;
					this.#readyReject(error);
					stream.close();
					this.#stream = undefined;
					await this.close();
					return;
				}
				this.#input.onDiagnostic?.(
					`tail_stream_error session=${this.sessionId} detail=${sanitizeDiagnostic(messageOf(error)) || "sdk_error"}`,
				);
			} finally {
				this.#helloResolve = undefined;
				this.#stream = undefined;
				this.#connectionId = undefined;
				stream.close();
				this.#failPending("relay ended");
			}
			if (this.#closed) return;
			// A relay that ends is a lost observer for the running turn: whatever the
			// host streamed while it was down is gone (content is best-effort by
			// host contract). The actor reconciles through status and the
			// transcript; this handle only reopens so later commands have a channel.
			if (this.#running) {
				this.#input.onDiagnostic?.(`tail_relay_lost session=${this.sessionId} opRef=${this.#opRef ?? "-"}`);
				await this.#input.onRelayLost?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration });
			}
			const sinceOpen = this.#runner.now() - openedAt;
			this.#reopenFailures = sinceOpen < 5_000 ? this.#reopenFailures + 1 : 0;
			if (!(await this.#backoff())) return;
		}
	}

	/** Exponential backoff between reopen attempts; false once the relay is declared dead. */
	async #backoff(): Promise<boolean> {
		if (this.#reopenFailures >= STREAM_REOPEN_GIVE_UP) {
			this.#input.onDiagnostic?.(`tail_stream_dead session=${this.sessionId} reopens=${this.#reopenFailures}`);
			await this.close();
			await this.#input.onRelayDead?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration });
			return false;
		}
		const backoff = Math.min(30_000, REOPEN_BASE_BACKOFF_MS * 2 ** this.#reopenFailures);
		this.#input.onDiagnostic?.(`tail_stream_reopen session=${this.sessionId} backoffMs=${backoff}`);
		await this.#runner.sleep(backoff);
		return !this.#closed;
	}

	#receiveLine(line: string): void {
		const trimmed = line.trim();
		if (!trimmed) return;
		let parsed: unknown;
		try {
			parsed = JSON.parse(trimmed);
		} catch {
			return;
		}
		const frame = recordOf(parsed);
		if (!frame) return;
		if (frame.ok === false && typeof frame.type !== "string") {
			// The serve CLI's own refusal envelope: it will exit right after this.
			const error = recordOf(frame.error);
			const code = typeof error?.code === "string" ? error.code : "relay_refused";
			this.#refusal = new RelayRefusedError(
				this.sessionId,
				sanitizeDiagnostic(code) || "relay_refused",
				typeof error?.message === "string" ? sanitizeDiagnostic(error.message).slice(0, 200) : undefined,
			);
			this.#helloResolve?.();
			return;
		}
		if (typeof frame.type !== "string") return;
		if (frame.type === "hello") {
			if (typeof frame.connectionId === "string" && frame.connectionId.length > 0) {
				this.#connectionId = frame.connectionId;
				this.#helloResolve?.();
			}
			return;
		}
		if (frame.type === "control_response" || frame.type === "query_response") {
			const id = typeof frame.id === "string" ? frame.id : undefined;
			const pending = id ? this.#pending.get(id) : undefined;
			if (!pending) return;
			clearTimeout(pending.timer);
			this.#pending.delete(id!);
			pending.resolve(decodeResponse(frame));
			return;
		}
		if (frame.type === "transport_error") {
			this.#input.onDiagnostic?.(
				`tail_transport_error session=${this.sessionId} code=${sanitizeDiagnostic(String(frame.code ?? "unknown"))}`,
			);
			return;
		}
		for (const decoded of decodeStreamFrame(frame)) this.#accept(decoded);
	}

	#accept(frame: TailFrame): void {
		if (frame.kind === "unknown" && this.#unknownDiagnostics < UNKNOWN_KIND_DIAGNOSTIC_CAP) {
			this.#unknownDiagnostics++;
			this.#input.onDiagnostic?.(`unknown_runtime_event session=${this.sessionId} kind=${frame.rawKind}`);
		}
		if (frame.commandId === undefined && frame.turnId === undefined) {
			// Uncorrelated: lifecycle noise (activity, identity_header, notifications
			// turn_stream). Only the idle marker matters to the actor.
			if (!frame.idle) return;
		} else if (!this.#matchesTurn(frame)) {
			if (this.#droppedForeign++ === 0)
				this.#input.onDiagnostic?.(
					`tail_frame_foreign session=${this.sessionId} kind=${frame.rawKind} commandId=${frame.commandId ?? "-"} turnId=${frame.turnId ?? "-"} expected=${this.#correlation.commandId ?? "-"}/${this.#correlation.turnId ?? "-"}`,
				);
			return;
		}
		// Only the turn's own activity moves the stall clock: hello, request
		// replies and unrelated notifications say nothing about the turn.
		this.#lastEventAt = this.#runner.now();
		this.#stallReported = false;
		this.#deliveries = this.#deliveries
			.then(async () => {
				if (this.#closed) return;
				await this.#input.onFrame?.(frame);
			})
			.catch((error: unknown) => {
				this.#input.onDiagnostic?.(
					`tail_frame_delivery_failed session=${this.sessionId} detail=${sanitizeDiagnostic(messageOf(error)) || "sdk_error"}`,
				);
			});
	}

	/**
	 * A frame belongs to the turn when its correlation matches what the accept
	 * receipt / agent_start named. Before any correlation is known (the receipt
	 * has not landed yet) the first correlated frame after beginTurn adopts it:
	 * this connection only ever owns the one turn it submitted.
	 */
	#matchesTurn(frame: TailFrame): boolean {
		if (this.#opRef === undefined) return false;
		const known = this.#correlation;
		if (known.commandId === undefined && known.turnId === undefined) {
			this.#correlation = {
				...(frame.commandId ? { commandId: frame.commandId } : {}),
				...(frame.turnId ? { turnId: frame.turnId } : {}),
			};
			return true;
		}
		// Every id both sides know must agree; a contradiction on either is a
		// foreign turn even when the other id happens to match.
		let compared = 0;
		if (known.commandId !== undefined && frame.commandId !== undefined) {
			if (known.commandId !== frame.commandId) return false;
			compared++;
		}
		if (known.turnId !== undefined && frame.turnId !== undefined) {
			if (known.turnId !== frame.turnId) return false;
			compared++;
		}
		return compared > 0;
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#stream?.close();
		this.#stream = undefined;
		this.#failPending("handle closed");
		if (!this.#ready) {
			this.#ready = true;
			this.#readyReject(new Error(`tail ${this.sessionId} was closed before readiness`));
		}
		this.#runner.release(this);
	}
}

function decodeResponse(frame: Record<string, unknown>): RelayResponse {
	const error = recordOf(frame.error);
	return {
		ok: frame.ok === true,
		...(recordOf(frame.result) ? { result: recordOf(frame.result) } : {}),
		...(error
			? {
					error: {
						...(typeof error.code === "string" ? { code: error.code } : {}),
						...(typeof error.message === "string" ? { message: error.message } : {}),
					},
				}
			: {}),
	};
}

/**
 * Host frames relayed by `gjc sdk serve --stdio`: lifecycle (`agent_start`,
 * `agent_end`, `agent_failed`, `activity`) and turn content wrapped as
 * `{type:"event", kind, payload:{event_type, event}, commandId, turnId}`.
 */
export function decodeStreamFrame(frame: Record<string, unknown>): readonly TailFrame[] {
	const type = typeof frame.type === "string" ? frame.type : "";
	const correlation = {
		...(typeof frame.commandId === "string" ? { commandId: frame.commandId } : {}),
		...(typeof frame.turnId === "string" ? { turnId: frame.turnId } : {}),
	};
	if (type === "event" && typeof frame.kind === "string") {
		const wrapper = recordOf(frame.payload) ?? {};
		const event = recordOf(wrapper.event) ?? wrapper;
		return [decodeEvent(frame.kind, event, correlation)];
	}
	if (type === "agent_start" || type === "agent_end" || type === "agent_failed") {
		const { type: _type, commandId: _c, turnId: _t, ...payload } = frame;
		return [
			{
				kind: type,
				rawKind: type,
				...correlation,
				payload,
				steerEcho: false,
				idle: type !== "agent_start",
			},
		];
	}
	if (type === "activity") {
		const { type: _type, ...payload } = frame;
		const idle = [payload.state, payload.status, payload.activity].some((value) => value === "idle");
		return [{ kind: "activity", rawKind: "activity", payload, steerEcho: false, idle }];
	}
	// Notifications-extension frames (identity_header, action_needed,
	// turn_stream, tool_activity, reasoning_summary, context_update): the owned
	// content stream already carries everything the gateway delivers.
	return [];
}

export function decodeStreamLine(line: string): readonly TailFrame[] {
	const trimmed = line.trim();
	if (!trimmed) return [];
	let parsed: unknown;
	try {
		parsed = JSON.parse(trimmed);
	} catch {
		return [];
	}
	const frame = recordOf(parsed);
	if (!frame || typeof frame.type !== "string") return [];
	return decodeStreamFrame(frame);
}

function decodeEvent(kind: string, event: Record<string, unknown>, correlation: TurnCorrelation): TailFrame {
	if (kind === "message_end") {
		const message = recordOf(event.message) ?? {};
		const role = typeof message.role === "string" ? message.role : "";
		const text = role === "assistant" ? contentText(message.content) : undefined;
		const id = typeof message.id === "string" ? message.id : undefined;
		return {
			kind: "message_end",
			rawKind: "message_end",
			...correlation,
			...(id ? { eventId: id } : {}),
			payload: { role, ...(message.content === undefined ? {} : { content: message.content }) },
			...(text ? { assistantText: text } : {}),
			steerEcho: role !== "assistant",
			idle: false,
		};
	}
	if (kind === "tool_execution_start" || kind === "tool_execution_update" || kind === "tool_execution_end") {
		const toolCallId = typeof event.toolCallId === "string" ? event.toolCallId : undefined;
		return {
			kind,
			rawKind: kind,
			...correlation,
			...(toolCallId ? { eventId: `${kind}:${toolCallId}` } : {}),
			payload: {
				...(typeof event.toolName === "string" ? { toolName: event.toolName } : {}),
				...(event.args === undefined ? {} : { args: event.args }),
				...(typeof event.intent === "string" ? { intent: event.intent } : {}),
				...(kind === "tool_execution_start" ? { toolCallStarted: true } : {}),
				...(kind === "tool_execution_end" && event.isError === true ? { isError: true } : {}),
			},
			steerEcho: false,
			idle: false,
		};
	}
	// message_update (streaming deltas) and anything else: progress only.
	return { kind: "unknown", rawKind: kind, ...correlation, payload: {}, steerEcho: false, idle: false };
}

/**
 * The chat-visible text of a content array. Only `text` blocks are speech:
 * a `thinking` block also carries `.text`, and joining it in would ship the
 * model's private reasoning to the channel as if the persona had said it.
 */
function contentText(content: unknown): string | undefined {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return undefined;
	const parts = content.flatMap((block) => {
		if (typeof block === "string") return [block];
		const record = recordOf(block);
		if (!record || typeof record.text !== "string") return [];
		return record.type === undefined || record.type === "text" ? [record.text] : [];
	});
	return parts.join("");
}

function messageOf(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${name} must be a positive integer`);
	return result;
}

/**
 * Identity of one INTERIM delivery of a persona turn: the inbound trigger,
 * the part's exact text, and its index. The terminal path hashes the final
 * answer the same way to recognise a slot the owned stream already shipped.
 */
export function deterministicInterimDeliveryId(
	originKey: string,
	triggerMessageId: string,
	text: string,
	part: number,
): string {
	const digest = createHash("sha256").update(`${originKey}|${triggerMessageId}|${part}|`).update(text).digest("hex");
	return `gw-i-${digest.slice(0, 32)}`;
}

/**
 * Identity of the turn's ONE terminal reply slot per part: the inbound trigger
 * and the part index, never the text. A regenerated or reconciled second answer
 * for the same trigger hashes to the same row and is dropped by the ledger.
 */
export function deterministicTerminalDeliveryId(originKey: string, triggerMessageId: string, part: number): string {
	return `gw-t-${createHash("sha256").update(`${originKey}|${triggerMessageId}|${part}`).digest("hex").slice(0, 32)}`;
}
