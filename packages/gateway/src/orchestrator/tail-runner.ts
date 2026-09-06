import { createHash } from "node:crypto";
import { SessionChannel } from "./session-channel";
import type { CliResult, CliRunner } from "@gajaeway/subsession";
import { sanitizeDiagnostic } from "./rebind";

/** The observed SDK-hosted top-level vocabulary from p2b §8. */
export const OBSERVED_TAIL_KINDS = [
	"transcript",
	"session_ready",
	"event",
	"identity_header",
	"agent_start",
	"agent_end",
	"agent_failed",
	"activity",
	"query_response",
	"tool_activity",
	"turn_stream",
] as const;

const OBSERVED_TAIL_KIND_SET = new Set<string>(OBSERVED_TAIL_KINDS);

export type TailEventKind = (typeof OBSERVED_TAIL_KINDS)[number] | "unknown";

export interface TailFrame {
	readonly kind: TailEventKind;
	readonly rawKind: string;
	readonly generation?: number;
	readonly seq?: number;
	/** Stable only when the runtime supplied id or generation+seq. Never invent an identity for delivery. */
	readonly eventId?: string;
	readonly payload: Record<string, unknown>;
	readonly assistantText?: string;
	/** A user/control echo is attribution evidence, never chat output. */
	readonly steerEcho: boolean;
	readonly idle: boolean;
}

export interface TailRetentionGap {
	readonly sessionId: string;
	readonly cursor?: string;
	readonly resync?: unknown;
}

export interface TailResyncCoordinate {
	readonly revision: number;
	readonly generation: number;
	readonly seq: number;
}

export interface TailAttachInput {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly repo: string;
	/** Caller-owned origin identity for grep-stable tail observability. */
	readonly originKey?: string;
	/** Opaque signed cursor persisted only after all preceding frame side effects commit. */
	cursor?: string;
	/** Current tails may wait for capacity; parked retired holds must not consume it. */
	priority?: "current" | "retired";
	/** Durable callback invoked only after preceding frames were delivered successfully. */
	onCursorCommitted?: (cursor: string) => void | Promise<void>;
	onCursorDiscarded?: () => void | Promise<void>;
	/** `false` is permitted only through attachResync after explicit operator approval. */
	strict?: boolean;
	/** Authenticated diagnostic coordinate retained for an explicit non-strict resync. */
	resync?: TailResyncCoordinate;
	onFrame?: (frame: TailFrame) => void | Promise<void>;
	onRetentionGap?: (gap: TailRetentionGap) => void | Promise<void>;
	onStall?: (input: { sessionId: string; brokerGeneration: number; elapsedMs: number }) => void | Promise<void>;
	onDiagnostic?: (line: string) => void;
}

export interface TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	/** Resolves only after the first broker-bound tail exchange completed safely. */
	readonly ready: Promise<void>;
	readonly cursor: string | undefined;
	/**
	 * Fresh-send boundary after initial attach/backfill and before the prompt.
	 * Historical buffered frames are intentionally fenced; the cursor is safe
	 * to commit because none belongs to the not-yet-started turn.
	 */
	beginTurn(opRef: string): Promise<void>;
	markAccepted(opRef: string): Promise<void>;
	setTurnRunning(running: boolean): void;
	close(): Promise<void>;
	/**
	 * Query multiplexer over this handle's resident relay (I4a). Undefined on the
	 * poll transport and while no stream is open; a complete read over it spawns
	 * nothing because the connection already exists.
	 */
	readonly channel: SessionChannel | undefined;
}

/** A resident event stream for one session (`gjc sdk serve --stdio --session <id>`). */
export interface TailStream {
	readonly lines: AsyncIterable<string>;
	/** Present on the resident relay: lets the session channel send query frames on this connection. */
	write?(line: string): void;
	close(): void;
}
export type TailStreamSpawner = (sessionId: string) => TailStream;

export interface TailRunnerOptions {
	readonly run: CliRunner;
	/**
	 * Event-driven transport. When present, each handle performs ONE non-strict
	 * backfill poll (pre-attach history, steer echoes), then consumes the live
	 * stream: frames arrive the instant the host emits them, no interval polling.
	 * Absent (tests), the handle falls back to the bounded poll loop.
	 */
	readonly stream?: TailStreamSpawner;
	/** Default session workspace. A handle may override it for future generic ports. */
	readonly repo: string;
	readonly maxTailProcesses?: number;
	readonly idleTtlMs?: number;
	readonly stallTimeoutMs?: number;
	readonly pollTimeoutMs?: number;
	readonly pollIntervalMs?: number;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	readonly log?: (line: string) => void;
}

const DEFAULT_MAX_TAIL_PROCESSES = 64;
const DEFAULT_IDLE_TTL_MS = 60_000;
const DEFAULT_STALL_TIMEOUT_MS = 120_000;
const DEFAULT_POLL_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_INTERVAL_MS = 250;
const UNKNOWN_KIND_DIAGNOSTIC_CAP = 20;
/** Tail frames may queue behind the actor mailbox while the channel keeps answering; beyond this the reader waits. */
const TAIL_DELIVERY_BACKLOG = 64;
const STREAM_REOPEN_GIVE_UP = 6;
const DELIVERED_ID_CAP = 2_000;

export class TailCapacityError extends Error {
	constructor() {
		super("tail capacity is reserved for current-generation work");
		this.name = "TailCapacityError";
	}
}

/**
 * Broker-bound tail supervisor. `CliRunner` is deliberately the only runtime
 * transport: no endpoint path/token is read by the gateway. Each finite CLI tail
 * response reattaches with the opaque cursor the runtime returned, producing one
 * long-lived logical tail while keeping process ownership in the broker layer.
 */
export class TailRunner {
	readonly #run: CliRunner;
	readonly #stream: TailStreamSpawner | undefined;
	readonly #repo: string;
	readonly #maxTailProcesses: number;
	readonly #idleTtlMs: number;
	#stallTimeoutMs: number;
	readonly #pollTimeoutMs: number;
	readonly #pollIntervalMs: number;
	readonly #now: () => number;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly #log: (line: string) => void;
	readonly #handles = new Set<ManagedTailHandle>();
	readonly #waiters: Array<() => void> = [];
	#lastSaturationAlertAt: number | undefined;
	readonly channelTimers: Pick<TailRunnerOptions, "setTimeout" | "clearTimeout">;
	#channelRestarts = 0;
	#channelFaults = 0;

	constructor(options: TailRunnerOptions) {
		this.#run = options.run;
		this.#stream = options.stream;
		this.#repo = options.repo;
		this.#maxTailProcesses = positiveInteger(options.maxTailProcesses, DEFAULT_MAX_TAIL_PROCESSES, "maxTailProcesses");
		this.#idleTtlMs = positiveInteger(options.idleTtlMs, DEFAULT_IDLE_TTL_MS, "idleTtlMs");
		this.#stallTimeoutMs = positiveInteger(options.stallTimeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
		this.#pollTimeoutMs = positiveInteger(options.pollTimeoutMs, DEFAULT_POLL_TIMEOUT_MS, "pollTimeoutMs");
		this.#pollIntervalMs = nonNegativeInteger(options.pollIntervalMs, DEFAULT_POLL_INTERVAL_MS, "pollIntervalMs");
		this.#now = options.now ?? (() => Date.now());
		this.#sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
		this.#log = options.log ?? ((line: string) => console.error(line));
		this.channelTimers = { setTimeout: options.setTimeout, clearTimeout: options.clearTimeout };
	}

	get activeCount(): number {
		return this.#handles.size;
	}

	/** Resident relays currently attached (healthy or degraded); the residency ceiling counts all of them. */
	get residentChannels(): number {
		return [...this.#handles].filter((handle) => handle.channel !== undefined).length;
	}

	#lastChannelFaultAt: number | undefined;
	get lastChannelFaultAt(): number | undefined {
		return this.#lastChannelFaultAt;
	}

	/** I9b: the resident channel for a session, when one is attached (healthy or not; callers check `health()`). */
	channel(sessionId: string): SessionChannel | undefined {
		for (const handle of this.#handles) if (handle.sessionId === sessionId && handle.channel) return handle.channel;
		return undefined;
	}

	get channelRestarts(): number {
		return this.#channelRestarts;
	}
	get channelFaults(): number {
		return this.#channelFaults;
	}
	recordChannelRestart(): void {
		this.#channelRestarts++;
	}
	recordChannelFault(): void {
		this.#channelFaults++;
		this.#lastChannelFaultAt = this.#now();
	}

	/** Internal clock boundary exposed to managed handles and deterministic tests. */
	now(): number {
		return this.#now();
	}

	get stallTimeoutMs(): number {
		return this.#stallTimeoutMs;
	}

	setStallTimeoutMs(timeoutMs: number): void {
		this.#stallTimeoutMs = positiveInteger(timeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
	}

	get pollIntervalMs(): number {
		return this.#pollIntervalMs;
	}

	get streamSpawner(): TailStreamSpawner | undefined {
		return this.#stream;
	}

	sleep(ms: number): Promise<void> {
		return this.#sleep(ms);
	}

	/**
	 * Attaches before a prompt is sent. When all slots are running, this waits
	 * without changing durable inbound state; callers retain their bound turn.
	 */
	async attach(input: TailAttachInput): Promise<TailHandle> {
		// I9b residency: a parked resident relay for the same session under the
		// same owner is adopted instead of spawned; the warm turn costs no launch.
		for (const parked of this.#handles) {
			if (parked.adopt({ ...input, repo: input.repo || this.#repo })) {
				this.#log(`tail_resident_adopted session=${input.sessionId} restarts=${parked.channel?.stats().restarts ?? 0}`);
				return parked;
			}
		}
		await this.#acquireSlot(input.priority ?? "current");
		const handle = new ManagedTailHandle(this, { ...input, repo: input.repo || this.#repo });
		this.#handles.add(handle);
		handle.start();
		try {
			await handle.ready;
			return handle;
		} catch (error) {
			await handle.terminate();
			throw error;
		}
	}

	/**
	 * Replays a strict retention gap only after an explicit operator-approved
	 * handoff. The signed opaque cursor remains the runtime authority; the public
	 * coordinate is validated and recorded so an arbitrary historical replay can
	 * never be started implicitly.
	 */
	async attachResync(
		input: TailAttachInput & {
			readonly cursor: string;
			readonly resync: TailResyncCoordinate;
			readonly operatorApproved: true;
		},
	): Promise<TailHandle> {
		if (!validResyncCoordinate(input.resync)) throw new Error("tail resync coordinate is invalid");
		const { operatorApproved: _operatorApproved, ...tailInput } = input;
		return await this.attach({ ...tailInput, strict: false });
	}

	/** Test/loop seam: checks exact threshold, so 119.9 seconds never emits a 120s alarm. */
	checkStalls(now = this.#now()): void {
		for (const handle of this.#handles) handle.checkStall(now);
	}

	/** Shutdown: terminates every handle, parked residents included. */
	async terminateAll(): Promise<void> {
		for (const handle of [...this.#handles]) await handle.terminate();
	}

	/** Reaps only idle handles; an active turn is never evicted for capacity. */
	async reapIdle(now = this.#now()): Promise<number> {
		const idle = [...this.#handles].filter(
			(handle) => handle.idleSince !== undefined && now - handle.idleSince >= this.#idleTtlMs,
		);
		for (const handle of idle) await handle.terminate();
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
				await evictable.terminate();
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

	async poll(handle: ManagedTailHandle): Promise<void> {
		const args = [
			"sdk",
			"session",
			"tail",
			handle.sessionId,
			...(handle.cursor ? ["--cursor", handle.cursor] : []),
			"--until-idle",
			// `--strict` only means something relative to a checkpoint. A cursorless
			// attach has no checkpoint, and the runtime reports the ring's pre-attach
			// drop as a retention gap on every poll (measured on gjc 0.16.0), so a
			// strict cursorless tail could never observe its own turn. Cursorless
			// polls run non-strict; duplicate/historical frames are fenced by the
			// accepted-op-ref attribution filter and deterministic delivery ids.
			...(handle.strict && handle.cursor ? ["--strict"] : []),
			"--all-events",
			"--timeout-ms",
			String(this.#pollTimeoutMs),
		];
		const result = await this.#run(args, { timeoutMs: this.#pollTimeoutMs + 5_000 });
		let decoded = decodeTailResult(result, handle.cursor !== undefined);
		if (decoded.cursorUnusable && handle.cursor) {
			await handle.discardCursor(decoded.cursorUnusable);
			const cursorless = args.filter(
				(arg, index) => arg !== "--strict" && arg !== "--cursor" && args[index - 1] !== "--cursor",
			);
			decoded = decodeTailResult(await this.#run(cursorless, { timeoutMs: this.#pollTimeoutMs + 5_000 }));
		}
		if (decoded.gap && handle.strict && handle.cursor) {
			await handle.retentionGap(decoded.gap);
			return;
		}
		if (decoded.gap) {
			// Non-strict poll: the gap is diagnostic (the ring dropped pre-attach
			// history), not a hold. Frames after the resync point still arrive.
			handle.noteGap(decoded.gap);
		}
		if (decoded.error) throw decoded.error;
		await handle.receiveBatch(decoded.frames, decoded.cursor);
		if (decoded.terminal) handle.setIdle();
	}

	#logSaturation(): void {
		const now = this.#now();
		if (this.#lastSaturationAlertAt !== undefined && now - this.#lastSaturationAlertAt < 60_000) return;
		this.#lastSaturationAlertAt = now;
		this.#log(`tail_saturation active=${this.#handles.size} limit=${this.#maxTailProcesses}`);
	}
}

class ManagedTailHandle implements TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly repo: string;
	readonly #runner: TailRunner;
	#input: TailAttachInput;
	readonly #readyResolve: () => void;
	readonly #readyReject: (error: unknown) => void;
	readonly ready: Promise<void>;
	readonly #preReceipt: TailFrame[] = [];
	#cursor: string | undefined;
	#pendingCursor: string | undefined;
	#acceptedOpRef: string | undefined;
	readonly #strict: boolean;
	#accepted = false;
	/** Ordered flush of pre-receipt frames; later frames queue behind it. */
	#flush: Promise<void> = Promise.resolve();
	#closed = false;
	#pausedForGap = false;
	/** Set once a frame's side effect failed: no later checkpoint may commit past the lost frame. */
	#deliveryFailed = false;
	#running = false;
	#polling = false;
	#ready = false;
	#lastEventAt: number;
	#idleSince: number | undefined;
	#stallReported = false;
	#unknownDiagnostics = 0;

	constructor(runner: TailRunner, input: TailAttachInput) {
		this.#runner = runner;
		this.#input = input;
		this.sessionId = input.sessionId;
		this.brokerGeneration = input.brokerGeneration;
		this.repo = input.repo;
		this.#cursor = input.cursor;
		this.#strict = input.strict !== false;
		this.#lastEventAt = runner.now();
		let resolve: () => void = () => {};
		let reject: (error: unknown) => void = () => {};
		this.ready = new Promise<void>((done, failed) => {
			resolve = done;
			reject = failed;
		});
		this.#readyResolve = resolve;
		this.#readyReject = reject;
	}

	get cursor(): string | undefined {
		return this.#cursor;
	}

	get strict(): boolean {
		return this.#strict;
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

	async beginTurn(opRef: string): Promise<void> {
		if (this.#closed || this.#accepted) return;
		this.#acceptedOpRef = opRef;
		this.#preReceipt.splice(0);
		await this.#commitPendingCursor();
	}
	async markAccepted(opRef: string): Promise<void> {
		this.#deliveredIds.clear();
		if (this.#closed || this.#accepted) return;
		this.#accepted = true;
		this.#acceptedOpRef = opRef;
		// The flush is deliberately NOT awaited by the caller: markAccepted is
		// invoked from inside the origin mailbox (dispatch/recovery), and onFrame
		// re-enters that same mailbox. Delivering synchronously here would
		// self-deadlock; delivering in the poll loop's turn keeps frame side
		// effects ordered (later frames wait on #flush) and ahead of the cursor
		// commit that follows them.
		const buffered = this.#preReceipt.splice(0);
		this.#flush = this.#flush
			.then(async () => {
				for (const frame of buffered) {
					// close() ends this owner's authority mid-flush: a queued frame
					// must not leak to the old owner, and no cursor may commit.
					if (this.#closed) return;
					await this.#deliver(frame);
				}
				await this.#commitPendingCursor();
			})
			.catch((error: unknown) => {
				// A failed side effect leaves the cursor uncommitted (retry-safe), and
				// the checkpoint that covered the failed frame is discarded so a later
				// successful batch cannot commit past it. The chain itself stays
				// settled so later frames are not poisoned.
				this.#pendingCursor = undefined;
				this.#deliveryFailed = true;
				this.#input.onDiagnostic?.(
					`tail_flush_failed session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			});
	}

	setTurnRunning(running: boolean): void {
		if (this.#closed) return;
		this.#running = running;
		this.#stallReported = false;
		if (running) this.#idleSince = undefined;
		else this.setIdle();
	}

	setCursor(cursor: string): void {
		// The cursor must be opaque and runtime-issued. Public checkpoint objects are not accepted by --cursor.
		if (cursor.length > 0) this.#pendingCursor = cursor;
	}

	setIdle(): void {
		this.#running = false;
		this.#stallReported = false;
		this.#idleSince ??= this.#runner.now();
	}

	checkStall(now: number): void {
		if (this.#closed || !this.#running || this.#stallReported) return;
		const elapsedMs = now - this.#lastEventAt;
		if (elapsedMs < this.#runner.stallTimeoutMs) return;
		this.#stallReported = true;
		void this.#input.onStall?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration, elapsedMs });
	}

	async receiveBatch(frames: readonly TailFrame[], cursor: string | undefined): Promise<void> {
		for (const frame of frames) await this.receive(frame);
		if (cursor) this.setCursor(cursor);
		// An empty terminal poll must not checkpoint past buffered pre-receipt
		// frames whose deferred flush is still delivering their side effects.
		await this.#flush;
		await this.#commitPendingCursor();
	}

	async receive(frame: TailFrame): Promise<void> {
		if (this.#closed) return;
		this.#lastEventAt = this.#runner.now();
		this.#stallReported = false;
		if (frame.idle) this.setIdle();
		if (frame.kind === "unknown" && this.#unknownDiagnostics < UNKNOWN_KIND_DIAGNOSTIC_CAP) {
			this.#unknownDiagnostics++;
			this.#input.onDiagnostic?.(`unknown_runtime_event session=${this.sessionId} kind=${frame.rawKind}`);
		}
		if (!this.#accepted) {
			this.#preReceipt.push(frame);
			return;
		}
		await this.#flush;
		if (this.#closed) return;
		await this.#deliver(frame);
	}

	#gapNoted = false;

	noteGap(gap: { cursor?: string; resync?: unknown }): void {
		if (this.#gapNoted) return;
		this.#gapNoted = true;
		this.#input.onDiagnostic?.(
			`tail_gap_nonstrict session=${this.sessionId} resync=${JSON.stringify(gap.resync ?? null)}`,
		);
	}

	async discardCursor(code: string): Promise<void> {
		await this.#input.onCursorDiscarded?.();
		this.#cursor = undefined;
		this.#pendingCursor = undefined;
		this.#input.onDiagnostic?.(`tail_cursor_discarded session=${this.sessionId} code=${code}`);
	}

	async retentionGap(gap: { cursor?: string; resync?: unknown }): Promise<void> {
		if (this.#closed) return;
		// A strict gap is not a terminal event. It never falls through into a
		// cursorless poll, which would silently replay historical transcript data.
		this.#cursor = undefined;
		this.#pendingCursor = undefined;
		this.#pausedForGap = true;
		await this.#input.onRetentionGap?.({
			sessionId: this.sessionId,
			...(gap.cursor ? { cursor: gap.cursor } : {}),
			...(gap.resync === undefined ? {} : { resync: gap.resync }),
		});
		if (!this.#ready) {
			this.#ready = true;
			this.#readyResolve();
		}
		await this.close();
	}

	/** Parked: the owner released the turn but the resident relay (and its channel) stays alive for the next attach. */
	#parked = false;

	get parked(): boolean {
		return this.#parked;
	}

	/**
	 * Adopts this parked handle for a new attach of the same session and owner.
	 * Returns false when the handle is not parked, belongs to another owner, or
	 * has no healthy resident channel (then the caller spawns as before).
	 */
	adopt(input: TailAttachInput): boolean {
		if (!this.#parked || this.#closed) return false;
		if (input.sessionId !== this.sessionId || input.originKey !== this.#input.originKey) return false;
		if (!this.#channel || !this.#channel.health().healthy) return false;
		if (input.brokerGeneration !== this.brokerGeneration) return false;
		this.#parked = false;
		this.#input = input;
		this.#accepted = false;
		this.#acceptedOpRef = undefined;
		this.#preReceipt.splice(0);
		this.#deliveredIds.clear();
		this.#deliveryFailed = false;
		this.#stallReported = false;
		this.#lastEventAt = this.#runner.now();
		this.#idleSince = undefined;
		return true;
	}

	/** Force-closes even a resident handle (idle eviction, capacity, shutdown, fault). */
	async terminate(): Promise<void> {
		this.#parked = false;
		this.#resident = false;
		await this.close();
	}

	#resident = false;

	async close(): Promise<void> {
		if (this.#closed) return;
		// A resident relay with a healthy channel is parked, not torn down: the
		// child keeps streaming, the channel keeps probing, and the next attach
		// for the same session/owner adopts it (I9b warm-turn zero-spawn).
		if (this.#resident && this.#channel?.health().healthy && !this.#parked) {
			this.#parked = true;
			this.#running = false;
			this.#idleSince ??= this.#runner.now();
			this.#accepted = false;
			this.#acceptedOpRef = undefined;
			this.#input = { ...this.#input, onFrame: undefined, onStall: undefined, onRetentionGap: undefined };
			return;
		}
		this.#closed = true;
		this.#channel?.close();
		this.#channel = undefined;
		this.#stream?.close();
		this.#stream = undefined;
		if (!this.#ready) this.#readyReject(new Error(`tail ${this.sessionId} was closed before readiness`));
		this.#runner.release(this);
	}

	async #commitPendingCursor(): Promise<void> {
		const cursor = this.#pendingCursor;
		// A lost frame is only recoverable from the last committed cursor; once a
		// delivery failed, this handle commits nothing more (re-attach resumes it).
		if (!cursor || !this.#accepted || this.#deliveryFailed || this.#closed) return;
		await this.#input.onCursorCommitted?.(cursor);
		this.#cursor = cursor;
		this.#pendingCursor = undefined;
	}

	/** Frame ids already handed to the actor; a backfill after a stream reopen replays history and must never re-deliver. */
	readonly #deliveredIds = new Set<string>();

	async #deliver(frame: TailFrame): Promise<void> {
		// Finalized answers are also keyed by messageRef so a backfill transcript row
		// and the live turn_stream frame for the same answer never both deliver.
		// gjc 0.16 synthesizes `generation:seq` ids that restart per revision, so
		// every turn's first answer is "1:1": never a dedupe key. Only a real id
		// or the answer text counts, and only within the current accepted turn.
		const synthetic = frame.eventId !== undefined && /^\d+:\d+$/.test(frame.eventId);
		const key =
			frame.eventId && !synthetic
				? frame.eventId
				: frame.assistantText
					? `text:${Bun.hash(frame.assistantText)}`
					: undefined;
		if (key) {
			if (this.#deliveredIds.has(key)) {
				this.#input.onDiagnostic?.(`tail_frame_duplicate session=${this.sessionId} event=${key}`);
				return;
			}
			this.#deliveredIds.add(key);
			if (this.#deliveredIds.size > DELIVERED_ID_CAP)
				this.#deliveredIds.delete(this.#deliveredIds.values().next().value as string);
		}
		const attributedOpRef = tailOperationRef(frame);
		if (attributedOpRef && this.#acceptedOpRef && attributedOpRef !== this.#acceptedOpRef) {
			this.#input.onDiagnostic?.(
				`tail_frame_ignored session=${this.sessionId} event=${frame.eventId ?? "unidentified"} opRef=${attributedOpRef}`,
			);
			return;
		}
		await this.#input.onFrame?.(frame);
	}

	#stream: TailStream | undefined;
	#channel: SessionChannel | undefined;
	#reopenFailures = 0;
	#channelRestartCount = 0;

	get channel(): SessionChannel | undefined {
		return this.#channel;
	}

	async #run(): Promise<void> {
		const spawner = this.#runner.streamSpawner;
		if (spawner) {
			await this.#runStreaming(spawner);
			return;
		}
		while (!this.#closed) {
			this.#polling = true;
			try {
				await this.#runner.poll(this);
				if (!this.#ready) {
					this.#ready = true;
					this.#readyResolve();
				}
			} catch (error) {
				if (!this.#ready) {
					this.#ready = true;
					this.#readyReject(error);
					await this.terminate();
					return;
				}
				this.#input.onDiagnostic?.(
					`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				this.#polling = false;
			}
			if (this.#closed || this.#pausedForGap) return;
			await this.#runner.sleep(this.#runner.pollIntervalMs);
		}
	}

	/**
	 * Event-driven observation: one backfill poll establishes readiness and
	 * replays pre-attach history, then the resident stream delivers every host
	 * frame as it is emitted. A stream that ends while the handle is open is
	 * re-opened after a backfill, so nothing emitted in between is lost.
	 */
	async #runStreaming(spawner: TailStreamSpawner): Promise<void> {
		while (!this.#closed) {
			this.#polling = true;
			try {
				await this.#runner.poll(this);
				if (!this.#ready) {
					this.#ready = true;
					this.#readyResolve();
				}
			} catch (error) {
				if (!this.#ready) {
					this.#ready = true;
					this.#readyReject(error);
					await this.terminate();
					return;
				}
				this.#input.onDiagnostic?.(
					`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				this.#polling = false;
			}
			if (this.#closed || this.#pausedForGap) return;
			let stream: TailStream;
			try {
				stream = spawner(this.sessionId);
			} catch (error) {
				this.#runner.recordChannelFault();
				this.#input.onDiagnostic?.(
					`channel_degraded session=${this.sessionId} reason=spawn_error detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error))}`,
				);
				await this.#runner.sleep(Math.min(5_000, 250 * 2 ** Math.min(this.#reopenFailures++, 5)));
				if (!this.#closed) {
					this.#channelRestartCount++;
					this.#runner.recordChannelRestart();
				}
				continue;
			}
			this.#stream = stream;
			const openedAt = this.#runner.now();
			// The channel shares this connection: it writes query frames to the
			// relay's stdin and claims only the responses carrying its own ids; every
			// other line stays a tail frame.
			const listeners = new Set<(line: string) => void>();
			const channel = stream.write
				? new SessionChannel({
						sessionId: this.sessionId,
						now: () => this.#runner.now(),
						...this.#runner.channelTimers,
						restarts: this.#channelRestartCount,
						transport: {
							write: (line) => stream.write?.(line),
							onLine: (listener) => {
								listeners.add(listener);
								return () => listeners.delete(listener);
							},
						},
						log: (line) => this.#input.onDiagnostic?.(line),
					})
				: undefined;
			this.#channel = channel;
			this.#resident = channel !== undefined;
			channel?.onFault(() => {
				this.#runner.recordChannelFault();
				stream.close();
			});
			channel?.startIdleProbe();
			try {
				// Channel responses are settled the moment they are read; only tail
				// frames go through the awaited delivery path. Otherwise a mailbox task
				// that awaits a channel query (e.g. reconcile -> turn.result) while the
				// reader awaits that same mailbox for a tail frame deadlocks until the
				// query times out (live soak 2026-09-06: channel_degraded reason=timeouts).
				const pendingFrames: string[] = [];
				let deliverChain: Promise<void> = Promise.resolve();
				const deliver = (line: string) => {
					pendingFrames.push(line);
					deliverChain = deliverChain.then(async () => {
						const next = pendingFrames.shift();
						if (next === undefined || this.#closed) return;
						for (const frame of decodeStreamLine(next)) await this.receive(frame);
					});
					return deliverChain;
				};
				let delivered: Promise<void> = Promise.resolve();
				for await (const line of stream.lines) {
					if (this.#closed) break;
					const consumed = channel?.consumes(line);
					for (const listener of listeners) listener(line);
					if (channel && !channel.health().healthy) break;
					if (consumed) continue;
					delivered = deliver(line).catch((error: unknown) => {
						this.#input.onDiagnostic?.(
							`tail_frame_delivery_failed session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
						);
					});
					// Bounded backpressure: never let undelivered tail frames pile up
					// unboundedly, but allow the channel to keep answering meanwhile.
					if (pendingFrames.length >= TAIL_DELIVERY_BACKLOG) await delivered;
				}
				await delivered;
			} catch (error) {
				this.#input.onDiagnostic?.(
					`tail_stream_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				if (!this.#closed) channel?.fault("child_exit");
				this.#channel = undefined;
				channel?.close();
				this.#stream = undefined;
				stream.close();
			}
			if (this.#closed || this.#pausedForGap) return;
			const sinceOpen = this.#runner.now() - openedAt;
			if (channel) {
				if (sinceOpen >= 5_000 && channel.health().lastFrameAt > openedAt) this.#reopenFailures = 0;
			} else {
				this.#reopenFailures = sinceOpen < 5_000 ? this.#reopenFailures + 1 : 0;
				if (this.#reopenFailures >= STREAM_REOPEN_GIVE_UP) {
					await this.retentionGap({ resync: { revision: 0, generation: 0, seq: 0 } });
					return;
				}
			}
			const backoff = channel
				? Math.min(5_000, 250 * 2 ** Math.min(this.#reopenFailures++, 5))
				: Math.min(30_000, this.#runner.pollIntervalMs * 2 ** this.#reopenFailures);
			this.#input.onDiagnostic?.(`tail_stream_reopen session=${this.sessionId} backoffMs=${backoff}`);
			await this.#runner.sleep(backoff);
			if (channel && !this.#closed) {
				this.#channelRestartCount++;
				this.#runner.recordChannelRestart();
			}
		}
	}
}

type DecodedTail = {
	readonly frames: readonly TailFrame[];
	readonly cursor?: string;
	readonly terminal: boolean;
	readonly gap?: { readonly cursor?: string; readonly resync?: unknown };
	readonly error?: Error;
	readonly cursorUnusable?: string;
};

export function decodeTailResult(result: CliResult, hasCursor = false): DecodedTail {
	let envelope: Record<string, unknown>;
	try {
		envelope = JSON.parse(result.stdout) as Record<string, unknown>;
	} catch {
		return {
			frames: [],
			terminal: false,
			error: new Error(`session tail did not print a JSON envelope (exit ${result.exitCode})`),
		};
	}
	const error = recordOf(envelope.error);
	const errorCode = typeof error?.code === "string" ? error.code : undefined;
	if (result.exitCode !== 0 || envelope.ok !== true) {
		if (
			errorCode &&
			(["invalid_cursor", "cursor_expired", "snapshot_capacity_exceeded"].includes(errorCode) ||
				(hasCursor && errorCode === "invalid_input"))
		) {
			return {
				frames: [],
				terminal: false,
				cursorUnusable: errorCode,
				error: new Error(`session tail failed: ${errorCode}`),
			};
		}
		if (errorCode === "retention_gap") {
			return { frames: [], terminal: false, gap: { resync: recordOf(error?.details)?.resync ?? error?.resync } };
		}
		// `--until-idle` bounded by `--timeout-ms` on a quiet session: nothing to
		// report this poll. Not an error, not terminal, not a gap.
		if (errorCode === "tail_timeout") return { frames: [], terminal: false };
		return {
			frames: [],
			terminal: false,
			error: new Error(`session tail failed${errorCode ? `: ${errorCode}` : ` (exit ${result.exitCode})`}`),
		};
	}
	const payload = recordOf(envelope.result) ?? {};
	const gap = recordOf(payload.gap);
	const items = Array.isArray(payload.items) ? payload.items : [];
	// A non-strict reply may carry BOTH a diagnostic gap (pre-checkpoint history
	// dropped) and the live frames after the resync point. The poll decides
	// whether the gap is a hold (strict, checkpointed) or a diagnostic.
	return {
		frames: items.flatMap(normalizeTailFrame),
		...(opaqueCursorOf(payload) ? { cursor: opaqueCursorOf(payload) } : {}),
		terminal: payload.terminal === true,
		...(gap?.code === "retention_gap"
			? { gap: { ...(opaqueCursorOf(payload) ? { cursor: opaqueCursorOf(payload) } : {}), resync: gap.resync } }
			: {}),
	};
}

/**
 * Host WebSocket frames relayed by `gjc sdk serve --stdio` are the same
 * lifecycle vocabulary the `tail` CLI projects into items (`activity`,
 * `agent_start`/`agent_end`, `turn_stream`, transcript rows), just unwrapped:
 * `{type, ...payload}` instead of `{kind, payload}`. Project them onto the one
 * TailFrame shape so the actor never learns which transport delivered them.
 */
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
	if (frame.type === "hello" || frame.type === "pong") return [];
	const { type, generation, seq, id, ...payload } = frame;
	if (type === "event" && typeof frame.kind === "string") {
		// Ring-projected events carry kind/payload/generation/seq already.
		return normalizeTailFrame({ kind: frame.kind, payload: recordOf(frame.payload) ?? payload, generation, seq, id });
	}
	if (type === "turn_stream") {
		// A finalized assistant answer is the live equivalent of an assistant
		// transcript row; partial phases are progress only.
		if (payload.phase === "finalized" && typeof payload.text === "string" && payload.text.length > 0) {
			const ref = typeof payload.messageRef === "string" ? payload.messageRef : undefined;
			return normalizeTailFrame({
				kind: "transcript",
				...(ref ? { id: `turn_stream:${ref}` } : {}),
				payload: {
					role: "assistant",
					content: [{ type: "text", text: payload.text }],
					...(payload.clientRef ? { clientRef: payload.clientRef } : {}),
				},
			});
		}
		return normalizeTailFrame({ kind: "turn_stream", payload });
	}
	const turnId = typeof payload.turnId === "string" ? payload.turnId : undefined;
	const stableId = typeof id === "string" ? id : turnId ? `${type}:${turnId}` : undefined;
	return normalizeTailFrame({ kind: type, payload, ...(stableId ? { id: stableId } : {}), generation, seq });
}

function normalizeTailFrame(value: unknown): readonly TailFrame[] {
	const item = recordOf(value);
	if (!item || typeof item.kind !== "string") return [];
	const payload = recordOf(item.payload) ?? {};
	const rawKind = item.kind;
	const kind: TailEventKind = OBSERVED_TAIL_KIND_SET.has(rawKind) ? (rawKind as TailEventKind) : "unknown";
	const generation = typeof item.generation === "number" ? item.generation : undefined;
	const seq = typeof item.seq === "number" ? item.seq : undefined;
	const id =
		typeof item.id === "string" && item.id.length > 0
			? item.id
			: generation !== undefined && seq !== undefined
				? `${generation}:${seq}`
				: undefined;
	const transcriptText =
		rawKind === "transcript" && payload.role === "assistant" ? contentText(payload.content) : undefined;
	// Current GJC hosts emit the live/final assistant channel as turn_stream.
	// Only the explicit finalized final-answer frame is chat output: live drafts
	// and reasoning summaries are progress, never deliverable text.
	const finalizedTurnText =
		rawKind === "turn_stream" && payload.phase === "finalized" && payload.finalAnswer === true
			? contentText(payload.text)
			: undefined;
	const text = transcriptText ?? finalizedTurnText;
	const clientRef =
		typeof payload.clientRef === "string"
			? payload.clientRef
			: typeof item.clientRef === "string"
				? item.clientRef
				: undefined;
	const idle =
		rawKind === "activity" && [payload.state, payload.status, payload.activity].some((value) => value === "idle");
	if (rawKind === "tool_activity" && (payload.phase === "started" || payload.phase === "start"))
		payload.toolCallStarted = true;
	return [
		{
			kind,
			rawKind,
			...(generation === undefined ? {} : { generation }),
			...(seq === undefined ? {} : { seq }),
			...(id === undefined ? {} : { eventId: id }),
			payload,
			...(text === undefined || text === "" ? {} : { assistantText: text }),
			steerEcho: clientRef !== undefined || (rawKind === "transcript" && payload.role === "user"),
			idle,
		},
	];
}

function contentText(content: unknown): string | undefined {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return undefined;
	const parts = content.flatMap((block) => {
		if (typeof block === "string") return [block];
		const record = recordOf(block);
		return typeof record?.text === "string" ? [record.text] : [];
	});
	return parts.join("");
}

export function tailOperationRef(frame: TailFrame): string | undefined {
	for (const value of [frame.payload.opRef, frame.payload.operationRef, frame.payload.clientRef])
		if (typeof value === "string" && value.length > 0) return value;
	return undefined;
}

/**
 * Host-stamped time of a transcript row (`ts`, ISO-8601 from the session's
 * entry timestamp). Live lifecycle frames carry none; only durable rows do,
 * and those are exactly the frames a cursorless resync can replay.
 */
export function tailFrameTimestampMs(frame: TailFrame): number | undefined {
	const ts = frame.payload.ts;
	if (typeof ts !== "string") return undefined;
	const at = Date.parse(ts);
	return Number.isFinite(at) ? at : undefined;
}

function validResyncCoordinate(value: TailResyncCoordinate): boolean {
	return [value.revision, value.generation, value.seq].every((part) => Number.isSafeInteger(part) && part >= 0);
}

function opaqueCursorOf(payload: Record<string, unknown>): string | undefined {
	for (const value of [payload.cursor, payload.nextCursor, payload.next_cursor, recordOf(payload.page)?.cursor]) {
		if (typeof value === "string" && value.length > 0) return value;
	}
	return undefined;
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

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result < 0) throw new Error(`${name} must be a non-negative integer`);
	return result;
}

/**
 * Identity of one INTERIM delivery of a persona turn: the inbound trigger,
 * the part's exact text, and its index. Two different mid-turn findings get two
 * ids; the same finding replayed (stream reopen backfill, gateway restart with
 * an id-less frame) hashes to the row that already exists.
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
