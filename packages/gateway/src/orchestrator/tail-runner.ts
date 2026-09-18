import { createHash } from "node:crypto";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
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
}

/** A resident event stream for one session (`gjc sdk serve --stdio --session <id>`). */
export interface TailStream {
	readonly lines: AsyncIterable<string>;
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
	readonly log?: (line: string) => void;
}

const DEFAULT_MAX_TAIL_PROCESSES = 64;
const DEFAULT_IDLE_TTL_MS = 60_000;
const DEFAULT_STALL_TIMEOUT_MS = 120_000;
const DEFAULT_POLL_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_INTERVAL_MS = 250;
/**
 * Wait window for a poll issued while a turn is running. gjc >= 0.17.2 returns
 * what it collected when the window closes (`terminal: false`); before that it
 * threw `tail_timeout` and discarded it, so the window doubled as the turn's
 * observation latency: a 30 s window meant one poll per turn, nothing mid-turn.
 */
const SIDE_POLL_WINDOW_MS = 1_500;
const UNKNOWN_KIND_DIAGNOSTIC_CAP = 20;
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
	}

	get activeCount(): number {
		return this.#handles.size;
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

	async poll(handle: ManagedTailHandle, options?: { readonly checkpointOnly?: boolean }): Promise<void> {
		// A poll alongside a running turn is a bounded observation, not a wait:
		// with `--until-idle` the CLI cannot exit until the turn ends, so the
		// window is what decides how soon mid-turn rows come back. The backfill
		// poll (no turn running) keeps the long window so a quiet session is
		// observed cheaply.
		const windowMs = handle.turnRunning || options?.checkpointOnly ? SIDE_POLL_WINDOW_MS : this.#pollTimeoutMs;
		const args = [
			"sdk",
			"session",
			"tail",
			handle.sessionId,
			...(handle.pollCursor ? ["--cursor", handle.pollCursor] : []),
			// A resumed tail pages a fresh snapshot and drops rows up to this id, so
			// the poll returns only the transcript written since the last one - the
			// rows that carry tool calls and interim text. Without it, resume returns
			// ring events only and every turn reads as toolCalls=0 (gjc >= 0.17.2).
			...(handle.pollCursor && handle.lastTranscriptId ? ["--after-transcript-id", handle.lastTranscriptId] : []),
			"--until-idle",
			// `--strict` only means something relative to a checkpoint. A cursorless
			// attach has no checkpoint, and the runtime reports the ring's pre-attach
			// drop as a retention gap on every poll (measured on gjc 0.16.0), so a
			// strict cursorless tail could never observe its own turn. Cursorless
			// polls run non-strict; duplicate/historical frames are fenced by the
			// accepted-op-ref attribution filter and deterministic delivery ids.
			...(handle.strict && handle.pollCursor ? ["--strict"] : []),
			"--all-events",
			"--timeout-ms",
			String(windowMs),
		];
		const result = await this.#run(args, { timeoutMs: windowMs + 5_000 });
		const decoded = decodeTailResult(result);
		if (decoded.invalidCursor) {
			// The host no longer honours our checkpoint (expired, or the host
			// restarted). Not a gap and not a failure: drop it, and the next poll is
			// a fresh cursorless tail whose replay is fenced downstream as before.
			handle.dropCursor();
			return;
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
		if (decoded.frames.length > 0 || decoded.cursor) {
			// One line per productive poll: enough to see, from the log alone, whether
			// a running turn's tool rows ever reached the handle.
			const kinds: Record<string, number> = {};
			for (const frame of decoded.frames) kinds[frame.rawKind] = (kinds[frame.rawKind] ?? 0) + 1;
			const tools = decoded.frames.filter((frame) => frame.payload.toolCallStarted === true).length;
			handle.noteDiagnostic(
				`tail_poll session=${handle.sessionId} resumed=${handle.pollCursor ? 1 : 0} after=${handle.lastTranscriptId ?? "-"} frames=${decoded.frames.length} tools=${tools} kinds=${JSON.stringify(kinds)} cursor=${decoded.cursor ? 1 : 0}`,
			);
		}
		if (decoded.checkpoint) handle.noteRingCheckpoint(decoded.checkpoint);
		// A checkpoint-only poll (turn start) wants the mark and the cursor, not
		// the frames: they predate the turn by construction and would only be
		// buffered and discarded.
		await handle.receiveBatch(options?.checkpointOnly ? [] : decoded.frames, decoded.cursor);
		// `terminal` here is the checkpoint's idle bit at the moment the CLI took
		// it. It is NOT evidence the turn ended: a side poll ~500 ms after send
		// runs before the persona has begun, reads idle, and marking the handle
		// idle on that stopped the side-poll loop for the entire turn (every
		// tool row went unobserved until the relay's finalized frame). Whether a
		// turn is running is the actor's call (setTurnRunning) plus the relay's
		// own idle frame (receive → frame.idle); a poll only reports.
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
	readonly #input: TailAttachInput;
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

	/**
	 * The cursor the NEXT poll must send. A checkpoint cursor is one-shot: the
	 * host consumes it on exchange and mints a replacement, so once a poll has
	 * returned a newer cursor the committed one is already dead. Sending it
	 * anyway (which happened whenever a poll landed before markAccepted, so its
	 * cursor stayed pending) got `invalid_cursor`, dropped the cursor, and the
	 * turn's frames were re-read cursorless. Commit semantics ("what a delivery
	 * failure can recover from") are unchanged; this is only what the poll sends.
	 */
	#lastSentCursor: string | undefined;
	get pollCursor(): string | undefined {
		this.#lastSentCursor = this.#pendingCursor ?? this.#cursor;
		return this.#lastSentCursor;
	}

	get turnRunning(): boolean {
		return this.#running;
	}

	/** Last transcript row id seen through this handle; the boundary a resumed poll pages after. */
	#lastTranscriptId: string | undefined;
	get lastTranscriptId(): string | undefined {
		return this.#lastTranscriptId;
	}

	noteDiagnostic(line: string): void {
		this.#input.onDiagnostic?.(line);
	}

	/** Forget the checkpoint: the next poll is cursorless. Keeps the transcript boundary. */
	dropCursor(): void {
		const pending = this.#pendingCursor?.slice(-12) ?? "-";
		const committed = this.#cursor?.slice(-12) ?? "-";
		this.#cursor = undefined;
		this.#pendingCursor = undefined;
		this.#input.onDiagnostic?.(
			`tail_cursor_invalid session=${this.sessionId} sent=${this.#lastSentCursor?.slice(-12) ?? "-"} pending=${pending} committed=${committed}`,
		);
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
		// Read the ring's end NOW, not as of the last poll: between turns no poll
		// runs, and the previous answer may have arrived on the relay (which
		// strips positions), so #ringHigh can sit below the previous turn's
		// finalized frame. One bounded poll; a failure leaves the last mark.
		if (!this.#polling) {
			this.#polling = true;
			try {
				await this.#runner.poll(this, { checkpointOnly: true });
			} catch {
				// Best-effort: the mark from the last successful poll stands.
			} finally {
				this.#polling = false;
			}
		}
		this.#preReceipt.splice(0);
		// Everything the ring holds right now predates the prompt about to be sent.
		this.#turnFloor = this.#ringHigh;
		this.#acceptedOpRef = opRef;
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
		if (cursor.length > 0) {
			this.#pendingCursor = cursor;
		}
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
		// The boundary advances with the batch, not with its delivery: a row that
		// failed to deliver is retried from the ledger, never re-fetched by paging.
		for (let index = frames.length - 1; index >= 0; index--) {
			const frame = frames[index];
			if (frame?.kind === "transcript" && frame.eventId && !/^\d+:\d+$/.test(frame.eventId)) {
				this.#lastTranscriptId = frame.eventId;
				break;
			}
		}
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
		this.#noteRingPosition(frame);
		if (frame.idle) this.setIdle();
		if (this.#acceptedOpRef !== undefined && frame.rawKind !== "transcript" && this.#predatesTurnFloor(frame)) {
			if (frame.assistantText)
				this.#input.onDiagnostic?.(
					`tail_frame_pre_floor session=${this.sessionId} kind=${frame.rawKind} at=${frame.generation}:${frame.seq} floor=${this.#turnFloor?.generation}:${this.#turnFloor?.seq}`,
				);
			return;
		}
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

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
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

	/**
	 * Highest event-ring position (generation, seq) observed through this handle,
	 * and the position at the moment the current turn began. A ring event at or
	 * below the turn floor was emitted BEFORE this turn's prompt was sent: it
	 * belongs to an earlier turn, whatever a resumed poll replays. Transcript
	 * rows are fenced by their host timestamp in the actor; ring events (the
	 * finalized answer, lifecycle) carry no timestamp, only a position, and a
	 * resume that replays the ring re-delivered eight earlier turns' finalized
	 * answers into one (live, 2026-09-18). Live: 15 messages for 4 steers, 14 of
	 * them earlier turns' answers.
	 */
	#ringHigh: { generation: number; seq: number } | undefined;
	#turnFloor: { generation: number; seq: number } | undefined;

	/**
	 * The host's checkpoint is the ring's end at poll time and is the
	 * authoritative high-water mark. Frames alone are not enough: the relay
	 * strips (generation, seq) from the frames it forwards, so a finalized
	 * answer that arrived on the relay never advanced the mark, the next turn's
	 * floor sat below it, and a resumed poll re-delivered it as this turn's
	 * interim (live, 2026-09-18: the previous answer posted twice, 66 s apart).
	 */
	noteRingCheckpoint(position: { generation: number; seq: number }): void {
		const high = this.#ringHigh;
		if (
			!high ||
			position.generation > high.generation ||
			(position.generation === high.generation && position.seq > high.seq)
		)
			this.#ringHigh = { generation: position.generation, seq: position.seq };
	}

	#noteRingPosition(frame: TailFrame): void {
		if (frame.generation === undefined || frame.seq === undefined) return;
		const high = this.#ringHigh;
		if (!high || frame.generation > high.generation || (frame.generation === high.generation && frame.seq > high.seq))
			this.#ringHigh = { generation: frame.generation, seq: frame.seq };
	}

	#predatesTurnFloor(frame: TailFrame): boolean {
		const floor = this.#turnFloor;
		if (!floor || frame.generation === undefined || frame.seq === undefined) return false;
		return frame.generation < floor.generation || (frame.generation === floor.generation && frame.seq <= floor.seq);
	}

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
	#reopenFailures = 0;

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
					await this.close();
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
					await this.close();
					return;
				}
				this.#input.onDiagnostic?.(
					`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				this.#polling = false;
			}
			if (this.#closed || this.#pausedForGap) return;
			const stream = spawner(this.sessionId);
			this.#stream = stream;
			const openedAt = this.#runner.now();
			// gjc >= 0.16.7's relay emits only lifecycle frames and the single
			// `turn_stream/finalized` answer; every transcript row (mid-work
			// assistant text, tool calls, tool results) is reachable only through
			// `tail --all-events`. While a turn is running, poll alongside the
			// stream: the relay keeps the answer immediate, the poll keeps the turn
			// observable. Both paths are fenced by the accepted-op-ref filter and
			// deterministic delivery ids, which is exactly how the gateway ran when
			// the relay was dying every second (2026-09-17: relay alive → interim
			// speech dropped to zero because the loop parked in the stream).
			const live = { open: true };
			const sidePoll = this.#pollWhileStreaming(live);
			try {
				for await (const line of stream.lines) {
					if (this.#closed) break;
					const frames = decodeStreamLine(line);
					for (const frame of frames) await this.receive(frame);
				}
			} catch (error) {
				this.#input.onDiagnostic?.(
					`tail_stream_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				live.open = false;
				this.#stream = undefined;
				stream.close();
				await sidePoll;
			}
			if (this.#closed || this.#pausedForGap) return;
			// A relay that ends immediately (endpoint_stale: the host died) must not
			// spin. Back off exponentially; after the cap, surface a retention gap
			// so the actor reconciles via status and rebinds instead of waiting on
			// a dead endpoint forever.
			const sinceOpen = this.#runner.now() - openedAt;
			this.#reopenFailures = sinceOpen < 5_000 ? this.#reopenFailures + 1 : 0;
			if (this.#reopenFailures >= STREAM_REOPEN_GIVE_UP) {
				this.#input.onDiagnostic?.(`tail_stream_dead session=${this.sessionId} reopens=${this.#reopenFailures}`);
				await this.retentionGap({ resync: { revision: 0, generation: 0, seq: 0 } });
				return;
			}
			const backoff = Math.min(30_000, this.#runner.pollIntervalMs * 2 ** this.#reopenFailures);
			this.#input.onDiagnostic?.(`tail_stream_reopen session=${this.sessionId} backoffMs=${backoff}`);
			await this.#runner.sleep(backoff);
		}
	}

	/**
	 * Polls at the runner interval for as long as the stream is open AND a turn
	 * is running. Idle sessions are not polled: the relay's lifecycle frames are
	 * enough to notice the next turn start, and `setTurnRunning(true)` from the
	 * actor wakes this loop. A poll failure is diagnostic, never fatal - the
	 * stream is still the authority for liveness, and a strict poll that reports
	 * a retention gap pauses the handle through the normal path.
	 */
	async #pollWhileStreaming(live: { open: boolean }): Promise<void> {
		while (live.open && !this.#closed && !this.#pausedForGap) {
			await this.#runner.sleep(this.#runner.pollIntervalMs);
			if (!live.open || this.#closed || this.#pausedForGap || !this.#running || this.#polling) continue;
			this.#polling = true;
			try {
				await this.#runner.poll(this);
			} catch (error) {
				this.#input.onDiagnostic?.(
					`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`,
				);
			} finally {
				this.#polling = false;
			}
		}
	}
}

type DecodedTail = {
	readonly frames: readonly TailFrame[];
	readonly cursor?: string;
	/** The event-ring position the host reported as current when this poll was taken. */
	readonly checkpoint?: { readonly generation: number; readonly seq: number };
	/** The host rejected our checkpoint cursor; the next poll must be cursorless. */
	readonly invalidCursor?: true;
	readonly terminal: boolean;
	readonly gap?: { readonly cursor?: string; readonly resync?: unknown };
	readonly error?: Error;
};

function decodeTailResult(result: CliResult): DecodedTail {
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
		if (errorCode === "retention_gap") {
			return { frames: [], terminal: false, gap: { resync: recordOf(error?.details)?.resync ?? error?.resync } };
		}
		// `--until-idle` bounded by `--timeout-ms` on a quiet session: nothing to
		// report this poll. Not an error, not terminal, not a gap.
		if (errorCode === "tail_timeout") return { frames: [], terminal: false };
		// snapshot_capacity_exceeded: the host's pin table is full (128), so it
		// cannot mint the checkpoint a resume needs. A cursorless tail still works
		// (it pages the checkpoint it is handed and does not need a spare pin), so
		// treat it like a lost cursor rather than a failed poll; pins expire on a
		// 15-minute TTL and resume comes back by itself.
		if (errorCode === "invalid_cursor" || errorCode === "cursor_expired" || errorCode === "snapshot_capacity_exceeded")
			return { frames: [], terminal: false, invalidCursor: true };
		// Keep the CLI's own message: `protocol_error` alone has meant three
		// different things (malformed session row, malformed control response,
		// list traversal) and the code never said which. Bounded and sanitised,
		// because it lands in the diagnostic log.
		const message =
			typeof error?.message === "string" && error.message !== errorCode
				? sanitizeDiagnostic(error.message).slice(0, 200)
				: "";
		return {
			frames: [],
			terminal: false,
			error: new Error(
				`session tail failed${errorCode ? `: ${errorCode}` : ` (exit ${result.exitCode})`}${message ? ` - ${message}` : ""}`,
			),
		};
	}
	const payload = recordOf(envelope.result) ?? {};
	const gap = recordOf(payload.gap);
	const items = Array.isArray(payload.items) ? payload.items : [];
	// A non-strict reply may carry BOTH a diagnostic gap (pre-checkpoint history
	// dropped) and the live frames after the resync point. The poll decides
	// whether the gap is a hold (strict, checkpointed) or a diagnostic.
	const checkpoint = recordOf(payload.checkpoint);
	const checkpointPosition =
		typeof checkpoint?.generation === "number" && typeof checkpoint?.seq === "number"
			? { generation: checkpoint.generation, seq: checkpoint.seq }
			: undefined;
	return {
		frames: items.flatMap(normalizeTailFrame),
		...(opaqueCursorOf(payload) ? { cursor: opaqueCursorOf(payload) } : {}),
		...(checkpointPosition ? { checkpoint: checkpointPosition } : {}),
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
	// gjc >= 0.16.7 records tool calls as `toolCall` content blocks on the
	// assistant transcript row and no longer emits `tool_activity` /
	// `tool_execution_start` frames (measured 2026-09-17: a read+answer turn
	// produced 4 transcript rows and zero tool frames). Project the block onto
	// the fields the counter and the activity hint already read, so a tool-using
	// turn is seen as one: without this `toolCallsSoFar` stayed 0 for every
	// turn, every mid-work message was suppressed as `pre-tool`, and presence
	// never left the queued marker.
	const toolCall =
		rawKind === "transcript" && payload.role === "assistant" ? firstToolCall(payload.content) : undefined;
	if (toolCall) {
		payload.toolCallStarted = true;
		if (typeof payload.toolName !== "string") payload.toolName = toolCall.name;
		if (payload.args === undefined) payload.args = toolCall.args;
	}
	const toolResult = rawKind === "transcript" && payload.role === "toolResult";
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
			// Surface the transcript-shaped tool lifecycle under the kinds the rest of
			// the gateway keys on, so nothing downstream learns about transcript blocks.
			rawKind: toolCall ? "tool_execution_start" : toolResult ? "tool_execution_end" : rawKind,
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

/** The first tool call block on an assistant transcript row, if any. */
function firstToolCall(content: unknown): { readonly name: string; readonly args: unknown } | undefined {
	if (!Array.isArray(content)) return undefined;
	for (const block of content) {
		const record = recordOf(block);
		if (!record) continue;
		const type = typeof record.type === "string" ? record.type : "";
		if (type !== "toolCall" && type !== "tool_call" && type !== "tool_use") continue;
		const name = typeof record.name === "string" && record.name.length > 0 ? record.name : "tool";
		return { name, args: record.arguments ?? record.input ?? record.args };
	}
	return undefined;
}

/**
 * The chat-visible text of a content array. Only `text` blocks are speech:
 * a `thinking` block also carries `.text`, and joining it in shipped the
 * model's private reasoning to the channel as if the persona had said it
 * (surfaced by the transcript tool-call projection, 2026-09-17).
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
