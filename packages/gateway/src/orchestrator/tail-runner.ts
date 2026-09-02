import { createHash } from "node:crypto";
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
const UNKNOWN_KIND_DIAGNOSTIC_CAP = 20;
const STREAM_REOPEN_GIVE_UP = 6;

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
	 * without changing durable inbound state; callers retain their settled batch.
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
	async attachResync(input: TailAttachInput & { readonly cursor: string; readonly resync: TailResyncCoordinate; readonly operatorApproved: true }): Promise<TailHandle> {
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
		const idle = [...this.#handles].filter((handle) => handle.idleSince !== undefined && now - handle.idleSince >= this.#idleTtlMs);
		for (const handle of idle) await handle.close();
		return idle.length;
	}

	/** An authenticated control reply is the only affirmative compaction observation. */
	recordCompactionReceipt(input: { readonly sessionId: string; readonly originKey: string; readonly result: unknown }): void {
		const receipt = recordOf(input.result);
		const outcome = receipt?.started === true ? "started" : receipt?.skipped === true ? "skipped" : "received";
		this.#log(`compaction_event sessionId=${input.sessionId} originKey=${input.originKey} source=control_receipt result=${outcome}`);
	}

	async #acquireSlot(priority: "current" | "retired"): Promise<void> {
		await this.reapIdle();
		while (this.#handles.size >= this.#maxTailProcesses) {
			const evictable = [...this.#handles]
				.filter((handle) => !handle.running)
				.sort((left, right) => (left.idleSince ?? Number.POSITIVE_INFINITY) - (right.idleSince ?? Number.POSITIVE_INFINITY))[0];
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
		const decoded = decodeTailResult(result);
		if (decoded.gap && (handle.strict && handle.cursor)) {
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

	async markAccepted(opRef: string): Promise<void> {
		if (this.#closed || this.#accepted) return;
		this.#accepted = true;
		this.#acceptedOpRef = opRef;
		// The flush is deliberately NOT awaited by the caller: markAccepted is
		// invoked from inside the origin mailbox (settle/recovery), and onFrame
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
		this.#input.onDiagnostic?.(`tail_gap_nonstrict session=${this.sessionId} resync=${JSON.stringify(gap.resync ?? null)}`);
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

	async #deliver(frame: TailFrame): Promise<void> {
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
				this.#input.onDiagnostic?.(`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`);
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
				this.#input.onDiagnostic?.(`tail_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`);
			} finally {
				this.#polling = false;
			}
			if (this.#closed || this.#pausedForGap) return;
			const stream = spawner(this.sessionId);
			this.#stream = stream;
			const openedAt = this.#runner.now();
			try {
				for await (const line of stream.lines) {
					if (this.#closed) break;
					const frames = decodeStreamLine(line);
					for (const frame of frames) await this.receive(frame);
				}
			} catch (error) {
				this.#input.onDiagnostic?.(`tail_stream_error session=${this.sessionId} detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error"}`);
			} finally {
				this.#stream = undefined;
				stream.close();
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
}

type DecodedTail = {
	readonly frames: readonly TailFrame[];
	readonly cursor?: string;
	readonly terminal: boolean;
	readonly gap?: { readonly cursor?: string; readonly resync?: unknown };
	readonly error?: Error;
};

function decodeTailResult(result: CliResult): DecodedTail {
	let envelope: Record<string, unknown>;
	try {
		envelope = JSON.parse(result.stdout) as Record<string, unknown>;
	} catch {
		return { frames: [], terminal: false, error: new Error(`session tail did not print a JSON envelope (exit ${result.exitCode})`) };
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
		...(gap?.code === "retention_gap" ? { gap: { ...(opaqueCursorOf(payload) ? { cursor: opaqueCursorOf(payload) } : {}), resync: gap.resync } } : {}),
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
				payload: { role: "assistant", content: [{ type: "text", text: payload.text }], ...(payload.clientRef ? { clientRef: payload.clientRef } : {}) },
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
	const id = typeof item.id === "string" && item.id.length > 0 ? item.id : generation !== undefined && seq !== undefined ? `${generation}:${seq}` : undefined;
	const text = rawKind === "transcript" && payload.role === "assistant" ? contentText(payload.content) : undefined;
	const clientRef = typeof payload.clientRef === "string" ? payload.clientRef : typeof item.clientRef === "string" ? item.clientRef : undefined;
	const idle = rawKind === "activity" && [payload.state, payload.status, payload.activity].some((value) => value === "idle");
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

function tailOperationRef(frame: TailFrame): string | undefined {
	for (const value of [frame.payload.opRef, frame.payload.operationRef, frame.payload.clientRef])
		if (typeof value === "string" && value.length > 0) return value;
	return undefined;
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
	return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
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

/** A stable ledger/delivery identity only when the runtime supplied a stable tail event identity. */
export function deterministicTailDeliveryId(sessionId: string, eventId: string): string {
	return `gw-d-${createHash("sha256").update(`${sessionId}|${eventId}`).digest("hex").slice(0, 32)}`;
}
