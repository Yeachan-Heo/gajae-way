import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Sweep-scope SDK CLI reads allowed in flight process-wide. Periodic recovery,
 * reconciliation and observer sweeps are background work: before this bound a
 * gateway restart fanned one `gjc` child out per origin (16-18 concurrent
 * children measured), each a full runtime process.
 */
export const SWEEP_READ_CONCURRENCY = 1;
/**
 * How long a sweep driver waits for an origin's mailbox to start the origin's
 * sweep unit. A mailbox still busy after this is deferred to the next sweep:
 * the driver never waits behind a running turn.
 */
export const SWEEP_MAILBOX_WAIT_MS = 2_000;
/** Default bound for a sweep-scope CLI read that brings none; a caller's own budget is kept. */
export const SWEEP_READ_TIMEOUT_MS = 10_000;
/**
 * A persona sweep unit holding its turn this long is logged (`sweep_unit_slow`).
 * A unit is the origin's existing sequential recovery chain, so it is never cut
 * off mid-way (that would apply half of an authority decision); its length is
 * bounded by its reads, each a single sweep read with its own bound.
 */
export const SWEEP_UNIT_SLOW_MS = 10_000;

interface SweepScope {
	readonly limiter: SweepLimiter;
	/** The persona sweep turn held by this unit, released around control operations. */
	readonly turn?: SweepTurn;
}

const scope = new AsyncLocalStorage<SweepScope>();

/**
 * How many priority grants in a row may pass over a waiting ordinary sweep
 * read: enough for one persona unit's reads (inspect, status, inspect,
 * liveness, reconcile, observer) to finish without interleaving, so the
 * mailbox a user message waits on is released quickly, while lane, monitor
 * and work-observer reads still get every ninth grant.
 */
const PRIORITY_RUN = 8;

/**
 * FIFO counting slot. A resuming holder is served first; then priority
 * waiters, except that an ordinary waiter is served after PRIORITY_RUN
 * priority grants passed it over, so neither class can starve the other.
 */
class Slot {
	#available: number;
	readonly #waiting: Array<() => void> = [];
	readonly #priority: Array<() => void> = [];
	readonly #resuming: Array<() => void> = [];
	#passedOver = 0;

	constructor(capacity: number) {
		this.#available = capacity;
	}

	tryAcquire(): boolean {
		if (this.#available <= 0) return false;
		this.#available--;
		return true;
	}

	acquire(lane: "ordinary" | "priority" | "resume" = "ordinary"): Promise<void> {
		if (this.#available > 0) {
			this.#available--;
			return Promise.resolve();
		}
		const queue = lane === "resume" ? this.#resuming : lane === "priority" ? this.#priority : this.#waiting;
		return new Promise<void>((resolve) => queue.push(resolve));
	}

	release(): void {
		const next = this.#resuming.shift() ?? this.#nextWaiter();
		if (next) next();
		else this.#available++;
	}

	#nextWaiter(): (() => void) | undefined {
		if (this.#waiting.length === 0) {
			this.#passedOver = 0;
			return this.#priority.shift();
		}
		if (this.#priority.length > 0 && this.#passedOver < PRIORITY_RUN) {
			this.#passedOver++;
			return this.#priority.shift();
		}
		this.#passedOver = 0;
		return this.#waiting.shift();
	}
}

/**
 * The process-wide bound on sweep reads. Code run through {@link SweepLimiter.run}
 * is sweep scope: every SDK CLI call it makes (and that is not a control
 * operation) waits for the shared read slot, so the gateway never has more than
 * SWEEP_READ_CONCURRENCY sweep children alive whatever the number of origins,
 * lanes or monitors being swept. Other sweeps hold the slot only for the leaf
 * CLI call. A persona sweep unit occupies an origin mailbox a user message may
 * be waiting on, so it keeps the slot from its first read until it ends or
 * lends its turn, and its reads run one at a time without queueing behind other
 * sweeps; it never waits on anything else while holding the slot, so no sweep
 * can deadlock on it. Control operations (send, bind, resume, relay attach)
 * leave the sweep scope and run as ordinary interactive work.
 */
export class SweepLimiter {
	readonly #reads = new Slot(SWEEP_READ_CONCURRENCY);
	#unitReads = 0;

	/** Sweep CLI reads persona sweep units have made (for the per-pass log line). */
	get unitReads(): number {
		return this.#unitReads;
	}

	/** Runs `work` in sweep scope. Nested calls join the enclosing scope. */
	run<T>(work: () => Promise<T>): Promise<T> {
		const current = scope.getStore();
		if (current?.limiter === this) return work();
		return scope.run({ limiter: this }, work);
	}

	/** @internal One sweep read under the shared slot. */
	async read<T>(work: () => Promise<T>): Promise<T> {
		const turn = scope.getStore()?.turn;
		// A read a finished unit left behind (a fire-and-forget promise) is an ordinary sweep read.
		if (turn && !turn.finished)
			return await turn.serialRead(async () => {
				this.#unitReads++;
				if (!turn.readSlotHeld) {
					await this.#reads.acquire("priority");
					if (turn.finished) {
						try {
							return await work();
						} finally {
							this.#reads.release();
						}
					}
					turn.holdReadSlot(() => this.#reads.release());
				}
				return await work();
			});
		await this.#reads.acquire("ordinary");
		try {
			return await work();
		} finally {
			this.#reads.release();
		}
	}

	/** @internal Runs `work` as a persona sweep unit holding `turn`. */
	runTurn<T>(turn: SweepTurn, work: () => Promise<T>): Promise<T> {
		return scope.run({ limiter: this, turn }, work);
	}
}

/**
 * Serializes persona sweep units across origins: a driver hands the next origin
 * its unit only when the current unit finished or is inside a control
 * operation. The turn is acquired outside every mailbox; it schedules units,
 * while the read slot is what bounds children.
 */
export class SweepTurns {
	readonly #slot = new Slot(1);

	/** The turn when it is free right now; undefined when another unit holds it. */
	tryTake(): SweepTurn | undefined {
		return this.#slot.tryAcquire() ? new SweepTurn(this.#slot) : undefined;
	}

	async take(): Promise<SweepTurn> {
		await this.#slot.acquire();
		return new SweepTurn(this.#slot);
	}
}

export class SweepTurn {
	readonly #slot: Slot;
	#held = true;
	#released = false;
	#resuming: Promise<void> | undefined;
	/** Releases the sweep read slot this unit holds between its reads. */
	#releaseReadSlot: (() => void) | undefined;
	#reads: Promise<void> = Promise.resolve();

	constructor(slot: Slot) {
		this.#slot = slot;
	}

	get held(): boolean {
		return this.#held && !this.#released;
	}

	/** @internal True once the unit ended (or was abandoned). */
	get finished(): boolean {
		return this.#released;
	}

	/** @internal True while this unit holds the sweep read slot. */
	get readSlotHeld(): boolean {
		return this.#releaseReadSlot !== undefined;
	}

	/** @internal Keeps the read slot until the unit ends or lends its turn. */
	holdReadSlot(release: () => void): void {
		this.#releaseReadSlot = release;
	}

	/** @internal Runs the unit's reads one at a time, fire-and-forget ones included. */
	async serialRead<T>(work: () => Promise<T>): Promise<T> {
		const previous = this.#reads;
		let done!: () => void;
		this.#reads = new Promise<void>((resolve) => {
			done = resolve;
		});
		await previous;
		try {
			return await work();
		} finally {
			done();
		}
	}

	release(): void {
		this.#dropReadSlot();
		if (this.#released) return;
		this.#released = true;
		if (this.#held) {
			this.#held = false;
			this.#slot.release();
		}
	}

	/** Gives the turn to the next origin for the duration of `work`, then takes it back first in line. */
	async lend<T>(work: () => Promise<T>): Promise<T> {
		this.#dropReadSlot();
		if (this.held) {
			this.#held = false;
			this.#slot.release();
		}
		try {
			return await work();
		} finally {
			await this.#reclaim();
		}
	}

	#dropReadSlot(): void {
		const release = this.#releaseReadSlot;
		this.#releaseReadSlot = undefined;
		release?.();
	}

	async #reclaim(): Promise<void> {
		if (this.#released || this.#held) return;
		this.#resuming ??= this.#slot.acquire("resume").then(() => {
			this.#resuming = undefined;
			if (this.#released) this.#slot.release();
			else this.#held = true;
		});
		await this.#resuming;
	}
}

/** True inside sweep scope. */
export function inSweep(): boolean {
	return scope.getStore() !== undefined;
}

/** The limiter of the enclosing sweep scope, if any. */
export function currentSweep(): SweepLimiter | undefined {
	return scope.getStore()?.limiter;
}

/** Runs `work` outside any sweep scope (mailbox tasks and callbacks a unit merely schedules). */
export function outsideSweep<T>(work: () => T): T {
	return scope.exit(work);
}

/**
 * A long sweep step that is not a control operation (host termination waits
 * through its signal grace): it stays in sweep scope, so its CLI reads remain
 * sweep reads, but the persona sweep turn is lent to the next origin meanwhile.
 */
export async function sweepLend<T>(work: () => Promise<T>): Promise<T> {
	const turn = scope.getStore()?.turn;
	return turn ? await turn.lend(work) : await work();
}

/**
 * Runs `run` at most once at a time: a call while the previous run is still in
 * flight is skipped (logged), and a failed run clears the guard.
 */
export function singleFlight(subsystem: string, run: () => Promise<void>, log: (line: string) => void): () => void {
	let inFlight: Promise<void> | undefined;
	return () => {
		if (inFlight) {
			log(`reconcile_sweep_skipped subsystem=${subsystem} reason=in_flight`);
			return;
		}
		const current = run().finally(() => {
			inFlight = undefined;
		});
		current.catch(() => {});
		inFlight = current;
	};
}

/**
 * A control operation. Inside sweep scope it runs as ordinary interactive work
 * (its CLI calls do not take the sweep read slot) and lends the persona sweep
 * turn to the next origin while it runs.
 */
export async function sweepControl<T>(work: () => Promise<T>): Promise<T> {
	const current = scope.getStore();
	if (!current) return await work();
	const run = () => scope.exit(work);
	return current.turn ? await current.turn.lend(run) : await run();
}
