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
/** Execution bound for one sweep-scope CLI read; interactive calls keep their own bounds. */
export const SWEEP_READ_TIMEOUT_MS = 10_000;

interface SweepScope {
	readonly limiter: SweepLimiter;
	/** The persona sweep turn held by this unit, released around control operations. */
	readonly turn?: SweepTurn;
}

const scope = new AsyncLocalStorage<SweepScope>();

/** FIFO counting slot; a resuming holder is served before new ones. */
class Slot {
	#available: number;
	readonly #waiting: Array<() => void> = [];
	readonly #resuming: Array<() => void> = [];

	constructor(capacity: number) {
		this.#available = capacity;
	}

	tryAcquire(): boolean {
		if (this.#available <= 0) return false;
		this.#available--;
		return true;
	}

	acquire(resume = false): Promise<void> {
		if (this.#available > 0) {
			this.#available--;
			return Promise.resolve();
		}
		return new Promise<void>((resolve) => (resume ? this.#resuming : this.#waiting).push(resolve));
	}

	release(): void {
		const next = this.#resuming.shift() ?? this.#waiting.shift();
		if (next) next();
		else this.#available++;
	}
}

/**
 * The process-wide bound on sweep reads. Code run through {@link SweepLimiter.run}
 * is sweep scope: every SDK CLI call it makes (and that is not a control
 * operation) waits for the shared read slot, so the gateway never has more than
 * SWEEP_READ_CONCURRENCY sweep children alive whatever the number of origins,
 * lanes or monitors being swept. Only the leaf CLI call holds the slot, so a
 * sweep can never deadlock on it. Control operations (send, bind, resume,
 * relay attach, host termination) leave the sweep scope and run as ordinary
 * interactive work.
 */
export class SweepLimiter {
	readonly #reads = new Slot(SWEEP_READ_CONCURRENCY);

	/** Runs `work` in sweep scope. Nested calls join the enclosing scope. */
	run<T>(work: () => Promise<T>): Promise<T> {
		const current = scope.getStore();
		if (current?.limiter === this) return work();
		return scope.run({ limiter: this }, work);
	}

	/** @internal One sweep read under the shared slot. */
	async read<T>(work: () => Promise<T>): Promise<T> {
		await this.#reads.acquire();
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

	constructor(slot: Slot) {
		this.#slot = slot;
	}

	get held(): boolean {
		return this.#held && !this.#released;
	}

	release(): void {
		if (this.#released) return;
		this.#released = true;
		if (this.#held) {
			this.#held = false;
			this.#slot.release();
		}
	}

	/** Gives the turn to the next origin for the duration of `work`, then takes it back first in line. */
	async lend<T>(work: () => Promise<T>): Promise<T> {
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

	async #reclaim(): Promise<void> {
		if (this.#released || this.#held) return;
		this.#resuming ??= this.#slot.acquire(true).then(() => {
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
