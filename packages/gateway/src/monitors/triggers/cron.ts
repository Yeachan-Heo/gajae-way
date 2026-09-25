export function cronMatches(schedule: string, date: Date): boolean {
	return compileCron(schedule)(date);
}

/** Largest value each field can take (minute, hour, day, month, weekday). */
const FIELD_MAX = [59, 23, 31, 12, 6] as const;

/**
 * Parses a five-field schedule once into per-field value sets; the returned
 * matcher runs in LOCAL time. Catch-up sweeps test every minute since the
 * durable cursor, so matching must not re-parse the schedule per minute.
 */
export function compileCron(schedule: string): (date: Date) => boolean {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) throw new Error("cron schedule must have five fields");
	const [minutes, hours, days, months, weekdays] = FIELD_MAX.map((max, index) => {
		const values = new Set<number>();
		for (let value = 0; value <= max; value++) if (cronFieldMatches(fields[index] ?? "", value, 0)) values.add(value);
		return values;
	}) as [Set<number>, Set<number>, Set<number>, Set<number>, Set<number>];
	return (date) =>
		minutes.has(date.getMinutes()) &&
		hours.has(date.getHours()) &&
		days.has(date.getDate()) &&
		months.has(date.getMonth() + 1) &&
		weekdays.has(date.getDay());
}
export function cronFieldMatches(field: string, value: number, min: number): boolean {
	return field.split(",").some((part) => {
		const [base, stepText] = part.split("/");
		const step = stepText ? Number(stepText) : 1;
		if (!Number.isInteger(step) || step < 1) return false;
		if (base === "*") return (value - min) % step === 0;
		const range = base.split("-").map(Number);
		if (range.length === 1) return value === range[0] && step === 1;
		return (
			range.length === 2 &&
			Number.isInteger(range[0]) &&
			Number.isInteger(range[1]) &&
			value >= range[0]! &&
			value <= range[1]! &&
			(value - range[0]!) % step === 0
		);
	});
}

/** Absolute minute epoch — identical for the same wall-clock minute worldwide. */
export function minuteEpoch(date: Date): number {
	return Math.floor(date.getTime() / 60_000);
}

/**
 * Ceiling on how much a single sweep may replay. Slots due since the monitor's
 * durable cursor are admitted when they are at most `maxAgeMs` old, newest
 * `maxSlots` first-come; everything else is reported as skipped, never dropped
 * silently.
 */
export interface CronCatchUpPolicy {
	readonly maxSlots: number;
	readonly maxAgeMs: number;
}

export const DEFAULT_CRON_CATCH_UP: CronCatchUpPolicy = { maxSlots: 24, maxAgeMs: 24 * 60 * 60 * 1000 };

/** Due slots a sweep refused under its policy; oldest/newest bound the gap. */
export interface CronSkip {
	readonly count: number;
	readonly oldest: Date;
	readonly newest: Date;
}

export interface CronCatchUpPlan {
	/** Slots to admit, oldest first. */
	readonly admit: Date[];
	readonly skipped?: CronSkip;
}

/**
 * Every schedule minute in the half-open window (after, now], split by the
 * policy. Slots older than `now - maxAgeMs` are skipped; of the rest, only the
 * newest `maxSlots` are admitted and the older overflow is skipped. Streaming,
 * so a long-abandoned monitor costs one pass and bounded memory.
 */
export function planCronCatchUp(
	matches: (date: Date) => boolean,
	after: Date,
	now: Date,
	policy: CronCatchUpPolicy,
): CronCatchUpPlan {
	const ageFloor = now.getTime() - policy.maxAgeMs;
	const admit: Date[] = [];
	let count = 0;
	let oldest: Date | undefined;
	let newest: Date | undefined;
	const skip = (slot: Date) => {
		count++;
		oldest ??= slot;
		newest = slot;
	};
	const cursor = new Date(after.getTime());
	cursor.setSeconds(0, 0);
	cursor.setMinutes(cursor.getMinutes() + 1);
	while (cursor.getTime() <= now.getTime()) {
		if (matches(cursor)) {
			const slot = new Date(cursor.getTime());
			if (slot.getTime() < ageFloor) skip(slot);
			else {
				admit.push(slot);
				const overflow = admit.length > policy.maxSlots ? admit.shift() : undefined;
				if (overflow) skip(overflow);
			}
		}
		cursor.setMinutes(cursor.getMinutes() + 1);
	}
	return oldest && newest ? { admit, skipped: { count, oldest, newest } } : { admit };
}

/**
 * Durable side of a cron trigger. The caller owns persistence: `cursor` is the
 * newest slot already admitted or skipped (or the monitor's creation instant),
 * `fire` claims one slot atomically and `skipped` records a policy refusal
 * durably. A sweep records the skip BEFORE admitting newer slots, so a crash
 * between the two never loses the diagnostic.
 */
export interface CronSink {
	cursor(): Date;
	fire(slotAt: Date): boolean;
	skipped(skip: CronSkip): void;
}

/**
 * Cron trigger driven by a durable per-monitor cursor (issue #157).
 *
 * Every sweep — the first one after a restart and each later tick — replays
 * all slots since the cursor, bounded by `policy`, instead of a fixed wall
 * clock lookback: downtime is bounded by dependency recovery, not by anything
 * the scheduler knows. `minute` is an absolute minute EPOCH so a suspended
 * process whose next tick lands on the same wall-minute still sweeps.
 */
export function startCron(
	schedule: string,
	sink: CronSink,
	options: { now?: () => Date; policy?: CronCatchUpPolicy; intervalMs?: number } = {},
): () => void {
	const now = options.now ?? (() => new Date());
	const policy = options.policy ?? DEFAULT_CRON_CATCH_UP;
	const matches = compileCron(schedule);
	let minute = -1;
	// In-process watermark: every minute up to here was already swept, so a
	// sparse schedule (cursor far behind, nothing due) is not rescanned from
	// the durable cursor on every tick. Only the first sweep scans the gap.
	let swept = -Infinity;
	const tick = () => {
		const date = now();
		const epoch = minuteEpoch(date);
		if (epoch === minute) return;
		minute = epoch;
		const after = new Date(Math.max(sink.cursor().getTime(), swept));
		const plan = planCronCatchUp(matches, after, date, policy);
		if (plan.skipped) sink.skipped(plan.skipped);
		for (const slot of plan.admit) sink.fire(slot);
		swept = date.getTime();
	};
	tick();
	const timer = setInterval(tick, options.intervalMs ?? 30_000);
	return () => clearInterval(timer);
}
