export function cronMatches(schedule: string, date: Date): boolean {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) throw new Error("cron schedule must have five fields");
	return [date.getMinutes(), date.getHours(), date.getDate(), date.getMonth() + 1, date.getDay()].every(
		(value, index) => cronFieldMatches(fields[index]!, value, 0),
	);
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
 * Exact scheduled slot timestamps for every schedule minute in the half-open
 * window (from, now], oldest first, computed in LOCAL time — the same
 * wall-clock contract `cronMatches` and every existing cron monitor already
 * run on; no timezone conversion is introduced.
 *
 * `fire` returning false means the slot was NOT newly admitted (already
 * claimed durably by an earlier tick/process). Only NEW admissions count
 * against `budget`: duplicates from previously claimed slots never consume
 * catch-up capacity, so a genuinely missed later slot is still reached.
 */
export function cronSlotsBetween(
	schedule: string,
	from: Date,
	now: Date,
	budget: number,
	fire: (slotAt: Date) => boolean,
): number {
	let fired = 0;
	const cursor = new Date(from.getTime());
	cursor.setSeconds(0, 0);
	cursor.setMinutes(cursor.getMinutes() + 1);
	while (cursor.getTime() <= now.getTime()) {
		if (cronMatches(schedule, cursor)) {
			// Budget counts only newly admitted slots; duplicates don't consume it.
			if (fired >= budget) break;
			if (fire(new Date(cursor.getTime()))) fired++;
		}
		cursor.setMinutes(cursor.getMinutes() + 1);
	}
	return fired;
}

/** Every schedule slot in the half-open window (from, now], oldest first. */
export function cronSlotList(schedule: string, from: Date, now: Date): Date[] {
	const slots: Date[] = [];
	cronSlotsBetween(schedule, from, now, Number.POSITIVE_INFINITY, (slot) => {
		slots.push(slot);
		return true;
	});
	return slots;
}

/** Safety valve: a single in-process suspension sweep never authorizes more than this many slots. */
export const DEFAULT_MAX_CATCH_UP_SLOTS = 8;

/** How far back a running process re-scans for slots skipped by a suspended tick. */
export const CATCH_UP_WINDOW_MS = 60 * 60 * 1000;

/** Oldest missed slot a fresh process may still owe after downtime. */
export const DEFAULT_MAX_CATCH_UP_AGE_MS = 24 * 60 * 60 * 1000;

/** Marks the single coalesced event a fresh process fires for slots missed while it was down. */
export type CronCatchUp = {
	readonly cause: "startup";
	/** Oldest missed slot inside the age bound. */
	readonly missedFrom: string;
	/** Newest missed slot — the one fired. */
	readonly missedTo: string;
	readonly missedSlots: number;
};

/**
 * Cron trigger with an absolute-minute cursor (red-team blocker 5).
 *
 * - `minute` is an absolute minute EPOCH, never minute-of-hour: a suspended
 *   process whose next tick lands +60m later on the same wall-minute does not
 *   return early — the epoch advanced, so the due-window scan still runs.
 * - `fire(slotAt)` receives the EXACT scheduled slot timestamp (red-team
 *   blocker 3); the caller persists it as the event's scheduled identity.
 * - Dedupe/budget durability lives with the caller (the propagator claims the
 *   slot and admits the event in one transaction); this module only computes
 *   WHEN slots are due.
 * - Startup (issue #162): `since` is the persisted schedule boundary (the
 *   monitor's last claimed slot, else its creation instant). Every slot missed
 *   between it and now — bounded by `maxCatchUpAgeMs` — coalesces into ONE
 *   fire of the newest missed slot, marked with a `CronCatchUp` record. A slot
 *   due in the current minute fires normally. Replaying every historical
 *   slot would storm the authoring path after a long outage.
 * - While running, each tick scans from the last evaluated instant (bounded by
 *   `CATCH_UP_WINDOW_MS` and the slot budget), so a suspended tick still fires
 *   the slots it skipped without re-scanning slots already evaluated.
 */
export function startCron(
	schedule: string,
	fire: (slotAt: Date, catchUp?: CronCatchUp) => boolean,
	options: {
		since: Date;
		now?: () => Date;
		maxCatchUpSlots?: number;
		maxCatchUpAgeMs?: number;
		intervalMs?: number;
	},
): () => void {
	const now = options.now ?? (() => new Date());
	const budget = options.maxCatchUpSlots ?? DEFAULT_MAX_CATCH_UP_SLOTS;
	const maxAge = options.maxCatchUpAgeMs ?? DEFAULT_MAX_CATCH_UP_AGE_MS;
	let minute = -1;
	let started = 0;
	const tick = () => {
		const date = now();
		const epoch = minuteEpoch(date);
		if (epoch === minute) return;
		if (minute === -1) {
			const from = Math.max(options.since.getTime(), date.getTime() - maxAge);
			const due = cronSlotList(schedule, new Date(from), date);
			const last = due.at(-1);
			const current = last && minuteEpoch(last) === epoch ? due.pop() : undefined;
			const oldest = due[0];
			const newest = due.at(-1);
			if (oldest && newest)
				fire(newest, {
					cause: "startup",
					missedFrom: oldest.toISOString(),
					missedTo: newest.toISOString(),
					missedSlots: due.length,
				});
			if (current) fire(current);
			started = date.getTime();
		} else {
			// A suspension can skip due slots even when the current minute also
			// matches (e.g. */30, prior tick 06:00, resume 07:30 — 06:30 and 07:00
			// fire alongside 07:30); caller-side slot claims make re-scanned slots
			// no-ops. The scan never reaches behind startup, whose missed slots were
			// already coalesced. The extra minute keeps the boundary slot (now-60m
			// exactly) inside the half-open scan.
			const from = Math.max(started, date.getTime() - CATCH_UP_WINDOW_MS - 60_000);
			cronSlotsBetween(schedule, new Date(from), date, budget, fire);
		}
		minute = epoch;
	};
	tick();
	const timer = setInterval(tick, options.intervalMs ?? 30_000);
	return () => clearInterval(timer);
}
