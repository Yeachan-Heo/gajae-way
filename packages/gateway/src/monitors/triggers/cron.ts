import type { GatewayDatabase } from "../../store/db";

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

/** Safety valve: a single catch-up sweep never authorizes more than this many slots. */
export const DEFAULT_MAX_CATCH_UP_SLOTS = 8;

/** How far back a fresh process looks for missed slots. */
export const CATCH_UP_WINDOW_MS = 60 * 60 * 1000;

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
	 * WHEN slots are due and always scans the bounded window when ticks were
 *   skipped, keeping catch-up bounded by the caller-side claim + budget.
 */
export function startCron(
	schedule: string,
	fire: (slotAt: Date) => boolean,
	options: { now?: () => Date; maxCatchUpSlots?: number; intervalMs?: number } = {},
): () => void {
	const now = options.now ?? (() => new Date());
	const budget = options.maxCatchUpSlots ?? DEFAULT_MAX_CATCH_UP_SLOTS;
	let minute = -1;
	const tick = () => {
		const date = now();
		const epoch = minuteEpoch(date);
		if (epoch === minute) return;
		if (minute === -1) {
			// First tick of a fresh process: catch up bounded missed slots with
			// exact scheduled timestamps. Without this, a restart spanning a due
			// minute silently skipped that slot forever (issue #29 restart-window
			// loss). The scan covers the whole window regardless of whether the
			// current minute itself matches the schedule; caller-side slot claims
			// keep it exactly-once per slot.
			minute = epoch;
			cronSlotsBetween(schedule, new Date(date.getTime() - CATCH_UP_WINDOW_MS), date, budget, fire);
			return;
		}
		minute = epoch;
		if (cronMatches(schedule, date)) {
			fire(date);
			return;
		}
		// Not a due minute — but a long suspension may have skipped due slots
		// between ticks; scan the bounded window (caller dedupes already-fired
		// slots, so this only fires genuinely missed ones).
		cronSlotsBetween(schedule, new Date(date.getTime() - CATCH_UP_WINDOW_MS), date, budget, fire);
	};
	tick();
	const timer = setInterval(tick, options.intervalMs ?? 30_000);
	return () => clearInterval(timer);
}
