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

/**
 * Exact scheduled slot timestamps for every schedule minute in the half-open
 * window (from, now], oldest first, bounded by `budget`. Slots are computed in
 * LOCAL time — the same wall-clock contract `cronMatches` and every existing
 * cron monitor already run on; no timezone conversion is introduced.
 *
 * `fire` is only called for slots that survived dedupe (the caller claims each
 * slot in `monitor_slots` first), so a restart that spans N due minutes fires
 * exactly N events with the EXACT slot timestamp as payload `at` — never a
 * "recovery storm" replaying the whole gap, and never `now` masquerading as the
 * scheduled time.
 */
export function cronSlotsBetween(
	schedule: string,
	from: Date,
	now: Date,
	budget: number,
	fire: (slotAt: Date) => void,
): number {
	let fired = 0;
	const cursor = new Date(from.getTime());
	cursor.setSeconds(0, 0);
	cursor.setMinutes(cursor.getMinutes() + 1);
	while (cursor.getTime() <= now.getTime()) {
		if (cronMatches(schedule, cursor)) {
			if (fired >= budget) break;
			fire(new Date(cursor.getTime()));
			fired++;
		}
		cursor.setMinutes(cursor.getMinutes() + 1);
	}
	return fired;
}

export function startCron(
	schedule: string,
	fire: (slotAt: Date) => void,
	options: { now?: () => Date; database?: GatewayDatabase; monitorId?: string; maxCatchUpSlots?: number } = {},
): () => void {
	const now = options.now ?? (() => new Date());
	const database = options.database;
	const monitorId = options.monitorId;
	const budget = options.maxCatchUpSlots ?? DEFAULT_MAX_CATCH_UP_SLOTS;
	let minute = -1;
	// How far back a fresh process looks for missed slots. Slots older than this
	// are intentionally skipped: a bounded window prevents a long outage from
	// turning the first tick into an unbounded authoring storm.
	const catchUpWindowMs = 60 * 60 * 1000;
	const claim = (slotAt: Date): boolean => {
		if (!database || !monitorId) return true;
		return database.monitorSlotClaim(monitorId, slotAt.toISOString());
	};
	const tick = () => {
		const date = now();
		if (date.getMinutes() === minute) return;
		if (minute === -1) {
			// First tick of a fresh process: catch up bounded missed slots with
			// exact scheduled timestamps, deduped through the persisted slot
			// ledger. Without this, a restart spanning a due minute silently
			// skipped that slot forever (issue #29 restart-window loss). The
			// scan covers the whole window, so it works regardless of whether
			// the current minute itself matches the schedule.
			minute = date.getMinutes();
			const from = new Date(date.getTime() - catchUpWindowMs);
			cronSlotsBetween(schedule, from, date, budget, (slotAt) => {
				if (claim(slotAt)) fire(slotAt);
			});
			return;
		}
		if (cronMatches(schedule, date) && claim(date)) fire(date);
		minute = date.getMinutes();
	};
	tick();
	const timer = setInterval(tick, 30_000);
	return () => clearInterval(timer);
}

/** Safety valve: a single catch-up sweep never authorizes more than this many slots. */
export const DEFAULT_MAX_CATCH_UP_SLOTS = 8;
