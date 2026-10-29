import { CronExpressionParser } from "cron-parser";

const timezoneFormatters = new Map<string, Intl.DateTimeFormat>();
const CRON_FIELD_RANGES = [
	[0, 59],
	[0, 23],
	[1, 31],
	[1, 12],
	[0, 6],
] as const;

function dateFields(date: Date, timezone?: string): [number, number, number, number, number] {
	if (timezone === undefined)
		return [date.getMinutes(), date.getHours(), date.getDate(), date.getMonth() + 1, date.getDay()];
	let formatter = timezoneFormatters.get(timezone);
	if (!formatter) {
		formatter = new Intl.DateTimeFormat("en-US", {
			timeZone: timezone,
			weekday: "short",
			month: "numeric",
			day: "numeric",
			hour: "numeric",
			minute: "numeric",
			hourCycle: "h23",
		});
		timezoneFormatters.set(timezone, formatter);
	}
	const parts = new Map(formatter.formatToParts(date).map(({ type, value }) => [type, value]));
	const weekdays: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
	const minute = Number(parts.get("minute"));
	const hour = Number(parts.get("hour"));
	const day = Number(parts.get("day"));
	const month = Number(parts.get("month"));
	const weekday = weekdays[parts.get("weekday") ?? ""];
	if (![minute, hour, day, month].every(Number.isFinite) || weekday === undefined)
		throw new Error(`could not read cron time in timezone ${timezone}`);
	return [minute, hour, day, month, weekday];
}
const CRON_FIELD_NAMES = ["minute", "hour", "day-of-month", "month", "day-of-week"] as const;
const CRON_PART = /^(\*|\d+|\d+-\d+)(?:\/(\d+))?$/;

/**
 * Write-time check for a cron schedule, stricter than the matcher: exactly five fields, every
 * comma part `*`, `N` or `N-M` with an optional `/step` inside the field's range, and at least one
 * future fire. A schedule that passes can never stop the gateway from starting; the error says
 * what is wrong so the caller (often an agent) can retry with a corrected value.
 */
export function validateCronSchedule(schedule: string, timezone?: string): void {
	const fields = schedule.trim().split(/\s+/).filter(Boolean);
	if (fields.length !== 5)
		throw new Error(
			`cron schedule must have five fields (minute hour day-of-month month day-of-week), got ${fields.length}: ${JSON.stringify(schedule)}. A year is not a cron field; for a one-time reminder use the month and day, e.g. "0 12 30 10 *", and remove the monitor after it fires`,
		);
	fields.forEach((field, index) => {
		const [min, max] = CRON_FIELD_RANGES[index] as readonly [number, number];
		const name = CRON_FIELD_NAMES[index];
		for (const part of field.split(",")) {
			const match = part.match(CRON_PART);
			const invalid = (why: string) =>
				new Error(
					`cron ${name} field ${JSON.stringify(field)} is invalid: ${why} (allowed ${min}-${max}, *, N-M, */step)`,
				);
			if (!match) throw invalid(`${JSON.stringify(part)} is not *, N, N-M or one of them with /step`);
			const [base, stepText] = [match[1] as string, match[2]];
			if (stepText !== undefined && Number(stepText) < 1) throw invalid("step must be at least 1");
			if (base === "*") continue;
			if (stepText !== undefined && !base.includes("-"))
				throw invalid(`${JSON.stringify(part)}: a step needs * or a range`);
			const [start, end = start] = base.split("-").map(Number) as [number, number?];
			if (start < min || (end as number) > max || (end as number) < start)
				throw invalid(`${JSON.stringify(part)} is out of range`);
		}
	});
	if (nextCronFire(schedule, new Date(), timezone ?? new Intl.DateTimeFormat().resolvedOptions().timeZone) === null)
		throw new Error(
			`cron schedule ${JSON.stringify(schedule)} never fires (no date matches day-of-month, month and day-of-week together)`,
		);
}

/**
 * Matcher-compatibility check: can this schedule be compiled and will it ever match?
 * Looser than validateCronSchedule for write-time policy (allows '5/1' style steps),
 * but still rejects schedules that cannot compile or never fire. Used at runtime
 * to distinguish 'not startable' (genuinely cannot run) from 'stricter-policy rejected'
 * (rejected at write-time but the matcher can handle it, e.g., a monitor saved before
 * the write-time validation was added).
 */
export function isCronScheduleRunnable(schedule: string, timezone?: string): boolean {
	const fields = schedule.trim().split(/\s+/).filter(Boolean);
	if (fields.length !== 5) return false;
	try {
		// Try to compile it with the matcher
		compileCron(schedule, timezone);
		// Check if it will ever fire
		if (nextCronFire(schedule, new Date(), timezone ?? new Intl.DateTimeFormat().resolvedOptions().timeZone) === null)
			return false;
		return true;
	} catch {
		return false;
	}
}

export function cronMatches(schedule: string, date: Date, timezone?: string): boolean {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) throw new Error("cron schedule must have five fields");
	const values = dateFields(date, timezone);
	return fields.every((field, index) => {
		const value = values[index];
		return field !== undefined && value !== undefined && cronFieldMatches(field, value, 0);
	});
}
/** Compiles a five-field cron schedule into a reusable, timezone-aware matcher. */
export function compileCron(schedule: string, timezone?: string): (date: Date) => boolean {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) throw new Error("cron schedule must have five fields");
	const values = CRON_FIELD_RANGES.map(([min, max], index) => {
		const field = fields[index];
		if (field === undefined) throw new Error("cron schedule must have five fields");
		const matches = new Set<number>();
		for (let value = min; value <= max; value++) if (cronFieldMatches(field, value, 0)) matches.add(value);
		return matches;
	});
	return (date) => dateFields(date, timezone).every((value, index) => values[index]?.has(value) ?? false);
}
export function cronFieldMatches(field: string, value: number, min: number): boolean {
	return field.split(",").some((part) => {
		const [base, stepText] = part.split("/");
		const step = stepText ? Number(stepText) : 1;
		if (!Number.isInteger(step) || step < 1) return false;
		if (base === "*") return (value - min) % step === 0;
		const range = base.split("-").map(Number);
		if (range.length === 1) return range[0] !== undefined && value === range[0] && step === 1;
		const start = range[0];
		const end = range[1];
		return (
			range.length === 2 &&
			start !== undefined &&
			end !== undefined &&
			Number.isInteger(start) &&
			Number.isInteger(end) &&
			value >= start &&
			value <= end &&
			(value - start) % step === 0
		);
	});
}

export function nextCronFire(schedule: string, from: Date, timezone: string): Date | null {
	const fields = schedule.trim().split(/\s+/);
	if (fields.length !== 5) return null;
	const nearTerm: Date[] = [];
	cronSlotsBetween(
		schedule,
		from,
		new Date(from.getTime() + 4 * 60 * 60 * 1000),
		1,
		(slot) => {
			nearTerm.push(slot);
			return true;
		},
		timezone,
	);
	if (nearTerm[0]) return nearTerm[0];

	const values = fields.map((field, index) => {
		const range = CRON_FIELD_RANGES[index];
		if (!range) return null;
		const matching: number[] = [];
		for (let value = range[0]; value <= range[1]; value++) {
			if (cronFieldMatches(field, value, 0)) matching.push(value);
		}
		return matching.length ? matching : null;
	});
	const [minutes, hours, days, months, weekdays] = values;
	if (!minutes?.length || !hours?.length || !days?.length || !months?.length || !weekdays?.length) return null;
	let hasCalendarMatch = false;
	const firstYear = from.getUTCFullYear();
	for (let year = firstYear; year <= firstYear + 400 && !hasCalendarMatch; year++) {
		for (const month of months) {
			const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
			for (const day of days) {
				if (day <= daysInMonth && weekdays.includes(new Date(Date.UTC(year, month - 1, day)).getUTCDay())) {
					hasCalendarMatch = true;
					break;
				}
			}
			if (hasCalendarMatch) break;
		}
	}
	if (!hasCalendarMatch) return null;
	const candidateSchedule = [minutes, hours, days, months, weekdays].map((field) => field.join(",")).join(" ");
	const endDate = new Date(from.getTime() + 400 * 366 * 24 * 60 * 60 * 1000);
	try {
		const candidates = CronExpressionParser.parse(candidateSchedule, {
			currentDate: from,
			endDate,
			tz: timezone,
		});
		while (candidates.hasNext()) {
			const candidate = candidates.next().toDate();
			if (candidate.getTime() <= from.getTime()) continue;
			if (cronMatches(schedule, candidate, timezone)) return candidate;
		}
	} catch {
		return null;
	}
	return null;
}

/** Absolute minute epoch — identical for the same wall-clock minute worldwide. */
export function minuteEpoch(date: Date): number {
	return Math.floor(date.getTime() / 60_000);
}

/**
 * Exact scheduled slot timestamps for every schedule minute in the half-open
 * window (from, now], oldest first. Each cursor step is an absolute minute;
 * fields are evaluated in the explicit IANA zone or the process-local zone.
 * A spring-forward minute does not exist; a repeated fall-back minute has two
 * distinct UTC slots.
 *
 * `fire` returning false means the slot was NOT newly admitted (already
 * claimed durably by an earlier tick/process). Only NEW admissions count
 * against `budget`, so duplicates never consume catch-up capacity.
 */
export function cronSlotsBetween(
	schedule: string,
	from: Date,
	now: Date,
	budget: number,
	fire: (slotAt: Date) => boolean,
	timezone?: string,
): number {
	let fired = 0;
	const cursor = new Date((minuteEpoch(from) + 1) * 60_000);
	while (cursor.getTime() <= now.getTime()) {
		if (cronMatches(schedule, cursor, timezone)) {
			if (fired >= budget) break;
			if (fire(new Date(cursor.getTime()))) fired++;
		}
		cursor.setTime(cursor.getTime() + 60_000);
	}
	return fired;
}

/** Catch-up policy for slots accumulated since a monitor's durable cursor. */
export interface CronCatchUpPolicy {
	readonly maxSlots: number;
	readonly maxAgeMs: number;
}

export const DEFAULT_CRON_CATCH_UP: CronCatchUpPolicy = { maxSlots: 24, maxAgeMs: 24 * 60 * 60 * 1000 };

/** Due slots refused under the catch-up policy; oldest/newest bound the gap. */
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
 * Splits every scheduled minute in (after, now] by the policy. Slots older than
 * the age floor and the oldest overflow beyond maxSlots are reported as skipped.
 */
export function planCronCatchUp(
	matches: (date: Date) => boolean,
	after: Date,
	now: Date,
	policy: CronCatchUpPolicy,
): CronCatchUpPlan {
	const ageFloor = now.getTime() - policy.maxAgeMs;
	const admit: Date[] = [];
	let skippedCount = 0;
	let oldestSkipped: Date | undefined;
	let newestSkipped: Date | undefined;
	const skip = (slot: Date) => {
		skippedCount++;
		oldestSkipped ??= slot;
		newestSkipped = slot;
	};
	const cursor = new Date((minuteEpoch(after) + 1) * 60_000);
	while (cursor.getTime() <= now.getTime()) {
		if (matches(cursor)) {
			const slot = new Date(cursor.getTime());
			if (slot.getTime() < ageFloor) skip(slot);
			else {
				admit.push(slot);
				if (admit.length > policy.maxSlots) {
					const overflow = admit.shift();
					if (overflow) skip(overflow);
				}
			}
		}
		cursor.setTime(cursor.getTime() + 60_000);
	}
	return oldestSkipped && newestSkipped
		? { admit, skipped: { count: skippedCount, oldest: oldestSkipped, newest: newestSkipped } }
		: { admit };
}

/** Durable operations supplied by one monitor's runtime. */
export interface CronSink {
	cursor(): Date;
	fire(slotAt: Date): boolean;
	skipped(skip: CronSkip): void;
}

/**
 * Cron trigger driven by a durable per-monitor cursor. Every sweep replays all
 * slots since that cursor, bounded by policy rather than a fixed lookback.
 */
export function startCron(
	schedule: string,
	sink: CronSink,
	options: { now?: () => Date; policy?: CronCatchUpPolicy; intervalMs?: number; timezone?: string } = {},
): () => void {
	const now = options.now ?? (() => new Date());
	const policy = options.policy ?? DEFAULT_CRON_CATCH_UP;
	const matches = compileCron(schedule, options.timezone);
	let minute = -1;
	let swept = Number.NEGATIVE_INFINITY;
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
