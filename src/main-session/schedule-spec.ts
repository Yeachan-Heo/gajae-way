/**
 * Schedule spec parsing and next-occurrence arithmetic.
 *
 * The durable store holds one absolute `next_fire_at_ms` per job, so all
 * calendar reasoning lives here and is never duplicated in Rust.
 *
 * Two deliberate divergences from common cron implementations, both chosen to
 * avoid a silent failure mode rather than to be surprising:
 *
 * 1. When day-of-month and day-of-week are both non-wildcard, Vixie cron and
 *    croner match when *either* field matches, which silently fires a job
 *    roughly 5-6 times a month instead of 0-1. Matching *both* instead would
 *    silently almost never fire, and a scheduled job that goes quiet looks
 *    healthy. Both are traps, so such an expression is rejected outright.
 * 2. "Non-wildcard" means anything other than the exact string `*`. `* / n` and
 *    a full-range list such as `1-31` are therefore non-wildcard even though
 *    they happen to match every day; otherwise the rejection rule above would
 *    itself be surprising.
 */

export class ScheduleSpecError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ScheduleSpecError";
	}
}

export type ScheduleKind = "at" | "every" | "cron";

/** A parsed cron field: `null` means the literal wildcard `*`. */
type FieldMatcher = ReadonlySet<number> | null;

interface CronMatchers {
	readonly second: FieldMatcher;
	readonly minute: FieldMatcher;
	readonly hour: FieldMatcher;
	readonly dayOfMonth: FieldMatcher;
	readonly month: FieldMatcher;
	readonly dayOfWeek: FieldMatcher;
}

const FIELD_BOUNDS: Record<
	"second" | "minute" | "hour" | "dayOfMonth" | "month" | "dayOfWeek",
	readonly [number, number]
> = {
	second: [0, 59],
	minute: [0, 59],
	hour: [0, 23],
	dayOfMonth: [1, 31],
	month: [1, 12],
	dayOfWeek: [0, 6],
};

const MAX_SEARCH_DAYS = 400;

function parseField(raw: string, field: keyof typeof FIELD_BOUNDS): FieldMatcher {
	const text = raw.trim();
	if (!text) throw new ScheduleSpecError(`cron field ${field} is empty`);
	if (text === "*") return null;
	const [low, high] = FIELD_BOUNDS[field];
	const allowed = new Set<number>();
	for (const part of text.split(",")) {
		const [rangePart, stepPart] = part.split("/");
		if (stepPart !== undefined && !/^\d+$/.test(stepPart)) {
			throw new ScheduleSpecError(`cron field ${field} has an invalid step in ${part}`);
		}
		const step = stepPart === undefined ? 1 : Number(stepPart);
		if (step < 1) throw new ScheduleSpecError(`cron field ${field} has a zero step in ${part}`);
		let start = low;
		let end = high;
		if (rangePart !== "*" && rangePart !== "") {
			const bounds = rangePart.split("-");
			if (bounds.length > 2) throw new ScheduleSpecError(`cron field ${field} has an invalid range in ${part}`);
			if (!bounds.every((bound) => /^\d+$/.test(bound))) {
				throw new ScheduleSpecError(`cron field ${field} has a non-numeric value in ${part}`);
			}
			start = Number(bounds[0]);
			end = bounds.length === 2 ? Number(bounds[1]) : start;
		}
		if (start < low || end > high || start > end) {
			throw new ScheduleSpecError(`cron field ${field} value ${part} is out of range ${low}-${high}`);
		}
		for (let value = start; value <= end; value += step) allowed.add(value);
	}
	if (allowed.size === 0) throw new ScheduleSpecError(`cron field ${field} matches nothing`);
	return allowed;
}

/**
 * Parses a 5- or 6-field cron expression.
 *
 * 6 fields are `sec min hour dom month dow`; 5 fields are
 * `min hour dom month dow` with an implied second of 0.
 */
export function parseCron(expression: string): CronMatchers {
	const fields = expression.trim().split(/\s+/);
	if (fields.length !== 5 && fields.length !== 6) {
		throw new ScheduleSpecError("a cron expression must have 5 or 6 fields");
	}
	const [secondRaw, minuteRaw, hourRaw, dayOfMonthRaw, monthRaw, dayOfWeekRaw] =
		fields.length === 6 ? fields : ["0", ...fields];

	// D5 / A4-2: reject the ambiguous both-non-wildcard case, where
	// "non-wildcard" is pinned to an exact `*` so `*/n` and `1-31` also reject.
	if (dayOfMonthRaw !== "*" && dayOfWeekRaw !== "*") {
		throw new ScheduleSpecError(
			"a cron expression must leave day-of-month or day-of-week as the exact wildcard *; " +
				"an expression constraining both is ambiguous, so write two jobs or guard the extra condition in the payload",
		);
	}

	return {
		second: parseField(secondRaw as string, "second"),
		minute: parseField(minuteRaw as string, "minute"),
		hour: parseField(hourRaw as string, "hour"),
		dayOfMonth: parseField(dayOfMonthRaw as string, "dayOfMonth"),
		month: parseField(monthRaw as string, "month"),
		dayOfWeek: parseField(dayOfWeekRaw as string, "dayOfWeek"),
	};
}

interface CivilTime {
	readonly year: number;
	readonly month: number;
	readonly day: number;
	readonly hour: number;
	readonly minute: number;
	readonly second: number;
	readonly weekday: number;
}

const WEEKDAYS: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
const formatterCache = new Map<string, Intl.DateTimeFormat>();

function formatterFor(timeZone: string): Intl.DateTimeFormat {
	const cached = formatterCache.get(timeZone);
	if (cached) return cached;
	let formatter: Intl.DateTimeFormat;
	try {
		formatter = new Intl.DateTimeFormat("en-US", {
			timeZone,
			hour12: false,
			year: "numeric",
			month: "2-digit",
			day: "2-digit",
			hour: "2-digit",
			minute: "2-digit",
			second: "2-digit",
			weekday: "short",
		});
	} catch {
		throw new ScheduleSpecError(`unknown IANA timezone: ${timeZone}`);
	}
	formatterCache.set(timeZone, formatter);
	return formatter;
}

/** Validates an IANA timezone id eagerly so a bad zone fails at create time. */
export function assertTimezone(timeZone: string): void {
	formatterFor(timeZone);
}

export function civilTimeAt(instantMs: number, timeZone: string): CivilTime {
	const parts = formatterFor(timeZone).formatToParts(new Date(instantMs));
	const lookup: Record<string, string> = {};
	for (const part of parts) if (part.type !== "literal") lookup[part.type] = part.value;
	const hour = Number(lookup.hour);
	return {
		year: Number(lookup.year),
		month: Number(lookup.month),
		day: Number(lookup.day),
		// Intl emits 24 for midnight in some ICU versions.
		hour: hour === 24 ? 0 : hour,
		minute: Number(lookup.minute),
		second: Number(lookup.second),
		weekday: WEEKDAYS[lookup.weekday ?? ""] ?? 0,
	};
}

function matches(matcher: FieldMatcher, value: number): boolean {
	return matcher === null || matcher.has(value);
}

const AMBIGUITY_LOOKBACK_MINUTES = 180;

/**
 * True when an earlier instant already represented this same civil minute.
 *
 * A fall-back transition repeats a civil hour, so the same wall-clock time
 * occurs at two instants. Firing on both would run a daily job twice; the
 * schedule fires on the first occurrence only.
 */
function isRepeatedCivilMinute(instantMs: number, civil: CivilTime, timeZone: string): boolean {
	for (let back = 1; back <= AMBIGUITY_LOOKBACK_MINUTES; back += 1) {
		const earlier = civilTimeAt(instantMs - back * MINUTE_MS, timeZone);
		if (
			earlier.year === civil.year &&
			earlier.month === civil.month &&
			earlier.day === civil.day &&
			earlier.hour === civil.hour &&
			earlier.minute === civil.minute
		) {
			return true;
		}
	}
	return false;
}

function dayMatches(matchers: CronMatchers, civil: CivilTime): boolean {
	if (!matches(matchers.month, civil.month)) return false;
	// Exactly one of these is non-wildcard by construction (see parseCron).
	return matches(matchers.dayOfMonth, civil.day) && matches(matchers.dayOfWeek, civil.weekday);
}

function civilMinuteTargets(matchers: CronMatchers): number[] {
	const hours = matchers.hour === null ? range(0, 23) : [...matchers.hour].sort((a, b) => a - b);
	const minutes = matchers.minute === null ? range(0, 59) : [...matchers.minute].sort((a, b) => a - b);
	const targets: number[] = [];
	for (const hour of hours) for (const minute of minutes) targets.push(hour * 60 + minute);
	return targets.sort((a, b) => a - b);
}

function range(low: number, high: number): number[] {
	const values: number[] = [];
	for (let value = low; value <= high; value += 1) values.push(value);
	return values;
}

const MINUTE_MS = 60_000;

/**
 * Next cron occurrence strictly after `afterMs`, as an absolute instant.
 *
 * Walks *instants* rather than civil times, which makes both DST edges fall out
 * of the same rule: a civil time inside a spring-forward gap is never produced,
 * so the first instant that passes the target fires instead (02:30 becomes 03:00
 * on the US spring-forward date); and an ambiguous fall-back civil time is
 * produced twice, so the earlier offset matches first and the job fires once.
 */
export function nextCronFire(matchers: CronMatchers, afterMs: number, timeZone: string): number | undefined {
	const secondOffset = matchers.second === null ? 0 : Math.min(...matchers.second);
	const targets = civilMinuteTargets(matchers);
	if (targets.length === 0) return undefined;
	const targetSet = new Set(targets);

	// Align the scan to the next whole minute strictly after `afterMs`.
	let cursor = Math.floor(afterMs / MINUTE_MS) * MINUTE_MS + MINUTE_MS;
	const limit = afterMs + MAX_SEARCH_DAYS * 24 * 60 * MINUTE_MS;
	let previous = civilTimeAt(cursor - MINUTE_MS, timeZone);

	while (cursor <= limit) {
		const civil = civilTimeAt(cursor, timeZone);
		if (dayMatches(matchers, civil)) {
			const civilMinutes = civil.hour * 60 + civil.minute;
			if (targetSet.has(civilMinutes)) {
				const candidate = cursor + secondOffset * 1_000;
				if (candidate > afterMs && !isRepeatedCivilMinute(cursor, civil, timeZone)) return candidate;
			} else {
				// A target may have been skipped by a DST gap; fire at the first
				// instant that lands after it on the same civil day.
				const previousMinutes =
					previous.year === civil.year && previous.month === civil.month && previous.day === civil.day
						? previous.hour * 60 + previous.minute
						: -1;
				const skipped = targets.some((target) => target > previousMinutes && target < civilMinutes);
				if (skipped && cursor > afterMs) return cursor;
			}
		}
		previous = civil;
		cursor += MINUTE_MS;
	}
	return undefined;
}

/** Next fixed-interval occurrence, anchored to the original epoch so it cannot drift. */
export function nextEveryFire(anchorMs: number, intervalMs: number, afterMs: number): number {
	if (intervalMs < 1) throw new ScheduleSpecError("an interval must be positive");
	if (afterMs < anchorMs) return anchorMs;
	const elapsed = afterMs - anchorMs + 1;
	const periods = Math.ceil(elapsed / intervalMs);
	return anchorMs + periods * intervalMs;
}

export interface ScheduleSpec {
	readonly kind: ScheduleKind;
	readonly spec: string;
	readonly timezone: string;
	/** Anchor for `every`; the absolute instant for `at`. */
	readonly anchorMs?: number;
}

/**
 * Computes the next absolute fire instant, or `undefined` when a one-shot job
 * has already run.
 */
export function nextFireAt(spec: ScheduleSpec, afterMs: number): number | undefined {
	switch (spec.kind) {
		case "at": {
			const instant = Number(spec.spec);
			if (!Number.isSafeInteger(instant))
				throw new ScheduleSpecError("an at spec must be an epoch-millisecond instant");
			return instant > afterMs ? instant : undefined;
		}
		case "every": {
			const intervalMs = Number(spec.spec);
			if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
				throw new ScheduleSpecError("an every spec must be a positive interval in milliseconds");
			}
			return nextEveryFire(spec.anchorMs ?? afterMs, intervalMs, afterMs);
		}
		case "cron":
			return nextCronFire(parseCron(spec.spec), afterMs, spec.timezone);
	}
}

/**
 * Counts occurrences strictly between `fromMs` (exclusive) and `nowMs`
 * (inclusive), so an overdue window collapses into one run carrying the real
 * missed count rather than replaying N runs.
 */
export function countMissedOccurrences(spec: ScheduleSpec, fromMs: number, nowMs: number, cap = 10_000): number {
	if (spec.kind === "at") return fromMs <= nowMs ? 1 : 0;
	let missed = 0;
	let cursor = fromMs;
	while (missed < cap) {
		const next = nextFireAt({ ...spec, anchorMs: spec.anchorMs ?? fromMs }, cursor);
		if (next === undefined || next > nowMs) break;
		missed += 1;
		cursor = next;
	}
	return missed;
}
