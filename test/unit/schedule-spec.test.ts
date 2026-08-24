import { expect, test } from "bun:test";
import {
	assertTimezone,
	civilTimeAt,
	countMissedOccurrences,
	nextCronFire,
	nextEveryFire,
	nextFireAt,
	parseCron,
	ScheduleSpecError,
} from "../../src/main-session/schedule-spec";

const NEW_YORK = "America/New_York";

function instant(iso: string): number {
	return Date.parse(iso);
}

test("a cron expression must have 5 or 6 fields", () => {
	expect(() => parseCron("0 9 * *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 0 9 * * * *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9 * * *")).not.toThrow();
	expect(() => parseCron("30 0 9 * * *")).not.toThrow();
});

/**
 * D5: both-non-wildcard day fields are ambiguous. Vixie/croner OR them (silent
 * over-firing); ANDing them silently almost never fires. Both are traps, so the
 * expression is refused at parse time instead.
 */
test("an expression constraining both day-of-month and day-of-week is rejected", () => {
	expect(() => parseCron("0 9 15 * 1")).toThrow(ScheduleSpecError);
	// Either one alone is fine.
	expect(() => parseCron("0 9 15 * *")).not.toThrow();
	expect(() => parseCron("0 9 * * 1")).not.toThrow();
});

/**
 * A4-2: "non-wildcard" is pinned to the exact string `*`, so forms that happen
 * to match every day are still non-wildcard. Otherwise the rejection above
 * would fire inconsistently depending on how a user spelled "every day".
 */
test("only an exact asterisk counts as a wildcard day field", () => {
	expect(() => parseCron("0 9 */1 * 1")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9 1-31 * 1")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9 15 * 0-6")).toThrow(ScheduleSpecError);
});

test("out-of-range and malformed fields are rejected", () => {
	expect(() => parseCron("0 24 * * *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("60 * * * *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9 * 13 *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9 * * 7")).toThrow(ScheduleSpecError);
	expect(() => parseCron("x 9 * * *")).toThrow(ScheduleSpecError);
	expect(() => parseCron("0 9-5 * * *")).toThrow(ScheduleSpecError);
});

test("an unknown IANA timezone is rejected eagerly", () => {
	expect(() => assertTimezone("Mars/Olympus_Mons")).toThrow(ScheduleSpecError);
	expect(() => assertTimezone(NEW_YORK)).not.toThrow();
});

/**
 * Spring forward: 02:30 does not exist on 2026-03-08 in America/New_York. The
 * rule fires at the first existing civil time after the gap, which is 03:00 EDT
 * on that date - not 03:30, and not a skip to the next day.
 */
test("a daily job inside the spring-forward gap fires once at the first existing time", () => {
	const matchers = parseCron("30 2 * * *");
	// Start just after midnight local on the transition date.
	const after = instant("2026-03-08T05:00:00Z"); // 00:00 EST
	const fire = nextCronFire(matchers, after, NEW_YORK);

	expect(fire).toBeDefined();
	const civil = civilTimeAt(fire as number, NEW_YORK);
	expect({ month: civil.month, day: civil.day, hour: civil.hour, minute: civil.minute }).toEqual({
		month: 3,
		day: 8,
		hour: 3,
		minute: 0,
	});

	// And it does not fire a second time later that same civil day.
	const next = nextCronFire(matchers, fire as number, NEW_YORK);
	const nextCivil = civilTimeAt(next as number, NEW_YORK);
	expect(nextCivil.day).toBe(9);
});

/**
 * Fall back: 01:30 occurs twice on 2026-11-01 in America/New_York. Walking
 * instants means the earlier (EDT) occurrence matches first, so the job fires
 * once rather than twice.
 */
test("a daily job inside the fall-back repeat fires exactly once", () => {
	const matchers = parseCron("30 1 * * *");
	const after = instant("2026-11-01T04:00:00Z"); // 00:00 EDT
	const first = nextCronFire(matchers, after, NEW_YORK);

	expect(first).toBeDefined();
	expect(new Date(first as number).toISOString()).toBe("2026-11-01T05:30:00.000Z");

	// The second 01:30 (06:30Z, EST) must not produce another run that day.
	const second = nextCronFire(matchers, first as number, NEW_YORK);
	const secondCivil = civilTimeAt(second as number, NEW_YORK);
	expect(secondCivil.day).toBe(2);
});

test("an interval anchors to its epoch so it cannot drift per fire", () => {
	const anchor = 1_000_000;
	const interval = 60_000;

	// A fire observed late must still land on the original lattice.
	expect(nextEveryFire(anchor, interval, anchor + interval + 5_000)).toBe(anchor + 2 * interval);
	expect(nextEveryFire(anchor, interval, anchor)).toBe(anchor + interval);
	expect(nextEveryFire(anchor, interval, anchor - 1)).toBe(anchor);
});

test("a one-shot instant in the past yields no next fire", () => {
	const spec = { kind: "at", spec: String(5_000), timezone: "UTC" } as const;
	expect(nextFireAt(spec, 4_999)).toBe(5_000);
	expect(nextFireAt(spec, 5_000)).toBeUndefined();
	expect(nextFireAt(spec, 6_000)).toBeUndefined();
});

/**
 * The collapse count is what keeps a long outage from replaying N runs: the
 * scheduler writes one row carrying this number instead.
 */
test("missed occurrences over an outage are counted, not replayed", () => {
	const spec = { kind: "every", spec: String(60_000), timezone: "UTC", anchorMs: 0 } as const;

	// Ten whole minutes elapsed while the daemon was down.
	expect(countMissedOccurrences(spec, 0, 600_000)).toBe(10);
	expect(countMissedOccurrences(spec, 0, 59_999)).toBe(0);

	const oneShot = { kind: "at", spec: String(1_000), timezone: "UTC" } as const;
	expect(countMissedOccurrences(oneShot, 1_000, 5_000)).toBe(1);
});
