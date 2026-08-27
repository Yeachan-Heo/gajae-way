import { describe, expect, test } from "bun:test";
import { nextCronFire, parseCron } from "../src/cron";

/** Local time throughout: a cron the gateway scheduled fires on the host clock. */
function local(text: string): Date {
	return new Date(text);
}

describe("parseCron", () => {
	test("rejects anything that is not five fields", () => {
		expect(parseCron("30 8 * *")).toBeNull();
		expect(parseCron("30 8 * * 1-5 7")).toBeNull();
		expect(parseCron("")).toBeNull();
	});

	test("rejects out-of-range and inverted ranges rather than guessing", () => {
		expect(parseCron("60 8 * * *")).toBeNull();
		expect(parseCron("30 24 * * *")).toBeNull();
		expect(parseCron("30 8 0 * *")).toBeNull();
		expect(parseCron("30 8 * 13 *")).toBeNull();
		expect(parseCron("30 8-2 * * *")).toBeNull();
		expect(parseCron("30 8 * * */0")).toBeNull();
	});

	test("expands lists, ranges and steps", () => {
		const cron = parseCron("0,30 8-10 * * *");
		expect([...(cron?.minute ?? [])]).toEqual([0, 30]);
		expect([...(cron?.hour ?? [])]).toEqual([8, 9, 10]);
		expect([...(parseCron("*/15 * * * *")?.minute ?? [])]).toEqual([0, 15, 30, 45]);
	});

	test("accepts day and month names, and folds Sunday-as-7 onto 0", () => {
		expect([...(parseCron("0 9 * * mon-fri")?.dayOfWeek ?? [])]).toEqual([1, 2, 3, 4, 5]);
		expect([...(parseCron("0 9 * jan *")?.month ?? [])]).toEqual([1]);
		expect([...(parseCron("0 9 * * 7")?.dayOfWeek ?? [])]).toEqual([0]);
	});

	test("notices when both day fields are restricted", () => {
		expect(parseCron("0 9 1 * 1")?.dayUnion).toBe(true);
		expect(parseCron("0 9 1 * *")?.dayUnion).toBe(false);
		expect(parseCron("0 9 * * 1")?.dayUnion).toBe(false);
	});
});

describe("nextCronFire", () => {
	test("finds the next matching minute, strictly after the given instant", () => {
		// 2026-08-27 is a Thursday.
		const next = nextCronFire("30 8 * * 1-5", local("2026-08-27T08:00:00"));
		expect(next?.toISOString()).toBe(local("2026-08-27T08:30:00").toISOString());
	});

	test("never returns the instant it was given", () => {
		const at = local("2026-08-27T08:30:00");
		expect(nextCronFire("30 8 * * *", at)?.toISOString()).toBe(local("2026-08-28T08:30:00").toISOString());
	});

	test("rolls to the next weekday across a weekend", () => {
		// 2026-08-28 is a Friday, so the next weekday fire after it is Monday.
		const next = nextCronFire("30 8 * * 1-5", local("2026-08-28T09:00:00"));
		expect(next?.toISOString()).toBe(local("2026-08-31T08:30:00").toISOString());
	});

	test("rolls across a month and a year boundary", () => {
		expect(nextCronFire("0 0 1 * *", local("2026-08-27T12:00:00"))?.toISOString()).toBe(
			local("2026-09-01T00:00:00").toISOString(),
		);
		expect(nextCronFire("0 0 1 1 *", local("2026-08-27T12:00:00"))?.toISOString()).toBe(
			local("2027-01-01T00:00:00").toISOString(),
		);
	});

	test("treats a restricted day-of-month and day-of-week as a union, as cron does", () => {
		// The 1st, or any Monday, whichever comes first.
		expect(nextCronFire("0 0 1 * 1", local("2026-08-27T12:00:00"))?.toISOString()).toBe(
			local("2026-08-31T00:00:00").toISOString(),
		);
	});

	test("returns null for an unparseable schedule instead of a plausible lie", () => {
		expect(nextCronFire("not a cron", local("2026-08-27T12:00:00"))).toBeNull();
	});

	test("returns null rather than spinning on a schedule that can never fire", () => {
		expect(nextCronFire("0 0 30 2 *", local("2026-08-27T12:00:00"))).toBeNull();
	});
});
