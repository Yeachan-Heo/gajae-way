import { describe, expect, test } from "bun:test";
import { parseCron } from "../src/cron";

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
