import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MonitorRegistry } from "../src/monitors/registry";
import { MonitorRuntime } from "../src/monitors/runtime";
import { validateCronSchedule } from "../src/monitors/triggers/cron";
import { Store } from "../src/store";

// Issue #724: a reminder saved as "0 12 30 10 * 2026" (a year appended, six fields) made every
// gateway start throw "cron schedule must have five fields" and crash-looped the pod.
const SIX_FIELDS = "0 12 30 10 * 2026";

const cleanups: (() => Promise<void> | void)[] = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
async function open(): Promise<{ database: Store; registry: MonitorRegistry }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cron-validation-"));
	const database = await Store.open(join(home, "gateway.db"));
	cleanups.push(
		() => rm(home, { recursive: true, force: true }),
		() => database.close(),
	);
	return { database, registry: new MonitorRegistry(database) };
}
/** A row as the old write path stored it: no schedule validation. */
function storeUnchecked(database: Store, id: string, schedule: string): void {
	database.transaction((tx) =>
		database.monitors.create(tx, {
			id,
			name: `legacy-${id}`,
			triggerJson: JSON.stringify({ kind: "cron", schedule, timezone: "Asia/Seoul" }),
			eventTypesJson: JSON.stringify(["reminder"]),
			burstPolicy: "coalesce",
			overlap: "queue",
			channelTargetJson: null,
			enabled: true,
			instruction: null,
			modelJson: null,
			serviceTier: null,
			procedureFilesJson: null,
		}),
	);
}

test("validateCronSchedule accepts five-field cron and explains each refusal", () => {
	for (const schedule of ["0 12 30 10 *", "*/15 9-18 * * 1-5", "0 * * * *", " 0  9 * * 0,6 ", "0 0 1-31/2 * *"])
		expect(() => validateCronSchedule(schedule, "Asia/Seoul")).not.toThrow();
	expect(() => validateCronSchedule(SIX_FIELDS, "Asia/Seoul")).toThrow(/five fields .* got 6.*"0 12 30 10 \*"/);
	expect(() => validateCronSchedule("0 12 * *", "UTC")).toThrow("got 4");
	expect(() => validateCronSchedule("", "UTC")).toThrow("got 0");
	expect(() => validateCronSchedule("61 * * * *", "UTC")).toThrow("minute field");
	expect(() => validateCronSchedule("0 0 * * 7", "UTC")).toThrow("day-of-week field");
	expect(() => validateCronSchedule("*/0 * * * *", "UTC")).toThrow("step must be at least 1");
	expect(() => validateCronSchedule("5/10 * * * *", "UTC")).toThrow("a step needs * or a range");
	expect(() => validateCronSchedule("0 9-5 * * *", "UTC")).toThrow("out of range");
	expect(() => validateCronSchedule("@daily * * * *", "UTC")).toThrow("is not *, N, N-M");
	expect(() => validateCronSchedule("0 0 31 2 *", "UTC")).toThrow("never fires");
});

test("a six-field schedule is refused on add and on update; nothing is stored", async () => {
	const { registry } = await open();
	expect(() =>
		registry.add({ name: "ping", trigger: { kind: "cron", schedule: SIX_FIELDS }, eventTypes: ["reminder"] }),
	).toThrow("five fields");
	expect(registry.list()).toEqual([]);
	const created = registry.add({
		name: "ping",
		trigger: { kind: "cron", schedule: "0 12 30 10 *" },
		eventTypes: ["reminder"],
	});
	expect(() => registry.update({ monitorId: created.monitorId, schedule: SIX_FIELDS })).toThrow("five fields");
	expect(() =>
		registry.update({ monitorId: created.monitorId, trigger: { kind: "cron", schedule: SIX_FIELDS } }),
	).toThrow("five fields");
	expect(registry.get(created.monitorId)?.trigger).toMatchObject({ kind: "cron", schedule: "0 12 30 10 *" });
});

test("a stored invalid schedule is skipped at start; the gateway's other monitors still run", async () => {
	const { database, registry } = await open();
	storeUnchecked(database, "legacy-six", SIX_FIELDS);
	const good = registry.add({ name: "hourly", trigger: { kind: "cron", schedule: "* * * * *" }, eventTypes: ["tick"] });
	const fired: string[] = [];
	const propagator = {
		submitSlot: (monitorId: string) => {
			fired.push(monitorId);
			return crypto.randomUUID();
		},
	};
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());
	const runtime = new MonitorRuntime({} as never, registry, propagator as never, {
		now: () => new Date(Date.parse(good.createdAt) + 3 * 60_000),
	});
	await runtime.start();
	await runtime.stop();
	expect(fired.length).toBeGreaterThan(0);
	expect(new Set(fired)).toEqual(new Set([good.monitorId]));
	expect(errors.mock.calls.map((call) => String(call[0]))).toContainEqual(
		expect.stringMatching(/^monitor_invalid id=legacy-six kind=cron reason=cron schedule must have five fields/),
	);
	// The bad row is untouched and can still be disabled (or fixed) without DB surgery.
	expect(registry.update({ monitorId: "legacy-six", enabled: false })).toMatchObject({
		enabled: false,
		trigger: { schedule: SIX_FIELDS },
	});
	expect(registry.update({ monitorId: "legacy-six", schedule: "0 12 30 10 *" })).toMatchObject({
		trigger: { schedule: "0 12 30 10 *" },
	});
});
