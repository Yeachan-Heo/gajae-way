import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
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

test("webhook startup failure is isolated; only affected webhook monitors are logged invalid", async () => {
	const { registry } = await open();
	// Create a webhook monitor (enabled).
	const webhook = registry.add({
		name: "webhook-monitor",
		trigger: { kind: "webhook", route: "webhook-route" },
		eventTypes: ["webhook.fire"],
	});
	// Create a cron monitor (enabled).
	const cron = registry.add({
		name: "cron-monitor",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: ["cron.fire"],
	});
	const fired: string[] = [];
	const propagator = {
		submitSlot: (monitorId: string) => {
			fired.push(monitorId);
			return crypto.randomUUID();
		},
		submit: () => crypto.randomUUID(),
	};
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());
	// Create runtime with no webhook config (missing configuration).
	const runtime = new MonitorRuntime({} as never, registry, propagator as never, {
		now: () => new Date(Date.parse(cron.createdAt) + 3 * 60_000),
	});
	// Start the runtime. The webhook startup should fail, but the cron monitor should still fire.
	await runtime.start();
	await runtime.stop();
	// The cron monitor should have fired (it doesn't depend on webhook config).
	expect(fired.length).toBeGreaterThan(0);
	expect(new Set(fired)).toEqual(new Set([cron.monitorId]));
	// The webhook monitor should be logged as invalid.
	const invalidLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes(webhook.monitorId));
	expect(invalidLogs.length).toBe(1);
	expect(invalidLogs[0]).toMatch(/^monitor_invalid id=[^ ]+ kind=webhook reason=webhook configuration is required/);
});

test("invalid stored monitors are reported without leaking placeholder records", async () => {
	const { database, registry } = await open();
	// A row that no longer loads at all (corrupt trigger JSON).
	database.transaction((tx) =>
		database.monitors.create(tx, {
			id: "corrupt",
			name: "corrupted",
			triggerJson: "{",
			eventTypesJson: JSON.stringify(["event.type"]),
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
	// A row that loads but carries a schedule saved before write-time validation.
	storeUnchecked(database, "legacy-six", SIX_FIELDS);
	const valid = registry.add({ name: "daily", trigger: { kind: "cron", schedule: "0 0 * * *" }, eventTypes: ["tick"] });

	// The unloadable row is never handed to list() consumers (runtime, schedule projection,
	// dispatch) as a fabricated record; the legacy row stays listable so it can be repaired.
	expect(
		registry
			.list()
			.map((monitor) => monitor.monitorId)
			.sort(),
	).toEqual(["legacy-six", valid.monitorId].sort());
	expect(registry.get("corrupt")).toBeUndefined();
	const invalid = registry.invalid();
	expect(invalid.map(({ monitorId, loadable }) => ({ monitorId, loadable }))).toEqual([
		{ monitorId: "corrupt", loadable: false },
		{ monitorId: "legacy-six", loadable: true },
	]);
	expect(invalid[0]?.reason).toContain("JSON");
	expect(invalid[1]?.reason).toContain("five fields");

	// Repairing the legacy row clears it from invalid() and keeps its other settings.
	registry.update({ monitorId: "legacy-six", schedule: "0 12 30 10 *" });
	expect(registry.get("legacy-six")?.trigger).toMatchObject({ schedule: "0 12 30 10 *", timezone: "Asia/Seoul" });
	expect(registry.invalid().map((record) => record.monitorId)).toEqual(["corrupt"]);

	// The unloadable row is removable, and an update cannot overwrite it with a placeholder.
	expect(registry.update({ monitorId: "corrupt", enabled: false })).toBeUndefined();
	expect(database.monitors.rows().find((row) => row.monitor_id === "corrupt")?.trigger_json).toBe("{");
	expect(registry.remove("corrupt")).toBe(true);
	expect(registry.invalid()).toEqual([]);
});

test("webhook monitor with unreadable credentialFile is skipped; valid webhook siblings serve", async () => {
	const { registry } = await open();
	// Create a temporary directory for credentials.
	const home = await mkdtemp(join(tmpdir(), "gajaeway-webhook-cred-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));

	// Write a valid credential file.
	const validCredFile = join(home, "valid-secret.txt");
	await writeFile(validCredFile, "valid-secret-value", "utf8");

	// Create webhook monitor with valid credential file.
	const validWebhook = registry.add({
		name: "valid-webhook",
		trigger: {
			kind: "webhook" as const,
			route: "valid-route",
			auth: { kind: "bearer" as const, credentialFile: validCredFile },
		},
		eventTypes: ["webhook.fire"],
	});

	// Create webhook monitor with non-existent credential file.
	const invalidCredFile = join(home, "nonexistent-secret.txt");
	const invalidWebhook = registry.add({
		name: "invalid-webhook",
		trigger: {
			kind: "webhook" as const,
			route: "invalid-route",
			auth: { kind: "bearer" as const, credentialFile: invalidCredFile },
		},
		eventTypes: ["webhook.fire"],
	});

	const propagator = {
		submit: () => crypto.randomUUID(),
	};
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());

	// Create runtime with webhook config.
	const runtime = new MonitorRuntime(
		{ webhook: { bind: "127.0.0.1", port: 0, exposeNonLoopback: false } } as never,
		registry,
		propagator as never,
	);

	// Start the runtime. Valid webhook should serve, invalid should be logged.
	await runtime.start();
	await runtime.stop();

	// The invalid webhook should be logged as monitor_invalid.
	const invalidLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes(invalidWebhook.monitorId));
	expect(invalidLogs.length).toBe(1);
	expect(invalidLogs[0]).toMatch(/^monitor_invalid id=[^ ]+ kind=webhook reason=ENOENT.*nonexistent-secret/);

	// The valid webhook should not be in the error logs.
	const validLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes(validWebhook.monitorId));
	expect(validLogs.length).toBe(0);
});

test("monitor.list includes invalid array with failed monitors", async () => {
	const { database, registry } = await open();
	// Store a cron monitor with invalid schedule.
	storeUnchecked(database, "legacy-six", SIX_FIELDS);
	// Store a webhook trigger with unreadable credential.
	const home = await mkdtemp(join(tmpdir(), "gajaeway-list-invalid-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const invalidCredFile = join(home, "nonexistent.txt");
	database.transaction((tx) =>
		database.monitors.create(tx, {
			id: "webhook-bad",
			name: "webhook-with-bad-cred",
			triggerJson: JSON.stringify({
				kind: "webhook",
				route: "bad-route",
				auth: { kind: "bearer", credentialFile: invalidCredFile },
			}),
			eventTypesJson: JSON.stringify(["webhook.fire"]),
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
	// Add a valid monitor.
	const valid = registry.add({ name: "daily", trigger: { kind: "cron", schedule: "0 0 * * *" }, eventTypes: ["tick"] });

	// Get the list of monitors and check invalid array.
	const list = registry.list();
	const invalid = registry.invalid();

	// The list should contain all monitors (valid, invalid cron, and invalid webhook).
	expect(list.map((m) => m.monitorId).sort()).toEqual(["legacy-six", "webhook-bad", valid.monitorId].sort());
	// Registry.invalid() only detects monitors that cannot be loaded/parsed, not runtime credential failures.
	// The cron with six fields is detected as invalid by the registry.
	expect(invalid.length).toBe(1);
	const invalidIds = invalid.map((m) => m.monitorId);
	expect(invalidIds).toEqual(["legacy-six"]);

	// The invalid monitor should have a reason.
	const legacyInvalid = invalid.find((m) => m.monitorId === "legacy-six");
	expect(legacyInvalid?.reason).toContain("five fields");

	// The webhook with bad credential is detected as invalid during runtime.start().
	// Create a runtime and verify the webhook is marked invalid.
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());
	const propagator = { submit: () => crypto.randomUUID() };
	const runtime = new MonitorRuntime(
		{ webhook: { bind: "127.0.0.1", port: 0, exposeNonLoopback: false } } as never,
		registry,
		propagator as never,
	);
	await runtime.start();
	await runtime.stop();

	// The webhook with bad credential should be logged as invalid.
	const invalidWebhookLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes("webhook-bad"));
	expect(invalidWebhookLogs.length).toBe(1);
	expect(invalidWebhookLogs[0]).toMatch(/^monitor_invalid id=webhook-bad kind=webhook reason=ENOENT/);
});

test("invalid cron monitor with oversized minute field is never started", async () => {
	const { database, registry } = await open();
	// Store a monitor with an invalid cron schedule (minute range 0-99 is not valid).
	storeUnchecked(database, "invalid-cron", "0-99 * * * *");

	// The invalid cron should be in the invalid list.
	const invalid = registry.invalid();
	const invalidIds = invalid.map((m) => m.monitorId);
	expect(invalidIds).toContain("invalid-cron");

	// The invalid cron should still be in list() since it's loadable (repairable).
	const listedMonitors = registry.list().map((m) => m.monitorId);
	expect(listedMonitors).toContain("invalid-cron");

	// At runtime, the invalid cron should be logged as invalid and not started.
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());

	const submitSlotCalls: string[] = [];
	const propagator = {
		submitSlot: (monitorId: string) => {
			submitSlotCalls.push(monitorId);
			return crypto.randomUUID();
		},
	};
	const runtime = new MonitorRuntime({} as never, registry, propagator as never);
	await runtime.start();

	// Check that the invalid cron was logged as invalid.
	const invalidCronLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes("invalid-cron"));
	expect(invalidCronLogs.length).toBe(1);
	// The log should contain the reason field.
	expect(invalidCronLogs[0]).toContain("reason=");
	// The invalid monitor should never call submitSlot.
	expect(submitSlotCalls).not.toContain("invalid-cron");
	await runtime.stop();
});

test("control characters in credential file errors are stripped before logging", async () => {
	const { registry } = await open();
	const home = await mkdtemp(join(tmpdir(), "gajaeway-webhook-control-chars-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));

	// Create a webhook monitor with a path containing a control character (ESC = 0x1b).
	const webhookWithEsc = registry.add({
		name: "webhook-with-esc",
		trigger: {
			kind: "webhook" as const,
			route: "webhook-route",
			auth: { kind: "bearer" as const, credentialFile: join(home, "secret\x1b.txt") },
		},
		eventTypes: ["event"],
	});

	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());

	const propagator = { submit: () => crypto.randomUUID() };
	const runtime = new MonitorRuntime(
		{ webhook: { bind: "127.0.0.1", port: 0, exposeNonLoopback: false } } as never,
		registry,
		propagator as never,
	);
	await runtime.start();

	// The error message should not contain the ESC character (0x1b).
	const logs = errors.mock.calls.map((call) => String(call[0]));
	const errorLog = logs.find((msg) => msg.includes("monitor_invalid") && msg.includes(webhookWithEsc.monitorId));
	expect(errorLog).toBeDefined();
	if (errorLog) {
		// Verify that the ESC character (0x1b) has been replaced with a space.
		expect(errorLog).not.toContain("\x1b");
	}
	await runtime.stop();
});

test("unloadable monitors (loadable:false) are warned on startup", async () => {
	const { database, registry } = await open();
	// Store a monitor with corrupt trigger JSON (cannot deserialize).
	database.transaction((tx) =>
		database.monitors.create(tx, {
			id: "corrupt-trigger",
			name: "corrupted",
			triggerJson: "{",
			eventTypesJson: JSON.stringify(["event.type"]),
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

	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => errors.mockRestore());

	const propagator = { submit: () => crypto.randomUUID() };
	const runtime = new MonitorRuntime({} as never, registry, propagator as never);
	await runtime.start();

	// The unloadable monitor should be warned on startup even though loadable:false.
	const unloadableLogs = errors.mock.calls
		.map((call) => String(call[0]))
		.filter((msg) => msg.includes("monitor_invalid") && msg.includes("corrupt-trigger"));
	expect(unloadableLogs.length).toBe(1);
	expect(unloadableLogs[0]).toContain("reason=");
	await runtime.stop();
});

test("restarts never overlap, coalesce while queued, and a refresh during a restart sees the later mutation", async () => {
	const { registry } = await open();
	const home = await mkdtemp(join(tmpdir(), "gajaeway-refresh-serial-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const credentialFile = join(home, "secret.txt");
	await writeFile(credentialFile, "s3cret");
	// The credential read makes each restart yield, so a second restart could interleave without serialization.
	registry.add({
		name: "webhook",
		trigger: { kind: "webhook", route: "serial-route", auth: { kind: "bearer" as const, credentialFile } },
		eventTypes: ["webhook.fire"],
	});
	const events: string[] = [];
	let active = 0;
	const list = registry.list.bind(registry);
	spyOn(registry, "list").mockImplementation(() => {
		events.push(`list:${list().length}`);
		return list();
	});
	const runtime = new MonitorRuntime(
		{ webhook: { bind: "127.0.0.1", port: 0, exposeNonLoopback: false } } as never,
		registry,
		{ submit: () => crypto.randomUUID() } as never,
	);
	cleanups.push(() => runtime.stop());
	const track = (restart: Promise<void>, label: string) => {
		active += 1;
		expect(active).toBeLessThanOrEqual(2);
		return restart.then(() => {
			active -= 1;
			events.push(label);
		});
	};

	// Three calls in one tick share one restart.
	const first = runtime.refresh();
	expect(runtime.refresh()).toBe(first);
	expect(runtime.start()).toBe(first);
	const firstDone = track(first, "done:first");
	await Promise.resolve();
	await Promise.resolve();

	// The first restart is in flight; a mutation plus refresh now must queue a second restart, not be dropped.
	registry.add({ name: "second", trigger: { kind: "webhook", route: "second-route" }, eventTypes: ["webhook.fire"] });
	const second = runtime.refresh();
	expect(second).not.toBe(first);
	await Promise.all([firstDone, track(second, "done:second")]);

	expect(events).toEqual(["list:1", "done:first", "list:2", "done:second"]);
});
