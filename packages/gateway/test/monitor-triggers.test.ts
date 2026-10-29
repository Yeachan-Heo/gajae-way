import { expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { chmod, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CATCH_ALL_EVENT_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { MonitorRegistry } from "../src/monitors/registry";
import { MonitorRuntime } from "../src/monitors/runtime";
import { cronMatches, cronSlotsBetween, nextCronFire, startCron } from "../src/monitors/triggers/cron";
import { startScript } from "../src/monitors/triggers/script";
import { startWatcher } from "../src/monitors/triggers/watcher";
import { startWebhook } from "../src/monitors/triggers/webhook";
import { Store } from "../src/store";

test("cron supports steps, lists, ranges, dow and injected clock", () => {
	const monday = new Date(2026, 0, 5, 10, 15);
	expect(cronMatches("*/5 10 1-10 1 1", monday)).toBe(true);
	expect(cronMatches("0,15,30 10 * * 1", monday)).toBe(true);
	let fired = 0;
	const stop = startCron(
		"15 10 * * 1",
		{
			cursor: () => new Date(2026, 0, 5, 10, 0),
			fire: () => {
				fired++;
				return true;
			},
			skipped: () => {},
		},
		{ now: () => monday },
	);
	stop();
	expect(fired).toBe(1);
});

test("cron matches and admits slots in the configured IANA timezone", () => {
	const slot = new Date("2026-08-27T12:00:00.000Z");
	const hostTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
	const localClock = new Intl.DateTimeFormat("en-US", {
		timeZone: hostTimezone,
		hour: "2-digit",
		minute: "2-digit",
		hourCycle: "h23",
	})
		.format(slot)
		.split(":")
		.map(Number);
	const timezone = ["Etc/UTC", "Asia/Seoul", "America/New_York"].find((candidate) => {
		const candidateClock = new Intl.DateTimeFormat("en-US", {
			timeZone: candidate,
			hour: "2-digit",
			minute: "2-digit",
			hourCycle: "h23",
		})
			.format(slot)
			.split(":")
			.map(Number);
		return candidateClock[0] !== localClock[0] || candidateClock[1] !== localClock[1];
	});
	expect(timezone).toBeDefined();
	const [hour, minute] = new Intl.DateTimeFormat("en-US", {
		timeZone: timezone,
		hour: "2-digit",
		minute: "2-digit",
		hourCycle: "h23",
	})
		.format(slot)
		.split(":")
		.map(Number);
	const schedule = `${minute} ${hour} * * *`;
	const admitted: Date[] = [];

	expect(cronMatches(schedule, slot, timezone)).toBe(true);
	expect(
		cronSlotsBetween(
			schedule,
			new Date(slot.getTime() - 60_000),
			slot,
			8,
			(scheduled) => {
				admitted.push(scheduled);
				return true;
			},
			timezone,
		),
	).toBe(1);
	expect(admitted.map((scheduled) => scheduled.toISOString())).toEqual([slot.toISOString()]);
});

test("Seoul cron start claims the exact UTC slot for its configured wall-clock time", () => {
	const slot = new Date("2026-08-27T12:00:00.000Z");
	expect(cronMatches("0 21 * * *", slot, "Asia/Seoul")).toBe(true);
	const admitted: string[] = [];
	const stop = startCron(
		"0 21 * * *",
		{
			cursor: () => new Date(slot.getTime() - 60_000),
			fire: (scheduled) => {
				admitted.push(scheduled.toISOString());
				return true;
			},
			skipped: () => {},
		},
		{ now: () => slot, timezone: "Asia/Seoul" },
	);
	stop();
	expect(admitted).toEqual([slot.toISOString()]);
});

test("cron evaluates repeated DST wall-clock slots as distinct UTC instants", () => {
	const from = new Date("2026-11-01T05:00:00.000Z");
	const now = new Date("2026-11-01T07:00:00.000Z");
	const admitted: string[] = [];
	const count = cronSlotsBetween(
		"30 1 * * *",
		from,
		now,
		8,
		(slot) => {
			admitted.push(slot.toISOString());
			return true;
		},
		"America/New_York",
	);
	expect(count).toBe(2);
	expect(admitted).toEqual(["2026-11-01T05:30:00.000Z", "2026-11-01T06:30:00.000Z"]);
});

test("next cron fire reports the configured wall-clock time as a UTC instant", () => {
	expect(nextCronFire("0 21 * * *", new Date("2026-08-27T11:59:00.000Z"), "Asia/Seoul")?.toISOString()).toBe(
		"2026-08-27T12:00:00.000Z",
	);
	expect(nextCronFire("30 1 * * *", new Date("2026-11-01T05:40:00.000Z"), "America/New_York")?.toISOString()).toBe(
		"2026-11-01T06:30:00.000Z",
	);
	expect(nextCronFire("0 0 29 2 *", new Date("2026-03-01T00:00:00.000Z"), "Etc/UTC")?.toISOString()).toBe(
		"2028-02-29T00:00:00.000Z",
	);
	expect(nextCronFire("0 0 30 2 *", new Date("2026-03-01T00:00:00.000Z"), "Etc/UTC")).toBeNull();
});

test("cron timezone validation rejects invalid zones and defaults to the gateway local zone", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-cron-timezone-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const registry = new MonitorRegistry(database);
		expect(() =>
			registry.add({
				name: "invalid-zone",
				trigger: { kind: "cron", schedule: "0 21 * * *", timezone: "Not/A_Real_Zone" },
				eventTypes: ["cron.invalid"],
			}),
		).toThrow("not a valid IANA timezone");
		expect(() =>
			registry.add({
				name: "empty-zone",
				trigger: { kind: "cron", schedule: "0 21 * * *", timezone: "" },
				eventTypes: ["cron.empty-zone"],
			}),
		).toThrow("non-empty IANA timezone");
		expect(registry.list()).toHaveLength(0);

		const monitor = registry.add({
			name: "local-zone",
			trigger: { kind: "cron", schedule: "0 21 * * *" },
			eventTypes: ["cron.local"],
		});
		const timezone = new Intl.DateTimeFormat().resolvedOptions().timeZone;
		expect(monitor.trigger).toEqual({ kind: "cron", schedule: "0 21 * * *", timezone });
		expect(registry.get(monitor.monitorId)?.trigger).toEqual(monitor.trigger);
	} finally {
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("webhook admits before 202, rejects unknown routes, replay and oversized bodies", async () => {
	const events: string[] = [];
	const server = startWebhook({
		port: 0,
		monitors: () => [
			{ monitorId: "m", route: "secret", eventType: "changed", auth: { kind: "hmac" as const, secret: "key" } },
		],
		submit: () => {
			events.push("admitted");
			return "event";
		},
	});
	try {
		const base = `http://127.0.0.1:${server.port}`;
		expect((await fetch(`${base}/hook/missing`, { method: "POST" })).status).toBe(404);
		const bytes = new TextEncoder().encode(JSON.stringify({ x: 1 }));
		const timestamp = Math.floor(Date.now() / 1000);
		const nonce = "nonce";
		const signature = createHmac("sha256", "key").update(`${timestamp}.${nonce}.`).update(bytes).digest("hex");
		const request = () =>
			fetch(`${base}/hook/secret`, {
				method: "POST",
				body: bytes,
				headers: {
					"x-gajaeway-timestamp": String(timestamp),
					"x-gajaeway-nonce": nonce,
					"x-gajaeway-signature": signature,
				},
			});
		expect((await request()).status).toBe(202);
		expect(events).toEqual(["admitted"]);
		expect((await request()).status).toBe(401);
		expect((await fetch(`${base}/hook/secret`, { method: "POST", body: new Uint8Array(256 * 1024 + 1) })).status).toBe(
			413,
		);
	} finally {
		server.stop(true);
	}
});

test("webhook refuses non-loopback exposure without explicit opt-in", () => {
	expect(() => startWebhook({ bind: "0.0.0.0", port: 0, monitors: () => [], submit: () => "event" })).toThrow();
});

test("watcher and script honor allowlists and ActionGuard", async () => {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-trigger-"));
	const outside = await mkdtemp(join(tmpdir(), "gajaeway-outside-"));
	try {
		const paths: string[] = [];
		const stop = await startWatcher(root, [root], (path) => paths.push(path), 5);
		for (let i = 0; i < 100 && paths.length === 0; i++) {
			if (i % 10 === 0) await writeFile(join(root, `file-${i}`), "x");
			await Bun.sleep(20);
		}
		stop();
		expect(paths.length).toBeGreaterThanOrEqual(1);
		await expect(startWatcher(outside, [root], () => {})).rejects.toThrow();
		await symlink(join(outside, "target"), join(root, "link"));
		const script = join(root, "echo.sh");
		await writeFile(script, "#!/bin/sh\necho payload\n");
		await chmod(script, 0o755);
		const output: string[] = [];
		const stopScript = await startScript([script], 100_000, root, (text) => output.push(text));
		for (let i = 0; i < 100 && output.length === 0; i++) await Bun.sleep(20);
		stopScript();
		expect(output[0]).toContain("payload");
		await expect(startScript([script, "rm -rf /"], 1, root, () => {})).rejects.toThrow();
	} finally {
		await rm(root, { recursive: true, force: true });
		await rm(outside, { recursive: true, force: true });
	}
});

test("catch-all origin retains undeclared event identity", () => {
	expect(originKey(CATCH_ALL_EVENT_ORIGIN)).toBe("monitor/eventtype/catch-all");
});

test("default memory monitors seed per-name and respect operator removals", async () => {
	const { mkdtemp: mkTemp } = await import("node:fs/promises");
	const { tmpdir: tmp } = await import("node:os");
	const { join: joinPath } = await import("node:path");
	const { Store: Db } = await import("../src/store");
	const { MonitorRegistry: Registry } = await import("../src/monitors/registry");
	const { seedDefaultMonitors } = await import("../src/monitors/defaults");

	// Fresh DB seeds all 3 monitors
	const dir = await mkTemp(joinPath(tmp(), "gajaeway-seed-"));
	const database = await Db.open(joinPath(dir, "gateway.db"));
	const registry = new Registry(database);
	expect(seedDefaultMonitors(registry, database)).toBe(3);
	const names = registry.list().map((monitor) => monitor.name);
	expect(names).toContain("memory-canonicalize");
	expect(names).toContain("memory-audit");
	expect(names).toContain("memory-debloat");

	// Second boot: per-name flags prevent reseeding
	expect(seedDefaultMonitors(registry, database)).toBe(0);

	// Operator removal survives later boots
	const canonical = registry.list().find((monitor) => monitor.name === "memory-canonicalize");
	registry.remove(canonical?.monitorId ?? "");
	expect(seedDefaultMonitors(registry, database)).toBe(0);
	expect(registry.list().map((monitor) => monitor.name)).not.toContain("memory-canonicalize");
	database.close();

	// Old-flag DB with legacy global flag: seeds only memory-debloat
	const dir2 = await mkTemp(joinPath(tmp(), "gajaeway-seed-legacy-"));
	const database2 = await Db.open(joinPath(dir2, "gateway.db"));
	database2.core.metaSet("default_monitors_seeded", new Date().toISOString());
	const registry2 = new Registry(database2);

	// Legacy boot seeded both old defaults; the operator has since removed memory-audit.
	const { DEFAULT_MEMORY_MONITORS } = await import("../src/monitors/defaults");
	registry2.add(DEFAULT_MEMORY_MONITORS[0]!);

	// Only the new default arrives; the removed legacy default stays removed.
	expect(seedDefaultMonitors(registry2, database2)).toBe(1);
	expect(
		registry2
			.list()
			.map((monitor) => monitor.name)
			.sort(),
	).toEqual(["memory-canonicalize", "memory-debloat"]);
	registry2.remove(registry2.list().find((monitor) => monitor.name === "memory-debloat")!.monitorId);
	expect(seedDefaultMonitors(registry2, database2)).toBe(0);
	database2.close();

	// A default the operator already created by name counts as seeded, so removing it is final.
	const dir3 = await mkTemp(joinPath(tmp(), "gajaeway-seed-existing-"));
	const database3 = await Db.open(joinPath(dir3, "gateway.db"));
	const registry3 = new Registry(database3);
	registry3.add(DEFAULT_MEMORY_MONITORS[2]!);
	expect(seedDefaultMonitors(registry3, database3)).toBe(2);
	registry3.remove(registry3.list().find((monitor) => monitor.name === "memory-debloat")!.monitorId);
	expect(seedDefaultMonitors(registry3, database3)).toBe(0);
	expect(registry3.list().map((monitor) => monitor.name)).not.toContain("memory-debloat");
	database3.close();
});

test("memory config validates debloatByteThreshold as positive integer", async () => {
	const { parseConfigFile } = await import("../src/config");

	// Valid: positive integer
	const valid = parseConfigFile({
		schemaVersion: 1,
		memory: { debloatByteThreshold: 102400 },
	});
	expect(valid.memory?.debloatByteThreshold).toBe(102400);

	// Valid: empty object keeps the default threshold
	const empty = parseConfigFile({
		schemaVersion: 1,
		memory: {},
	});
	expect(empty.memory?.debloatByteThreshold).toBeUndefined();

	// Invalid: zero
	expect(() =>
		parseConfigFile({
			schemaVersion: 1,
			memory: { debloatByteThreshold: 0 },
		}),
	).toThrow("positive integer");

	// Invalid: negative
	expect(() =>
		parseConfigFile({
			schemaVersion: 1,
			memory: { debloatByteThreshold: -1 },
		}),
	).toThrow("positive integer");

	// Invalid: non-integer
	expect(() =>
		parseConfigFile({
			schemaVersion: 1,
			memory: { debloatByteThreshold: 102.5 },
		}),
	).toThrow("positive integer");

	// Invalid: unknown field in memory
	expect(() =>
		parseConfigFile({
			schemaVersion: 1,
			memory: { unknownField: 123 },
		}),
	).toThrow("unknown field");
});

test("monitor cron validation rejects 6-field schedules on write", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-invalid-cron-"));
	let database: Store | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		// Valid: 5-field cron
		const valid = registry.add({
			name: "valid-cron",
			trigger: { kind: "cron", schedule: "0 12 30 10 *" },
			eventTypes: ["test.valid"],
		});
		expect(valid).toBeTruthy();
		// Invalid: 6-field cron
		expect(() =>
			registry.add({
				name: "invalid-cron",
				trigger: { kind: "cron", schedule: "0 12 30 10 * 2026" },
				eventTypes: ["test.invalid"],
			}),
		).toThrow("cron schedule must have five fields");
		// Invalid: 4-field cron
		expect(() =>
			registry.add({
				name: "invalid-cron",
				trigger: { kind: "cron", schedule: "0 12 30 10" },
				eventTypes: ["test.invalid"],
			}),
		).toThrow("cron schedule must have five fields");
		// Only the valid monitor should be in the registry
		const listed = registry.list();
		expect(listed.length).toBe(1);
		expect(listed[0]).toMatchObject({
			name: "valid-cron",
			trigger: { kind: "cron", schedule: "0 12 30 10 *" },
		});
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor cron validation rejects invalid schedules on update", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-update-invalid-cron-"));
	let database: Store | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "update-test",
			trigger: { kind: "cron", schedule: "0 12 * * *" },
			eventTypes: ["test.update"],
		});
		// Valid: update to a different valid schedule
		const updated = registry.update({
			monitorId: monitor.monitorId,
			schedule: "0 */2 * * *",
		});
		expect(updated?.trigger).toMatchObject({ kind: "cron", schedule: "0 */2 * * *" });
		// Invalid: update to a 6-field schedule
		expect(() =>
			registry.update({
				monitorId: monitor.monitorId,
				schedule: "0 12 30 10 * 2026",
			}),
		).toThrow("cron schedule must have five fields");
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("gateway boot skips monitors with invalid cron schedules and logs them", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-boot-invalid-cron-"));
	let database: Store | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		// Create a valid monitor
		const valid = registry.add({
			name: "valid-boot-test",
			trigger: { kind: "cron", schedule: "0 12 * * *" },
			eventTypes: ["test.boot.valid"],
			enabled: true,
		});
		// Manually insert an invalid monitor directly into the database
		database.transaction((tx) => {
			database!.monitors.create(tx, {
				id: "invalid-monitor-id",
				name: "invalid-boot-test",
				triggerJson: JSON.stringify({
					kind: "cron",
					schedule: "0 12 30 10 * 2026", // 6-field invalid cron
					timezone: "UTC",
				}),
				eventTypesJson: JSON.stringify(["test.boot.invalid"]),
				burstPolicy: "coalesce",
				channelTargetJson: null,
				enabled: true,
				instruction: null,
				modelJson: null,
				serviceTier: null,
			});
		});
		// Verify the invalid monitor was inserted
		const allRows = database.monitors.rows();
		expect(allRows.length).toBe(2);
		// Boot the real runtime: the invalid row must not throw, must not run, and
		// must stay visible to operators until removed.
		const warnings: string[] = [];
		const originalWarn = console.warn;
		console.warn = (...args: unknown[]) => {
			warnings.push(args.map(String).join(" "));
		};
		try {
			const runtime = new MonitorRuntime({} as never, registry, { submitSlot: () => "slot" } as never, {
				now: () => new Date(),
			});
			await runtime.start();
			await runtime.stop();
		} finally {
			console.warn = originalWarn;
		}
		expect(warnings.some((line) => line.startsWith("monitor_invalid id=invalid-monitor-id reason="))).toBe(true);
		expect(registry.list().map((monitor) => monitor.monitorId)).toEqual([valid.monitorId]);
		expect(registry.invalid()).toEqual([
			{
				monitorId: "invalid-monitor-id",
				name: "invalid-boot-test",
				reason: "monitor cron schedule must have five fields: minute hour day month weekday",
			},
		]);
		expect(registry.remove("invalid-monitor-id")).toBe(true);
		expect(registry.invalid()).toEqual([]);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});
