import { expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { chmod, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CATCH_ALL_EVENT_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { MonitorRegistry } from "../src/monitors/registry";
import { cronMatches, cronSlotsBetween, nextCronFire, startCron } from "../src/monitors/triggers/cron";
import { startScript } from "../src/monitors/triggers/script";
import { startWatcher } from "../src/monitors/triggers/watcher";
import { startWebhook } from "../src/monitors/triggers/webhook";
import { GatewayDatabase } from "../src/store/db";

test("cron supports steps, lists, ranges, dow and injected clock", () => {
	const monday = new Date(2026, 0, 5, 10, 15);
	expect(cronMatches("*/5 10 1-10 1 1", monday)).toBe(true);
	expect(cronMatches("0,15,30 10 * * 1", monday)).toBe(true);
	let fired = 0;
	const stop = startCron(
		"15 10 * * 1",
		() => {
			fired++;
			return true;
		},
		{ now: () => monday, since: new Date(2026, 0, 5, 10, 0) },
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
		(scheduled) => {
			admitted.push(scheduled.toISOString());
			return true;
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
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
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

test("default memory monitors seed once and respect operator removals", async () => {
	const { mkdtemp: mkTemp } = await import("node:fs/promises");
	const { tmpdir: tmp } = await import("node:os");
	const { join: joinPath } = await import("node:path");
	const { GatewayDatabase: Db } = await import("../src/store/db");
	const { MonitorRegistry: Registry } = await import("../src/monitors/registry");
	const { seedDefaultMonitors } = await import("../src/monitors/defaults");
	const dir = await mkTemp(joinPath(tmp(), "gajaeway-seed-"));
	const database = await Db.open(joinPath(dir, "gateway.db"));
	const registry = new Registry(database);
	expect(seedDefaultMonitors(registry, database)).toBe(2);
	const names = registry.list().map((monitor) => monitor.name);
	expect(names).toContain("memory-canonicalize");
	expect(names).toContain("memory-audit");
	// Second boot: flag prevents reseeding.
	expect(seedDefaultMonitors(registry, database)).toBe(0);
	// Operator removal survives later boots.
	const canonical = registry.list().find((monitor) => monitor.name === "memory-canonicalize");
	registry.remove(canonical?.monitorId ?? "");
	expect(seedDefaultMonitors(registry, database)).toBe(0);
	expect(registry.list().map((monitor) => monitor.name)).not.toContain("memory-canonicalize");
	database.close();
});
