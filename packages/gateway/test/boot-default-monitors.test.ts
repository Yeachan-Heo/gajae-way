import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { bootGateway } from "../src/boot";
import type { MemoryConfig } from "../src/config";
import { DEFAULT_MEMORY_MONITORS } from "../src/monitors/defaults";
import { MonitorRegistry } from "../src/monitors/registry";
import { readPinnedGjcVersion, streamRelayArgs } from "../src/orchestrator/broker";
import { Store } from "../src/store";

const homes: string[] = [];
afterEach(async () => {
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function temporaryHome(): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-boot-monitors-"));
	homes.push(home);
	return home;
}

async function configure(home: string, memory?: MemoryConfig): Promise<void> {
	await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1, ...(memory ? { memory } : {}) }));
}

function fakeBroker() {
	const calls: string[][] = [];
	const relayProbe = streamRelayArgs("00000000-0000-4000-8000-000000000000");
	const allowed = (args: readonly string[]) =>
		args[0] === "--version" || (args[1] === "session" && args[2] === "list") || Bun.deepEquals(args, relayProbe);
	const command: CliRunner = async (args) => {
		calls.push([...args]);
		if (args[0] === "--version") return { exitCode: 0, stdout: `gjc/${readPinnedGjcVersion()}\n`, stderr: "" };
		if (args[1] === "session" && args[2] === "list")
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
		if (Bun.deepEquals(args, relayProbe)) return { exitCode: 1, stdout: "", stderr: "probe session does not exist" };
		throw new Error(`Unexpected broker command: ${args.join(" ")}`);
	};
	return {
		calls,
		allowed,
		dependencies: {
			executable: "/test-only/gjc",
			command,
			healthProbe: async () => true,
			discovery: async () => ({ pid: 1, url: "ws://127.0.0.1:1", token: "test-only", heartbeatAt: Date.now() }),
			healthIntervalMs: 60_000,
			log: () => {},
		},
	};
}

function seedMarkers(database: Store) {
	return [
		"default_monitors_seeded",
		...DEFAULT_MEMORY_MONITORS.map((spec) => `default_monitor_seeded:${spec.name}`),
	].map((key) => database.core.metaGet(key));
}

async function withBoot(home: string, check: (database: Store) => void) {
	const broker = fakeBroker();
	const server = await bootGateway({ home, broker: broker.dependencies });
	try {
		const database = await Store.open(join(home, "gateway.db"));
		try {
			check(database);
		} finally {
			database.close();
		}
	} finally {
		await server.stop("test shutdown");
	}
	// The fake cannot execute a real broker or author a monitor turn.
	expect(broker.calls.some((args) => args[1] === "session" && args[2] === "list")).toBe(true);
	expect(broker.calls.every(broker.allowed)).toBe(true);
}

test("false reaches boot before seeding: fresh DB and restart have no monitors, events, or seed markers", async () => {
	const home = await temporaryHome();
	await configure(home, { seedDefaultMonitors: false });
	for (let boot = 0; boot < 2; boot++) {
		await withBoot(home, (database) => {
			expect(new MonitorRegistry(database).list()).toEqual([]);
			expect(database.monitors.eventStageCounts()).toEqual([]);
			expect(seedMarkers(database)).toEqual([undefined, undefined, undefined, undefined]);
		});
	}
	// Skipping did not consume the seed-once opportunity.
	await configure(home, { seedDefaultMonitors: true });
	await withBoot(home, (database) => {
		expect(
			new MonitorRegistry(database)
				.list()
				.map((monitor) => monitor.name)
				.sort(),
		).toEqual(DEFAULT_MEMORY_MONITORS.map((spec) => spec.name).sort());
		expect(seedMarkers(database).slice(1).every(Boolean)).toBe(true);
	});
});

for (const memory of [undefined, {}, { seedDefaultMonitors: true }]) {
	test(`default/true boot seeds once and preserves removed, disabled and custom records: ${JSON.stringify(memory)}`, async () => {
		const home = await temporaryHome();
		await configure(home, memory);
		let records: ReturnType<MonitorRegistry["list"]> = [];
		let markers: ReturnType<typeof seedMarkers> = [];
		await withBoot(home, (database) => {
			const registry = new MonitorRegistry(database);
			expect(registry.list()).toHaveLength(3);
			expect(registry.list().every((monitor) => monitor.enabled)).toBe(true);
			const [removed, disabled] = registry.list();
			if (!removed || !disabled) throw new Error("missing seeded defaults");
			registry.remove(removed.monitorId);
			registry.update({ monitorId: disabled.monitorId, enabled: false });
			registry.add({ name: "custom", trigger: { kind: "cron", schedule: "0 0 29 2 *" }, eventTypes: ["custom.check"] });
			records = registry.list();
			markers = seedMarkers(database);
		});
		for (const policy of [{ seedDefaultMonitors: false }, { seedDefaultMonitors: true }]) {
			await configure(home, policy);
			await withBoot(home, (database) => {
				expect(new MonitorRegistry(database).list()).toEqual(records);
				expect(seedMarkers(database)).toEqual(markers);
			});
		}
	});
}

test("false leaves pre-existing unseeded custom and disabled default-name records untouched", async () => {
	const home = await temporaryHome();
	await configure(home, { seedDefaultMonitors: false });
	let records: ReturnType<MonitorRegistry["list"]> = [];
	await withBoot(home, (database) => {
		const registry = new MonitorRegistry(database);
		const spec = DEFAULT_MEMORY_MONITORS[0];
		if (!spec) throw new Error("missing default spec");
		registry.add({ ...spec, enabled: false });
		registry.add({
			name: "custom-disabled",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["custom.check"],
			enabled: false,
		});
		records = registry.list();
	});
	for (let boot = 0; boot < 2; boot++) {
		await withBoot(home, (store) => {
			expect(new MonitorRegistry(store).list()).toEqual(records);
			expect(store.monitors.eventStageCounts()).toEqual([]);
			expect(seedMarkers(store)).toEqual([undefined, undefined, undefined, undefined]);
		});
	}
});

test("false preserves a legacy seed marker and defers the remaining default until enabled", async () => {
	const home = await temporaryHome();
	await configure(home, { seedDefaultMonitors: false });
	const legacy = "2026-01-01T00:00:00.000Z";
	await withBoot(home, (database) => {
		database.core.metaSet("default_monitors_seeded", legacy);
	});
	await withBoot(home, (database) => {
		expect(new MonitorRegistry(database).list()).toEqual([]);
		expect(seedMarkers(database)).toEqual([legacy, undefined, undefined, undefined]);
	});
	await configure(home, { seedDefaultMonitors: true });
	await withBoot(home, (database) => {
		expect(new MonitorRegistry(database).list().map((monitor) => monitor.name)).toEqual(["memory-debloat"]);
		expect(seedMarkers(database)[0]).toBe(legacy);
		expect(seedMarkers(database).slice(1).every(Boolean)).toBe(true);
	});
});

for (const memory of [{ seedDefaultMonitors: "false" }, { seedDefaultMonitor: false }]) {
	test(`invalid memory seed config fails before database creation or broker access: ${JSON.stringify(memory)}`, async () => {
		const home = await temporaryHome();
		await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1, memory }));
		const broker = fakeBroker();
		await expect(bootGateway({ home, broker: broker.dependencies })).rejects.toMatchObject({ code: "config_invalid" });
		expect(await Bun.file(join(home, "gateway.db")).exists()).toBe(false);
		expect(broker.calls).toEqual([]);
	});
}
