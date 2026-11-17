import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { sessionPortFromResponder } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;
let database: Store | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

test("a late [SILENT] from async-result wake-up does not replace already-authored monitor answer", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-silent-wake-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	database = await Store.open(config.dbPath);
	if (!database) throw new Error("Failed to open database");

	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async () => "real answer",
	});
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database?.close(),
	});
	if (!database) throw new Error("Database lost");

	const _monitorId = `test-monitor-${crypto.randomUUID()}`;
	const eventId = `event-${crypto.randomUUID()}`;
	const eventType = "test.event";

	// Create monitor
	const { MonitorRegistry } = await import("../src/monitors/registry");
	const monitors = new MonitorRegistry(database as Store);
	monitors.add({
		name: "test-monitor",
		trigger: { kind: "cron", schedule: "30 6 * * *" },
		eventTypes: [eventType],
		enabled: true,
	});
	const actualMonitorId = monitors.list().find((m) => m.name === "test-monitor")?.monitorId;
	if (!actualMonitorId) throw new Error("Failed to create monitor");

	// Simulate the first authoring turn that produces a real answer
	(database as Store).transaction((tx) => {
		(database as Store).monitors.eventCreate(tx, {
			eventId,
			monitorId: actualMonitorId,
			eventType,
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
	});

	// First turn authorizes with a real answer
	let authored: boolean = false;
	(database as Store).transaction((tx) => {
		authored = (database as Store).monitors.authorFenced(tx, eventId, "", "real answer", false);
	});
	if (!authored) throw new Error("First author failed");
	expect(authored).toBe(true);

	let answer = (database as Store).monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");

	// Later, async-result wake-up turn tries to replace with [SILENT]
	// This should NOT replace the already-authored answer
	let replaced: boolean = false;
	(database as Store).transaction((tx) => {
		replaced = (database as Store).monitors.authorFenced(tx, eventId, "", "[SILENT]", false);
	});

	// The replace attempt should fail because the event is already in 'authored' stage
	if (replaced) throw new Error("Second author should have failed");
	expect(replaced).toBe(false);

	// Verify that the original answer is still there
	answer = (database as Store).monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");
});
