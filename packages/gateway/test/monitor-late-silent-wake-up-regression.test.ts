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

test("REGRESSION: a late [SILENT] from async-result wake-up does not move authored→authored_no_delivery and replace already-authored answer", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-silent-wake-regression-"));
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

	// Create an event
	(database as Store).transaction((tx) => {
		(database as Store).monitors.eventCreate(tx, {
			eventId,
			monitorId: actualMonitorId,
			eventType,
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
	});

	// First turn: author with a real, non-silent answer
	const firstAuthored = (database as Store).transaction((tx) =>
		(database as Store).monitors.authorFenced(tx, eventId, "", "real answer", false),
	);
	expect(firstAuthored).toBe(true);

	// Verify the stage is 'authored' and answer is stored
	let event = (database as Store).monitors.eventGet(eventId);
	expect(event?.stage).toBe("authored");
	let answer = (database as Store).monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");

	// Later, async-result wake-up turn tries to author again with [SILENT]
	// This should be a no-op (rejected) because:
	// 1. The event is already in 'authored' stage
	// 2. The output is SILENT
	// 3. Even with noDelivery=true, re-authoring an already-authored event should be rejected
	const lateReauthor = (database as Store).transaction((tx) =>
		(database as Store).monitors.authorFenced(tx, eventId, "", "[SILENT]", true),
	);

	// The re-authoring attempt should fail
	expect(lateReauthor).toBe(false);

	// Verify that:
	// 1. The stage is still 'authored' (not moved to 'authored_no_delivery')
	event = (database as Store).monitors.eventGet(eventId);
	expect(event?.stage).toBe("authored");

	// 2. The stored output is still the original non-silent answer
	answer = (database as Store).monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");
});
