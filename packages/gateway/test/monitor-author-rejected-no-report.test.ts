import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { sessionPortFromResponder } from "./session-port.fake";

const target: OriginRef = { platform: "discord", kind: "channel", conversationId: "report-room" };

let directory = "";
let server: GatewayServer | undefined;
let database: Store | undefined;
let propagator: MonitorPropagator | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
	propagator = undefined;
});

test("REGRESSION: authorFenced rejects on authored row, and no report is enqueued by the calling code", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-author-rejected-authored-"));
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

	// Create monitor with reportTo
	const { MonitorRegistry: MonitorRegistryClass } = await import("../src/monitors/registry");
	const monitors = new MonitorRegistryClass(database as Store);
	const monitor = monitors.add({
		name: "test-monitor",
		trigger: { kind: "cron", schedule: "30 6 * * *" },
		eventTypes: [eventType],
		reportTo: target,
		enabled: true,
	});
	const actualMonitorId = monitor.monitorId;

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

	try {
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

		// Count initial outbox reports
		const initialReportCount = (database as Store).outbox.rows().length;

		// Try to author on the authored row - authorFenced should return false
		const updateResult = (database as Store).transaction((tx) => {
			return (database as Store).monitors.authorFenced(tx, eventId, "", "new rejected answer", false);
		});

		// authorFenced should return false because the row is already in authored state
		expect(updateResult).toBe(false);

		// Verify that the stage is still authored
		expect((database as Store).monitors.eventGet(eventId)?.stage).toBe("authored");

		// Verify that the stored output is unchanged
		expect((database as Store).monitors.authoredOutput(eventId)).toBe("real answer");

		// Most importantly: no report should be enqueued when authorFenced returns false
		const finalReportCount = (database as Store).outbox.rows().length;
		expect(finalReportCount).toBe(initialReportCount);
	} finally {
		propagator = undefined;
	}
});

test("REGRESSION: authorFenced rejects on delivered row, and no report is enqueued by the calling code", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-author-rejected-delivered-"));
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

	// Create monitor with reportTo
	const { MonitorRegistry: MonitorRegistryClass } = await import("../src/monitors/registry");
	const monitors = new MonitorRegistryClass(database as Store);
	const monitor = monitors.add({
		name: "test-monitor",
		trigger: { kind: "cron", schedule: "30 6 * * *" },
		eventTypes: [eventType],
		reportTo: target,
		enabled: true,
	});
	const actualMonitorId = monitor.monitorId;

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

	try {
		const registry = new MonitorRegistry(database as Store);

		// First turn: author with a real, non-silent answer
		const firstAuthored = (database as Store).transaction((tx) =>
			(database as Store).monitors.authorFenced(tx, eventId, "", "real answer", false),
		);
		expect(firstAuthored).toBe(true);

		// Move to delivered state
		(database as Store).transaction((tx) => {
			(database as Store).monitors.eventUpdate(tx, eventId, "delivered");
		});

		// Verify the stage is 'delivered' and answer is stored
		let event = (database as Store).monitors.eventGet(eventId);
		expect(event?.stage).toBe("delivered");
		let answer = (database as Store).monitors.authoredOutput(eventId);
		expect(answer).toBe("real answer");

		// Count initial outbox reports
		const initialReportCount = (database as Store).outbox.rows().length;

		// Try to author on the delivered row - authorFenced should return false
		const updateResult = (database as Store).transaction((tx) => {
			return (database as Store).monitors.authorFenced(tx, eventId, "", "new rejected answer", false);
		});

		// authorFenced should return false because the row is in delivered state
		expect(updateResult).toBe(false);

		// Verify that the stored output is unchanged
		expect((database as Store).monitors.authoredOutput(eventId)).toBe("real answer");

		// Most importantly: no report should be enqueued when authorFenced returns false
		const finalReportCount = (database as Store).outbox.rows().length;
		expect(finalReportCount).toBe(initialReportCount);
	} finally {
		propagator = undefined;
	}
});
