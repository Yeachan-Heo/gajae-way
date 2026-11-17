import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";
import { sessionPortFromScript } from "./session-port.fake";

const target: OriginRef = { platform: "discord", kind: "channel", conversationId: "report-room" };

function eventsFromPrompt(text: string): Array<{ eventId: string; eventType: string; payload?: unknown }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string; eventType: string; payload?: unknown }>;
}

async function openTestStore() {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-author-rejected-"));
	const database = await Store.open(join(home, "gateway.db"));
	return { home, database };
}

test("REGRESSION: MonitorPropagator rejects late non-silent on authored row via reconcile, and no report is enqueued", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "author-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["author.test"],
			reportTo: target,
		});

		const eventId = crypto.randomUUID();
		const eventType = "author.test";

		// Manually set up: create an event in authored state with stored output, but no memory intent
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId,
				monitorId: monitor.monitorId,
				eventType,
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			// Author the event - this sets the stage to authored and stores the output
			const result = database.monitors.authorFenced(tx, eventId, "", "original answer", false);
			expect(result).toBe(true);
		});

		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.monitors.authoredOutput(eventId)).toBe("original answer");
		expect(!database.memory.coversEvent(eventId)).toBe(true); // No memory for this event

		// Count initial reports in inbox
		const initialReports = database.inbox.pendingByOrigin();
		const initialReportCount = initialReports.find((r) => r.origin_key === originKey(target))?.n ?? 0;

		// Now create propagator with a session that returns a different answer
		const notifications: string[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) => {
				const events = eventsFromPrompt(text);
				return JSON.stringify(
					events.map(({ eventId }) => ({
						eventId,
						note: "different late answer",
					})),
				);
			},
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		// Call reconcile - this will find the event with output but no memory
		// Since stage === "authored" && output is not silent && !hasMemory,
		// it will call #author (line 711 in reconcile)
		// When #author calls authorFenced, it will return false (already authored)
		// The fix ensures #notifyMonitorReport is not called in this case
		await propagator.reconcile();

		// Verify the event is still authored
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");

		// Verify no extra reports were enqueued
		const finalReports = database.inbox.pendingByOrigin();
		const finalReportCount = finalReports.find((r) => r.origin_key === originKey(target))?.n ?? 0;
		expect(finalReportCount).toBe(initialReportCount);

		// Verify no notification was sent
		expect(notifications).toEqual([]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("REGRESSION: MonitorPropagator rejects late SILENT on authored row via reconcile, stage changes but no report is enqueued", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "silent-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["silent.test"],
			reportTo: target,
		});

		const eventId = crypto.randomUUID();
		const eventType = "silent.test";

		// Manually set up: create an event in authored state with silent output, but no memory intent
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId,
				monitorId: monitor.monitorId,
				eventType,
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			// Author the event with SILENT - this sets the stage to authored and stores the output
			const result = database.monitors.authorFenced(tx, eventId, "", "[SILENT]", false);
			expect(result).toBe(true);
		});

		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.monitors.authoredOutput(eventId)).toBe("[SILENT]");
		expect(!database.memory.coversEvent(eventId)).toBe(true); // No memory for this event

		// Count initial reports
		const initialReports = database.inbox.pendingByOrigin();
		const initialReportCount = initialReports.find((r) => r.origin_key === originKey(target))?.n ?? 0;

		// Now create propagator
		const notifications: string[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async () => JSON.stringify([]),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		// Call reconcile - this will find the event with SILENT output and no memory
		// Since stage === "authored" && output IS silent && !hasMemory,
		// it will NOT call #author but will update stage to authored_no_delivery (line 712-714 in reconcile)
		// No report should be enqueued in this case
		await propagator.reconcile();

		// Verify the event stage changed to authored_no_delivery
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");

		// Verify no extra reports were enqueued
		const finalReports = database.inbox.pendingByOrigin();
		const finalReportCount = finalReports.find((r) => r.origin_key === originKey(target))?.n ?? 0;
		expect(finalReportCount).toBe(initialReportCount);

		// Verify no notification was sent
		expect(notifications).toEqual([]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});
