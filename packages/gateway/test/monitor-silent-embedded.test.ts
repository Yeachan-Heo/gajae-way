import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import type { SessionPortResponder } from "./session-port.fake";
import { sessionPortFromScript } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let propagators: MonitorPropagator[] = [];

afterEach(async () => {
	for (const propagator of propagators) await propagator.drain();
	propagators = [];
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function fakeSessionPort(respond: SessionPortResponder) {
	return sessionPortFromScript({ bind: async () => ({ sessionId: "s1" }), respond });
}

async function harness(
	respond: SessionPortResponder,
	options: {
		ownerTarget?: { origin: { platform: "loopback"; kind: "loopback"; conversationId: "loopback" } };
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-silent-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "test",
		trigger: { kind: "cron", schedule: "30 */6 * * *" },
		eventTypes: ["memory.canonicalize"],
		burstPolicy: "dedupe",
		enabled: true,
	});
	const sessionPort = fakeSessionPort(respond);
	const propagator = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
	});
	propagators.push(propagator);
	return { propagator, monitor, database, registry };
}

function eventsFromPrompt(text: string): Array<{ eventId: string }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string }>;
}

const stage = (db: GatewayDatabase, id: string) => db.monitorEventRows().find((row) => row.event_id === id)?.stage;

describe("isSilentOutput integration (issue #338: propagate.ts must use embedded marker grammar)", () => {
	test("note starting with [SILENT] plus narration is NOT delivered", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(
			async (_id, text) =>
				JSON.stringify(
					eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "[SILENT] This is a status update" })),
				),
			{ ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } } },
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && db!.authoredOutput(eventId) === undefined; attempt++) await Bun.sleep(10);
		await propagator.drain();
		expect(db!.authoredOutput(eventId)).toBe("[SILENT] This is a status update");
		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(db!.deliveryRows()).toHaveLength(0);
	});

	test("note with [SILENT] mid-text is also NOT delivered (grammar: anywhere silences)", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(
			async (_id, text) =>
				JSON.stringify(
					eventsFromPrompt(text).map(({ eventId }) => ({
						eventId,
						note: "Please see [SILENT] in docs for details",
					})),
				),
			{ ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } } },
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && db!.authoredOutput(eventId) === undefined; attempt++) await Bun.sleep(10);
		await propagator.drain();
		expect(db!.authoredOutput(eventId)).toBe("Please see [SILENT] in docs for details");
		// [SILENT] anywhere silences (per containsSilenceToken grammar)
		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(db!.deliveryRows()).toHaveLength(0);
	});

	test("only [SILENT] token (exact match) is also silent", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(
			async (_id, text) => JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "[SILENT]" }))),
			{ ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } } },
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && db!.authoredOutput(eventId) === undefined; attempt++) await Bun.sleep(10);
		await propagator.drain();
		expect(db!.authoredOutput(eventId)).toBe("[SILENT]");
		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(db!.deliveryRows()).toHaveLength(0);
	});
});
