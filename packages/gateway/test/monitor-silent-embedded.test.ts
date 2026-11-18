import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";
import type { SessionPortResponder } from "./session-port.fake";
import { sessionPortFromScript } from "./session-port.fake";

let home = "";
let database: Store | undefined;
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
		onReadinessSuccess?: (input: { originKey: string; at: number }) => void;
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-silent-"));
	database = await Store.open(join(home, "gateway.db"));
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
		delivery: new DeliveryService(database),
		emit: () => {},
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
		...(options.onReadinessSuccess ? { onReadinessSuccess: options.onReadinessSuccess } : {}),
	});
	propagators.push(propagator);
	return { propagator, monitor, database, registry };
}

function eventsFromPrompt(text: string): Array<{ eventId: string }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string }>;
}

const stage = (db: Store, id: string) => db.monitors.eventRows().find((row) => row.event_id === id)?.stage;

describe("isSilentOutput integration (issue #338: propagate.ts must use the closing-marker grammar)", () => {
	test("note with a reasoning preamble closed by [SILENT] is NOT delivered", async () => {
		const readinessSuccesses: Array<{ originKey: string; at: number }> = [];
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(
			async (_id, text) =>
				JSON.stringify(
					eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "Nothing new since last sweep.\n\n[SILENT]" })),
				),
			{
				ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } },
				onReadinessSuccess: (input) => readinessSuccesses.push(input),
			},
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && db!.monitors.authoredOutput(eventId) === undefined; attempt++)
			await Bun.sleep(10);
		await propagator.drain();
		expect(db!.monitors.authoredOutput(eventId)).toBe("Nothing new since last sweep.\n\n[SILENT]");
		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(db!.outbox.rows()).toHaveLength(0);
		expect(readinessSuccesses).toHaveLength(1);
		expect(Number.isFinite(readinessSuccesses[0]?.at)).toBe(true);
	});

	test("successful dispatch with no delivery target clears monitor readiness", async () => {
		const readinessSuccesses: Array<{ originKey: string; at: number }> = [];
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(
			async (_id, text) =>
				JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "completed" }))),
			{ onReadinessSuccess: (input) => readinessSuccesses.push(input) },
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && stage(db!, eventId) !== "authored_no_delivery"; attempt++)
			await Bun.sleep(10);

		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(readinessSuccesses).toHaveLength(1);
		expect(Number.isFinite(readinessSuccesses[0]?.at)).toBe(true);
	});

	test("note that merely mentions [SILENT] mid-text IS delivered (a mention is not a directive)", async () => {
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
		for (let attempt = 0; attempt < 100 && db!.monitors.authoredOutput(eventId) === undefined; attempt++)
			await Bun.sleep(10);
		await propagator.drain();
		expect(db!.monitors.authoredOutput(eventId)).toBe("Please see [SILENT] in docs for details");
		expect(stage(db!, eventId)).not.toBe("authored_no_delivery");
		expect(db!.outbox.rows()).toHaveLength(1);
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
		for (let attempt = 0; attempt < 100 && db!.monitors.authoredOutput(eventId) === undefined; attempt++)
			await Bun.sleep(10);
		await propagator.drain();
		expect(db!.monitors.authoredOutput(eventId)).toBe("[SILENT]");
		expect(stage(db!, eventId)).toBe("authored_no_delivery");
		expect(db!.outbox.rows()).toHaveLength(0);
	});
});
