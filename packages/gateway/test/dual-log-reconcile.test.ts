import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromScript } from "./session-port.fake";

test("reconciliation uses retained authored output and otherwise re-dispatches", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-reconcile-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "reconcile",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		const queued: unknown[] = [];
		const dispatches: string[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async (origin: string) => {
				dispatches.push(origin);
				return { sessionId: "event-session" };
			},
			respond: async (_id: string, text: string) =>
				JSON.stringify(
					(JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "authored by model",
					})),
				),
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: {
				enqueue: (mutation: unknown) => {
					queued.push(mutation);
					return "intent";
				},
				enqueueExistingId: () => {},
			} as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		const retained = crypto.randomUUID();
		database.withTransaction(() => {
			database.monitorEventCreate({
				eventId: retained,
				monitorId: monitor.monitorId,
				eventType: "changed",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			database.authoredOutputCreate(retained, "retained authored knowledge");
			database.monitorEventUpdate(retained, "authored");
		});
		const missing = crypto.randomUUID();
		database.withTransaction(() =>
			database.monitorEventCreate({
				eventId: missing,
				monitorId: monitor.monitorId,
				eventType: "changed",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			}),
		);
		await pipeline.reconcile();
		const intents = database.memoryIntentRowsByRowid().filter((row) => row.kind === "monitor-event");
		expect(intents).toHaveLength(2);
		const replyTexts = intents.map((row) => (JSON.parse(row.payload_json) as { replyText: string }).replyText);
		expect(replyTexts.some((text) => text.includes("retained authored knowledge"))).toBe(true);
		expect(replyTexts.some((text) => text.includes("authored by model"))).toBe(true);
		expect(dispatches).toContain(originKey(eventTypeOrigin("changed")));
		expect(database.authoredOutput(missing)).toBe("authored by model");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("reconcile replays same-millisecond events oldest-first", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-reconcile-order-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "order",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPortFromScript({
				bind: async () => ({ sessionId: "s" }),
				respond: async () => "[]",
			}),
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		// One fired_at for both rows: ordering must come from insertion order, not row layout.
		const firedAt = new Date().toISOString();
		const ids = [crypto.randomUUID(), crypto.randomUUID()];
		database.withTransaction(() => {
			for (const [index, eventId] of ids.entries()) {
				database.monitorEventCreate({
					eventId,
					monitorId: monitor.monitorId,
					eventType: "changed",
					payloadJson: "{}",
					firedAt,
				});
				database.authoredOutputCreate(eventId, `authored-${index}`);
				database.monitorEventUpdate(eventId, "authored");
			}
		});
		await pipeline.reconcile();
		// Insertion order (rowid) preserves the oldest-first replay; the
		// (created_at, id) index scrambles same-millisecond UUID ties.
		const intents = database
			.memoryIntentRowsByRowid()
			.filter((row) => row.kind === "monitor-event")
			.map((row) => (JSON.parse(row.payload_json) as { replyText: string }).replyText);
		expect(intents).toEqual(["authored-0", "authored-1"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor without its own channel target reports authored notes to the ownerTarget", async () => {
	const { mkdtemp } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { GatewayDatabase } = await import("../src/store/db");
	const { MonitorRegistry } = await import("../src/monitors/registry");
	const { MonitorPropagator } = await import("../src/monitors/propagate");
	const { DeliveryService } = await import("../src/delivery/delivery");
	const { DeliveryLedger } = await import("../src/store/ledger");
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ownertarget-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "targetless",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: ["memory.canonicalize"],
	});
	const delivery = new DeliveryService(new DeliveryLedger(database));
	const pushed: unknown[] = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort: sessionPortFromScript({
			bind: async () => ({ sessionId: "s" }),
			respond: async (_id: string, prompt: string) =>
				JSON.stringify(
					(JSON.parse(prompt.match(/\[.*\]$/s)?.[0] ?? "[]") as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "owner-target note",
					})),
				),
		}),
		memory: { enqueue: () => {}, enqueueExistingId: () => {} } as never,
		delivery,
		emit: () => {},
		deliver: (payload: unknown) => void pushed.push(payload),
		ownerTarget: {
			origin: { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: "owner" },
		},
	});
	pipeline.submit(monitor.monitorId, "memory.canonicalize", { source: "test" });
	// Wait for the note to be pushed to live adapters immediately.
	for (let attempt = 0; attempt < 400; attempt++) {
		if (pushed.length > 0) break;
		await Bun.sleep(5);
	}
	expect(pushed).toHaveLength(1);
	expect((pushed[0] as { text: string }).text).toBe("owner-target note");

	// Verify the note is persisted in the ledger as inflight (awaiting confirmation).
	const rows = database.deliveryRows();
	expect(rows).toHaveLength(1);
	expect(rows[0]).toMatchObject({
		state: "inflight",
		origin_key: "discord/dm/owner-dm/peer=owner",
	});
	const payload = JSON.parse(rows[0]!.payload_json);
	expect(payload).toMatchObject({
		origin: { platform: "discord", kind: "dm", conversationId: "owner-dm" },
		text: "owner-target note",
	});
	database.close();
});
