import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";
import { sessionPortFromScript } from "./session-port.fake";

test("coalesce preserves every event identity while using one authoring turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-"));
	let database: Store | undefined;
	let pipeline: MonitorPropagator | undefined;
	let resolveIntentsEnqueued!: () => void;
	const intentsEnqueued = new Promise<void>((resolve) => (resolveIntentsEnqueued = resolve));
	let enqueuedIntents = 0;
	try {
		database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "burst",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		let turns = 0;
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "event-session" }),
			respond: async (_id: string, text: string) => {
				turns++;
				return JSON.stringify(
					(JSON.parse(text.match(/\[.*\]$/s)?.[0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "recorded",
					})),
				);
			},
		});
		const memory = {
			enqueue: () => crypto.randomUUID(),
			enqueueExistingId: () => {
				enqueuedIntents++;
				if (enqueuedIntents === 200) resolveIntentsEnqueued();
			},
		};
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: memory as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});
		for (let i = 0; i < 200; i++) pipeline.submit(monitor.monitorId, "changed", { i });
		await intentsEnqueued;
		await pipeline.drain();
		expect(database.monitors.eventRows().length).toBe(200);
		// Intents are now admitted atomically at the DB level: count durable rows.
		expect(database.memory.rows().length).toBe(200);
		expect(turns).toBe(1);
	} finally {
		await pipeline?.drain();
		database?.close();
		await rm(directory, { recursive: true, force: true });
	}
});
