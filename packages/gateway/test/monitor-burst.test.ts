import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

test("coalesce preserves every event identity while using one authoring turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "burst",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		let turns = 0;
		const gjc = {
			ensureSession: async () => ({ sessionId: "event-session" }),
			forgetRebinds: () => {},
			sendTurn: async (_id: string, text: string) => {
				turns++;
				return JSON.stringify(
					(JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "recorded",
					})),
				);
			},
		};
		const memory = {
			enqueue: () => crypto.randomUUID(),
			enqueueExistingId: () => {},
		};
		const pipeline = new MonitorPropagator({
			database,
			registry,
			gjc,
			memory: memory as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		for (let i = 0; i < 200; i++) pipeline.submit(monitor.monitorId, "changed", { i });
		await Bun.sleep(350);
		expect(database.monitorEventRows().length).toBe(200);
		// Intents are now admitted atomically at the DB level: count durable rows.
		expect(database.memoryIntentRows().length).toBe(200);
		expect(turns).toBe(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
