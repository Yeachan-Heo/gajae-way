import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajaeway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

test("event-type sessions persist until their explicit epoch is bumped", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-eventtype-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "events",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["build"],
			burstPolicy: "serialize",
		});
		const calls: Array<{ origin: string; epoch: number }> = [];
		const gjc = {
			ensureSession: async (origin: string, epoch = 0) => {
				calls.push({ origin, epoch });
				return { sessionId: `session-${epoch}` };
			},
			sendTurn: async (_id: string, text: string) =>
				JSON.stringify([
					{ eventId: (JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>)[0]!.eventId, note: "ok" },
				]),
		};
		const pipeline = new MonitorPropagator({
			database,
			registry,
			gjc,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		pipeline.submit(monitor.monitorId, "build", {});
		pipeline.submit(monitor.monitorId, "build", {});
		await Bun.sleep(20);
		expect(calls.map((call) => call.origin)).toEqual([
			originKey(eventTypeOrigin("build")),
			originKey(eventTypeOrigin("build")),
		]);
		expect(calls.map((call) => call.epoch)).toEqual([0, 0]);
		database.withTransaction(() =>
			database.bumpEpoch(originKey(eventTypeOrigin("build")), JSON.stringify(eventTypeOrigin("build"))),
		);
		pipeline.submit(monitor.monitorId, "build", {});
		await Bun.sleep(20);
		expect(calls[2]!.epoch).toBe(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
