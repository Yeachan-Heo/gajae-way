import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromScript } from "./session-port.fake";

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
		const sessionPort = sessionPortFromScript({
			bind: async (origin: string, epoch = 0) => {
				calls.push({ origin, epoch });
				return { sessionId: `session-${epoch}` };
			},
			respond: async (_id: string, text: string) =>
				JSON.stringify([
					{ eventId: (JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>)[0]!.eventId, note: "ok" },
				]),
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		pipeline.submit(monitor.monitorId, "build", {});
		pipeline.submit(monitor.monitorId, "build", {});
		await Bun.sleep(20);
		expect(calls.map((call) => call.origin)).toEqual([
			originKey(monitorSessionOrigin(monitor.monitorId, "build")),
			originKey(monitorSessionOrigin(monitor.monitorId, "build")),
		]);
		expect(calls.map((call) => call.epoch)).toEqual([0, 0]);
		database.withTransaction(() =>
			database.bumpEpoch(
				originKey(monitorSessionOrigin(monitor.monitorId, "build")),
				JSON.stringify(monitorSessionOrigin(monitor.monitorId, "build")),
			),
		);
		pipeline.submit(monitor.monitorId, "build", {});
		await Bun.sleep(20);
		expect(calls[2]!.epoch).toBe(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
