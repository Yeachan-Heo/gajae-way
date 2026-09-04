import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromScript } from "./session-port.fake";

const responseFor = (prompt: string) =>
	JSON.stringify([
		{ eventId: (JSON.parse(prompt.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>)[0]!.eventId, note: "ok" },
	]);

test("monitors inherit gateway model/tier by default and may override both per monitor", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-model-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const inherited = registry.add({
			name: "inherited",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["inherit.tick"],
			burstPolicy: "serialize",
		});
		const overridden = registry.add({
			name: "overridden",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["override.tick"],
			burstPolicy: "serialize",
			model: "z-ai/glm-5.3",
			serviceTier: "none",
		});
		const port = sessionPortFromScript({
			bind: async (origin) => ({ sessionId: origin }),
			respond: async (_id, prompt) => responseFor(prompt),
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: port,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
			repo: join(home, "workspace"),
			model: { preset: "gpt-heavy" },
			serviceTier: "priority",
		});

		await pipeline.submitAwaitable(inherited.monitorId, "inherit.tick", {});
		await pipeline.submitAwaitable(overridden.monitorId, "override.tick", {});

		expect(port.binds.map((bind) => [bind.originKey, bind.model])).toEqual([
			["monitor/eventtype/inherit.tick", { preset: "gpt-heavy" }],
			["monitor/eventtype/override.tick", "z-ai/glm-5.3"],
		]);
		expect(port.serviceTiers.map((entry) => [entry.sessionId, entry.tier])).toEqual([
			["monitor/eventtype/inherit.tick", "priority"],
			["monitor/eventtype/override.tick", "none"],
		]);
		expect(registry.get(inherited.monitorId)).toMatchObject({ model: undefined, serviceTier: undefined });
		expect(registry.get(overridden.monitorId)).toMatchObject({ model: "z-ai/glm-5.3", serviceTier: "none" });
		expect(database.schemaVersion).toBe(20);
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
