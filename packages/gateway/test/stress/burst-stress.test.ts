import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../../src/delivery/delivery";
import { MemoryClosureQueue } from "../../src/memory/closure";
import { MonitorPropagator } from "../../src/monitors/propagate";
import { MonitorRegistry } from "../../src/monitors/registry";
import { GatewayDatabase } from "../../src/store/db";
import { DeliveryLedger } from "../../src/store/ledger";
import { sessionPortFromScript } from "../session-port.fake";

const stress = process.env.GAJAEWAY_STRESS === "1";

if (!stress) {
	test.skip("200-event burst stress requires GAJAEWAY_STRESS=1", () => {});
} else {
	test("processes 200 monitor events and receipts each memory closure within the contractual burst bounds", async () => {
		const started = performance.now();
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-burst-stress-"));
		try {
			const database = await GatewayDatabase.open(join(directory, "gateway.db"));
			const registry = new MonitorRegistry(database);
			const monitor = registry.add({
				name: "contractual-burst",
				trigger: { kind: "cron", schedule: "* * * * *" },
				eventTypes: ["changed"],
			});
			let turns = 0;
			const memory = new MemoryClosureQueue(database, directory);
			await memory.initialize();
			const pipeline = new MonitorPropagator({
				database,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "stress-session" }),
					respond: async (_sessionId, prompt) => {
						turns++;
						return JSON.stringify(
							(JSON.parse(prompt.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
								eventId,
								note: `receipted ${eventId}`,
							})),
						);
					},
				}),
				memory,
				delivery: new DeliveryService(new DeliveryLedger(database)),
				emit: () => {},
			});

			for (let batch = 0; batch < 20; batch++) {
				for (let offset = 0; offset < 10; offset++)
					pipeline.submit(monitor.monitorId, "changed", { sequence: batch * 10 + offset });
				await Bun.sleep(3_000);
			}
			for (
				let attempt = 0;
				attempt < 600 && database.monitorEventRows().filter((event) => event.stage === "authored").length !== 200;
				attempt++
			)
				await Bun.sleep(100);
			await memory.drain();
			const monitorEvents = database.monitorEventRows().length;
			const receipted = database
				.memoryIntentRows()
				.filter((intent) => intent.kind === "monitor-event" && intent.state === "receipted").length;
			const elapsedMs = Math.round(performance.now() - started);
			console.log(JSON.stringify({ stress: "burst", monitorEvents, receipted, turns, elapsedMs }));
			expect(monitorEvents).toBe(200);
			expect(receipted).toBe(200);
			expect(turns).toBeLessThan(30);
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	}, 180_000);
}
