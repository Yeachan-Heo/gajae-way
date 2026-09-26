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

// #177: two monitors declaring one event type used to share one
// `monitor/eventtype/<type>` session — its history, its instruction, and its
// failure domain.
test("monitors sharing an event type each own a separate session and failure domain", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-scope-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const way = registry.add({
			name: "way-backlog-watch",
			trigger: { kind: "cron", schedule: "0 */2 * * *" },
			eventTypes: ["backlog.watch"],
			burstPolicy: "serialize",
			instruction: "WAY-INSTRUCTION",
		});
		const other = registry.add({
			name: "openinstinct-backlog-watch",
			trigger: { kind: "cron", schedule: "30 */2 * * *" },
			eventTypes: ["backlog.watch"],
			burstPolicy: "serialize",
			instruction: "OPENINSTINCT-INSTRUCTION",
		});
		const wayKey = originKey(monitorSessionOrigin(way.monitorId, "backlog.watch"));
		const otherKey = originKey(monitorSessionOrigin(other.monitorId, "backlog.watch"));
		expect(wayKey).not.toBe(otherKey);
		const sessionByOrigin = new Map<string, string>();
		const promptsBySession = new Map<string, string[]>();
		let poisoned: string | undefined;
		const sessionPort = sessionPortFromScript({
			bind: async (origin: string) => {
				const sessionId = sessionByOrigin.get(origin) ?? `session-${sessionByOrigin.size}`;
				sessionByOrigin.set(origin, sessionId);
				return { sessionId };
			},
			respond: async (sessionId: string, text: string) => {
				promptsBySession.set(sessionId, [...(promptsBySession.get(sessionId) ?? []), text]);
				if (sessionId === poisoned) throw new Error("session terminal: failed");
				const [event] = JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>;
				return JSON.stringify([{ eventId: event!.eventId, note: "ok" }]);
			},
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		await pipeline.submitAwaitable(way.monitorId, "backlog.watch", {});
		await pipeline.submitAwaitable(other.monitorId, "backlog.watch", {});
		// (a) two distinct session origins and gjc sessions.
		expect([...sessionByOrigin.keys()].sort()).toEqual([wayKey, otherKey].sort());
		const waySession = sessionByOrigin.get(wayKey)!;
		const otherSession = sessionByOrigin.get(otherKey)!;
		expect(waySession).not.toBe(otherSession);
		// (b) neither session ever sees the other monitor's instruction.
		expect(promptsBySession.get(waySession)!.join("\n")).not.toContain("OPENINSTINCT-INSTRUCTION");
		expect(promptsBySession.get(otherSession)!.join("\n")).not.toContain("WAY-INSTRUCTION");
		// (c) a poisoned session fails only its own monitor.
		poisoned = waySession;
		const wayFailed = await pipeline.submitAwaitable(way.monitorId, "backlog.watch", {});
		const otherOk = await pipeline.submitAwaitable(other.monitorId, "backlog.watch", {});
		const stages = new Map(database.monitorEventRows().map((row) => [row.event_id, row.stage]));
		expect(stages.get(wayFailed)).toBe("failed");
		expect(stages.get(otherOk)).not.toBe("failed");
		expect(database.authoredOutput(otherOk)).toBe("ok");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
