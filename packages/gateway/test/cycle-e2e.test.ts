import { afterEach, expect, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OriginRef } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromResponder } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop("test teardown");
	server = undefined;
});

const discordDm: OriginRef = { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" };

function testConfig(dir: string): GatewayConfig {
	return {
		schemaVersion: 1,
		home: dir,
		configPath: join(dir, "config.json"),
		socketPath: join(dir, "gateway.sock"),
		dbPath: join(dir, "gateway.db"),
		logVerbosity: "info",
	};
}

test("projector reads durable rows through the database and stays fail-closed", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-db-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const key = "discord/dm/c1/peer=p1";
		// Mid-rebind state: /new bumped the epoch and cleared the session binding.
		database.bumpEpoch(key, JSON.stringify(discordDm));
		const projector = new RuntimeCycleProjector(database, { queueDepth: 0 });
		const afterBump = projector.project();
		expect(afterBump.gates).toContain("stale_session_identity");
		expect(afterBump.phase).toBe("degraded");
		expect(afterBump.sessions[0]).toMatchObject({ originKey: key, epoch: 1, sessionId: "" });

		// A bound session clears the stale-identity gate.
		database.putSession(key, "sess-bound-000000000");
		const bound = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(bound.gates).toEqual([]);
		expect(bound.phase).toBe("idle");
		expect(bound.instanceId).toBeString();
		expect(bound.agentDisk).toBeNull();
		// Broker-bound: the projector observes the agent directory's filesystem (issue #15).
		const observed = new RuntimeCycleProjector(database, { queueDepth: 0 }, { agentDir: directory }).project();
		expect(observed.agentDisk?.path).toBe(directory);
		expect(observed.agentDisk?.freeBytes).toBeGreaterThan(0);
		const missing = new RuntimeCycleProjector(
			database,
			{ queueDepth: 0 },
			{ agentDir: join(directory, "missing-agent") },
		).project();
		expect(missing.gates).toContain("agent_disk_headroom");

		// A durable pending inbound message projects dispatching and attaches to its origin.
		database.inboundEnqueue({
			messageId: "m1",
			originKey: key,
			originRefJson: JSON.stringify(discordDm),
			body: "hello",
		});
		const dispatching = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(dispatching.phase).toBe("dispatching");
		expect(dispatching.sessions[0].pendingInbound).toBe(1);

		// Binding it as a turn keeps it dispatching; terminal completion returns to idle.
		database.inboundBindTurn({ messageId: "m1", originKey: key, epoch: 0, opRef: "gw-p-m1", sessionId: "s1" });
		const boundCycle = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(boundCycle.phase).toBe("dispatching");
		expect(database.inboundTurnComplete("gw-p-m1")).toBe(1);
		expect(new RuntimeCycleProjector(database, { queueDepth: 0 }).project().phase).toBe("idle");

		// A quarantined memory intent is a gate, not silence.
		database.memoryIntentCreate({ id: "mi1", kind: "daily_capture", payloadJson: "{}" });
		database.memoryIntentQuarantine("mi1", "Error: test quarantine");
		const gated = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(gated.gates).toContain("memory_closure_blocked");
		expect(gated.phase).toBe("degraded");

		// Real-DB age regression (architect blocker): a seconds-vs-ms precedence bug in
		// the census SQL once returned ~-1.8e12 here. The age must be a plausible ms value.
		const ledger = new DeliveryLedger(database);
		const created = Date.now();
		ledger.createPending({
			deliveryId: "age-check",
			turnId: "t-age",
			originKey: key,
			payloadJson: JSON.stringify({ turnId: "t-age", origin: discordDm, role: "assistant", text: "x", final: true }),
		});
		await Bun.sleep(1100);
		const aged = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		const age = aged.sessions[0].oldestUnsettledAgeMs;
		expect(age).toBeNumber();
		expect(age as number).toBeGreaterThanOrEqual(1000);
		expect(age as number).toBeLessThan(Date.now() - created + 5_000);
		expect(aged.deliveries.pending).toBe(1);
	} finally {
		database.close();
	}
});

test("starvation is judged per origin from turn_state: an old accepted trigger is busy, an old unbound row alone is starved", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-starve-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const key = "discord/dm/c1/peer=p1";
		database.putSession(key, "sess-bound-000000000");
		const old = new Date(Date.now() - 20 * 60_000).toISOString();
		// An accepted trigger that has been running for 20 minutes is a long turn, not starvation.
		database.inboundEnqueue({
			messageId: "long",
			originKey: key,
			originRefJson: JSON.stringify(discordDm),
			body: "x",
			receivedAt: old,
		});
		database.inboundBindTurn({ messageId: "long", originKey: key, epoch: 0, opRef: "gw-p-long", sessionId: "s1" });
		database.inboundTurnAccept("gw-p-long");
		// A message queued behind it for 20 minutes is waiting on that turn, not stuck.
		database.inboundEnqueue({
			messageId: "queued",
			originKey: key,
			originRefJson: JSON.stringify(discordDm),
			body: "y",
			receivedAt: old,
		});
		const busy = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(busy.gates).toEqual([]);
		expect(busy.phase).toBe("dispatching");
		expect(busy.inFlightInbound).toBe(1);
		// Another origin with an old unbound row and nothing in flight is stuck.
		const other = { ...discordDm, conversationId: "c2", peerId: "p2" };
		const otherKey = "discord/dm/c2/peer=p2";
		database.putSession(otherKey, "sess-other-000000000");
		database.inboundEnqueue({
			messageId: "stuck",
			originKey: otherKey,
			originRefJson: JSON.stringify(other),
			body: "z",
			receivedAt: old,
		});
		const starved = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(starved.gates).toEqual(["inbound_starved"]);
	} finally {
		database.close();
	}
});

test("monitor authoring loss gates ops.cycle while delivery stays healthy, and clears on recovery (#160)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-monitor-loss-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const at = (minutesAgo: number) => new Date(Date.now() - minutesAgo * 60_000).toISOString();
		const seed = (eventType: string, stage: string, minutesAgo: number, authored = false) => {
			const eventId = crypto.randomUUID();
			database.monitorEventCreate({
				eventId,
				monitorId: "m1",
				eventType,
				payloadJson: "{}",
				firedAt: at(minutesAgo),
			});
			database.monitorEventUpdate(eventId, stage as never, null);
			if (authored) database.authoredOutputCreate(eventId, "note");
			return eventId;
		};
		const project = () => new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		// One lost slot of one type after a delivered one is not yet an outage.
		seed("backlog.watch", "delivered", 120, true);
		seed("backlog.watch", "failed_no_retry", 60);
		expect(project().gates).toEqual([]);
		// Two monitor types whose latest slot was lost pre-author: a cross-type outage.
		seed("memory.canonicalize", "failed_no_retry", 50);
		const crossType = project();
		expect(crossType.gates).toEqual(["monitor_authoring_lost"]);
		expect(crossType.phase).toBe("degraded");
		expect(crossType.monitorAuthoringLost).toEqual([
			{ eventType: "backlog.watch", consecutive: 1, lastFiredAt: expect.any(String) },
			{ eventType: "memory.canonicalize", consecutive: 1, lastFiredAt: expect.any(String) },
		]);
		// The canonicalize type recovers; backlog.watch then loses a second consecutive slot.
		seed("memory.canonicalize", "delivered", 40, true);
		expect(project().gates).toEqual([]);
		seed("backlog.watch", "failed_no_retry", 30);
		const streak = project();
		expect(streak.gates).toEqual(["monitor_authoring_lost"]);
		expect(streak.monitorAuthoringLost).toMatchObject([{ eventType: "backlog.watch", consecutive: 2 }]);
		// A delivered slot of the lost type clears the gate again.
		seed("backlog.watch", "delivered", 10, true);
		expect(project().gates).toEqual([]);
		// Losses older than the observation window age out instead of gating forever.
		seed("retired.a", "failed_no_retry", 25 * 60);
		seed("retired.b", "failed_no_retry", 25 * 60);
		expect(project().gates).toEqual([]);
	} finally {
		database.close();
	}
});

test("issue #189: consecutive failed_no_retry monitor events degrade the projection; one delivery clears it", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-monitor-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const fire = (id: string) =>
			database.monitorEventCreate({
				eventId: id,
				monitorId: "m1",
				eventType: "digest",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
		fire("ok-0");
		database.monitorEventUpdate("ok-0", "delivered");
		for (const id of ["lost-1", "lost-2"]) {
			fire(id);
			expect(database.monitorEventTerminalFail(id, "internal_error", "authoring turn failed")).toBe(true);
		}
		const projector = new RuntimeCycleProjector(database, { queueDepth: 0 });
		expect(database.monitorConsecutiveTerminalFailures()).toBe(2);
		// Two same-type pre-author losses already trip the #160 authoring gate;
		// the #189 dispatch streak stays below its threshold.
		expect(projector.project().gates).toEqual(["monitor_authoring_lost"]);
		fire("lost-3");
		database.monitorEventTerminalFail("lost-3", "internal_error", "authoring turn failed");
		const outage = projector.project();
		expect(outage.gates).toEqual(["monitor_authoring_lost", "monitor_dispatch_failing"]);
		expect(outage.phase).toBe("degraded");
		await Bun.sleep(2);
		fire("ok-4");
		database.monitorEventUpdate("ok-4", "delivered");
		expect(database.monitorConsecutiveTerminalFailures()).toBe(0);
		expect(projector.project().gates).toEqual([]);
		const churning = new RuntimeCycleProjector(database, { queueDepth: 0 }, { brokerRespawnChurn: () => true });
		expect(churning.project().gates).toEqual(["broker_respawn_churn"]);
	} finally {
		database.close();
	}
});

test("ops.cycle verb serves a fresh fail-closed snapshot over the socket", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-e2e-"));
	const config = testConfig(directory);
	const database = await GatewayDatabase.open(config.dbPath);
	const key = "discord/dm/c1/peer=p1";
	// Leave the session mid-rebind so the served projection must gate.
	database.bumpEpoch(key, JSON.stringify(discordDm));
	const ledger = new DeliveryLedger(database);
	ledger.createPending({
		deliveryId: "d1",
		turnId: "t1",
		originKey: key,
		payloadJson: JSON.stringify({ turnId: "t1", origin: discordDm, role: "assistant", text: "hi", final: true }),
	});
	const sessionPort = sessionPortFromResponder({ respond: async () => "mock reply" });
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

	// Minimal negotiated client, mirroring server.test.ts's raw-socket helper.
	const frames: unknown[] = [];
	const socket = Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				for (const line of new TextDecoder().decode(data).split("\n")) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	const conn = await socket;
	const send = (frame: unknown) => conn.write(`${JSON.stringify(frame)}\n`);
	send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	send({ v: "0.1", type: "request", id: "cycle", verb: "ops.cycle" });
	for (let i = 0; i < 100 && frames.length < 2; i++) await Bun.sleep(5);
	conn.end();

	const response = frames.find((f) => (f as { type?: string }).type === "response") as {
		result?: {
			phase?: string;
			gates?: string[];
			sessions?: Array<Record<string, unknown>>;
			deliveries?: Record<string, number>;
		};
	};
	expect(response?.result?.phase).toBe("degraded");
	expect(response?.result?.gates).toContain("stale_session_identity");
	expect(response?.result?.deliveries).toMatchObject({ pending: 1 });
	expect(response?.result?.sessions?.[0]).toMatchObject({ originKey: key, epoch: 1 });
});
