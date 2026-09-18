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
		database.memoryIntentUpdate("mi1", "quarantined");
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
