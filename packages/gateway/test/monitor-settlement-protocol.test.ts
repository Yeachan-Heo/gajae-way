import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromResponder } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;
let database: GatewayDatabase | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function startWithMonitor(): Promise<{
	send: (value: unknown) => void;
	frames: Array<Record<string, unknown>>;
	close: () => void;
	db: GatewayDatabase;
	monitorId: string;
	batchId: string;
	eventIds: [string, string];
	deliveryId: string;
}> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-settle-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const db = await GatewayDatabase.open(config.dbPath);
	database = db;
	const sessionPort = sessionPortFromResponder({ respond: async () => "mock reply" });
	server = await startUnixServer({ config, database: db, sessionPort, onStop: () => db.close() });
	const client = await connect(config.socketPath);
	// Seed a monitor batch with two authored events directly through the store:
	// the handler wiring under test is delivery settlement, not authoring.
	const registry = (await import("../src/monitors/registry")).MonitorRegistry;
	const monitors = new registry(db);
	const monitor = monitors.add({
		name: "settle",
		trigger: { kind: "cron", schedule: "30 6 * * *" },
		eventTypes: ["memory.canonicalize"],
		enabled: true,
	});
	const batchId = crypto.randomUUID();
	const eventIds: [string, string] = [crypto.randomUUID(), crypto.randomUUID()];
	for (const eventId of eventIds) {
		db.monitorEventCreate({
			eventId,
			monitorId: monitor.monitorId,
			eventType: "memory.canonicalize",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
		db.monitorEventUpdate(eventId, "authored", batchId);
	}
	// A prepared ledger delivery whose turn_id is the monitor batch id — exactly
	// what the real dispatch path produces.
	const { DeliveryService } = await import("../src/delivery/delivery");
	const { DeliveryLedger } = await import("../src/store/ledger");
	const delivery = new DeliveryService(new DeliveryLedger(db));
	const payload = delivery.prepare(
		batchId,
		{ platform: "loopback", kind: "loopback", conversationId: "loopback" },
		"monitor note",
	);
	const deliveryId = (payload as { deliveryId: string }).deliveryId;
	return { ...client, db, monitorId: monitor.monitorId, batchId, eventIds, deliveryId };
}

async function connect(socketPath: string) {
	const frames: Array<Record<string, unknown>> = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return {
		send: (value: unknown) => socket.write(`${JSON.stringify(value)}\n`),
		frames,
		close: () => socket.end(),
	};
}

async function response(frames: Array<Record<string, unknown>>, id: string): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 400; attempt++) {
		const frame = frames.find((f) => f.id === id && f.type === "response");
		if (frame) return frame;
		await Bun.sleep(5);
	}
	throw new Error(`no response for ${id}`);
}

test("delivery.confirm on a monitor batch advances authored events to delivered (server protocol)", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await response(ctx.frames, "__negotiated__").catch(() => undefined);
	ctx.send({
		v: "0.1",
		type: "request",
		id: "confirm",
		verb: "delivery.confirm",
		params: { deliveryId: ctx.deliveryId },
	});
	const res = await response(ctx.frames, "confirm");
	expect(res.result).toEqual({ settled: true });
	const stages = ctx.db.monitorEventRows().filter((row) => ctx.eventIds.includes(row.event_id));
	// The REAL handler settled both events — not a manual DB update.
	expect(stages.map((row) => row.stage)).toEqual(["delivered", "delivered"]);
});

test("late delivery.fail cannot regress a delivered monitor event (server protocol)", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	ctx.send({
		v: "0.1",
		type: "request",
		id: "confirm",
		verb: "delivery.confirm",
		params: { deliveryId: ctx.deliveryId },
	});
	await response(ctx.frames, "confirm");
	// A late, out-of-order fail for the same delivery id (e.g. a duplicate
	// adapter retry after confirmation):
	ctx.send({
		v: "0.1",
		type: "request",
		id: "fail",
		verb: "delivery.fail",
		params: { deliveryId: ctx.deliveryId, reason: "adapter flake", ambiguous: true },
	});
	await response(ctx.frames, "fail");
	const stages = ctx.db.monitorEventRows().filter((row) => ctx.eventIds.includes(row.event_id));
	// Monotonic: delivered stays delivered.
	expect(stages.map((row) => row.stage)).toEqual(["delivered", "delivered"]);
});

test("RT-29 duplicate confirm on confirmed keeps monitor events delivered", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	ctx.send({ v: "0.1", type: "request", id: "c1", verb: "delivery.confirm", params: { deliveryId: ctx.deliveryId } });
	await response(ctx.frames, "c1");
	expect(
		ctx.db
			.monitorEventRows()
			.filter((row) => ctx.eventIds.includes(row.event_id))
			.map((row) => row.stage),
	).toEqual(["delivered", "delivered"]);
	// Duplicate confirm (idempotent ack):
	ctx.send({ v: "0.1", type: "request", id: "c2", verb: "delivery.confirm", params: { deliveryId: ctx.deliveryId } });
	const dup = await response(ctx.frames, "c2");
	expect(dup.result).toEqual({ settled: true });
	expect(
		ctx.db
			.monitorEventRows()
			.filter((row) => ctx.eventIds.includes(row.event_id))
			.map((row) => row.stage),
	).toEqual(["delivered", "delivered"]);
});

test("delivery.fail before confirmation keeps authored events authored (server protocol)", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	ctx.send({
		v: "0.1",
		type: "request",
		id: "fail",
		verb: "delivery.fail",
		params: { deliveryId: ctx.deliveryId, reason: "adapter down", ambiguous: false },
	});
	await response(ctx.frames, "fail");
	const stages = ctx.db.monitorEventRows().filter((row) => ctx.eventIds.includes(row.event_id));
	// Distinguishable, not delivered, not silently dropped.
	expect(stages.map((row) => row.stage)).toEqual(["authored", "authored"]);
	expect(ctx.db.deliveryRows().find((row) => row.delivery_id === ctx.deliveryId)?.state).toBe("pending");
});

test("RT-29 ledger monotonicity: late fail after confirmed is a no-op on the ledger row", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	ctx.send({
		v: "0.1",
		type: "request",
		id: "confirm",
		verb: "delivery.confirm",
		params: { deliveryId: ctx.deliveryId },
	});
	await response(ctx.frames, "confirm");
	expect(ctx.db.deliveryRows().find((row) => row.delivery_id === ctx.deliveryId)?.state).toBe("confirmed");
	// Adapter retries a stale failure AFTER the confirm landed:
	ctx.send({
		v: "0.1",
		type: "request",
		id: "late-fail",
		verb: "delivery.fail",
		params: { deliveryId: ctx.deliveryId, reason: "stale retry", ambiguous: true },
	});
	const lateFail = await response(ctx.frames, "late-fail");
	// Terminal duplicate gets an idempotent SUCCESS ack (not invalid_params)...
	expect(lateFail.result).toEqual({ recorded: true });
	const row = ctx.db.deliveryRows().find((row) => row.delivery_id === ctx.deliveryId);
	// ...while the terminal confirmed state is never rewritten.
	expect(row?.state).toBe("confirmed");
	// And the monitor events stay delivered (already covered by the other test).
});

test("RT-29 ledger monotonicity: expired row cannot be resurrected by a late confirm", async () => {
	const ctx = await startWithMonitor();
	ctx.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	// Three non-ambiguous fails expire the delivery (3-attempt policy).
	for (const id of ["f1", "f2", "f3"]) {
		ctx.send({
			v: "0.1",
			type: "request",
			id,
			verb: "delivery.fail",
			params: { deliveryId: ctx.deliveryId, reason: "adapter down", ambiguous: false },
		});
		await response(ctx.frames, id);
	}
	expect(ctx.db.deliveryRows().find((row) => row.delivery_id === ctx.deliveryId)?.state).toBe("expired");
	// A late confirm cannot resurrect an expired delivery:
	ctx.send({
		v: "0.1",
		type: "request",
		id: "late-confirm",
		verb: "delivery.confirm",
		params: { deliveryId: ctx.deliveryId },
	});
	const lateConfirm = await response(ctx.frames, "late-confirm");
	// Expired is terminal: idempotent success ack, state unchanged.
	expect(lateConfirm.result).toEqual({ settled: true });
	expect(ctx.db.deliveryRows().find((row) => row.delivery_id === ctx.deliveryId)?.state).toBe("expired");
	// Round-4 blocker 3: the late confirm on an EXPIRED delivery must NOT settle
	// the batch's monitor events to delivered — they stay authored.
	const stagesAfter = ctx.db.monitorEventRows().filter((row) => ctx.eventIds.includes(row.event_id));
	expect(stagesAfter.map((row) => row.stage)).toEqual(["authored", "authored"]);
	// An UNKNOWN delivery id is still invalid_params (an error frame):
	ctx.send({ v: "0.1", type: "request", id: "unknown-id", verb: "delivery.confirm", params: { deliveryId: "nope" } });
	for (let attempt = 0; attempt < 400; attempt++) {
		const frame = ctx.frames.find((f) => f.id === "unknown-id");
		if (frame) {
			expect(frame.type).toBe("error");
			expect((frame as { error: { code: string } }).error.code).toBe("invalid_params");
			break;
		}
		await Bun.sleep(5);
	}
});
