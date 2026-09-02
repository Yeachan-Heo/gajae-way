import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { DeliveryService } from "../src/delivery/delivery";
import { type GatewayServer, type LocalGatewayPort, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
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

async function boot(options: { shutdown?: (reason: string) => Promise<void>; seedUndelivered?: number } = {}) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-local-port-"));
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
	const delivery = new DeliveryService(new DeliveryLedger(db));
	const seeded: string[] = [];
	for (let i = 0; i < (options.seedUndelivered ?? 0); i++) {
		const payload = delivery.prepare(
			crypto.randomUUID(),
			{ platform: "loopback", kind: "loopback", conversationId: "loopback" },
			`replay ${i}`,
		) as { deliveryId: string };
		seeded.push(payload.deliveryId);
	}
	const sessionPort = sessionPortFromResponder({ respond: async () => "mock reply" });
	server = await startUnixServer({
		config,
		database: db,
		sessionPort,
		onStop: () => db.close(),
		shutdown: options.shutdown,
	});
	return { server, config, db, seeded };
}

function chatMessages(port: LocalGatewayPort): Array<Record<string, unknown>> {
	const seen: Array<Record<string, unknown>> = [];
	port.on("chat.message", (payload) => seen.push(payload as Record<string, unknown>));
	return seen;
}

test("replay reaches handlers registered before open() and open() reports the count", async () => {
	const { server, seeded } = await boot({ seedUndelivered: 2 });
	const port = server.attach("test");
	const seen = chatMessages(port);
	const { replayed } = await port.open();
	expect(replayed).toBe(2);
	expect(seen.map((m) => m.deliveryId).sort()).toEqual([...seeded].sort());
	expect(seen.every((m) => m.redelivered === true)).toBe(true);
});

test("a port with no handler at open() drops replay; a later handler sees only later events", async () => {
	const { server } = await boot({ seedUndelivered: 1 });
	const port = server.attach("late");
	const { replayed } = await port.open();
	expect(replayed).toBe(1);
	const seen = chatMessages(port);
	expect(seen).toHaveLength(0);
});

test("open() twice throws; request() before open and after close rejects", async () => {
	const { server } = await boot();
	const port = server.attach("states");
	await expect(port.request("gateway.status")).rejects.toThrow("local port states not open");
	await port.open();
	await expect(port.open()).rejects.toThrow("already open");
	port.close();
	port.close();
	await expect(port.request("gateway.status")).rejects.toThrow("local port states closed");
});

test("request('gateway.status') resolves in-process and results are cloned values", async () => {
	const { server } = await boot();
	const port = server.attach("status");
	await port.open();
	const a = await port.request<{ pid: number; capabilities: string[] }>("gateway.status");
	const b = await port.request<{ pid: number; capabilities: string[] }>("gateway.status");
	expect(a.pid).toBe(process.pid);
	expect(a).not.toBe(b);
	expect(a.capabilities).not.toBe(b.capabilities);
});

test("undefined params round-trip as an omitted property (wire semantics)", async () => {
	const { server } = await boot();
	const port = server.attach("params");
	await port.open();
	// gateway.status ignores params; the point is that admission does not throw on undefined.
	await expect(port.request("gateway.status", undefined)).resolves.toBeDefined();
	await expect(port.request("session.list")).resolves.toBeDefined();
});

test("a throwing handler is contained: sibling handler and a second port still receive the event", async () => {
	const { server } = await boot({ seedUndelivered: 1 });
	const first = server.attach("first");
	const second = server.attach("second");
	const sibling: unknown[] = [];
	const other: unknown[] = [];
	first.on("chat.message", () => {
		throw new Error("boom");
	});
	first.on("chat.message", (p) => sibling.push(p));
	second.on("chat.message", (p) => other.push(p));
	await first.open();
	await second.open();
	expect(sibling).toHaveLength(1);
	expect(other).toHaveLength(1);
});

test("protocol errors surface as rejections with the wire error code", async () => {
	const { server } = await boot();
	const port = server.attach("errors");
	await port.open();
	await expect(port.request("no.such.verb")).rejects.toMatchObject({ code: expect.any(String) });
});

test("gateway.shutdown routes through the injected shutdown; gateway stops only when it calls stop()", async () => {
	let calls: string[] = [];
	let release!: () => void;
	const gate = new Promise<void>((r) => {
		release = r;
	});
	let stopper: (() => Promise<void>) | undefined;
	const { server, config } = await boot({
		shutdown: async (reason) => {
			calls.push(reason);
			await gate;
			await stopper?.();
		},
	});
	stopper = () => server.stop("composite");
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: { data() {} },
	});
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await Bun.sleep(50);
	socket.write(`${JSON.stringify({ v: "0.1", type: "request", id: "sd", verb: "gateway.shutdown" })}\n`);
	await Bun.sleep(100);
	expect(calls).toEqual(["gateway.shutdown verb"]);
	// Gateway still serves: the injected owner has not stopped it yet.
	const probe = server.attach("probe");
	await probe.open();
	expect((await probe.request<{ pid: number }>("gateway.status")).pid).toBe(process.pid);
	release();
	await Bun.sleep(100);
	socket.end();
	calls = [];
});

test("close() rejects callers but admitted server work still drains on stop()", async () => {
	let release!: () => void;
	const gate = new Promise<void>((r) => {
		release = r;
	});
	directory = await mkdtemp(join(tmpdir(), "gajaeway-local-port-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		settleWindowMs: 0,
	};
	const db = await GatewayDatabase.open(config.dbPath);
	database = db;
	let turnsStarted = 0;
	const sessionPort = sessionPortFromResponder({
		respond: async () => {
			turnsStarted++;
			await gate;
			return "late reply";
		},
	});
	server = await startUnixServer({ config, database: db, sessionPort, onStop: () => db.close() });
	const port = server.attach("drain");
	await port.open();
	const sent = await port.request<{ turnId: string | null }>("chat.send", {
		origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" },
		text: "hello",
	});
	expect(sent.turnId).not.toBeNull();
	for (let i = 0; i < 100 && turnsStarted === 0; i++) await Bun.sleep(10);
	expect(turnsStarted).toBe(1);
	// A request that was already admitted resolves even if the port closes afterwards;
	// a request issued after close() is rejected. Neither cancels server-side work.
	const admitted = port.request("gateway.status");
	port.close();
	await expect(admitted).resolves.toBeDefined();
	await expect(port.request("gateway.status")).rejects.toThrow("closed");
	// The turn is still running server-side; stop() must drain it, not hang on the closed port.
	const stopping = server.stop();
	release();
	await stopping;
});

test("owner /restart routes through the injected composite restart owner instead of exiting the process", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-local-port-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: "owner" } },
	};
	const db = await GatewayDatabase.open(config.dbPath);
	database = db;
	const restarts: string[] = [];
	const exits: number[] = [];
	server = await startUnixServer({
		config,
		database: db,
		sessionPort: sessionPortFromResponder({ respond: async () => "unused" }),
		onStop: () => db.close(),
		exitProcess: (code) => exits.push(code),
		restart: async (reason) => {
			restarts.push(reason);
		},
	});
	const port = server.attach("owner");
	await port.open();
	const sent = await port.request<{ engaged: boolean }>("chat.send", {
		origin: { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: "owner" },
		text: "/restart",
		engagement: { mentioned: false, group: false, authorId: "owner" },
	});
	expect(sent.engaged).toBe(true);
	for (let i = 0; i < 300 && restarts.length === 0; i++) await Bun.sleep(10);
	expect(restarts).toEqual(["owner /restart"]);
	// The gateway did not stop itself and did not exit: the composite owner decides.
	expect(exits).toEqual([]);
	expect((await port.request<{ pid: number }>("gateway.status")).pid).toBe(process.pid);
});
