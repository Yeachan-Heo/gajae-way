import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(socketPath: string): Promise<{ send(value: unknown): void; frames: any[]; close(): void }> {
	const frames: any[] = [];
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
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function waitFor(frames: any[], count: number): Promise<void> {
	for (let attempt = 0; attempt < 100 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

test("requires negotiation then serves status, shutdown, and validates chat params", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => "mock reply",
	};
	server = await startUnixServer({
		config,
		database,
		gjc,
		startedAt: "2026-01-01T00:00:00.000Z",
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "request", id: "before", verb: "gateway.status" });
	await waitFor(client.frames, 1);
	expect(client.frames[0].error.code).toBe("negotiation_required");
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 2);
	expect(client.frames[1].type).toBe("negotiated");
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	await waitFor(client.frames, 3);
	expect(client.frames[2].result.schemaVersion).toBe(3);
	expect(client.frames[2].result.startedAt).toBe("2026-01-01T00:00:00.000Z");
	client.send({
		v: "0.1",
		type: "request",
		id: "bad-chat",
		verb: "chat.send",
		params: { origin: { platform: "discord", kind: "channel", conversationId: "x" }, text: "hello" },
	});
	await waitFor(client.frames, 4);
	expect(client.frames[3].error.code).toBe("invalid_params");
	client.send({
		v: "0.1",
		type: "request",
		id: "chat",
		verb: "chat.send",
		params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "hello" },
	});
	await waitFor(client.frames, 6);
	expect(client.frames[4].result.turnId).toBeString();
	expect(client.frames[5].payload).toMatchObject({ text: "mock reply", final: true });
	client.send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
	await waitFor(client.frames, 8);
	expect(client.frames[6]).toMatchObject({ type: "response", id: "shutdown", result: { stopping: true } });
	expect(client.frames[7]).toMatchObject({ type: "event", event: "gateway.stopping" });
	client.close();
});
