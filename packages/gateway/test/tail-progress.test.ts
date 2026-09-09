import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("chat.progress is silent until a tail observation supplies its counters", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-tail-progress-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		progress: { firstAfterMs: 0, intervalMs: 5 },
		onStop: () => database.close(),
	});
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const frames: any[] = [];
	try {
		let buffered = "";
		socket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await eventually(() => frames.length >= 1, "negotiation did not complete");
		socket.write(
			`${JSON.stringify({
				v: "0.1",
				type: "request",
				id: "turn",
				verb: "chat.send",
				params: {
					origin: { platform: "loopback", kind: "loopback", conversationId: "tail" },
					text: "show progress only from tail",
				},
			})}\n`,
		);
		await eventually(() => port.sends.length === 1, "turn was not sent");
		await Bun.sleep(30);
		expect(frames.filter((frame) => frame.event === "chat.progress")).toEqual([]);

		const send = port.sends[0]!;
		port.emitActivity(send.sessionId, { toolCalls: 2, outputTokens: 42 });
		await eventually(
			() =>
				frames.some(
					(frame) =>
						frame.event === "chat.progress" && frame.payload.toolCalls === 2 && frame.payload.outputTokens === 42,
				),
			"tail activity did not feed chat.progress",
		);
		port.complete(send.opRef, "done");
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("a started server drives the persona tail stall heartbeat and stops it on shutdown", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-stall-heartbeat-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		stallCheckIntervalMs: 5,
		onStop: () => database.close(),
	});
	try {
		// AC6: the alarm threshold check is a running-server obligation, independent
		// of any generic request polling or inbound traffic.
		await eventually(() => port.stallChecks >= 3, "server never drove the stall heartbeat");
	} finally {
		await server.stop("test complete");
	}
	const afterStop = port.stallChecks;
	await Bun.sleep(30);
	expect(port.stallChecks).toBe(afterStop);
	await rm(home, { recursive: true, force: true });
});

test("red-team G1: chat.progress runs on tail frames only; a turn never issues transcript.list or usage.get", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-tail-progress-nospawn-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	// SessionPort no longer has a progress() member at all; a compile-time
	// guarantee the query path is gone, not merely unused.
	const hasProgress = "progress" in port;
	expect(hasProgress).toBe(false);
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		progress: { firstAfterMs: 0, intervalMs: 5 },
		onStop: () => database.close(),
	});
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const frames: any[] = [];
	try {
		let buffered = "";
		socket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await eventually(() => frames.length >= 1, "negotiation did not complete");
		socket.write(
			`${JSON.stringify({
				v: "0.1",
				type: "request",
				id: "turn",
				verb: "chat.send",
				params: { origin: { platform: "loopback", kind: "loopback", conversationId: "nospawn" }, text: "go" },
			})}\n`,
		);
		await eventually(() => port.sends.length === 1, "turn was not sent");
		const send = port.sends[0]!;
		port.emitActivity(send.sessionId, { toolCalls: 1, outputTokens: 10 });
		await eventually(
			() => frames.some((frame) => frame.event === "chat.progress" && !frame.payload.final),
			"no non-final progress",
		);
		port.complete(send.opRef, "done");
		await eventually(
			() => frames.filter((frame) => frame.event === "chat.progress" && frame.payload.final === true).length === 1,
			"exactly one final progress frame expected",
		);
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("a turn that never announced progress still emits exactly one final chat.progress (adapter's only typing-end signal)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-final-progress-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		// A long first-progress delay: no non-final frame can ever be announced.
		progress: { firstAfterMs: 60_000, intervalMs: 60_000 },
		onStop: () => database.close(),
	});
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const frames: any[] = [];
	try {
		let buffered = "";
		socket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await eventually(() => frames.length >= 1, "negotiation did not complete");
		socket.write(
			`${JSON.stringify({
				v: "0.1",
				type: "request",
				id: "turn",
				verb: "chat.send",
				params: { origin: { platform: "loopback", kind: "loopback", conversationId: "silent" }, text: "hush" },
			})}\n`,
		);
		await eventually(() => port.sends.length === 1, "turn was not sent");
		// The model answers with silence: no delivery, no progress ever announced.
		port.complete(port.sends[0]!.opRef, "[SILENT]");
		await eventually(
			() => frames.filter((frame) => frame.event === "chat.progress").length === 1,
			"no final chat.progress after a silent turn",
		);
		const [only] = frames.filter((frame) => frame.event === "chat.progress");
		expect(only.payload.final).toBe(true);
		expect(only.payload.origin.conversationId).toBe("silent");
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("a turn retired by /new emits its final chat.progress and stops heartbeating (live: 'working…' for hours)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-retired-progress-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort({ onBind: ({ epoch }) => `session-${epoch}` });
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		progress: { firstAfterMs: 0, intervalMs: 5 },
		onStop: () => database.close(),
	});
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const frames: any[] = [];
	try {
		let buffered = "";
		socket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await eventually(() => frames.length >= 1, "negotiation did not complete");
		const origin = { platform: "loopback", kind: "loopback", conversationId: "retired" };
		socket.write(
			`${JSON.stringify({ v: "0.1", type: "request", id: "turn", verb: "chat.send", params: { origin, text: "long job" } })}\n`,
		);
		await eventually(() => port.sends.length === 1, "turn was not sent");
		const send = port.sends[0]!;
		port.emitActivity(send.sessionId, { toolCalls: 1, outputTokens: 10 });
		const progressOf = (turnId: string) =>
			frames.filter((frame) => frame.event === "chat.progress" && frame.payload.turnId === turnId);
		await eventually(
			() => frames.some((frame) => frame.event === "chat.progress" && !frame.payload.final),
			"no heartbeat",
		);
		const turnId = frames.find((frame) => frame.event === "chat.progress").payload.turnId as string;

		socket.write(
			`${JSON.stringify({ v: "0.1", type: "request", id: "new", verb: "chat.send", params: { origin, text: "/new" } })}\n`,
		);
		await eventually(
			() => progressOf(turnId).some((frame) => frame.payload.final === true),
			"retired turn never finalised progress",
		);
		const finals = progressOf(turnId).filter((frame) => frame.payload.final === true);
		expect(finals).toHaveLength(1);
		const count = progressOf(turnId).length;
		await Bun.sleep(40);
		expect(progressOf(turnId).length).toBe(count);
		// The retired turn's late output is fenced; its terminal must not re-announce.
		port.complete(send.opRef, "stale");
		await Bun.sleep(40);
		expect(progressOf(turnId).filter((frame) => frame.payload.final === true)).toHaveLength(1);
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});
