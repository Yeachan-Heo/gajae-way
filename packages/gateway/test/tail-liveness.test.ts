import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { SessionRelayStream } from "../src/orchestrator/broker";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "tail-liveness" } as const;
const ORIGIN_KEY = "loopback/loopback/tail-liveness";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

class FakeRelay implements SessionRelayStream {
	readonly lines: AsyncIterable<string>;
	readonly #queue: Array<string | null> = [];
	readonly #waiters: Array<(line: string | null) => void> = [];

	constructor() {
		this.lines = {
			[Symbol.asyncIterator]: () => ({
				next: async () => {
					const line = await new Promise<string | null>((resolve) => {
						const queued = this.#queue.shift();
						if (queued !== undefined) resolve(queued);
						else this.#waiters.push(resolve);
					});
					return line === null ? { done: true, value: undefined } : { done: false, value: line };
				},
			}),
		};
	}

	write(line: string): void {
		if (JSON.parse(line).type === "hello")
			this.host({ type: "hello", protocolVersion: 3, connectionId: "tail-liveness" });
	}

	host(frame: Record<string, unknown>): void {
		this.#push(JSON.stringify(frame));
	}

	close(): void {
		this.#push(null);
	}

	#push(line: string | null): void {
		const waiter = this.#waiters.shift();
		if (waiter) waiter(line);
		else this.#queue.push(line);
	}
}

test("persona state stays running until a terminal tail event is injected", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-tail-state-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "tail-state",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	try {
		expect(
			database.inboundEnqueue({
				messageId: "m-1",
				originKey: ORIGIN_KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "wait for tail evidence",
			}),
		).toBe(true);
		await manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "persistent send did not start");
		const send = port.sends[0]!;
		const batch = database.inboundNonterminalTurns(ORIGIN_KEY)[0]!;

		// Status can become terminal before a tail arrives, but that fact alone is
		// intentionally not a persona state transition.
		const status = port.status.bind(port);
		port.status = async (input) => ({
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState: "present" },
			summaryCompleted: true,
		});
		await manager.tick(ORIGIN_KEY);
		port.status = status;
		expect(manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(database.inboundTurnRows(batch.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });

		port.emitTool(send.sessionId, { toolName: "read" });
		await Bun.sleep(0);
		expect(manager.state(ORIGIN_KEY)).toBe("turn-running");
		port.complete(send.opRef, "terminal tail evidence");
		await eventually(() => manager.state(ORIGIN_KEY) === "idle", "agent_end tail frame did not end the actor turn");
		expect(database.inboundTurnRows(batch.opRef)[0]).toMatchObject({ state: "done", turn_state: "done" });
	} finally {
		await manager.stop();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("tail runner alarms exactly at the stall threshold, diagnoses unknown kinds, and records authenticated compaction receipts", async () => {
	let now = 0;
	const stalls: number[] = [];
	const logs: string[] = [];
	const relay = new FakeRelay();
	const runner = new TailRunner({
		stream: () => relay,
		repo: "/tmp/tail-liveness",
		stallTimeoutMs: 120_000,
		now: () => now,
		log: (line) => logs.push(line),
	});
	const tail = await runner.attach({
		sessionId: "tail-session",
		brokerGeneration: 1,
		repo: "/tmp/tail-liveness",
		originKey: ORIGIN_KEY,
		onStall: ({ elapsedMs }) => {
			stalls.push(elapsedMs);
		},
		onDiagnostic: (line) => logs.push(line),
	});
	try {
		tail.beginTurn("op-1", { commandId: "cmd-1", turnId: "turn-1" });
		relay.host({
			type: "event",
			kind: "compaction_observed",
			commandId: "cmd-1",
			turnId: "turn-1",
			payload: { event_type: "compaction_observed", event: { trigger: "native_auto" } },
		});
		await eventually(
			() => logs.includes("unknown_runtime_event session=tail-session kind=compaction_observed"),
			"unknown relay event was not diagnosed",
		);
		expect(logs).toContain("unknown_runtime_event session=tail-session kind=compaction_observed");
		runner.recordCompactionReceipt({ sessionId: "tail-session", originKey: ORIGIN_KEY, result: { started: true } });
		expect(logs).toContain(
			`compaction_event sessionId=tail-session originKey=${ORIGIN_KEY} source=control_receipt result=started`,
		);
		tail.setTurnRunning(true);
		now = 119_999;
		runner.checkStalls(now);
		expect(stalls).toEqual([]);
		now = 120_000;
		runner.checkStalls(now);
		expect(stalls).toEqual([120_000]);
		runner.checkStalls(now + 1);
		expect(stalls).toEqual([120_000]);
	} finally {
		await tail.close();
	}
});

test("streaming message_update deltas are progress, not unrecognized runtime events", async () => {
	const logs: string[] = [];
	const relay = new FakeRelay();
	const runner = new TailRunner({
		stream: () => relay,
		repo: "/tmp/tail-progress-kind",
		stallTimeoutMs: 120_000,
		now: () => 0,
		log: (line) => logs.push(line),
	});
	const tail = await runner.attach({
		sessionId: "tail-session",
		brokerGeneration: 1,
		repo: "/tmp/tail-progress-kind",
		originKey: ORIGIN_KEY,
		onStall: () => {},
		onDiagnostic: (line) => logs.push(line),
	});
	try {
		tail.beginTurn("op-1", { commandId: "cmd-1", turnId: "turn-1" });
		for (let index = 0; index < 20; index++)
			relay.host({
				type: "event",
				kind: "message_update",
				commandId: "cmd-1",
				turnId: "turn-1",
				payload: { event_type: "message_update", event: { delta: `chunk-${index}` } },
			});
		relay.host({
			type: "event",
			kind: "compaction_observed",
			commandId: "cmd-1",
			turnId: "turn-1",
			payload: { event_type: "compaction_observed", event: { trigger: "native_auto" } },
		});
		await eventually(
			() => logs.includes("unknown_runtime_event session=tail-session kind=compaction_observed"),
			"a genuinely unrecognized relay event must still be diagnosed",
		);
		expect(logs.filter((line) => line.includes("kind=message_update"))).toEqual([]);
		expect(logs.filter((line) => line.startsWith("unknown_runtime_event"))).toEqual([
			"unknown_runtime_event session=tail-session kind=compaction_observed",
		]);
	} finally {
		await tail.close();
	}
});

test("chat.progress is emitted only from observed tail activity and preserves tail counters", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-tail-progress-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
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
		await eventually(() => frames.length >= 1, "gateway negotiation did not complete");
		socket.write(
			`${JSON.stringify({
				v: "0.1",
				type: "request",
				id: "turn",
				verb: "chat.send",
				params: { origin: ORIGIN, messageId: "m-progress", text: "show tail-derived progress" },
			})}\n`,
		);
		await eventually(() => port.sends.length === 1, "persistent turn did not start");
		await Bun.sleep(30);
		expect(frames.filter((frame) => frame.event === "chat.progress")).toEqual([]);

		const send = port.sends[0]!;
		for (let call = 0; call < 3; call++) port.emitTool(send.sessionId, { toolName: "read" });
		port.emitAssistant(send.sessionId, "x".repeat(308));
		await eventually(
			() =>
				frames.some(
					(frame) =>
						frame.event === "chat.progress" && frame.payload.toolCalls === 3 && frame.payload.outputTokens === 77,
				),
			"tail activity did not become progress",
		);
		port.complete(send.opRef, "done");
		await eventually(() => database.inboundPendingCount(ORIGIN_KEY) === 0, "completed turn did not settle");
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});
