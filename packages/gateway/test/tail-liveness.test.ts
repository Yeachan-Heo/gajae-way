import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "tail-liveness" } as const;
const ORIGIN_KEY = "loopback/loopback/tail-liveness";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("persona state stays running until a terminal tail event is injected", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-tail-state-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort();
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

		// While the op is in flight, tail activity keeps the actor running and no
		// terminal fires: the tail is the progress source.
		port.emitActivity(send.sessionId, { toolCalls: 1, outputTokens: 9 });
		await Bun.sleep(0);
		expect(manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(database.inboundTurnRows(batch.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });

		// I4b: a decidable-terminal `turn.result` witness with content completes the
		// turn on the reconcile that observes it - the tail no longer gates completion.
		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "witness terminal");
		await manager.tick(ORIGIN_KEY);
		await eventually(() => manager.state(ORIGIN_KEY) === "idle", "terminal witness did not end the actor turn");
		expect(database.inboundTurnRows(batch.opRef)[0]).toMatchObject({ state: "done", turn_state: "done" });
	} finally {
		await manager.stop();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("tail runner alarms exactly at the stall threshold, diagnoses unknown kinds, and records authenticated compaction receipts", async () => {
	let now = 0;
	let wakePoll!: () => void;
	const stalls: number[] = [];
	const logs: string[] = [];
	const run: CliRunner = async () => ({
		exitCode: 0,
		stdout: JSON.stringify({
			ok: true,
			result: { items: [{ kind: "compaction_observed", payload: { trigger: "native_auto" } }], terminal: false },
		}),
		stderr: "",
	});
	const runner = new TailRunner({
		run,
		repo: "/tmp/tail-liveness",
		stallTimeoutMs: 120_000,
		pollIntervalMs: 1,
		now: () => now,
		sleep: () =>
			new Promise<void>((resolve) => {
				wakePoll = resolve;
			}),
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
		wakePoll();
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
		port.emitActivity(send.sessionId, { toolCalls: 3, outputTokens: 77 });
		await eventually(
			() =>
				frames.some(
					(frame) =>
						frame.event === "chat.progress" && frame.payload.toolCalls === 3 && frame.payload.outputTokens === 77,
				),
			"tail activity did not become progress",
		);
		port.complete(send.opRef, "done");
	} finally {
		socket?.end();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});
