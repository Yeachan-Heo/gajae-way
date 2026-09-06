import { afterEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { BrokerSupervisor, type SpawnFn } from "../src/orchestrator/broker";
import { ChannelQueryError, SessionChannel } from "../src/orchestrator/session-channel";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { createFakeGjc, runFakeGjc } from "./fixtures/fake-gjc.mjs";
import { harness, KEY } from "./red-first-harness";
import { ScriptedSessionPort } from "./session-port.fake";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const close of cleanup.splice(0).reverse()) await close();
});

async function fixture() {
	const home = await mkdtemp(join(tmpdir(), "channel-routing-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const calls: string[][] = [];
	const frames: Array<Record<string, unknown>> = [];
	const logs: string[] = [];
	const fake = createFakeGjc({ modes: "serve:bidirectional,status:content" });
	const cli: CliRunner = async (args) => {
		calls.push([...args]);
		return await runFakeGjc(
			args.filter((arg, index) => arg !== "--agent-dir" && args[index - 1] !== "--agent-dir"),
			fake,
		);
	};
	const children: ReturnType<typeof Bun.spawn>[] = [];
	const spawn = ((options: { cmd: string[]; env?: Record<string, string>; cwd?: string }) => {
		const child = Bun.spawn({
			cmd: [process.execPath, join(import.meta.dir, "fixtures/fake-gjc.mjs"), ...options.cmd.slice(1)],
			stdin: "pipe",
			stdout: "pipe",
			stderr: "pipe",
			env: { ...options.env, GAJAEWAY_FAKE_GJC_MODES: "serve:bidirectional,status:content" },
		});
		children.push(child);
		return child;
	}) as SpawnFn;
	const broker = new BrokerSupervisor({
		home,
		instanceId: "routing",
		cwd: home,
		ssotAgentDir: null,
		command: cli,
		spawn,
		healthProbe: () => true,
		healthIntervalMs: 60_000,
		log: () => {},
	});
	cleanup.push(async () => {
		await broker.stop();
		for (const child of children) {
			child.kill();
			await child.exited;
		}
		database.close();
		await rm(home, { recursive: true, force: true });
	});
	await broker.start();
	const runner = new TailRunner({
		run: broker.cli,
		repo: home,
		stream: (sessionId) => {
			const stream = broker.openStream(sessionId);
			return {
				...stream,
				write(line: string) {
					frames.push(JSON.parse(line));
					return stream.write(line);
				},
			};
		},
	});
	const port = new BrokerSessionPort({
		database,
		cli: broker.cli,
		instanceId: "routing",
		tailRunner: runner,
		transport: "channel",
		log: (line) => logs.push(line),
		bootIncarnation: "boot-test",
	});
	const tail = await port.attachTail({
		sessionId: "stub-session-1",
		brokerGeneration: broker.generation,
		repo: home,
		originKey: "work/routing",
	});
	if (!tail.channel) throw new Error("fixture failed to establish resident channel");
	await tail.channel.checkpoint();
	cleanup.push(async () => tail.close());
	calls.length = 0;
	logs.length = 0;
	return { database, home, calls, logs, frames, port, runner, channel: tail.channel, cli: broker.cli };
}

const turn = { sessionId: "stub-session-1", repo: "/tmp", text: "same prompt", opRef: "gw-routing-1" };

test("warm resident channel routes prompt, controls and reads with zero CLI launches", async () => {
	const f = await fixture();
	expect(f.port.residentHealthy(turn.sessionId)).toBe(true);
	f.database.putSession("work/routing", turn.sessionId);
	const manager = new PersonaSessionManager({
		database: f.database,
		port: f.port,
		repo: f.home,
		instanceId: "routing",
	});
	cleanup.push(async () => manager.stop());
	// rebindModel enters the actor's real #ensureSession path; fresh channel health must replace inspect.
	await manager.rebindModel("work/routing", "test-model");
	const receipt = await f.port.send(turn);
	expect(receipt).toMatchObject({
		sessionId: turn.sessionId,
		operationRef: turn.opRef,
		commandId: "stub-command-1",
		taskKey: "gateway",
	});
	await f.port.steer({ ...turn, clientRef: "gw-steer-1" });
	expect(await f.port.setModel({ ...turn, selection: "test-model" })).toEqual({ changed: true });
	expect(await f.port.setModel({ ...turn, selection: { preset: "coding" } })).toEqual({ changed: true });
	expect(await f.port.setServiceTier({ ...turn, tier: "priority" })).toEqual({ changed: true });
	expect((await f.port.status(turn)).status.status).toBe("terminal_ok");
	expect(await f.port.queueEmpty(turn)).toBe(true);
	expect((await f.port.fetchLastAssistant(turn)).text).toBe("stub reply");
	expect(f.calls).toEqual([]);
	expect(f.logs).toEqual([]);
	expect(f.frames.filter((frame) => frame.op === "turn.prompt")).toHaveLength(1);
	expect(f.port.transportStamp(turn.sessionId)).toEqual({ transport: "channel", cold: false });
});

test("rollback CLI mode preserves receipt shape and terminal status but launches commands", async () => {
	const f = await fixture();
	const channelReceipt = await f.port.send(turn);
	const channelStatus = await f.port.status(turn);
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "cli",
		log: (line) => f.logs.push(line),
		bootIncarnation: "rollback",
	});
	const receipt = await port.send(turn);
	expect(Object.keys(receipt).sort()).toEqual(Object.keys(channelReceipt).sort());
	expect({ ...receipt, acceptedAt: "clock" }).toEqual({ ...channelReceipt, acceptedAt: "clock" });
	expect((await port.status(turn)).status.status).toBe(channelStatus.status.status);
	await port.steer({ ...turn, clientRef: "gw-steer-2" });
	await port.setModel({ ...turn, selection: "model" });
	await port.setServiceTier({ ...turn, tier: "priority" });
	await port.queueEmpty(turn);
	await port.fetchLastAssistant(turn);
	expect(f.calls).toHaveLength(7);
	expect(f.logs).toHaveLength(7);
	expect(f.logs[0]).toBe(
		"cli_launch op=session.send class=warm session=stub-session-1 opRef=gw-routing-1 boot=rollback",
	);
	expect(port.residentHealthy(turn.sessionId)).toBe(false);
});

test("missing channel refuses prompt before model mutation without fallback spawn", async () => {
	const f = await fixture();
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "channel",
		channels: () => undefined,
	});
	await expect(port.send({ ...turn, model: "model" })).rejects.toMatchObject({
		code: "channel_unavailable",
		retryable: true,
	});
	await expect(port.request({ ...turn, observeTail: false })).rejects.toMatchObject({
		code: "channel_unavailable",
		bytesWritten: 0,
	});
	expect(f.calls).toEqual([]);
	expect(port.transportStamp(turn.sessionId)).toEqual({ transport: "channel", cold: true });
	f.channel.close();
	const unhealthy = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "channel",
		channels: () => f.channel,
	});
	await expect(unhealthy.send(turn)).rejects.toMatchObject({ code: "channel_unavailable", bytesWritten: 0 });
	expect(f.calls).toEqual([]);
});

test("ambiguous prompt write reconciles the same clientRef once and never re-prompts", async () => {
	const f = await fixture();
	const requests: Array<Record<string, unknown>> = [];
	let listener: (line: string) => void = () => {};
	const channel = new SessionChannel({
		sessionId: turn.sessionId,
		transport: {
			onLine(sink) {
				listener = sink;
				return () => {};
			},
			write(line) {
				const frame = JSON.parse(line) as Record<string, unknown>;
				requests.push(frame);
				if (frame.op === "turn.prompt") throw new Error("write progress unknown");
				listener(
					JSON.stringify({
						type: "query_response",
						id: frame.id,
						ok: true,
						result: { kind: "prompt", status: "terminal_ok", commandId: "reconciled" },
					}),
				);
			},
		},
	});
	cleanup.push(async () => channel.close());
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "channel",
		channels: () => channel,
	});
	const receipt = await port.send(turn);
	expect(receipt.commandId).toBe("reconciled");
	expect(requests.map((frame) => frame.op ?? frame.query)).toEqual(["turn.prompt", "turn.result"]);
	expect(requests[1]?.input).toEqual({ kind: "prompt", clientRef: turn.opRef });
	expect(f.calls).toEqual([]);
});

test("channel busy refusal is not re-queried or sent through CLI", async () => {
	const f = await fixture();
	const original = f.channel.control.bind(f.channel);
	f.channel.control = async () => {
		throw new ChannelQueryError("full", "channel_busy");
	};
	try {
		await expect(f.port.send(turn)).rejects.toMatchObject({ code: "channel_busy" });
	} finally {
		f.channel.control = original;
	}
	expect(f.calls).toEqual([]);
	expect(f.frames.filter((frame) => frame.op === "turn.prompt" || frame.query === "turn.result")).toEqual([]);
});

test("CLI launch logging distinguishes cold, recovery, probe and boot commands", async () => {
	const f = await fixture();
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "cli",
		log: (line) => f.logs.push(line),
		bootIncarnation: "classes",
	});
	await port.inspect(turn);
	await port.inspect({ ...turn, recovery: true });
	await port.status({ ...turn, recovery: true });
	await port.liveness({ ...turn, sessionId: "probe-session" });
	await port.bind({ originKey: "work/new", epoch: 0, repo: f.home });
	expect(f.logs).toEqual(
		expect.arrayContaining([
			"cli_launch op=session.inspect class=cold session=stub-session-1 opRef=- boot=classes",
			"cli_launch op=session.inspect class=recovery session=stub-session-1 opRef=- boot=classes",
			"cli_launch op=session.status class=recovery session=stub-session-1 opRef=gw-routing-1 boot=classes",
			"cli_launch op=session.inspect class=probe session=probe-session opRef=- boot=classes",
			"cli_launch op=session.create class=boot session=- opRef=- boot=classes",
		]),
	);
	expect(f.logs).toHaveLength(f.calls.length);
});

test.each([false, true])("persona stamps channel transport and cold=%s at bind", async (cold) => {
	class StampedPort extends ScriptedSessionPort {
		transportStamp() {
			return { transport: "channel" as const, cold };
		}
	}
	const port = new StampedPort();
	const h = await harness(port);
	cleanup.push(async () => h.close());
	h.enqueue(`stamp-${cold}`);
	await h.manager.notifyInbound(KEY);
	expect(port.sends).toHaveLength(1);
	const db = new Database(join(h.home, "gateway.db"), { readonly: true });
	try {
		expect(db.query("SELECT transport, cold FROM turn_attempts WHERE op_ref = ?").get(port.sends[0]!.opRef)).toEqual({
			transport: "channel",
			cold: cold ? 1 : 0,
		});
	} finally {
		db.close();
	}
});

test.each([
	"channel_timeout",
	"channel_write_failed",
])("failed reconciliation preserves %s send ambiguity", async (code) => {
	const f = await fixture();
	const control = f.channel.control.bind(f.channel);
	const turnResult = f.channel.turnResult.bind(f.channel);
	const ambiguous = new ChannelQueryError("prompt may have landed", code, 42);
	let prompts = 0;
	const queries: string[] = [];
	f.channel.control = async () => {
		prompts++;
		throw ambiguous;
	};
	f.channel.turnResult = async (ref) => {
		queries.push(ref);
		throw new ChannelQueryError("recovery channel closed", "channel_closed", 0);
	};
	try {
		await expect(f.port.send(turn)).rejects.toBe(ambiguous);
	} finally {
		f.channel.control = control;
		f.channel.turnResult = turnResult;
	}
	expect(prompts).toBe(1);
	expect(queries).toEqual([turn.opRef]);
	expect(f.calls).toEqual([]);
});

test("persona liveness shortcut expires at ten seconds even when the channel remains healthy", async () => {
	const f = await fixture();
	const health = f.channel.health();
	let now = Math.max(health.lastFrameAt, health.lastProbeAt) + 9_999;
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "channel",
		channels: () => f.channel,
		now: () => now,
		log: (line) => f.logs.push(line),
	});
	expect(port.residentHealthy(turn.sessionId)).toBe(true);
	now++;
	expect(port.residentHealthy(turn.sessionId)).toBe(false);
	f.database.putSession("work/stale", turn.sessionId);
	const manager = new PersonaSessionManager({ database: f.database, port, repo: f.home, instanceId: "routing" });
	cleanup.push(async () => manager.stop());
	await manager.rebindModel("work/stale", "test-model");
	expect(f.calls).toHaveLength(1);
	expect(f.calls[0]).toContain("inspect");
});

test("channel application refusals retain CLI error contracts without launching recovery CLI", async () => {
	const f = await fixture();
	const port = new BrokerSessionPort({
		database: f.database,
		cli: f.cli,
		instanceId: "routing",
		tailRunner: f.runner,
		transport: "channel",
		channels: () => f.channel,
	});
	const control = f.channel.control.bind(f.channel);
	const turnResult = f.channel.turnResult.bind(f.channel);
	f.channel.control = async (op) => {
		throw new ChannelQueryError("refused", op === "turn.prompt" ? "client_ref_conflict" : "busy");
	};
	f.channel.turnResult = async () => {
		throw new ChannelQueryError("unavailable", "session_unavailable");
	};
	try {
		await expect(port.send(turn)).rejects.toMatchObject({
			name: "OpRefRejectedError",
			code: "client_ref_conflict",
			opRef: turn.opRef,
		});
		await expect(port.steer({ ...turn, clientRef: "gw-steer-refused" })).rejects.toMatchObject({
			details: { code: "busy" },
		});
		await expect(port.status(turn)).rejects.toMatchObject({ details: { code: "session_unavailable" } });
	} finally {
		f.channel.control = control;
		f.channel.turnResult = turnResult;
	}
	expect(f.calls).toEqual([]);
});
