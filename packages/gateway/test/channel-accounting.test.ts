import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import { BrokerSupervisor } from "../src/orchestrator/broker";
import { SessionChannel } from "../src/orchestrator/session-channel";
import { TailCapacityError, TailRunner } from "../src/orchestrator/tail-runner";
import { brokerStatus, channelCycleGates } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { createFakeGjc, runFakeGjc } from "./fixtures/fake-gjc.mjs";

const fixturePath = join(import.meta.dir, "fixtures/fake-gjc.mjs");

function supervisor(home: string) {
	let processes = 0;
	const broker = new BrokerSupervisor({
		home,
		instanceId: "channel-accounting",
		ssotAgentDir: null,
		log: () => {},
		spawn: ((options: Parameters<typeof Bun.spawn>[0]) => {
			processes++;
			const input = options as unknown as { cmd: string[]; stdin: "ignore" | "pipe" };
			return Bun.spawn({
				cmd: [process.execPath, fixturePath, ...input.cmd.slice(1)],
				cwd: home,
				stdin: input.stdin,
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_FAKE_GJC_MODES: "serve:bidirectional" },
			});
		}) as typeof Bun.spawn,
	});
	return { broker, processes: () => processes };
}

test("real CLI spawns are counted once; resident control/query traffic never changes the count", async () => {
	const home = await mkdtemp(join(tmpdir(), "channel-accounting-"));
	const { broker, processes } = supervisor(home);
	try {
		const boot = broker.cliSpawnsAtBoot;
		for (let i = 0; i < 3; i++) await broker.cli(["sdk", "session", "list"]);
		expect(broker.spawnsSince(boot)).toBe(3);
		expect(processes()).toBe(3);
		const mark = broker.cliSpawnsTotal;
		const stream = broker.openStream("stub-session-1");
		let receive: ((line: string) => void) | undefined;
		const channel = new SessionChannel({
			sessionId: "stub-session-1",
			transport: {
				write: stream.write,
				onLine: (listener) => {
					receive = listener;
					return () => {
						receive = undefined;
					};
				},
			},
		});
		const reading = (async () => {
			for await (const line of stream.lines) receive?.(line);
		})();
		try {
			const receipt = await channel.control("turn.prompt", { text: "accounting", clientRef: "accounting-turn" });
			expect(receipt.ok).toBe(true);
			const result = await channel.turnResult("accounting-turn");
			expect(result?.status).toBe("terminal_ok");
			expect(broker.spawnsSince(mark)).toBe(0);
			expect(processes()).toBe(4);
		} finally {
			channel.close();
			stream.close();
			await reading;
		}
		expect(brokerStatus(broker).cliSpawnsTotal).toBe(3);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("capacity refusal preserves residency ceiling and does not launch CLI work", async () => {
	const home = await mkdtemp(join(tmpdir(), "channel-capacity-accounting-"));
	const { broker } = supervisor(home);
	const fake = createFakeGjc({ modes: "serve:bidirectional" });
	let cliCalls = 0;
	const tails = new TailRunner({
		repo: home,
		maxTailProcesses: 1,
		run: async (args) => {
			cliCalls++;
			return runFakeGjc(args, fake);
		},
		stream: (id) => broker.openStream(id),
		log: () => {},
	});
	const handle = await tails.attach({ sessionId: "stub-session-1", brokerGeneration: 1, repo: home });
	try {
		handle.setTurnRunning(true);
		expect(tails.residentChannels).toBe(1);
		const mark = broker.cliSpawnsTotal;
		const calls = cliCalls;
		await expect(
			tails.attach({ sessionId: "refused", brokerGeneration: 1, repo: home, priority: "retired" }),
		).rejects.toBeInstanceOf(TailCapacityError);
		expect(tails.residentChannels).toBeLessThanOrEqual(1);
		expect(broker.spawnsSince(mark)).toBe(0);
		expect(cliCalls).toBe(calls);
		expect(brokerStatus(broker, tails).residentChannels).toBe(1);
	} finally {
		// I9b residency: close() parks a healthy resident relay; terminateAll tears it down.
		await tails.terminateAll();
		await rm(home, { recursive: true, force: true });
	}
	expect(tails.residentChannels).toBe(0);
});

test("channel fault gate expires from the fault timestamp, not the first projector poll", async () => {
	const home = await mkdtemp(join(tmpdir(), "channel-gate-accounting-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const faultAt = Date.parse("2026-09-06T00:00:00Z");
	const tails = {
		residentChannels: 1,
		channelRestarts: 2,
		channelFaults: 1,
		lastChannelFaultAt: faultAt as number | undefined,
	};
	const projector = new RuntimeCycleProjector(database, { queueDepth: 0 }, (now) => channelCycleGates(tails, now));
	try {
		expect(projector.project(new Date(faultAt + 599_999)).gates).toContain("channel_degraded");
		expect(projector.project(new Date(faultAt + 600_000)).gates).not.toContain("channel_degraded");
		tails.lastChannelFaultAt = faultAt + 600_001;
		tails.channelFaults++;
		expect(projector.project(new Date(faultAt + 600_002)).gates).toContain("channel_degraded");
		expect(brokerStatus(undefined, tails)).toMatchObject({ channelRestarts: 2, channelFaults: 2, residentChannels: 1 });
		tails.lastChannelFaultAt = undefined;
		expect(projector.project(new Date(faultAt + 600_003)).gates).not.toContain("channel_degraded");
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});
