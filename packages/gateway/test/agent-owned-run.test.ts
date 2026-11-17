import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { testOnlyBrokerDependencies } from "../src/orchestrator/test-broker";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { initializeTestBrokerAuthority } from "./session-port.fake";

const ORIGIN = { platform: "telegram", kind: "dm", conversationId: "8390645913", peerId: "8390645913" } as const;
const KEY = originKey(ORIGIN);
let home = "";
let server: GatewayServer | undefined;
let broker: GlobalGjcClient | undefined;
let database: Store | undefined;
const restore: Array<() => void> = [];
afterEach(async () => {
	await server?.stop();
	await broker?.stop();
	database?.close();
	for (const undo of restore.splice(0)) undo();
	if (home) await rm(home, { recursive: true, force: true });
	server = undefined;
	broker = undefined;
	database = undefined;
	home = "";
});
async function until(predicate: () => boolean) {
	for (let n = 0; n < 900 && !predicate(); n++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}
async function fixture(jobs = 0, noticeReply = "비교표", maxTailProcesses = 128) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-agent-run-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const host = testOnlyBrokerDependencies({
		reply: (prompt) => (prompt.includes("[Gateway notice]") ? noticeReply : "stub reply"),
	});
	broker = new GlobalGjcClient({ ...host, agentDir: config.gjc.agentDir, cwd: home });
	await broker.start();
	database = await Store.open(config.dbPath);
	const db = database;
	const authority = initializeTestBrokerAuthority(db, broker.agentDir);
	const tailRunner = new TailRunner({
		stream: (sessionId) => broker!.openStream(sessionId),
		repo: join(home, "workspace"),
		sleep: async () => {},
		maxTailProcesses,
	});
	const port = new BrokerSessionPort({
		database: db,
		authority,
		cli: broker.cli,
		tailRunner,
	});
	const logs: string[] = [];
	for (const level of ["info", "warn", "error"] as const) {
		const spy = spyOn(console, level).mockImplementation((...args: unknown[]) => {
			logs.push(args.join(" "));
		});
		restore.push(() => spy.mockRestore());
	}
	let send = (_value: unknown) => {};
	const start = async () => {
		// gjc host/broker intentionally survives gateway shutdown/restart.
		server = await startUnixServer({ config, database: db, sessionPort: port });
		const socket = await Bun.connect({ unix: config.socketPath, socket: { data() {} } });
		restore.push(() => socket.end());
		send = (value) => {
			socket.write(`${JSON.stringify(value)}\n`);
		};
		send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await Bun.sleep(10);
	};
	await start();
	const chat = (id: string, conversationId: string = ORIGIN.conversationId) =>
		send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: { ...ORIGIN, conversationId },
				text: id,
				engagement: { mentioned: false, group: false, authorId: ORIGIN.peerId },
			},
		});
	chat("seed");
	await until(() => !!db.bindings.getSessionRecord(KEY)?.sessionId);
	const sessionId = db.bindings.getSessionRecord(KEY)!.sessionId;
	host.setRunningJobs(sessionId, jobs);
	await until(
		() =>
			db.inbox.nonterminalTurns(KEY).length === 0 && logs.some((line) => line.startsWith("agent_run_observer_open ")),
	);
	const notices = () =>
		db.core
			.groupDb("inbox")
			.prepare(
				"SELECT * FROM inbound_messages WHERE origin_key = ? AND message_id LIKE 'agent-run/%' ORDER BY received_at",
			)
			.all(KEY) as Array<{ message_id: string; source: string; body: string; turn_state: string }>;
	const replies = () => db.outbox.rows().filter((row) => JSON.parse(row.payload_json).text === noticeReply);
	const restart = async () => {
		await server!.stop();
		server = undefined;
		await start();
	};
	const wake = (options?: Parameters<typeof host.endAgentRun>[1]) => {
		host.startAgentRun(sessionId);
		host.endAgentRun(sessionId, options);
	};
	return {
		host,
		sessionId,
		logs,
		db,
		port,
		chat,
		notices,
		replies,
		restart,
		wake,
		tailRunner,
	};
}

test("autonomous end with jobs=0 enqueues one internal report and re-prompts through normal delivery", async () => {
	const f = await fixture();
	f.wake();
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
	expect(f.notices()[0]!.source).toBe("lane_report");
	expect(f.notices()[0]!.body).toContain(
		"Text you wrote in those runs was NOT delivered, except replies you gave to user messages",
	);
	expect(f.replies()[0]!.origin_key).toBe(KEY);
	expect(f.replies()[0]!.delivery_id.startsWith("gw-ar-")).toBe(false);
	const prompt = f.host.submitted.find((entry) => entry.text.includes("[Gateway notice]"))!.text;
	expect(prompt).not.toContain("from your work lane");
	expect(prompt).toContain("send it now in full; otherwise reply [SILENT]");
	expect(f.logs.filter((line) => line.startsWith("agent_run_autonomous_end "))).toHaveLength(1);
});

test("an autonomous end queued during settlement survives runningJobs already being empty", async () => {
	const f = await fixture();
	const status = f.port.status.bind(f.port);
	let injected = false;
	f.port.status = async (input) => {
		const report = await status(input);
		if (!injected && report.status.status === "terminal_ok") {
			injected = true;
			f.wake();
		}
		return report;
	};
	f.chat("job-completed-during-settle");
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

test("two ends with more jobs defer, then the last end produces one notice", async () => {
	const f = await fixture(2);
	f.wake();
	f.wake();
	await until(() => f.logs.some((line) => line.startsWith("agent_run_notice_deferred ")));
	expect(f.notices()).toHaveLength(0);
	f.host.setRunningJobs(f.sessionId, 0);
	f.wake();
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
}, 10_000);

test("positioned-only autonomous failed end also re-prompts", async () => {
	const f = await fixture();
	f.wake({ positionedOnly: true, failed: true });
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

test("raw and positioned correlated ends from another relay never enqueue a notice", async () => {
	const f = await fixture();
	f.host.endAgentRun(f.sessionId, { correlation: { commandId: "foreign", turnId: "foreign-turn" } });
	f.host.endAgentRun(f.sessionId, {
		positionedOnly: true,
		correlation: { commandId: "foreign-2", turnId: "foreign-turn-2" },
	});
	f.chat("successor-gateway-turn");
	await until(() => f.host.submitted.length === 2 && f.db.inbox.nonterminalTurns(KEY).length === 0);
	await Bun.sleep(3_100);
	expect(f.notices()).toHaveLength(0);
	expect(f.logs.filter((line) => line.startsWith("agent_run_autonomous_end "))).toHaveLength(0);
});

test("restart with a live host and jobs re-arms observation then delivers a notice reply", async () => {
	const f = await fixture(1);
	await f.restart();
	await until(() => f.logs.some((line) => line.includes("agent_run_observer_open") && line.includes("reason=recover")));
	f.host.setRunningJobs(f.sessionId, 0);
	f.wake();
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

test("observer transport loss warns and reopened relay still detects an autonomous end", async () => {
	const f = await fixture();
	f.host.loseRelays(f.sessionId);
	await until(() => f.logs.some((line) => line.startsWith("agent_run_observer_lost ")));
	await until(() => f.host.relayCount(f.sessionId) === 1);
	f.wake();
	await until(() => f.replies().length === 1);
});

test("unavailable job report permits a quiescent notice instead of suppressing it", async () => {
	const f = await fixture();
	f.host.setJobsUnavailable(f.sessionId, true);
	f.wake();
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

test("an undispatched internal notice coalesces the next autonomous completion", async () => {
	const f = await fixture();
	f.db.transaction((tx) =>
		f.db.inbox.enqueue(tx, {
			messageId: `agent-run/${f.sessionId}/existing`,
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "[Gateway notice] existing notice",
			source: "lane_report",
		}),
	);
	f.wake();
	await until(() => f.logs.some((line) => line.startsWith("agent_run_notice_coalesced ")));
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

for (const method of ["permission.request", "ui.ask"])
	test(`an idle observer explicitly refuses ${method} rather than leaving the autonomous tool blocked`, async () => {
		const f = await fixture();
		f.host.startAgentRun(f.sessionId);
		const response = await f.host.requestReverse(f.sessionId, method);
		expect(response).toMatchObject({
			type: "reverse_response",
			ok: false,
			error: { code: "unavailable", message: "no bound turn for this reverse request" },
		});
		f.host.endAgentRun(f.sessionId);
		await until(() => f.replies().length === 1);
	});

test("an autonomous run joined by a gateway prompt ends correlated but still schedules one notice", async () => {
	const f = await fixture();
	f.host.startAgentRun(f.sessionId);
	await Bun.sleep(10);
	f.chat("user-joins-autonomous-run");
	await until(() => f.host.submitted.length >= 2 && f.db.inbox.nonterminalTurns(KEY).length === 0);
	// The owner-only normal answer is delivered, but the pre-join output still
	// needs a notice despite the now-correlated terminal from the gateway turn.
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
	expect(f.notices()[0]!.body).toContain("except replies you gave to user messages");
	expect(f.logs.filter((line) => line.startsWith("agent_run_autonomous_end "))).toHaveLength(1);
});

test("bare uncorrelated end without an autonomous start is attributed to nobody and never re-prompts", async () => {
	const f = await fixture();
	f.host.endAgentRun(f.sessionId);
	await until(() => f.logs.some((line) => line.startsWith("agent_run_unattributed_end ")));
	await Bun.sleep(3_100);
	expect(f.notices()).toHaveLength(0);
});

test("restart recovers a jobs-done but still-working autonomous wake", async () => {
	const f = await fixture(0);
	f.host.startAgentRun(f.sessionId);
	await f.restart();
	await until(() => f.logs.some((line) => line.includes("agent_run_observer_open") && line.includes("reason=recover")));
	f.host.endAgentRun(f.sessionId);
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
});

test("one real tail slot permits a same-origin successor during an active autonomous run", async () => {
	const f = await fixture(0, "비교표", 1);
	f.host.startAgentRun(f.sessionId);
	await Bun.sleep(10);
	f.chat("successor-at-tail-capacity");
	f.host.endAgentRun(f.sessionId);
	await until(() => f.host.submitted.length >= 2);
	await until(() => f.replies().length === 1);
	expect(f.host.relayCount(f.sessionId)).toBe(1);
	await server!.stop();
	server = undefined;
});

test("a complete autonomous run received during delayed successor preparation produces exactly one notice", async () => {
	const f = await fixture(0, "비교표", 1);
	let entered = () => {};
	let release = () => {};
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const resume = new Promise<void>((resolve) => {
		release = resolve;
	});
	// Delay a real pre-send port operation while lifecycle frames are received
	// on the retained connection and admitted behind the dispatch mailbox item.
	const usage = f.port.contextUsage!.bind(f.port);
	f.port.contextUsage = async (input) => {
		entered();
		await resume;
		return usage(input);
	};
	f.chat("delayed-successor");
	await started;
	f.wake();
	await Bun.sleep(10);
	release();
	await until(() => f.replies().length === 1);
	expect(f.notices()).toHaveLength(1);
	expect(f.logs.filter((line) => line.startsWith("agent_run_autonomous_end "))).toHaveLength(1);
});

test("late predecessor content queued during successor preparation cannot deliver or terminate the successor", async () => {
	const f = await fixture(0, "비교표", 1);
	let entered = () => {};
	let release = () => {};
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const resume = new Promise<void>((resolve) => {
		release = resolve;
	});
	const usage = f.port.contextUsage!.bind(f.port);
	f.port.contextUsage = async (input) => {
		entered();
		await resume;
		return usage(input);
	};
	f.chat("successor-with-late-predecessor");
	await started;
	f.host.replayTurnFrames(f.sessionId, "late predecessor must never reach chat");
	await Bun.sleep(10);
	release();
	await until(() => f.host.submitted.length === 2 && f.db.inbox.nonterminalTurns(KEY).length === 0);
	expect(f.db.outbox.rows().map((row) => JSON.parse(row.payload_json).text)).toEqual(["stub reply", "stub reply"]);
	expect(f.notices()).toHaveLength(0);
});

test("predecessor frames after beginTurn but before the successor receipt are never adopted", async () => {
	const f = await fixture(0, "비교표", 1);
	let entered = () => {};
	let release = () => {};
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const resume = new Promise<void>((resolve) => {
		release = resolve;
	});
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		entered();
		await resume;
		return send(input);
	};
	f.chat("successor-with-delayed-receipt");
	await started;
	f.host.replayTurnFrames(f.sessionId, "late prior correlation");
	await Bun.sleep(10);
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.db.inbox.nonterminalTurns(KEY)).toHaveLength(1);
	release();
	await until(() => f.host.submitted.length === 2 && f.db.inbox.nonterminalTurns(KEY).length === 0);
	expect(f.db.outbox.rows().map((row) => JSON.parse(row.payload_json).text)).toEqual(["stub reply", "stub reply"]);
});

test("a cross-origin tail waiter already blocked before settlement dispatches when the owner becomes an observer", async () => {
	const f = await fixture(0, "비교표", 1);
	let entered = () => {};
	let release = () => {};
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const resume = new Promise<void>((resolve) => {
		release = resolve;
	});
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		const receipt = await send(input);
		if (input.sessionId === f.sessionId) {
			entered();
			await resume;
		}
		return receipt;
	};
	f.chat("hold-A-in-flight");
	await started;
	f.chat("B-awaits-tail", "cross-origin-B");
	await until(() => f.logs.some((line) => line.startsWith("tail_saturation ")));
	expect(f.host.submitted).toHaveLength(2);
	expect(f.tailRunner.activeCount).toBe(1);
	release();
	await until(() => f.host.submitted.length === 3);
	await until(() => f.db.outbox.rows().length === 3);
	expect(f.logs.some((line) => line.startsWith("agent_run_observer_lost ") && line.includes("reason=evicted"))).toBe(
		true,
	);
	expect(f.tailRunner.activeCount).toBe(1);
	await server!.stop();
	server = undefined;
	expect(f.tailRunner.activeCount).toBe(0);
});

test("a SILENT re-prompt answer does not invent any delivery", async () => {
	const f = await fixture(0, "[SILENT]");
	f.wake();
	await until(() => f.notices()[0]?.turn_state === "done");
	expect(f.replies()).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(1);
});
