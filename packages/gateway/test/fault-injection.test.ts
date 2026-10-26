/**
 * #689 fault-injection matrix. Each case injects the fault at the session-port
 * boundary the gateway actually observes (a thrown attach/send, a broker
 * generation change, a host that stops being live, a send that times out), and
 * asserts that the origin answers again within a bound and that no possibly
 * accepted operation is ever submitted twice. The "gateway crash between bind
 * and send" row lives in send-intent-recovery.test.ts (every pre-send await).
 *
 * Process-level faults (a real SIGKILL of a session host or broker) are not
 * reproduced here; they reach the gateway as exactly these port observations.
 */
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { BrokerSessionPort, type SessionSendInput } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import {
	BROKER_PROMPT_BUSY_REFUSAL,
	type BrokerAuthority,
	isBrokerPromptBusyRefusal,
} from "../src/store/groups/bindings";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	ScriptedSessionPort,
	scriptedRelay,
} from "./session-port.fake";

const MINUTE = 60_000;
/** Gateway time a recoverable fault may take before its origin answers again (retry and grace timers). */
const RECOVERY_BUDGET_MS = 5_000;
let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function until(check: () => boolean, attempts = 400): Promise<void> {
	for (let i = 0; i < attempts && !check(); i++) await new Promise((resolve) => setTimeout(resolve, 5));
	expect(check()).toBe(true);
}

type Timer = { work: () => void; due: number };

/**
 * Gateway time owned by the test: timers fire at their absolute due time.
 * `elapse(budgetMs)` is ONE recovery deadline: it fires every timer due before
 * it in due order (including timers those callbacks schedule), advances the
 * clock as they fire, and leaves later timers pending. A retry chain whose
 * delays add up past the budget therefore does not recover within it.
 */
function virtualTime(start = Date.now()) {
	let clock = start;
	const timers: Timer[] = [];
	return {
		now: () => clock,
		advance: (ms: number) => {
			clock += ms;
		},
		setTimeout: (work: () => void, delay: number) => {
			const timer = { work, due: clock + delay };
			timers.push(timer);
			return timer;
		},
		clearTimeout: (timer: unknown) => {
			const i = timers.indexOf(timer as Timer);
			if (i >= 0) timers.splice(i, 1);
		},
		elapse: (budgetMs: number) => {
			const deadline = clock + budgetMs;
			fireUntil(deadline);
			clock = deadline;
		},
		/** Fires timers due by `deadline`, leaving the clock at the last one fired, so one deadline spans several calls. */
		fireUntil: (deadline: number) => fireUntil(deadline),
	};
	function fireUntil(deadline: number): void {
		for (;;) {
			const next = timers.filter((timer) => timer.due <= deadline).sort((a, b) => a.due - b.due)[0];
			if (!next) return;
			timers.splice(timers.indexOf(next), 1);
			clock = Math.max(clock, next.due);
			next.work();
		}
	}
}

/** One persona origin over a scripted host; the test owns time and the retry timers. */
async function origin(name: string, port: ScriptedSessionPort, options: { unresolvedHoldMs?: number } = {}) {
	home = await mkdtemp(join(tmpdir(), `gajaeway-fault-${name}-`));
	database = await Store.open(join(home, "gateway.db"));
	const db = database;
	const originKey = `loopback/loopback/${name}`;
	const originRefJson = JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: name });
	const repo = join(home, "workspace");
	const time = virtualTime();
	const logs: string[] = [];
	const answers: Array<{ opRef: string; text: string }> = [];
	let generation = 0;
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "fault",
		repo,
		now: time.now,
		brokerGeneration: () => generation,
		...(options.unresolvedHoldMs ? { unresolvedHoldMs: options.unresolvedHoldMs } : {}),
		log: (line: string) => logs.push(line),
		setTimeout: time.setTimeout,
		clearTimeout: time.clearTimeout,
		onTurnStart: ({ trigger, turn }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				answers.push({ opRef: turn.opRef, text });
			},
		}),
	});
	const enqueue = (messageId: string, body: string) =>
		expect(
			db.transaction((tx) =>
				db.inbox.enqueue(tx, {
					messageId,
					originKey,
					originRefJson,
					body,
					receivedAt: new Date(time.now()).toISOString(),
				}),
			),
		).toBe(true);
	return {
		db,
		manager,
		originKey,
		logs,
		answers,
		enqueue,
		elapse: time.elapse,
		bumpGeneration: async () => {
			generation += 1;
			await manager.onBrokerGeneration(generation);
		},
		advance: time.advance,
	};
}

/** Every attempt under one op-ref; a possibly accepted op must never have more than one. */
function attemptsByOpRef(port: ScriptedSessionPort): Map<string, number> {
	const counts = new Map<string, number>();
	for (const send of port.sendAttempts) counts.set(send.opRef, (counts.get(send.opRef) ?? 0) + 1);
	return counts;
}

test("broker restart mid-attach: the never-sent trigger is answered once after the broker returns (#689)", async () => {
	let brokerDown = true;
	const port = new (class extends ScriptedSessionPort {
		override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
			if (brokerDown) throw new Error("broker_unavailable (client stopped or broker unavailable)");
			return await super.attachTail(input);
		}
	})({ onSend: (input, scripted) => scripted.complete(input.opRef, "answer") });
	const f = await origin("broker-mid-attach", port);
	f.enqueue("m-1", "answer me");
	try {
		await f.manager.recover();
		expect(port.sendAttempts).toHaveLength(0);
		// The broker restarts: a new generation, then the retry timer fires.
		brokerDown = false;
		await f.bumpGeneration();
		f.elapse(RECOVERY_BUDGET_MS);
		await until(() => f.answers.length === 1);
		expect(port.sendAttempts).toHaveLength(1);
		expect(f.answers[0]!.text).toBe("answer");
	} finally {
		await f.manager.stop();
	}
});

test("broker restart mid-send: an accepted prompt whose receipt was lost is reconciled, never resent (#689)", async () => {
	let tearReceipt = true;
	const port = new (class extends ScriptedSessionPort {
		override async send(input: SessionSendInput) {
			const receipt = await super.send(input);
			if (tearReceipt) {
				tearReceipt = false;
				throw new Error("relay closed: broker exited before the receipt");
			}
			return receipt;
		}
	})();
	const f = await origin("broker-mid-send", port);
	f.enqueue("m-1", "answer me");
	try {
		await f.manager.recover();
		const opRef = port.sendAttempts[0]!.opRef;
		expect(f.db.inbox.turnRow(opRef)?.turn_state).not.toBe("done");
		await f.bumpGeneration();
		await f.manager.recover();
		f.elapse(RECOVERY_BUDGET_MS);
		// The original op finishes on the host; the gateway learns it from status.
		port.complete(opRef, "late answer");
		await f.manager.reconcile(f.originKey);
		await until(() => f.answers.length === 1);
		expect(f.answers[0]).toEqual({ opRef, text: "late answer" });
		expect([...attemptsByOpRef(port).values()]).toEqual([1]);
	} finally {
		await f.manager.stop();
	}
});

test("host death mid-turn: past the hold bound the origin answers new messages, the held op is never resent (#689)", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			if (!input.text.includes("held")) scripted.complete(input.opRef, "fresh answer");
		},
	});
	const f = await origin("host-death-mid-turn", port, { unresolvedHoldMs: 10 * MINUTE });
	f.enqueue("m-1", "held request");
	try {
		await f.manager.recover();
		const held = port.sendAttempts[0]!;
		// The host is killed while the turn runs: no longer live, its operation unreadable.
		port.setSessionState(held.sessionId, { live: false });
		port.forgetOperation(held.opRef);
		f.enqueue("m-2", "newer message");
		await f.manager.reconcile(f.originKey);
		expect(port.sendAttempts).toHaveLength(1);
		f.advance(11 * MINUTE);
		await f.manager.reconcile(f.originKey);
		await until(() => f.answers.length === 1);
		const after = port.sendAttempts.slice(1);
		expect(after).toHaveLength(1);
		expect(after[0]!.text).toContain("newer message");
		expect(after[0]!.text).not.toContain("held request");
		expect(attemptsByOpRef(port).get(held.opRef)).toBe(1);
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(true);
	} finally {
		await f.manager.stop();
	}
});

/**
 * A host that refuses as `busy` while the gateway waits, with the refusal
 * journaled in broker_operations exactly as BrokerSessionPort records it, and
 * read back from that journal (never from test memory).
 */
class JournaledBusyPort extends ScriptedSessionPort {
	authority!: BrokerAuthority;
	store!: Store;
	busy: "hang" | undefined;

	override async send(input: SessionSendInput) {
		if (this.busy === undefined) return await super.send(input);
		this.sendAttempts.push(input);
		const scope = JSON.stringify([input.sessionId, input.opRef]);
		this.store.transaction((tx) => {
			this.store.bindings.beginBrokerOperation(tx, this.authority, {
				operation: "turn.prompt",
				scope,
				operationRef: input.opRef,
				requestJson: "{}",
			});
			this.store.bindings.completeBrokerOperation(
				tx,
				this.authority,
				"turn.prompt",
				scope,
				input.opRef,
				JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL),
			);
		});
		return await new Promise<never>(() => {});
	}

	promptRefusal(input: { sessionId: string; repo: string; opRef: string }): "busy" | undefined {
		const intent = this.store.bindings.brokerOperation(
			this.authority,
			"turn.prompt",
			JSON.stringify([input.sessionId, input.opRef]),
		);
		return intent?.operationRef === input.opRef && isBrokerPromptBusyRefusal(intent) ? "busy" : undefined;
	}
}

async function journaledOrigin(name: string, port: JournaledBusyPort) {
	const f = await origin(name, port);
	port.store = f.db;
	port.authority = initializeTestBrokerAuthority(f.db, join(home, "agent"));
	attachTestBrokerOwnership(f.db, port, join(home, "agent"));
	return f;
}

test("host death during busy-wait: the real busy loop keeps the refusal and the trigger is answered once within the budget (#689)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-fault-busy-host-death-"));
	database = await Store.open(join(home, "gateway.db"));
	const db = database;
	const agentDir = join(home, "agent");
	const authority = initializeTestBrokerAuthority(db, agentDir);
	const repo = join(home, "workspace");
	const sessionId = "sdk-1";
	const originKey = "loopback/loopback/busy-host-death";
	await createOwnedSessionFixture(db, authority, { sessionId, originKey, epoch: 0, repo });
	await mkdir(join(agentDir, "sessions", "bucket"), { recursive: true });
	await writeFile(join(agentDir, "sessions", "bucket", "2026-10-26T00-00-00_sdk-1.jsonl"), '{"type":"session"}\n');
	const prompts: string[] = [];
	const answered = { commandId: "command-2", turnId: "turn-2" };
	let finished = false;
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.result") {
			const done = finished && request.input.clientRef === prompts[1];
			return {
				ok: true,
				result: done
					? {
							status: "terminal_ok",
							...answered,
							clientRef: request.input.clientRef,
							startedAt: Date.now() - 1_000,
							terminalAt: Date.now(),
							receiptState: "present",
							outcome: { reason: "end_turn" },
						}
					: { status: "in_flight", ...answered },
			};
		}
		if (request.operation !== "turn.prompt") return { ok: true, result: {} };
		prompts.push(String(request.input.clientRef));
		return prompts.length === 1
			? { ok: false, error: { code: "busy", message: "host busy" } }
			: { ok: true, result: { accepted: true, receipt: answered } };
	});
	let hostLive = true;
	const cli: CliRunner = async (args) => {
		if (args.join(" ") !== `sdk session inspect ${sessionId}`) throw new Error(`unexpected CLI ${args.join(" ")}`);
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				result: { session: { live: hostLive, activity: { state: "active", at: 1_000 } } },
			}),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		database: db,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		// The host process exits while the gateway sleeps out its busy refusal.
		sleep: async () => {
			hostLive = false;
			relay.streams.at(-1)?.close();
		},
	});
	const time = virtualTime();
	const logs: string[] = [];
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "fault",
		repo,
		now: time.now,
		log: (line: string) => logs.push(line),
		setTimeout: time.setTimeout,
		clearTimeout: time.clearTimeout,
		onTurnStart: ({ trigger, turn }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				answers.push({ opRef: turn.opRef, text });
			},
		}),
	});
	const answers: Array<{ opRef: string; text: string }> = [];
	db.transaction((tx) =>
		db.inbox.enqueue(tx, {
			messageId: "m-1",
			originKey,
			originRefJson: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "busy-host-death" }),
			body: "answer me",
			receivedAt: new Date().toISOString(),
		}),
	);
	try {
		await manager.recover();
		const refused = prompts[0]!;
		// The retry never reached the host, so the journal still proved non-admission
		// when the actor read it (the refusal is released with the requeued attempt).
		expect(prompts).toEqual([refused]);
		expect(db.inbox.turnRow(refused)).toBeUndefined();
		expect(db.inbox.pendingOldest(originKey)?.message_id).toBe("m-1");
		expect(
			logs.some((line) => line.startsWith("send_unadmitted_requeue") && line.includes("prompt_busy_refused")),
		).toBe(true);
		// The host is back. From here the whole recovery shares ONE deadline.
		hostLive = true;
		const deadline = time.now() + RECOVERY_BUDGET_MS;
		time.fireUntil(deadline);
		await until(() => prompts.length === 2);
		expect(prompts[1]).not.toBe(refused);
		// The resubmitted turn answers and ends on the relay that submitted it.
		const host = relay.streams.at(-1)!;
		host.host({
			type: "event",
			kind: "message_end",
			...answered,
			payload: {
				event: { message: { id: "message-2", role: "assistant", content: [{ type: "text", text: "the answer" }] } },
			},
		});
		finished = true;
		host.host({ type: "agent_end", ...answered });
		for (let i = 0; i < 40 && answers.length === 0; i++) {
			time.fireUntil(deadline);
			await new Promise((resolve) => setTimeout(resolve, 5));
		}
		expect(answers).toEqual([{ opRef: prompts[1]!, text: "the answer" }]);
		expect(time.now()).toBeLessThanOrEqual(deadline);
		expect(new Set(prompts).size).toBe(2);
		expect(prompts).toHaveLength(2);
	} finally {
		await manager.stop();
	}
});

test("gateway crash during the busy wait: restart requeues from the journaled refusal alone (#685, #689)", async () => {
	const port = new JournaledBusyPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "answer") });
	port.busy = "hang";
	const f = await journaledOrigin("crash-busy-wait", port);
	f.enqueue("m-1", "answer me");
	void f.manager.recover();
	await until(() => port.sendAttempts.length === 1);
	const refused = port.sendAttempts[0]!.opRef;
	expect(f.db.inbox.sendAttempted(refused)).toBe(true);
	// The process dies mid-wait. A new process sees only the database.
	port.busy = undefined;
	const restarted = new PersonaSessionManager({
		database: f.db,
		port,
		instanceId: "fault",
		repo: join(home, "workspace"),
		log: (line: string) => f.logs.push(line),
		onTurnStart: ({ trigger, turn }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				f.answers.push({ opRef: turn.opRef, text });
			},
		}),
	});
	try {
		await restarted.recover();
		await until(() => f.answers.length === 1);
		expect(f.answers[0]!.opRef).not.toBe(refused);
		expect(
			f.logs.some(
				(line) =>
					line.startsWith("recovery_requeue_unsent") &&
					line.includes(`opRef=${refused}`) &&
					line.includes("prompt_busy_refused"),
			),
		).toBe(true);
		expect([...attemptsByOpRef(port).values()]).toEqual([1, 1]);
	} finally {
		await restarted.stop();
	}
});

for (const outcome of ["host_finishes", "host_dies"] as const) {
	test(`relay timeout during host compaction (${outcome}): the ambiguous op is never resent and the origin answers again (#689)`, async () => {
		let timeoutNext = true;
		const port = new (class extends ScriptedSessionPort {
			override async send(input: SessionSendInput) {
				if (!timeoutNext) return await super.send(input);
				timeoutNext = false;
				// The host was compacting: it took the prompt, but the relay gave up first.
				this.seedAcceptedSend(input);
				throw new Error("relay request timed out after 30000ms (turn.prompt)");
			}
		})({ onSend: (input, scripted) => scripted.complete(input.opRef, "fresh answer") });
		const f = await origin(`compaction-timeout-${outcome}`, port, { unresolvedHoldMs: 10 * MINUTE });
		f.enqueue("m-1", "request during compaction");
		try {
			await f.manager.recover();
			const ambiguous = port.sendAttempts[0]!;
			expect(f.db.inbox.turnRow(ambiguous.opRef)?.turn_state).not.toBe("done");
			if (outcome === "host_finishes") {
				port.complete(ambiguous.opRef, "answer after compaction");
				await f.manager.reconcile(f.originKey);
				// The tail grace for terminal evidence elapses; status then settles the turn.
				f.elapse(RECOVERY_BUDGET_MS);
				await until(() => f.answers.length === 1);
				expect(f.answers[0]).toEqual({ opRef: ambiguous.opRef, text: "answer after compaction" });
			} else {
				port.setSessionState(ambiguous.sessionId, { live: false });
				port.forgetOperation(ambiguous.opRef);
				f.enqueue("m-2", "newer message");
				f.advance(11 * MINUTE);
				await f.manager.reconcile(f.originKey);
				await until(() => f.answers.length === 1);
				// Exactly one new prompt: the newer message, never the ambiguous request under a fresh op-ref.
				const after = port.sendAttempts.slice(1);
				expect(after).toHaveLength(1);
				expect(after[0]!.text).toContain("newer message");
				expect(after[0]!.text).not.toContain("request during compaction");
				expect(f.answers[0]!.opRef).toBe(after[0]!.opRef);
				expect(f.db.inbox.turnRow(ambiguous.opRef)?.turn_state).not.toBe("done");
			}
			expect(attemptsByOpRef(port).get(ambiguous.opRef)).toBe(1);
		} finally {
			await f.manager.stop();
		}
	});
}
