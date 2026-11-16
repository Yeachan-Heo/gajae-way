import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DEFAULT_SESSIONS_BUSY_STALL_MS, DEFAULT_SESSIONS_UNRESOLVED_HOLD_MS } from "../src/config";
import {
	type PersonaBindHoldInput,
	PersonaSessionManager,
	personaTurnOpRef,
	UNRESOLVED_HOLD_ESCALATED,
} from "../src/orchestrator/persona-session";
import { RelayClosedError } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { ScriptedSessionPort, type ScriptedTailHandle } from "./session-port.fake";

let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function until(check: () => boolean): Promise<void> {
	for (let i = 0; i < 400 && !check(); i++) await new Promise((resolve) => setTimeout(resolve, 5));
	expect(check()).toBe(true);
}

/** A host whose status reads can be forced to `unknown`, as after an endpoint loss. */
class HoldPort extends ScriptedSessionPort {
	readonly unknownOps = new Set<string>();

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (this.unknownOps.has(input.opRef))
			return { operationRef: input.opRef, status: { status: "unknown" as const }, summaryCompleted: false };
		return await super.status(input);
	}
}

const MINUTE = 60_000;

/** Timers the manager schedules, fired only when the test advances virtual time. */
class VirtualClock {
	#now = Date.now();
	#timers = new Set<{ at: number; work: () => void }>();

	managerOptions() {
		return {
			now: () => this.#now,
			setTimeout: (work: () => void, delayMs: number) => {
				const timer = { at: this.#now + delayMs, work };
				this.#timers.add(timer);
				return timer;
			},
			clearTimeout: (timer: unknown) => {
				this.#timers.delete(timer as { at: number; work: () => void });
			},
		};
	}

	/** Advances to `untilMs`, firing every due timer in order and letting its async work run. */
	async advance(byMs: number): Promise<void> {
		const until = this.#now + byMs;
		for (;;) {
			const due = [...this.#timers].filter((timer) => timer.at <= until).sort((a, b) => a.at - b.at)[0];
			if (!due) break;
			this.#timers.delete(due);
			this.#now = Math.max(this.#now, due.at);
			due.work();
			for (let i = 0; i < 20; i++) await new Promise((resolve) => setTimeout(resolve, 0));
		}
		this.#now = until;
	}
}

/** The broker's answer for a session id it no longer serves. */
function endpointStale(): Error {
	return Object.assign(new Error("endpoint_stale: session is not live"), { code: "endpoint_stale" });
}

async function heldOrigin(
	name: string,
	options: { heldForMs: number; live: boolean; reuseHome?: boolean; newerMessage?: boolean; clock?: VirtualClock },
) {
	if (!options.reuseHome) home = await mkdtemp(join(tmpdir(), "gajaeway-unresolved-hold-"));
	database = await Store.open(join(home, "gateway.db"));
	const db = database;
	const port = new HoldPort({
		onBind: (input) => `session-${input.epoch}`,
		onSend: (input, scripted) => scripted.complete(input.opRef, `answer to ${input.opRef}`),
	});
	const originKey = `loopback/loopback/${name}`;
	const originRefJson = JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: name });
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey, epoch: 0, repo });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, originKey, binding.sessionId, 0))).toBe(true);
	const enqueue = (messageId: string, body: string, receivedAt: string) =>
		expect(
			db.transaction((tx) => db.inbox.enqueue(tx, { messageId, originKey, originRefJson, body, receivedAt })),
		).toBe(true);
	const sentAt = new Date(Date.now() - options.heldForMs).toISOString();
	enqueue("m-held", "the ambiguous request", sentAt);
	const heldOpRef = personaTurnOpRef("hold-test", originKey, 0, "m-held");
	db.transaction((tx) => {
		db.inbox.bindTurn(tx, {
			messageId: "m-held",
			originKey,
			epoch: 0,
			opRef: heldOpRef,
			sessionId: binding.sessionId,
			dispatchedAt: sentAt,
		});
		expect(db.inbox.markSendAttempted(tx, heldOpRef, [], sentAt)).toBe(true);
	});
	port.setSessionState(binding.sessionId, { live: options.live });
	if (options.newerMessage !== false) enqueue("m-next", "a newer message", new Date().toISOString());
	const logs: string[] = [];
	const notices: PersonaBindHoldInput[] = [];
	const answers: Array<{ opRef: string; text: string }> = [];
	let noticeFailures = 0;
	const startManager = () =>
		new PersonaSessionManager({
			database: db,
			port,
			instanceId: "hold-test",
			repo,
			unresolvedHoldMs: 10 * MINUTE,
			...(options.clock ? options.clock.managerOptions() : {}),
			log: (line: string) => logs.push(line),
			onBindHold: (input) => {
				if (noticeFailures > 0) {
					noticeFailures--;
					throw new Error("adapter offline");
				}
				notices.push(input);
			},
			onTurnStart: ({ trigger, turn }) => ({
				text: trigger.body,
				onTerminal: ({ text }) => {
					answers.push({ opRef: turn.opRef, text });
				},
			}),
		});
	const manager = startManager();
	return {
		db,
		port,
		originKey,
		binding,
		heldOpRef,
		logs,
		notices,
		answers,
		manager,
		startManager,
		enqueue,
		failNotices: (count: number) => {
			noticeFailures = count;
		},
	};
}

test("a held ambiguous turn on a dead host past the bound moves to a retired hold and the origin answers new messages (#686)", async () => {
	const f = await heldOrigin("escalated", { heldForMs: 11 * MINUTE, live: false });
	try {
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);
		const [next] = f.port.sends;
		expect(next!.text).toContain("a newer message");
		expect(next!.sessionId).toBe("session-1");
		// The held op is never resent.
		expect(f.port.sends.some((send) => send.opRef === f.heldOpRef)).toBe(false);
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).toBe("bound");
		expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(1);
		expect(f.notices).toHaveLength(1);
		expect(f.notices[0]).toMatchObject({ originKey: f.originKey, reason: UNRESOLVED_HOLD_ESCALATED });
		expect(f.notices[0]!.trigger.message_id).toBe("m-held");
		const escalated = f.logs.find((line) => line.startsWith("recovery_hold_escalated"));
		expect(escalated).toContain(`opRef=${f.heldOpRef}`);
		expect(escalated).toContain("nextEpoch=1");
		expect(escalated).toMatch(/heldMs=\d+/);
		expect(escalated).toContain("live=false");
		await until(() => f.db.inbox.turnRow(next!.opRef)?.turn_state === "done");

		// The late outcome of the held op is still reconciled, never dropped or resent.
		f.port.setSessionState(f.binding.sessionId, { live: true });
		// (the seed below records the broker-side acceptance as a scripted send entry)
		f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: next!.repo, text: "", opRef: f.heldOpRef });
		f.port.complete(f.heldOpRef, "late answer");
		await f.manager.reconcile(f.originKey);
		await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
		await f.manager.tick();
		expect(f.port.sends.map((send) => send.opRef)).toEqual([next!.opRef, f.heldOpRef]);
		expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(1);
		expect(f.notices).toHaveLength(1);
		// The late answer reaches the origin, as the hold notice promised.
		expect(f.answers).toContainEqual({ opRef: f.heldOpRef, text: "late answer" });
	} finally {
		await f.manager.stop();
	}
});

test("an unresolved turn under the bound keeps holding its origin (#686)", async () => {
	const f = await heldOrigin("under-bound", { heldForMs: 2 * MINUTE, live: false });
	try {
		await f.manager.recover();
		await f.manager.tick();
		expect(f.port.sends).toHaveLength(0);
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
		expect(f.logs.some((line) => line.includes("reason=operation_state_unknown"))).toBe(true);
		expect(f.notices).toHaveLength(0);
	} finally {
		await f.manager.stop();
	}
});

test("an unresolved turn whose host is still live is never escalated, however long it is held (#686)", async () => {
	const f = await heldOrigin("live-host", { heldForMs: 60 * MINUTE, live: true });
	try {
		await f.manager.recover();
		await f.manager.tick();
		expect(f.port.sends).toHaveLength(0);
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
		expect(f.notices).toHaveLength(0);
	} finally {
		await f.manager.stop();
	}
});

test("an adopted turn swept as unknown escalates once its host dies past the bound (#686)", async () => {
	const f = await heldOrigin("adopted", { heldForMs: 11 * MINUTE, live: true });
	// Accepted and running when the gateway restarted: recovery adopts it as current.
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	const gatewaySends = () => f.port.sends.filter((send) => send.opRef !== f.heldOpRef);
	try {
		await f.manager.recover();
		expect(gatewaySends()).toHaveLength(0);
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).toBe("accepted");
		// Then the host dies and the op's state can no longer be read.
		f.port.unknownOps.add(f.heldOpRef);
		f.port.setSessionState(f.binding.sessionId, { live: false });
		// The newer message was steered into the then-live turn; this one arrives while it is wedged.
		f.enqueue("m-later", "a message after the host died", new Date().toISOString());
		await f.manager.reconcile(f.originKey);
		await until(() => gatewaySends().length === 1);
		expect(gatewaySends()[0]!.text).toContain("a message after the host died");
		expect(gatewaySends()[0]!.sessionId).toBe("session-1");
		expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(1);
		expect(f.notices.map((notice) => notice.reason)).toEqual([UNRESOLVED_HOLD_ESCALATED]);
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(true);
	} finally {
		await f.manager.stop();
	}
});

test("an escalated hold keeps its late answer across a restart (#686)", async () => {
	const f = await heldOrigin("restart", { heldForMs: 11 * MINUTE, live: false });
	await f.manager.recover();
	await until(() => f.port.sends.length === 1);
	await f.manager.stop();
	// The gateway restarts; then the held op finishes.
	f.port.setSessionState(f.binding.sessionId, { live: true });
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	f.port.complete(f.heldOpRef, "late answer after restart");
	const restarted = f.startManager();
	try {
		await restarted.recover();
		await restarted.reconcile(f.originKey);
		await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
		expect(f.answers).toContainEqual({ opRef: f.heldOpRef, text: "late answer after restart" });
		expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(1);
		expect(f.notices).toHaveLength(1);
	} finally {
		await restarted.stop();
	}
});

test("a detached escalated hold on a dead host drains, is readopted and delivers its late answer once", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-escalated", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	const attachTail = f.port.attachTail.bind(f.port);
	f.port.setHostExited(f.binding.sessionId, true);
	f.port.attachTail = async (input) => {
		if (input.sessionId === f.binding.sessionId) throw endpointStale();
		return await attachTail(input);
	};
	try {
		await f.manager.recover();
		expect(f.notices).toHaveLength(1);
		// Exercise the failed reattach and the still-unknown periodic status read (#715).
		await clock.advance(25);
		await f.manager.reconcile(f.originKey);
		expect(f.port.getTailHandles(f.binding.sessionId).size).toBe(0);
		expect(f.logs.some((line) => line.includes("reason=retired_tail_reattach_failed"))).toBe(true);
		await expect(f.manager.drainForRestart("escalated-drain", 20)).resolves.toBe("drained");
		expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
		await f.manager.stop();
		const restarted = f.startManager();
		try {
			await restarted.recover();
			expect(f.logs).toContainEqual(expect.stringContaining(`opRef=${f.heldOpRef} reason=unresolved_hold_readopted`));
			await expect(restarted.drainForRestart("readopted-drain", 20)).resolves.toBe("drained");
			expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });
			expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
			expect(f.port.sendAttempts).toHaveLength(0);
			expect(f.notices).toHaveLength(1);
			// The same operation later completes; recovery must deliver it, never resend it.
			f.port.setSessionState(f.binding.sessionId, { live: true });
			f.port.setHostExited(f.binding.sessionId, false);
			f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
			const sendsBeforeCompletion = f.port.sendAttempts.length;
			f.port.complete(f.heldOpRef, "late answer after drained restart");
			await restarted.reconcile(f.originKey);
			// A second sweep settles the existing one-read terminal-evidence grace.
			await restarted.reconcile(f.originKey);
			await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
			await restarted.reconcile(f.originKey);
			expect(f.answers).toEqual([{ opRef: f.heldOpRef, text: "late answer after drained restart" }]);
			expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "done", turn_state: "done" });
			expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeUndefined();
			expect(f.port.sendAttempts).toHaveLength(sendsBeforeCompletion);
		} finally {
			await restarted.stop();
		}
	} finally {
		await f.manager.stop();
	}
});

test("a detached retired hold without an escalation marker still blocks restart drain", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-retired", {
		heldForMs: 2 * MINUTE,
		live: true,
		newerMessage: false,
		clock,
	});
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	try {
		await f.manager.recover();
		await f.manager.reset(
			f.originKey,
			JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "restart-drain-retired" }),
		);
		expect(f.logs.some((line) => line.includes(`opRef=${f.heldOpRef}`) && line.includes("reason=/new"))).toBe(true);
		expect(f.port.getTailHandles(f.binding.sessionId).size).toBe(0);
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeUndefined();
		await expect(f.manager.drainForRestart("retired-drain", 20)).resolves.toBe("timed_out");
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).toBe("accepted");
	} finally {
		await f.manager.stop();
	}
});

test("an escalated hold with a successfully reattached tail still blocks restart drain", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-reattached", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	f.port.setHostExited(f.binding.sessionId, true);
	try {
		await f.manager.recover();
		expect(f.notices).toHaveLength(1);
		expect(f.port.getTailHandles(f.binding.sessionId).size).toBe(0);
		await expect(f.manager.drainForRestart("reattached-drain", 20)).resolves.toBe("drained");
		// The real retired-reattach timer opens a tail and clears detached.
		f.port.setSessionState(f.binding.sessionId, { live: true });
		await clock.advance(25);
		expect(f.port.getTailHandles(f.binding.sessionId).size).toBe(1);
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
		await expect(f.manager.drainForRestart("reattached-drain", 20)).resolves.toBe("timed_out");
		expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });
	} finally {
		await f.manager.stop();
	}
});

for (const live of [true, false]) {
	test(`a detached escalated in-flight hold blocks restart drain on a ${live ? "live" : "dead"} host`, async () => {
		const clock = new VirtualClock();
		const f = await heldOrigin(`restart-drain-in-flight-${live}`, {
			heldForMs: 11 * MINUTE,
			live: true,
			newerMessage: false,
			clock,
		});
		f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
		try {
			await f.manager.recover();
			const handle = [...f.port.getTailHandles(f.binding.sessionId)][0] as ScriptedTailHandle | undefined;
			expect(handle).toBeDefined();
			f.port.attachTail = async () => {
				throw endpointStale();
			};
			handle?.die();
			await clock.advance(0);
			// CLI status stays in_flight while real relay-loss evidence ages into escalation.
			for (let i = 0; i < 22; i++) {
				await clock.advance(30_000);
				await f.manager.reconcile(f.originKey);
			}
			expect(f.notices.map((notice) => notice.reason)).toEqual([UNRESOLVED_HOLD_ESCALATED]);
			expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
			expect(f.port.getTailHandles(f.binding.sessionId).size).toBe(0);
			expect(f.logs.some((line) => line.includes("reason=retired_tail_reattach_failed"))).toBe(true);
			// Even a subsequently dead host cannot override the operation's active status.
			f.port.setSessionState(f.binding.sessionId, { live });
			f.port.setHostExited(f.binding.sessionId, !live);
			await expect(
				f.port.status({ sessionId: f.binding.sessionId, repo: "", opRef: f.heldOpRef }),
			).resolves.toMatchObject({ status: { status: "in_flight" } });
			await expect(f.manager.drainForRestart(`in-flight-drain-${live}`, 20)).resolves.toBe("timed_out");
			expect(f.answers).toHaveLength(0);
		} finally {
			await f.manager.stop();
		}
	});
}

for (const evidence of ["live-process", "unreadable-exit", "failed-exit-probe", "missing-exit-probe"] as const) {
	test(`a detached escalated hold blocks restart drain with ${evidence} evidence`, async () => {
		const clock = new VirtualClock();
		const f = await heldOrigin(`restart-drain-${evidence}`, {
			heldForMs: 11 * MINUTE,
			live: false,
			newerMessage: false,
			clock,
		});
		f.port.attachTail = async () => {
			throw endpointStale();
		};
		try {
			await f.manager.recover();
			expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
			if (evidence === "live-process") {
				f.port.setSessionState(f.binding.sessionId, { live: true });
				f.port.setHostExited(f.binding.sessionId, false);
			} else if (evidence === "unreadable-exit") {
				f.port.hostExited = async () => false;
			} else if (evidence === "failed-exit-probe") {
				f.port.hostExited = async () => {
					throw new Error("ps unreadable");
				};
			} else {
				Object.defineProperty(f.port, "hostExited", { value: undefined });
			}
			await expect(f.manager.drainForRestart(`evidence-${evidence}`, 20)).resolves.toBe("timed_out");
		} finally {
			await f.manager.stop();
		}
	});
}

for (const [label, operationRef] of [
	["a mismatched", "other-op-ref"],
	["a missing", ""],
] as const) {
	for (const reported of ["terminal_ok", "unknown"] as const) {
		test(`a detached escalated hold whose fresh ${reported} report carries ${label} operation identity blocks restart drain on an exited host`, async () => {
			const clock = new VirtualClock();
			const f = await heldOrigin(`restart-drain-${operationRef || "missing"}-ref-${reported}`, {
				heldForMs: 11 * MINUTE,
				live: false,
				newerMessage: false,
				clock,
			});
			f.port.attachTail = async () => {
				throw endpointStale();
			};
			try {
				await f.manager.recover();
				expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
				f.port.setHostExited(f.binding.sessionId, true);
				const status = f.port.status.bind(f.port);
				f.port.status = async (input) => {
					const result = await status(input);
					if (input.opRef !== f.heldOpRef) return result;
					return { ...result, operationRef, status: { status: reported } };
				};
				// A report about another (or no) operation is not evidence that the held one is inactive.
				await expect(f.manager.drainForRestart(`${operationRef || "missing"}-ref-${reported}`, 20)).resolves.toBe(
					"timed_out",
				);
				expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });
			} finally {
				await f.manager.stop();
			}
		});
	}
}

test("a detached escalated hold whose status endpoint is stale drains on a dead host", async () => {
	// The production shape: `gjc sdk session status` fails with endpoint_stale for good
	// and the host is gone, so the hold can never settle and no producer is running.
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-stale-endpoint", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	f.port.setHostExited(f.binding.sessionId, true);
	f.port.attachTail = async () => {
		throw endpointStale();
	};
	f.port.status = async () => {
		throw endpointStale();
	};
	try {
		await f.manager.recover();
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
		await expect(f.manager.drainForRestart("stale-endpoint-drain", 20)).resolves.toBe("drained");
		expect(f.db.inbox.turnRow(f.heldOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });
	} finally {
		await f.manager.stop();
	}
});

test("a stale endpoint and live:false cannot drain while the host process has not exited", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-stale-live-process", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	f.port.setHostExited(f.binding.sessionId, false);
	f.port.attachTail = async () => {
		throw endpointStale();
	};
	f.port.status = async () => {
		throw endpointStale();
	};
	try {
		await f.manager.recover();
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
		await expect(f.manager.drainForRestart("stale-live-process-drain", 20)).resolves.toBe("timed_out");
	} finally {
		await f.manager.stop();
	}
});

test("restart drain caches only blocked probes briefly and never reuses an exempt verdict", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-cache", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	f.port.attachTail = async () => {
		throw endpointStale();
	};
	try {
		await f.manager.recover();
		await clock.advance(25);
		const status = f.port.status.bind(f.port);
		const hostExited = f.port.hostExited.bind(f.port);
		let statusReads = 0;
		let exitReads = 0;
		f.port.status = async (input) => {
			statusReads++;
			return await status(input);
		};
		f.port.hostExited = async (input) => {
			exitReads++;
			return await hostExited(input);
		};
		await expect(f.manager.drainForRestart("cached-drain", 20)).resolves.toBe("timed_out");
		await expect(f.manager.drainForRestart("cached-drain", 20)).resolves.toBe("timed_out");
		expect({ statusReads, exitReads }).toEqual({ statusReads: 1, exitReads: 1 });
		await clock.advance(250);
		f.port.setHostExited(f.binding.sessionId, true);
		await expect(f.manager.drainForRestart("cached-drain", 20)).resolves.toBe("drained");
		expect({ statusReads, exitReads }).toEqual({ statusReads: 2, exitReads: 2 });
		// No virtual time passes: an exemption must be re-proven even inside the interval.
		f.port.setHostExited(f.binding.sessionId, false);
		await expect(f.manager.drainForRestart("cached-drain", 20)).resolves.toBe("timed_out");
		expect({ statusReads, exitReads }).toEqual({ statusReads: 3, exitReads: 3 });
		// Repeated blocked iterations use the same verdict rather than spawning more probes.
		await expect(f.manager.drainForRestart("cached-drain", 20)).resolves.toBe("timed_out");
		expect({ statusReads, exitReads }).toEqual({ statusReads: 3, exitReads: 3 });
	} finally {
		await f.manager.stop();
	}
});

test("restart drain uses process exit rather than registry liveness", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("restart-drain-process-proof", {
		heldForMs: 11 * MINUTE,
		live: false,
		newerMessage: false,
		clock,
	});
	f.port.attachTail = async () => {
		throw endpointStale();
	};
	try {
		await f.manager.recover();
		f.port.setHostExited(f.binding.sessionId, true);
		f.port.setSessionState(f.binding.sessionId, { live: true });
		await expect(f.manager.drainForRestart("process-proof-drain", 20)).resolves.toBe("drained");
		let livenessReads = 0;
		f.port.liveness = async () => {
			livenessReads++;
			throw endpointStale();
		};
		await expect(f.manager.drainForRestart("process-proof-drain", 20)).resolves.toBe("drained");
		expect(livenessReads).toBe(0);
	} finally {
		await f.manager.stop();
	}
});

for (const probe of ["status", "hostExited"] as const) {
	test(`a ${probe} probe still running at the drain deadline blocks the drain and frees the origin's queue`, async () => {
		const clock = new VirtualClock();
		const f = await heldOrigin(`restart-drain-deadline-${probe}`, {
			heldForMs: 11 * MINUTE,
			live: false,
			newerMessage: false,
			clock,
		});
		f.port.attachTail = async () => {
			throw endpointStale();
		};
		const release = Promise.withResolvers<void>();
		try {
			await f.manager.recover();
			const status = f.port.status.bind(f.port);
			let statusReads = 0;
			let exitReads = 0;
			let hung = 0;
			f.port.status = async (input) => {
				statusReads++;
				if (probe === "status" && hung++ === 0) await release.promise;
				return await status(input);
			};
			f.port.hostExited = async () => {
				exitReads++;
				if (probe === "hostExited" && hung++ === 0) await release.promise;
				return true;
			};
			// A hung probe is not exit evidence, and the drain returns at its own deadline.
			await expect(f.manager.drainForRestart("deadline-drain", 30)).resolves.toBe("timed_out");
			expect(probe === "status" ? statusReads : exitReads).toBe(1);
			// The origin's actor queue is free while the probe is still hung.
			await expect(
				Promise.race([f.manager.reconcile(f.originKey).then(() => "ran"), Bun.sleep(1_000).then(() => "blocked")]),
			).resolves.toBe("ran");
			// Nothing was cached from the unfinished probe: the next drain probes again.
			const before = probe === "status" ? statusReads : exitReads;
			await expect(f.manager.drainForRestart("deadline-drain", 1_000)).resolves.toBe("drained");
			expect(probe === "status" ? statusReads : exitReads).toBe(before + 1);
		} finally {
			release.resolve();
			await f.manager.stop();
		}
	});
}

for (const probe of ["status", "hostExited"] as const) {
	test(`restart drain stops after an in-progress ${probe} probe when the manager stops`, async () => {
		const clock = new VirtualClock();
		const f = await heldOrigin(`restart-drain-stop-${probe}`, {
			heldForMs: 11 * MINUTE,
			live: false,
			newerMessage: false,
			clock,
		});
		f.port.attachTail = async () => {
			throw endpointStale();
		};
		const release = Promise.withResolvers<void>();
		let stop: Promise<void> | undefined;
		try {
			await f.manager.recover();
			const status = f.port.status.bind(f.port);
			let statusReads = 0;
			let exitReads = 0;
			f.port.status = async (input) => {
				statusReads++;
				if (probe === "status") await release.promise;
				return await status(input);
			};
			f.port.hostExited = async () => {
				exitReads++;
				if (probe === "hostExited") await release.promise;
				return true;
			};
			const drain = f.manager.drainForRestart("stop-during-probe", 1_000);
			await until(() => (probe === "status" ? statusReads : exitReads) === 1);
			stop = f.manager.stop();
			release.resolve();
			await expect(drain).resolves.toBe("stopped");
			await stop;
			expect(statusReads).toBe(1);
			expect(exitReads).toBe(probe === "status" ? 0 : 1);
		} finally {
			release.resolve();
			await (stop ?? f.manager.stop());
		}
	});
}

test("a failed escalation notice is durable and posted by the next recovery sweep, once (#686)", async () => {
	const f = await heldOrigin("notice-retry", { heldForMs: 11 * MINUTE, live: false });
	f.failNotices(1);
	try {
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);
		expect(f.notices).toHaveLength(0);
		expect(f.logs.some((line) => line.startsWith("persona_bind_hold_notice_failed"))).toBe(true);
		await f.manager.recover();
		expect(f.notices.map((notice) => notice.reason)).toEqual([UNRESOLVED_HOLD_ESCALATED]);
		expect(f.notices[0]!.trigger.message_id).toBe("m-held");
		await f.manager.recover();
		expect(f.notices).toHaveLength(1);
	} finally {
		await f.manager.stop();
	}
});

test("the recovery bounds reload live and fall back to their defaults (#686, #687)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-unresolved-hold-"));
	database = await Store.open(join(home, "gateway.db"));
	const manager = new PersonaSessionManager({
		database,
		port: new HoldPort(),
		instanceId: "bounds",
		repo: join(home, "workspace"),
		busyStallMs: 120_000,
		unresolvedHoldMs: 180_000,
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	try {
		expect([manager.busyStallMs, manager.unresolvedHoldMs]).toEqual([120_000, 180_000]);
		manager.setHostLimits({ busyStallMs: 900_000, unresolvedHoldMs: 60_000 });
		expect([manager.busyStallMs, manager.unresolvedHoldMs]).toEqual([900_000, 60_000]);
		manager.setHostLimits(undefined);
		expect([manager.busyStallMs, manager.unresolvedHoldMs]).toEqual([
			DEFAULT_SESSIONS_BUSY_STALL_MS,
			DEFAULT_SESSIONS_UNRESOLVED_HOLD_MS,
		]);
	} finally {
		await manager.stop();
	}
});

test("a failed escalation notice is still posted after its turn settles with no newer message, also after a restart (#686)", async () => {
	const f = await heldOrigin("notice-orphan", { heldForMs: 11 * MINUTE, live: false, newerMessage: false });
	f.failNotices(1);
	await f.manager.recover();
	expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(true);
	expect(f.notices).toHaveLength(0);
	// The held op settles; nothing else is pending and the rotated binding has no session.
	f.port.setSessionState(f.binding.sessionId, { live: true });
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	f.port.complete(f.heldOpRef, "late answer");
	await f.manager.reconcile(f.originKey);
	await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
	expect(f.db.inbox.nonterminalOrigins()).not.toContain(f.originKey);
	expect(f.db.inbox.pendingOrigins()).not.toContain(f.originKey);
	await f.manager.stop();

	const restarted = f.startManager();
	try {
		await restarted.recover();
		expect(f.notices.map((notice) => notice.reason)).toEqual([UNRESOLVED_HOLD_ESCALATED]);
		await restarted.recover();
		expect(f.notices).toHaveLength(1);
	} finally {
		await restarted.stop();
	}
});

test("retention keeps an old trigger until its escalation notice is posted, then sweeps it (#686)", async () => {
	const DAY = 24 * 60 * MINUTE;
	const f = await heldOrigin("notice-retention", { heldForMs: 31 * DAY, live: false, newerMessage: false });
	const sweep = () =>
		f.db.transaction((tx) =>
			f.db.inbox.sweep(tx, { historyCutoff: new Date(Date.now() - 30 * DAY).toISOString(), batch: 100 }),
		);
	f.failNotices(1);
	await f.manager.recover();
	expect(f.notices).toHaveLength(0);
	f.port.setSessionState(f.binding.sessionId, { live: true });
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	f.port.complete(f.heldOpRef, "late answer");
	await f.manager.reconcile(f.originKey);
	await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
	// The settled trigger is past the 30-day horizon, but its notice is still owed.
	sweep();
	expect(f.db.inbox.turnRow(f.heldOpRef)).toBeDefined();
	await f.manager.recover();
	expect(f.notices.map((notice) => notice.reason)).toEqual([UNRESOLVED_HOLD_ESCALATED]);
	expect(f.logs.some((line) => line.startsWith("escalation_notice_unresolvable"))).toBe(false);
	sweep();
	expect(f.db.inbox.turnRow(f.heldOpRef)).toBeUndefined();
	await f.manager.stop();
});

/** The gateway restarted (or `/new` ran) while the turn was unresolved: its epoch is retired. */
function retireHeldEpoch(f: Awaited<ReturnType<typeof heldOrigin>>): void {
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, f.originKey));
	expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(1);
}

test("a retired-epoch unresolved turn past the bound is escalated once, without another rotation (#705)", async () => {
	const f = await heldOrigin("retired-escalated", { heldForMs: 11 * MINUTE, live: false });
	retireHeldEpoch(f);
	try {
		await f.manager.recover();
		await until(() => f.notices.length === 1);
		expect(f.notices[0]).toMatchObject({ originKey: f.originKey, reason: UNRESOLVED_HOLD_ESCALATED });
		expect(f.notices[0]!.trigger.message_id).toBe("m-held");
		const escalated = f.logs.filter((line) => line.startsWith("recovery_hold_escalated"));
		expect(escalated).toHaveLength(1);
		expect(escalated[0]).toContain(`opRef=${f.heldOpRef}`);
		expect(escalated[0]).toContain("epoch=0 nextEpoch=1");
		expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(1);
		// The origin already moved on: the newer message is answered there.
		await until(() => f.port.sends.length === 1);
		expect(f.port.sends[0]!.text).toContain("a newer message");
		expect(f.port.sends.some((send) => send.opRef === f.heldOpRef)).toBe(false);
		// Further sweeps and a restart never post a second notice or resend the op.
		await f.manager.recover();
		await f.manager.recover();
		await f.manager.stop();
		const restarted = f.startManager();
		try {
			await restarted.recover();
			await restarted.recover();
			expect(f.notices).toHaveLength(1);
			expect(f.logs.filter((line) => line.startsWith("recovery_hold_escalated"))).toHaveLength(1);
			expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(0);
		} finally {
			await restarted.stop();
		}
	} finally {
		await f.manager.stop();
	}
});

test("a retired-epoch unresolved turn under the bound keeps holding, with no notice (#705)", async () => {
	const f = await heldOrigin("retired-under-bound", { heldForMs: 2 * MINUTE, live: false });
	retireHeldEpoch(f);
	try {
		await f.manager.recover();
		await f.manager.recover();
		expect(f.notices).toHaveLength(0);
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).toBe("bound");
	} finally {
		await f.manager.stop();
	}
});

test("a backfilled send marker does not exempt a retired unresolved turn from the bound (#705)", async () => {
	const f = await heldOrigin("retired-backfilled", { heldForMs: 11 * MINUTE, live: false });
	// Schema 39 stamps pre-upgrade rows with a non-timestamp marker.
	const raw = new Database(join(home, "gateway.db"));
	raw.exec("PRAGMA busy_timeout = 5000");
	raw.exec("UPDATE inbound_messages SET send_attempted_at = 'pre-migration-39' WHERE message_id = 'm-held'");
	raw.close();
	expect(f.db.inbox.sendAttemptedAt(f.heldOpRef)).toBe("pre-migration-39");
	retireHeldEpoch(f);
	try {
		await f.manager.recover();
		await until(() => f.notices.length === 1);
		expect(f.notices[0]!.trigger.message_id).toBe("m-held");
		expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(0);
	} finally {
		await f.manager.stop();
	}
});

test("an unchanged recovery hold is logged once across sweeps, not on every scan (#705)", async () => {
	const f = await heldOrigin("hold-log-once", { heldForMs: 2 * MINUTE, live: false });
	retireHeldEpoch(f);
	try {
		for (let sweep = 0; sweep < 5; sweep++) await f.manager.recover();
		const holds = f.logs.filter((line) => line.startsWith("recovery_hold ") && line.includes(`opRef=${f.heldOpRef}`));
		expect(holds).toHaveLength(1);
		expect(holds[0]).toContain("reason=operation_state_unknown");
	} finally {
		await f.manager.stop();
	}
});

test("an escalated hold whose session stays endpoint_stale keeps holding, read only by the per-minute sweep, also across a restart (#715)", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("endpoint-stale", { heldForMs: 11 * MINUTE, live: false, clock });
	try {
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);
		expect(f.notices).toHaveLength(1);
		let statusReads = 0;
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			statusReads++;
			throw endpointStale();
		};
		// One simulated hour: the gateway sweep reconciles every minute and the
		// manager's own recheck/reattach timers fire as virtual time passes.
		for (let minute = 0; minute < 60; minute++) {
			await clock.advance(MINUTE);
			await f.manager.reconcile(f.originKey);
		}
		// Broker absence is not terminal evidence: the hold stays open and durable,
		// read about once a minute rather than by the 5 s status recheck (~9/min in #715).
		expect(statusReads).toBeGreaterThanOrEqual(60);
		expect(statusReads).toBeLessThanOrEqual(64);
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).not.toBe("done");
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeDefined();
		const holds = f.logs.filter((line) => line.startsWith("recovery_hold ") && line.includes(f.heldOpRef));
		expect(holds.filter((line) => line.includes("reason=status_unavailable"))).toHaveLength(1);
		// The held request is never resent and the user is not told twice.
		expect(f.port.sends.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(0);
		expect(f.notices).toHaveLength(1);
		// The newer message's own turn is untouched.
		expect(f.answers.map((answer) => answer.text).join("\n")).toContain("answer to");

		await f.manager.stop();
		const readsBeforeRestart = statusReads;
		const restarted = f.startManager();
		try {
			await restarted.recover();
			for (let minute = 0; minute < 10; minute++) {
				await clock.advance(MINUTE);
				await restarted.reconcile(f.originKey);
			}
			expect(statusReads - readsBeforeRestart).toBeLessThanOrEqual(14);
			expect(f.notices).toHaveLength(1);
			expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).not.toBe("done");
		} finally {
			await restarted.stop();
		}
	} finally {
		await f.manager.stop();
	}
});

test("an escalated hold whose status is only transiently unreadable keeps holding (#715)", async () => {
	const clock = new VirtualClock();
	const f = await heldOrigin("transient-stale", { heldForMs: 11 * MINUTE, live: false, clock });
	try {
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);
		let reads = 0;
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			reads++;
			// Every other read is a different failure: no run of session-gone reads.
			throw reads % 2 === 0 ? new Error("broker_unavailable: socket closed") : endpointStale();
		};
		for (let minute = 0; minute < 60; minute++) {
			await clock.advance(MINUTE);
			await f.manager.reconcile(f.originKey);
		}
		// Only the per-minute sweep (plus the bounded reattach tries) reads it.
		expect(reads).toBeGreaterThanOrEqual(60);
		expect(reads).toBeLessThanOrEqual(64);
		expect(f.db.inbox.turnRow(f.heldOpRef)?.turn_state).not.toBe("done");
	} finally {
		await f.manager.stop();
	}
});

test("relay unavailable escalation measures time from the first failure, not the latest, even with frequent retries (#766)", async () => {
	const clock = new VirtualClock();
	const heldMs = 10 * MINUTE; // Hold will escalate after 10 minutes
	const f = await heldOrigin("relay-unavailable-timestamp", { heldForMs: heldMs, live: true, clock });
	// Seed accepted operation before recovery so status checks can exercise relay path (#766)
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	try {
		await f.manager.recover();

		const escalationNotices = f.notices.length;
		const status = f.port.status.bind(f.port);
		const relayUnavailableMs = 30_000; // Probe every 30 seconds to make test ~5s on darwin
		// With 11 minute duration and 30s intervals: ~22 reconciles, proving relay unavailable
		// evidence is tracked from first failure, not latest.

		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			const isRelay = input.relay !== undefined;
			if (isRelay) {
				// Simulate relay transport failure with the real error type production code recognizes
				throw new RelayClosedError(f.binding.sessionId, "relay unavailable", false);
			}
			return await status(input);
		};

		// Advance by 11 minutes with relay failures to meet the escalation threshold.
		// The relay status is probed every ~250ms at the start of reconciliation, so many retries occur.
		// Since relay failures start immediately, after 10+ minutes the relay unavailable evidence
		// will span >= 10 minutes and trigger escalation on a live host.
		for (let i = 0; i < 11; i++) {
			for (let j = 0; j < MINUTE / relayUnavailableMs; j++) {
				await clock.advance(relayUnavailableMs);
				await f.manager.reconcile(f.originKey);
			}
		}

		// Escalation must occur with exactly one notice with the right reason
		expect(f.notices.length).toBe(escalationNotices + 1);
		expect(f.notices[f.notices.length - 1]?.reason).toBe(UNRESOLVED_HOLD_ESCALATED);

		// Advance further to verify escalation does not fire again
		const noticesAfterEscalation = f.notices.length;
		for (let i = 0; i < 5; i++) {
			await clock.advance(MINUTE);
			await f.manager.reconcile(f.originKey);
		}

		expect(f.notices.length).toBe(noticesAfterEscalation);
	} finally {
		await f.manager.stop();
	}
});

test("a dead relay (onRelayDead) followed only by CLI-only status probes still escalates after unresolvedHoldMs (#766)", async () => {
	const clock = new VirtualClock();
	const heldMs = 10 * MINUTE; // Hold will escalate after 10 minutes
	const f = await heldOrigin("relay-dead-escalation", { heldForMs: heldMs, live: true, clock });
	// Seed accepted operation before recovery so status checks can exercise relay path (#766)
	f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
	try {
		await f.manager.recover();

		// Exercise the relay-death callback to set bound.detached = true (#766).
		// After onRelayDead, statusOf must record relay unavailable evidence via CLI-only probes.
		const relayHandles = f.port.getTailHandles(f.binding.sessionId);
		const handle = [...relayHandles][0] as ScriptedTailHandle | undefined;
		expect(handle).toBeDefined();
		const escalationNotices = f.notices.length;

		// Trigger the relay death callback; this sets bound.detached and records relay unavailable evidence.
		handle!.die();
		await clock.advance(0); // Let async work settle

		// After relay death, only CLI-only status probes can check the operation.
		const status = f.port.status.bind(f.port);
		const relayUnavailableMs = 30_000; // Probe every 30 seconds
		let cliProbeCount = 0;

		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			const isRelay = input.relay !== undefined;
			if (isRelay) {
				// After onRelayDead, the bound should be detached and no relay probe should happen.
				throw new Error("relay probe should not happen after onRelayDead");
			}
			cliProbeCount++;
			return await status(input);
		};

		// Advance by 11 minutes with CLI-only status probes.
		// Escalation should trigger based on relay unavailable evidence recorded at death.
		for (let i = 0; i < 11; i++) {
			for (let j = 0; j < MINUTE / relayUnavailableMs; j++) {
				await clock.advance(relayUnavailableMs);
				await f.manager.reconcile(f.originKey);
			}
		}

		// Verify CLI-only probes were used (no relay probes after death)
		expect(cliProbeCount).toBeGreaterThan(0);

		// Escalation must occur exactly once with the right reason.
		// It escalates because relay unavailable evidence is >= unresolvedHoldMs old.
		expect(f.notices.length).toBe(escalationNotices + 1);
		expect(f.notices[f.notices.length - 1]?.reason).toBe(UNRESOLVED_HOLD_ESCALATED);

		// Verify no second escalation
		const noticesAfterEscalation = f.notices.length;
		for (let i = 0; i < 5; i++) {
			await clock.advance(MINUTE);
			await f.manager.reconcile(f.originKey);
		}
		expect(f.notices.length).toBe(noticesAfterEscalation);
	} finally {
		await f.manager.stop();
	}
});

test("a held op alternating between two reasons across scans logs recovery_hold exactly once per reason (#772 regression)", async () => {
	const f = await heldOrigin("alternating-reasons", { heldForMs: 2 * MINUTE, live: true });
	try {
		// Mock status to toggle between unknown and terminal_uncertain
		let useUnknownStatus = true;
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			const result = await status(input);
			if (useUnknownStatus) {
				return { ...result, status: { status: "unknown" } };
			} else {
				return { ...result, status: { status: "terminal_ok" as const, receiptState: "unknown" as const } };
			}
		};

		// Call recover() three times: A (unknown), B (terminal_uncertain), A (unknown again)
		await f.manager.recover(); // Call 1: unknown -> logs reason A
		useUnknownStatus = false; // Switch to terminal_uncertain
		await f.manager.recover(); // Call 2: terminal_uncertain -> logs reason B (changed)
		useUnknownStatus = true; // Switch back to unknown
		await f.manager.recover(); // Call 3: unknown -> dev logs reason A (changed), HEAD doesn't (A seen before)

		// A -> B -> A logs each reason exactly once: neither re-logged on return nor suppressed.
		const holds = f.logs.filter((line) => line.startsWith("recovery_hold ") && line.includes(`opRef=${f.heldOpRef}`));
		const reasonA = holds.filter((line) => line.includes("reason=operation_state_unknown"));
		const reasonB = holds.filter((line) =>
			line.includes("reason=operation state terminal_uncertain is not decidable; do not resend, resume or recreate"),
		);
		expect(reasonA).toHaveLength(1);
		expect(reasonB).toHaveLength(1);

		// No escalation should occur since host is live and under the bound
		expect(f.notices.length).toBe(0);
	} finally {
		await f.manager.stop();
	}
});

test("a terminal_uncertain hold held past unresolvedHoldMs emits exactly one recovery_hold_escalated (#772 regression)", async () => {
	const f = await heldOrigin("terminal-uncertain-escalate", { heldForMs: 11 * MINUTE, live: false });
	try {
		// Return a status that projects to terminal_uncertain but is not status.status="unknown"
		// This tests that operation_uncertain hold escalates regardless of status (#772)
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			const result = await status(input);
			// Return terminal_ok with unknown receipt -> projects to terminal_uncertain
			// This should trigger operation_uncertain hold in decideRecovery
			return {
				...result,
				status: {
					status: "terminal_ok" as const,
					receiptState: "unknown" as const,
				},
			};
		};

		// The fix adds holdKind to RecoveryDecision, and #escalateUnresolvedHold checks for
		// operation_uncertain to enable escalation regardless of current status (#772).
		// In dev, operation_uncertain doesn't have escalation logic, so this would NOT escalate.
		// In the fix, operation_uncertain with holdKind DOES escalate.
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);

		// Verify that escalation occurred exactly once (test fails in dev, passes in fix)
		const escalations = f.notices.filter((n) => n.reason === UNRESOLVED_HOLD_ESCALATED);
		expect(escalations).toHaveLength(1);

		// Verify that exactly one recovery_hold_escalated log exists for this operation
		const escalationLogs = f.logs.filter(
			(line) => line.includes("recovery_hold_escalated") && line.includes(`opRef=${f.heldOpRef}`),
		);
		expect(escalationLogs).toHaveLength(1);
		// The evidence names the status actually read and the hold kind, not a blanket unknown.
		expect(escalationLogs[0]).toContain('evidence="status=terminal_ok holdKind=operation_uncertain live=false');
	} finally {
		await f.manager.stop();
	}
});

test("a terminal_uncertain hold persisted as escalated but not adopted before a crash is adopted again on restart (#772)", async () => {
	const f = await heldOrigin("escalated-crash-window", { heldForMs: 11 * MINUTE, live: false });
	await f.manager.stop();
	// The escalation transaction committed (fresh epoch + marker), then the process
	// died before #adoptRecoveredTurn registered the held turn.
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, f.originKey));
	f.db.core.metaSet(`escalated-hold:${f.heldOpRef}`, "1");
	const status = f.port.status.bind(f.port);
	let uncertain = true;
	f.port.status = async (input) => {
		const result = await status(input);
		if (input.opRef !== f.heldOpRef || !uncertain) return result;
		// terminal_ok with an unknown receipt projects to an operation_uncertain operator hold.
		return { ...result, status: { status: "terminal_ok" as const, receiptState: "unknown" as const } };
	};
	const restarted = f.startManager();
	try {
		await restarted.recover();
		expect(
			f.logs.some((line) => line.includes(`opRef=${f.heldOpRef}`) && line.includes("reason=unresolved_hold_readopted")),
		).toBe(true);
		// Re-adoption is not a second escalation: no fresh epoch, no second notice.
		expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
		expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(1);
		// The held op later settles; the periodic reconcile observes the adopted
		// turn and delivers the answer the escalation notice promised.
		uncertain = false;
		f.port.setSessionState(f.binding.sessionId, { live: true });
		f.port.seedAcceptedSend({ sessionId: f.binding.sessionId, repo: "", text: "", opRef: f.heldOpRef });
		f.port.complete(f.heldOpRef, "late answer after crash");
		await restarted.reconcile(f.originKey);
		await until(() => f.db.inbox.turnRow(f.heldOpRef)?.turn_state === "done");
		expect(f.answers).toContainEqual({ opRef: f.heldOpRef, text: "late answer after crash" });
		expect(f.port.sendAttempts.filter((send) => send.opRef === f.heldOpRef)).toHaveLength(1);
		expect(f.db.core.metaGet(`escalated-hold:${f.heldOpRef}`)).toBeUndefined();
	} finally {
		await restarted.stop();
	}
});

test("an ambiguous authority hold held past unresolvedHoldMs emits exactly one recovery_hold_escalated (#772 regression)", async () => {
	const f = await heldOrigin("ambiguous-authority-escalate", { heldForMs: 11 * MINUTE, live: false });
	try {
		// Corrupt inspect to return a mismatched sessionId, triggering ambiguous authority hold
		const inspect = f.port.inspect.bind(f.port);
		let callCount = 0;
		f.port.inspect = async (input) => {
			if (input.sessionId !== f.binding.sessionId) return await inspect(input);
			callCount++;
			const result = await inspect(input);
			// First call: return a different sessionId to trigger ambiguous authority via authority shift
			if (callCount === 1 && result) {
				return { ...result, sessionId: "mismatched-session-id" };
			}
			return result;
		};

		// Mock status to return terminal_ok (not unknown)
		// This ensures dev does NOT escalate (dev requires status="unknown" for non-bound operations)
		// But HEAD DOES escalate because holdKind="ambiguous_authority" overrides the status check (#772)
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef !== f.heldOpRef) return await status(input);
			const result = await status(input);
			return { ...result, status: { status: "terminal_ok" as const } };
		};

		// Dead host + ambiguous authority + status != unknown:
		// - Dev does NOT escalate because status is not "unknown" (dev's check fails)
		// - HEAD DOES escalate because ambiguous_authority holdKind overrides status check
		await f.manager.recover();
		await until(() => f.port.sends.length === 1);

		// One escalation notice with the right reason
		const escalations = f.notices.filter((n) => n.reason === UNRESOLVED_HOLD_ESCALATED);
		expect(escalations).toHaveLength(1);

		// Verify that exactly one recovery_hold_escalated log exists for this operation
		const escalationLogs = f.logs.filter(
			(line) => line.includes("recovery_hold_escalated") && line.includes(`opRef=${f.heldOpRef}`),
		);
		expect(escalationLogs).toHaveLength(1);
		expect(escalationLogs[0]).toContain('evidence="status=terminal_ok holdKind=ambiguous_authority live=false');

		// Verify no second escalation happens on next recovery
		const initialNoticeCount = f.notices.length;
		await f.manager.recover();

		const finalEscalations = f.notices.filter((n) => n.reason === UNRESOLVED_HOLD_ESCALATED);
		expect(finalEscalations).toHaveLength(1);
		expect(f.notices.length).toBe(initialNoticeCount);
	} finally {
		await f.manager.stop();
	}
});

for (const active of ["accepted", "in_flight"] as const) {
	test(`an ambiguous authority hold whose fresh status reads ${active} stays fenced past unresolvedHoldMs on a dead host (#772)`, async () => {
		const f = await heldOrigin(`ambiguous-${active}-fenced`, { heldForMs: 11 * MINUTE, live: false });
		try {
			// Every recovery scan sees the broker identity shift between its two inspects.
			const inspect = f.port.inspect.bind(f.port);
			let calls = 0;
			f.port.inspect = async (input) => {
				const result = await inspect(input);
				if (input.sessionId !== f.binding.sessionId || !result) return result;
				calls++;
				return calls % 2 === 1 ? { ...result, sessionId: "mismatched-session-id" } : result;
			};
			const status = f.port.status.bind(f.port);
			f.port.status = async (input) => {
				const result = await status(input);
				return input.opRef === f.heldOpRef ? { ...result, status: { status: active } } : result;
			};

			for (let scan = 0; scan < 3; scan++) await f.manager.recover();

			// The op may still be mutating the workspace: no rotation, no queued dispatch, no notice.
			expect(f.port.sends).toHaveLength(0);
			expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(0);
			expect(f.notices).toHaveLength(0);
			expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
			expect(
				f.logs.filter(
					(line) =>
						line.startsWith("recovery_hold ") &&
						line.includes(`opRef=${f.heldOpRef}`) &&
						line.includes("reason=broker authority is ambiguous"),
				),
			).toHaveLength(1);
		} finally {
			await f.manager.stop();
		}
	});
}

for (const [label, operationRef] of [
	["a mismatched", "other-op-ref"],
	["a missing", ""],
] as const) {
	for (const kind of ["ambiguous_authority", "operation_uncertain"] as const) {
		test(`a ${kind === "operation_uncertain" ? "receipt-uncertain" : "broker-identity-shifted"} hold whose fresh terminal report carries ${label} operation identity stays fenced on a dead host (#772)`, async () => {
			const f = await heldOrigin(`${kind}-${operationRef || "missing"}-ref-fenced`, {
				heldForMs: 11 * MINUTE,
				live: false,
			});
			try {
				if (kind === "ambiguous_authority") {
					// Every recovery scan sees the broker identity shift between its two inspects.
					const inspect = f.port.inspect.bind(f.port);
					let calls = 0;
					f.port.inspect = async (input) => {
						const result = await inspect(input);
						if (input.sessionId !== f.binding.sessionId || !result) return result;
						calls++;
						return calls % 2 === 1 ? { ...result, sessionId: "mismatched-session-id" } : result;
					};
				}
				const status = f.port.status.bind(f.port);
				f.port.status = async (input) => {
					const result = await status(input);
					if (input.opRef !== f.heldOpRef) return result;
					return {
						...result,
						operationRef,
						status:
							kind === "operation_uncertain"
								? { status: "terminal_ok" as const, receiptState: "unknown" as const }
								: { status: "terminal_ok" as const },
					};
				};

				for (let scan = 0; scan < 3; scan++) await f.manager.recover();

				// A terminal report for another (or no) op says nothing about the held one:
				// no rotation, no queued dispatch, no notice.
				expect(f.port.sends).toHaveLength(0);
				expect(f.db.bindings.getSessionRecord(f.originKey)?.epoch).toBe(0);
				expect(f.notices).toHaveLength(0);
				expect(f.logs.some((line) => line.startsWith("recovery_hold_escalated"))).toBe(false);
			} finally {
				await f.manager.stop();
			}
		});
	}
}
