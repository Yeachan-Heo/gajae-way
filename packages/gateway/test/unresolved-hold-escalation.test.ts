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
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

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
