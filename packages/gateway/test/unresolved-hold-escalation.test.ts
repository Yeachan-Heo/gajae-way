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

async function heldOrigin(
	name: string,
	options: { heldForMs: number; live: boolean; reuseHome?: boolean; newerMessage?: boolean },
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
