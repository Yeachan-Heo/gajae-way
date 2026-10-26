import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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

async function heldOrigin(name: string, options: { heldForMs: number; live: boolean }) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-unresolved-hold-"));
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
		expect(db.inbox.markSendAttempted(tx, heldOpRef, sentAt)).toBe(true);
	});
	port.setSessionState(binding.sessionId, { live: options.live });
	enqueue("m-next", "a newer message", new Date().toISOString());
	const logs: string[] = [];
	const notices: PersonaBindHoldInput[] = [];
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "hold-test",
		repo,
		unresolvedHoldMs: 10 * MINUTE,
		log: (line: string) => logs.push(line),
		onBindHold: (input) => {
			notices.push(input);
		},
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	return { db, port, originKey, binding, heldOpRef, logs, notices, manager, enqueue };
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
