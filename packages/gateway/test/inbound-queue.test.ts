import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase, InboundTurnConflictError } from "../src/store/db";

const ORIGIN_KEY = "discord:dm:1";

let home = "";
let database: GatewayDatabase | undefined;
afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function open(): Promise<GatewayDatabase> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-inbound-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	return database;
}

const message = (id: string, body: string) => ({
	messageId: id,
	originKey: ORIGIN_KEY,
	originRefJson: '{"platform":"discord"}',
	body,
});

const timestamp = (now: number, offsetMs: number) => new Date(now + offsetMs).toISOString();

test("the same platform message id is accepted exactly once", async () => {
	const db = await open();
	expect(db.inboundEnqueue(message("m1", "hello"))).toBe(true);
	expect(db.inboundEnqueue(message("m1", "hello"))).toBe(false);
	expect(db.inboundPendingCount(ORIGIN_KEY)).toBe(1);
});

test("rows received in the same millisecond are served in insertion order, not platform-id order", async () => {
	const db = await open();
	const at = new Date().toISOString();
	// Discord snowflakes are not monotonic across shards; a burst can even land
	// with ids that sort backwards. Arrival order is the only order that matters.
	for (const id of ["z-first", "m-second", "a-third"])
		expect(db.inboundEnqueue({ ...message(id, id), receivedAt: at })).toBe(true);
	const served: string[] = [];
	for (let i = 0; i < 3; i++) {
		const row = db.inboundPendingOldest(ORIGIN_KEY);
		if (!row) break;
		served.push(row.message_id);
		expect(db.inboundSteerAccepted({ messageId: row.message_id, epoch: 0, opRef: "gw-p-x" })).toBe(true);
	}
	expect(served).toEqual(["z-first", "m-second", "a-third"]);
	expect(db.inboundTurnRows("gw-p-x").map((row) => row.message_id)).toEqual(["z-first", "m-second", "a-third"]);
});

test("messages arriving while a turn is running become steers in arrival order", async () => {
	const db = await open();
	const now = Date.now();
	const opRef = "gw-p-running";
	db.inboundEnqueue({ ...message("m1", "런타임으로 다시 반영하고 체크해봐."), receivedAt: timestamp(now, 0) });

	expect(
		db.inboundBindTurn({
			messageId: "m1",
			originKey: ORIGIN_KEY,
			epoch: 0,
			opRef,
			sessionId: "session-1",
			dispatchedAt: timestamp(now, 1),
		}),
	).toMatchObject({
		message_id: "m1",
		state: "pending",
		turn_role: "trigger",
		turn_epoch: 0,
		turn_state: "bound",
		turn_op_ref: opRef,
		bound_session_id: "session-1",
		dispatched_at: timestamp(now, 1),
	});
	expect(db.inboundTurnAccept(opRef)).toBe(true);

	for (const [id, body, offset] of [
		["m2", "어이", 2],
		["m3", "ㅇㅑ", 3],
		["m4", "야", 4],
		["m5", "가재야?", 5],
	] as const) {
		db.inboundEnqueue({ ...message(id, body), receivedAt: timestamp(now, offset) });
		expect(db.inboundSteerAccepted({ messageId: id, epoch: 0, opRef })).toBe(true);
	}

	expect(
		db.inboundTurnRows(opRef).map((row) => [row.message_id, row.body, row.state, row.turn_role, row.turn_state]),
	).toEqual([
		["m1", "런타임으로 다시 반영하고 체크해봐.", "pending", "trigger", "accepted"],
		["m2", "어이", "done", "steer", "done"],
		["m3", "ㅇㅑ", "done", "steer", "done"],
		["m4", "야", "done", "steer", "done"],
		["m5", "가재야?", "done", "steer", "done"],
	]);
	expect(db.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
});

test("#247: completion keeps a delivery claim, and a claim replaces a no-delivery reason", async () => {
	const db = await open();
	for (const [id, opRef] of [
		["m1", "gw-p-claimed"],
		["m2", "gw-p-marked"],
	] as const) {
		db.inboundEnqueue(message(id, id));
		db.inboundBindTurn({ messageId: id, originKey: ORIGIN_KEY, epoch: id === "m1" ? 0 : 1, opRef, sessionId: "s" });
	}
	expect(db.inboundTurnClaimTerminal("gw-p-claimed", 0, "gw-t-a")).toBe("gw-t-a");
	expect(db.inboundTurnComplete("gw-p-claimed", "silent")).toBe(1);
	expect(db.inboundTurnRow("gw-p-claimed")?.terminal_delivery_id).toBe('{"0":"gw-t-a"}');

	db.inboundTurnMarkUnlinked("gw-p-marked", "silent");
	db.inboundTurnMarkUnlinked("gw-p-marked", "turn_failed");
	expect(db.inboundTurnRow("gw-p-marked")?.terminal_delivery_id).toBe('{"none":"silent"}');
	expect(db.inboundTurnClaimTerminal("gw-p-marked", 0, "gw-t-b")).toBe("gw-t-b");
	expect(db.inboundTurnRow("gw-p-marked")?.terminal_delivery_id).toBe('{"0":"gw-t-b"}');
});

test("the terminal link audit counts done triggers with a NULL link", async () => {
	const db = await open();
	db.inboundEnqueue(message("m1", "hello"));
	db.inboundBindTurn({ messageId: "m1", originKey: ORIGIN_KEY, epoch: 0, opRef: "gw-p-a", sessionId: "s" });
	expect(db.inboundTurnComplete("gw-p-a")).toBe(1);
	const since = new Date(Date.now() - 60_000).toISOString();
	expect(db.inboundTerminalLinkAudit(since)).toEqual({ done: 1, unlinked: 0 });
	const raw = new (await import("bun:sqlite")).Database(join(home, "gateway.db"));
	raw.query("UPDATE inbound_messages SET terminal_delivery_id = NULL").run();
	raw.close();
	expect(db.inboundTerminalLinkAudit(since)).toEqual({ done: 1, unlinked: 1 });
});

test("an old pending message remains eligible for a turn", async () => {
	const db = await open();
	const now = Date.now();
	db.inboundEnqueue({ ...message("m1", "hello"), receivedAt: timestamp(now, -24 * 60 * 60 * 1_000) });

	expect(db.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "m1",
		state: "pending",
		turn_role: null,
		turn_state: null,
	});
});

test("completed turns are never pending or nonterminal", async () => {
	const db = await open();
	const opRef = "gw-p-completed";
	db.inboundEnqueue(message("m1", "hello"));
	db.inboundBindTurn({ messageId: "m1", originKey: ORIGIN_KEY, epoch: 0, opRef, sessionId: "session-1" });
	expect(db.inboundTurnAccept(opRef)).toBe(true);
	expect(db.inboundTurnComplete(opRef)).toBe(1);

	expect(db.inboundTurnRow(opRef)).toMatchObject({
		state: "done",
		turn_role: "trigger",
		turn_state: "done",
		terminal_delivery_id: '{"none":"no_delivery"}',
	});
	expect(db.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
	expect(db.inboundNonterminalTurns(ORIGIN_KEY)).toEqual([]);
	expect(db.inboundPendingCount(ORIGIN_KEY)).toBe(0);
});

test("turn bindings are scoped per origin", async () => {
	const db = await open();
	const secondOrigin = "discord:dm:2";
	db.inboundEnqueue(message("m1", "a"));
	db.inboundEnqueue({ ...message("m2", "b"), originKey: secondOrigin });

	expect(db.inboundPendingOldest(ORIGIN_KEY)?.body).toBe("a");
	expect(db.inboundPendingOldest(secondOrigin)?.body).toBe("b");
	db.inboundBindTurn({
		messageId: "m1",
		originKey: ORIGIN_KEY,
		epoch: 0,
		opRef: "gw-p-origin-one",
		sessionId: "session-1",
	});
	db.inboundBindTurn({
		messageId: "m2",
		originKey: secondOrigin,
		epoch: 0,
		opRef: "gw-p-origin-two",
		sessionId: "session-2",
	});

	expect(db.inboundNonterminalTurns(ORIGIN_KEY)).toEqual([
		{
			originKey: ORIGIN_KEY,
			epoch: 0,
			state: "bound",
			opRef: "gw-p-origin-one",
			sessionId: "session-1",
			triggerMessageId: "m1",
		},
	]);
	expect(db.inboundNonterminalTurns(secondOrigin)).toEqual([
		{
			originKey: secondOrigin,
			epoch: 0,
			state: "bound",
			opRef: "gw-p-origin-two",
			sessionId: "session-2",
			triggerMessageId: "m2",
		},
	]);
});

test("discarding at /new touches only unbound pending rows and terminal completion moves both lifecycle columns", async () => {
	const db = await open();
	const now = Date.now();
	const cutoff = timestamp(now, 3);
	const opRef = "gw-p-discard";
	db.inboundEnqueue({ ...message("trigger", "start"), receivedAt: timestamp(now, 0) });
	db.inboundBindTurn({
		messageId: "trigger",
		originKey: ORIGIN_KEY,
		epoch: 0,
		opRef,
		sessionId: "session-1",
		dispatchedAt: timestamp(now, 0),
	});
	expect(db.inboundTurnAccept(opRef)).toBe(true);
	db.inboundEnqueue({ ...message("steer", "follow"), receivedAt: timestamp(now, 1) });
	expect(db.inboundSteerAccepted({ messageId: "steer", epoch: 0, opRef })).toBe(true);
	db.inboundEnqueue({ ...message("unbound", "discard"), receivedAt: timestamp(now, 2) });

	expect(db.inboundDiscardBefore(ORIGIN_KEY, cutoff)).toEqual(["unbound"]);
	expect(db.inboundTurnRows(opRef)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({ message_id: "trigger", state: "pending", turn_state: "accepted" }),
			expect.objectContaining({ message_id: "steer", state: "done", turn_role: "steer", turn_state: "done" }),
		]),
	);
	expect(db.inboundTurnComplete(opRef)).toBe(1);
	expect(db.inboundTurnRow(opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("one nonterminal trigger per epoch permits steers and retired epochs", async () => {
	const db = await open();
	const now = Date.now();
	const oldOpRef = "gw-p-old";
	const currentOpRef = "gw-p-current";
	db.inboundEnqueue({ ...message("a", "a"), receivedAt: timestamp(now, 0) });
	db.inboundBindTurn({
		messageId: "a",
		originKey: ORIGIN_KEY,
		epoch: 0,
		opRef: oldOpRef,
		sessionId: "session-old",
	});
	expect(db.inboundTurnAccept(oldOpRef)).toBe(true);
	db.inboundEnqueue({ ...message("steer", "mid-turn"), receivedAt: timestamp(now, 1) });
	expect(db.inboundSteerAccepted({ messageId: "steer", epoch: 0, opRef: oldOpRef })).toBe(true);
	db.inboundEnqueue({ ...message("c", "c"), receivedAt: timestamp(now, 2) });
	expect(
		db.inboundBindTurn({
			messageId: "c",
			originKey: ORIGIN_KEY,
			epoch: 1,
			opRef: currentOpRef,
			sessionId: "session-current",
		}),
	).toMatchObject({ message_id: "c", turn_role: "trigger", turn_epoch: 1, turn_state: "bound" });
	db.inboundEnqueue({ ...message("d", "d"), receivedAt: timestamp(now, 3) });

	expect(() =>
		db.inboundBindTurn({
			messageId: "d",
			originKey: ORIGIN_KEY,
			epoch: 1,
			opRef: "gw-p-conflict",
			sessionId: "session-conflict",
		}),
	).toThrow(InboundTurnConflictError);
	expect(db.inboundNonterminalTurns(ORIGIN_KEY)).toEqual([
		{
			originKey: ORIGIN_KEY,
			epoch: 0,
			state: "accepted",
			opRef: oldOpRef,
			sessionId: "session-old",
			triggerMessageId: "a",
		},
		{
			originKey: ORIGIN_KEY,
			epoch: 1,
			state: "bound",
			opRef: currentOpRef,
			sessionId: "session-current",
			triggerMessageId: "c",
		},
	]);
});

test("terminal delivery claims are per-part and first claimant wins", async () => {
	const db = await open();
	const opRef = "gw-p-terminal";
	db.inboundEnqueue(message("m1", "hello"));
	db.inboundBindTurn({ messageId: "m1", originKey: ORIGIN_KEY, epoch: 0, opRef, sessionId: "session-1" });
	expect(db.inboundTurnAccept(opRef)).toBe(true);

	expect(db.inboundTurnClaimTerminal(opRef, 0, "gw-t-a")).toBe("gw-t-a");
	expect(db.inboundTurnClaimTerminal(opRef, 0, "gw-t-b")).toBe("gw-t-a");
	expect(db.inboundTurnClaimTerminal(opRef, 1, "gw-t-c")).toBe("gw-t-c");
	expect(JSON.parse(db.inboundTurnRow(opRef)?.terminal_delivery_id ?? "{}")).toEqual({
		0: "gw-t-a",
		1: "gw-t-c",
	});
});

test("an accepted turn with a delivered steer requeues its trigger for fresh attempts", async () => {
	const db = await open();
	const now = Date.now();
	const opRef = "gw-p-requeue";
	const retryOpRef = "gw-p-requeue-retry";
	db.inboundEnqueue({ ...message("trigger", "start"), receivedAt: timestamp(now, 0) });
	db.inboundBindTurn({
		messageId: "trigger",
		originKey: ORIGIN_KEY,
		epoch: 0,
		opRef,
		sessionId: "session-1",
		dispatchedAt: timestamp(now, 0),
	});
	expect(db.inboundTurnAccept(opRef)).toBe(true);
	db.inboundEnqueue({ ...message("steer-1", "mid-turn"), receivedAt: timestamp(now, 1) });
	expect(db.inboundSteerAccepted({ messageId: "steer-1", epoch: 0, opRef })).toBe(true);
	expect(db.inboundTurnClaimTerminal(opRef, 0, "gw-t-requeue")).toBe("gw-t-requeue");

	expect(db.inboundTurnRequeue(opRef)).toBe(1);
	expect(db.freshTurnAttempt(ORIGIN_KEY, 0, "trigger")).toBe(1);
	expect(db.inboundTurnRow(opRef)).toBeUndefined();
	expect(db.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "trigger",
		state: "pending",
		turn_role: null,
		turn_epoch: null,
		turn_state: null,
		turn_op_ref: null,
		bound_session_id: null,
		dispatched_at: null,
		terminal_delivery_id: null,
	});
	expect(db.inboundPendingCount(ORIGIN_KEY)).toBe(1);
	expect(db.inboundTurnRows(opRef)).toEqual([
		expect.objectContaining({ message_id: "steer-1", state: "done", turn_role: "steer", turn_state: "done" }),
	]);
	expect(db.inboundNonterminalTurns(ORIGIN_KEY)).toEqual([]);

	db.inboundBindTurn({
		messageId: "trigger",
		originKey: ORIGIN_KEY,
		epoch: 0,
		opRef: retryOpRef,
		sessionId: "session-2",
		dispatchedAt: timestamp(now, 2),
	});
	expect(db.inboundTurnRequeue(retryOpRef)).toBe(2);
	expect(db.freshTurnAttempt(ORIGIN_KEY, 0, "trigger")).toBe(2);
});
