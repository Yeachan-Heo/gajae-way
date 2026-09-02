import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase, InboundBatchConflictError } from "../src/store/db";

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
	originKey: "discord:dm:1",
	originRefJson: '{"platform":"discord"}',
	body,
});

test("the same platform message id is accepted exactly once", async () => {
	const db = await open();
	expect(db.inboundEnqueue(message("m1", "hello"))).toBe(true);
	expect(db.inboundEnqueue(message("m1", "hello"))).toBe(false);
	expect(db.inboundPendingCount("discord:dm:1")).toBe(1);
});

test("messages arriving while a turn is in flight are all retained in arrival order", async () => {
	const db = await open();
	// Simulate a turn holding the origin: the first message is claimed and still processing.
	db.inboundEnqueue(message("m1", "런타임으로 다시 반영하고 체크해봐."));
	const claimed = db.inboundClaimNext("discord:dm:1");
	expect(claimed?.message_id).toBe("m1");

	for (const [id, body] of [
		["m2", "어이"],
		["m3", "ㅇㅑ"],
		["m4", "야"],
		["m5", "가재야?"],
	])
		db.inboundEnqueue(message(id!, body!));

	expect(db.inboundPendingCount("discord:dm:1")).toBe(4);
	db.inboundComplete("m1");

	const drained: string[] = [];
	for (;;) {
		const next = db.inboundClaimNext("discord:dm:1");
		if (!next) break;
		drained.push(next.body);
		db.inboundComplete(next.message_id);
	}
	expect(drained).toEqual(["어이", "ㅇㅑ", "야", "가재야?"]);
});

test("a claimed message stranded by a killed turn is recovered and reclaimable", async () => {
	const db = await open();
	db.inboundEnqueue(message("m1", "hello"));
	expect(db.inboundClaimNext("discord:dm:1")?.message_id).toBe("m1");
	expect(db.inboundClaimNext("discord:dm:1")).toBeUndefined();

	expect(db.inboundRecoverProcessing()).toBe(1);
	expect(db.inboundClaimNext("discord:dm:1")?.message_id).toBe("m1");
});

test("completed messages are never recovered or reclaimed", async () => {
	const db = await open();
	db.inboundEnqueue(message("m1", "hello"));
	const claimed = db.inboundClaimNext("discord:dm:1");
	db.inboundComplete(claimed!.message_id);

	expect(db.inboundRecoverProcessing()).toBe(0);
	expect(db.inboundClaimNext("discord:dm:1")).toBeUndefined();
	expect(db.inboundPendingCount("discord:dm:1")).toBe(0);
});

test("claims are scoped per origin", async () => {
	const db = await open();
	db.inboundEnqueue(message("m1", "a"));
	db.inboundEnqueue({ ...message("m2", "b"), originKey: "discord:dm:2" });

	expect(db.inboundClaimNext("discord:dm:1")?.body).toBe("a");
	expect(db.inboundClaimNext("discord:dm:1")).toBeUndefined();
	expect(db.inboundClaimNext("discord:dm:2")?.body).toBe("b");
});

test("discarding at /new touches only unbatched pending rows and terminal completion moves both lifecycle columns", async () => {
	const db = await open();
	const cutoff = "2026-09-01T00:00:02.000Z";
	db.inboundEnqueue({ ...message("trigger", "start"), receivedAt: "2026-09-01T00:00:00.000Z" });
	db.inboundEnqueue({ ...message("member", "follow"), receivedAt: "2026-09-01T00:00:01.000Z" });
	const settled = db.inboundSettleBatch({
		originKey: "discord:dm:1",
		epoch: 0,
		cutoff,
		batchKey: "discord:dm:1|0|trigger|2026-09-01T00:00:02.000Z",
		opRef: "gw-p-0123456789abcdef0123456789abcdef",
	});
	expect(settled.map((item) => item.batch_role)).toEqual(["trigger", "member"]);
	expect(db.inboundBatchAccept(settled[0]!.batch_key!)).toBe(true);
	db.inboundEnqueue({ ...message("unbatched", "discard"), receivedAt: "2026-09-01T00:00:01.500Z" });

	expect(db.inboundDiscardBefore("discord:dm:1", cutoff)).toEqual(["unbatched"]);
	const accepted = db.inboundBatchRows(settled[0]!.batch_key!);
	expect(accepted).toEqual(
		expect.arrayContaining([
			expect.objectContaining({ message_id: "trigger", state: "pending", batch_state: "accepted" }),
			expect.objectContaining({ message_id: "member", state: "pending", batch_state: "accepted" }),
		]),
	);
	expect(db.inboundBatchComplete(settled[0]!.batch_key!)).toBe(2);
	expect(db.inboundBatchRows(settled[0]!.batch_key!)).toEqual(
		expect.arrayContaining([expect.objectContaining({ state: "done", batch_state: "done" })]),
	);
});

test("trigger-only uniqueness permits members and retired epochs but rejects a second current trigger", async () => {
	const db = await open();
	const settle = (id: string, epoch: number) =>
		db.inboundSettleBatch({
			originKey: "discord:dm:1",
			epoch,
			cutoff: "2026-09-01T00:00:02.000Z",
			batchKey: `discord:dm:1|${epoch}|${id}|cutoff`,
			opRef: `gw-p-${id.padEnd(32, "0")}`,
		});
	db.inboundEnqueue({ ...message("a", "a"), receivedAt: "2026-09-01T00:00:00.000Z" });
	db.inboundEnqueue({ ...message("b", "b"), receivedAt: "2026-09-01T00:00:01.000Z" });
	const old = settle("a", 0);
	db.inboundBatchAccept(old[0]!.batch_key!);
	db.inboundEnqueue({ ...message("c", "c"), receivedAt: "2026-09-01T00:00:02.000Z" });
	const current = settle("c", 1);
	expect(current).toHaveLength(1);
	db.inboundEnqueue({ ...message("d", "d"), receivedAt: "2026-09-01T00:00:02.000Z" });
	expect(() => settle("d", 1)).toThrow(InboundBatchConflictError);
});

test("an accepted batch with a delivered steer can still be released for a fresh turn (steer rows never block requeue)", async () => {
	const db = await open();
	const cutoff = "2026-09-01T00:00:02.000Z";
	db.inboundEnqueue({ ...message("trigger", "start"), receivedAt: "2026-09-01T00:00:00.000Z" });
	const batchKey = "discord:dm:1|0|trigger|2026-09-01T00:00:02.000Z";
	const opRef = "gw-p-0123456789abcdef0123456789abcdef";
	db.inboundSettleBatch({ originKey: "discord:dm:1", epoch: 0, cutoff, batchKey, opRef });
	expect(db.inboundBatchAccept(batchKey)).toBe(true);
	db.inboundEnqueue({ ...message("steer-1", "mid-turn"), receivedAt: "2026-09-01T00:00:05.000Z" });
	expect(db.inboundSteerAccepted({ messageId: "steer-1", batchKey, epoch: 0, opRef })).toBe(true);
	// Live finding (layofflabs-2): this threw "cannot be requeued" because the
	// delivered steer row is already done, and recovery stranded the origin.
	expect(db.inboundBatchRequeueFreshTurn(batchKey)).toBe(1);
	expect(db.inboundPendingCount("discord:dm:1")).toBe(1);
	expect(db.inboundNonterminalBatches("discord:dm:1")).toEqual([]);
});
