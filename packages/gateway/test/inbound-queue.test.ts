import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";

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
