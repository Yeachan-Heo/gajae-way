import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { alertSilentOrigins } from "../src/ops/origin-silent";
import { Store } from "../src/store";

const OWNER: OriginRef = { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" };
const HELD = "discord/channel/held";
let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function open(): Promise<Store> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-origin-silent-"));
	database = await Store.open(join(home, "gateway.db"));
	return database;
}

function enqueue(store: Store, originKey: string, messageId: string, receivedAt: string): void {
	store.transaction((tx) =>
		store.inbox.enqueue(tx, { messageId, originKey, originRefJson: "{}", body: "hello", receivedAt }),
	);
}

function cycle(store: Store, nowMs: number, sent: ChatMessagePayload[]): void {
	alertSilentOrigins({
		database: store,
		delivery: new DeliveryService(store),
		ownerTarget: OWNER,
		send: (payload) => sent.push(payload),
		nowMs,
	});
}

test("a held origin alerts the owner exactly once across repeated cycles and restarts, then clears once (#688)", async () => {
	let store = await open();
	const now = Date.now();
	enqueue(store, HELD, "held-1", new Date(now - 20 * 60_000).toISOString());
	store.transaction((tx) =>
		store.inbox.bindTurn(tx, { messageId: "held-1", originKey: HELD, epoch: 0, opRef: "gw-p-held", sessionId: "s" }),
	);
	const sent: ChatMessagePayload[] = [];
	for (let index = 0; index < 3; index++) cycle(store, now + index * 60_000, sent);
	expect(sent).toHaveLength(1);
	expect(sent[0]!.text).toContain(`[origin silent] ${HELD}`);
	expect(sent[0]!.text).toContain("reason=operation_state_unknown opRef=gw-p-held");

	// A restarted gateway reads the open incident and stays quiet.
	store.close();
	store = database = await Store.open(join(home, "gateway.db"));
	cycle(store, now + 5 * 60_000, sent);
	expect(sent).toHaveLength(1);

	// The origin answers: one cleared notice, then nothing.
	store.transaction((tx) =>
		store.outbox.create(tx, { deliveryId: "reply", turnId: "t", originKey: HELD, payloadJson: "{}" }),
	);
	cycle(store, now + 6 * 60_000, sent);
	cycle(store, now + 7 * 60_000, sent);
	expect(sent).toHaveLength(2);
	expect(sent[1]!.text).toBe(`[origin recovered] ${HELD}: 다시 답하고 있습니다.`);
});

test("only a human message past the threshold on a chat origin alerts (#688)", async () => {
	const store = await open();
	const now = Date.now();
	const old = new Date(now - 30 * 60_000).toISOString();
	enqueue(store, "discord/channel/fresh", "fresh", new Date(now - 5 * 60_000).toISOString());
	store.transaction((tx) =>
		store.inbox.enqueue(tx, {
			messageId: "bot",
			originKey: "discord/channel/bot",
			originRefJson: "{}",
			body: "beep",
			receivedAt: old,
			engagementJson: JSON.stringify({ authorIsBot: true }),
		}),
	);
	enqueue(store, "discord/thread/t/parent=c", "muted", old);
	store.transaction((tx) => store.inbox.muteThread(tx, "discord/thread/t/parent=c"));
	enqueue(store, "work/task/lane", "lane", old);
	const sent: ChatMessagePayload[] = [];
	cycle(store, now, sent);
	expect(sent).toEqual([]);
});

test("the owner alert itself is not mistaken for the silent origin's answer (#688)", async () => {
	const store = await open();
	const now = Date.now();
	const ownerKey = "discord/dm/owner/peer=owner";
	enqueue(store, ownerKey, "owner-1", new Date(now - 20 * 60_000).toISOString());
	const sent: ChatMessagePayload[] = [];
	alertSilentOrigins({
		database: store,
		delivery: new DeliveryService(store),
		ownerTarget: OWNER,
		send: (payload) => sent.push(payload),
		nowMs: now,
	});
	alertSilentOrigins({
		database: store,
		delivery: new DeliveryService(store),
		ownerTarget: OWNER,
		send: (payload) => sent.push(payload),
		nowMs: now + 60_000,
	});
	expect(sent.map((payload) => payload.text.split(":")[0])).toEqual([`[origin silent] ${ownerKey}`]);
});
