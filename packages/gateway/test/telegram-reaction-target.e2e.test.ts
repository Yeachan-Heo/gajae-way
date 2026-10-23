import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { subscribeTelegramDeliveries, TelegramAdapter, type TelegramUpdate } from "../../adapter-telegram/src/main";
import { TelegramAdapterState } from "../../adapter-telegram/src/state";
import { GajaewayClient } from "../../sdk/src/index";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromScript } from "./session-port.fake";

// The real Telegram adapter against a real gateway: the id the adapter keys an
// inbound message with must come back on the gateway's reaction deliveries as
// something setMessageReaction can address in that chat.

let home = "";
let server: GatewayServer | undefined;
let client: GajaewayClient | undefined;
afterEach(async () => {
	await client?.close();
	client = undefined;
	await server?.stop();
	server = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function until(condition: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 400 && !condition(); attempt++) await Bun.sleep(5);
}

async function fixture(reply: string, hold?: Promise<void>) {
	home = await mkdtemp(join(tmpdir(), "telegram-reaction-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		channels: { "telegram:-100123": { engagement: "mention-open" } },
		dmPolicy: "open",
	};
	const database = await Store.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = attachTestBrokerOwnership(
		database,
		sessionPortFromScript({
			bind: async (key, epoch) => `session-${key}-${epoch}`,
			respond: async (_id, text) => {
				turns.push(text);
				if (hold) await hold;
				return reply;
			},
		}),
		join(home, "agent"),
	);
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	client = await GajaewayClient.connectSocket(config.socketPath);
	const state = await TelegramAdapterState.load(home);
	const reactions: Array<[string, string, string | undefined]> = [];
	const sent: Array<[string, string]> = [];
	subscribeTelegramDeliveries(
		client,
		{
			sendMessage: async (chatId, text) => void sent.push([chatId, text]),
			setMessageReaction: async (chatId, messageId, emoji) => void reactions.push([chatId, messageId, emoji]),
		},
		state,
	);
	const adapter = new TelegramAdapter(
		state,
		"agent",
		"900",
		{ chats: {} },
		{ save: async () => Promise.reject(new Error("a text message never carries an attachment")) },
	);
	const gateway = client;
	let updateId = 0;
	const receive = (message: NonNullable<TelegramUpdate["message"]>) =>
		adapter.handleUpdate(gateway, { update_id: ++updateId, message });
	return { database, turns, reactions, sent, receive };
}

const group = { id: -100123, type: "supergroup", title: "war room" } as const;

test("a default [REACT] on a Telegram trigger reacts to that Telegram message", async () => {
	const f = await fixture("[REACT:👍] on it");
	await f.receive({ message_id: 7, chat: group, from: { id: 42 }, text: "ship it @agent" });
	await until(() => f.reactions.length > 0 && f.sent.length > 0);
	expect(f.reactions).toEqual([["-100123", "7", "👍"]]);
	expect(f.sent).toEqual([["-100123", "on it"]]);
	await until(() => f.database.outbox.rows().every((row) => row.state === "confirmed"));
	expect(f.database.outbox.rows().map((row) => row.state)).toEqual(["confirmed", "confirmed"]);
});

test("an accepted steer is acknowledged with 👀 on the steered Telegram message", async () => {
	let finish!: () => void;
	const running = new Promise<void>((resolve) => {
		finish = resolve;
	});
	const f = await fixture("done", running);
	const dm = { id: 22, type: "private" } as const;
	try {
		await f.receive({ message_id: 554, chat: dm, from: { id: 44 }, text: "first" });
		await until(() => f.turns.length > 0);
		await f.receive({ message_id: 555, chat: dm, from: { id: 44 }, text: "keep going" });
		await until(() => f.reactions.length > 0);
		expect(f.reactions).toEqual([["22", "555", "👀"]]);
		await until(() => f.database.outbox.rows().some((row) => row.state === "confirmed"));
		expect(f.database.outbox.rows().map((row) => row.state)).toEqual(["confirmed"]);
	} finally {
		finish();
	}
});

test("an unaddressed Telegram message is context the next turn reads under its chat-scoped id", async () => {
	const f = await fixture("[REACT:👍@-100123:6] noted");
	await f.receive({ message_id: 6, chat: group, from: { id: 43, first_name: "reader" }, text: "the build is red" });
	expect(f.turns).toEqual([]);
	await f.receive({ message_id: 7, chat: group, from: { id: 42 }, text: "see above @agent" });
	await until(() => f.reactions.length > 0 && f.sent.length > 0);
	expect(f.turns).toHaveLength(1);
	expect(f.turns[0]).toContain("msg:-100123:6): the build is red");
	expect(f.reactions).toEqual([["-100123", "6", "👍"]]);
	await until(() => f.database.outbox.rows().every((row) => row.state === "confirmed"));
	expect(f.database.outbox.rows().map((row) => row.state)).toEqual(["confirmed", "confirmed"]);
});

// The gateway's reaction budget keys targets by string, so a Telegram message must
// have exactly one spelling: a bare id would dodge duplicate/cap checks on add and
// fail to pair with the add on remove.
test("chat.react pairs add and remove on the one chat-scoped id and never reaches Telegram with a bare id", async () => {
	const f = await fixture("done");
	const dm = { platform: "telegram", kind: "dm", conversationId: "22", peerId: "44" } as const;
	await f.receive({ message_id: 555, chat: { id: 22, type: "private" }, from: { id: 44 }, text: "hello" });
	await until(() => f.sent.length > 0);
	const gateway = client as GajaewayClient;
	await gateway.request("chat.react", { origin: dm, targetMessageId: "22:555", emoji: "👍" });
	await until(() => f.reactions.length > 0);
	expect(f.reactions).toEqual([["22", "555", "👍"]]);
	// The add was keyed `22:555`; a remove spelled `555` names nothing the persona added.
	await expect(
		gateway.request("chat.react", { origin: dm, targetMessageId: "555", emoji: "👍", action: "remove" }),
	).rejects.toThrow("the persona did not add this reaction");
	// A bare add is admitted by the gateway but fails definitively at the adapter.
	const bare = await gateway.request<{ deliveryId: string }>("chat.react", {
		origin: dm,
		targetMessageId: "555",
		emoji: "👍",
	});
	const bareRow = () => f.database.outbox.rows().find((row) => row.delivery_id === bare.deliveryId);
	await until(() => (bareRow()?.attempts ?? 0) > 0);
	// A definitive (non-ambiguous) failure, recorded without any Telegram call.
	expect(bareRow()?.attempts).toBe(1);
	expect(bareRow()?.state).toBe("pending");
	expect(f.reactions).toEqual([["22", "555", "👍"]]);
	await gateway.request("chat.react", { origin: dm, targetMessageId: "22:555", emoji: "👍", action: "remove" });
	await until(() => f.reactions.length > 1);
	expect(f.reactions).toEqual([
		["22", "555", "👍"],
		["22", "555", undefined],
	]);
});
