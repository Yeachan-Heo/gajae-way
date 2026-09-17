import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AdapterAlreadyRunningError, AdapterLock } from "../../adapter-discord/src/lock";
import { ReconnectingGateway } from "../../adapter-slack/src/main";
import { GajaewayClient } from "../../sdk/src/index";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

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
const origin = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const engagement = { mentioned: true, group: true, authorId: "U1" };
async function settle() {
	for (let i = 0; i < 60; i++) await Bun.sleep(5);
}
async function fixture(channels: GatewayConfig["channels"], reply = "<script>&") {
	home = await mkdtemp(join(tmpdir(), "slack-rt-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		channels,
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = attachTestBrokerOwnership(
		database,
		sessionPortFromResponder({
			bind: async (key, epoch) => `session-${key}-${epoch}`,
			respond: async (_id, text) => {
				turns.push(text);
				return reply;
			},
		}),
		join(home, "agent"),
	);
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const posts: unknown[][] = [];
	const reactions: unknown[][] = [];
	const adapter = new ReconnectingGateway(config.socketPath, {
		async postMessage(...args: [string, string, string?]) {
			posts.push(args);
			return { ts: "9.0", channel: args[0] };
		},
		async addReaction(...args: [string, string, string]) {
			reactions.push(args);
		},
	});
	await adapter.connect();
	client = await GajaewayClient.connectSocket(config.socketPath);
	return { adapter, database, turns, posts, reactions, client };
}

test("RT-SLACK-20 unconfigured unmentioned Slack channel is context only", async () => {
	const f = await fixture({});
	const result = await f.adapter.requestInbound("C1:1.0", origin, "unaddressed", { ...engagement, mentioned: false });
	expect(result?.engaged).toBe(false);
	await settle();
	expect(f.turns).toEqual([]);
	expect(f.posts).toEqual([]);
	expect(f.database.deliveryRows()).toEqual([]);
});

test("RT-SLACK-21 mention-open engages and persona markup is escaped at Slack boundary", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } });
	expect((await f.adapter.requestInbound("C1:1.0", origin, "addressed", engagement))?.engaged).toBe(true);
	await settle();
	expect(f.turns).toHaveLength(1);
	expect(f.posts).toEqual([["C1", "&lt;script&gt;&amp;", undefined]]);
	expect(f.database.deliveryRows()[0]?.state).toBe("confirmed");
});

test("RT-SLACK-22 check reaction token delivers on Slack and is refused on Telegram", async () => {
	const f = await fixture(
		{ "slack:C1": { engagement: "mention-open" }, "telegram:C2": { engagement: "mention-open" } },
		"[REACT:✅]",
	);
	await f.adapter.requestInbound("C1:1.0", origin, "check", engagement);
	await settle();
	expect(f.reactions).toEqual([["C1", "1.0", "white_check_mark"]]);
	expect(f.database.deliveryRows()[0]?.state).toBe("confirmed");
	await f.client.request("chat.send", {
		origin: { platform: "telegram", kind: "channel", conversationId: "C2" },
		messageId: "2",
		text: "check",
		engagement,
	});
	await settle();
	expect(f.turns).toHaveLength(2);
	expect(
		f.database
			.deliveryRows()
			.filter((row) => row.origin_key.startsWith("telegram") && JSON.parse(row.payload_json).reaction),
	).toEqual([]);
	await expect(
		f.client.request("chat.react", {
			origin: { platform: "telegram", kind: "channel", conversationId: "C2" },
			targetMessageId: "2",
			emoji: "✅",
		}),
	).rejects.toThrow();
});

test("RT-SLACK-23 monitor and loopback cannot chat.react", async () => {
	const f = await fixture({});
	for (const blocked of [
		{ platform: "loopback", kind: "loopback", conversationId: "console" },
		{ platform: "monitor", kind: "eventtype", conversationId: "deploy" },
	])
		await expect(
			f.client.request("chat.react", { origin: blocked, targetMessageId: "C1:1.0", emoji: "✅" }),
		).rejects.toThrow();
	expect(f.database.deliveryRows()).toEqual([]);
});

test("RT-SLACK-24 thread and channel isolate sessions and route replies", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } }, "answer");
	await f.adapter.requestInbound("C1:1.0", origin, "channel", engagement);
	await f.adapter.requestInbound(
		"C1:2.0",
		{ platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" },
		"thread",
		engagement,
	);
	await settle();
	expect(
		f.database
			.sessionRows()
			.map((row) => JSON.parse(row.origin_ref_json ?? "{}").conversationId)
			.sort(),
	).toEqual(["C1", "C1:1.0"]);
	expect(f.posts).toContainEqual(["C1", "answer", undefined]);
	expect(f.posts).toContainEqual(["C1", "answer", "1.0"]);
	expect(f.database.deliveryRows().map((row) => row.state)).toEqual(["confirmed", "confirmed"]);
});

for (const explicit of [false, true]) {
	test(`RT-SLACK-34 real gateway threaded DM chunks ${explicit ? "explicit reply wins" : "default to inbound root"}`, async () => {
		const text = "x".repeat(4500);
		const f = await fixture({}, `${explicit ? "[REPLY:D1:9.0] " : ""}${text}`);
		const dm = { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" } as const;
		expect(
			(
				await f.adapter.requestInbound("D1:2.0", dm, "threaded dm", {
					...engagement,
					group: false,
					replyTo: { messageId: "D1:1.0", fromSelf: true },
				})
			)?.engaged,
		).toBe(true);
		await settle();
		expect(f.posts).toHaveLength(2);
		for (const post of f.posts) {
			expect(post[0]).toBe("D1");
			expect(post[2]).toBe(explicit ? "9.0" : "1.0");
		}
		expect(f.posts.map((post) => post[1]).join("")).toBe(text);
		expect(f.database.deliveryRows().every((row) => row.state === "confirmed")).toBe(true);
	});
}

test("RT-SLACK-34 plain channel reply does not inherit a thread", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } }, "plain reply");
	await f.adapter.requestInbound("C1:2.0", origin, "plain", engagement);
	await settle();
	expect(f.posts).toEqual([["C1", "plain reply", undefined]]);
});

test("RT-SLACK-33 discord stale lock has exactly one winner under 20 concurrent reclaims", async () => {
	const lockHome = await mkdtemp(join(tmpdir(), "slack-discord-lock-redteam-"));
	try {
		const path = join(lockHome, "adapter-discord.pid");
		await writeFile(path, "99999\n");
		const results = await Promise.allSettled(
			Array.from({ length: 20 }, (_, i) => AdapterLock.acquire(lockHome, { pid: i + 1, alive: () => false })),
		);
		const winners = results.filter((r) => r.status === "fulfilled");
		expect(winners).toHaveLength(1);
		for (const result of results)
			if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
		expect((await readFile(path, "utf8")).trim()).toBe(String(winners[0]?.value.pid));
	} finally {
		await rm(lockHome, { recursive: true, force: true });
	}
});
