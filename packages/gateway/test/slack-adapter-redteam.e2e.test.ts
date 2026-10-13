import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatProgressPayload } from "@gajae-gateway/protocol";

import { liveGateway, stopLiveGatewaysAfterEach } from "../../adapter-slack/test/live-gateways";
import { GajaewayClient } from "../../sdk/src/index";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort, sessionPortFromResponder } from "./session-port.fake";

stopLiveGatewaysAfterEach();

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
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		channels,
		dmPolicy: "open",
	};
	const database = await Store.open(config.dbPath);
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
	const adapter = liveGateway(config.socketPath, {
		async postMessage(channel: string, text: string, threadTs?: string) {
			const args: [string, string, string?] = [channel, text, threadTs];
			posts.push(args);
			return { ts: "9.0", channel: args[0] };
		},
		async addReaction(...args: [string, string, string]) {
			reactions.push(args);
		},
		async removeReaction(_channel: string, _ts: string, _name: string) {
			// Mock remove
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
	expect(f.database.outbox.rows()).toEqual([]);
});

test("RT-SLACK-21 mention-open engages and persona markup is escaped at Slack boundary", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } });
	expect((await f.adapter.requestInbound("C1:1.0", origin, "addressed", engagement))?.engaged).toBe(true);
	await settle();
	expect(f.turns).toHaveLength(1);
	// A channel mention is answered in a thread rooted at the triggering message.
	expect(f.posts).toEqual([["C1", "&lt;script&gt;&amp;", "1.0"]]);
	expect(f.database.outbox.rows()[0]?.state).toBe("confirmed");
});

test("RT-SLACK-22 check reaction token delivers on Slack and is refused on Telegram", async () => {
	const f = await fixture(
		{ "slack:C1": { engagement: "mention-open" }, "telegram:C2": { engagement: "mention-open" } },
		"[REACT:✅]",
	);
	await f.adapter.requestInbound("C1:1.0", origin, "check", engagement);
	await settle();
	expect(f.reactions).toEqual([["C1", "1.0", "white_check_mark"]]);
	expect(f.database.outbox.rows()[0]?.state).toBe("confirmed");
	await f.client.request("chat.send", {
		origin: { platform: "telegram", kind: "channel", conversationId: "C2" },
		messageId: "2",
		text: "check",
		engagement,
	});
	await settle();
	expect(f.turns).toHaveLength(2);
	expect(
		f.database.outbox
			.rows()
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
	expect(f.database.outbox.rows()).toEqual([]);
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
		f.database.bindings
			.sessionRows()
			.map((row) => JSON.parse(row.origin_ref_json ?? "{}").conversationId)
			.sort(),
	).toEqual(["C1", "C1:1.0"]);
	// The channel turn (root 1.0) and the thread turn (thread C1:1.0) both land in thread 1.0.
	expect(f.posts).toEqual([
		["C1", "answer", "1.0"],
		["C1", "answer", "1.0"],
	]);
	expect(f.database.outbox.rows().map((row) => row.state)).toEqual(["confirmed", "confirmed"]);
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
		expect(f.database.outbox.rows().every((row) => row.state === "confirmed")).toBe(true);
	});
}

test("RT-SLACK-34 plain channel reply threads under the triggering message; an explicit target wins", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } }, "plain reply");
	await f.adapter.requestInbound("C1:2.0", origin, "plain", engagement);
	await settle();
	expect(f.posts).toEqual([["C1", "plain reply", "2.0"]]);
	const explicit = await fixture({ "slack:C1": { engagement: "mention-open" } }, "[REPLY:C1:9.0] elsewhere");
	await explicit.adapter.requestInbound("C1:3.0", origin, "plain", engagement);
	await settle();
	expect(explicit.posts).toEqual([["C1", "elsewhere", "9.0"]]);
});

for (const platform of ["slack", "discord", "telegram"] as const) {
	test(`RT-SLACK-46 ${platform} channel triggering root preserves platform routing on redelivery`, async () => {
		const f = await fixture({ [`${platform}:C1`]: { engagement: "mention-open" } }, "answer");
		const messageId = platform === "slack" ? "C1:7.0" : "7";
		const params = { origin: { ...origin, platform }, messageId, text: "recovered", engagement };
		await f.client.request("chat.send", params);
		await settle();
		await f.client.request("chat.send", params);
		await settle();
		const rows = f.database.outbox.rows();
		expect(rows).toHaveLength(1);
		const payload = JSON.parse(rows[0]?.payload_json);
		expect(payload.replyToMessageId).toBe(platform === "slack" ? messageId : undefined);
		if (platform === "slack") expect(f.posts).toEqual([["C1", "answer", "7.0"]]);
	});
}

test("RT-SLACK-46 slash synthetic message id cannot become Slack thread root", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } }, "answer");
	await f.adapter.requestInbound("slash-trigger", origin, "/new", engagement);
	await settle();
	expect(f.posts.length).toBeGreaterThan(0);
	for (const post of f.posts) expect(post[2]).toBeUndefined();
});

async function eventually(predicate: () => boolean) {
	for (let i = 0; i < 200 && !predicate(); i++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

test("RT-SLACK-50 real socket activity bounds burst details thinking writing and exactly one final", async () => {
	home = await mkdtemp(join(tmpdir(), "slack-activity-g4-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		channels: { "slack:C1": { engagement: "mention-open" } },
	};
	const database = await Store.open(config.dbPath);
	const port = attachTestBrokerOwnership(database, new ScriptedSessionPort(), join(home, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		progress: { firstAfterMs: 0, intervalMs: 20 },
		onStop: () => database.close(),
	});
	client = await GajaewayClient.connectSocket(config.socketPath);
	const progress: ChatProgressPayload[] = [];
	client.onChatProgress((payload) => progress.push(payload));
	await client.request("chat.send", { origin, messageId: "C1:1.0", text: "work", engagement });
	await eventually(() => port.sends.length === 1);
	const send = port.sends[0];
	if (!send) throw new Error("Expected an accepted turn");
	port.emitTool(send.sessionId, { toolName: "bash", intent: "Running tests" });
	await eventually(() =>
		progress.some(
			(p) => p.activity?.kind === "tool" && p.activity.label === "bash" && p.activity.detail === "Running tests",
		),
	);
	await Bun.sleep(25);
	const before = progress.length;
	// Ten starts in one synchronous burst fit inside the requested 10ms window.
	for (let i = 0; i < 10; i++) port.emitTool(send.sessionId, { toolName: `tool-${i}`, intent: `burst-${i}` });
	await Bun.sleep(10);
	expect(progress.length - before).toBeLessThanOrEqual(2);
	await Bun.sleep(25);
	port.emitTool(send.sessionId, { toolName: "bash", intent: `line\n\u0001${"x".repeat(300)}` });
	await eventually(() => progress.some((p) => p.activity?.detail?.startsWith("line")));
	const detail = progress.findLast((p) => p.activity?.detail?.startsWith("line"))?.activity?.detail;
	if (!detail) throw new Error("Expected sanitized tool detail");
	expect(detail.length).toBeLessThanOrEqual(120);
	expect([...detail].every((character) => character.charCodeAt(0) >= 32 && character.charCodeAt(0) !== 127)).toBe(true);
	await Bun.sleep(25);
	port.emitToolEnd(send.sessionId, "bash");
	await eventually(() => progress.some((p) => p.activity?.kind === "thinking"));
	await Bun.sleep(25);
	port.emitAssistant(send.sessionId, "writing a response");
	await eventually(() => progress.some((p) => p.activity?.kind === "writing"));
	port.complete(send.opRef, "done");
	await eventually(() => progress.some((p) => p.final));
	await Bun.sleep(50);
	expect(progress.filter((p) => p.final)).toHaveLength(1);
});

for (const silent of [false, true])
	test(`RT-SLACK-56 real gateway presence precedes progress and clears ${silent ? "silent final" : "thread delivery"}`, async () => {
		const { WorkingStatus } = await import("../../adapter-slack/src/status");
		home = await mkdtemp(join(tmpdir(), "slack-presence-g5-"));
		const config: GatewayConfig = {
			schemaVersion: 1,
			home,
			gjc: { agentDir: join(home, "agent") },
			configPath: join(home, "config.json"),
			socketPath: join(home, "gateway.sock"),
			dbPath: join(home, "gateway.db"),
			logVerbosity: "info",
			dmPolicy: "open",
			channels: { "slack:C1": { engagement: "mention-open" } },
		};
		const database = await Store.open(config.dbPath);
		const port = attachTestBrokerOwnership(database, new ScriptedSessionPort(), join(home, "agent"));
		server = await startUnixServer({
			config,
			database,
			sessionPort: port,
			progress: { firstAfterMs: 60000, intervalMs: 60000 },
			onStop: () => database.close(),
		});
		const added: string[][] = [];
		const removed: string[][] = [];
		const posts: unknown[][] = [];
		const api = {
			async addReaction(channel: string, ts: string, name: string) {
				added.push([channel, ts, name]);
			},
			async removeReaction(channel: string, ts: string, name: string) {
				removed.push([channel, ts, name]);
			},
			async setThreadStatus() {},
			async postMessage(channel: string, text: string, threadTs?: string) {
				posts.push([channel, text, threadTs]);
				return { channel, ts: "9.0" };
			},
		};
		const status = new WorkingStatus(api);
		const adapter = liveGateway(config.socketPath, api, undefined, status);
		await adapter.connect();
		client = await GajaewayClient.connectSocket(config.socketPath);
		const progress: ChatProgressPayload[] = [];
		client.onChatProgress((p) => progress.push(p));
		await adapter.requestInbound("C1:1.0", origin, "addressed", engagement);
		await eventually(() => added.length === 1 && port.sends.length === 1);
		expect(added).toEqual([["C1", "1.0", "hourglass_flowing_sand"]]);
		expect(progress).toEqual([]);
		port.complete(port.sends[0]?.opRef, silent ? "[SILENT]" : "answer");
		await eventually(() => progress.some((p) => p.final));
		await settle();
		expect(removed).toEqual(added);
		expect(posts).toEqual(silent ? [] : [["C1", "answer", "1.0"]]);
		expect(database.outbox.rows().every((row) => row.state === "confirmed")).toBe(true);
	});

test("RT-SLACK-63 edited channel response uses original root and slash new remains unthreaded", async () => {
	const f = await fixture({ "slack:C1": { engagement: "mention-open" } }, "answer");
	await f.adapter.requestInbound("C1:1.0", origin, "original", engagement);
	await settle();
	f.adapter.sendEdit("C1:1.0", origin, "edited", engagement);
	await settle();
	expect(f.posts).toHaveLength(2);
	for (const post of f.posts) expect(post[2]).toBe("1.0");
	f.posts.length = 0;
	await f.adapter.requestInbound("slash-trigger", origin, "/new", engagement);
	await settle();
	expect(f.posts.length).toBeGreaterThan(0);
	for (const post of f.posts) expect(post[2]).toBeUndefined();
});
