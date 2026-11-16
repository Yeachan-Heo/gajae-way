import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SlackWebApi } from "../../adapter-slack/src/api";
import { type SlackInboundMessage, startSlackAdapter } from "../../adapter-slack/src/main";
import type { WebSocketLike } from "../../adapter-slack/src/socket";
import { GajaewayClient } from "../../sdk/src/client";
import type { GatewayConfig } from "../src/config";
import { startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

class CommandApi extends SlackWebApi {
	readonly posts: { channel: string; text: string; threadTs?: string }[] = [];

	constructor() {
		super("unused", async () => {
			throw new Error("Slack command integration must not call an external API");
		});
	}

	override async authTest() {
		return { user_id: "UBOT", bot_id: "B1", user: "Moa", team_id: "T1", team: "Workspace" };
	}

	override async usersInfo(id: string) {
		return { id, name: id === "UBOT" ? "Moa" : "alice" };
	}

	override async conversationsInfo(id: string) {
		return { id, name: "general" };
	}

	override async connectionsOpen() {
		return { url: "wss://slack.invalid" };
	}

	override async postMessage(channel: string, text: string, threadTs?: string) {
		this.posts.push({ channel, text, ...(threadTs ? { threadTs } : {}) });
		return { channel, ts: `10.${this.posts.length}` };
	}

	override async addReaction() {}
	override async removeReaction() {}
	override async setThreadStatus() {}
}

class CommandSocket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	readonly acknowledgements: string[] = [];

	send(data: string) {
		this.acknowledgements.push(data);
	}

	close() {}
}

async function eventually(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate()).toBe(true);
}

test("real Slack envelopes reset only the addressed gateway session and retain authorization", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-command-gateway-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		ownerTarget: { origin: { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" } },
		restartNotice: { enabled: false },
		channels: {
			"slack:C1": { engagement: "mention-open" },
			"slack:C2": { engagement: "closed" },
		},
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "answer") });
	attachTestBrokerOwnership(database, port, config.gjc.agentDir);
	let exitCode: number | undefined;
	const server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		onStop: () => database.close(),
		exitProcess: (code) => {
			exitCode = code;
		},
		ownerRestartSupervisorWaitMs: 20,
		ownerRestartSupervisorCancelWaitMs: 30,
	});
	const api = new CommandApi();
	const socket = new CommandSocket();
	let adapter: Awaited<ReturnType<typeof startSlackAdapter>> | undefined;
	let client: GajaewayClient | undefined;
	try {
		adapter = await startSlackAdapter(
			{
				botToken: "xoxb-test",
				appToken: "xapp-test",
				botTokenFile: "bot",
				appTokenFile: "app",
				configPath: join(home, "adapter-slack.json"),
				gatewaySocket: config.socketPath,
			},
			{
				api,
				connectGateway: async () => {},
				recoveryCursorPath: join(home, "recovery.json"),
				socketFactory: () => {
					queueMicrotask(() => socket.onopen?.({}));
					return socket;
				},
			},
		);
		adapter.gateway.onConnected = undefined;
		client = await GajaewayClient.connectSocket(config.socketPath);
		adapter.gateway.adoptClient(client);
		const active = adapter;
		let sequence = 0;
		const event = async (message: SlackInboundMessage) => {
			const id = `event-${++sequence}`;
			socket.onmessage?.({
				data: JSON.stringify({
					type: "events_api",
					envelope_id: id,
					payload: { event_id: id, event: message },
				}),
			});
			await active.ingress.drain();
			expect(socket.acknowledgements.at(-1)).toBe(JSON.stringify({ envelope_id: id }));
		};
		const message = (ts: string, text: string, extra: Partial<SlackInboundMessage> = {}): SlackInboundMessage => ({
			type: "app_mention",
			channel: "C1",
			user: "U1",
			ts,
			text,
			...extra,
		});
		await event(message("1.0", "<@UBOT> first conversation"));
		await eventually(() => api.posts.length === 1);
		await event(message("3.0", "<@UBOT> unrelated conversation"));
		await eventually(() => api.posts.length === 2);
		const first = "slack/thread/C1:1.0/parent=C1";
		const other = "slack/thread/C1:3.0/parent=C1";
		const unrelated = database.bindings.getSessionRecord(other);
		expect(database.bindings.getSessionRecord(first)?.epoch).toBe(0);
		expect(unrelated?.epoch).toBe(0);
		expect(port.sends).toHaveLength(2);

		await event(message("4.0", "<@UBOT> /new", { thread_ts: "1.0", parent_user_id: "U1" }));
		await eventually(() => api.posts.length === 3);
		expect(database.bindings.getSessionRecord(first)?.epoch).toBe(1);
		expect(database.bindings.getSessionRecord(other)).toEqual(unrelated);
		expect(port.sends).toHaveLength(2);
		expect(api.posts[2]).toEqual({ channel: "C1", text: "Started a fresh session.", threadTs: "1.0" });

		await event(message("5.0", "<@UBOT> /reset"));
		await eventually(() => api.posts.length === 4);
		expect(api.posts[3]).toEqual({ channel: "C1", text: "Started a fresh session." });
		expect(database.bindings.getSessionRecord(first)?.epoch).toBe(1);
		expect(database.bindings.getSessionRecord(other)).toEqual(unrelated);
		expect(port.sends).toHaveLength(2);

		await event(message("6.0", "<@UBOT> /new", { channel: "C2", user: "U2" }));
		expect(database.bindings.getSessionRecord("slack/channel/C2")).toBeUndefined();
		expect(api.posts).toHaveLength(4);
		expect(port.sends).toHaveLength(2);

		const channel = "slack/channel/C1";
		const botRoot = { thread_ts: "20.0", parent_user_id: "UBOT" };
		await event(message("21.0", "<@UBOT> channel conversation", botRoot));
		await eventually(() => api.posts.length === 5);
		const channelBefore = database.bindings.getSessionRecord(channel);
		expect(channelBefore?.epoch).toBe(1);
		expect(port.sends).toHaveLength(3);

		await event(message("22.0", "<@UBOT> /model\tset fast", botRoot));
		await eventually(() => api.posts.length === 6);
		const modelText =
			"🦞 model set to **preset fast** for this conversation. It applies from the next turn on this same session.";
		expect(api.posts[5]).toEqual({
			channel: "C1",
			text: "🦞 model set to *preset fast* for this conversation. It applies from the next turn on this same session.",
			threadTs: "20.0",
		});
		expect(database.bindings.modelGet(channel)?.selection).toEqual({ preset: "fast" });
		expect(database.bindings.getSessionRecord(channel)).toEqual(channelBefore);
		expect(port.models.at(-1)?.selection).toEqual({ preset: "fast" });

		await event(message("23.0", "<@UBOT> /new", botRoot));
		await eventually(() => api.posts.length === 7);
		expect(api.posts[6]).toEqual({ channel: "C1", text: "Started a fresh session.", threadTs: "20.0" });
		expect(database.bindings.getSessionRecord(channel)?.epoch).toBe(2);
		await event(message("24.0", "<@UBOT> /reset", botRoot));
		await eventually(() => api.posts.length === 8);
		expect(api.posts[7]).toEqual({ channel: "C1", text: "Started a fresh session.", threadTs: "20.0" });
		expect(database.bindings.getSessionRecord(channel)?.epoch).toBe(3);

		await event(message("25.0", "<@UBOT> /model"));
		await eventually(() => api.posts.length === 9);
		const showText = "🦞 model: **preset fast** (this conversation)";
		expect(api.posts[8]).toEqual({ channel: "C1", text: "🦞 model: *preset fast* (this conversation)" });
		await event(message("26.0", "<@UBOT> /model set denied", { ...botRoot, channel: "C2", user: "U2" }));
		await event(message("27.0", "<@UBOT> /restart", { ...botRoot, user: "U2" }));
		expect(database.bindings.modelGet("slack/channel/C2")).toBeUndefined();
		expect(api.posts).toHaveLength(9);
		expect(database.bindings.getSessionRecord(first)?.epoch).toBe(1);
		expect(database.bindings.getSessionRecord(other)).toEqual(unrelated);
		expect(port.sends).toHaveLength(3);

		// A stalled supervisor forces restart cancellation without stopping the server.
		await eventually(() => database.outbox.list().every((row) => row.state === "confirmed"));
		await client.request("gateway.restart.prepare", { restartId: "stalled-supervisor", waitMs: 1000 });
		await event(message("28.0", "<@UBOT> /restart", botRoot));
		await eventually(() => api.posts.length === 11);
		const restartText = "Restarting the gateway; back in a few seconds.";
		const cancelledText =
			"Restart cancelled: the stack supervisor did not finish its current service command before the takeover deadline.";
		expect(api.posts[9]).toEqual({ channel: "C1", text: restartText, threadTs: "20.0" });
		expect(api.posts[10]).toEqual({ channel: "C1", text: cancelledText, threadTs: "20.0" });
		expect(exitCode).toBeUndefined();
		expect(database.bindings.getSessionRecord(channel)?.epoch).toBe(3);
		expect(database.bindings.getSessionRecord(other)).toEqual(unrelated);
		await client.request("gateway.restart.cancel", { restartId: "stalled-supervisor" });

		const channelOrigin = { platform: "slack", kind: "channel", conversationId: "C1" };
		const deliveries = database.outbox.list().map((row) => ({ row, payload: JSON.parse(row.payloadJson) }));
		for (const text of [modelText, restartText, cancelledText]) {
			const match = deliveries.find(({ payload }) => payload.text === text);
			expect(match?.row.originKey).toBe(channel);
			expect(match?.payload.origin).toEqual(channelOrigin);
			expect(match?.payload.replyToMessageId).toBe("C1:20.0");
		}
		const botRootResets = deliveries.filter(
			({ payload }) => payload.text === "Started a fresh session." && payload.replyToMessageId === "C1:20.0",
		);
		expect(botRootResets).toHaveLength(2);
		for (const { row, payload } of botRootResets) {
			expect(row.originKey).toBe(channel);
			expect(payload.origin).toEqual(channelOrigin);
		}
		const topLevelModel = deliveries.find(({ payload }) => payload.text === showText);
		expect(topLevelModel?.payload.origin).toEqual(channelOrigin);
		expect(topLevelModel?.payload.replyToMessageId).toBeUndefined();
		const topLevelReset = deliveries.find(
			({ row, payload }) =>
				row.originKey === channel && payload.text === "Started a fresh session." && !payload.replyToMessageId,
		);
		expect(topLevelReset?.payload.origin).toEqual(channelOrigin);

		// Malformed or empty reply metadata must not become a delivery target.
		const previousIds = new Set(deliveries.map(({ row }) => row.deliveryId));
		for (const messageId of [42, ""]) {
			await client.request("chat.send", {
				origin: channelOrigin,
				text: "/model",
				engagement: { mentioned: true, group: true, authorId: "U1", replyTo: { messageId } },
			});
		}
		await eventually(() => api.posts.length === 13);
		for (const post of api.posts.slice(11)) {
			expect(post).toEqual({ channel: "C1", text: "🦞 model: *preset fast* (this conversation)" });
		}
		const malformedReplies = database.outbox.list().filter((row) => !previousIds.has(row.deliveryId));
		expect(malformedReplies).toHaveLength(2);
		for (const row of malformedReplies) {
			expect(JSON.parse(row.payloadJson).replyToMessageId).toBeUndefined();
		}
		expect(port.sends).toHaveLength(3);
	} finally {
		adapter?.socket.stop();
		adapter?.recovery.stop();
		adapter?.gateway.stop();
		await client?.close();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});
