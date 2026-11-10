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
		channels: {
			"slack:C1": { engagement: "mention-open" },
			"slack:C2": { engagement: "closed" },
		},
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "answer") });
	attachTestBrokerOwnership(database, port, config.gjc.agentDir);
	const server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
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

		await event(message("6.0", "<@UBOT> /new", { channel: "C2" }));
		expect(database.bindings.getSessionRecord("slack/channel/C2")).toBeUndefined();
		expect(api.posts).toHaveLength(4);
		expect(port.sends).toHaveLength(2);
	} finally {
		adapter?.socket.stop();
		adapter?.recovery.stop();
		adapter?.gateway.stop();
		await client?.close();
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});
