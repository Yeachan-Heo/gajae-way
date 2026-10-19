import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import type { ScriptedSessionPort } from "./session-port.fake";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const THREAD = { platform: "discord", kind: "thread", conversationId: "thread-root", parentId: "channel" } as const;
const OTHER_THREAD = { ...THREAD, conversationId: "other-root" } as const;
const CHANNEL = { platform: "discord", kind: "channel", conversationId: "channel" } as const;
const OWNER_DM = { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: "owner" } as const;
const OWNER = "owner";
const SLACK_CHANNEL = { platform: "slack", kind: "channel", conversationId: "C0SHUTUP" } as const;
/** Root of a Slack thread started on the persona's own message; replies stay on the channel session. */
const SLACK_BOT_ROOT = "C0SHUTUP:1700000000.000100";
const SLACK_BOT_THREAD_KEY = originKey({
	platform: "slack",
	kind: "thread",
	conversationId: SLACK_BOT_ROOT,
	parentId: SLACK_CHANNEL.conversationId,
});

let directory = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

type Client = Awaited<ReturnType<typeof connect>>;

async function connect(socketPath: string): Promise<{ send(value: unknown): void; frames: any[]; close(): void }> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function waitUntil(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 400 && !predicate(); attempt++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

async function waitForTurn(
	f: { turns: string[]; port: ScriptedSessionPort; database: Store },
	count: number,
): Promise<void> {
	await waitUntil(() => f.turns.length >= count);
	const operation = f.port.sends[count - 1];
	if (!operation) throw new Error(`turn ${count} has no accepted send`);
	await waitUntil(() => f.database.inbox.turnRow(operation.opRef)?.turn_state === "done");
}

function config(home: string, engagement: "open" | "mention-open"): GatewayConfig {
	return {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		channels: {
			"discord:channel": { engagement, audience: "all" },
			"slack:C0SHUTUP": { engagement, audience: "all" },
		},
		dmPolicy: "open",
		ownerTarget: { origin: OWNER_DM },
	};
}

async function start(
	gatewayConfig: GatewayConfig,
	database: Store,
	port: ScriptedSessionPort,
	originalBind: ScriptedSessionPort["bind"],
	originalResume: ScriptedSessionPort["resume"],
): Promise<Client> {
	port.bind = originalBind;
	port.resume = originalResume;
	attachTestBrokerOwnership(database, port, join(gatewayConfig.home, "agent"));
	server = await startUnixServer({
		config: gatewayConfig,
		database,
		sessionPort: port,
		onStop: () => {
			port.bind = originalBind;
			port.resume = originalResume;
			database.close();
		},
	});
	const client = await connect(gatewayConfig.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitUntil(() => client.frames.length > 0);
	return client;
}

async function fixture(
	engagement: "open" | "mention-open",
	firstReply = "[SHUTUP]\nthis control response must not be delivered",
	firstTurnGate?: Promise<void>,
) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-thread-shutup-"));
	const gatewayConfig = config(directory, engagement);
	let database = await Store.open(gatewayConfig.dbPath);
	const turns: string[] = [];
	const port = sessionPortFromResponder({
		bind: async (key, epoch) => `session-${key}-${epoch}`,
		respond: async (_session, text) => {
			turns.push(text);
			if (turns.length === 1) await firstTurnGate;
			return turns.length === 1 ? firstReply : `normal reply ${turns.length}`;
		},
	});
	const originalBind = port.bind.bind(port);
	const originalResume = port.resume.bind(port);
	let client = await start(gatewayConfig, database, port, originalBind, originalResume);
	return {
		gatewayConfig,
		get database() {
			return database;
		},
		set database(next: Store) {
			database = next;
		},
		turns,
		port,
		originalBind,
		originalResume,
		get client() {
			return client;
		},
		set client(next: Client) {
			client = next;
		},
	};
}

async function send(
	client: Client,
	id: string,
	origin: typeof THREAD | typeof OTHER_THREAD | typeof CHANNEL | typeof OWNER_DM | typeof SLACK_CHANNEL,
	engagement: {
		mentioned: boolean;
		explicitMention?: boolean;
		group: boolean;
		authorId: string;
		authorIsBot?: boolean;
		replyTo?: { messageId: string; fromSelf: boolean };
	},
): Promise<any> {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: { origin, text: id, messageId: id, engagement },
	});
	await waitUntil(() => client.frames.some((frame) => frame.type === "response" && frame.id === id));
	return client.frames.find((frame) => frame.type === "response" && frame.id === id);
}

const threadMention = {
	mentioned: true,
	explicitMention: true,
	group: true,
	authorId: OWNER,
};

test("SHUTUP records muted follow-ups, survives restart, and is scoped to one thread", async () => {
	const f = await fixture("open");
	const mutedSkips: string[] = [];
	const info = spyOn(console, "info").mockImplementation((line) => mutedSkips.push(String(line)));
	try {
		await send(f.client, "mute-trigger", THREAD, threadMention);
		await waitForTurn(f, 1);
		await waitUntil(() => f.database.inbox.isThreadMuted(originKey(THREAD)));
		expect(f.client.frames.filter((frame) => frame.event === "chat.message")).toEqual([]);

		// Slack open-channel adapters mark all traffic `mentioned`; the separate explicitMention
		// signal ensures that this owner follow-up remains muted. Bots are covered by the same gate.
		const openChannelMessage = { mentioned: true, explicitMention: false, group: true, authorId: OWNER };
		const botMessage = {
			mentioned: true,
			explicitMention: false,
			group: true,
			authorId: "other-bot",
			authorIsBot: true,
		};
		expect((await send(f.client, "muted-owner-1", THREAD, openChannelMessage)).result.engaged).toBe(false);
		expect((await send(f.client, "muted-bot", THREAD, botMessage)).result.engaged).toBe(false);
		expect(f.turns).toHaveLength(1);
		expect(
			f.database.inbox
				.contextUnread(originKey(THREAD))
				.map((row) => row.message_id)
				.sort(),
		).toEqual(["muted-bot", "muted-owner-1"]);
		expect(
			mutedSkips
				.filter((line) => line.startsWith("gateway engagement-muted skip"))
				.map((line) => line.split(" ").at(-1)),
		).toEqual(["message=muted-owner-1", "message=muted-bot"]);

		f.client.close();
		await server?.stop();
		server = undefined;
		f.database = await Store.open(f.gatewayConfig.dbPath);
		expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(true);
		f.client = await start(f.gatewayConfig, f.database, f.port, f.originalBind, f.originalResume);
		expect((await send(f.client, "muted-owner-after-restart", THREAD, openChannelMessage)).result.engaged).toBe(false);
		expect(f.turns).toHaveLength(1);
		expect(
			f.database.inbox
				.contextUnread(originKey(THREAD))
				.map((row) => row.message_id)
				.sort(),
		).toEqual(["muted-bot", "muted-owner-1", "muted-owner-after-restart"]);

		expect(
			(await send(f.client, "other-thread", OTHER_THREAD, { ...openChannelMessage, mentioned: false })).result.engaged,
		).toBe(true);
		expect(
			(await send(f.client, "channel-root", CHANNEL, { ...openChannelMessage, mentioned: false })).result.engaged,
		).toBe(true);
		expect(
			(await send(f.client, "owner-dm", OWNER_DM, { ...openChannelMessage, mentioned: false, group: false })).result
				.engaged,
		).toBe(true);
		await waitUntil(() => f.turns.length === 4);
		await waitUntil(() => f.client.frames.filter((frame) => frame.event === "chat.message").length === 3);
		expect(
			f.client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text),
		).toEqual(["normal reply 2", "normal reply 3", "normal reply 4"]);
		expect(
			f.client.frames.some((frame) => frame.event === "chat.message" && frame.payload.text.includes("SHUTUP")),
		).toBe(false);
		expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(true);
	} finally {
		info.mockRestore();
		f.client.close();
	}
});

test("SHUTUP composes with reaction control tokens without leaking its marker", async () => {
	const f = await fixture("mention-open", "[REACT:👍][SHUTUP]");
	try {
		await send(f.client, "react-and-mute", THREAD, threadMention);
		await waitForTurn(f, 1);
		await waitUntil(() => f.database.inbox.isThreadMuted(originKey(THREAD)));
		const deliveries = f.client.frames.filter((frame) => frame.event === "chat.message");
		expect(deliveries.some((frame) => frame.payload.reaction?.emoji === "👍")).toBe(true);
		expect(deliveries.every((frame) => !String(frame.payload.text ?? "").includes("SHUTUP"))).toBe(true);
	} finally {
		f.client.close();
	}
});

test("SHUTUP mutes a Slack thread that is carried by its channel session", async () => {
	const f = await fixture("open");
	// Every reply under the persona's own Slack message carries replyTo.fromSelf and
	// stays on the channel origin (adapter-slack decideInbound).
	const botThreadReply = {
		mentioned: true,
		explicitMention: false,
		group: true,
		authorId: OWNER,
		replyTo: { messageId: SLACK_BOT_ROOT, fromSelf: true },
	};
	try {
		await send(f.client, "slack-mute-trigger", SLACK_CHANNEL, botThreadReply);
		await waitForTurn(f, 1);
		await waitUntil(() => f.database.inbox.isThreadMuted(SLACK_BOT_THREAD_KEY));
		expect(f.database.inbox.isThreadMuted(originKey(SLACK_CHANNEL))).toBe(false);

		expect((await send(f.client, "slack-muted-reply", SLACK_CHANNEL, botThreadReply)).result.engaged).toBe(false);
		expect(f.turns).toHaveLength(1);
		expect(f.database.inbox.isThreadMuted(SLACK_BOT_THREAD_KEY)).toBe(true);

		// The channel itself and other bot-rooted threads in it stay engaged.
		const channelRoot = { mentioned: true, explicitMention: false, group: true, authorId: OWNER };
		expect((await send(f.client, "slack-channel-root", SLACK_CHANNEL, channelRoot)).result.engaged).toBe(true);
		await waitForTurn(f, 2);
		const otherThread = { ...botThreadReply, replyTo: { messageId: "C0SHUTUP:1700000000.000200", fromSelf: true } };
		expect((await send(f.client, "slack-other-thread", SLACK_CHANNEL, otherThread)).result.engaged).toBe(true);
		await waitForTurn(f, 3);

		// A literal mention inside the muted thread unmutes it.
		const mention = { ...botThreadReply, explicitMention: true };
		expect((await send(f.client, "slack-unmute", SLACK_CHANNEL, mention)).result.engaged).toBe(true);
		expect(f.database.inbox.isThreadMuted(SLACK_BOT_THREAD_KEY)).toBe(false);
		await waitForTurn(f, 4);
		expect((await send(f.client, "slack-after-unmute", SLACK_CHANNEL, botThreadReply)).result.engaged).toBe(true);
	} finally {
		f.client.close();
	}
});

test("SHUTUP mutes the Slack thread of the message steered into the turn, not the trigger's", async () => {
	let finish!: () => void;
	const running = new Promise<void>((resolve) => {
		finish = resolve;
	});
	const f = await fixture("open", "[SHUTUP]", running);
	const threadB = "C0SHUTUP:1700000000.000300";
	const threadBKey = originKey({ platform: "slack", kind: "thread", conversationId: threadB, parentId: "C0SHUTUP" });
	try {
		const channelRoot = { mentioned: true, explicitMention: true, group: true, authorId: OWNER };
		await send(f.client, "slack-root-trigger", SLACK_CHANNEL, channelRoot);
		await waitUntil(() => f.turns.length === 1);
		const threadBReply = {
			mentioned: true,
			explicitMention: false,
			group: true,
			authorId: OWNER,
			replyTo: { messageId: threadB, fromSelf: true },
		};
		expect((await send(f.client, "slack-thread-b-steer", SLACK_CHANNEL, threadBReply)).result.engaged).toBe(true);
		await waitUntil(() => f.port.steers.length === 1);
		finish();
		await waitForTurn(f, 1);
		await waitUntil(() => f.database.inbox.isThreadMuted(threadBKey));
		expect(f.database.inbox.isThreadMuted(SLACK_BOT_THREAD_KEY)).toBe(false);
		expect((await send(f.client, "slack-thread-b-muted", SLACK_CHANNEL, threadBReply)).result.engaged).toBe(false);
	} finally {
		finish();
		f.client.close();
	}
});

test("an unmute commits atomically with the addressed message that carries it", async () => {
	const f = await fixture("mention-open");
	try {
		await send(f.client, "atomic-trigger", THREAD, threadMention);
		await waitForTurn(f, 1);
		await waitUntil(() => f.database.inbox.isThreadMuted(originKey(THREAD)));
		const unmute = spyOn(f.database.inbox, "unmuteThread").mockImplementation(() => {
			throw new Error("crash between admission and unmute");
		});
		const error = spyOn(console, "error").mockImplementation(() => {});
		try {
			f.client.send({
				v: "0.1",
				type: "request",
				id: "atomic-unmute-crash",
				verb: "chat.send",
				params: { origin: THREAD, text: "atomic-unmute", messageId: "atomic-unmute", engagement: threadMention },
			});
			await waitUntil(() =>
				f.client.frames.some((frame) => frame.type === "error" && frame.id === "atomic-unmute-crash"),
			);
		} finally {
			unmute.mockRestore();
			error.mockRestore();
		}
		// Nothing of the failed admission is durable, so the replayed message is admitted and unmutes.
		expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(true);
		const replay = await send(f.client, "atomic-unmute", THREAD, threadMention);
		expect(replay.result).toMatchObject({ engaged: true });
		expect(replay.result.turnId).not.toBeNull();
		expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(false);
		await waitForTurn(f, 2);
	} finally {
		f.client.close();
	}
});

for (const [label, addressed] of [
	["explicit mention", threadMention],
	// An adapter that predates explicitMention reports a literal mention only as `mentioned`.
	["legacy mention without explicitMention", { mentioned: true, group: true, authorId: OWNER }],
	[
		"reply to persona",
		{
			mentioned: false,
			explicitMention: false,
			group: true,
			authorId: OWNER,
			replyTo: { messageId: "persona-message", fromSelf: true },
		},
	],
] as const) {
	test(`a ${label} unmutes and resumes ordinary thread follow-ups`, async () => {
		const f = await fixture("mention-open");
		try {
			await send(f.client, `initial-${label}`, THREAD, threadMention);
			await waitForTurn(f, 1);
			await waitUntil(() => f.database.inbox.isThreadMuted(originKey(THREAD)));

			const response = await send(f.client, `unmute-${label}`, THREAD, addressed);
			expect(response.result.engaged).toBe(true);
			expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(false);
			await waitForTurn(f, 2);

			const followUp = await send(f.client, `follow-up-${label}`, THREAD, {
				mentioned: false,
				explicitMention: false,
				group: true,
				authorId: OWNER,
			});
			expect(followUp.result.engaged).toBe(true);
			await waitForTurn(f, 3);
			expect(f.database.inbox.isThreadMuted(originKey(THREAD))).toBe(false);
			expect(
				f.client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text),
			).toEqual(["normal reply 2", "normal reply 3"]);
		} finally {
			f.client.close();
		}
	});
}
