import { AsyncLocalStorage } from "node:async_hooks";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { expect, test } from "bun:test";
import { DiscordOutbox, discordChunkWireNonce, discordDedupeKey, discordWireNonce } from "../../src/adapter/discord/outbox";
import { loadDiscordAdapterConfig, type DiscordAdapterConfig } from "../../src/adapter/discord/config";
import { loadWayProfile } from "../../src/profile";

import { startDiscordAdapter } from "../../src/adapter/discord/main";
import { DiscordRouteHandler, type DiscordRoute } from "../../src/adapter/discord/route";

import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";
import { DiscordFixture, DiscordFixtureClock } from "../fixtures/discord-fixture";
import { createExternalGateway, eventually, type ExternalGateway, type ExternalGatewaySurface } from "../helpers/external-gateway";


const gatewayScope = new AsyncLocalStorage<ExternalGateway[]>();

type ExternalTestBody = () => void | Promise<void>;

let externalTestTail: Promise<void> = Promise.resolve();

function externalTest(name: string, body: ExternalTestBody, timeoutMs?: number): void {
	test(name, async () => {
		let release: (() => void) | undefined;
		const previous = externalTestTail;
		externalTestTail = new Promise<void>(resolve => {
			release = resolve;
		});
		await previous;
		const gateways: ExternalGateway[] = [];
		try {
			await gatewayScope.run(gateways, body);
		} finally {
			for (const active of gateways.splice(0)) await active.stop();
			release?.();
		}
	}, Math.max(timeoutMs ?? 0, 60_000));
}

async function gateway(
	knownSurfaces: readonly ExternalGatewaySurface[] = [{ id: "discord:guest-channel", platform: "discord", kind: "channel" }],
): Promise<ExternalGateway> {
	const active = await createExternalGateway({
		ownerSurface: { id: "discord:owner-dm", platform: "discord", kind: "dm" },
		knownSurfaces,
	});
	const gateways = gatewayScope.getStore();
	if (!gateways) throw new Error("gateway() must run inside externalTest().");
	gateways.push(active);
	return active;
}


const BOT_USER_ID = "999999999999999999";
const OWNER_ROUTE = { channelId: "123456789012345678", surfaceId: "discord:owner-dm", kind: "dm" } as const;
const TYPING_CHANNEL_ID = OWNER_ROUTE.channelId;
const TYPING_ROUTE = OWNER_ROUTE;
const GUILD_A_ROUTE = { channelId: "222222222222222222", surfaceId: "discord:guild-a", kind: "channel", engagement: "always" } as const;
const GUILD_B_ROUTE = { channelId: "333333333333333333", surfaceId: "discord:guild-b", kind: "channel", engagement: "always" } as const;
const MENTION_ROUTE = { channelId: "444444444444444444", surfaceId: "discord:guild-mention", kind: "channel" } as const;


async function startRoutedFixtureAdapter(
	fixture: DiscordFixture,
	rpc: JsonRpcClient,
	routes: readonly DiscordRoute[],
	options: {
		readonly unattributedDelivery?: "owner-dm" | "suppress";
		readonly blockedAuthorIds?: readonly string[];
		readonly allowBots?: boolean;

		readonly unattributedRoute?: DiscordRoute;
		readonly onError?: (error: Error) => void;
		readonly onDiagnostic?: (message: string) => void;
		readonly typingKeepaliveClock?: DiscordFixtureClock;

	} = {},
) {
	const unattributedDelivery = options.unattributedDelivery ?? "owner-dm";
	const config: DiscordAdapterConfig = {
		rpcSocketPath: "/tmp/gajaeway-discord-routes-fixture.sock",
		token: "fixture-token",
		routes,
		blockedAuthorIds: options.blockedAuthorIds ?? [],
		allowBots: options.allowBots,
		unattributedDelivery,

		...(options.unattributedRoute === undefined ? {} : { unattributedRoute: options.unattributedRoute }),
		ackBudgetMs: 2_000,
		claimTtlMs: 5_000,
		readWaitMs: 0,
	};
	return await startDiscordAdapter(config, {
		rpcConnect: async () => rpc,
		platformFactory: () => fixture,
		...(options.onError === undefined ? {} : { onError: options.onError }),
		...(options.onDiagnostic === undefined ? {} : { onDiagnostic: options.onDiagnostic }),
		...(options.typingKeepaliveClock === undefined ? {} : { typingKeepaliveClock: options.typingKeepaliveClock }),
	});
}

function nonClosingRpc(rpc: JsonRpcClient): JsonRpcClient {
	return { request: async (method, params, options) => await rpc.request(method, params, options), close() {} };
}


function outbox(
	gatewayUnderTest: ExternalGateway,
	fixture: DiscordFixture,
	hooks: ConstructorParameters<typeof DiscordOutbox>[0]["hooks"] = undefined,
	onDiagnostic?: (message: string) => void,
): DiscordOutbox {
	return new DiscordOutbox({
		rpc: gatewayUnderTest.client,
		platform: fixture,
		routes: [OWNER_ROUTE],
		unattributedDelivery: "owner-dm",
		unattributedRoute: OWNER_ROUTE,

		claimTtlMs: 5_000,
		readWaitMs: 0,
		hooks,
		onDiagnostic,
	});
}



function typingAdapterRpc(
	accepted: boolean,
	acceptedResponse: Record<string, unknown> = accepted ? { accepted: true, journal_head_cursor: "1:0" } : { accepted: false },
): JsonRpcClient {

	let claimCount = 0;
	return {
		async request(method): Promise<JsonRpcResponse> {
			switch (method) {
				case "way.health":
					return typingRpcResult({ status: "healthy", state: "running" });
				case "main.submit":
					return typingRpcResult(acceptedResponse);
				case "consumer.claim":
					claimCount += 1;
					return typingRpcResult({ claim_id: `typing-claim-${claimCount}`, cursor: "1:0", expires_at: Date.now() + 5_000 });
				case "main.events.read":
					return typingRpcResult({ events: [], next_cursor: "1:0" });
				case "consumer.commit":
					return typingRpcResult({});
				default:
					throw new Error(`Unexpected typing fixture RPC method: ${method}`);
			}
		},
		close() {},
	};
}

function typingRpcResult(result: unknown): JsonRpcResponse {
	return { jsonrpc: "2.0", id: 1, result };
}

async function startTypingFixtureAdapter(
	fixture: DiscordFixture,
	clock: DiscordFixtureClock,
	rpc: JsonRpcClient,
	onError: (error: Error) => void = () => undefined,
) {
	return await startDiscordAdapter(
		{
			rpcSocketPath: "/tmp/gajaeway-discord-typing-fixture.sock",
			token: "fixture-token",
			routes: [TYPING_ROUTE],
			unattributedDelivery: "owner-dm",
			blockedAuthorIds: [],

			unattributedRoute: TYPING_ROUTE,
			ackBudgetMs: 2_000,
			claimTtlMs: 5_000,
			readWaitMs: 0,
		},
		{
			rpcConnect: async () => rpc,
			platformFactory: () => fixture,
			typingKeepaliveClock: clock,
			onError,
		},
	);
}

async function advanceTypingClock(clock: DiscordFixtureClock, milliseconds: number): Promise<void> {
	clock.advanceTimersBy(milliseconds);
	await clock.flushAsync();
}

async function advanceTypingIntervals(clock: DiscordFixtureClock, count: number): Promise<void> {
	for (let index = 0; index < count; index += 1) await advanceTypingClock(clock, 8_000);
}


test("Discord wire nonce is deterministic, distinct, and exactly 96 bits", () => {
	const firstKey = discordDedupeKey("discord:owner-dm", "1");
	const secondKey = discordDedupeKey("discord:owner-dm", "2");
	const firstNonce = discordWireNonce(firstKey);
	expect(firstNonce).toMatch(/^[a-f0-9]{24}$/);
	expect(discordWireNonce(firstKey)).toBe(firstNonce);
	expect(discordWireNonce(secondKey)).not.toBe(firstNonce);
});

test("Discord config normalizes route tables and legacy single-route fields", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-discord-routes-"));
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profile = (adapter: string): string => `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "discord:owner-dm"
platform = "discord"
kind = "dm"

[[surfaces.known]]
id = "discord:guild-a"
platform = "discord"
kind = "channel"

${adapter}`;
	try {
		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]
token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"
unattributed_delivery = "suppress"
blocked_author_ids = ["777777777777777777"]


[[adapter.discord.routes]]
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
kind = "dm"

[[adapter.discord.routes]]
channel_id = "222222222222222222"
surface_id = "discord:guild-a"
`),
		);
		const tableConfig = loadDiscordAdapterConfig({
			profile: loadWayProfile(profilePath),
			environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
		});
		expect(tableConfig).toMatchObject({
			routes: [OWNER_ROUTE, { channelId: GUILD_A_ROUTE.channelId, surfaceId: GUILD_A_ROUTE.surfaceId, kind: "channel", engagement: "mention" }],
			blockedAuthorIds: ["777777777777777777"],
			unattributedDelivery: "suppress",
		});
		expect(tableConfig.unattributedRoute).toBeUndefined();


		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]
token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"

[[adapter.discord.routes]]
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
kind = "dm"

[[adapter.discord.routes]]
channel_id = "222222222222222222"
surface_id = "discord:guild-a"
engagement = "always"
`),
		);
		const alwaysConfig = loadDiscordAdapterConfig({
			profile: loadWayProfile(profilePath),
			environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
		});
		expect(alwaysConfig.routes).toEqual([OWNER_ROUTE, GUILD_A_ROUTE]);

		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]
token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
engagement = "mention"
`),
		);
		expect(() =>
			loadDiscordAdapterConfig({
				profile: loadWayProfile(profilePath),
				environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
			}),
		).toThrow("not configurable for Discord dm routes");


		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]
token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
`),
		);
		const legacyConfig = loadDiscordAdapterConfig({
			profile: loadWayProfile(profilePath),
			environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
		});
		expect(legacyConfig).toMatchObject({
			routes: [OWNER_ROUTE],
			blockedAuthorIds: [],
			unattributedDelivery: "owner-dm",
			unattributedRoute: OWNER_ROUTE,
		});
		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]

token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"
allowBots = false

[[adapter.discord.routes]]
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
kind = "dm"

[[adapter.discord.routes]]
channel_id = "222222222222222222"
surface_id = "discord:guild-a"
groupPolicy = "open"
`),
		);
		const openConfig = loadDiscordAdapterConfig({
			profile: loadWayProfile(profilePath),
			environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
		});
		expect(openConfig.allowBots).toBe(false);
		expect(openConfig.routes).toContainEqual(expect.objectContaining({ surfaceId: GUILD_A_ROUTE.surfaceId, groupPolicy: "open" }));

		fs.writeFileSync(
			profilePath,
			profile(`[adapter.discord]
token_env = "GAJAEWAY_DISCORD_BOT_TOKEN"

[[adapter.discord.routes]]
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
kind = "dm"

[[adapter.discord.routes]]
channel_id = "222222222222222222"
surface_id = "discord:guild-a"
groupPolicy = "mention"
engagement = "always"
`),
		);
		expect(() =>
			loadDiscordAdapterConfig({
				profile: loadWayProfile(profilePath),
				environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
			}),
		).toThrow("conflicts with legacy engagement");
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
});

externalTest("Discord groupPolicy open admits other bots while self-authored and allowBots=false traffic stay refused", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	await fixture.connect();
	try {
		const openRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [{ ...GUILD_A_ROUTE, groupPolicy: "open" }],
			botUserId: BOT_USER_ID,
			allowBots: true,
		});
		expect(
			await openRoute.handle({ id: "open-user", channelId: GUILD_A_ROUTE.channelId, text: "open user prompt", authorId: "333333333333333333", acceptedAt: Date.now() }),
		).toBe(true);
		const mentionRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [{ channelId: GUILD_A_ROUTE.channelId, surfaceId: GUILD_A_ROUTE.surfaceId, kind: "channel", groupPolicy: "mention" }],
			botUserId: BOT_USER_ID,
		});
		expect(
			await mentionRoute.handle({ id: "mention-user", channelId: GUILD_A_ROUTE.channelId, text: "mention only prompt", authorId: "444444444444444444", acceptedAt: Date.now() }),
		).toBe(false);
		expect(
			await openRoute.handle({ id: "bot-allowed", channelId: GUILD_A_ROUTE.channelId, text: "bot prompt", authorId: "111111111111111111", authorBot: true, acceptedAt: Date.now() }),
		).toBe(true);
		expect(
			await openRoute.handle({ id: "bot-self", channelId: GUILD_A_ROUTE.channelId, text: "self prompt", authorId: BOT_USER_ID, authorBot: true, acceptedAt: Date.now() }),
		).toBe(false);
		const closedRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [{ ...GUILD_A_ROUTE, groupPolicy: "open" }],
			botUserId: BOT_USER_ID,
			allowBots: false,
		});
		expect(
			await closedRoute.handle({ id: "bot-blocked", channelId: GUILD_A_ROUTE.channelId, text: "blocked bot", authorId: "222222222222222222", authorBot: true, acceptedAt: Date.now() }),
		).toBe(false);
		expect(gatewayUnderTest.fixture.commands()).toEqual([expect.objectContaining({ text: "open user prompt" }), expect.objectContaining({ text: "bot prompt" })]);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord mention-mode channels engage direct bot mentions and strip only a leading mention", async () => {
	const gatewayUnderTest = await gateway([{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, MENTION_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
	});
	try {
		await fixture.emitMessage({
			id: "mention-content",
			channelId: MENTION_ROUTE.channelId,
			text: `<@${BOT_USER_ID}> clean mention prompt`,
			authorId: "111111111111111111",
		});
		await fixture.emitMessage({
			id: "mention-array",
			channelId: MENTION_ROUTE.channelId,
			text: "metadata direct mention prompt",
			authorId: "111111111111111111",
			mentionedUserIds: [BOT_USER_ID],
		});
		await eventually(
			() => (gatewayUnderTest.fixture.commands().length === 2 ? true : undefined),
			"direct bot mentions were not admitted",
		);
		expect(gatewayUnderTest.fixture.commands().map(command => command.text)).toEqual(["clean mention prompt", "metadata direct mention prompt"]);
		expect(fixture.acknowledgements.map(acknowledgement => acknowledgement.channelId)).toEqual([
			MENTION_ROUTE.channelId,
			MENTION_ROUTE.channelId,
		]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord mention-mode channels ignore role and everyone mentions", async () => {
	const gatewayUnderTest = await gateway([{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const diagnostics: string[] = [];
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, MENTION_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		onDiagnostic: message => diagnostics.push(message),
	});
	try {
		await fixture.emitMessage({
			id: "role-mention",
			channelId: MENTION_ROUTE.channelId,
			text: `<@&${BOT_USER_ID}> role mention must not engage`,
			authorId: "111111111111111111",
		});
		await fixture.emitMessage({
			id: "here-mention",
			channelId: MENTION_ROUTE.channelId,
			text: "@here here mention must not engage",
			authorId: "111111111111111111",
		});
		await fixture.emitMessage({
			id: "everyone-mention",
			channelId: MENTION_ROUTE.channelId,
			text: "@everyone everyone mention must not engage",
			authorId: "111111111111111111",
		});
		expect(gatewayUnderTest.fixture.commands()).toEqual([]);
		expect(fixture.acknowledgements).toEqual([]);
		expect(diagnostics).toEqual([expect.stringContaining("unengaged mention-mode")]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord mention-mode channels engage replies to the bot but not other users", async () => {
	const gatewayUnderTest = await gateway([{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const diagnostics: string[] = [];

	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, MENTION_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		onDiagnostic: message => diagnostics.push(message),

	});
	try {
		fixture.setReferencedMessageAuthor(MENTION_ROUTE.channelId, "555555555555555555", BOT_USER_ID);

		await fixture.emitMessage({
			id: "reply-to-bot",
			channelId: MENTION_ROUTE.channelId,
			text: "reply to bot prompt",
			authorId: "111111111111111111",
			messageReference: { channelId: MENTION_ROUTE.channelId, messageId: "555555555555555555" },

		});
		await fixture.emitMessage({
			id: "reply-to-other",
			channelId: MENTION_ROUTE.channelId,
			text: "reply to other must not engage",
			authorId: "111111111111111111",
			messageReference: { channelId: MENTION_ROUTE.channelId, messageId: "666666666666666666" },
			referencedMessageAuthorId: "222222222222222222",
		});
		await eventually(
			() => (gatewayUnderTest.fixture.commands().length === 1 ? true : undefined),
			"reply to the bot was not admitted",
		);
		expect(gatewayUnderTest.fixture.commands().map(command => command.text)).toEqual(["reply to bot prompt"]);
		expect(fixture.messageAuthorLookups).toEqual([{ channelId: MENTION_ROUTE.channelId, messageId: "555555555555555555" }]);
		expect(fixture.acknowledgements.map(acknowledgement => acknowledgement.channelId)).toEqual([MENTION_ROUTE.channelId]);
		expect(diagnostics).toEqual([expect.stringContaining("unengaged mention-mode")]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord mention-mode channels fail closed when a reply reference cannot resolve", async () => {
	const gatewayUnderTest = await gateway([{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const diagnostics: string[] = [];
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, MENTION_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		onDiagnostic: message => diagnostics.push(message),
	});
	try {
		await fixture.emitMessage({
			id: "reply-reference-missing",
			channelId: MENTION_ROUTE.channelId,
			text: "unresolvable reply must not engage",
			authorId: "111111111111111111",
			messageReference: { channelId: MENTION_ROUTE.channelId, messageId: "777777777777777778" },
		});
		expect(gatewayUnderTest.fixture.commands()).toEqual([]);
		expect(fixture.acknowledgements).toEqual([]);
		expect(fixture.messageAuthorLookups).toEqual([{ channelId: MENTION_ROUTE.channelId, messageId: "777777777777777778" }]);
		expect(diagnostics).toEqual([expect.stringContaining("unresolved reply reference")]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord always-on channel overrides, routed threads, and DMs bypass mention gating", async () => {
	const gatewayUnderTest = await gateway([
		{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" },
		{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" },
	]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE, MENTION_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
	});
	const threadChannelId = "888888888888888888";
	try {
		fixture.setThreadParent(threadChannelId, MENTION_ROUTE.channelId);
		await fixture.emitMessage({ id: "always-channel", channelId: GUILD_A_ROUTE.channelId, text: "always-on channel prompt", authorId: "111111111111111111" });
		await fixture.emitMessage({ id: "always-thread", channelId: threadChannelId, text: "thread prompt without a mention", authorId: "111111111111111111" });
		await fixture.emitMessage({ id: "always-dm", channelId: OWNER_ROUTE.channelId, text: "dm prompt without a mention", authorId: "111111111111111111" });
		await eventually(
			() => (gatewayUnderTest.fixture.commands().length === 3 ? true : undefined),
			"always-on channel, thread, or DM ingress was not admitted",
		);
		expect(gatewayUnderTest.fixture.commands().map(command => command.text)).toEqual([
			"always-on channel prompt",
			"thread prompt without a mention",
			"dm prompt without a mention",
		]);
		expect(fixture.acknowledgements.map(acknowledgement => acknowledgement.channelId)).toEqual([
			GUILD_A_ROUTE.channelId,
			threadChannelId,
			OWNER_ROUTE.channelId,
		]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord blocks configured authors on DM and channel routes before admission", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const diagnostics: string[] = [];
	const blockedAuthorId = "777777777777777777";
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		blockedAuthorIds: [blockedAuthorId],
		onDiagnostic: message => diagnostics.push(message),
	});
	try {
		await fixture.emitMessage({ id: "blocked-dm", channelId: OWNER_ROUTE.channelId, text: "blocked dm", authorId: blockedAuthorId });
		await fixture.emitMessage({ id: "blocked-channel", channelId: GUILD_A_ROUTE.channelId, text: "blocked channel", authorId: blockedAuthorId });
		expect(gatewayUnderTest.fixture.commands()).toEqual([]);
		expect(fixture.acknowledgements).toEqual([]);
		expect(diagnostics).toEqual([expect.stringContaining("blacklisted-author")]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord ingress drop diagnostics are bounded and rate-limited", async () => {
	const gatewayUnderTest = await gateway([{ id: MENTION_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture({ botUserId: BOT_USER_ID });
	const diagnostics: string[] = [];
	let now = 1_000;
	await fixture.connect();
	const route = new DiscordRouteHandler({
		rpc: gatewayUnderTest.client,
		platform: fixture,
		routes: [MENTION_ROUTE],
		botUserId: BOT_USER_ID,
		onDiagnostic: message => diagnostics.push(message),
		diagnosticNow: () => now,
	});
	const unsubscribe = fixture.onMessage(async message => {
		await route.handle(message);
	});
	try {
		for (const id of ["drop-rate-1", "drop-rate-2", "drop-rate-3"]) {
			await fixture.emitMessage({ id, channelId: MENTION_ROUTE.channelId, text: "ordinary mention-mode chatter", authorId: "111111111111111111" });
		}
		expect(gatewayUnderTest.fixture.commands()).toEqual([]);
		expect(fixture.acknowledgements).toEqual([]);
		expect(diagnostics).toEqual([expect.stringContaining("unengaged mention-mode")]);
		now += 30_000;
		await fixture.emitMessage({ id: "drop-rate-after-window", channelId: MENTION_ROUTE.channelId, text: "ordinary chatter after window", authorId: "111111111111111111" });
		expect(diagnostics).toEqual([
			expect.stringContaining("unengaged mention-mode"),
			expect.stringContaining("unengaged mention-mode"),
		]);
	} finally {
		unsubscribe();
		await fixture.disconnect();
	}
});

externalTest("Discord one-entry route table deduplicates a message id before external broker admission and sends finalized output", async () => {

	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [OWNER_ROUTE],
			botUserId: BOT_USER_ID,


		});
		const unsubscribe = fixture.onMessage(async message => {
			await route.handle(message);
		});
		const inbound = { id: "message-id-once", channelId: "123456789012345678", text: "external broker round trip" };
		await Promise.all([fixture.emitMessage(inbound), fixture.emitMessage(inbound)]);
		unsubscribe();

		expect(fixture.acknowledgements).toHaveLength(1);
		expect(gatewayUnderTest.fixture.commands()).toEqual([
			expect.objectContaining({ operation: "turn.prompt", text: "external broker round trip" }),
		]);
		await eventually(
			() => (gatewayUnderTest.core.journalRead("1:0", 20).events.some(event => event.kind === "assistant_message") ? true : undefined),
			"external assistant output was not journaled",
		);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toEqual([expect.objectContaining({ channelId: "123456789012345678", text: "ack" })]);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord route table admits a guild-channel message and returns its reply to that channel", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture();
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
	});
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "guild-route-round-trip", channelId: GUILD_A_ROUTE.channelId, text: "guild route round trip" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"guild-channel message was not admitted",
		);
		expect(gatewayUnderTest.fixture.commands()).toEqual([expect.objectContaining({ operation: "turn.follow_up", text: inbound.text, opRef })]);
		gatewayUnderTest.fixture.complete(opRef, { text: "guild route reply" });
		await eventually(
			() => (fixture.sends.some(send => send.channelId === GUILD_A_ROUTE.channelId && send.text === "guild route reply") ? true : undefined),
			"guild route reply was not posted to its guild channel",
		);
		expect(fixture.sends).toEqual([expect.objectContaining({ channelId: GUILD_A_ROUTE.channelId, text: "guild route reply" })]);
		expect(fixture.acknowledgements).toEqual([expect.objectContaining({ channelId: GUILD_A_ROUTE.channelId })]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord replies thread to the accepted trigger and fall back to a plain post when it is gone", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture();
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
	});
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const first = { id: "reply-trigger-one", channelId: GUILD_A_ROUTE.channelId, text: "first trigger" };
		await fixture.emitMessage(first);
		const firstOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === first.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"first reply trigger was not admitted",
		);
		gatewayUnderTest.fixture.complete(firstOpRef, { text: "first threaded reply" });
		await eventually(() => fixture.sends.find(send => send.text === "first threaded reply"), "first threaded reply was not sent");
		expect(fixture.sends.at(-1)?.replyTo).toEqual({ channelId: first.channelId, messageId: first.id });

		fixture.setMissingReplyReference(GUILD_A_ROUTE.channelId, "reply-trigger-two");
		gatewayUnderTest.fixture.holdNextTurn();
		const second = { id: "reply-trigger-two", channelId: GUILD_A_ROUTE.channelId, text: "second trigger" };
		await fixture.emitMessage(second);
		const secondOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === second.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"second reply trigger was not admitted",
		);
		gatewayUnderTest.fixture.complete(secondOpRef, { text: "fallback threaded reply" });
		await eventually(() => fixture.sends.find(send => send.text === "fallback threaded reply"), "fallback reply was not sent");
		expect(fixture.sends.at(-1)?.replyTo).toBeUndefined();
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord route table resolves a thread first seen mid-session and returns to that thread", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture();
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
	});
	const threadChannelId = "444444444444444444";
	try {
		// The adapter has already connected; this is a thread discovered only when
		// its first MESSAGE_CREATE arrives, not pre-registered fixture state.
		fixture.setThreadParent(threadChannelId, GUILD_A_ROUTE.channelId);
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "thread-route-round-trip", channelId: threadChannelId, text: "thread route round trip" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"thread message was not admitted",
		);
		expect(fixture.threadParentLookups).toEqual([threadChannelId]);
		gatewayUnderTest.fixture.complete(opRef, { text: "thread route reply" });
		await eventually(
			() => (fixture.sends.some(send => send.channelId === threadChannelId && send.text === "thread route reply") ? true : undefined),
			"thread route reply was not posted to its thread channel",
		);
		expect(fixture.acknowledgements).toEqual([expect.objectContaining({ channelId: threadChannelId })]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord route table never submits an unrouted channel", async () => {
	const gatewayUnderTest = await gateway([{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" }]);
	const fixture = new DiscordFixture();
	const diagnostics: string[] = [];

	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		onDiagnostic: message => diagnostics.push(message),

	});
	try {
		await fixture.emitMessage({ id: "unrouted-channel", channelId: "555555555555555555", text: "must remain outside the route table" });
		expect(gatewayUnderTest.fixture.commands()).toEqual([]);
		expect(fixture.acknowledgements).toEqual([]);
		expect(fixture.threadParentLookups).toEqual(["555555555555555555"]);
		expect(diagnostics).toEqual([expect.stringContaining("unrouted message")]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord route table keeps concurrent surface replies isolated across an adapter restart", async () => {
	const gatewayUnderTest = await gateway([
		{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" },
		{ id: GUILD_B_ROUTE.surfaceId, platform: "discord", kind: "channel" },
	]);
	const fixture = new DiscordFixture();
	const rpc = nonClosingRpc(gatewayUnderTest.client);
	const routes = [OWNER_ROUTE, GUILD_A_ROUTE, GUILD_B_ROUTE];
	let adapter = await startRoutedFixtureAdapter(fixture, rpc, routes, { unattributedRoute: OWNER_ROUTE });
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const first = { id: "restart-route-a", channelId: GUILD_A_ROUTE.channelId, text: "surface A before restart" };
		await fixture.emitMessage(first);
		const firstOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === first.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"first concurrent surface was not admitted",
		);
		gatewayUnderTest.fixture.holdNextTurn();
		const second = { id: "restart-route-b", channelId: GUILD_B_ROUTE.channelId, text: "surface B across restart" };
		await fixture.emitMessage(second);
		const secondOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === second.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"second concurrent surface was not admitted",
		);

		gatewayUnderTest.fixture.complete(firstOpRef, { text: "reply A before restart" });
		await eventually(
			() => (fixture.sends.some(send => send.channelId === GUILD_A_ROUTE.channelId && send.text === "reply A before restart") ? true : undefined),
			"first routed reply was not delivered before restart",
		);
		await eventually(
			() => (fixture.sends.some(send => send.channelId === GUILD_B_ROUTE.channelId && send.text === "ack") ? true : undefined),
			"second concurrent routed reply was not delivered before restart",
		);
		expect(secondOpRef).toEqual(expect.any(String));
		await adapter.stop();
		const sendsBeforeRestart = fixture.sends.length;
		adapter = await startRoutedFixtureAdapter(fixture, rpc, routes, { unattributedRoute: OWNER_ROUTE });
		await Bun.sleep(100);
		expect(fixture.sends).toEqual([
			expect.objectContaining({ channelId: GUILD_A_ROUTE.channelId, text: "reply A before restart" }),
			expect.objectContaining({ channelId: GUILD_B_ROUTE.channelId, text: "ack" }),
		]);
		expect(fixture.sends).toHaveLength(sendsBeforeRestart);
		expect(fixture.sendAttempts).toHaveLength(2);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive stops only the delivered route's channel", async () => {
	const gatewayUnderTest = await gateway([
		{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" },
		{ id: GUILD_B_ROUTE.surfaceId, platform: "discord", kind: "channel" },
	]);
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startRoutedFixtureAdapter(fixture, gatewayUnderTest.client, [OWNER_ROUTE, GUILD_A_ROUTE, GUILD_B_ROUTE], {
		unattributedRoute: OWNER_ROUTE,
		typingKeepaliveClock: clock,
	});
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const first = { id: "typing-route-a", channelId: GUILD_A_ROUTE.channelId, text: "keep channel A typing" };
		await fixture.emitMessage(first);
		const firstOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === first.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"first route typing admission was not recorded",
		);
		gatewayUnderTest.fixture.holdNextTurn();
		const second = { id: "typing-route-b", channelId: GUILD_B_ROUTE.channelId, text: "keep channel B typing" };
		await fixture.emitMessage(second);
		const secondOpRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.text === second.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"second route typing admission was not recorded",
		);
		const delivered = gatewayUnderTest.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "channel A delivery stops only A", surface_id: GUILD_A_ROUTE.surfaceId }),
		);
		await eventually(
			() => (fixture.sends.some(send => send.channelId === GUILD_A_ROUTE.channelId && send.text === "channel A delivery stops only A") ? true : undefined),
			"channel A attributed delivery was not posted",
		);
		expect(delivered.seq).toEqual(expect.any(String));
		expect(firstOpRef).toEqual(expect.any(String));
		expect(secondOpRef).toEqual(expect.any(String));
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toEqual([
			{ channelId: GUILD_A_ROUTE.channelId, at: 1_000 },
			{ channelId: GUILD_B_ROUTE.channelId, at: 1_000 },
			{ channelId: GUILD_B_ROUTE.channelId, at: 9_000 },
		]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord outbox applies owner-dm and suppress policies to unattributed replies", async () => {
	const gatewayUnderTest = await gateway([
		{ id: GUILD_A_ROUTE.surfaceId, platform: "discord", kind: "channel" },
	]);
	const routes = [OWNER_ROUTE, GUILD_A_ROUTE];
	const ownerFixture = new DiscordFixture();
	const suppressFixture = new DiscordFixture();
	await ownerFixture.connect();
	await suppressFixture.connect();
	try {
		const ownerEvent = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "unattributed to owner dm" }));
		const ownerOutbox = new DiscordOutbox({
			rpc: gatewayUnderTest.client,
			platform: ownerFixture,
			routes,
			unattributedDelivery: "owner-dm",
			unattributedRoute: OWNER_ROUTE,
			consumerId: "discord-owner-default-policy",
			claimTtlMs: 5_000,
			readWaitMs: 0,
		});
		expect(await ownerOutbox.runOnce()).toBe("sent");
		expect(ownerFixture.sends).toEqual([expect.objectContaining({ channelId: OWNER_ROUTE.channelId, text: "unattributed to owner dm" })]);
		expect(gatewayUnderTest.core.consumerCursor("discord-owner-default-policy")).toBe(ownerEvent.cursor);

		const unmappedEvent = gatewayUnderTest.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "unmapped attribution defaults to owner dm", surface_id: "discord:unrouted" }),
		);
		expect(await ownerOutbox.runOnce()).toBe("sent");
		expect(ownerFixture.sends).toEqual([
			expect.objectContaining({ channelId: OWNER_ROUTE.channelId, text: "unattributed to owner dm" }),
			expect.objectContaining({ channelId: OWNER_ROUTE.channelId, text: "unmapped attribution defaults to owner dm" }),
		]);
		expect(gatewayUnderTest.core.consumerCursor("discord-owner-default-policy")).toBe(unmappedEvent.cursor);

		const suppressedEvent = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "unattributed suppression" }));
		const diagnostics: string[] = [];
		const suppressOutbox = new DiscordOutbox({
			rpc: gatewayUnderTest.client,
			platform: suppressFixture,
			routes,
			unattributedDelivery: "suppress",
			consumerId: "discord-suppress-policy",
			claimTtlMs: 5_000,
			readWaitMs: 0,
			onDiagnostic: message => diagnostics.push(message),
		});
		expect(await suppressOutbox.runOnce()).toBe("sent");
		expect(suppressFixture.sends).toEqual([]);
		expect(gatewayUnderTest.core.consumerCursor("discord-suppress-policy")).toBe(suppressedEvent.cursor);
		expect(diagnostics).toEqual(
		expect.arrayContaining([expect.stringContaining("suppressed assistant_message"), expect.stringContaining("payload has no surface_id")]),
	);
	} finally {
		await ownerFixture.disconnect();
		await suppressFixture.disconnect();
	}
});

externalTest("Discord typing starts after delayed durable main.submit acceptance", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const ordering: string[] = [];
	let durableAcceptedAt: number | undefined;
	const delayedRpc: JsonRpcClient = {
		async request(method, params, options) {
			const request = gatewayUnderTest.client.request(method, params, options);
			if (method === "main.submit") clock.advance(2_001);
			const response = await request;
			if (method === "main.submit") {
				if (response.error || (response.result as { accepted?: unknown } | undefined)?.accepted !== true) {
					throw new Error("delayed main.submit did not return durable acceptance");
				}
				durableAcceptedAt = clock.now();
				ordering.push("durably accepted");
			}
			return response;
		},
		close() {},
	};
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: delayedRpc,
			platform: fixture,
			routes: [OWNER_ROUTE],
			botUserId: BOT_USER_ID,


			acknowledgement: { now: clock.now, budgetMs: 2_000 },
			onAcknowledged: () => ordering.push("typing acknowledged"),
		});
		const inbound = {
			id: "delayed-admission",
			channelId: "123456789012345678",
			text: "acknowledge after delayed admission",
			acceptedAt: clock.now(),
		};

		expect(await route.handle(inbound)).toBe(true);
		const acceptedAt = durableAcceptedAt;
		if (acceptedAt === undefined) throw new Error("delayed main.submit acceptance was not observed");
		expect(acceptedAt - inbound.acceptedAt).toBeGreaterThan(2_000);
		expect(fixture.acknowledgements).toEqual([{ channelId: inbound.channelId, at: acceptedAt }]);
		expect(ordering).toEqual(["durably accepted", "typing acknowledged"]);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord typing keepalive refreshes a slow accepted turn and stops after reply delivery", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, gatewayUnderTest.client);
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-slow-turn", channelId: TYPING_CHANNEL_ID, text: "keep typing until the reply arrives" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"slow Discord turn was not admitted",
		);

		await advanceTypingIntervals(clock, 3);

		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 17_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 25_000 },
		]);

		gatewayUnderTest.fixture.complete(opRef, { text: "typing keepalive delivered reply" });
		await eventually(
			() => (fixture.sends.some(send => send.text === "typing keepalive delivered reply") ? true : undefined),
			"Discord outbox did not deliver the slow turn reply",
		);
		const acknowledgementsAtDelivery = fixture.acknowledgements.length;
		await advanceTypingClock(clock, 30_000);

		expect(fixture.acknowledgements).toHaveLength(acknowledgementsAtDelivery);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive ignores an older delayed delivery but stops at a later reply", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, gatewayUnderTest.client);

	let releasedOlderSend = false;
	try {
		const olderText = "older assistant delivery must not clear newer typing";
		fixture.deferNextSend();
		const older = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: olderText }));
		await eventually(() => (fixture.pendingSendCount === 1 ? true : undefined), "old Discord outbox send was not paused");

		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-causal-new", channelId: TYPING_CHANNEL_ID, text: "new accepted command survives old delivery" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"new Discord turn was not admitted",
		);

		fixture.releaseNextSend();
		releasedOlderSend = true;
		await eventually(
			() => (gatewayUnderTest.core.consumerCursor("gajaeway-discord") === older.cursor ? true : undefined),
			"old Discord outbox delivery did not settle",
		);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);

		const replyText = "newer assistant delivery stops typing";
		gatewayUnderTest.fixture.complete(opRef, { text: replyText });
		await eventually(() => (fixture.sends.some(send => send.text === replyText) ? true : undefined), "new assistant reply was not delivered");
		await eventually(() => (clock.scheduledTimerCount === 0 ? true : undefined), "newer delivery did not stop the typing keepalive");
		const acknowledgementsAfterReply = fixture.acknowledgementAttempts.length;
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(acknowledgementsAfterReply);
	} finally {
		if (!releasedOlderSend && fixture.pendingSendCount > 0) {
			fixture.releaseNextSend();
			await clock.flushAsync();
		}
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive stops for a reply journaled before the accepted response reaches the adapter", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const replyText = "fast reply predates client receipt of acceptance";
	let completed = false;
	let serverBoundary: unknown;
	const adapterRpc: JsonRpcClient = {
		async request(method, params, options) {
			const response = await gatewayUnderTest.client.request(method, params, options);
			if (method !== "main.submit" || completed) return response;
			const result = response.result as { accepted?: unknown; op_ref?: unknown; journal_head_cursor?: unknown } | undefined;
			if (result?.accepted !== true || typeof result.op_ref !== "string") return response;
			completed = true;
			serverBoundary = result.journal_head_cursor;
			gatewayUnderTest.fixture.complete(result.op_ref, { text: replyText });
			await eventually(
				() =>
					gatewayUnderTest.core
						.journalRead("1:0", 100)
						.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes(replyText))
						? true
						: undefined,
				"fast assistant reply was not journaled before main.submit returned to the adapter",
			);
			return response;
		},
		close() {
			gatewayUnderTest.client.close();
		},
	};
	const adapter = await startTypingFixtureAdapter(fixture, clock, adapterRpc);
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		fixture.deferNextSend();
		await fixture.emitMessage({ id: "typing-fast-reply", channelId: TYPING_CHANNEL_ID, text: "respond before adapter observes acceptance" });
		expect(serverBoundary).toMatch(/^\d+:\d+$/);
		await eventually(() => (fixture.pendingSendCount === 1 ? true : undefined), "fast journaled reply was not held before Discord delivery");
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
		expect(clock.scheduledTimerCount).toBe(2);

		fixture.releaseNextSend();
		await eventually(() => (fixture.sends.some(send => send.text === replyText) ? true : undefined), "fast reply was not delivered to Discord");
		await eventually(() => (clock.scheduledTimerCount === 0 ? true : undefined), "fast reply did not stop the typing keepalive");
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
	} finally {
		if (fixture.pendingSendCount > 0) {
			fixture.releaseNextSend();
			await clock.flushAsync();
		}
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive shares one channel interval and extends its cap for a later admission", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	try {
		await fixture.emitMessage({ id: "typing-first", channelId: TYPING_CHANNEL_ID, text: "first accepted admission" });
		clock.advance(4_000);
		await fixture.emitMessage({ id: "typing-second", channelId: TYPING_CHANNEL_ID, text: "second accepted admission" });

		await advanceTypingIntervals(clock, 75);

		const refreshes = fixture.acknowledgements.slice(2);
		expect(fixture.acknowledgements.slice(0, 2)).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 5_000 },
		]);
		expect(refreshes).toHaveLength(74);
		for (const [index, acknowledgement] of refreshes.entries()) {
			expect(acknowledgement).toEqual({ channelId: TYPING_CHANNEL_ID, at: 13_000 + index * 8_000 });
		}

		expect(clock.scheduledTimerCount).toBe(0);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive never starts for a rejected admission", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(false));
	try {
		await fixture.emitMessage({ id: "typing-rejected", channelId: TYPING_CHANNEL_ID, text: "must not type" });
		await advanceTypingClock(clock, 30_000);

		expect(fixture.acknowledgements).toEqual([]);
		expect(fixture.acknowledgementAttempts).toEqual([]);
		expect(clock.scheduledTimerCount).toBe(0);
		expect(fixture.connected).toBe(true);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive diagnoses missing and malformed admission boundaries while retaining typing", async () => {
	for (const scenario of [
		{ name: "missing", response: { accepted: true }, detail: "omitted journal_head_cursor" },
		{ name: "malformed", response: { accepted: true, journal_head_cursor: "not-a-cursor" }, detail: "invalid journal_head_cursor" },
	] as const) {
		const clock = new DiscordFixtureClock(1_000);
		const fixture = new DiscordFixture({ now: clock.now });
		const errors: Error[] = [];
		const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true, scenario.response), error => errors.push(error));
		try {
			await fixture.emitMessage({
				id: `typing-boundary-${scenario.name}`,
				channelId: TYPING_CHANNEL_ID,
				text: `${scenario.name} accepted boundary retains typing`,
			});
			expect(errors).toEqual([expect.objectContaining({ message: expect.stringContaining(scenario.detail) })]);
			expect(clock.scheduledTimerCount).toBe(2);
			await advanceTypingClock(clock, 8_000);
			expect(fixture.acknowledgementAttempts).toEqual([
				{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
				{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
			]);
			expect(clock.scheduledTimerCount).toBe(2);
		} finally {
			await adapter.stop();
		}
	}
});

externalTest("Discord typing keepalive diagnoses a stale-generation admission boundary while retaining typing", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const staleBoundaryRpc: JsonRpcClient = {
		async request(method, params, options) {
			const response = await gatewayUnderTest.client.request(method, params, options);
			if (method !== "main.submit" || !response.result || typeof response.result !== "object") return response;
			return {
				...response,
				result: { ...(response.result as Record<string, unknown>), journal_head_cursor: "0:0" },
			};
		},
		close() {
			gatewayUnderTest.client.close();
		},
	};
	const adapter = await startTypingFixtureAdapter(fixture, clock, staleBoundaryRpc, error => errors.push(error));
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-stale-generation", channelId: TYPING_CHANNEL_ID, text: "stale generation must retain typing" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"stale-boundary Discord turn was not admitted",
		);
		gatewayUnderTest.fixture.complete(opRef, { text: "stale generation reply" });
		await eventually(() => (fixture.sends.some(send => send.text === "stale generation reply") ? true : undefined), "stale-boundary reply was not delivered");
		await eventually(
			() => (errors.some(error => error.message.includes("does not match accepted generation")) ? true : undefined),
			"stale-generation boundary diagnostic was not reported",
		);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive cancels its timer when the adapter stops", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	await fixture.emitMessage({ id: "typing-teardown", channelId: TYPING_CHANNEL_ID, text: "stop before refresh" });
	expect(clock.scheduledTimerCount).toBe(2);

	await adapter.stop();
	expect(clock.scheduledTimerCount).toBe(0);
	await advanceTypingClock(clock, 30_000);

	expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
});

externalTest("Discord typing keepalive retries after a non-fatal typing failure", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true), error => errors.push(error));
	try {
		await fixture.emitMessage({ id: "typing-retry", channelId: TYPING_CHANNEL_ID, text: "retry typing after failure" });
		fixture.failNextTyping();
		await advanceTypingClock(clock, 8_000);

		expect(fixture.acknowledgementAttempts).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
		expect(errors).toEqual([expect.objectContaining({ message: `Discord typing keepalive failed for channel ${TYPING_CHANNEL_ID}: fixture typing acknowledgement failed` })]);

		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 17_000 },
		]);
		expect(fixture.connected).toBe(true);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive allows only one deferred refresh per channel", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	try {
		await fixture.emitMessage({ id: "typing-deferred-refresh", channelId: TYPING_CHANNEL_ID, text: "hold one refresh request" });
		await clock.flushAsync();
		fixture.deferNextTyping();
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(clock.scheduledTimerCount).toBe(1);

		await advanceTypingIntervals(clock, 3);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(clock.scheduledTimerCount).toBe(1);

		fixture.resolveNextTyping();
		await clock.flushAsync();
		expect(fixture.pendingTypingCount).toBe(0);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(3);
	} finally {
		while (fixture.pendingTypingCount > 0) fixture.resolveNextTyping();
		await clock.flushAsync();
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive does not reschedule or report after stop during a refresh", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true), error => errors.push(error));
	let stopped = false;
	try {
		await fixture.emitMessage({ id: "typing-stop-pending", channelId: TYPING_CHANNEL_ID, text: "stop pending refresh" });
		await clock.flushAsync();
		fixture.deferNextTyping();
		await advanceTypingClock(clock, 8_000);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);

		await adapter.stop();
		stopped = true;
		expect(clock.scheduledTimerCount).toBe(0);
		fixture.rejectNextTyping(new Error("typing request rejected after adapter stop"));
		await clock.flushAsync();
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(clock.scheduledTimerCount).toBe(0);
		expect(errors).toEqual([]);
	} finally {
		if (fixture.pendingTypingCount > 0) fixture.resolveNextTyping();
		await clock.flushAsync();
		if (!stopped) await adapter.stop();
	}
});

externalTest("Discord typing follows accepted admission, not a stale healthy-to-fenced connection", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [OWNER_ROUTE],
			botUserId: BOT_USER_ID,


		});
		gatewayUnderTest.fixture.holdNextTurn();
		gatewayUnderTest.fixture.suppressNextAdmissionReceipt();
		const ambiguous = await gatewayUnderTest.client.request("main.submit", {
			text: "establish an admission recovery fence",
			surface_id: "discord:owner-dm",
			idempotency_key: "discord-health-flap-fence",
		});
		expect(ambiguous.error).toMatchObject({ code: -32603 });
		const [pending] = gatewayUnderTest.core.mainAdmissionOperationsPending();
		const intent = JSON.parse(pending?.intentJson ?? "{}") as { op_ref?: unknown };
		if (typeof intent.op_ref !== "string") throw new Error("fenced Discord setup did not retain its operation reference");
		await eventually(
			() => (gatewayUnderTest.host.mutationReadinessReason === "admission_recovery_pending" ? true : undefined),
			"gateway did not enter the admission recovery fence",
		);

		const blocked = { id: "discord-fenced-message", channelId: "123456789012345678", text: "must remain unacknowledged" };
		await expect(route.handle({ ...blocked, acceptedAt: Date.now() })).rejects.toMatchObject({ name: "RpcResponseError", code: 1003 });
		expect(fixture.connected).toBe(true);
		expect(fixture.acknowledgements).toEqual([]);
		expect(gatewayUnderTest.fixture.commands().filter(command => command.text === blocked.text)).toEqual([]);

		gatewayUnderTest.fixture.complete(intent.op_ref, { text: "recovery fence terminal reply" });
		await eventually(
			() => (gatewayUnderTest.host.mutationReadinessReason === undefined ? true : undefined),
			"gateway did not promote after the fenced admission's terminal evidence",
		);
		const fresh = { id: "discord-after-promotion", channelId: "123456789012345678", text: "fresh ingress after promotion", acceptedAt: Date.now() };
		expect(await route.handle(fresh)).toBe(true);
		expect(fixture.acknowledgements).toHaveLength(1);
		expect(gatewayUnderTest.fixture.commands()).toEqual(
			expect.arrayContaining([expect.objectContaining({ operation: "turn.prompt", text: fresh.text })]),
		);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord startup reports rate-limited verifying and transport readiness diagnostics without opening ingress", async () => {
	const config = {
		rpcSocketPath: "/tmp/discord-gateway-readiness.sock",
		token: "fixture-token",
		routes: [OWNER_ROUTE],
		unattributedDelivery: "owner-dm" as const,
		blockedAuthorIds: [],
		unattributedRoute: OWNER_ROUTE,

		ackBudgetMs: 2_000,
		claimTtlMs: 5_000,
		readWaitMs: 0,
	};
	const verifyingPlatform = new DiscordFixture();
	const verifyingAbort = new AbortController();
	const verifyingDiagnostics: string[] = [];
	let healthRequests = 0;
	const verifyingRpc: JsonRpcClient = {
		async request() {
			healthRequests += 1;
			return { jsonrpc: "2.0", id: healthRequests, result: { status: "booting", state: "verifying" } };
		},
		close() {},
	};
	const verifyingStart = startDiscordAdapter(config, {
		rpcConnect: async () => verifyingRpc,
		platformFactory: () => verifyingPlatform,
		startupSignal: verifyingAbort.signal,
		onDiagnostic: message => verifyingDiagnostics.push(message),
	});
	await eventually(() => (healthRequests >= 3 ? true : undefined), "adapter did not poll the verifying gateway");
	verifyingAbort.abort();
	await expect(verifyingStart).rejects.toMatchObject({ name: "AbortError" });
	expect(verifyingPlatform.connectCount).toBe(0);
	expect(verifyingDiagnostics).toEqual(["gateway verifying (expected wait); delaying Discord connection until healthy/running."]);

	const transportAbort = new AbortController();
	const transportDiagnostics: string[] = [];
	let connectionAttempts = 0;
	const transportStart = startDiscordAdapter(config, {
		rpcConnect: async () => {
			connectionAttempts += 1;
			throw new Error("fixture UDS unavailable");
		},
		startupSignal: transportAbort.signal,
		onDiagnostic: message => transportDiagnostics.push(message),
	});
	await eventually(() => (connectionAttempts >= 3 ? true : undefined), "adapter did not retry the unavailable gateway transport");
	transportAbort.abort();
	await expect(transportStart).rejects.toMatchObject({ name: "AbortError" });
	expect(transportDiagnostics).toEqual(["gateway transport/protocol error while waiting: could not connect to the local gateway: fixture UDS unavailable"]);
});

externalTest("Discord outbox settles server-side delivery before a restart can repost", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "settle this" }));
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
		expect(gatewayUnderTest.core.consumerOutbox("gajaeway-discord")).toEqual([
			expect.objectContaining({ seq: appended.seq, state: "sent", dedupeKey: `gajaeway-discord:discord:owner-dm:${appended.seq}` }),
		]);

		const restarted = outbox(gatewayUnderTest, fixture);
		expect(await restarted.runOnce()).toBe("idle");
		expect(fixture.sends).toHaveLength(1);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord crash before send leaves the durable checkpoint for restart delivery", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "retry before send" }));
		const abort = new AbortController();
		const interrupted = outbox(gatewayUnderTest, fixture, { beforeSend: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toEqual([]);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
	}
}, 15_000);

externalTest("Discord crash after send before settlement retries with a nonce and does not double-post", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "retry after send" }));
		const abort = new AbortController();
		const interrupted = outbox(gatewayUnderTest, fixture, { afterSendBeforeCommit: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toHaveLength(1);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		const wireNonce = discordWireNonce(discordDedupeKey("discord:owner-dm", String(appended.seq)));
		expect(wireNonce.length).toBeLessThanOrEqual(25);
		expect(fixture.sendAttempts).toEqual([
			expect.objectContaining({ duplicate: false, nonce: wireNonce }),
			expect.objectContaining({ duplicate: true, nonce: wireNonce }),
		]);
		for (const attempt of fixture.sendAttempts) {
			// Live Discord rejects nonces longer than 25 characters (50035 NONCE_TYPE_TOO_LONG).
			expect(attempt.nonce.length).toBeLessThanOrEqual(25);
		}
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
	}
}, 15_000);

externalTest("Discord outbox delivers bounded chunks and replays identical chunk nonces after a mid-set restart", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	const diagnostics: string[] = [];
	try {
		const text = ["```ts", ...Array.from({ length: 260 }, (_, index) => `const value${index} = ${index};`), "```"].join("\n");
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text }));
		const abort = new AbortController();
		fixture.deferNextSend();
		const interrupted = outbox(gatewayUnderTest, fixture, undefined, message => diagnostics.push(message)).runOnce(abort.signal);
		await eventually(() => (fixture.pendingSendCount === 1 ? true : undefined), "outbox did not reach the first deferred chunk");
		abort.abort();
		fixture.releaseNextSend();
		await expect(interrupted).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toHaveLength(1);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		expect(await outbox(gatewayUnderTest, fixture, undefined, message => diagnostics.push(message)).runOnce()).toBe("sent");
		const dedupeKey = discordDedupeKey("discord:owner-dm", String(appended.seq));
		const chunkCount = fixture.sends.length;
		const expectedNonces = Array.from({ length: chunkCount }, (_, index) => discordChunkWireNonce(dedupeKey, index));
		expect(chunkCount).toBeGreaterThan(1);
		expect(fixture.sendAttempts.map(attempt => attempt.nonce)).toEqual([expectedNonces[0], expectedNonces[0], ...expectedNonces.slice(1)]);
		expect(fixture.sendAttempts[0]?.duplicate).toBe(false);
		expect(fixture.sendAttempts[1]?.duplicate).toBe(true);
		expect(fixture.sendAttempts.slice(2).every(attempt => !attempt.duplicate)).toBe(true);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
		expect(diagnostics).toEqual([]);

		const huge = "x".repeat(2_000 * 40);
		gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: huge }));
		expect(await outbox(gatewayUnderTest, fixture, undefined, message => diagnostics.push(message)).runOnce()).toBe("sent");
		expect(diagnostics).toEqual([expect.stringContaining("chunk bound")]);
		expect(fixture.sends.at(-1)?.text).toContain("chunk limit reached");
	} finally {
		await fixture.disconnect();
	}
}, 15_000);

externalTest("Discord outbound formatting changes only the wire copy and preserves journal text and nonce", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const original = ["| Name | Value |", "| --- | --- |", "| alpha | one |", "", "See https://one.example/a and https://two.example/b."].join("\n");
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: original }));
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		expect(fixture.sends[0]?.text).toContain("- Name: alpha; Value: one");
		expect(fixture.sends[0]?.text).toContain("<https://one.example/a>");
		expect(fixture.sends[0]?.text).not.toContain("| --- | --- |");
		const journalEvent = gatewayUnderTest.core.journalRead("1:0", 20).events.find(event => event.seq === appended.seq);
		expect(JSON.parse(journalEvent?.payloadJson ?? "{}").text).toBe(original);
		expect(fixture.sends[0]?.nonce).toBe(discordWireNonce(discordDedupeKey("discord:owner-dm", String(appended.seq))));
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord non-owner engagement is admitted as follow_up whether the external turn is idle or busy", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const guestRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [{ channelId: "222222222222222222", surfaceId: "discord:guest-channel", kind: "channel", engagement: "always" }],

			botUserId: BOT_USER_ID,


		});
		await guestRoute.handle({ id: "guest-idle", channelId: "222222222222222222", text: "idle guest message", acceptedAt: Date.now() });

		gatewayUnderTest.fixture.holdNextTurn();
		const ownerRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			routes: [OWNER_ROUTE],
			botUserId: BOT_USER_ID,
		});
		await ownerRoute.handle({ id: "owner-held", channelId: "123456789012345678", text: "held owner message", acceptedAt: Date.now() });
		await eventually(() => (gatewayUnderTest.host.turnState === "busy" ? true : undefined), "owner turn was not admitted as busy");
		await guestRoute.handle({ id: "guest-busy", channelId: "222222222222222222", text: "busy guest message", acceptedAt: Date.now() });

		expect(gatewayUnderTest.fixture.commands().map(command => ({ operation: command.operation, text: command.text }))).toEqual([
			{ operation: "turn.follow_up", text: "idle guest message" },
			{ operation: "turn.prompt", text: "held owner message" },
			{ operation: "turn.follow_up", text: "busy guest message" },
		]);
		expect(fixture.acknowledgements).toHaveLength(3);
		const heldPrompt = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt");
		if (typeof heldPrompt?.opRef !== "string") throw new Error("held Discord owner prompt was not recorded with an operation reference");
		gatewayUnderTest.fixture.complete(heldPrompt.opRef, { text: "settled after Discord follow-up assertion" });
		await eventually(
			() =>
				gatewayUnderTest.core
					.journalRead("1:0", 100)
					.events.some(event => event.kind === "assistant_message" && JSON.parse(event.payloadJson).text === "settled after Discord follow-up assertion")
					? true
					: undefined,
			"held Discord owner prompt did not settle before teardown",
			15_000,
		);
	} finally {
		await fixture.disconnect();
	}
});
