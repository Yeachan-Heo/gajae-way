import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { handleSlashCommand, ReconnectingGateway, resolveApplicationOwnerIds } from "../../adapter-discord/src/main";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

/** Create a discord.js Collection-like object that iterates values (mirrors app-owner.test.ts). */
function createMockCollection(
	members: Array<{ id: string; user: { id: string }; membershipState: number; role?: string }>,
) {
	return {
		values() {
			return members[Symbol.iterator]();
		},
		[Symbol.iterator]() {
			return members[Symbol.iterator]();
		},
	};
}

/**
 * Issue #862 end to end: the adapter resolves its application's owners at
 * login, reports them on its authenticated hello, and the gateway gates
 * owner-only `/model` on them for discord origins. Stubbed discord.js
 * application objects; the real ReconnectingGateway and the real server.
 */

const live = new Set<ReconnectingGateway>();
let server: GatewayServer | undefined;
let directory: string | undefined;

afterEach(async () => {
	live.clear();
	try {
		await server?.stop();
	} finally {
		server = undefined;
		if (directory !== undefined) {
			await rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	}
});

const APP_OWNER = "1000000000000000001";
const MEMBER = "3000000000000000003";
const CHANNEL_ORIGIN = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
const as = (authorId: string) => ({ mentioned: true, group: true, authorId });

async function startGateway(): Promise<{ config: GatewayConfig; database: Store }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-discord-app-owner-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		channels: { "discord:chan-1": { engagement: "mention-open" as const } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => "ok",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	return { config, database };
}

function adapterFor(config: GatewayConfig, ownerIds: readonly string[] | undefined): ReconnectingGateway {
	const gateway = new ReconnectingGateway(
		config.socketPath,
		{ channels: { fetch: async () => undefined }, application: undefined } as never,
		{ tokenFile: "token", token: "redacted", configPath: "config", channels: {} } as never,
	);
	gateway.setAppOwnerIds(ownerIds);
	live.add(gateway);
	return gateway;
}

test("unset ownerTarget: the adapter-reported application owner may /model and a member may not", async () => {
	const { config, database } = await startGateway();
	const gateway = adapterFor(
		config,
		await resolveApplicationOwnerIds({
			fetch: () => Promise.resolve({ owner: { id: APP_OWNER }, team: null }),
		}),
	);
	await gateway.connect();
	await Bun.sleep(20);
	const set = await gateway.requestInbound("m-owner", CHANNEL_ORIGIN, "/model set frontier-heavy", as(APP_OWNER));
	expect(set?.engaged).toBe(true);
	const key = "discord/channel/chan-1";
	expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });

	const member = await gateway.requestInbound("m-member", CHANNEL_ORIGIN, "/model clear", as(MEMBER));
	expect(member).toMatchObject({ engaged: false, turnId: null });
	expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });
});

test("lookup failure stays fail closed: the refusal names ownerTarget", async () => {
	const { config, database } = await startGateway();
	const gateway = adapterFor(
		config,
		await resolveApplicationOwnerIds({
			fetch: () => Promise.reject(new Error("DiscordAPIError: 500")),
		}),
	);
	await gateway.connect();
	await Bun.sleep(20);
	const refused = await gateway.requestInbound("m-owner", CHANNEL_ORIGIN, "/model set frontier-heavy", as(APP_OWNER));
	expect(refused).toMatchObject({ engaged: false, turnId: null });
	expect(database.bindings.modelGet("discord/channel/chan-1")).toBeUndefined();

	// The slash-command refusal names the fix when no owner resolved anywhere.
	const edits: string[] = [];
	await handleSlashCommand(
		{
			isChatInputCommand: () => true,
			commandName: "model",
			id: "itx-no-owner",
			user: { id: APP_OWNER, username: "snowy" },
			channel: { id: "chan-1", type: 0 },
			options: { getSubcommand: () => "set", getString: () => "frontier-heavy" },
			reply: async () => {},
			deferReply: async () => {},
			editReply: async (options: { content: string }) => {
				edits.push(options.content);
			},
		} as never,
		gateway,
		{ error: () => {} },
		false,
	);
	expect(edits).toEqual(["not authorized: /model is owner-only (no owner resolved; set ownerTarget)"]);
});

test("an explicit ownerTarget overrides the app owners: the app owner is refused when it differs", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-discord-app-owner-explicit-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: MEMBER } },
		channels: { "discord:chan-1": { engagement: "mention-open" as const } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => "ok",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

	const gateway = adapterFor(config, [APP_OWNER]);
	await gateway.connect();
	await Bun.sleep(20);
	expect(
		await gateway.requestInbound("m-app-owner", CHANNEL_ORIGIN, "/model set frontier-heavy", as(APP_OWNER)),
	).toMatchObject({ engaged: false });
	expect(
		await gateway.requestInbound("m-configured-owner", CHANNEL_ORIGIN, "/model set frontier-heavy", as(MEMBER)),
	).toMatchObject({ engaged: true });
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });
});

test("a discord slash command carries the invoking user and routes through the same gate", async () => {
	const { config } = await startGateway();
	const gateway = adapterFor(config, [APP_OWNER]);
	await gateway.connect();
	await Bun.sleep(20);
	const edits: string[] = [];
	await handleSlashCommand(
		{
			isChatInputCommand: () => true,
			commandName: "model",
			id: "itx-owner",
			user: { id: APP_OWNER, username: "owner" },
			channel: { id: "chan-1", type: 0 },
			options: { getSubcommand: () => "set", getString: () => "frontier-heavy" },
			reply: async () => {},
			deferReply: async () => {},
			editReply: async (options: { content: string }) => {
				edits.push(options.content);
			},
		} as never,
		gateway,
		{ error: () => {} },
	);
	// The gateway accepted the owner's set, so the ack is honest.
	expect(edits).toEqual(["🦞 model command accepted"]);
});

test("a slash command while the gateway is down answers retryable, never 'not authorized' (#862, finding 2)", async () => {
	const { config } = await startGateway();
	// No connect(): the adapter's gateway link is not up, which is exactly the
	// startup window (and any later disconnect) where dev's listener was active.
	const gateway = adapterFor(config, [APP_OWNER]);
	const replies: Array<{ content: string; ephemeral?: boolean }> = [];
	await handleSlashCommand(
		{
			isChatInputCommand: () => true,
			commandName: "model",
			id: "itx-startup",
			user: { id: APP_OWNER, username: "owner" },
			channel: { id: "chan-1", type: 0 },
			options: { getSubcommand: () => "set", getString: () => "frontier-heavy" },
			reply: async (options: { content: string; ephemeral?: boolean }) => {
				replies.push(options);
			},
			deferReply: async () => {
				throw new Error("an unavailable gateway must reply before deferring");
			},
			editReply: async (options: { content: string }) => {
				replies.push(options);
			},
		} as never,
		gateway,
		{ error: () => {} },
	);
	expect(replies).toHaveLength(1);
	expect(replies[0]?.ephemeral).toBe(true);
	expect(replies[0]?.content).toContain("gateway not ready");
	expect(replies[0]?.content).toContain("try again");
	expect(replies[0]?.content.toLowerCase()).not.toContain("not authorized");
});

test("after a disconnect the owner set refreshes and a removed member is revoked on the live connection (#862, finding 3)", async () => {
	const { config, database } = await startGateway();
	let members = createMockCollection([
		{ id: APP_OWNER, user: { id: APP_OWNER }, membershipState: 2, role: "admin" },
		{ id: "5000000000000000005", user: { id: "5000000000000000005" }, membershipState: 2, role: "developer" },
	]);
	const application = {
		fetch: async () => ({ owner: { members } }),
	};
	const gateway = new ReconnectingGateway(
		config.socketPath,
		{ channels: { fetch: async () => undefined }, application } as never,
		{ tokenFile: "token", token: "redacted", configPath: "config", channels: {} } as never,
	);
	gateway.setAppOwnerIds(await resolveApplicationOwnerIds(application));
	await gateway.connect();
	await Bun.sleep(20);
	// The starting team: owner and developer both gate /model.
	await expect(
		gateway.requestInbound("m-live-owner", CHANNEL_ORIGIN, "/model set frontier-heavy", as(APP_OWNER)),
	).resolves.toMatchObject({ engaged: true });

	// Remove the developer from the stubbed team; NO reconnect happens.
	members = createMockCollection([{ id: APP_OWNER, user: { id: APP_OWNER }, membershipState: 2, role: "admin" }]);
	await gateway.refreshAppOwnerIds();

	await expect(
		gateway.requestInbound("m-live-removed", CHANNEL_ORIGIN, "/model set other-model", as("5000000000000000005")),
	).resolves.toMatchObject({ engaged: false, turnId: null });
	// The still-owner keeps authority without any reconnect.
	await expect(
		gateway.requestInbound("m-live-owner-2", CHANNEL_ORIGIN, "/model clear", as(APP_OWNER)),
	).resolves.toMatchObject({ engaged: true });
	expect(database.bindings.modelGet("discord/channel/chan-1")).toBeUndefined();
});

test("channel ownerTarget with explicit owner: configured owner may /model, app owner is rejected, non-owner members rejected", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-channel-owner-explicit-e2e-"));
	const CONFIGURED_OWNER = "2000000000000000002";
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		// Channel ownerTarget with explicit owner: not a DM, so peerId is not set
		ownerTarget: {
			origin: { platform: "discord", kind: "channel", conversationId: "owner-chan" },
			owner: CONFIGURED_OWNER,
		},
		channels: { "discord:chan-1": { engagement: "mention-open" as const } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => "ok",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

	const gateway = adapterFor(config, [APP_OWNER]);
	await gateway.connect();
	await Bun.sleep(20);

	// Configured owner can /model
	const configuredOwnerSet = await gateway.requestInbound(
		"m-configured-owner",
		CHANNEL_ORIGIN,
		"/model set frontier-heavy",
		as(CONFIGURED_OWNER),
	);
	expect(configuredOwnerSet?.engaged).toBe(true);
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });

	// App owner (different from configured owner) is REJECTED
	const appOwnerRejected = await gateway.requestInbound(
		"m-app-owner-rejected",
		CHANNEL_ORIGIN,
		"/model set openai-only",
		as(APP_OWNER),
	);
	expect(appOwnerRejected?.engaged).toBe(false);
	expect(appOwnerRejected?.turnId).toBeNull();
	// Model should not have changed
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });

	// Non-owner member is REJECTED
	const memberRejected = await gateway.requestInbound("m-member-rejected", CHANNEL_ORIGIN, "/model clear", as(MEMBER));
	expect(memberRejected?.engaged).toBe(false);
	expect(memberRejected?.turnId).toBeNull();
	// Model should not have changed
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });
});

test("DM ownerTarget keeps working unchanged: peerId takes precedence over adapter owners", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-dm-owner-unchanged-e2e-"));
	const DM_OWNER = "4000000000000000004";
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		// DM ownerTarget with peerId: should work as before
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "dm-1", peerId: DM_OWNER } },
		channels: { "discord:chan-1": { engagement: "mention-open" as const } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => "ok",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

	const gateway = adapterFor(config, [APP_OWNER]);
	await gateway.connect();
	await Bun.sleep(20);

	// DM owner (from peerId) can /model
	const dmOwnerSet = await gateway.requestInbound(
		"m-dm-owner",
		CHANNEL_ORIGIN,
		"/model set frontier-heavy",
		as(DM_OWNER),
	);
	expect(dmOwnerSet?.engaged).toBe(true);
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });

	// App owner is REJECTED (even though it's configured as app owner)
	const appOwnerRejected = await gateway.requestInbound(
		"m-app-owner-rejected-dm",
		CHANNEL_ORIGIN,
		"/model clear",
		as(APP_OWNER),
	);
	expect(appOwnerRejected?.engaged).toBe(false);
	expect(appOwnerRejected?.turnId).toBeNull();
	expect(database.bindings.modelGet("discord/channel/chan-1")?.selection).toEqual({ preset: "frontier-heavy" });
});

test("channel ownerTarget without explicit owner field: fails closed, no access to anyone", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-channel-owner-no-field-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		// Channel ownerTarget without explicit owner: incomplete config, should fail closed
		ownerTarget: { origin: { platform: "discord", kind: "channel", conversationId: "owner-chan" } },
		channels: { "discord:chan-1": { engagement: "mention-open" as const } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => "ok",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

	const gateway = adapterFor(config, [APP_OWNER]);
	await gateway.connect();
	await Bun.sleep(20);

	// App owner is REJECTED (incomplete config requires explicit owner)
	const appOwnerRejected = await gateway.requestInbound(
		"m-app-owner-rejected",
		CHANNEL_ORIGIN,
		"/model set frontier-heavy",
		as(APP_OWNER),
	);
	expect(appOwnerRejected?.engaged).toBe(false);
	expect(appOwnerRejected?.turnId).toBeNull();
	expect(database.bindings.modelGet("discord/channel/chan-1")).toBeUndefined();
});
