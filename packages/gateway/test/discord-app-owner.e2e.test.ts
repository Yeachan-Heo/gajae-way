import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { handleSlashCommand, ReconnectingGateway, resolveApplicationOwnerIds } from "../../adapter-discord/src/main";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

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
		{ channels: { fetch: async () => undefined } } as never,
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
