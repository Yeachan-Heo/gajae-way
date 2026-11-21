import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

/**
 * Issue #862: with `ownerTarget` unset, owner-only verbs accept the Discord
 * application owners the ADAPTER reported on its own authenticated hello
 * (`adapter.ownerIds`) — and only for discord-platform origins. An explicit
 * `ownerTarget` supersedes that set; lookup failure stays fail closed.
 */

async function connect(socketPath: string): Promise<{
	send(value: unknown): void;
	close(): void;
	frames: Array<Record<string, unknown>>;
}> {
	const frames: Array<Record<string, unknown>> = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, chunk) {
				buffered += chunk.toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line.trim()) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), close: () => socket.end(), frames };
}

async function waitFor(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

interface GatewayHandle {
	config: GatewayConfig;
	database: Store;
	client: Awaited<ReturnType<typeof connect>>;
	stop(): Promise<void>;
}

/** A gateway with NO ownerTarget: owner authority must come from the adapter hello. */
async function withGateway<T>(
	body: (gateway: GatewayHandle) => Promise<T>,
	options: {
		readonly hello?: Record<string, unknown>;
		readonly channels?: GatewayConfig["channels"];
		readonly dmPolicy?: GatewayConfig["dmPolicy"];
	} = {},
): Promise<T> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-app-owner-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		...(options.dmPolicy ? { dmPolicy: options.dmPolicy } : {}),
		...(options.channels ? { channels: options.channels } : {}),
	};
	const database = await Store.open(config.dbPath);
	const port = attachTestBrokerOwnership(database, new ScriptedSessionPort(), join(home, "agent"));
	let server: GatewayServer | undefined;
	let client: Awaited<ReturnType<typeof connect>> | undefined;
	try {
		server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"], ...(options.hello ?? {}) } });
		await Bun.sleep(5);
		return await body({ config, database, client, stop: server.stop });
	} finally {
		client?.close();
		try {
			await server?.stop();
		} finally {
			if (!server) database.close();
			await rm(home, { recursive: true, force: true });
		}
	}
}

async function requestOverSocket(
	client: Awaited<ReturnType<typeof connect>>,
	id: string,
	verb: string,
	params?: unknown,
): Promise<Record<string, unknown>> {
	client.send({ v: "0.1", type: "request", id, verb, ...(params === undefined ? {} : { params }) });
	await waitFor(
		() => client.frames.some((frame) => frame.id === id && frame.type !== "event"),
		`${verb} ${id} did not answer`,
	);
	return client.frames.find((frame) => frame.id === id && frame.type !== "event") as Record<string, unknown>;
}

const CHANNEL = { platform: "discord", kind: "channel", conversationId: "public" };
const SLACK_CHANNEL = { platform: "slack", kind: "channel", conversationId: "public" };
const APP_OWNER = "1111111111111111111";
const TEAM_MEMBER = "2222222222222222222";
const MEMBER = "3333333333333333333";

const mentionOpen = { "discord:public": { engagement: "mention-open" as const } };
const as = (authorId: string) => ({ mentioned: true, group: true, authorId });

describe("adapter-reported application owners (#862)", () => {
	test("unset ownerTarget plus an app owner: the owner may /model, a member is refused", async () => {
		await withGateway(
			async ({ client, database }) => {
				const key = "discord/channel/public";
				expect(
					await requestOverSocket(client, "owner-set", "chat.send", {
						origin: CHANNEL,
						text: "/model set frontier-heavy",
						engagement: as(APP_OWNER),
					}),
				).toMatchObject({ type: "response", result: { engaged: true } });
				expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });

				expect(
					await requestOverSocket(client, "member-set", "chat.send", {
						origin: CHANNEL,
						text: "/model set other-model",
						engagement: as(MEMBER),
					}),
				).toMatchObject({ type: "response", result: { turnId: null, engaged: false } });
				expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });

				// The autocomplete gate follows the same owner set.
				expect(
					await requestOverSocket(client, "owner-choices", "session.modelChoices", {
						authorId: APP_OWNER,
						originPlatform: "discord",
					}),
				).toMatchObject({ type: "response", result: { choices: expect.anything() } });
				expect(
					await requestOverSocket(client, "member-choices", "session.modelChoices", {
						authorId: MEMBER,
						originPlatform: "discord",
					}),
				).toMatchObject({ type: "response", result: { choices: [] } });
			},
			{ hello: { adapter: { platform: "discord", ownerIds: [APP_OWNER] } }, channels: mentionOpen },
		);
	});

	test("a team-owned app: every accepted team member may /model", async () => {
		await withGateway(
			async ({ client, database }) => {
				const key = "discord/channel/public";
				expect(
					await requestOverSocket(client, "member-set", "chat.send", {
						origin: CHANNEL,
						text: "/model set frontier-heavy",
						engagement: as(TEAM_MEMBER),
					}),
				).toMatchObject({ type: "response", result: { engaged: true } });
				expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });
				expect(
					await requestOverSocket(client, "second-member-set", "chat.send", {
						origin: CHANNEL,
						text: "/model clear",
						engagement: as(APP_OWNER),
					}),
				).toMatchObject({ type: "response", result: { engaged: true } });
				expect(database.bindings.modelGet(key)).toBeUndefined();
			},
			{ hello: { adapter: { platform: "discord", ownerIds: [APP_OWNER, TEAM_MEMBER] } }, channels: mentionOpen },
		);
	});

	test("lookup failure (no adapter owners): fail closed for everyone outside loopback", async () => {
		await withGateway(
			async ({ client, database }) => {
				const key = "discord/channel/public";
				expect(
					await requestOverSocket(client, "anyone-set", "chat.send", {
						origin: CHANNEL,
						text: "/model set frontier-heavy",
						engagement: as(APP_OWNER),
					}),
				).toMatchObject({ type: "response", result: { turnId: null, engaged: false } });
				expect(
					await requestOverSocket(client, "choices", "session.modelChoices", {
						authorId: APP_OWNER,
						originPlatform: "discord",
					}),
				).toMatchObject({ type: "response", result: { choices: [] } });
				expect(database.bindings.modelGet(key)).toBeUndefined();
			},
			{ channels: mentionOpen },
		);
	});

	test("an explicit ownerTarget overrides the app owners: the app owner is refused when it differs", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-app-owner-explicit-"));
		const config: GatewayConfig = {
			schemaVersion: 1,
			home,
			gjc: { agentDir: join(home, "agent") },
			configPath: join(home, "config.json"),
			socketPath: join(home, "gateway.sock"),
			dbPath: join(home, "gateway.db"),
			logVerbosity: "info",
			model: { preset: "base" },
			ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: MEMBER } },
			channels: mentionOpen,
		};
		const database = await Store.open(config.dbPath);
		const port = attachTestBrokerOwnership(database, new ScriptedSessionPort(), join(home, "agent"));
		let server: GatewayServer | undefined;
		let client: Awaited<ReturnType<typeof connect>> | undefined;
		try {
			server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
			client = await connect(config.socketPath);
			client.send({
				v: "0.1",
				type: "hello",
				payload: { supportedVersions: ["0.1"], adapter: { platform: "discord", ownerIds: [APP_OWNER] } },
			});
			await Bun.sleep(5);
			const key = "discord/channel/public";
			expect(
				await requestOverSocket(client, "app-owner-set", "chat.send", {
					origin: CHANNEL,
					text: "/model set frontier-heavy",
					engagement: as(APP_OWNER),
				}),
			).toMatchObject({ type: "response", result: { turnId: null, engaged: false } });
			expect(
				await requestOverSocket(client, "configured-owner-set", "chat.send", {
					origin: CHANNEL,
					text: "/model set frontier-heavy",
					engagement: as(MEMBER),
				}),
			).toMatchObject({ type: "response", result: { engaged: true } });
			expect(database.bindings.modelGet(key)?.selection).toEqual({ preset: "frontier-heavy" });
		} finally {
			client?.close();
			try {
				await server?.stop();
			} finally {
				if (!server) database.close();
				await rm(home, { recursive: true, force: true });
			}
		}
	});

	test("the app-owner set never authorizes another platform's origin", async () => {
		await withGateway(
			async ({ client, database }) => {
				const key = "slack/channel/public";
				expect(
					await requestOverSocket(client, "slack-owner-set", "chat.send", {
						origin: SLACK_CHANNEL,
						text: "/model set frontier-heavy",
						engagement: { mentioned: true, group: true, authorId: APP_OWNER },
					}),
				).toMatchObject({ type: "response", result: { turnId: null, engaged: false } });
				// The slack author types into the SLACK adapter, which reports no
				// owners; the bare params (no originPlatform) must ride nothing.
				expect(
					await requestOverSocket(client, "slack-choices", "session.modelChoices", { authorId: APP_OWNER }),
				).toMatchObject({ type: "response", result: { choices: [] } });
				expect(database.bindings.modelGet(key)).toBeUndefined();
			},
			{
				hello: { adapter: { platform: "discord", ownerIds: [APP_OWNER] } },
				channels: { "slack:public": { engagement: "mention-open" as const } },
			},
		);
	});
});
