import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, spyOn, test } from "bun:test";
import { loadDiscordAdapterConfig } from "../../src/adapter/discord/config";
import { runDiscordAdapter } from "../../src/adapter/discord/main";
import { DiscordOutbox } from "../../src/adapter/discord/outbox";
import { DiscordRouteHandler } from "../../src/adapter/discord/route";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { DiscordFixture, DiscordFixtureClock } from "../fixtures/discord-fixture";
import { FileSdkDouble } from "../helpers/main-session";

const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `way-d-${name.slice(0, 8)}-`));
	temporaryDirectories.push(directory);
	return directory;
}


async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The UDS listener can exist before its accept loop is ready.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

interface JournalGateway {
	readonly core: WayCoreHandle;
	readonly client: RpcClient;
	readonly stateDirectory: string;
	stop(): Promise<void>;
}

async function journalGateway(name: string): Promise<JournalGateway> {
	const root = temporaryDirectory(name);
	const stateDirectory = path.join(root, "state");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new RpcBridgeException(-32601, "method not found");
	}));
	const client = await connectEventually(socketPath);
	return {
		core,
		client,
		stateDirectory,
		async stop() {
			client.close();
			core.shutdownRpcServer();
			await Bun.sleep(40);
		},
	};
}

interface OwnerHostGateway extends JournalGateway {
	readonly host: MainSessionHost;
	readonly sdk: FileSdkDouble;
	readonly sessionFile: string;
}

function ownerProfile(corpus: string, workspace: string, adapter = ""): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md"]

[surfaces.owner]
id = "discord:owner-dm"
platform = "discord"
kind = "dm"
${adapter}`;
}

async function ownerHostGateway(): Promise<OwnerHostGateway> {
	const root = temporaryDirectory("owner-host");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDirectory = path.join(root, "state");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, ownerProfile(corpus, workspace));
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(stateDirectory);
	const state = new GatewayStateStore(core);
	const sdk = new FileSdkDouble();
	await bootstrapMainSession({ confirm: true, profile, state, sdk });
	const resumed = await strictResumeMainSession({ profile, state, sdk });
	const host = createMainSessionHost({ session: resumed.session, identity: resumed.identity, state, journal: core });
	const submit = createMainAdmissionHandler(host, profile, core);
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		throw new RpcBridgeException(-32601, "method not found");
	};
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, handler));
	const client = await connectEventually(socketPath);
	return {
		core,
		client,
		stateDirectory,
		host,
		sdk,
		sessionFile: resumed.identity.canonicalPath,
		async stop() {
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			await Bun.sleep(40);
		},
	};
}

function outbox(client: RpcClient, fixture: DiscordFixture, hooks: ConstructorParameters<typeof DiscordOutbox>[0]["hooks"] = undefined): DiscordOutbox {
	return new DiscordOutbox({
		rpc: client,
		platform: fixture,
		route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		claimTtlMs: 5_000,
		readWaitMs: 0,
		hooks,
	});
}

test("Discord fixture inbound uses message-id idempotency, acknowledges before egress, and completes an owner-DM round trip", async () => {
	const gateway = await ownerHostGateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gateway.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
			acknowledgement: { now: clock.now, budgetMs: 2_000 },
		});
		const unsubscribe = fixture.onMessage(async message => {
			await route.handle(message);
		});
		const inbound = { id: "message-1", channelId: "123456789012345678", text: "[[discord-inbound-1]]" };
		await Promise.all([fixture.emitMessage(inbound), fixture.emitMessage(inbound)]);
		unsubscribe();

		expect(fixture.acknowledgements).toHaveLength(1);
		expect(fixture.acknowledgements[0]?.at).toBeLessThanOrEqual(clock.now() + 2_000);
		const transcript = fs.readFileSync(gateway.sessionFile, "utf8");
		expect(transcript.match(/\[\[discord-inbound-1\]\]/g)).toHaveLength(1);

		const delivery = outbox(gateway.client, fixture);
		expect(await delivery.runOnce()).toBe("sent");
		expect(fixture.sends).toEqual([
			expect.objectContaining({ channelId: "123456789012345678", text: "ack", nonce: "way-discord:discord:owner-dm:2" }),
		]);
		expect(gateway.core.consumerCursor("way-discord")).toBe("1:3");
	} finally {
		await fixture.disconnect();
		await gateway.stop();
	}
});

test("Discord egress sends before settlement and a normal restart does not repost", async () => {
	const gateway = await journalGateway("settlement");
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gateway.core.journalAppend("assistant_message", JSON.stringify({ text: "settle this" }));
		let cursorAtSend: string | undefined;
		const first = outbox(gateway.client, fixture, {
			beforeSend: () => {
				cursorAtSend = gateway.core.consumerCursor("way-discord");
			},
		});
		expect(await first.runOnce()).toBe("sent");
		expect(cursorAtSend).toBe("1:0");
		expect(fixture.sends).toHaveLength(1);
		expect(gateway.core.consumerCursor("way-discord")).toBe(appended.cursor);
		expect(gateway.core.consumerOutbox("way-discord")).toEqual([
			expect.objectContaining({ seq: appended.seq, state: "sent", platformMsgId: "discord-send-1", dedupeKey: "way-discord:discord:owner-dm:1" }),
		]);

		const restarted = outbox(gateway.client, fixture);
		expect(await restarted.runOnce()).toBe("idle");
		expect(fixture.sends).toHaveLength(1);
	} finally {
		await fixture.disconnect();
		await gateway.stop();
	}
});

test("crash before Discord send leaves the server checkpoint and resumes from it", async () => {
	const gateway = await journalGateway("crash-before-send");
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gateway.core.journalAppend("assistant_message", JSON.stringify({ text: "retry before send" }));
		const abort = new AbortController();
		const interrupted = outbox(gateway.client, fixture, { beforeSend: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toHaveLength(0);
		expect(gateway.core.consumerCursor("way-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		const restarted = outbox(gateway.client, fixture);
		expect(await restarted.runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		expect(gateway.core.consumerCursor("way-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
		await gateway.stop();
	}
}, 15_000);

test("crash after Discord send but before commit is bounded by nonce dedupe on restart", async () => {
	const gateway = await journalGateway("crash-after-send");
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gateway.core.journalAppend("assistant_message", JSON.stringify({ text: "retry after send" }));
		const abort = new AbortController();
		const interrupted = outbox(gateway.client, fixture, { afterSendBeforeCommit: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toHaveLength(1);
		expect(gateway.core.consumerCursor("way-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		const restarted = outbox(gateway.client, fixture);
		expect(await restarted.runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		expect(fixture.sendAttempts).toEqual([
			expect.objectContaining({ duplicate: false }),
			expect.objectContaining({ duplicate: true, nonce: "way-discord:discord:owner-dm:1" }),
		]);
		expect(gateway.core.consumerCursor("way-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
		await gateway.stop();
	}
}, 15_000);

test("way-discord --check validates the configured token with mocked REST and gateway health", async () => {
	const root = temporaryDirectory("check");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		ownerProfile(
			corpus,
			workspace,
			`
[adapter.discord]
token_env = "TEST_DISCORD_TOKEN"
channel_id = "123456789012345678"
surface_id = "discord:owner-dm"
api_base_url = "https://discord.test/api/v10"
`,
		),
	);
	const environment = { TEST_DISCORD_TOKEN: "test-token", WAY_PROFILE: profilePath } as NodeJS.ProcessEnv;
	const config = loadDiscordAdapterConfig({ environment, profilePath, stateDir: path.join(root, "state") });
	const calls: string[] = [];
	const rpc = {
		async request(method: string): Promise<{ jsonrpc: "2.0"; id: number; result: unknown }> {
			calls.push(method);
			return { jsonrpc: "2.0", id: 1, result: { status: "healthy", state: "running" } };
		},
		close(): void {},
	};
	const fetchMock = async (input: string) => {
		expect(String(input)).toBe("https://discord.test/api/v10/users/@me");
		return new Response(JSON.stringify({ id: "discord-bot" }), { status: 200, headers: { "Content-Type": "application/json" } });
	};
	const log = spyOn(console, "log").mockImplementation(() => undefined);
	try {
		await runDiscordAdapter(["--check", "--profile", profilePath, "--state-dir", path.join(root, "state")], {
			environment,
			fetch: fetchMock,
			rpcConnect: async socketPath => {
				expect(socketPath).toBe(config.rpcSocketPath);
				return rpc;
			},
		});
		expect(calls).toEqual(["way.health"]);
		expect(log).toHaveBeenCalledWith(expect.stringContaining('"status":"ok"'));
	} finally {
		log.mockRestore();
	}
});
