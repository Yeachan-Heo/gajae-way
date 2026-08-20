import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { DiscordOutbox } from "../../src/adapter/discord/outbox";
import { DiscordRouteHandler } from "../../src/adapter/discord/route";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { DiscordFixture, DiscordFixtureClock } from "../fixtures/discord-fixture";
import { FakeBrokerFixture } from "../helpers/main-session";

interface Gateway {
	readonly fixture: FakeBrokerFixture;
	readonly core: WayCoreHandle;
	readonly host: MainSessionHost;
	readonly client: RpcClient;
	stop(): Promise<void>;
}

const gateways: Gateway[] = [];

afterEach(async () => {
	for (const gateway of gateways.splice(0)) await gateway.stop();
});

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// Listener startup races are expected.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (predicate()) return;
		await Bun.sleep(20);
	}
	throw new Error(message);
}

async function gateway(): Promise<Gateway> {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	fs.mkdirSync(corpus, { recursive: true });
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]
path = "${corpus}"
workspace = "${fixture.workspace}"

[injection]
files = []

[main_session]
session_id = "${fixture.sessionId}"

[surfaces.owner]
id = "discord:owner-dm"
platform = "discord"
kind = "dm"
`,
	);
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(path.join(fixture.root, "state"));
	const state = new GatewayStateStore(core);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
		tailTimeoutMs: 100,
	});
	await bootstrapMainSession({ confirm: true, profile, state, supervisor, sessionId: fixture.sessionId });
	const resumed = await strictResumeMainSession({ profile, state, supervisor });
	const host = createMainSessionHost({
		supervisor,
		identity: resumed.identity,
		state,
		journal: core,
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	const submit = createMainAdmissionHandler(host, profile, core);
	const socketPath = path.join(fixture.root, "state", "rpc.sock");
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		throw new RpcBridgeException(-32601, "method not found");
	};
	core.startRpcServer(socketPath, createRpcBridge(core, handler));
	const client = await connectEventually(socketPath);
	const output: Gateway = {
		fixture,
		core,
		host,
		client,
		async stop() {
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			fixture.dispose();
		},
	};
	gateways.push(output);
	return output;
}

function outbox(client: RpcClient, fixture: DiscordFixture): DiscordOutbox {
	return new DiscordOutbox({
		rpc: client,
		platform: fixture,
		route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		claimTtlMs: 5_000,
		readWaitMs: 0,
	});
}

test("Discord inbound uses message-id idempotency and egress consumes finalized external supervisor output", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
			acknowledgement: { now: clock.now, budgetMs: 2_000 },
		});
		const unsubscribe = fixture.onMessage(async message => {
			await route.handle(message);
		});
		const inbound = { id: "message-1", channelId: "123456789012345678", text: "external broker round trip" };
		await Promise.all([fixture.emitMessage(inbound), fixture.emitMessage(inbound)]);
		unsubscribe();
		expect(fixture.acknowledgements).toHaveLength(1);
		expect(gatewayUnderTest.fixture.commands()).toHaveLength(1);
		await eventually(
			() => gatewayUnderTest.core.journalRead(undefined, 20).events.some(event => event.kind === "assistant_message"),
			"external assistant output was not journaled",
		);
		expect(await outbox(gatewayUnderTest.client, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toEqual([expect.objectContaining({ channelId: "123456789012345678", text: "ack" })]);
	} finally {
		await fixture.disconnect();
	}
});

test("Discord outbox settles server-side journal delivery before a restart can repost", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "settle this" }));
		expect(await outbox(gatewayUnderTest.client, fixture).runOnce()).toBe("sent");
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
		expect(await outbox(gatewayUnderTest.client, fixture).runOnce()).toBe("idle");
		expect(fixture.sends).toHaveLength(1);
	} finally {
		await fixture.disconnect();
	}
});
