import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient } from "../helpers/rpc-client";
import { FakeBrokerFixture } from "../helpers/main-session";

interface HostedServer {
	readonly fixture: FakeBrokerFixture;
	readonly core: WayCoreHandle;
	readonly state: GatewayStateStore;
	readonly profile: ReturnType<typeof loadWayProfile>;
	readonly host: MainSessionHost;
	readonly client: RpcClient;
	stop(): Promise<void>;
}

const servers: HostedServer[] = [];

afterEach(async () => {
	for (const server of servers.splice(0)) await server.stop();
});

function profileFor(fixture: FakeBrokerFixture): ReturnType<typeof loadWayProfile> {
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
id = "owner"
platform = "test"
kind = "dm"

[[surfaces.known]]
id = "guest"
platform = "test"
kind = "channel"
`,
	);
	return loadWayProfile(profilePath);
}

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

async function eventually<T>(read: () => T | undefined, message: string): Promise<T> {
	for (let attempt = 0; attempt < 500; attempt += 1) {
		const value = read();
		if (value !== undefined) return value;
		await Bun.sleep(20);
	}
	throw new Error(message);
}

async function hostedServer(): Promise<HostedServer> {
	const fixture = new FakeBrokerFixture();
	const profile = profileFor(fixture);
	const stateDir = path.join(fixture.root, "state");
	const core = loadWayCore().WayCore.open(stateDir);
	const state = new GatewayStateStore(core);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
		tailTimeoutMs: 100,
		commandTimeoutMs: 1_000,
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
	const submit = createMainAdmissionHandler(host, profile, core, { newOpRef: () => crypto.randomUUID() });
	const answer = createMainGateAnswerHandler(host, core);
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		if (method === "main.gate.answer") return await answer(params);
		throw new RpcBridgeException(-32601, `method not found: ${method}`);
	};
	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, handler));
	const client = await connectEventually(socketPath);
	const server: HostedServer = {
		fixture,
		core,
		state,
		profile,
		host,
		client,
		async stop() {
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			fixture.dispose();
		},
	};
	servers.push(server);
	return server;
}

test("main.submit returns admission before a held external turn and preserves idempotent delivery", async () => {
	const server = await hostedServer();
	server.fixture.holdNextTurn();
	const response = await server.client.request(
		"main.submit",
		{ text: "held owner prompt", surface_id: "owner", idempotency_key: "held-owner" },
		{ timeoutMs: 1_000 },
	);
	expect(response.error).toBeUndefined();
	const heldOpRef = (response.result as { op_ref: string }).op_ref;
	expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt", op_ref: expect.any(String) });
	expect(server.fixture.commands()).toHaveLength(1);
	expect(server.core.rpcBridgeStats().timeouts).toBe(0);
	expect(server.state.read().growthIntent).toBeDefined();
	expect(server.core.journalRead(undefined, 100).events.filter(event => event.kind === "assistant_message")).toHaveLength(0);

	const status = await server.client.request("way.status", {});
	expect(status.result).toMatchObject({ turn_state: "busy" });
	const followUp = await server.client.request(
		"main.submit",
		{ text: "guest continuation", surface_id: "guest", idempotency_key: "guest-followup" },
		{ timeoutMs: 1_000 },
	);
	expect(followUp.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });

	const replay = await server.client.request(
		"main.submit",
		{ text: "held owner prompt", surface_id: "owner", idempotency_key: "held-owner" },
		{ timeoutMs: 1_000 },
	);
	expect(replay.result).toEqual(response.result);
	expect(server.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === "held owner prompt")).toHaveLength(1);

	server.fixture.complete(heldOpRef, { text: "final delayed answer" });
	const assistant = await eventually(
		() =>
			server.core
				.journalRead(undefined, 100)
				.events.find(event => event.kind === "assistant_message" && JSON.parse(event.payloadJson).text === "final delayed answer"),
		"held external turn did not publish an assistant message",
	);
	expect(JSON.parse(assistant.payloadJson)).toMatchObject({ finalized: true, text: "final delayed answer" });
	await eventually(() => (server.state.read().growthIntent === undefined ? true : undefined), "growth intent did not settle");
}, 15_000);

test("main.events.read receives one stable lifecycle attempt from overlapping external SDK events", async () => {
	const server = await hostedServer();
	server.fixture.holdNextTurn();
	const response = await server.client.request("main.submit", {
		text: "journal shape",
		surface_id: "owner",
		idempotency_key: "journal-shape",
	});
	const opRef = (response.result as { op_ref: string }).op_ref;
	server.fixture.complete(opRef, { text: "finalized only" });
	await eventually(
		() => (server.core.journalRead(undefined, 100).events.filter(event => event.kind === "turn_end").length === 1 ? true : undefined),
		"external lifecycle did not settle",
	);
	const events = await server.client.request("main.events.read", { cursor: "1:0", kinds: ["turn_start", "assistant_message", "turn_end"] });
	const rows = (events.result as { events: Array<{ kind: string; payload: Record<string, unknown> }> }).events;
	expect(rows.map(row => row.kind)).toEqual(["turn_start", "assistant_message", "turn_end"]);
	expect(rows[0]?.payload).toMatchObject({ attempt_id: `${server.fixture.sessionId}:${opRef}`, lineage: "main" });
	expect(rows[1]?.payload).toMatchObject({ finalized: true, text: "finalized only" });
	expect(rows[2]?.payload).toMatchObject({ attempt_id: `${server.fixture.sessionId}:${opRef}`, lineage: "main" });
}, 15_000);

test("post-admission external failure degrades gateway health and unsupported gate answers stay explicit", async () => {
	const server = await hostedServer();
	server.fixture.holdNextTurn();
	const response = await server.client.request("main.submit", {
		text: "fails later",
		surface_id: "owner",
		idempotency_key: "fails-later",
	});
	const opRef = (response.result as { op_ref: string }).op_ref;
	server.fixture.complete(opRef, { failure: true });
	await eventually(() => (server.host.degraded ? true : undefined), "host did not degrade after external terminal failure");
	const health = await server.client.request("way.health", {});
	expect(health.result).toMatchObject({ status: "unhealthy", state: "degraded", reason: "turn_execution_failed" });
	server.host.gates.observeOpen({ gateId: "unsupported", expectedSessionId: server.fixture.sessionId });
	const gate = await server.client.request("main.gate.answer", {
		gate_id: "unsupported",
		expected_session_id: server.fixture.sessionId,
		answer: { value: true },
		idempotency_key: "unsupported-gate",
	});
	expect(gate.result).toEqual({ accepted: false, gate_state: "unsupported" });
	const replay = await server.client.request("main.gate.answer", {
		gate_id: "unsupported",
		expected_session_id: server.fixture.sessionId,
		answer: { value: true },
		idempotency_key: "unsupported-gate",
	});
	expect(replay.result).toEqual({ accepted: false, gate_state: "unsupported" });
}, 15_000);
