import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { FileSdkDouble } from "../helpers/main-session";
import { RpcClient } from "../helpers/rpc-client";

const temporaryDirectories: string[] = [];

afterEach(async () => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-p4-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The listener can exist before its accept loop is ready.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

function error(response: Awaited<ReturnType<RpcClient["request"]>>): { code: number; message: string } {
	if (!response.error) throw new Error(`Expected an RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

function profileContents(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"

[[surfaces.known]]
id = "guest"
platform = "test"
kind = "channel"
`;
}

interface HostedServer {
	readonly root: string;
	readonly stateDirectory: string;
	readonly core: WayCoreHandle;
	readonly host: MainSessionHost;
	readonly sdk: FileSdkDouble;
	readonly sessionFile: string;
	readonly client: RpcClient;
	stop(): Promise<void>;
}

async function hostedServer(): Promise<HostedServer> {
	const root = temporaryDirectory("host");
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(stateDirectory);
	const state = new GatewayStateStore(core);
	const sdk = new FileSdkDouble();
	await bootstrapMainSession({ confirm: true, profile, state, sdk });
	const resumed = await strictResumeMainSession({ profile, state, sdk });
	const host = createMainSessionHost({ session: resumed.session, identity: resumed.identity, state, journal: core });
	const submit = createMainAdmissionHandler(host, profile, core, { newOpRef: () => crypto.randomUUID() });
	const answer = createMainGateAnswerHandler(host, core);
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		if (method === "main.gate.answer") return await answer(params);
		throw new RpcBridgeException(-32601, `method not found: ${method}`);
	};
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, handler));
	const client = await connectEventually(socketPath);
	return {
		root,
		stateDirectory,
		core,
		host,
		sdk,
		sessionFile: resumed.identity.canonicalPath,
		client,
		async stop() {
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			await Bun.sleep(40);
		},
	};
}

test("main.submit derives delivery only from profile ownership and turn state", async () => {
	const server = await hostedServer();
	try {
		const prompt = await server.client.request("main.submit", {
			text: "owner prompt",
			surface_id: "owner",
			idempotency_key: "prompt-1",
		});
		expect(prompt.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		expect((prompt.result as { op_ref: string }).op_ref).toBeString();

		const callerDelivery = await server.client.request("main.submit", {
			text: "bad",
			surface_id: "owner",
			idempotency_key: "bad-delivery",
			delivered_as: "steer",
		});
		expect(error(callerDelivery)).toMatchObject({ code: -32602 });
		const unknownSurface = await server.client.request("main.submit", {
			text: "bad",
			surface_id: "unknown",
			idempotency_key: "unknown-surface",
		});
		expect(error(unknownSurface)).toMatchObject({ code: 1300, message: "unknown_surface" });

		server.sdk.emitEvent(server.sessionFile, { type: "turn_start" });
		const steer = await server.client.request("main.submit", {
			text: "owner steer",
			surface_id: "owner",
			idempotency_key: "steer-1",
		});
		expect(steer.result).toMatchObject({ accepted: true, delivered_as: "steer" });
		server.sdk.emitEvent(server.sessionFile, { type: "turn_end" });
		const followUp = await server.client.request("main.submit", {
			text: "guest follow-up",
			surface_id: "guest",
			idempotency_key: "follow-up-1",
		});
		expect(followUp.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });
		const status = await server.client.request("way.status");
		expect(status.result).toMatchObject({ turn_state: "idle", follow_up_queue_depth: 1 });

		const replay = await server.client.request("main.submit", {
			text: "guest follow-up",
			surface_id: "guest",
			idempotency_key: "follow-up-1",
		});
		expect(replay.result).toEqual(followUp.result);
	} finally {
		await server.stop();
	}
});

test("main gate answers use durable IDs, session fencing, expiry, and replay state", async () => {
	const server = await hostedServer();
	try {
		server.sdk.openGate(server.sessionFile, "gate-live");
		const opened = await server.client.request("main.events.read", { cursor: "1:0", kinds: ["gate_open"] });
		expect((opened.result as { events: Array<{ kind: string; payload: { gate_id: string } }> }).events).toContainEqual(
			expect.objectContaining({ kind: "gate_open", payload: expect.objectContaining({ gate_id: "gate-live" }) }),
		);

		const mismatch = await server.client.request("main.gate.answer", {
			gate_id: "gate-live",
			expected_session_id: "other-session",
			answer: { selected: ["Yes"] },
			idempotency_key: "gate-mismatch",
		});
		expect(error(mismatch)).toMatchObject({ code: 1102, message: "gate_session_mismatch" });
		const missing = await server.client.request("main.gate.answer", {
			gate_id: "gate-missing",
			expected_session_id: server.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "gate-missing",
		});
		expect(error(missing)).toMatchObject({ code: 1100, message: "gate_not_found" });
		server.sdk.openGate(server.sessionFile, "gate-expired", { expiresAt: Date.now() - 1 });
		const expired = await server.client.request("main.gate.answer", {
			gate_id: "gate-expired",
			expected_session_id: server.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "gate-expired",
		});
		expect(error(expired)).toMatchObject({ code: 1101, message: "gate_expired" });

		const accepted = await server.client.request("main.gate.answer", {
			gate_id: "gate-live",
			expected_session_id: server.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "gate-live-answer",
		});
		expect(accepted.result).toEqual({ accepted: true, gate_state: "resolved" });
		const replay = await server.client.request("main.gate.answer", {
			gate_id: "gate-live",
			expected_session_id: server.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "gate-live-answer",
		});
		expect(replay.result).toEqual({ accepted: true, gate_state: "already_resolved" });
		const resolved = await server.client.request("main.events.read", { cursor: "1:0", kinds: ["gate_resolved"] });
		expect((resolved.result as { events: Array<{ kind: string; payload: { gate_id: string } }> }).events).toContainEqual(
			expect.objectContaining({ kind: "gate_resolved", payload: expect.objectContaining({ gate_id: "gate-live" }) }),
		);
	} finally {
		await server.stop();
	}
});

test("main.events.read long-polls with filtering and returns gaps", async () => {
	const stateDirectory = temporaryDirectory("events");
	const core = loadWayCore().WayCore.open(stateDirectory);
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new RpcBridgeException(-32601, "method not found");
	}));
	const client = await connectEventually(socketPath);
	try {
		core.journalAppend("turn_start", JSON.stringify({ turn: "one" }));
		core.journalAppend("assistant_message", JSON.stringify({ text: "skip" }));
		const filtered = await client.request("main.events.read", { cursor: "1:0", kinds: ["turn_start"] });
		expect(filtered.result).toMatchObject({
			events: [expect.objectContaining({ kind: "turn_start" })],
			next_cursor: "1:2",
		});

		const wait = client.request("main.events.read", { cursor: "1:2", kinds: ["assistant_message"], wait_ms: 1_000 });
		await Bun.sleep(50);
		core.journalAppend("assistant_message", JSON.stringify({ text: "arrived" }));
		const delivered = await wait;
		expect(delivered.result).toMatchObject({ events: [expect.objectContaining({ kind: "assistant_message" })] });

		const gap = await client.request("main.events.read", { cursor: "0:0" });
		expect(gap.result).toMatchObject({ events: [], gap: { resync_cursor: "1:0" } });
	} finally {
		client.close();
		core.shutdownRpcServer();
		await Bun.sleep(40);
	}
});

test("consumer claim and commit fence contention and survive an RPC restart", async () => {
	const stateDirectory = temporaryDirectory("consumers");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	let core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new RpcBridgeException(-32601, "method not found");
	}));
	let client = await connectEventually(socketPath);
	try {
		const appended = core.journalAppend("assistant_message", JSON.stringify({ text: "deliver" }));
		const claim = await client.request("consumer.claim", { consumer_id: "discord" });
		expect(claim.result).toMatchObject({ cursor: "1:0" });
		const held = await client.request("consumer.claim", { consumer_id: "discord" });
		expect(error(held)).toMatchObject({ code: 1601, message: "consumer_claim_held" });
		const claimResult = claim.result as { claim_id: string };
		const committed = await client.request("consumer.commit", {
			consumer_id: "discord",
			claim_id: claimResult.claim_id,
			cursor: appended.cursor,
			proofs: [{ seq: Number(appended.seq), platform_msg_id: "message-1", dedupe_key: "discord:message-1" }],
		});
		expect(committed.result).toEqual({ committed_cursor: appended.cursor });
		expect(core.consumerOutbox("discord")).toMatchObject([{ seq: appended.seq, state: "sent", dedupeKey: "discord:message-1" }]);

		client.close();
		core.shutdownRpcServer();
		await Bun.sleep(50);
		core = loadWayCore().WayCore.open(stateDirectory);
		core.startRpcServer(socketPath, createRpcBridge(core, () => {
			throw new RpcBridgeException(-32601, "method not found");
		}));
		client = await connectEventually(socketPath);
		const afterRestart = await client.request("consumer.claim", { consumer_id: "discord" });
		expect(afterRestart.result).toMatchObject({ cursor: appended.cursor });
	} finally {
		client.close();
		core.shutdownRpcServer();
		await Bun.sleep(40);
	}
});
