import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { OwnerConsole } from "../../src/console/console";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient, type JsonRpcClient, type RpcRequestOptions, type JsonRpcResponse } from "../../src/rpc-client";
import { FileSdkDouble } from "../helpers/main-session";

const temporaryDirectories: string[] = [];

afterEach(async () => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-console-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The socket can exist before the native accept loop is ready.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

function ownerProfile(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`;
}

interface HostedConsoleGateway {
	readonly core: WayCoreHandle;
	readonly client: RpcClient;
	readonly host: MainSessionHost;
	readonly sdk: FileSdkDouble;
	readonly sessionFile: string;
	readonly socketPath: string;
	stop(): Promise<void>;
}

async function hostedConsoleGateway(): Promise<HostedConsoleGateway> {
	const root = temporaryDirectory("host");
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
	const answer = createMainGateAnswerHandler(host, core);
	const bridge: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		if (method === "main.gate.answer") return await answer(params);
		throw new RpcBridgeException(-32601, `method not found: ${method}`);
	};
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, bridge));
	core.setRpcHealth("running");
	const client = await connectEventually(socketPath);
	return {
		core,
		client,
		host,
		sdk,
		sessionFile: resumed.identity.canonicalPath,
		socketPath,
		async stop() {
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			await Bun.sleep(40);
		},
	};
}

function recordingClient(client: RpcClient): { rpc: JsonRpcClient; calls: Array<{ method: string; params: unknown }> } {
	const calls: Array<{ method: string; params: unknown }> = [];
	return {
		calls,
		rpc: {
			async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
				calls.push({ method, params });
				return await client.request(method, params, options);
			},
			close(): void {
				client.close();
			},
		},
	};
}

test("console submits through real UDS, renders finalized replies before settlement, and resumes its server checkpoint after restart", async () => {
	const gateway = await hostedConsoleGateway();
	const output: string[] = [];
	let cursorAtAssistantRender: string | undefined;
	const recorded = recordingClient(gateway.client);
	let idempotencyCount = 0;
	const consoleSurface = new OwnerConsole({
		rpc: recorded.rpc,
		ownerSurfaceId: "owner",
		write: text => {
			if (text.startsWith("Assistant:")) cursorAtAssistantRender = gateway.core.consumerCursor("way-console");
			output.push(text);
		},
		idempotencyKey: () => `console-submit-${++idempotencyCount}`,
	});
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		expect(recorded.calls.filter(call => call.method === "way.health")).toHaveLength(1);
		expect(recorded.calls.filter(call => call.method === "way.status")).toHaveLength(1);
		await consoleSurface.submit("owner request");
		expect(await consoleSurface.consumeOnce()).toBe("rendered");

		const submit = recorded.calls.find(call => call.method === "main.submit");
		expect(submit?.params).toEqual({ text: "owner request", surface_id: "owner", idempotency_key: "console-submit-1" });
		expect(output.join("")).toContain("Delivered as: prompt");
		const consumerClaim = recorded.calls.find(call => call.method === "consumer.claim");
		expect(consumerClaim?.params).toEqual({ consumer_id: "way-console", claim_ttl_ms: 5_000 });
		const eventRead = recorded.calls.find(call => call.method === "main.events.read");
		expect(eventRead?.params).toEqual({
			consumer_id: "way-console",
			limit: 100,
			wait_ms: 1_000,
			kinds: ["assistant_message", "turn_start", "turn_end", "gate_open", "gate_resolved", "health_change", "lock_event"],
		});
		expect(output.join("")).toContain("Main turn started — busy.");
		expect(output.join("")).toContain("Assistant:\nack\n");
		expect(output.join("")).toContain("Main turn ended — idle.");
		expect(cursorAtAssistantRender).toBe("1:0");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:3");
		expect(gateway.core.consumerOutbox("way-console")).toHaveLength(3);
		gateway.core.journalAppend("health_change", JSON.stringify({ state: "degraded", reason: "journal_append_failed" }));
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(output.join("")).toContain("Gateway health changed: degraded reason=journal_append_failed.");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:4");
		expect(gateway.core.consumerOutbox("way-console")).toHaveLength(4);

		gateway.client.close();
		const restartedClient = await connectEventually(gateway.socketPath);
		const restartedOutput: string[] = [];
		const restarted = new OwnerConsole({
			rpc: restartedClient,
			ownerSurfaceId: "owner",
			write: text => restartedOutput.push(text),
			readWaitMs: 0,
		});
		try {
			expect((await restarted.start()).accepted).toBe(true);
			expect(await restarted.consumeOnce()).toBe("idle");
			expect(restartedOutput.join("")).not.toContain("Assistant:\nack\n");
			expect(gateway.core.consumerCursor("way-console")).toBe("1:4");
		} finally {
			restartedClient.close();
		}
	} finally {
		await gateway.stop();
	}
});

test("console refusal drill blocks interactive mode when a real UDS daemon is failed closed", async () => {
	const stateDirectory = temporaryDirectory("failed-closed");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new RpcBridgeException(-32601, "method not found");
	}));
	core.setRpcHealth("failed_closed", "profile_drift");
	const client = await connectEventually(socketPath);
	const output: string[] = [];
	try {
		const consoleSurface = new OwnerConsole({ rpc: client, ownerSurfaceId: "owner", write: text => output.push(text), readWaitMs: 0 });
		const startup = await consoleSurface.start();
		expect(startup).toMatchObject({ accepted: false });
		expect(output.join("")).toContain("Refusing interactive console");
		expect(output.join("")).toContain("failed closed");
		expect(output.join("")).toContain("profile_drift");
	} finally {
		client.close();
		core.shutdownRpcServer();
		await Bun.sleep(40);
	}
});

test("console gate drill uses durable gate fencing, rejects a mismatched session, and renders resolution", async () => {
	const gateway = await hostedConsoleGateway();
	const output: string[] = [];
	const consoleSurface = new OwnerConsole({ rpc: gateway.client, ownerSurfaceId: "owner", write: text => output.push(text), readWaitMs: 0 });
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.sdk.openGate(gateway.sessionFile, "gate-live");
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(output.join("")).toContain("Gate opened: gate_id=gate-live expected_session_id=");

		await expect(consoleSurface.answerGate("gate-live", "wrong-session", { selected: ["Yes"] })).rejects.toMatchObject({ code: 1102 });
		expect(await consoleSurface.answerGate("gate-live", gateway.host.sessionId, { selected: ["Yes"] })).toEqual({
			accepted: true,
			gateState: "resolved",
		});
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(output.join("")).toContain("Gate resolved: gate_id=gate-live.");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:2");
	} finally {
		await gateway.stop();
	}
});
