import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import type { CreateSdkSessionInput, HostedSdkSession, MainSessionSdk, ResumeSdkSessionInput } from "../../src/main-session/sdk";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { FileSdkDouble } from "../helpers/main-session";
import { RpcClient } from "../helpers/rpc-client";
import { ManagedProcessRegistry } from "../helpers/managed-process";

const temporaryDirectories: string[] = [];
const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-p4-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

function socketTemporaryDirectory(): string {
	const directory = fs.mkdtempSync("/tmp/gajaeway-");
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

async function eventually<T>(read: () => T | undefined, message: string): Promise<T> {
	const deadline = Date.now() + 1_000;
	for (;;) {
		const result = read();
		if (result !== undefined) return result;
		if (Date.now() >= deadline) throw new Error(message);
		await Bun.sleep(10);
	}
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

interface ControlledPromptSdkOptions {
	readonly failure?: Error;
}

class ControlledPromptSdk implements MainSessionSdk {
	readonly #delegate = new FileSdkDouble();
	readonly #started: Promise<void>;
	readonly #released: Promise<void>;
	readonly #failure: Error | undefined;
	#markStarted!: () => void;
	#release!: () => void;
	#promptCalls = 0;

	constructor(options: ControlledPromptSdkOptions = {}) {
		this.#failure = options.failure;
		this.#started = new Promise(resolve => {
			this.#markStarted = resolve;
		});
		this.#released = new Promise(resolve => {
			this.#release = resolve;
		});
	}

	get promptCalls(): number {
		return this.#promptCalls;
	}

	async waitForPromptStart(): Promise<void> {
		await this.#started;
	}

	release(): void {
		this.#release();
	}

	async createNew(input: CreateSdkSessionInput): Promise<HostedSdkSession> {
		return await this.#delegate.createNew(input);
	}

	async findBootstrapNonceCandidates(workspace: string, nonce: string): Promise<readonly string[]> {
		return await this.#delegate.findBootstrapNonceCandidates(workspace, nonce);
	}

	async openExistingStrict(input: ResumeSdkSessionInput): Promise<HostedSdkSession> {
		const session = await this.#delegate.openExistingStrict(input);
		return {
			sessionFile: session.sessionFile,
			sessionId: session.sessionId,
			subscribe: listener => session.subscribe(listener),
			subscribeGates: listener => session.subscribeGates(listener),
			prompt: async text => {
				this.#promptCalls += 1;
				this.#markStarted();
				await this.#released;
				if (this.#failure) throw this.#failure;
				await session.prompt(text);
			},
			steer: text => session.steer(text),
			followUp: text => session.followUp(text),
			followUpQueueDepth: () => session.followUpQueueDepth(),
			answerGate: (gateId, answer, idempotencyKey) => session.answerGate(gateId, answer, idempotencyKey),
			sendBootstrapMessage: nonce => session.sendBootstrapMessage(nonce),
			dispose: () => session.dispose(),
		};
	}
}

interface HostedServer<Sdk extends MainSessionSdk = FileSdkDouble> {
	readonly root: string;
	readonly stateDirectory: string;
	readonly core: WayCoreHandle;
	readonly profile: ReturnType<typeof loadWayProfile>;
	readonly state: GatewayStateStore;
	readonly host: MainSessionHost;
	readonly sdk: Sdk;
	readonly sessionFile: string;
	readonly client: RpcClient;
	stop(): Promise<void>;
}

async function hostedServer(): Promise<HostedServer<FileSdkDouble>> {
	return await hostedServerWithSdk(new FileSdkDouble());
}

async function hostedServerWithSdk<Sdk extends MainSessionSdk>(sdk: Sdk): Promise<HostedServer<Sdk>> {
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
		profile,
		state,
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

test("durable failed-closed state is unhealthy through RPC and health.json", async () => {
	const root = socketTemporaryDirectory();
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	const core = loadWayCore().WayCore.open(stateDirectory);
	new GatewayStateStore(core).markFailedClosed("growth_protocol_invalid");
	const child = managedProcesses.spawnDaemon({
		cmd: ["bun", "src/main.ts", "serve", "--state-dir", stateDirectory, "--profile", profilePath, "--fail-closed-linger-ms", "2000"],
		cwd: process.cwd(),
		env: { ...process.env },
		stderr: "pipe",
	});
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		let health: unknown;
		for (let attempt = 0; attempt < 100; attempt += 1) {
			const response = await client.request("way.health", {});
			health = response.result;
			if ((health as { state?: unknown } | undefined)?.state === "failed_closed") break;
			await Bun.sleep(10);
		}
		expect(health).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "growth_protocol_invalid" });
		const status = await client.request("way.status", {});
		expect(status.result).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "growth_protocol_invalid" });
		const healthFile = await eventually(() => {
			try {
				const payload = JSON.parse(fs.readFileSync(path.join(stateDirectory, "health.json"), "utf8")) as { state?: unknown };
				return payload.state === "failed_closed" ? payload : undefined;
			} catch {
				return undefined;
			}
		}, "failed-closed health.json was not written");
		expect(healthFile).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "growth_protocol_invalid" });
	} finally {
		client?.close();
		await managedProcesses.stopDaemon(child);
	}
}, 10_000);

test("bridge exceptions expose a safe reason and log correlated diagnostics", async () => {
	const stateDirectory = socketTemporaryDirectory();
	const child = managedProcesses.spawnDaemon({
		cmd: [
			"bun",
			"-e",
			`const { loadWayCore } = await import("./src/native-loader.ts");
const { createRpcBridge } = await import("./src/rpc-bridge.ts");
const core = loadWayCore().WayCore.open(process.env.GAJAEWAY_STATE_DIR);
core.startRpcServer(process.env.GAJAEWAY_RPC_SOCKET, createRpcBridge(core, () => {
  const error = new Error("simulated bridge failure");
  error.name = "SyntheticBridgeFailure";
  error.reason = "growth_protocol_invalid";
  throw error;
}));
process.once("SIGTERM", () => { core.shutdownRpcServer(); process.exit(0); });
await new Promise(() => {});`,
		],
		cwd: process.cwd(),
		env: {
			...process.env,
			GAJAEWAY_STATE_DIR: stateDirectory,
			GAJAEWAY_RPC_SOCKET: path.join(stateDirectory, "rpc.sock"),
		},
		stderr: "pipe",
	});
	let client: RpcClient | undefined;
	let correlationId: string | undefined;
	let stderr = "";
	try {
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		const response = await client.request("main.throw", {});
		const data = response.error?.data;
		if (typeof data !== "object" || data === null || Array.isArray(data)) throw new Error("bridge exception did not return object data");
		const safeData = data as { correlation_id?: unknown; reason?: unknown };
		if (typeof safeData.correlation_id !== "string" || safeData.reason !== "growth_protocol_invalid") {
			throw new Error(`bridge exception returned unsafe data: ${JSON.stringify(data)}`);
		}
		if (Object.keys(data).sort().join(",") !== "correlation_id,reason") {
			throw new Error(`bridge exception returned unexpected data fields: ${JSON.stringify(data)}`);
		}
		correlationId = safeData.correlation_id;
		expect(response.error).toMatchObject({
			code: -32603,
			message: "bridge_exception",
			data: { correlation_id: expect.stringMatching(/^rpc-/), reason: "growth_protocol_invalid" },
		});
	} finally {
		client?.close();
		await managedProcesses.stopDaemon(child);
		stderr = await new Response(child.stderr).text();
	}
	expect(correlationId).toBeDefined();
	expect(stderr).toContain(`correlation_id=${correlationId}`);
	expect(stderr).toContain("reason=growth_protocol_invalid");
	expect(stderr).toContain('error_type="SyntheticBridgeFailure"');
	expect(stderr).toContain('message="simulated bridge failure"');
	expect(stderr).toContain("stack=");
}, 10_000);

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

test("main.submit admits a controllably delayed turn without a bridge timeout", async () => {
	const sdk = new ControlledPromptSdk();
	const server = await hostedServerWithSdk(sdk);
	try {
		const prompt = await server.client.request(
			"main.submit",
			{ text: "delayed owner prompt", surface_id: "owner", idempotency_key: "delayed-prompt" },
			{ timeoutMs: 1_000 },
		);
		expect(prompt.error).toBeUndefined();
		expect(prompt.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		await sdk.waitForPromptStart();
		expect(server.state.read().growthIntent).toBeDefined();

		const followUp = await server.client.request(
			"main.submit",
			{ text: "while prompt is delayed", surface_id: "guest", idempotency_key: "delayed-follow-up" },
			{ timeoutMs: 1_000 },
		);
		expect(followUp.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });

		const replay = await server.client.request(
			"main.submit",
			{ text: "delayed owner prompt", surface_id: "owner", idempotency_key: "delayed-prompt" },
			{ timeoutMs: 1_000 },
		);
		expect(replay.result).toEqual(prompt.result);
		expect(sdk.promptCalls).toBe(1);
		expect(server.core.rpcBridgeStats().timeouts).toBe(0);

		sdk.release();
		await eventually(
			() => server.core.journalRead(undefined, 100).events.find(event => event.kind === "assistant_message"),
			"delayed SDK turn did not publish its finalized assistant message",
		);
		await eventually(
			() => (server.state.read().growthIntent === undefined ? true : undefined),
			"delayed SDK turn did not settle its growth window",
		);

	} finally {
		sdk.release();
		await server.stop();
	}
});

test("main.submit returns delivered_as before the delayed assistant message is journaled", async () => {
	const sdk = new ControlledPromptSdk();
	const server = await hostedServerWithSdk(sdk);
	try {
		const response = await server.client.request(
			"main.submit",
			{ text: "order response before assistant", surface_id: "owner", idempotency_key: "delivery-before-assistant" },
			{ timeoutMs: 1_000 },
		);
		expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		await sdk.waitForPromptStart();
		expect(server.core.journalRead(undefined, 100).events.filter(event => event.kind === "assistant_message")).toHaveLength(0);

		sdk.release();
		const assistant = await eventually(
			() => server.core.journalRead(undefined, 100).events.find(event => event.kind === "assistant_message"),
			"assistant message was not journaled after delayed turn release",
		);
		expect(JSON.parse(assistant.payloadJson)).toMatchObject({ finalized: true, text: "ack" });
		await eventually(
			() => (server.state.read().growthIntent === undefined ? true : undefined),
			"assistant turn did not settle its growth window",
		);
	} finally {
		sdk.release();
		await server.stop();
	}
});

test("main.submit exposes a post-acceptance SDK failure through gateway health", async () => {
	const sdk = new ControlledPromptSdk({ failure: new Error("injected delayed SDK failure") });
	const server = await hostedServerWithSdk(sdk);
	try {
		const response = await server.client.request(
			"main.submit",
			{ text: "accepted then fails", surface_id: "owner", idempotency_key: "post-acceptance-failure" },
			{ timeoutMs: 1_000 },
		);
		expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		await sdk.waitForPromptStart();

		sdk.release();
		await eventually(() => (server.host.degraded ? true : undefined), "host did not expose the accepted operation failure");
		const health = await server.client.request("way.health", {});
		expect(health.result).toMatchObject({ status: "unhealthy", state: "degraded", reason: "turn_execution_failed" });
		expect(server.state.read().growthIntent).toBeUndefined();
	} finally {
		sdk.release();
		await server.stop();
	}
});

test("queued follow-up and owner prompt share one growth window and restart cleanly", async () => {
	const server = await hostedServer();
	try {
		const queued = await server.client.request("main.submit", {
			text: "queued follow-up",
			surface_id: "guest",
			idempotency_key: "queued-follow-up",
		});
		expect(queued.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });
		expect(server.host.followUpQueueDepth).toBe(1);
		expect(server.state.read().growthIntent).toBeDefined();

		// The queued SDK work may have appended before the next terminal turn while
		// the host is otherwise idle. The owner admission must join this same window.
		server.sdk.appendRaw(server.sessionFile, { type: "message", role: "user", content: "queued follow-up" });
		server.sdk.appendRaw(server.sessionFile, { type: "message", role: "assistant", content: "queued follow-up completed" });
		const ownerPrompt = await server.client.request("main.submit", {
			text: "owner prompt while queued",
			surface_id: "owner",
			idempotency_key: "owner-prompt-while-queued",
		});
		expect(ownerPrompt.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		expect(server.host.followUpQueueDepth).toBe(0);
		expect(server.state.read()).toMatchObject({ bootstrapState: "COMMITTED", growthIntent: undefined, failedClosedReason: undefined });
		const transcript = fs.readFileSync(server.sessionFile, "utf8");
		expect(transcript).toContain("owner prompt while queued");
		expect(transcript).toContain("queued follow-up completed");

		await server.host.dispose();
		const restarted = await strictResumeMainSession({ profile: server.profile, state: server.state, sdk: server.sdk });
		expect(restarted.recoveredGrowthIntent).toBe(false);
		await restarted.session.dispose();
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
