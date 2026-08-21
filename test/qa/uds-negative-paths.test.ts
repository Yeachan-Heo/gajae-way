import * as fs from "node:fs";
import * as os from "node:os";
import { createConnection } from "node:net";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { durableTestJournal, FakeBrokerFixture } from "../helpers/main-session";

function temporaryDirectory(name: string): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-qa-${name}-`));
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

async function stopCore(core: WayCoreHandle, stateDirectory: string): Promise<void> {
	core.shutdownRpcServer();
	await Bun.sleep(40);
	fs.rmSync(stateDirectory, { force: true, recursive: true });
}

async function rawFrameUntilServerCloses(socketPath: string, frame: Uint8Array): Promise<Buffer> {
	return await new Promise<Buffer>((resolve, reject) => {
		const socket = createConnection(socketPath);
		const chunks: Buffer[] = [];
		let connected = false;
		let settled = false;
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			callback();
		};
		const timeout = setTimeout(() => finish(() => reject(new Error("server did not close malformed RPC connection"))), 5_000);
		socket.on("data", chunk => chunks.push(Buffer.from(chunk)));
		socket.once("connect", () => {
			connected = true;
			socket.write(frame);
		});
		socket.once("error", error => {
			if (!connected) finish(() => reject(error));
		});
		socket.once("close", () => finish(() => resolve(Buffer.concat(chunks))));
	});
}

function responseError(response: Awaited<ReturnType<RpcClient["request"]>>): { code: number; message: string } {
	if (!response.error) throw new Error(`Expected JSON-RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

function profileContents(corpus: string, workspace: string, sessionId: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[main_session]
session_id = "${sessionId}"

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

function acquireNativeLease(core: WayCoreHandle, label: string, sessionId: string) {
	const identity = core.processIdentity(process.pid);
	return core.lockAcquire({
		label,
		ttlMs: 5_000,
		holder: {
			holderKind: "in_daemon",
			sessionId,
			pid: identity.pid,
			pidStartTime: identity.pidStartTime,
			pgid: identity.pgid,
			...(identity.pgidStartTime ? { pgidStartTime: identity.pgidStartTime } : {}),
			connId: "way.in_daemon_executor.v1",
		},
	});
}

test("real UDS closes malformed frames and leaves unknown cancellation harmless", async () => {
	const stateDirectory = temporaryDirectory("framing");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core));
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(socketPath);
		const oversized = await rawFrameUntilServerCloses(socketPath, Buffer.concat([Buffer.alloc(1_048_577, "x"), Buffer.from("\n")]));
		expect(JSON.parse(oversized.toString("utf8"))).toMatchObject({ error: { code: -32600, message: "payload_too_large" } });
		expect(await rawFrameUntilServerCloses(socketPath, Buffer.from([0xff, 0x0a]))).toHaveLength(0);
		expect(responseError(await client.request("unknown.method", {}))).toMatchObject({ code: -32601 });
		expect((await client.request("rpc.cancel", { id: "not-in-flight" })).result).toEqual({ cancelled: false });
	} finally {
		client?.close();
		await stopCore(core, stateDirectory);
	}
}, 10_000);

test("main.submit validates its UDS contract and durable idempotency through an external broker fixture", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	fs.mkdirSync(corpus, { recursive: true });
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(stateDirectory);
	const state = new GatewayStateStore(core);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
	});
	let host: ReturnType<typeof createMainSessionHost> | undefined;
	let client: RpcClient | undefined;
	try {
		await bootstrapMainSession({ confirm: true, profile, state, supervisor, sessionId: fixture.sessionId });
		const resumed = await strictResumeMainSession({ profile, state, supervisor });
		host = createMainSessionHost({
			supervisor,
			identity: resumed.identity,
			state,
			journal: durableTestJournal(state, { journalAppend: core.journalAppend.bind(core) }),
			initialTurnState: resumed.turnState,
			initialFollowUpQueueDepth: resumed.followUpQueueDepth,
			initialVerificationState: resumed.verificationState,
			...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
		});
		const submit = createMainAdmissionHandler(host, profile, core);
		core.startRpcServer(path.join(stateDirectory, "rpc.sock"), createRpcBridge(core, async (method, params) => {
			if (method === "main.submit") return await submit(params);
			throw new RpcBridgeException(-32601, `method not found: ${method}`);
		}));
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		expect(responseError(await client.request("main.submit", {
			text: "must reject unknown fields", surface_id: "guest", idempotency_key: "qa-extra-field", unexpected: true,
		}))).toMatchObject({ code: -32602, message: "unknown parameter: unexpected" });
		const first = await client.request("main.submit", {
			text: "qa non-owner request", surface_id: "guest", idempotency_key: "qa-main-submit-conflict",
		});
		expect(first.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });
		expect(fixture.commands()).toEqual([expect.objectContaining({ operation: "turn.follow_up" })]);
		expect(responseError(await client.request("main.submit", {
			text: "qa changed request", surface_id: "guest", idempotency_key: "qa-main-submit-conflict",
		}))).toMatchObject({ code: 1500, message: "idempotency_conflict" });
	} finally {
		client?.close();
		await host?.dispose();
		core.shutdownRpcServer();
		fixture.dispose();
	}
});

test("real RPC makes explicit release idempotent and refuses an expired renewal", async () => {
	const stateDirectory = temporaryDirectory("lock");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core));
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(socketPath);
		const releasedLease = acquireNativeLease(core, "qa-double-release", "qa-double-release");
		expect((await client.request("gitlock.release", { lease_id: releasedLease.leaseId, idempotency_key: "qa-release-first" })).result).toMatchObject({ released: true });
		expect((await client.request("gitlock.release", { lease_id: releasedLease.leaseId, idempotency_key: "qa-release-second" })).result).toMatchObject({ released: false });
		const expiringLease = acquireNativeLease(core, "qa-expired-renew", "qa-expired-renew");
		await Bun.sleep(5_100);
		expect(responseError(await client.request("gitlock.renew", { lease_id: expiringLease.leaseId, idempotency_key: "qa-expired-renew" }))).toMatchObject({ code: 1202, message: "lease_expired" });
	} finally {
		client?.close();
		await stopCore(core, stateDirectory);
	}
}, 10_000);
