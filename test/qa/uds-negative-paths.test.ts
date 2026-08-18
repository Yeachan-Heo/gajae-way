import * as fs from "node:fs";
import * as os from "node:os";
import { createConnection } from "node:net";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { FileSdkDouble } from "../helpers/main-session";

function temporaryDirectory(name: string): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-qa-${name}-`));
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
		const timeout = setTimeout(() => finish(() => reject(new Error("server did not close the malformed RPC connection"))), 5_000);
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			callback();
		};
		socket.on("data", chunk => chunks.push(Buffer.from(chunk)));
		socket.once("connect", () => {
			connected = true;
			socket.write(frame, () => undefined);
		});
		socket.once("error", error => {
			if (!connected) finish(() => reject(error));
		});
		socket.once("close", () => {
			if (!connected) {
				finish(() => reject(new Error("RPC connection closed before it was established")));
				return;
			}
			finish(() => resolve(Buffer.concat(chunks)));
		});
	});
}

function responseError(response: Awaited<ReturnType<RpcClient["request"]>>): { code: number; message: string } {
	if (!response.error) throw new Error(`Expected JSON-RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

function profileContents(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

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
		expect(JSON.parse(oversized.toString("utf8"))).toMatchObject({
			jsonrpc: "2.0",
			id: null,
			error: { code: -32600, message: "payload_too_large" },
		});

		const nonUtf8 = await rawFrameUntilServerCloses(socketPath, Buffer.from([0xff, 0x0a]));
		expect(nonUtf8).toHaveLength(0);

		const unknownMethod = await client.request("unknown.method", {});
		expect(responseError(unknownMethod)).toMatchObject({ code: -32601 });

		const unknownCancel = await client.request("rpc.cancel", { id: "not-in-flight" });
		expect(unknownCancel.result).toEqual({ cancelled: false });
		expect((await client.request("way.health", {})).result).toMatchObject({ status: "healthy", state: "running" });
	} finally {
		client?.close();
		await stopCore(core, stateDirectory);
	}
}, 10_000);

test("main.submit rejects unknown fields, conflicts durable idempotency, and keeps non-owner ingress off prompt and steer", async () => {
	const root = temporaryDirectory("admission");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDirectory = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(stateDirectory);
	const state = new GatewayStateStore(core);
	const sdk = new FileSdkDouble();
	let host: ReturnType<typeof createMainSessionHost> | undefined;
	let client: RpcClient | undefined;
	try {
		await bootstrapMainSession({ confirm: true, profile, state, sdk });
		const resumed = await strictResumeMainSession({ profile, state, sdk });
		host = createMainSessionHost({ session: resumed.session, identity: resumed.identity, state, journal: core });
		const submit = createMainAdmissionHandler(host, profile, core);
		core.startRpcServer(
			path.join(stateDirectory, "rpc.sock"),
			createRpcBridge(core, async (method, params) => {
				if (method === "main.submit") return await submit(params);
				throw new RpcBridgeException(-32601, `method not found: ${method}`);
			}),
		);
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));

		const unexpectedField = await client.request("main.submit", {
			text: "must reject unknown fields",
			surface_id: "guest",
			idempotency_key: "qa-extra-field",
			unexpected: true,
		});
		expect(responseError(unexpectedField)).toMatchObject({ code: -32602, message: "unknown parameter: unexpected" });

		const first = await client.request("main.submit", {
			text: "qa non-owner request",
			surface_id: "guest",
			idempotency_key: "qa-main-submit-conflict",
		});
		expect(first.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });
		expect(fs.readFileSync(resumed.identity.canonicalPath, "utf8")).not.toContain("qa non-owner request");

		const conflict = await client.request("main.submit", {
			text: "qa changed request",
			surface_id: "guest",
			idempotency_key: "qa-main-submit-conflict",
		});
		expect(responseError(conflict)).toMatchObject({ code: 1500, message: "idempotency_conflict" });
	} finally {
		client?.close();
		await host?.dispose();
		core.shutdownRpcServer();
		await Bun.sleep(40);
		fs.rmSync(root, { force: true, recursive: true });
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
		const firstRelease = await client.request("gitlock.release", {
			lease_id: releasedLease.leaseId,
			idempotency_key: "qa-release-first",
		});
		expect(firstRelease.result).toMatchObject({ released: true });
		const secondRelease = await client.request("gitlock.release", {
			lease_id: releasedLease.leaseId,
			idempotency_key: "qa-release-second",
		});
		expect(secondRelease.result).toMatchObject({ released: false });

		const expiringLease = acquireNativeLease(core, "qa-expired-renew", "qa-expired-renew");
		await Bun.sleep(5_100);
		const renewed = await client.request("gitlock.renew", {
			lease_id: expiringLease.leaseId,
			idempotency_key: "qa-expired-renew",
		});
		expect(responseError(renewed)).toMatchObject({ code: 1202, message: "lease_expired" });
	} finally {
		client?.close();
		await stopCore(core, stateDirectory);
	}
}, 10_000);
