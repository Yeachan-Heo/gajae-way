import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadWayCore, type RpcBridgeCallback, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge } from "../../src/rpc-bridge";
import { ManagedProcessRegistry } from "../helpers/managed-process";
import { RpcClient } from "../helpers/rpc-client";

const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});

function temporaryStateDirectory(name: string): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-rpc-${name}-`));
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) return RpcClient.connect(socketPath);
		await Bun.sleep(20);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

async function stopCore(core: WayCoreHandle, stateDirectory: string): Promise<void> {
	core.shutdownRpcServer();
	await Bun.sleep(40);
	fs.rmSync(stateDirectory, { force: true, recursive: true });
}

function responseError(response: Awaited<ReturnType<RpcClient["request"]>>): {
	code: number;
	message: string;
	data?: unknown;
} {
	if (!response.error) throw new Error(`expected JSON-RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

test("native RPC server serves health/status and rejects all caller-controlled git-lock holder identities", async () => {
	const stateDirectory = temporaryStateDirectory("serve");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const child = managedProcesses.spawnDaemon({
		cmd: [
			"bun",
			"-e",
			`const { startWayServer } = await import("./src/main.ts"); const core = startWayServer(process.env.GAJAEWAY_STATE_DIR); process.once("SIGTERM", () => { core.shutdownRpcServer(); process.exit(0); }); await new Promise(() => {});`,
		],
		cwd: process.cwd(),
		env: { ...process.env, GAJAEWAY_STATE_DIR: stateDirectory },
		stderr: "pipe",
	});
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(socketPath);
		const health = await client.request("way.health");
		expect(health.result).toMatchObject({ status: "healthy", state: "running" });
		const status = await client.request("way.status");
		expect(status.result).toMatchObject({ status: "healthy", lock: { held: false, queue_len: 0 } });

		for (const holderKind of ["in_daemon", "external"]) {
			const rejected = await client.request("gitlock.acquire", {
				label: "integration",
				holder: {
					holder_kind: holderKind,
					session_id: "caller-controlled",
					pid: process.pid,
					pid_start_time: "0",
					pgid: process.pid,
					conn_id: "way.in_daemon_executor.v1",
				},
				idempotency_key: `acquire-${holderKind}`,
			});
			expect(responseError(rejected)).toMatchObject({
				code: -32602,
				message: "gitlock.acquire is reserved for the daemon's supervised in-daemon closure executor in v1",
			});
		}
	} finally {
		client?.close();
		await managedProcesses.stopDaemon(child);
		fs.rmSync(stateDirectory, { force: true, recursive: true });
	}
});

test("bridge maps JavaScript exceptions to correlated internal errors", async () => {
	const stateDirectory = temporaryStateDirectory("exception");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(
		socketPath,
		createRpcBridge(core, () => {
			throw new Error("bridge boom");
		}),
	);
	const client = await connectEventually(socketPath);
	try {
		const response = await client.request("main.throw", {});
		expect(responseError(response)).toMatchObject({ code: -32603, message: "bridge_exception" });
		expect((response.error?.data as { correlation_id?: string }).correlation_id).toMatch(/^rpc-/);
	} finally {
		client.close();
		await stopCore(core, stateDirectory);
	}
});

test("bridge completes at most once", async () => {
	const stateDirectory = temporaryStateDirectory("double");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	const callback: RpcBridgeCallback = (_error, request) => {
		core.bridgeComplete(request.reqId, JSON.stringify({ winner: 1 }));
		core.bridgeComplete(request.reqId, JSON.stringify({ winner: 2 }));
	};
	core.startRpcServer(socketPath, callback);
	const client = await connectEventually(socketPath);
	try {
		const response = await client.request("main.double", {});
		expect(response.result).toEqual({ winner: 1 });
		expect(core.rpcBridgeStats()).toMatchObject({ duplicateCompletions: 1, inFlight: 0 });
	} finally {
		client.close();
		await stopCore(core, stateDirectory);
	}
});

test("the sixty-fifth bridge request fails closed as overloaded", async () => {
	const stateDirectory = temporaryStateDirectory("overload");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, (() => undefined) as RpcBridgeCallback);
	const clients: RpcClient[] = [];
	try {
		for (let clientIndex = 0; clientIndex < 4; clientIndex += 1) {
			const client = await connectEventually(socketPath);
			clients.push(client);
			for (let requestIndex = 0; requestIndex < 16; requestIndex += 1) {
				void client.request("main.wait", {}).catch(() => undefined);
			}
		}
		await Bun.sleep(50);
		const sixtyFifthClient = await connectEventually(socketPath);
		clients.push(sixtyFifthClient);
		const response = await sixtyFifthClient.request("main.wait", {});
		expect(responseError(response)).toMatchObject({ code: -32603, message: "bridge_overloaded" });
		expect(core.rpcBridgeStats()).toMatchObject({ inFlight: 64, overloads: 1 });
	} finally {
		for (const client of clients) client.close();
		await stopCore(core, stateDirectory);
	}
});

test("shutdown returns a correlated shutting_down error for pending bridge calls", async () => {
	const stateDirectory = temporaryStateDirectory("shutdown");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, (() => undefined) as RpcBridgeCallback);
	const client = await connectEventually(socketPath);
	try {
		const pending = client.request("main.wait", {});
		for (let attempt = 0; core.rpcBridgeStats().inFlight === 0 && attempt < 20; attempt += 1) await Bun.sleep(10);
		core.shutdownRpcServer();
		const response = await pending;
		expect(responseError(response)).toMatchObject({ code: -32603, message: "shutting_down" });
		expect((response.error?.data as { correlation_id?: string }).correlation_id).toMatch(/^rpc-/);
	} finally {
		client.close();
		await Bun.sleep(40);
		fs.rmSync(stateDirectory, { force: true, recursive: true });
	}
});
