import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { loadWayCore, type RpcBridgeCallback, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge } from "../../src/rpc-bridge";
import { RpcClient } from "../helpers/rpc-client";

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

function processGroupId(pid: number): number {
	const result = Bun.spawnSync({ cmd: ["ps", "-o", "pgid=", "-p", String(pid)], stdout: "pipe", stderr: "pipe" });
	if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr));
	const pgid = Number(new TextDecoder().decode(result.stdout).trim());
	if (!Number.isSafeInteger(pgid) || pgid <= 0) throw new Error("could not determine process group id");
	return pgid;
}

function processStartTime(pid: number): string {
	if (process.platform === "linux") {
		const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
		const fields = stat.slice(stat.lastIndexOf(")") + 1).trim().split(/\s+/);
		const startTime = fields[19];
		if (!startTime) throw new Error("could not read Linux process start time");
		return startTime;
	}
	if (process.platform === "darwin") {
		const script = String.raw`
import ctypes, os, sys
class ProcBsdInfo(ctypes.Structure):
    _fields_ = [("prefix", ctypes.c_uint32 * 12), ("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32), ("suffix", ctypes.c_uint32 * 6), ("seconds", ctypes.c_uint64), ("microseconds", ctypes.c_uint64)]
info = ProcBsdInfo()
lib = ctypes.CDLL("/usr/lib/libproc.dylib")
size = lib.proc_pidinfo(int(sys.argv[1]), 3, 0, ctypes.byref(info), ctypes.sizeof(info))
if size != ctypes.sizeof(info): raise SystemExit(1)
print(info.seconds * 1000000 + info.microseconds)
`;
		const result = Bun.spawnSync({ cmd: ["python3", "-c", script, String(pid)], stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error("could not read macOS process start time");
		return new TextDecoder().decode(result.stdout).trim();
	}
	throw new Error(`unsupported test platform: ${process.platform}`);
}

function holder(sessionId: string, pid = process.pid): Record<string, unknown> {
	return {
		holder_kind: "in_daemon",
		session_id: sessionId,
		pid,
		pid_start_time: processStartTime(pid),
		pgid: processGroupId(pid),
	};
}

function responseError(response: Awaited<ReturnType<RpcClient["request"]>>): { code: number; message: string; data?: unknown } {
	if (!response.error) throw new Error(`expected JSON-RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

test("native RPC server serves health/status and git-lock lifecycle over its real UDS", async () => {
	const stateDirectory = temporaryStateDirectory("serve");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const child = Bun.spawn(
		[
			"bun",
			"-e",
			`const { startWayServer } = await import("./src/main.ts"); const core = startWayServer(process.env.WAY_STATE_DIR); process.once("SIGTERM", () => { core.shutdownRpcServer(); process.exit(0); }); await new Promise(() => {});`,
		],
		{
			cwd: process.cwd(),
			env: { ...process.env, WAY_STATE_DIR: stateDirectory },
			stderr: "pipe",
			stdout: "ignore",
		},
	);
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(socketPath);
		const health = await client.request("way.health");
		expect(health.result).toMatchObject({ status: "healthy", state: "running" });
		const status = await client.request("way.status");
		expect(status.result).toMatchObject({ status: "healthy", lock: { held: false, queue_len: 0 } });

		const acquired = await client.request("gitlock.acquire", {
			label: "integration",
			holder: holder("integration-owner"),
			idempotency_key: "acquire-1",
		});
		const leaseId = (acquired.result as { lease_id: string }).lease_id;
		expect(leaseId).toBeString();
		const renewed = await client.request("gitlock.renew", { lease_id: leaseId, idempotency_key: "renew-1" });
		expect((renewed.result as { expires_at: number }).expires_at).toBeGreaterThan(0);
		const released = await client.request("gitlock.release", { lease_id: leaseId, idempotency_key: "release-1" });
		expect(released.result).toMatchObject({ released: true });

		const guarded = await client.request("gitlock.acquire", {
			label: "guarded",
			holder: holder("guarded-owner"),
			idempotency_key: "acquire-guarded",
		});
		const guardedLeaseId = (guarded.result as { lease_id: string }).lease_id;
		const forceRelease = await client.request("gitlock.force_release", {
			lease_id: guardedLeaseId,
			confirm: true,
			idempotency_key: "force-guarded",
		});
		expect(responseError(forceRelease)).toMatchObject({ code: 1207, message: "lock_holder_unverified" });
		const quarantined = await client.request("gitlock.quarantine_override", {
			lease_id: guardedLeaseId,
			confirm: true,
			acknowledge_unverified: true,
			idempotency_key: "quarantine-guarded",
		});
		expect(quarantined.result).toMatchObject({ quarantined: true });
		const cleared = await client.request("gitlock.clear_quarantine", {
			verification_receipt_id: "verified-receipt",
			confirm: true,
			idempotency_key: "clear-guarded",
		});
		expect(cleared.result).toMatchObject({ quarantined: false });
	} finally {
		client?.close();
		child.kill("SIGTERM");
		await child.exited;
		fs.rmSync(stateDirectory, { force: true, recursive: true });
	}
});

test("bridge maps JavaScript exceptions to correlated internal errors", async () => {
	const stateDirectory = temporaryStateDirectory("exception");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new Error("bridge boom");
	}));
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
