import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { parseWayConfig, type WayConfig } from "./config";
import { createMainAdmissionHandler } from "./main-session/admission";
import { createMainGateAnswerHandler } from "./main-session/gates";
import { createClosureExecutor, runClosureWorker, type ClosureExecutor } from "./main-session/closure";

import { bootstrapMainSession, recoverBootstrap } from "./main-session/bootstrap";
import { createMainSessionHost, type MainSessionHost } from "./main-session/host";
import { approveProfile, previewProfileApproval } from "./main-session/profile-approval";
import { ResumeError, strictResumeMainSession } from "./main-session/resume";
import { createPublishedSdk } from "./main-session/sdk";
import { GatewayStateError, GatewayStateStore } from "./main-session/state";
import { ProfileRevisionTracker } from "./profile";

import { loadWayCore, type WayCoreHandle } from "./native-loader";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "./rpc-bridge";

const usage = `Usage:
  way [serve] [--state-dir PATH] [--profile PATH] [--fail-closed-linger-ms MS]
  way bootstrap --confirm [--state-dir PATH] [--profile PATH]
  way profile approve --confirm [--state-dir PATH] [--profile PATH]
  way --health | --version`;

class FailedClosedExit extends Error {
	constructor() {
		super("failed_closed");
		this.name = "FailedClosedExit";
	}
}

export function healthPayload(): Record<string, unknown> {
	const healthInfo = loadWayCore().healthInfo();
	return {
		status: "healthy",
		state: "running",
		version: healthInfo.version,
		bootEpoch: healthInfo.bootEpoch,
	};
}

export function defaultStateDirectory(): string {
	return process.env.WAY_STATE_DIR || path.join(os.homedir(), ".local", "state", "gajae-way");
}

/** Starts only the native RPC server; tests and later phases can host their own bridge. */
export function startWayServer(stateDirectory = defaultStateDirectory(), bridgeHandler?: RpcBridgeHandler): WayCoreHandle {
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(path.join(stateDirectory, "rpc.sock"), createRpcBridge(core, bridgeHandler));
	return core;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function failureReason(error: unknown): string {
	if (error instanceof ResumeError || error instanceof GatewayStateError) return error.reason;
	if (error instanceof Error && error.name === "ProfileValidationError") return "profile_invalid";
	return "startup_failed";
}

function healthFilePath(stateDirectory: string): string {
	return path.join(stateDirectory, "health.json");
}

async function writeHealthFile(stateDirectory: string, payload: Record<string, unknown>): Promise<void> {
	await fsp.mkdir(stateDirectory, { recursive: true, mode: 0o700 });
	const destination = healthFilePath(stateDirectory);
	const temporary = `${destination}.tmp.${process.pid}`;
	await fsp.writeFile(temporary, `${JSON.stringify(payload)}\n`, { encoding: "utf8", mode: 0o600 });
	await fsp.rename(temporary, destination);
}

async function enterFailedClosed(
	core: WayCoreHandle,
	state: GatewayStateStore,
	config: WayConfig,
	reason: string,
	persist = true,
): Promise<never> {
	if (persist) {
		try {
			const durable = state.read();
			if (durable.bootstrapState !== "FAILED_CLOSED" || durable.failedClosedReason !== reason) state.markFailedClosed(reason);
		} catch {
			// Health observability and exit 78 remain mandatory even when a secondary
			// metadata write is unavailable.
		}
	}
	const payload = { status: "unhealthy", state: "failed_closed", reason };
	try {
		await writeHealthFile(config.stateDir, payload);
	} catch (error) {
		console.error(`Could not write failed-closed health file: ${error instanceof Error ? error.message : String(error)}`);
	}
	try {
		core.setRpcHealth("failed_closed", reason);
	} catch {
		// The process can still terminate safely if the listener was already lost.
	}
	try {
		core.sdNotifyStatus(`gajae-way failed_closed: ${reason}`);
	} catch {
		// NOTIFY_SOCKET is optional and status notification is best effort.
	}
	await Bun.sleep(config.failClosedLingerMs);
	try {
		core.shutdownRpcServer();
	} catch {
		// Shutdown is idempotent from the process' point of view.
	}
	throw new FailedClosedExit();
}

function profileBridgeHandler(
	state: GatewayStateStore,
	config: WayConfig,
	profiles: ProfileRevisionTracker,
): RpcBridgeHandler {

	return async (method, params) => {
		if (method !== "profile.approve") throw new RpcBridgeException(-32601, `method not found: ${method}`);
		if (!isRecord(params) || params.confirm !== true || Object.keys(params).some(key => key !== "confirm")) {
			throw new RpcBridgeException(-32602, "profile.approve requires { confirm: true }.");
		}
		try {
			const profile = profiles.load(config.profilePath);
			const result = approveProfile(state, profile, true);
			return {
				receipt_id: result.receiptId,
				approved_at: result.approvedAt,
				cursor: result.cursor,
				previous_digest: result.previousDigest,
				next_digest: result.nextDigest,
				changes: result.changes,
			};
		} catch (error) {
			throw new RpcBridgeException(
				error instanceof Error && error.name === "ProfileApprovalError" ? -32602 : -32603,
				error instanceof Error ? error.message : String(error),
			);
		}
	};
}

async function waitForShutdown(core: WayCoreHandle, host: MainSessionHost, closures: ClosureExecutor): Promise<void> {
	await new Promise<void>(resolve => {
		const stop = () => resolve();
		process.once("SIGINT", stop);
		process.once("SIGTERM", stop);
	});
	await closures.shutdown();
	await host.dispose();
	core.shutdownRpcServer();
}

async function serveWay(config: WayConfig): Promise<void> {
	const core = loadWayCore().WayCore.open(config.stateDir);
	const closures = createClosureExecutor({ core });
	const state = new GatewayStateStore(core);
	const profiles = new ProfileRevisionTracker();
	const profileHandler = profileBridgeHandler(state, config, profiles);
	let mainSessionHandler: RpcBridgeHandler | undefined;
	const bridgeHandler: RpcBridgeHandler = async (method, params) => {
		if (method === "profile.approve") return await profileHandler(method, params);
		if (!mainSessionHandler) throw new RpcBridgeException(1301, "unknown_session");
		return await mainSessionHandler(method, params);
	};
	core.startRpcServer(path.join(config.stateDir, "rpc.sock"), createRpcBridge(core, bridgeHandler));
	core.setRpcHealth("verifying");
	let host: MainSessionHost | undefined;
	try {
		const profile = profiles.load(config.profilePath);
		const recovery = await recoverBootstrap({ profile, state, sdk: createPublishedSdk() });
		if (recovery.kind === "bootstrap_required") {
			// The durable state remains ABSENT so an explicit `way bootstrap --confirm`
			// can proceed after this unhealthy daemon exits.
			await enterFailedClosed(core, state, config, "bootstrap_required", false);
		}
		if (recovery.kind === "failed_closed") await enterFailedClosed(core, state, config, recovery.reason, false);
		const resumed = await strictResumeMainSession({
			profile,
			state,
			sdk: createPublishedSdk(),
			onInjectionLog: entry => console.warn(`way injection ${entry.kind}: ${entry.path}`),
		});
		host = createMainSessionHost({
			session: resumed.session,
			identity: resumed.identity,
			state,
			journal: core,
		});
		const admissionHandler = createMainAdmissionHandler(host, profile, core);
		const gateAnswerHandler = createMainGateAnswerHandler(host, core);
		mainSessionHandler = async (method, params) => {
			if (method === "main.submit") return await admissionHandler(params);
			if (method === "main.gate.answer") return await gateAnswerHandler(params);
			throw new RpcBridgeException(-32601, `method not found: ${method}`);
		};
		core.setRpcHealth("running");
		await writeHealthFile(config.stateDir, { status: "healthy", state: "running" });
		await waitForShutdown(core, host, closures);
	} catch (error) {
		if (error instanceof FailedClosedExit) {
			await closures.shutdown();
			throw error;
		}
		if (host) await host.dispose();
		await closures.shutdown();
		await enterFailedClosed(core, state, config, failureReason(error));
	}
}

function requireConfirm(arguments_: readonly string[], command: string): void {
	if (arguments_.length !== 2 || arguments_[0] !== command || arguments_[1] !== "--confirm") {
		throw new Error(`${command} requires --confirm.\n${usage}`);
	}
}

async function bootstrapCommand(config: WayConfig, arguments_: readonly string[]): Promise<void> {
	requireConfirm(arguments_, "bootstrap");
	const core = loadWayCore().WayCore.open(config.stateDir);
	const state = new GatewayStateStore(core);
	const profiles = new ProfileRevisionTracker();
	const profile = profiles.load(config.profilePath);
	const sdk = createPublishedSdk();
	const recovery = await recoverBootstrap({ profile, state, sdk });
	if (recovery.kind === "failed_closed") throw new Error(`Bootstrap recovery is failed closed: ${recovery.reason}`);
	if (recovery.kind === "committed") {
		console.log(JSON.stringify({ state: "committed", session_id: recovery.identity.sessionId, recovered: recovery.nonce !== "" }));
		return;
	}
	const committed = await bootstrapMainSession({ confirm: true, profile, state, sdk });
	console.log(JSON.stringify({ state: "committed", session_id: committed.identity.sessionId, nonce: committed.nonce }));
}

interface RpcResponse {
	readonly result?: unknown;
	readonly error?: { readonly code: number; readonly message: string; readonly data?: unknown };
}

async function requestOwnerRpc(socketPath: string, method: string, params: unknown): Promise<unknown> {
	return await new Promise<unknown>((resolve, reject) => {
		const socket = net.createConnection(socketPath);
		let buffer = "";
		let settled = false;
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			socket.destroy();
			callback();
		};
		const timeout = setTimeout(() => finish(() => reject(new Error("Timed out waiting for owner RPC."))), 5_000);
		socket.setEncoding("utf8");
		socket.once("error", error => finish(() => reject(error)));
		socket.on("data", chunk => {
			buffer += String(chunk);
			const newline = buffer.indexOf("\n");
			if (newline < 0) return;
			try {
				const response = JSON.parse(buffer.slice(0, newline)) as RpcResponse;
				if (response.error) throw new Error(`${response.error.code} ${response.error.message}`);
				finish(() => resolve(response.result));
			} catch (error) {
				finish(() => reject(error));
			}
		});
		socket.once("connect", () => {
			socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: "profile-approve", method, params })}\n`);
		});
	});
}

async function profileApproveCommand(config: WayConfig, arguments_: readonly string[]): Promise<void> {
	if (arguments_.length !== 3 || arguments_[0] !== "profile" || arguments_[1] !== "approve" || arguments_[2] !== "--confirm") {
		throw new Error(`profile approve requires --confirm.\n${usage}`);
	}
	const socketPath = path.join(config.stateDir, "rpc.sock");
	if (fs.existsSync(socketPath)) {
		const result = await requestOwnerRpc(socketPath, "profile.approve", { confirm: true });
		console.log(JSON.stringify(result, null, 2));
		return;
	}
	const core = loadWayCore().WayCore.open(config.stateDir);
	const state = new GatewayStateStore(core);
	const profile = new ProfileRevisionTracker().load(config.profilePath);
	const preview = previewProfileApproval(state, profile);
	console.log(JSON.stringify({ previous_digest: preview.previousDigest, next_digest: preview.nextDigest, changes: preview.changes }, null, 2));
	const result = approveProfile(state, profile, true);
	console.log(JSON.stringify({ receipt_id: result.receiptId, approved_at: result.approvedAt, cursor: result.cursor }, null, 2));
}

/** CLI entrypoint. The daemon never calls bootstrap; it only strict-resumes a committed identity. */
export async function runWay(arguments_ = process.argv.slice(2)): Promise<void> {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return;
	}
	if (arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`way ${loadWayCore().healthInfo().version}`);
		return;
	}
	if (arguments_.includes("--health")) {
		console.log(JSON.stringify(healthPayload()));
		return;
	}
	const parsed = parseWayConfig(arguments_);
	if (parsed.remaining.length === 0 || (parsed.remaining.length === 1 && parsed.remaining[0] === "serve")) {
		await serveWay(parsed.config);
		return;
	}
	if (parsed.remaining[0] === "bootstrap") {
		await bootstrapCommand(parsed.config, parsed.remaining);
		return;
	}
	if (parsed.remaining[0] === "profile") {
		await profileApproveCommand(parsed.config, parsed.remaining);
		return;
	}
	throw new Error(`Unknown command: ${parsed.remaining.join(" ")}\n${usage}`);
}

if (import.meta.main && process.env.WAY_INTERNAL_CLOSURE_WORKER === "1") {
	await runClosureWorker();
} else if (import.meta.main) {
	try {
		await runWay();
	} catch (error) {
		if (error instanceof FailedClosedExit) {
			process.exitCode = 78;
		} else {
			console.error(error instanceof Error ? error.message : String(error));
			process.exitCode = 1;
		}
	}
}