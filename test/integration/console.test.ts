import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import {
	ConsoleDeliveryUnavailableError,
	ConsoleOutput,
	OwnerConsole,
	runWayConsole,
	type ConsoleTerminal,
} from "../../src/console/console";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient, rpcResult, type JsonRpcClient, type RpcRequestOptions, type JsonRpcResponse } from "../../src/rpc-client";
import { FileSdkDouble } from "../helpers/main-session";

const repositoryRoot = path.resolve(import.meta.dir, "../..");
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
	readonly stateDirectory: string;
	readonly profilePath: string;
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
		stateDirectory,
		profilePath,
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

function recordedOutput(writer?: (text: string) => void | Promise<void>): { output: ConsoleOutput; writes: string[] } {
	const writes: string[] = [];
	return {
		writes,
		output: new ConsoleOutput(async text => {
			await writer?.(text);
			writes.push(text);
		}),
	};
}

function scriptedTerminal(lines: readonly string[]): { terminal: ConsoleTerminal; writes: string[] } {
	const pending = [...lines];
	const writes: string[] = [];
	return {
		writes,
		terminal: {
			async writeTrusted(text: string): Promise<void> {
				writes.push(text);
			},
			async readLine(): Promise<string | undefined> {
				return pending.shift();
			},
			close(): void {},
		},
	};
}

function retentionGapClient(delegate: RpcClient): JsonRpcClient {
	return {
		async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
			if (method === "main.events.read") {
				return {
					jsonrpc: "2.0",
					id: 9,
					result: {
						events: [],
						next_cursor: "1:0",
						gap: { missing_from: "1:0", missing_to: "1:9", resync_cursor: "1:10" },
					},
				};
			}
			return await delegate.request(method, params, options);
		},
		close(): void {
			delegate.close();
		},
	};
}

function deferred(): { promise: Promise<void>; resolve(): void } {
	let resolve: (() => void) | undefined;
	const promise = new Promise<void>(resolvePromise => {
		resolve = resolvePromise;
	});
	return {
		promise,
		resolve(): void {
			resolve?.();
		},
	};
}

test("console submits through real UDS, renders finalized replies before settlement, and resumes its server checkpoint after restart", async () => {
	const gateway = await hostedConsoleGateway();
	let cursorAtAssistantRender: string | undefined;
	const recorded = recordingClient(gateway.client);
	const rendered = recordedOutput(text => {
		if (text === "Assistant:\n") cursorAtAssistantRender = gateway.core.consumerCursor("way-console");
	});
	let idempotencyCount = 0;
	const consoleSurface = new OwnerConsole({
		rpc: recorded.rpc,
		ownerSurfaceId: "owner",
		output: rendered.output,
		readWaitMs: 0,
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
		expect(rendered.writes.join("")).toContain("Delivered as: prompt");
		const consumerClaim = recorded.calls.find(call => call.method === "consumer.claim");
		expect(consumerClaim?.params).toEqual({ consumer_id: "way-console", claim_ttl_ms: 5_000 });
		const eventRead = recorded.calls.find(call => call.method === "main.events.read");
		expect(eventRead?.params).toEqual({
			consumer_id: "way-console",
			limit: 100,
			wait_ms: 0,
			kinds: ["assistant_message", "turn_start", "turn_end", "gate_open", "gate_resolved", "health_change", "lock_event"],
		});
		expect(rendered.writes.join("")).toContain("Main turn started — busy.");
		expect(rendered.writes.join("")).toContain("Assistant:\nack\n");
		expect(rendered.writes.join("")).toContain("Main turn ended — idle.");
		expect(cursorAtAssistantRender).toBe("1:0");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:3");
		expect(gateway.core.consumerOutbox("way-console")).toHaveLength(3);
		gateway.core.journalAppend("health_change", JSON.stringify({ state: "degraded", reason: "journal_append_failed" }));
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(rendered.writes.join("")).toContain("Gateway health changed: degraded reason=journal_append_failed.");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:4");
		expect(gateway.core.consumerOutbox("way-console")).toHaveLength(4);

		gateway.client.close();
		const restartedClient = await connectEventually(gateway.socketPath);
		const restartedOutput = recordedOutput();
		const restarted = new OwnerConsole({
			rpc: restartedClient,
			ownerSurfaceId: "owner",
			output: restartedOutput.output,
			readWaitMs: 0,
		});
		try {
			expect((await restarted.start()).accepted).toBe(true);
			expect(await restarted.consumeOnce()).toBe("idle");
			expect(restartedOutput.writes.join("")).not.toContain("Assistant:\nack\n");
			expect(gateway.core.consumerCursor("way-console")).toBe("1:4");
		} finally {
			restartedClient.close();
		}
	} finally {
		await gateway.stop();
	}
});

test("console sanitizes assistant CSI, OSC 52, C0, and C1 text before terminal publication", async () => {
	const gateway = await hostedConsoleGateway();
	const rendered = recordedOutput();
	const consoleSurface = new OwnerConsole({ rpc: gateway.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
	const hostile = "readable \x1b[2J CSI \x1b]52;c;SGVsbG8=\u0007 OSC52 \u0000\b\t\n\r\u009b1A C1";
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: hostile }));
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		const assistantText = rendered.writes.find(text => text.includes("readable"));
		expect(assistantText).toContain("\\x1B[2J");
		expect(assistantText).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
		expect(assistantText).toContain("\\u0000\\u0008\\t\\n\\r\\u009B1A");
		expect(assistantText).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
		const errorOutput = recordedOutput();
		const hostileErrorRpc: JsonRpcClient = {
			async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
				if (method === "main.submit") {
					return { jsonrpc: "2.0", id: 12, error: { code: -32603, message: hostile } };
				}
				return await gateway.client.request(method, params, options);
			},
			close(): void {},
		};
		const errorConsole = new OwnerConsole({ rpc: hostileErrorRpc, ownerSurfaceId: "owner", output: errorOutput.output, readWaitMs: 0 });
		expect((await errorConsole.start()).accepted).toBe(true);
		expect(await errorConsole.handleInput("request that returns a hostile RPC error")).toBe(true);
		const errorText = errorOutput.writes.find(text => text.includes("RPC main.submit failed"));
		expect(errorText).toContain("\\x1B[2J");
		expect(errorText).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
		expect(errorText).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
	} finally {
		await gateway.stop();
	}
});

test("console readiness refuses an existing way-console claim before it can submit owner work", async () => {
	const gateway = await hostedConsoleGateway();
	const holder = await connectEventually(gateway.socketPath);
	const contender = await connectEventually(gateway.socketPath);
	const rendered = recordedOutput();
	try {
		const claim = rpcResult<{ claim_id: string; cursor: string }>(
			await holder.request("consumer.claim", { consumer_id: "way-console", claim_ttl_ms: 5_000 }),
			"consumer.claim",
		);
		const consoleSurface = new OwnerConsole({ rpc: contender, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
		const startup = await consoleSurface.start();
		expect(startup).toMatchObject({ accepted: false });
		expect(rendered.writes.join("")).toContain("Another way console currently owns");
		await expect(consoleSurface.submit("must not submit while delivery is elsewhere")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		await holder.request("consumer.commit", {
			consumer_id: "way-console",
			claim_id: claim.claim_id,
			cursor: claim.cursor,
			proofs: [],
		});
	} finally {
		holder.close();
		contender.close();
		await gateway.stop();
	}
});

test("console readiness terminates on a retention gap instead of accepting blind submissions", async () => {
	const gateway = await hostedConsoleGateway();
	const client = await connectEventually(gateway.socketPath);
	const rendered = recordedOutput();
	try {
		const consoleSurface = new OwnerConsole({
			rpc: retentionGapClient(client),
			ownerSurfaceId: "owner",
			output: rendered.output,
			readWaitMs: 0,
		});
		const startup = await consoleSurface.start();
		expect(startup).toMatchObject({ accepted: false });
		expect(rendered.writes.join("")).toContain("behind journal retention");
		expect(rendered.writes.join("")).toContain("restart way console");
		await expect(consoleSurface.submit("must not submit without a recoverable checkpoint")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(gateway.core.consumerCursor("way-console")).toBe("1:0");
	} finally {
		client.close();
		await gateway.stop();
	}
});

test("console commits only after an asynchronous terminal publication resolves", async () => {
	const gateway = await hostedConsoleGateway();
	const publication = deferred();
	let delayAssistant = false;
	const rendered = recordedOutput(async text => {
		if (delayAssistant && text === "delayed assistant") await publication.promise;
	});
	const consoleSurface = new OwnerConsole({ rpc: gateway.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		const appended = gateway.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "delayed assistant" }));
		delayAssistant = true;
		const consume = consoleSurface.consumeOnce();
		await Bun.sleep(25);
		expect(gateway.core.consumerCursor("way-console")).toBe("1:0");
		publication.resolve();
		expect(await consume).toBe("rendered");
		expect(gateway.core.consumerCursor("way-console")).toBe(appended.cursor);
	} finally {
		await gateway.stop();
	}
});

test("console leaves its checkpoint unadvanced and blocks input when terminal publication fails", async () => {
	const gateway = await hostedConsoleGateway();
	let failAssistant = false;
	const rendered = recordedOutput(text => {
		if (failAssistant && text === "terminal write rejected") throw new Error("simulated terminal writer failure");
	});
	const consoleSurface = new OwnerConsole({ rpc: gateway.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "terminal write rejected" }));
		failAssistant = true;
		await expect(consoleSurface.consumeOnce()).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(gateway.core.consumerCursor("way-console")).toBe("1:0");
		await expect(consoleSurface.submit("must not submit after a publication failure")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
	} finally {
		await gateway.stop();
	}
});

test("actual way console CLI exits non-zero after a failed-closed startup refusal", async () => {
	const root = temporaryDirectory("cli-refusal");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDirectory = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, ownerProfile(corpus, workspace));
	const core = loadWayCore().WayCore.open(stateDirectory);
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, () => {
		throw new RpcBridgeException(-32601, "method not found");
	}));
	core.setRpcHealth("failed_closed", "profile_drift");
	try {
		await connectEventually(socketPath).then(client => client.close());
		const child = Bun.spawn({
			cmd: ["bun", "src/main.ts", "console", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: repositoryRoot,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [exitCode, childStdout, childStderr] = await Promise.all([
			child.exited,
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
		]);
		expect(exitCode).toBe(1);
		expect(childStdout).toContain("Refusing interactive console");
		expect(childStdout).toContain("failed closed");
		expect(childStdout).toContain("profile_drift");
		expect(childStderr).toBe("");
	} finally {
		core.shutdownRpcServer();
		await Bun.sleep(40);
	}
});


test("healthy non-TTY CLI preserves the console checkpoint until a valid terminal can render pending events", async () => {
	const gateway = await hostedConsoleGateway();
	const checkpointBefore = gateway.core.consumerCursor("way-console");
	const pending = gateway.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "pending before terminal validation" }));
	try {
		const child = Bun.spawn({
			cmd: ["bun", "src/main.ts", "console", "--state-dir", gateway.stateDirectory, "--profile", gateway.profilePath],
			cwd: repositoryRoot,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [exitCode, childStdout, childStderr] = await Promise.all([
			child.exited,
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
		]);
		expect(exitCode).toBe(1);
		expect(childStderr).toContain("way console requires an interactive TTY");
		expect(childStdout).toBe("");
		expect(gateway.core.consumerCursor("way-console")).toBe(checkpointBefore);
		expect(gateway.core.consumerOutbox("way-console")).toEqual([]);

		const subsequent = scriptedTerminal(["/quit"]);
		await runWayConsole(
			{ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath },
			[],
			{ terminal: subsequent.terminal },
		);
		expect(subsequent.writes.join("")).toContain("Assistant:\npending before terminal validation\n");
		expect(gateway.core.consumerCursor("way-console")).toBe(pending.cursor);
	} finally {
		await gateway.stop();
	}
});
test("console gate drill uses durable gate fencing, rejects a mismatched session, and renders resolution", async () => {
	const gateway = await hostedConsoleGateway();
	const rendered = recordedOutput();
	const consoleSurface = new OwnerConsole({ rpc: gateway.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.sdk.openGate(gateway.sessionFile, "gate-live");
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(rendered.writes.join("")).toContain("Gate opened: gate_id=gate-live expected_session_id=");

		await expect(consoleSurface.answerGate("gate-live", "wrong-session", { selected: ["Yes"] })).rejects.toMatchObject({ code: 1102 });
		expect(await consoleSurface.answerGate("gate-live", gateway.host.sessionId, { selected: ["Yes"] })).toEqual({
			accepted: true,
			gateState: "resolved",
		});
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(rendered.writes.join("")).toContain("Gate resolved: gate_id=gate-live.");
		expect(gateway.core.consumerCursor("way-console")).toBe("1:2");
	} finally {
		await gateway.stop();
	}
});
