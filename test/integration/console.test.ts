import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import {
	ConsoleDeliveryUnavailableError,
	ConsoleOutput,
	MAX_RAW_CONSOLE_LINE_BYTES,
	MAX_RAW_CONSOLE_QUEUED_BYTES,
	MAX_RAW_CONSOLE_QUEUED_LINES,
	OwnerConsole,
	RawConsoleTerminal,
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
import {
	RpcClient,
	rpcResult,
	type JsonRpcClient,
	type RpcRequestOptions,
	type JsonRpcResponse,
} from "../../src/rpc-client";
import { FileSdkDouble } from "../helpers/main-session";
import { ManagedProcessRegistry } from "../helpers/managed-process";

const repositoryRoot = path.resolve(import.meta.dir, "../..");
const temporaryDirectories: string[] = [];
const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
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

async function hostedConsoleGateway(options: { readonly sdk?: FileSdkDouble } = {}): Promise<HostedConsoleGateway> {
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
	const sdk = options.sdk ?? new FileSdkDouble();
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
		output: new ConsoleOutput(async (text) => {
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

interface ControlledTerminal {
	readonly terminal: ConsoleTerminal;
	readonly writes: string[];
	readonly lifecycle: string[];
	send(line: string): void;
	isReading(): boolean;
	isClosed(): boolean;
}

interface ControlledTerminalOptions {
	onWrite?(text: string): void | Promise<void>;
}

function controlledTerminal(options: ControlledTerminalOptions = {}): ControlledTerminal {
	const pending: string[] = [];
	const writes: string[] = [];
	const lifecycle: string[] = [];
	let resolveLine: ((line: string | undefined) => void) | undefined;
	let closed = false;
	const send = (line: string): void => {
		if (closed) throw new Error("terminal is closed");
		const resolve = resolveLine;
		if (!resolve) {
			pending.push(line);
			return;
		}
		resolveLine = undefined;
		resolve(line);
	};
	return {
		writes,
		lifecycle,
		send,
		isReading: () => resolveLine !== undefined,
		isClosed: () => closed,
		terminal: {
			async writeTrusted(text: string): Promise<void> {
				if (closed) throw new Error("terminal is closed");
				await options.onWrite?.(text);
				writes.push(text);
				lifecycle.push(`write:${text}`);
			},
			async readLine(): Promise<string | undefined> {
				if (closed) return undefined;
				const queued = pending.shift();
				if (queued !== undefined) return queued;
				return await new Promise((resolve) => {
					resolveLine = resolve;
				});
			},
			close(): void {
				if (closed) return;
				closed = true;
				lifecycle.push("close");
				const resolve = resolveLine;
				resolveLine = undefined;
				resolve?.(undefined);
			},
		},
	};
}

class PausableRawInputHarness {
	readonly isTTY = true;
	isRaw = false;
	isPaused = false;
	pauseCalls = 0;
	resumeCalls = 0;
	readonly #listeners = new Set<(chunk: string | Buffer) => void>();
	readonly #blockedChunks: string[] = [];

	get blockedChunkCount(): number {
		return this.#blockedChunks.length;
	}

	setEncoding(_encoding: BufferEncoding): void {}

	setRawMode(enabled: boolean): void {
		this.isRaw = enabled;
	}

	resume(): void {
		this.resumeCalls += 1;
		if (!this.isPaused) return;
		this.isPaused = false;
		while (!this.isPaused && this.#blockedChunks.length > 0) {
			const chunk = this.#blockedChunks.shift() as string;
			this.deliver(chunk);
		}
	}

	pause(): void {
		this.pauseCalls += 1;
		this.isPaused = true;
	}

	on(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.add(listener);
	}

	off(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.delete(listener);
	}

	send(chunk: string): void {
		if (this.isPaused) {
			this.#blockedChunks.push(chunk);
			return;
		}
		this.deliver(chunk);
	}

	private deliver(chunk: string): void {
		for (const listener of [...this.#listeners]) listener(chunk);
	}
}

class RawOutputHarness {
	readonly isTTY = true;
	readonly writes: string[] = [];

	write(text: string, callback: (error?: Error | null) => void): boolean {
		this.writes.push(text);
		callback();
		return true;
	}

	once(_event: "drain", _listener: () => void): void {}
}

class PermanentlyStalledRawOutputHarness {
	readonly isTTY = true;
	readonly writes: string[] = [];
	#stalled = false;
	readonly #pending: Array<{ callback: (error?: Error | null) => void; drain?: () => void }> = [];

	get pendingWriteCount(): number {
		return this.#pending.length;
	}

	stall(): void {
		this.#stalled = true;
	}

	write(text: string, callback: (error?: Error | null) => void): boolean {
		this.writes.push(text);
		if (!this.#stalled) {
			callback();
			return true;
		}
		this.#pending.push({ callback });
		return false;
	}

	once(_event: "drain", listener: () => void): void {
		const pending = this.#pending.at(-1);
		if (!pending) throw new Error("drain listener was registered without a pending write");
		pending.drain = listener;
	}
}

interface PromptFrameTerminal extends ControlledTerminal {
	snapshot(): string;
}

/** Models RawConsoleTerminal's clear-line/repaint behavior for frame assertions. */
function promptFrameTerminal(): PromptFrameTerminal {
	const pending: string[] = [];
	const writes: string[] = [];
	const lifecycle: string[] = [];
	const completedLines: string[] = [];
	let currentLine = "";
	let prompt = "";
	let buffer = "";
	let resolveLine: ((line: string | undefined) => void) | undefined;
	let closed = false;
	const append = (text: string): void => {
		const segments = text.split("\n");
		for (let index = 0; index < segments.length; index += 1) {
			currentLine += segments[index] as string;
			if (index < segments.length - 1) {
				completedLines.push(currentLine);
				currentLine = "";
			}
		}
	};
	const completeInput = (line: string): void => {
		currentLine += line;
		completedLines.push(currentLine);
		currentLine = "";
		prompt = "";
		buffer = "";
		const resolve = resolveLine;
		resolveLine = undefined;
		resolve?.(line);
	};
	const send = (line: string): void => {
		if (closed) throw new Error("terminal is closed");
		if (!resolveLine) {
			pending.push(line);
			return;
		}
		completeInput(line);
	};
	return {
		writes,
		lifecycle,
		send,
		isReading: () => resolveLine !== undefined,
		isClosed: () => closed,
		snapshot: () => [...completedLines, ...(currentLine ? [currentLine] : [])].join("\n"),
		terminal: {
			async writeTrusted(text: string): Promise<void> {
				if (closed) throw new Error("terminal is closed");
				writes.push(text);
				lifecycle.push(`write:${text}`);
				if (resolveLine) currentLine = "";
				append(text);
				if (resolveLine) currentLine += `${prompt}${buffer}`;
			},
			async readLine(nextPrompt: string): Promise<string | undefined> {
				if (closed) return undefined;
				prompt = nextPrompt;
				buffer = "";
				currentLine += prompt;
				const queued = pending.shift();
				if (queued !== undefined) {
					completedLines.push(`${currentLine}${queued}`);
					currentLine = "";
					prompt = "";
					return queued;
				}
				return await new Promise((resolve) => {
					resolveLine = resolve;
				});
			},
			close(): void {
				if (closed) return;
				closed = true;
				lifecycle.push("close");
				const resolve = resolveLine;
				resolveLine = undefined;
				resolve?.(undefined);
			},
		},
	};
}

async function eventually(read: () => boolean, description: string, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (read()) return;
		await Bun.sleep(10);
	}
	throw new Error(description);
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
	const promise = new Promise<void>((resolvePromise) => {
		resolve = resolvePromise;
	});
	return {
		promise,
		resolve(): void {
			resolve?.();
		},
	};
}

function blockingSubmissionRpc(release: Promise<void>, submissions: string[]): JsonRpcClient {
	let eventReads = 0;
	return {
		async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
			if (method === "way.health") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { status: "healthy", state: "running", main: { resumed: true, session_id: "fake" } },
				};
			}
			if (method === "way.status") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: {
						status: "healthy",
						state: "running",
						main: { resumed: true, session_id: "fake" },
						turn_state: "idle",
						follow_up_queue_depth: 0,
						journal: { head_cursor: "1:0", degraded: false },
						lock: { held: false, queue_len: 0, stuck: false, quarantined: false },
						write_mode: true,
						reconcile: { drift_count: 0 },
					},
				};
			}
			if (method === "consumer.claim") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { claim_id: `claim-${eventReads}`, cursor: "1:0", expires_at: Date.now() + 60_000 },
				};
			}
			if (method === "main.events.read") {
				eventReads += 1;
				if (eventReads === 1) return { jsonrpc: "2.0", id: 1, result: { events: [], next_cursor: "1:0" } };
				return await new Promise<JsonRpcResponse>((_resolve, reject) => {
					const signal = options?.signal;
					const abort = () => reject(new Error("events read aborted"));
					if (signal?.aborted) return abort();
					signal?.addEventListener("abort", abort, { once: true });
				});
			}
			if (method === "consumer.commit") return { jsonrpc: "2.0", id: 1, result: {} };
			if (method === "main.submit") {
				const text = (params as { text?: unknown } | undefined)?.text;
				if (typeof text !== "string") throw new Error("main.submit text was not a string");
				submissions.push(text);
				await release;
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { accepted: true, op_ref: `op-${submissions.length}`, delivered_as: "prompt" },
				};
			}
			throw new Error(`unexpected test RPC method: ${method}`);
		},
		close(): void {},
	};
}

async function assertRawControlEscapesPermanentlyStalledOutput(control: "\u0003" | "\u0004"): Promise<void> {
	const gateway = await hostedConsoleGateway();
	const input = new PausableRawInputHarness();
	const output = new PermanentlyStalledRawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const initial = Array.from({ length: 16 }, (_value, index) => `busy-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const refusedPrefix = "REFUSED_PAYLOAD_MUST_NOT_ECHO";
	const refusedBurst = Array.from({ length: 256 }, (_value, index) => `${refusedPrefix}-${index}`).join("\n");
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal,
		profile: loadWayProfile(gateway.profilePath),
		rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions),
		exitDrainMs: 25,
	});
	try {
		await eventually(() => output.writes.includes("way> "), "raw console did not begin reading interactive input");
		for (const line of initial) input.send(`${line}\n`);
		await eventually(() => submissions.length === initial.length, "raw console did not saturate owner operations");
		await eventually(() => !terminal.rawPublicationPending, "initial raw echo did not flush");

		output.stall();
		const writesBeforeStall = output.writes.length;
		input.send(`${queued[0]}\n`);
		await eventually(() => output.pendingWriteCount === 1, "first stalled echo did not begin publication");
		input.send(`${queued.slice(1).join("\n")}\n${refusedBurst}\n`);

		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.bufferedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_LINE_BYTES);
		expect(terminal.pendingEchoRedrawCount).toBe(1);
		expect(terminal.rawPublicationPending).toBe(true);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(output.pendingWriteCount).toBe(1);
		expect(output.writes.slice(writesBeforeStall)).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(refusedPrefix);
		expect(input.pauseCalls).toBe(0);

		await Bun.sleep(300);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(output.writes.slice(writesBeforeStall)).toHaveLength(1);

		const startedAt = performance.now();
		input.send(control);
		const completed = await Promise.race([running.then(() => true), Bun.sleep(500).then(() => false)]);
		expect(completed).toBe(true);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(terminal.inputPaused).toBe(true);
		expect(input.isPaused).toBe(true);
		expect(output.writes.join("")).not.toContain(refusedPrefix);
	} finally {
		release.resolve();
		terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}

async function assertRawSaturatedControlStartsBoundedDrain(control: "\u0003" | "\u0004"): Promise<void> {
	const gateway = await hostedConsoleGateway();
	const input = new PausableRawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const initial = Array.from({ length: 16 }, (_value, index) => `busy-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const rejected = ["rejected-after-saturation", "rejected-after-saturation-again"];
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal,
		profile: loadWayProfile(gateway.profilePath),
		rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions),
		exitDrainMs: 25,
	});
	let completed = false;
	try {
		await eventually(() => output.writes.includes("way> "), "raw console did not begin reading interactive input");
		for (const line of initial) input.send(`${line}\n`);
		await eventually(() => submissions.length === initial.length, "raw console did not saturate owner operations");
		for (const line of queued) input.send(`${line}\n`);
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.inputPaused).toBe(false);
		expect(input.isPaused).toBe(false);
		expect(input.pauseCalls).toBe(0);

		input.send(`${rejected.join("\n")}\n`);
		await eventually(
			() => output.writes.join("").includes("Input queue is full"),
			"raw console did not render its saturated-input refusal before control input",
		);
		const startedAt = performance.now();
		input.send(control);
		completed = await Promise.race([running.then(() => true), Bun.sleep(500).then(() => false)]);
		expect(completed).toBe(true);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(output.writes.join("")).toContain("Exit requested; 16 console operations are still outstanding.");
		expect(terminal.inputPaused).toBe(true);
	} finally {
		release.resolve();
		terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}

test("console submits through real UDS, renders finalized replies before settlement, and resumes its server checkpoint after restart", async () => {
	const gateway = await hostedConsoleGateway();
	let cursorAtAssistantRender: string | undefined;
	const recorded = recordingClient(gateway.client);
	const rendered = recordedOutput((text) => {
		if (text.startsWith("Assistant:\n")) cursorAtAssistantRender = gateway.core.consumerCursor("way-console");
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
		expect(recorded.calls.filter((call) => call.method === "way.health")).toHaveLength(1);
		expect(recorded.calls.filter((call) => call.method === "way.status")).toHaveLength(1);
		await consoleSurface.submit("owner request");
		expect(await consoleSurface.consumeOnce()).toBe("rendered");

		const submit = recorded.calls.find((call) => call.method === "main.submit");
		expect(submit?.params).toEqual({ text: "owner request", surface_id: "owner", idempotency_key: "console-submit-1" });
		expect(rendered.writes.join("")).toContain("Delivered as: prompt");
		const consumerClaim = recorded.calls.find((call) => call.method === "consumer.claim");
		expect(consumerClaim?.params).toEqual({ consumer_id: "way-console", claim_ttl_ms: 5_000 });
		const eventRead = recorded.calls.find((call) => call.method === "main.events.read");
		expect(eventRead?.params).toEqual({
			consumer_id: "way-console",
			limit: 100,
			wait_ms: 0,
			kinds: [
				"assistant_message",
				"turn_start",
				"turn_end",
				"gate_open",
				"gate_resolved",
				"health_change",
				"lock_event",
			],
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
	const consoleSurface = new OwnerConsole({
		rpc: gateway.client,
		ownerSurfaceId: "owner",
		output: rendered.output,
		readWaitMs: 0,
	});
	const hostile = "readable \x1b[2J CSI \x1b]52;c;SGVsbG8=\u0007 OSC52 \u0000\b\t\n\r\u009b1A C1";
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: hostile }));
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		const assistantText = rendered.writes.find((text) => text.includes("readable"));
		expect(assistantText).toContain("\\x1B[2J");
		expect(assistantText).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
		expect(assistantText).toContain("\\u0000\\u0008\\t\\n\\r\\u009B1A");
		if (!assistantText) throw new Error("assistant frame was not rendered");
		expect(assistantText.slice("Assistant:\n".length, -1)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
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
		const errorConsole = new OwnerConsole({
			rpc: hostileErrorRpc,
			ownerSurfaceId: "owner",
			output: errorOutput.output,
			readWaitMs: 0,
		});
		expect((await errorConsole.start()).accepted).toBe(true);
		expect(await errorConsole.handleInput("request that returns a hostile RPC error")).toBe(true);
		const errorText = errorOutput.writes.find((text) => text.includes("RPC main.submit failed"));
		expect(errorText).toContain("\\x1B[2J");
		expect(errorText).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
		if (!errorText) throw new Error("error frame was not rendered");
		expect(errorText.slice("Request failed: ".length, -1)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
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
		const consoleSurface = new OwnerConsole({
			rpc: contender,
			ownerSurfaceId: "owner",
			output: rendered.output,
			readWaitMs: 0,
		});
		const startup = await consoleSurface.start();
		expect(startup).toMatchObject({ accepted: false });
		expect(rendered.writes.join("")).toContain("Another way console currently owns");
		await expect(consoleSurface.submit("must not submit while delivery is elsewhere")).rejects.toBeInstanceOf(
			ConsoleDeliveryUnavailableError,
		);
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
		await expect(consoleSurface.submit("must not submit without a recoverable checkpoint")).rejects.toBeInstanceOf(
			ConsoleDeliveryUnavailableError,
		);
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
	const rendered = recordedOutput(async (text) => {
		if (delayAssistant && text === "Assistant:\ndelayed assistant\n") await publication.promise;
	});
	const consoleSurface = new OwnerConsole({
		rpc: gateway.client,
		ownerSurfaceId: "owner",
		output: rendered.output,
		readWaitMs: 0,
	});
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		const appended = gateway.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "delayed assistant" }),
		);
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
	const rendered = recordedOutput((text) => {
		if (failAssistant && text === "Assistant:\nterminal write rejected\n")
			throw new Error("simulated terminal writer failure");
	});
	const consoleSurface = new OwnerConsole({
		rpc: gateway.client,
		ownerSurfaceId: "owner",
		output: rendered.output,
		readWaitMs: 0,
	});
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "terminal write rejected" }),
		);
		failAssistant = true;
		await expect(consoleSurface.consumeOnce()).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(gateway.core.consumerCursor("way-console")).toBe("1:0");
		await expect(consoleSurface.submit("must not submit after a publication failure")).rejects.toBeInstanceOf(
			ConsoleDeliveryUnavailableError,
		);
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
	core.startRpcServer(
		socketPath,
		createRpcBridge(core, () => {
			throw new RpcBridgeException(-32601, "method not found");
		}),
	);
	core.setRpcHealth("failed_closed", "profile_drift");
	try {
		await connectEventually(socketPath).then((client) => client.close());
		const child = managedProcesses.trackBun(
			Bun.spawn({
				cmd: ["bun", "src/main.ts", "console", "--state-dir", stateDirectory, "--profile", profilePath],
				cwd: repositoryRoot,
				stdout: "pipe",
				stderr: "pipe",
			}),
		);
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
	const pending = gateway.core.journalAppend(
		"assistant_message",
		JSON.stringify({ finalized: true, text: "pending before terminal validation" }),
	);
	try {
		const child = managedProcesses.trackBun(
			Bun.spawn({
				cmd: ["bun", "src/main.ts", "console", "--state-dir", gateway.stateDirectory, "--profile", gateway.profilePath],
				cwd: repositoryRoot,
				stdout: "pipe",
				stderr: "pipe",
			}),
		);
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
		await runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
			terminal: subsequent.terminal,
		});
		expect(subsequent.writes.join("")).toContain("Assistant:\npending before terminal validation\n");
		expect(gateway.core.consumerCursor("way-console")).toBe(pending.cursor);
	} finally {
		await gateway.stop();
	}
});
test("console drains a submitted operation before closing after an immediate /quit", async () => {
	const gateway = await hostedConsoleGateway();
	const terminal = controlledTerminal();
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		terminal.send("submit before immediate quit");
		terminal.send("/quit");
		await running;
		const delivered = terminal.lifecycle.findIndex((entry) => entry === "write:Delivered as: prompt\n");
		const closed = terminal.lifecycle.indexOf("close");
		expect(delivered).toBeGreaterThanOrEqual(0);
		expect(closed).toBeGreaterThan(delivered);
	} finally {
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("console reports outstanding operations when exit grace elapses", async () => {
	const gateId = "gate-exit-grace";
	const sdk = new FileSdkDouble({ gateOnPrompt: { text: "hold beyond exit grace", gateId } });
	const gateway = await hostedConsoleGateway({ sdk });
	const terminal = controlledTerminal();
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
		exitDrainMs: 25,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		terminal.send("hold beyond exit grace");
		await eventually(
			() =>
				terminal.writes
					.join("")
					.includes(`Gate opened: gate_id=${gateId} expected_session_id=${gateway.host.sessionId}`),
			"prompt did not enter the pending workflow gate",
		);
		terminal.send("/quit");
		await running;
		expect(terminal.writes.join("")).toContain("Exit requested; 1 console operation is still outstanding.");
		expect(terminal.writes.join("")).toContain(
			"Results will remain available through the journal at the way-console consumer checkpoint.",
		);
		const release = await gateway.client.request("main.gate.answer", {
			gate_id: gateId,
			expected_session_id: gateway.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "release-exit-grace",
		});
		expect(rpcResult(release, "main.gate.answer")).toMatchObject({ accepted: true });
		await eventually(
			() => gateway.host.turnState === "idle",
			"pending prompt did not settle after cleanup gate answer",
		);
	} finally {
		await gateway.client
			.request("main.gate.answer", {
				gate_id: gateId,
				expected_session_id: gateway.host.sessionId,
				answer: { selected: ["Yes"] },
				idempotency_key: "release-exit-grace-cleanup",
			})
			.catch(() => undefined);
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("console closes within a bound when a stalled terminal writer blocks the outstanding-operation frame", async () => {
	const gateway = await hostedConsoleGateway();
	const never = new Promise<void>(() => undefined);
	let stallWrites = false;
	const terminal = controlledTerminal({
		onWrite: async () => {
			if (stallWrites) await never;
		},
	});
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
		exitDrainMs: 25,
	});
	let finished = false;
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		stallWrites = true;
		const startedAt = performance.now();
		terminal.send("stall a completion frame");
		terminal.send("/quit");
		finished = await Promise.race([running.then(() => true), Bun.sleep(500).then(() => false)]);
		expect(finished).toBe(true);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(terminal.isClosed()).toBe(true);
	} finally {
		terminal.terminal.close();
		await gateway.stop();
		if (finished) await running.catch(() => undefined);
	}
}, 15_000);

test("delivery loss closes immediately without waiting for outstanding owner operations", async () => {
	const gateId = "gate-delivery-loss";
	const sdk = new FileSdkDouble({ gateOnPrompt: { text: "hold through delivery loss", gateId } });
	const gateway = await hostedConsoleGateway({ sdk });
	let failPublication = false;
	const terminal = controlledTerminal({
		onWrite(text) {
			if (failPublication && text === "Assistant:\nforce terminal delivery loss\n") {
				throw new Error("simulated terminal delivery failure");
			}
		},
	});
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
		exitDrainMs: 1_000,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		terminal.send("hold through delivery loss");
		await eventually(
			() =>
				terminal.writes
					.join("")
					.includes(`Gate opened: gate_id=${gateId} expected_session_id=${gateway.host.sessionId}`),
			"prompt did not enter the pending workflow gate",
		);
		failPublication = true;
		const startedAt = performance.now();
		gateway.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "force terminal delivery loss" }),
		);
		await expect(running).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(terminal.isClosed()).toBe(true);
		const release = await gateway.client.request("main.gate.answer", {
			gate_id: gateId,
			expected_session_id: gateway.host.sessionId,
			answer: { selected: ["Yes"] },
			idempotency_key: "release-delivery-loss",
		});
		expect(rpcResult(release, "main.gate.answer")).toMatchObject({ accepted: true });
		await eventually(
			() => gateway.host.turnState === "idle",
			"pending prompt did not settle after cleanup gate answer",
		);
	} finally {
		await gateway.client
			.request("main.gate.answer", {
				gate_id: gateId,
				expected_session_id: gateway.host.sessionId,
				answer: { selected: ["Yes"] },
				idempotency_key: "release-delivery-loss-cleanup",
			})
			.catch(() => undefined);
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("console queues cap-saturated owner lines until all queued lines are admitted", async () => {
	const gateway = await hostedConsoleGateway();
	const terminal = controlledTerminal();
	const submissionsReleased = deferred();
	let submitCalls = 0;
	let eventReads = 0;
	const fakeRpc: JsonRpcClient = {
		async request(method: string, _params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
			if (method === "way.health") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { status: "healthy", state: "running", main: { resumed: true, session_id: "fake" } },
				};
			}
			if (method === "way.status") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: {
						status: "healthy",
						state: "running",
						main: { resumed: true, session_id: "fake" },
						turn_state: "idle",
						follow_up_queue_depth: 0,
						journal: { head_cursor: "1:0", degraded: false },
						lock: { held: false, queue_len: 0, stuck: false, quarantined: false },
						write_mode: true,
						reconcile: { drift_count: 0 },
					},
				};
			}
			if (method === "consumer.claim") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { claim_id: `claim-${eventReads}`, cursor: "1:0", expires_at: Date.now() + 60_000 },
				};
			}
			if (method === "main.events.read") {
				eventReads += 1;
				if (eventReads === 1) return { jsonrpc: "2.0", id: 1, result: { events: [], next_cursor: "1:0" } };
				return await new Promise<JsonRpcResponse>((_resolve, reject) => {
					const signal = options?.signal;
					const abort = () => reject(new Error("events read aborted"));
					if (signal?.aborted) return abort();
					signal?.addEventListener("abort", abort, { once: true });
				});
			}
			if (method === "consumer.commit") return { jsonrpc: "2.0", id: 1, result: {} };
			if (method === "main.submit") {
				submitCalls += 1;
				await submissionsReleased.promise;
				return {
					jsonrpc: "2.0",
					id: 1,
					result: { accepted: true, op_ref: `op-${submitCalls}`, delivered_as: "prompt" },
				};
			}
			throw new Error(`unexpected test RPC method: ${method}`);
		},
		close(): void {},
	};
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
		profile: loadWayProfile(gateway.profilePath),
		rpcConnect: async () => fakeRpc,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		for (let index = 0; index < 17; index += 1) terminal.send(`queued owner line ${index}`);
		terminal.send("/quit");
		await eventually(() => submitCalls === 16, "console did not saturate the bounded input-operation cap");
		submissionsReleased.resolve();
		await running;
		expect(submitCalls).toBe(17);
		expect(terminal.writes.filter((write) => write === "Delivered as: prompt\n")).toHaveLength(17);
	} finally {
		submissionsReleased.resolve();
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("raw same-chunk oversized input renders a refusal and leaves the owner command loop usable", async () => {
	const gateway = await hostedConsoleGateway();
	const input = new PausableRawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const submissions: string[] = [];
	const accepted = "submit after oversized input";
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal,
		profile: loadWayProfile(gateway.profilePath),
		rpcConnect: async () => blockingSubmissionRpc(Promise.resolve(), submissions),
	});
	try {
		await eventually(() => output.writes.includes("way> "), "raw console did not begin reading interactive input");
		input.send(`${oversized}\n`);
		await eventually(
			() => output.writes.join("").includes(`Input line exceeds ${MAX_RAW_CONSOLE_LINE_BYTES} bytes and was refused.`),
			"same-chunk oversized input did not render a refusal",
		);
		expect(output.writes.filter((write) => write.includes("Input line exceeds"))).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
		expect(terminal.bufferedInputBytes).toBe(0);
		expect(submissions).toEqual([]);

		input.send(`${accepted}\n`);
		await eventually(() => submissions.length === 1, "normal input after oversized refusal was not submitted");
		expect(submissions).toEqual([accepted]);
		input.send("/quit\n");
		await running;
	} finally {
		terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("raw console bounds queued input, visibly refuses excess lines, and preserves FIFO after operation-cap saturation", async () => {
	const gateway = await hostedConsoleGateway();
	const input = new PausableRawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const initial = Array.from({ length: 16 }, (_value, index) => `busy-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const rejected = Array.from({ length: 4 }, (_value, index) => `rejected-${index}`);
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal,
		profile: loadWayProfile(gateway.profilePath),
		rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions),
	});
	try {
		await eventually(() => output.writes.includes("way> "), "raw console did not begin reading interactive input");
		for (const line of initial) input.send(`${line}\n`);
		await eventually(() => submissions.length === initial.length, "console did not saturate the owner-operation cap");
		for (const line of queued) input.send(`${line}\n`);
		for (const line of rejected) input.send(`${line}\n`);
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.inputPaused).toBe(false);
		expect(input.isPaused).toBe(false);
		expect(input.pauseCalls).toBe(0);
		expect(input.blockedChunkCount).toBe(0);
		await eventually(
			() => output.writes.join("").includes("Input queue is full"),
			"raw console did not visibly refuse excess saturated input",
		);

		release.resolve();
		await eventually(
			() => submissions.length === initial.length + queued.length,
			"raw console did not admit every accepted queued line",
		);
		expect(submissions).toEqual([...initial, ...queued]);
		input.send("/quit\n");
		await running;
	} finally {
		release.resolve();
		terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);
test("raw Ctrl-C stays observable at saturated queue capacity and starts bounded drain", async () => {
	await assertRawSaturatedControlStartsBoundedDrain("\u0003");
}, 15_000);

test("raw Ctrl-D stays observable at saturated queue capacity and starts bounded drain", async () => {
	await assertRawSaturatedControlStartsBoundedDrain("\u0004");
}, 15_000);

test("raw Ctrl-C remains observable through permanently stalled stdout at saturated queue capacity", async () => {
	await assertRawControlEscapesPermanentlyStalledOutput("\u0003");
}, 15_000);

test("raw Ctrl-D remains observable through permanently stalled stdout at saturated queue capacity", async () => {
	await assertRawControlEscapesPermanentlyStalledOutput("\u0004");
}, 15_000);
test("real console command loop answers a gate opened by its pending prompt and lets that turn complete", async () => {
	const gateId = "gate-command-loop";
	const sdk = new FileSdkDouble({ gateOnPrompt: { text: "wait for the workflow gate", gateId } });
	const gateway = await hostedConsoleGateway({ sdk });
	const terminal = controlledTerminal();
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		terminal.send("wait for the workflow gate");
		await eventually(
			() =>
				terminal.writes
					.join("")
					.includes(`Gate opened: gate_id=${gateId} expected_session_id=${gateway.host.sessionId}`),
			"pending prompt did not publish its workflow gate",
		);
		terminal.send(`/gate ${gateId} ${gateway.host.sessionId} {"selected":["Yes"]}`);
		await eventually(
			() => terminal.writes.join("").includes(`Gate ${gateId}: resolved`),
			"command-loop gate answer did not complete",
		);
		await eventually(
			() => terminal.writes.join("").includes("Main turn ended — idle."),
			"original prompt did not complete after the command-loop gate answer",
		);
		expect(terminal.writes.join("")).toContain("Delivered as: prompt");
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("real console command loop admits an owner message typed during a busy turn as a steer", async () => {
	const gateId = "gate-steer";
	const sdk = new FileSdkDouble({ gateOnPrompt: { text: "hold this turn open", gateId } });
	const gateway = await hostedConsoleGateway({ sdk });
	const terminal = controlledTerminal();
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
	});
	try {
		await eventually(terminal.isReading, "console did not begin reading interactive input");
		terminal.send("hold this turn open");
		await eventually(
			() =>
				terminal.writes
					.join("")
					.includes(`Gate opened: gate_id=${gateId} expected_session_id=${gateway.host.sessionId}`),
			"pending prompt did not enter a busy gate state",
		);
		terminal.send("steer this busy turn");
		await eventually(
			() => terminal.writes.join("").includes("Delivered as: steer"),
			"owner message typed during the busy turn was not admitted as a steer",
		);
		terminal.send(`/gate ${gateId} ${gateway.host.sessionId} {"selected":["Yes"]}`);
		await eventually(
			() => terminal.writes.join("").includes("Main turn ended — idle."),
			"busy prompt did not finish after gate resolution",
		);
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("prompt repaint preserves complete assistant and gate frames while way> is active", async () => {
	const gateway = await hostedConsoleGateway();
	const terminal = promptFrameTerminal();
	const running = runWayConsole({ stateDir: gateway.stateDirectory, profilePath: gateway.profilePath }, [], {
		terminal: terminal.terminal,
	});
	try {
		await eventually(terminal.isReading, "console did not leave an active prompt for frame rendering");
		const assistant = gateway.core.journalAppend(
			"assistant_message",
			JSON.stringify({ finalized: true, text: "assistant frame remains visible" }),
		);
		const gate = gateway.core.journalAppend(
			"gate_open",
			JSON.stringify({ gate_id: "gate-frame-visible", session_id: "session-frame-visible" }),
		);
		await eventually(
			() => gateway.core.consumerCursor("way-console") === gate.cursor,
			"console did not publish and commit the prompt-active frames",
		);
		const screen = terminal.snapshot();
		expect(assistant.cursor).not.toBe(gate.cursor);
		expect(screen).toContain("Assistant:\nassistant frame remains visible");
		expect(screen).toContain("gate_id=gate-frame-visible");
		expect(screen).toContain("expected_session_id=session-frame-visible");
		expect(screen.endsWith("way> ")).toBe(true);
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await gateway.stop();
		await running.catch(() => undefined);
	}
}, 15_000);

test("console gate drill uses durable gate fencing, rejects a mismatched session, and renders resolution", async () => {
	const gateway = await hostedConsoleGateway();
	const rendered = recordedOutput();
	const consoleSurface = new OwnerConsole({
		rpc: gateway.client,
		ownerSurfaceId: "owner",
		output: rendered.output,
		readWaitMs: 0,
	});
	try {
		expect((await consoleSurface.start()).accepted).toBe(true);
		gateway.sdk.openGate(gateway.sessionFile, "gate-live");
		expect(await consoleSurface.consumeOnce()).toBe("rendered");
		expect(rendered.writes.join("")).toContain("Gate opened: gate_id=gate-live expected_session_id=");

		await expect(consoleSurface.answerGate("gate-live", "wrong-session", { selected: ["Yes"] })).rejects.toMatchObject({
			code: 1102,
		});
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
