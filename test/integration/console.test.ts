import { AsyncLocalStorage } from "node:async_hooks";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
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
import { canonicalJson } from "../../src/main-session/gates";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge, RpcBridgeException } from "../../src/rpc-bridge";
import type { JsonRpcClient, JsonRpcResponse, RpcRequestOptions } from "../../src/rpc-client";
import { connectEventually, createExternalGateway, eventually, type ExternalGateway } from "../helpers/external-gateway";

const repositoryRoot = path.resolve(import.meta.dir, "../..");
const gatewayScope = new AsyncLocalStorage<ExternalGateway[]>();

type ExternalTestBody = () => void | Promise<void>;

let externalTestTail: Promise<void> = Promise.resolve();

function externalTest(name: string, body: ExternalTestBody, timeoutMs?: number): void {
	test(name, async () => {
		let release: (() => void) | undefined;
		const previous = externalTestTail;
		externalTestTail = new Promise<void>(resolve => {
			release = resolve;
		});
		await previous;
		const gateways: ExternalGateway[] = [];
		try {
			await gatewayScope.run(gateways, body);
		} finally {
			for (const active of gateways.splice(0)) await active.stop();
			release?.();
		}
	}, Math.max(timeoutMs ?? 0, 60_000));
}

async function gateway(): Promise<ExternalGateway> {
	const active = await createExternalGateway();
	const gateways = gatewayScope.getStore();
	if (!gateways) throw new Error("gateway() must run inside externalTest().");
	gateways.push(active);
	return active;
}

const EXTERNAL_HOST_GATE_ANSWER_GUIDANCE =
	"The gateway cannot answer gates in the external-host architecture. The owner must answer each gate in the attached gjc TUI: tmux attach -t <session> when the tmux backend hosts it, or whatever terminal runs gjc.";

interface ControlledTerminal {
	readonly terminal: ConsoleTerminal;
	readonly writes: string[];
	readonly lifecycle: string[];
	send(line: string): void;
	isReading(): boolean;
	isClosed(): boolean;
}

function controlledTerminal(options: { readonly onWrite?: (text: string) => void | Promise<void> } = {}): ControlledTerminal {
	const pending: string[] = [];
	const writes: string[] = [];
	const lifecycle: string[] = [];
	let resolveLine: ((line: string | undefined) => void) | undefined;
	let closed = false;
	return {
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
				return await new Promise(resolve => {
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
		writes,
		lifecycle,
		send(line: string): void {
			if (closed) throw new Error("terminal is closed");
			const resolve = resolveLine;
			if (!resolve) {
				pending.push(line);
				return;
			}
			resolveLine = undefined;
			resolve(line);
		},
		isReading: () => resolveLine !== undefined,
		isClosed: () => closed,
	};
}

class RawInputHarness {
	readonly isTTY = true;
	isRaw = false;
	isPaused = false;
	pauseCalls = 0;
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
		if (!this.isPaused) return;
		this.isPaused = false;
		while (!this.isPaused && this.#blockedChunks.length > 0) this.deliver(this.#blockedChunks.shift() as string);
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

function visibleTerminalText(text: string): string {
	return text
		.replace(/\x1b\][\s\S]*?(?:\x07|\x1b\\)/gu, "")
		.replace(/\x1b\[[0-?]*[ -/]*[@-~]/gu, "")
		.replace(/\x1b[()][A-Za-z0-9]?/gu, "");
}

function latestTuiFrame(writes: readonly string[]): string {
	return visibleTerminalText(writes.at(-1) ?? "");
}

function deferred(): { readonly promise: Promise<void>; resolve(): void } {
	let resolve: (() => void) | undefined;
	const promise = new Promise<void>(resolvePromise => {
		resolve = resolvePromise;
	});
	return { promise, resolve: () => resolve?.() };
}

function recordedOutput(writer?: (text: string) => void | Promise<void>): { readonly output: ConsoleOutput; readonly writes: string[] } {
	const writes: string[] = [];
	return {
		writes,
		output: new ConsoleOutput(async text => {
			await writer?.(text);
			writes.push(text);
		}),
	};
}

function recordingClient(client: JsonRpcClient): { readonly rpc: JsonRpcClient; readonly calls: Array<{ method: string; params: unknown }> } {
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

function blockingSubmissionRpc(release: Promise<void>, submissions: string[]): JsonRpcClient {
	let eventReads = 0;
	return {
		async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
			if (method === "way.health") {
				return { jsonrpc: "2.0", id: 1, result: { status: "healthy", state: "running", main: { resumed: true, session_id: "fake" } } };
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
				return { jsonrpc: "2.0", id: 1, result: { claim_id: `claim-${eventReads}`, cursor: "1:0", expires_at: Date.now() + 60_000 } };
			}
			if (method === "main.events.read") {
				eventReads += 1;
				if (eventReads === 1) return { jsonrpc: "2.0", id: 1, result: { events: [], next_cursor: "1:0" } };
				return await new Promise<JsonRpcResponse>((_resolve, reject) => {
					const abort = () => reject(Object.assign(new Error("events read aborted"), { name: "AbortError" }));
					if (options?.signal?.aborted) return abort();
					options?.signal?.addEventListener("abort", abort, { once: true });
				});
			}
			if (method === "consumer.commit") return { jsonrpc: "2.0", id: 1, result: {} };
			if (method === "main.submit") {
				const text = (params as { text?: unknown } | undefined)?.text;
				if (typeof text !== "string") throw new Error("main.submit text was not a string");
				submissions.push(text);
				await release;
				return { jsonrpc: "2.0", id: 1, result: { accepted: true, op_ref: `op-${submissions.length}`, delivered_as: "prompt" } };
			}
			throw new Error(`unexpected test RPC method: ${method}`);
		},
		close(): void {},
	};
}

function bareConsoleGateway(name: string, health: "running" | "failed_closed"): {
	readonly root: string;
	readonly profilePath: string;
	readonly stateDirectory: string;
	readonly core: WayCoreHandle;
	stop(): void;
} {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), `gajaeway-console-${name}-`));
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDirectory = path.join(root, "state");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\n`,
	);
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(
		path.join(stateDirectory, "rpc.sock"),
		createRpcBridge(core, () => {
			throw new RpcBridgeException(-32601, "method not found");
		}),
	);
	core.setRpcHealth(health, health === "failed_closed" ? "profile_drift" : undefined);
	return {
		root,
		profilePath,
		stateDirectory,
		core,
		stop(): void {
			core.shutdownRpcServer();
			fs.rmSync(root, { force: true, recursive: true });
		},
	};
}

function acquireInDaemonLock(core: WayCoreHandle, sessionId: string, label: string) {
	const identity = core.processIdentity(process.pid);
	return core.lockAcquire({
		label,
		waitMs: 0,
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

externalTest("console consumes the external supervisor UDS surface and restart resumes its checkpoint without duplicate render", async () => {
	const active = await gateway();
	let cursorAtAssistantRender: string | undefined;
	const firstOutput = recordedOutput(text => {
		if (text.startsWith("Assistant:\n")) cursorAtAssistantRender = active.core.consumerCursor("gajaeway-console");
	});
	const consoleSurface = new OwnerConsole({
		rpc: active.client,
		ownerSurfaceId: "owner",
		output: firstOutput.output,
		readWaitMs: 0,
		idempotencyKey: () => "console-external-submit",
	});

	expect((await consoleSurface.start()).accepted).toBe(true);
	await consoleSurface.submit("console external prompt");
	await eventually(
		() => (active.core.journalRead("1:0", 20).events.some(event => event.kind === "assistant_message") ? true : undefined),
		"external supervisor output was not journaled",
	);
	expect(await consoleSurface.consumeOnce()).toBe("rendered");
	expect(firstOutput.writes.join("")).toContain("Delivered as: prompt");
	expect(firstOutput.writes.join("")).toContain("Assistant:\nack\n");
	expect(cursorAtAssistantRender).toBe("1:0");
	const committedCursor = active.core.consumerCursor("gajaeway-console");
	expect(committedCursor).not.toBe("1:0");

	active.client.close();
	const restartedClient = await connectEventually(active.socketPath);
	const restartedOutput = recordedOutput();
	const restarted = new OwnerConsole({ rpc: restartedClient, ownerSurfaceId: "owner", output: restartedOutput.output, readWaitMs: 0 });
	try {
		expect((await restarted.start()).accepted).toBe(true);
		expect(await restarted.consumeOnce()).toBe("idle");
		expect(restartedOutput.writes.join("")).not.toContain("Assistant:\nack\n");
		expect(active.core.consumerCursor("gajaeway-console")).toBe(committedCursor);
	} finally {
		restartedClient.close();
	}
});

externalTest("console commits a journal checkpoint only after a stalled terminal publication resolves", async () => {
	const active = await gateway();
	const publication = deferred();
	let delayAssistant = false;
	const rendered = recordedOutput(async text => {
		if (delayAssistant && text === "Assistant:\ndelayed assistant\n") await publication.promise;
	});
	const consoleSurface = new OwnerConsole({ rpc: active.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });

	expect((await consoleSurface.start()).accepted).toBe(true);
	const checkpointBefore = active.core.consumerCursor("gajaeway-console");
	const appended = active.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "delayed assistant" }));
	delayAssistant = true;
	const consume = consoleSurface.consumeOnce();
	await Bun.sleep(25);
	expect(active.core.consumerCursor("gajaeway-console")).toBe(checkpointBefore);
	publication.resolve();
	expect(await consume).toBe("rendered");
	expect(active.core.consumerCursor("gajaeway-console")).toBe(appended.cursor);
});

externalTest("terminal publication failure leaves the checkpoint unadvanced and fences further console submission", async () => {
	const active = await gateway();
	let failAssistant = false;
	const rendered = recordedOutput(text => {
		if (failAssistant && text === "Assistant:\nterminal write rejected\n") throw new Error("simulated terminal writer failure");
	});
	const consoleSurface = new OwnerConsole({ rpc: active.client, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });

	expect((await consoleSurface.start()).accepted).toBe(true);
	const checkpointBefore = active.core.consumerCursor("gajaeway-console");
	active.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "terminal write rejected" }));
	failAssistant = true;
	await expect(consoleSurface.consumeOnce()).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
	expect(active.core.consumerCursor("gajaeway-console")).toBe(checkpointBefore);
	await expect(consoleSurface.submit("must remain fenced after failed publication")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
	expect(active.fixture.commands()).toEqual([]);
});

externalTest("actual console CLI exits non-zero after the failed-closed startup fence", async () => {
	const bare = bareConsoleGateway("failed-closed", "failed_closed");
	try {
		const ready = await connectEventually(path.join(bare.stateDirectory, "rpc.sock"));
		ready.close();
		const child = Bun.spawn({
			cmd: ["bun", "src/main.ts", "console", "--state-dir", bare.stateDirectory, "--profile", bare.profilePath],
			cwd: repositoryRoot,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
		expect(exitCode).toBe(1);
		expect(stdout).toContain("Refusing interactive console");
		expect(stdout).toContain("failed closed");
		expect(stdout).toContain("profile_drift");
		expect(stderr).toBe("");
	} finally {
		bare.stop();
	}
});

externalTest("healthy non-TTY console exits before claiming or consuming the journal checkpoint", async () => {
	const bare = bareConsoleGateway("non-tty", "running");
	const checkpointBefore = bare.core.consumerCursor("gajaeway-console");
	const pending = bare.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "pending before TTY validation" }));
	try {
		const ready = await connectEventually(path.join(bare.stateDirectory, "rpc.sock"));
		ready.close();
		const child = Bun.spawn({
			cmd: ["bun", "src/main.ts", "console", "--state-dir", bare.stateDirectory, "--profile", bare.profilePath],
			cwd: repositoryRoot,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
		expect(exitCode).toBe(1);
		expect(stderr).toContain("gajaeway console requires an interactive TTY");
		expect(stdout).toBe("");
		expect(bare.core.consumerCursor("gajaeway-console")).toBe(checkpointBefore);
		expect(bare.core.consumerOutbox("gajaeway-console")).toEqual([]);

		const terminal = controlledTerminal();
		terminal.send("/quit");
		await runWayConsole({ stateDir: bare.stateDirectory, profilePath: bare.profilePath }, [], { terminal: terminal.terminal });
		expect(terminal.writes.join("")).toContain("Assistant:\npending before TTY validation\n");
		expect(bare.core.consumerCursor("gajaeway-console")).toBe(pending.cursor);
	} finally {
		bare.stop();
	}
});

externalTest("real console command loop admits owner input during a held external turn as steer", async () => {
	const active = await gateway();
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, exitDrainMs: 1_000, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "console did not begin reading owner input");
		active.fixture.holdNextTurn();
		terminal.send("hold this external turn");
		await eventually(() => (active.host.turnState === "busy" ? true : undefined), "owner prompt did not enter the busy state");
		terminal.send("steer this busy turn");
		await eventually(
			() => (terminal.writes.join("").includes("Delivered as: steer") ? true : undefined),
			"busy owner input was not admitted as steer",
		);
		expect(active.fixture.commands().map(command => ({ operation: command.operation, text: command.text }))).toEqual([
			{ operation: "turn.prompt", text: "hold this external turn" },
			{ operation: "turn.steer", text: "steer this busy turn" },
		]);
		const heldPrompt = active.fixture.commands().find(command => command.operation === "turn.prompt");
		if (typeof heldPrompt?.opRef !== "string") throw new Error("held console prompt was not recorded with an operation reference");
		active.fixture.complete(heldPrompt.opRef, { text: "settled after steer assertion" });
		await eventually(
			() => (active.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_end") ? true : undefined),
			"held console prompt did not settle before teardown",
			15_000,
		);
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("console drains a submitted broker admission and renders delivered_as before close", async () => {
	const active = await gateway();
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, exitDrainMs: 1_000, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "console did not begin reading owner input");
		terminal.send("submit before immediate quit");
		terminal.send("/quit");
		await running;
		const delivered = terminal.lifecycle.indexOf("write:Delivered as: prompt\n");
		const closed = terminal.lifecycle.indexOf("close");
		expect(delivered).toBeGreaterThanOrEqual(0);
		expect(closed).toBeGreaterThan(delivered);
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("console reports outstanding admissions when its exit grace elapses", async () => {
	const active = await gateway();
	const terminal = controlledTerminal();
	const admission = deferred();
	const submissions: string[] = [];
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{
			terminal: terminal.terminal,
			profile: active.profile,
			rpcConnect: async () => blockingSubmissionRpc(admission.promise, submissions),
			exitDrainMs: 25,
			statusPollMs: 25,
		},
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "console did not begin reading owner input");
		terminal.send("hold admission beyond exit grace");
		await eventually(() => (submissions.length === 1 ? true : undefined), "console did not begin the held admission");
		terminal.send("/quit");
		await running;
		expect(terminal.writes.join("")).toContain("Exit requested; 1 console operation is still outstanding.");
		expect(terminal.writes.join("")).toContain("Results will remain available through the journal at the gajaeway-console consumer checkpoint.");
	} finally {
		admission.resolve();
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("delivery loss tears the console down immediately rather than waiting for the input loop", async () => {
	const active = await gateway();
	let failAssistant = false;
	const terminal = controlledTerminal({
		onWrite(text) {
			if (failAssistant && text === "Assistant:\nforce terminal delivery loss\n") throw new Error("simulated terminal delivery failure");
		},
	});
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, exitDrainMs: 1_000, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "console did not begin reading owner input");
		failAssistant = true;
		const startedAt = performance.now();
		active.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "force terminal delivery loss" }));
		await expect(running).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(terminal.isClosed()).toBe(true);
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("raw console visibly refuses a same-chunk oversized line and continues with later owner input", async () => {
	const active = await gateway();
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const submissions: string[] = [];
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const accepted = "submit after oversized input";
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal, profile: active.profile, rpcConnect: async () => blockingSubmissionRpc(Promise.resolve(), submissions), statusPollMs: 25 },
	);
	try {
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> ") ? true : undefined), "raw console did not begin reading input");
		input.send(`${oversized}\n`);
		await eventually(
			() => (output.writes.join("").includes(`Input line exceeds ${MAX_RAW_CONSOLE_LINE_BYTES} bytes and was refused.`) ? true : undefined),
			"same-chunk oversized input did not render a refusal",
		);
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
		expect(terminal.bufferedInputBytes).toBe(0);
		input.send(`${accepted}\n`);
		await eventually(() => (submissions.length === 1 ? true : undefined), "normal input after oversized refusal was not submitted");
		expect(submissions).toEqual([accepted]);
		input.send("/quit\n");
		await running;
	} finally {
		terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("raw console coalesces mixed oversized and full-queue refusals without accepting dropped input", async () => {
	const active = await gateway();
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const inFlight = Array.from({ length: 16 }, (_value, index) => `busy-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const dropped = "MIXED_QUEUE_FULL_COMMAND_MUST_NOT_SUBMIT";
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal, profile: active.profile, rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions), exitDrainMs: 25, statusPollMs: 25 },
	);
	try {
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> ") ? true : undefined), "raw console did not begin reading input");
		for (const line of inFlight) input.send(`${line}\n`);
		await eventually(() => (submissions.length === inFlight.length ? true : undefined), "console did not saturate owner operations");
		input.send(`${oversized}\n${queued.join("\n")}\n${dropped}\n`);
		await eventually(() => (output.writes.some(write => write.includes("Input refused:")) ? true : undefined), "mixed refusal was not rendered");
		const refusal = visibleTerminalText(output.writes.filter(write => write.includes("Input refused:")).at(-1) as string);
		expect(refusal).toContain("oversized-line=1");
		expect(refusal).toContain("queue-full-lines=1");
		expect(refusal).not.toContain("queue-full-bytes=");
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));
		expect(output.writes.join("")).not.toContain(dropped);
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(submissions).toEqual(inFlight);

		release.resolve();
		await eventually(
			() => (submissions.length === inFlight.length + queued.length ? true : undefined),
			"accepted queued input was not admitted after operation capacity released",
		);
		expect(submissions).toEqual([...inFlight, ...queued]);
		input.send("/quit\n");
		await running;
	} finally {
		release.resolve();
		terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("raw console bounds its queued lines and preserves FIFO after operation saturation", async () => {
	const active = await gateway();
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const initial = Array.from({ length: 16 }, (_value, index) => `initial-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const rejected = Array.from({ length: 4 }, (_value, index) => `rejected-${index}`);
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal, profile: active.profile, rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions), exitDrainMs: 25, statusPollMs: 25 },
	);
	try {
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> ") ? true : undefined), "raw console did not begin reading input");
		for (const line of initial) input.send(`${line}\n`);
		await eventually(() => (submissions.length === initial.length ? true : undefined), "console did not saturate owner operations");
		for (const line of queued) input.send(`${line}\n`);
		for (const line of rejected) input.send(`${line}\n`);
		await eventually(() => (output.writes.join("").includes("Input queue is full") ? true : undefined), "full input queue was not visible");
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(input.isPaused).toBe(false);
		expect(input.blockedChunkCount).toBe(0);

		release.resolve();
		await eventually(
			() => (submissions.length === initial.length + queued.length ? true : undefined),
			"full queue did not drain into admissions",
		);
		expect(submissions).toEqual([...initial, ...queued]);
		input.send("/quit\n");
		await running;
	} finally {
		release.resolve();
		terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

async function assertRawSaturatedControlStartsDrain(control: "\u0003" | "\u0004"): Promise<void> {
	const active = await gateway();
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const release = deferred();
	const submissions: string[] = [];
	const initial = Array.from({ length: 16 }, (_value, index) => `busy-${index}`);
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal, profile: active.profile, rpcConnect: async () => blockingSubmissionRpc(release.promise, submissions), exitDrainMs: 25, statusPollMs: 25 },
	);
	try {
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> ") ? true : undefined), "raw console did not begin reading input");
		for (const line of initial) input.send(`${line}\n`);
		await eventually(() => (submissions.length === initial.length ? true : undefined), "console did not saturate owner operations");
		for (const line of queued) input.send(`${line}\n`);
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		const startedAt = performance.now();
		input.send(control);
		const completed = await Promise.race([running.then(() => true), Bun.sleep(500).then(() => false)]);
		expect(completed).toBe(true);
		expect(performance.now() - startedAt).toBeLessThan(500);
		expect(terminal.inputPaused).toBe(true);
		expect(input.isPaused).toBe(true);
		expect(output.writes.join("")).toContain("Exit requested; 16 console operations are still outstanding.");
	} finally {
		release.resolve();
		terminal.close();
		await running.catch(() => undefined);
	}
}

externalTest("raw Ctrl-C stays observable at saturated queue capacity and starts bounded drain", async () => {
	await assertRawSaturatedControlStartsDrain("\u0003");
}, 15_000);

externalTest("raw Ctrl-D stays observable at saturated queue capacity and starts bounded drain", async () => {
	await assertRawSaturatedControlStartsDrain("\u0004");
}, 15_000);

externalTest("full-screen prompt repaint preserves assistant and gate frames while editing", async () => {
	const active = await gateway();
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal, statusPollMs: 25 },
	);
	try {
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> ") ? true : undefined), "console did not leave an active editor");
		input.send("draft while events arrive");
		await eventually(() => (latestTuiFrame(output.writes).includes("gajaeway> draft while events arrive") ? true : undefined), "editor did not repaint draft");
		active.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "assistant frame remains visible" }));
		const gate = active.core.journalAppend("gate_open", JSON.stringify({ gate_id: "gate-frame-visible", session_id: "session-frame-visible" }));
		await eventually(
			() => (active.core.consumerCursor("gajaeway-console") === gate.cursor ? true : undefined),
			"console did not commit the editor-active event frames",
		);
		const screen = latestTuiFrame(output.writes);
		expect(screen).toContain("Assistant:\nassistant frame remains visible");
		expect(screen).toContain("gate_id=gate-frame-visible");
		expect(screen.replace(/\n/gu, "")).toContain("expected_session_id=session-frame-visible");
		expect(screen.replace(/\n/gu, "")).toContain(EXTERNAL_HOST_GATE_ANSWER_GUIDANCE);
		expect(screen.replace(/\n/gu, "")).toContain(
			"/gate gate-frame-visible session-frame-visible <JSON answer> remains a capability probe for a future backend that supports validated gate receipts.",
		);

		expect(screen).toContain("gajaeway> draft while events arrive");
		input.send("\u0003");
		await running;
	} finally {
		terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("cockpit status pane renders lock quarantine and reconcile state from way.status", async () => {
	const active = await gateway();
	const lease = acquireInDaemonLock(active.core, "cockpit-status", "cockpit-status-drill");
	active.core.lockQuarantineOverride(lease.leaseId, true, true);
	active.core.setReconcileStatus({ lastOkAt: Date.now(), cycleMs: 5_000, driftCount: 3 });
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "cockpit did not begin reading input");
		terminal.send("/status");
		await eventually(
			() => (terminal.writes.some(write => write.startsWith("Gateway status") && write.includes("quarantined=true")) ? true : undefined),
			"cockpit did not render quarantined lock state",
		);
		const status = terminal.writes.filter(write => write.startsWith("Gateway status")).at(-1) as string;
		expect(status).toContain("lock: held=true");
		expect(status).toContain("quarantined=true write_mode=false");
		expect(status).toContain("reconcile: freshness=fresh");
		expect(status).toContain("drift_count=3");
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("cockpit renders durable-tail loss notices as warnings and exposes them in journal filters", async () => {
	const active = await gateway();
	const recorded = recordingClient(active.client);
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, rpcConnect: async () => recorded.rpc, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "cockpit did not begin reading input");
		active.core.journalAppend(
			"tail_ring_rotation",
			JSON.stringify({ prior_watermark: { generation: 1, seq: 4 }, resync_point: { generation: 2, seq: 1 } }),
		);
		active.core.journalAppend(
			"transcript_delivery_gap",
			JSON.stringify({ reason: "transcript_delivery_unprovable", delivered_through_entry_id: "entry-4" }),
		);
		await eventually(
			() => (terminal.writes.some(write => write.includes("WARNING: Lifecycle event-ring retention advanced")) ? true : undefined),
			"cockpit did not render the lifecycle-ring warning",
		);
		await eventually(
			() => (terminal.writes.some(write => write.includes("WARNING: Transcript delivery gap detected")) ? true : undefined),
			"cockpit did not render the transcript-gap warning",
		);
		terminal.send("/journal transcript_delivery_gap 20");
		await eventually(
			() => (terminal.writes.some(write => write.startsWith("Journal tail:") && write.includes("transcript_delivery_gap")) ? true : undefined),
			"cockpit journal view did not render the transcript-gap event",
		);
		const filteredRead = recorded.calls
			.filter(call => call.method === "main.events.read" && Array.isArray((call.params as Record<string, unknown> | undefined)?.kinds))
			.at(-1);
		expect((filteredRead?.params as { kinds?: readonly string[] }).kinds).toEqual(["transcript_delivery_gap"]);
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("cockpit /journal filters registry noise by default and honors an explicit kind", async () => {
	const active = await gateway();
	const recorded = recordingClient(active.client);
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, rpcConnect: async () => recorded.rpc, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "cockpit did not begin reading input");
		active.core.journalAppend("registry_change", JSON.stringify({ session_id: "registry-noise", reason: "poll" }));
		active.core.journalAppend("turn_start", JSON.stringify({ attempt_id: "cockpit-tail" }));
		terminal.send("/journal 20");
		await eventually(
			() => (terminal.writes.some(write => write.startsWith("Journal tail:") && write.includes("turn_start")) ? true : undefined),
			"default journal tail did not render matching lifecycle event",
		);
		const defaultTail = terminal.writes.filter(write => write.startsWith("Journal tail:")).at(-1) as string;
		expect(defaultTail).not.toContain("registry_change");
		const defaultRead = recorded.calls
			.filter(call => call.method === "main.events.read" && typeof (call.params as Record<string, unknown> | undefined)?.cursor === "string")
			.at(-1);
		expect((defaultRead?.params as { kinds?: readonly string[] }).kinds).not.toContain("registry_change");

		terminal.send("/journal registry_change 20");
		await eventually(
			() => (terminal.writes.filter(write => write.startsWith("Journal tail:")).some(write => write.includes("registry_change")) ? true : undefined),
			"explicit registry filter did not render registry events",
		);
		const explicitRead = recorded.calls
			.filter(call => call.method === "main.events.read" && typeof (call.params as Record<string, unknown> | undefined)?.cursor === "string")
			.at(-1);
		expect((explicitRead?.params as { kinds?: readonly string[] }).kinds).toEqual(["registry_change"]);
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("cockpit /registry list and inspect render durable UDS registry rows", async () => {
	const active = await gateway();
	active.core.registryApplyBrokerSnapshot({
		observedAt: Date.now(),
		rows: [
			{
				sessionId: "cockpit-lane-1",
				locator: JSON.stringify({ repo: "/workspace/cockpit-repo", stateRoot: "/workspace/cockpit-repo/.gjc/state" }),
				endpointGeneration: 1,
				identityProvenance: "composite",
				indexSeq: 1,
				live: true,
				deleted: false,
				terminalUncertain: false,
				ambiguous: false,
				activityState: "active",
				activityAt: Date.now(),
				lastHeartbeatAt: Date.now(),
			},
		],
	});
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "cockpit did not begin reading input");
		terminal.send("/registry list");
		await eventually(
			() => (terminal.writes.some(write => write.startsWith("Registry list:") && write.includes("cockpit-lane-1")) ? true : undefined),
			"registry list did not render the UDS row",
		);
		const listed = terminal.writes.filter(write => write.startsWith("Registry list:")).at(-1) as string;
		expect(listed).toContain("id=cockpit-lane-1");
		expect(listed).toContain("status=discovered");
		expect(listed).toContain("live=true");
		expect(listed).toContain("repo=/workspace/cockpit-repo");

		terminal.send("/registry inspect cockpit-lane-1");
		await eventually(
			() => (terminal.writes.some(write => write.startsWith("Registry inspect") && write.includes("cockpit-lane-1")) ? true : undefined),
			"registry inspect did not render the UDS row",
		);
		const inspected = terminal.writes.filter(write => write.startsWith("Registry inspect")).at(-1) as string;
		expect(inspected).toContain("id=cockpit-lane-1");
		expect(inspected).toContain('"repo":"/workspace/cockpit-repo"');
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("cockpit /lock force-release requires exact confirmation and renders the gateway refusal", async () => {
	const active = await gateway();
	const lease = acquireInDaemonLock(active.core, "cockpit-main", "cockpit-force-release-drill");
	const recorded = recordingClient(active.client);
	const terminal = controlledTerminal();
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, rpcConnect: async () => recorded.rpc, idempotencyKey: () => "cockpit-force-release", statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "cockpit did not begin reading input");
		terminal.send(`/lock force-release ${lease.leaseId}`);
		await eventually(
			() => (terminal.writes.some(write => write.includes(`CONFIRM FORCE-RELEASE ${lease.leaseId}`)) ? true : undefined),
			"force release did not request exact confirmation",
		);
		expect(recorded.calls.filter(call => call.method === "gitlock.force_release")).toHaveLength(0);

		terminal.send(`/lock force-release ${lease.leaseId} CONFIRM FORCE-RELEASE ${lease.leaseId}`);
		await eventually(
			() => (terminal.writes.some(write => write.includes("Git lock force-release refused: lock_holder_unverified")) ? true : undefined),
			"force release did not render the gateway refusal",
		);
		const calls = recorded.calls.filter(call => call.method === "gitlock.force_release");
		expect(calls).toHaveLength(1);
		expect(calls[0]?.params).toEqual({ lease_id: lease.leaseId, confirm: true, idempotency_key: "cockpit-force-release" });
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("console startup refuses an existing console delivery claim before owner input can be sent", async () => {
	const active = await gateway();
	const holder = await connectEventually(active.socketPath);
	const contender = await connectEventually(active.socketPath);
	const rendered = recordedOutput();
	try {
		const claim = await holder.request("consumer.claim", { consumer_id: "gajaeway-console", claim_ttl_ms: 5_000 });
		if (!claim.result) throw new Error(`consumer claim failed: ${JSON.stringify(claim)}`);
		const claimResult = claim.result as { claim_id: string; cursor: string };
		const consoleSurface = new OwnerConsole({ rpc: contender, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });
		expect(await consoleSurface.start()).toMatchObject({ accepted: false });
		expect(rendered.writes.join("")).toContain("Another gajaeway console currently owns");
		await expect(consoleSurface.submit("must not submit while delivery is elsewhere")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
		expect(active.fixture.commands()).toEqual([]);
		await holder.request("consumer.commit", { consumer_id: "gajaeway-console", claim_id: claimResult.claim_id, cursor: claimResult.cursor, proofs: [] });
	} finally {
		holder.close();
		contender.close();
	}
});

externalTest("console retention-gap startup refusal preserves its checkpoint and fences owner input", async () => {
	const active = await gateway();
	const rendered = recordedOutput();
	const gapRpc: JsonRpcClient = {
		async request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse> {
			if (method === "main.events.read") {
				return {
					jsonrpc: "2.0",
					id: 1,
					result: {
						events: [],
						next_cursor: "1:0",
						gap: { missing_from: "1:0", missing_to: "1:9", resync_cursor: "1:10" },
					},
				};
			}
			return await active.client.request(method, params, options);
		},
		close(): void {},
	};
	const consoleSurface = new OwnerConsole({ rpc: gapRpc, ownerSurfaceId: "owner", output: rendered.output, readWaitMs: 0 });

	expect(await consoleSurface.start()).toMatchObject({ accepted: false });
	expect(rendered.writes.join("")).toContain("behind journal retention");
	expect(rendered.writes.join("")).toContain("restart gajaeway console");
	expect(active.core.consumerCursor("gajaeway-console")).toBe("1:0");
	expect(active.core.consumerOutbox("gajaeway-console")).toEqual([]);
	await expect(consoleSurface.submit("must not submit without a recoverable checkpoint")).rejects.toBeInstanceOf(ConsoleDeliveryUnavailableError);
	expect(active.fixture.commands()).toEqual([]);
});

externalTest("console command loop renders the external-host gate limitation and help", async () => {
	const active = await gateway();
	const terminal = controlledTerminal();
	const gateId = "console-command-loop-unsupported-gate";
	const unsupportedFrame = `Gate ${gateId}: ${EXTERNAL_HOST_GATE_ANSWER_GUIDANCE}\n`;
	active.host.gates.observeOpen({ gateId, expectedSessionId: active.fixture.sessionId });
	const running = runWayConsole(
		{ stateDir: active.stateDirectory, profilePath: active.profilePath },
		[],
		{ terminal: terminal.terminal, statusPollMs: 25 },
	);
	try {
		await eventually(() => (terminal.isReading() ? true : undefined), "console did not begin reading owner input");
		terminal.send("/help");
		await eventually(
			() => (terminal.writes.some(write => write.includes(EXTERNAL_HOST_GATE_ANSWER_GUIDANCE)) ? true : undefined),
			"console help did not explain the external-host gate limitation",
		);
		const help = terminal.writes.find(write => write.startsWith("Gateway cockpit commands:\n"));
		if (!help) throw new Error("console did not render its help frame");
		expect(help).toContain("/gate <gate_id> <expected_session_id> <JSON answer> (capability probe; a future backend may support validated gate receipts)");
		expect(help).toContain(EXTERNAL_HOST_GATE_ANSWER_GUIDANCE);

		terminal.send(`/gate ${gateId} ${active.fixture.sessionId} {"selected":["Yes"]}`);
		await eventually(
			() => (terminal.writes.includes(unsupportedFrame) ? true : undefined),
			"console did not render the external-host unsupported-gate frame",
		);
		expect(terminal.writes).toContain(unsupportedFrame);
		expect(terminal.writes.join("")).not.toContain("Request failed: main.gate.answer returned an invalid response.");
		terminal.send("/quit");
		await running;
	} finally {
		terminal.terminal.close();
		await running.catch(() => undefined);
	}
}, 15_000);

externalTest("console gateway gate answers report unsupported and durably replay that broker limitation", async () => {
	const active = await gateway();
	active.host.gates.observeOpen({ gateId: "console-unsupported-gate", expectedSessionId: active.fixture.sessionId });
	const request = {
		gate_id: "console-unsupported-gate",
		expected_session_id: active.fixture.sessionId,
		answer: { selected: ["Yes"] },
		idempotency_key: "console-unsupported-gate-key",
	};
	const first = await active.client.request("main.gate.answer", request);
	const replay = await active.client.request("main.gate.answer", request);
	expect(first.result).toEqual({ accepted: false, gate_state: "unsupported" });
	expect(replay.result).toEqual(first.result);
	expect(
		active.core.idempotencyReplay({ scope: "main.gate.answer", key: request.idempotency_key, requestJson: canonicalJson(request) }),
	).toEqual(expect.objectContaining({ replayed: true, responseJson: canonicalJson({ accepted: false, gate_state: "unsupported" }) }));
});
