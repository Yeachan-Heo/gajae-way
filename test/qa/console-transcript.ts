/**
 * QA evidence generator (not a test): drives `gajaeway console` end to end against a real
 * in-process gateway over a real Unix socket and persists the complete terminal
 * write-stream as an app-automation transcript artifact.
 *
 * Run: bun test/qa/console-transcript.ts
 * Writes: artifacts/console-automation-transcript.json
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { RawConsoleTerminal, runWayConsole } from "../../src/console/console";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import { createMainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { FileSdkDouble } from "../helpers/main-session";

const repositoryRoot = path.resolve(import.meta.dir, "../..");

function ownerProfile(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`;
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 200; attempt += 1) {
		try {
			return await RpcClient.connect(socketPath);
		} catch (error) {
			lastError = error;
			await Bun.sleep(25);
		}
	}
	throw new Error(`gateway socket never accepted: ${String(lastError)}`);
}

async function waitForRendered(read: () => boolean, description: string, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (read()) return;
		await Bun.sleep(10);
	}
	throw new Error(description);
}

interface RecordedWrite {
	readonly sequence: number;
	readonly atMs: number;
	readonly raw: string;
	readonly containsEscape: boolean;
}

class TranscriptRawInput {
	readonly isTTY = true;
	isRaw = false;
	readonly #listeners = new Set<(chunk: string | Buffer) => void>();

	setEncoding(_encoding: BufferEncoding): void {}

	setRawMode(enabled: boolean): void {
		this.isRaw = enabled;
	}

	resume(): void {}

	pause(): void {}

	on(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.add(listener);
	}

	off(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.delete(listener);
	}

	send(chunk: string): void {
		for (const listener of [...this.#listeners]) listener(chunk);
	}
}

class TranscriptRawOutput {
	readonly isTTY = true;
	readonly columns = 100;
	readonly rows = 30;
	onWrite: ((text: string) => void) | undefined;

	write(text: string, callback: (error?: Error | null) => void): boolean {
		this.onWrite?.(text);
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

async function main(): Promise<void> {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-console-transcript-"));
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
	const host = createMainSessionHost({
		session: resumed.session,
		identity: resumed.identity,
		state,
		journal: core,
	});
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
	const probe = await connectEventually(socketPath);
	probe.close();

	const startedAt = Date.now();
	const writes: RecordedWrite[] = [];
	const scripted = ["hello from the automation transcript", "/quit"];
	const input = new TranscriptRawInput();
	const output = new TranscriptRawOutput();
	output.onWrite = (chunk) => {
		writes.push({
			sequence: writes.length,
			atMs: Date.now() - startedAt,
			raw: chunk,
			containsEscape: chunk.includes("\u001b"),
		});
	};
	const terminal = new RawConsoleTerminal({ input, output });
	const running = runWayConsole({ stateDir: stateDirectory, profilePath }, [], { terminal });
	await waitForRendered(
		() => writes.some((write) => write.raw.includes("gajaeway> ")),
		"console did not render its dedicated TUI editor",
	);
	input.send(`${scripted[0]}\n`);
	await waitForRendered(
		() => writes.some((write) => /Delivered as: (prompt|steer|follow_up)/.test(write.raw)),
		"console did not render the gateway-derived delivered_as frame before the assistant reply",
	);
	await waitForRendered(
		() => writes.some((write) => write.raw.includes("Assistant:")),
		"console did not stream the finalized assistant reply after acceptance",
	);
	input.send(`${scripted[1]}\n`);
	await running;

	const rendered = writes.map((w) => w.raw).join("");
	const visible = visibleTerminalText(rendered);
	const deliveredAsRendered = /Delivered as: (prompt|steer|follow_up)/.test(rendered);
	if (!deliveredAsRendered) throw new Error("console transcript is missing the gateway-derived delivered_as frame.");
	const cursorAfter = core.consumerCursor("gajaeway-console");
	const stamp = (): string => new Date().toISOString();
	const actions = [
		{
			ordinal: 1,
			timestamp: stamp(),
			selector: "process:gajaeway console",
			type: "launch",
			detail: "RawConsoleTerminal TUI driving runWayConsole against the live gateway Unix socket",
			result: "console started",
		},
		{
			ordinal: 2,
			timestamp: stamp(),
			selector: 'stdout:"Gateway status"',
			type: "observe",
			detail: "gateway status block (daemon/main/journal/lock/reconcile)",
			result: visible.includes("Gateway status") ? "rendered" : "absent",
		},
		{
			ordinal: 3,
			timestamp: stamp(),
			selector: 'stdout:"Owner console ready"',
			type: "observe",
			detail: "readiness announcement after delivery readiness established",
			result: visible.includes("Owner console ready") ? "rendered" : "absent",
		},
		{
			ordinal: 4,
			timestamp: stamp(),
			selector: "stdin:gajaeway> prompt",
			type: "input",
			detail: scripted[0],
			result: "submitted through main.submit with a fresh idempotency key",
		},
		{
			ordinal: 5,
			timestamp: stamp(),
			selector: 'stdout:"Delivered as:"',
			type: "observe",
			detail: "server-derived delivered_as rendered verbatim",
			result: /Delivered as: (prompt|steer|follow_up)/.exec(rendered)?.[0] ?? "absent",
		},
		{
			ordinal: 6,
			timestamp: stamp(),
			selector: 'stdout:"Assistant:"',
			type: "observe",
			detail: "finalized assistant reply rendered asynchronously after delivered_as acceptance",
			result: visible.includes("Assistant:") ? "rendered" : "absent",
		},
		{
			ordinal: 7,
			timestamp: stamp(),
			selector: "stdin:gajaeway> prompt",
			type: "input",
			detail: scripted[1],
			result: "console exited cleanly",
		},
		{
			ordinal: 8,
			timestamp: stamp(),
			selector: 'rpc:consumerCursor("gajaeway-console")',
			type: "observe",
			detail: "gajaeway-console consumer checkpoint after the session",
			result: cursorAfter,
		},
	] as const;

	const transcript = {
		schemaVersion: 1,
		kind: "app-automation-transcript",
		tool: "bun (virtual raw-TTY TUI harness: test/qa/console-transcript.ts)",
		surface: "cli",
		surfaceNote:
			"gajaeway console is a subcommand of the gajaeway CLI that renders to a terminal; the transcript records its terminal write-stream.",
		subject: "gajaeway console (gajaeway local owner surface)",
		producedBy: "test/qa/console-transcript.ts",
		generatedAt: new Date().toISOString(),
		harness:
			"RawConsoleTerminal driven through a virtual TTY against a real in-process gateway over a real Unix-domain socket",
		scriptedInput: scripted,
		actions,
		steps: writes,
		renderedOutput: rendered,
		observations: {
			writeCount: writes.length,
			renderedBytes: rendered.length,
			statusRendered: visible.includes("Gateway status"),
			readyLineRendered: visible.includes("Owner console ready"),
			promptRendered: visible.includes("gajaeway>"),
			deliveredAsRendered,
			assistantReplyRendered: visible.includes("Assistant:"),
			alternateScreenEntered: rendered.includes("\x1b[?1049h"),
			alternateScreenExited: rendered.includes("\x1b[?1049l"),
			consumerCursorAfter: cursorAfter,
		},
	};

	fs.writeFileSync(
		path.join(repositoryRoot, "artifacts", "console-automation-transcript.json"),
		`${JSON.stringify(transcript, null, 2)}\n`,
	);

	core.shutdownRpcServer();
	await host.dispose();
	fs.rmSync(root, { force: true, recursive: true });

	console.log(JSON.stringify(transcript.observations, null, 2));
}

await main();
