import { lstat, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as net from "node:net";

type CommandResult = {
	args: string[];
	exitCode: number;
	stdout: string;
	stderr: string;
	elapsedMs: number;
};

type JsonRecord = Record<string, unknown>;

const gjcPath = Bun.which("gjc");
if (!gjcPath) {
	throw new Error("gjc was not found on PATH");
}
const gjc: string = gjcPath;

const root = await mkdtemp(join(tmpdir(), "gajaeway-gjc-spike-"));
const sessions = join(root, "sessions");
const sdkAgent = join(root, "sdk-agent");
const repo = join(root, "repo");
const sdkRepo = join(root, "sdk-repo");
const realSessions = join(root, "credentialed-sessions");
const socketPath = join(root, "s.sock");
await Promise.all([mkdir(sessions), mkdir(realSessions), mkdir(sdkAgent), mkdir(repo), mkdir(sdkRepo)]);

const sanitizedEnvironment = {
	PATH: process.env.PATH ?? "",
	HOME: process.env.HOME ?? root,
	TERM: "dumb",
	LANG: "C",
	LC_ALL: "C",
	// This is read-only configuration lookup. Session persistence is always
	// overridden by --session-dir below.
	GJC_CODING_AGENT_DIR: process.env.GJC_CODING_AGENT_DIR ?? join(process.env.HOME ?? root, ".gjc", "agent"),
};

// The real-turn probe preserves the host's credential environment without
// copying any credential or configuration file into root. SDK probes use the
// narrower environment below and remain completely scratch-rooted.
const credentialedEnvironment = {
	...process.env,
	TERM: "dumb",
	LANG: "C",
	LC_ALL: "C",
	GJC_CODING_AGENT_DIR: process.env.GJC_CODING_AGENT_DIR ?? join(process.env.HOME ?? root, ".gjc", "agent"),
};
const sdkEnvironment = {
	...sanitizedEnvironment,
	GJC_AGENT_DIR: sdkAgent,
	GJC_CODING_AGENT_DIR: sdkAgent,
};

// Stage 0e retains the parent process environment for runtime wiring and
// provider authentication while forcing all GJC state into the temporary agent
// directory. Values are never rendered, logged, or persisted.
const sdkInheritedEnvironment: NodeJS.ProcessEnv = {
	...credentialedEnvironment,
	GJC_AGENT_DIR: sdkAgent,
	GJC_CODING_AGENT_DIR: sdkAgent,
};

async function run(args: string[], cwd = repo, env: NodeJS.ProcessEnv = sanitizedEnvironment): Promise<CommandResult> {
	const started = performance.now();
	const proc = Bun.spawn([gjc, ...args], {
		cwd,
		env,
		stdout: "pipe",
		stderr: "pipe",
	});
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);
	return { args, exitCode, stdout, stderr, elapsedMs: Math.round(performance.now() - started) };
}

function pipedText(stream: unknown): Promise<string> {
	if (!(stream instanceof ReadableStream)) throw new Error("expected a piped subprocess stream");
	return new Response(stream as ReadableStream<Uint8Array>).text();
}

async function runSdkCreate(
	args: string[],
	env: NodeJS.ProcessEnv = sdkEnvironment,
): Promise<{ result: CommandResult; attempts: CommandResult[] }> {
	const attempts: CommandResult[] = [];
	for (let attempt = 0; attempt < 5; attempt++) {
		const result = await run(args, sdkRepo, env);
		attempts.push(result);
		if (result.exitCode === 0) return { result, attempts };
		if (attempt < 4) await Bun.sleep(1_000);
	}
	return { result: attempts.at(-1)!, attempts };
}

function sessionId(output: string): string {
	for (const line of output.split("\n")) {
		if (!line) continue;
		const frame = JSON.parse(line) as { type?: string; id?: string };
		if (frame.type === "session" && typeof frame.id === "string") return frame.id;
	}
	throw new Error("JSON output did not contain a session frame with an id");
}

function asRecord(value: unknown): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : undefined;
}

function jsonOutput(result: CommandResult): JsonRecord {
	try {
		const parsed = JSON.parse(result.stdout) as unknown;
		const record = asRecord(parsed);
		if (record) return record;
	} catch {
		// Preserve malformed command output as evidence rather than masking it.
	}
	return { raw: result.stdout, stderr: result.stderr };
}

function lifecycleSessionId(result: CommandResult): string {
	const response = jsonOutput(result);
	const payload = asRecord(response.result);
	if (typeof payload?.sessionId !== "string")
		throw new Error(`SDK lifecycle create did not return a sessionId: ${result.stdout || result.stderr}`);
	return payload.sessionId;
}

function renderCommandArgs(args: readonly string[]): string[] {
	return args.map((arg, index) => {
		const previous = args[index - 1];
		if (previous === "--cursor") return "<redacted-cursor>";
		if (previous === "--text" && arg.length > 256) return `<text:${Buffer.byteLength(arg)}-bytes>`;
		return arg;
	});
}

function compact(result: CommandResult): Record<string, unknown> {
	return {
		command: ["gjc", ...renderCommandArgs(result.args)].join(" "),
		exitCode: result.exitCode,
		elapsedMs: result.elapsedMs,
		stdout: result.stdout.slice(0, 700),
		stderr: result.stderr.slice(0, 700),
	};
}

function compactValue(value: unknown): unknown {
	try {
		const rendered = JSON.stringify(value);
		if (rendered === undefined || rendered.length <= 600) return value;
		return { truncated: true, preview: rendered.slice(0, 600) };
	} catch {
		return { unserializable: true };
	}
}

function summarizeTail(result: CommandResult): Record<string, unknown> {
	const response = jsonOutput(result);
	const payload = asRecord(response.result);
	const items = Array.isArray(payload?.items) ? payload.items : [];
	return {
		exitCode: result.exitCode,
		ok: response.ok,
		error: response.error,
		checkpoint: payload?.checkpoint,
		gap: payload?.gap,
		terminal: payload?.terminal,
		items: items.map(item => {
			const record = asRecord(item);
			return {
				kind: record?.kind,
				id: record?.id,
				generation: record?.generation,
				seq: record?.seq,
				payload: compactValue(record?.payload),
			};
		}),
	};
}

function summarizeTailVocabulary(result: CommandResult): Record<string, unknown> {
	const response = jsonOutput(result);
	const payload = asRecord(response.result);
	const items = Array.isArray(payload?.items) ? payload.items : [];
	const records = items.map(item => asRecord(item));
	const assistantText = records.flatMap(record => {
		const itemPayload = asRecord(record?.payload);
		return itemPayload?.role === "assistant" ? textBlocks(itemPayload.content) : [];
	});
	const kindSequence = records.map(record => record?.kind).filter((kind): kind is string => typeof kind === "string");
	const nestedEventTerms = (value: unknown, depth = 0): string[] => {
		if (depth >= 4) return [];
		const eventPayload = asRecord(value);
		if (!eventPayload) return [];
		const terms = [eventPayload.kind, eventPayload.type].filter(
			(term): term is string => typeof term === "string" && term !== "event",
		);
		return [...terms, ...nestedEventTerms(eventPayload.payload, depth + 1)];
	};
	const eventTypeSequence = records.flatMap(record => (record?.kind === "event" ? nestedEventTerms(record.payload) : []));
	return {
		exitCode: result.exitCode,
		ok: response.ok,
		error: response.error,
		terminal: payload?.terminal,
		kindSequence,
		kinds: [...new Set(kindSequence)],
		eventTypeSequence,
		eventTypes: [...new Set(eventTypeSequence)],
		assistantText: [...new Set(assistantText)].map(text => text.slice(0, 500)),
	};
}
type ScratchCheckpointCursor = {
	token: string;
	checkpoint: JsonRecord;
	metadata: Record<string, unknown>;
};

async function requestScratchEndpoint(sessionId: string, frame: JsonRecord): Promise<JsonRecord> {
	const endpointPath = join(sdkRepo, ".gjc", "state", "sdk", `${sessionId}.json`);
	const endpoint = asRecord(JSON.parse(await readFile(endpointPath, "utf8")) as unknown);
	const url = endpoint?.url;
	const token = endpoint?.token;
	if (typeof url !== "string" || typeof token !== "string")
		throw new Error("Scratch SDK endpoint did not expose a usable local connection record.");

	const id = `p2b-${Date.now()}-${Math.random().toString(36).slice(2)}`;
	const socket = new WebSocket(`${url}?token=${encodeURIComponent(token)}`);
	try {
		return await new Promise<JsonRecord>((resolve, reject) => {
			let sent = false;
			const timeout = setTimeout(() => reject(new Error("Scratch SDK endpoint request timed out.")), 5_000);
			const finish = (callback: () => void) => {
				clearTimeout(timeout);
				callback();
			};
			socket.addEventListener("message", event => {
				let incoming: JsonRecord | undefined;
				try {
					incoming = asRecord(JSON.parse(String(event.data)) as unknown);
				} catch {
					return;
				}
				if (!incoming) return;
				if (!sent && incoming.type === "hello") {
					sent = true;
					socket.send(JSON.stringify({ ...frame, id }));
					return;
				}
				if (incoming.id === id) finish(() => resolve(incoming!));
			});
			socket.addEventListener("error", () => finish(() => reject(new Error("Scratch SDK endpoint connection failed."))));
		});
	} finally {
		socket.close();
	}
}

function checkpointCursor(response: JsonRecord): ScratchCheckpointCursor {
	const result = asRecord(response.result);
	const checkpoint = asRecord(result?.checkpoint);
	const token = result?.checkpointToken;
	if (!checkpoint || typeof token !== "string" || token.length === 0)
		throw new Error("Scratch SDK checkpoint did not return a signed cursor token.");
	return {
		token,
		checkpoint,
		metadata: {
			encoding: "opaque signed checkpoint token",
			byteLength: Buffer.byteLength(token),
			checkpoint,
		},
	};
}

function retentionResync(result: CommandResult): JsonRecord | undefined {
	const response = jsonOutput(result);
	const payload = asRecord(response.result);
	const error = asRecord(response.error);
	return asRecord(asRecord(error?.details)?.resync) ?? asRecord(asRecord(payload?.gap)?.resync);
}

function summarizeEndpointReplay(response: JsonRecord): Record<string, unknown> {
	const events = Array.isArray(response.events) ? response.events : [];
	return {
		type: response.type,
		ok: response.ok,
		gap: response.gap,
		generation: response.generation,
		lastSeq: response.lastSeq,
		events: events.map(event => {
			const record = asRecord(event);
			return { kind: record?.kind, generation: record?.generation, seq: record?.seq };
		}),
	};
}

function textBlocks(value: unknown): string[] {
	if (typeof value === "string") return [value];
	if (!Array.isArray(value)) return [];
	return value.flatMap(content => {
		const item = asRecord(content);
		return typeof item?.text === "string" ? [item.text] : [];
	});
}

function summarizeCredentialedTurn(result: CommandResult): Record<string, unknown> {
	const frames = result.stdout
		.split("\n")
		.flatMap(line => {
			if (line.trim() === "") return [];
			try {
				const parsed = asRecord(JSON.parse(line) as unknown);
				return parsed ? [parsed] : [];
			} catch {
				return [];
			}
		});
	const assistantMessages = frames.flatMap(frame => {
		if (frame.type === "message_end") {
			const message = asRecord(frame.message);
			return message?.role === "assistant" ? textBlocks(message.content) : [];
		}
		if (frame.type !== "agent_end" || !Array.isArray(frame.messages)) return [];
		return frame.messages.flatMap(messageValue => {
			const message = asRecord(messageValue);
			return message?.role === "assistant" ? textBlocks(message.content) : [];
		});
	});
	return {
		exitCode: result.exitCode,
		types: frames.map(frame => frame.type).filter((type): type is string => typeof type === "string"),
		assistantText: [...new Set(assistantMessages)],
		stderr: result.stderr.slice(0, 700),
	};
}


async function waitForBroker(): Promise<JsonRecord> {
	const discoveryPath = join(sdkAgent, "sdk", "broker.json");
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		try {
			const discovery = asRecord(JSON.parse(await readFile(discoveryPath, "utf8")) as unknown);
			if (discovery) return discovery;
		} catch {
			// The broker publishes discovery asynchronously.
		}
		await Bun.sleep(50);
	}
	throw new Error("SDK broker did not publish discovery within 10 seconds");
}

async function waitForSocket(path: string): Promise<{ mode: number }> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		try {
			const stat = await lstat(path);
			if (stat.isSocket()) return { mode: stat.mode & 0o777 };
		} catch {
			// The relay has not bound its path yet.
		}
		await Bun.sleep(50);
	}
	throw new Error("SDK relay did not bind a Unix socket within 10 seconds");
}

async function waitForSocketRemoval(path: string): Promise<boolean> {
	const deadline = Date.now() + 5_000;
	while (Date.now() < deadline) {
		try {
			await lstat(path);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
			throw error;
		}
		await Bun.sleep(50);
	}
	return false;
}

async function probeSocketConnection(path: string): Promise<string> {
	return await new Promise<string>((resolve, reject) => {
		const socket = net.createConnection(path);
		let received = "";
		let settled = false;
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			callback();
		};
		const complete = () => {
			if (received.includes('"code":"auth_failed"')) finish(() => resolve(received));
			else finish(() => reject(new Error(`SDK relay did not return its authentication failure frame: ${received}`)));
		};
		const timer = setTimeout(() => {
			socket.destroy();
			finish(() => reject(new Error("SDK relay socket authentication probe timed out")));
		}, 1_000);
		socket.setEncoding("utf8");
		socket.once("connect", () => socket.write("gjc-sdk-transport/1 token=invalid\n"));
		socket.on("data", chunk => {
			received += String(chunk);
		});
		socket.once("end", complete);
		socket.once("close", complete);
		socket.once("error", error => finish(() => reject(error)));
	});
}

/**
 * Stage 0c is deliberately opt-in: it may touch the credentialed SDK profile
 * only through one fresh lifecycle session, while its workspace stays under the
 * throwaway root. The normal P2b harness remains fully scratch-rooted.
 */
async function runStage0cProfileProbe(): Promise<Record<string, unknown>> {
	const profileAgent = process.env.GJC_CODING_AGENT_DIR;
	const stageRepo = join(root, "stage0c-profile-sdk-repo");
	const stageEvidence: Record<string, unknown> = {
		mode: "fresh scratch workspace with installed credentialed SDK profile",
		profileAgent: "<installed credentialed agent dir>",
		modelSetIssued: false,
	};
	if (!profileAgent) {
		stageEvidence.skipped = "GJC_CODING_AGENT_DIR was unavailable; profile access was not guessed.";
		return stageEvidence;
	}

	await mkdir(stageRepo);
	const profileEnvironment = {
		...credentialedEnvironment,
		GJC_AGENT_DIR: profileAgent,
		GJC_CODING_AGENT_DIR: profileAgent,
	};
	let stageSessionId: string | undefined;
	try {
		const create = await run(
			[
				"sdk",
				"session",
				"raw",
				"global",
				"--agent-dir",
				profileAgent,
				"--op",
				"session.create",
				"--idempotency-key",
				`stage0c-profile-create-${crypto.randomUUID()}`,
				"--json-input",
				JSON.stringify({ cwd: stageRepo }),
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.create = compact(create);
		const createPayload = asRecord(jsonOutput(create).result);
		if (typeof createPayload?.sessionId !== "string") {
			stageEvidence.compaction = {
				status: "inconclusive",
				reason: "No fresh SDK session ID was issued, so no session control was attempted.",
			};
			return stageEvidence;
		}
		stageSessionId = createPayload.sessionId;

		const sent = await run(
			[
				"sdk",
				"session",
				"send",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--text",
				"Reply with exactly: STAGE0C_SDK_SUCCESS",
				"--op-ref",
				`stage0c-profile-send-${crypto.randomUUID()}`,
				"--wait",
				"--timeout-ms",
				"90000",
			],
			stageRepo,
			profileEnvironment,
		);
		const initialTail = await run(
			[
				"sdk",
				"session",
				"tail",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				"15000",
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.send = compact(sent);
		stageEvidence.tailAfterSend = summarizeTail(initialTail);
		const sentPayload = asRecord(jsonOutput(sent).result);
		if (sent.exitCode !== 0 || sentPayload?.status !== "terminal_ok") {
			stageEvidence.compaction = {
				status: "inconclusive",
				reason: "The fresh SDK-hosted turn did not terminally succeed; compaction was not sent to an unavailable session.",
			};
			return stageEvidence;
		}

		const filler = "STAGE0C_COMPACTION_FILLER ".repeat(1_200);
		const fills: CommandResult[] = [];
		for (let index = 0; index < 4; index++) {
			const fill = await run(
				[
					"sdk",
					"session",
					"send",
					stageSessionId,
					"--agent-dir",
					profileAgent,
					"--text",
					filler,
					"--op-ref",
					`stage0c-compaction-fill-${index + 1}-${crypto.randomUUID()}`,
					"--wait",
					"--timeout-ms",
					"90000",
				],
				stageRepo,
				profileEnvironment,
			);
			fills.push(fill);
			const fillPayload = asRecord(jsonOutput(fill).result);
			if (fill.exitCode !== 0 || fillPayload?.status !== "terminal_ok") break;
		}
		stageEvidence.compactionFills = fills.map(compact);
		if (fills.length !== 4 || fills.some(fill => asRecord(jsonOutput(fill).result)?.status !== "terminal_ok")) {
			stageEvidence.compaction = {
				status: "inconclusive",
				reason: "A bounded filler turn did not terminally succeed; compaction was not sent after partial history.",
			};
			return stageEvidence;
		}

		const compaction = await run(
			[
				"sdk",
				"session",
				"raw",
				"control",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--op",
				"compaction.run",
				"--json-input",
				"{}",
				"--timeout-ms",
				"90000",
			],
			stageRepo,
			profileEnvironment,
		);
		const compactionTail = await run(
			[
				"sdk",
				"session",
				"tail",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				"15000",
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.compaction = {
			control: compact(compaction),
			tail: summarizeTail(compactionTail),
		};
	} catch (error) {
		stageEvidence.error = error instanceof Error ? error.message : String(error);
	} finally {
		if (stageSessionId) {
			const closed = await run(
				[
					"sdk",
					"session",
					"raw",
					"global",
					"--agent-dir",
					profileAgent,
					"--op",
					"session.close",
					"--idempotency-key",
					`stage0c-profile-close-${crypto.randomUUID()}`,
					"--json-input",
					JSON.stringify({ sessionId: stageSessionId, cwd: stageRepo }),
				],
				stageRepo,
				profileEnvironment,
			);
			stageEvidence.close = compact(closed);
		}
	}
	return stageEvidence;
}

/**
 * Stage 0d tests the proposed resident relay path. `sdk serve` is owned as a
 * foreground child and its socket must pass the same invalid-auth readiness
 * exchange as the baseline before any profile-backed send is attempted.
 */
async function runStage0dProfileServeProbe(): Promise<Record<string, unknown>> {
	const profileAgent = process.env.GJC_CODING_AGENT_DIR;
	const stageRepo = join(root, "stage0d-profile-sdk-repo");
	const stageSocket = join(root, "stage0d-host.sock");
	const stageEvidence: Record<string, unknown> = {
		mode: "fresh profile-backed lifecycle session with owned sdk serve relay",
		profileAgent: "<installed credentialed agent dir>",
		workspace: "<temporary>/stage0d-profile-sdk-repo",
		serveSocket: "<temporary>/stage0d-host.sock",
		modelSetIssued: false,
	};
	if (!profileAgent) {
		stageEvidence.skipped = "GJC_CODING_AGENT_DIR was unavailable; profile access was not guessed.";
		return stageEvidence;
	}

	await mkdir(stageRepo);
	const profileEnvironment = {
		...credentialedEnvironment,
		GJC_AGENT_DIR: profileAgent,
		GJC_CODING_AGENT_DIR: profileAgent,
	};
	let stageSessionId: string | undefined;
	let stageServe: ReturnType<typeof Bun.spawn> | undefined;
	let stageServeStdout: Promise<string> | undefined;
	let stageServeStderr: Promise<string> | undefined;

	const pollPromptStatus = async (
		opRef: string,
		until: "active" | "terminal",
		timeoutMs: number,
	): Promise<Record<string, unknown>> => {
		const observations: Array<Record<string, unknown>> = [];
		const deadline = Date.now() + timeoutMs;
		while (Date.now() < deadline) {
			const status = await run(
				[
					"sdk",
					"session",
					"status",
					stageSessionId!,
					opRef,
					"--agent-dir",
					profileAgent,
					"--timeout-ms",
					"3000",
				],
				stageRepo,
				profileEnvironment,
			);
			const response = jsonOutput(status);
			const statusBody = asRecord(asRecord(response.result)?.status);
			const value = statusBody?.status;
			observations.push({ exitCode: status.exitCode, status: value, error: response.error });
			if (status.exitCode !== 0) return { outcome: "unavailable", observations };
			if (value === "terminal_ok" || value === "failed") return { outcome: "terminal", status: value, observations };
			if (value === "unknown") return { outcome: "unknown", observations };
			if (until === "active" && (value === "accepted" || value === "in_flight"))
				return { outcome: "active", status: value, observations };
			await Bun.sleep(250);
		}
		return { outcome: "timeout", observations };
	};

	try {
		const created = await run(
			[
				"sdk",
				"session",
				"raw",
				"global",
				"--agent-dir",
				profileAgent,
				"--op",
				"session.create",
				"--idempotency-key",
				`stage0d-profile-create-${crypto.randomUUID()}`,
				"--json-input",
				JSON.stringify({ cwd: stageRepo }),
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.create = compact(created);
		const createdPayload = asRecord(jsonOutput(created).result);
		if (typeof createdPayload?.sessionId !== "string") {
			stageEvidence.result = "No lifecycle session ID was issued; serve and send were not attempted.";
			return stageEvidence;
		}
		stageSessionId = createdPayload.sessionId;

		const serveEvidence: Record<string, unknown> = {
			command: "gjc sdk serve --socket <temporary>/stage0d-host.sock --session <stage0d-session>",
		};
		stageEvidence.serve = serveEvidence;
		stageServe = Bun.spawn([gjc, "sdk", "serve", "--socket", stageSocket, "--session", stageSessionId], {
			cwd: stageRepo,
			env: profileEnvironment,
			stdout: "pipe",
			stderr: "pipe",
		});
		stageServeStdout = pipedText(stageServe.stdout);
		stageServeStderr = pipedText(stageServe.stderr);
		const readiness = await Promise.race([
			waitForSocket(stageSocket).then(
				socket => ({ kind: "socket" as const, socket }),
				error => ({ kind: "socket_error" as const, error: error instanceof Error ? error.message : String(error) }),
			),
			stageServe.exited.then(exitCode => ({ kind: "exited" as const, exitCode })),
		]);
		serveEvidence.readiness = readiness;
		if (readiness.kind !== "socket") {
			stageEvidence.send = { skipped: "The owned sdk serve relay did not become ready." };
			return stageEvidence;
		}
		try {
			serveEvidence.authProbe = (await probeSocketConnection(stageSocket)).trim();
		} catch (error) {
			serveEvidence.authProbeError = error instanceof Error ? error.message : String(error);
			stageEvidence.send = { skipped: "The relay socket did not pass the invalid-auth readiness probe." };
			return stageEvidence;
		}

		const sent = await run(
			[
				"sdk",
				"session",
				"send",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--text",
				"Reply with exactly: STAGE0D_SDK_SUCCESS",
				"--op-ref",
				`stage0d-profile-send-${crypto.randomUUID()}`,
				"--wait",
				"--timeout-ms",
				"90000",
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.send = compact(sent);
		const sentPayload = asRecord(jsonOutput(sent).result);
		if (sent.exitCode !== 0 || sentPayload?.status !== "terminal_ok") {
			stageEvidence.socketContext = {
				supported: false,
				reason: "gjc sdk session exposes no --socket flag; the installed CLI has no socket-context send verb.",
			};
			stageEvidence.result = "The send did not terminally succeed with the owned relay ready; no additional operation was submitted.";
			return stageEvidence;
		}

		const initialTail = await run(
			[
				"sdk",
				"session",
				"tail",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				"15000",
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.initialTail = {
			summary: summarizeTail(initialTail),
			vocabulary: summarizeTailVocabulary(initialTail),
		};

		const steerOpRef = `stage0d-steer-seed-${crypto.randomUUID()}`;
		const steerSeed = await run(
			[
				"sdk",
				"session",
				"send",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--text",
				"Without using tools, write about 900 words explaining why independent capability probes must prove cleanup. End with STAGE0D_STEER_BASELINE.",
				"--op-ref",
				steerOpRef,
			],
			stageRepo,
			profileEnvironment,
		);
		const steerEvidence: Record<string, unknown> = { seed: compact(steerSeed) };
		stageEvidence.steer = steerEvidence;
		const steerSeedPayload = asRecord(jsonOutput(steerSeed).result);
		if (steerSeed.exitCode !== 0 || !["accepted", "in_flight"].includes(String(steerSeedPayload?.status))) {
			steerEvidence.skipped = "The longer prompt was not accepted; no steering control was sent.";
			stageEvidence.compaction = { skipped: "The mid-turn probe did not leave a safely reconcilable state." };
			return stageEvidence;
		}
		const activeStatus = await pollPromptStatus(steerOpRef, "active", 5_000);
		steerEvidence.activeStatus = activeStatus;
		let terminalStatus: Record<string, unknown> | undefined;
		if (activeStatus.outcome === "active") {
			const steering = await run(
				[
					"sdk",
					"session",
					"raw",
					"control",
					stageSessionId,
					"--agent-dir",
					profileAgent,
					"--op",
					"turn.steer",
					"--json-input",
					JSON.stringify({
						text: "For this probe, make your final line exactly STAGE0D_STEER_ECHO.",
						clientRef: `stage0d-steer-${crypto.randomUUID()}`,
					}),
				],
				stageRepo,
				profileEnvironment,
			);
			steerEvidence.control = compact(steering);
			terminalStatus = await pollPromptStatus(steerOpRef, "terminal", 90_000);
		} else if (activeStatus.outcome === "terminal") {
			steerEvidence.skipped = "The longer prompt reached a terminal state before steering could be sent.";
			terminalStatus = activeStatus;
		} else {
			steerEvidence.skipped = "The longer prompt could not be safely classified as active or terminal.";
			stageEvidence.compaction = { skipped: "The mid-turn probe remained uncertain." };
			return stageEvidence;
		}
		steerEvidence.terminalStatus = terminalStatus;
		if (terminalStatus?.outcome !== "terminal") {
			stageEvidence.compaction = { skipped: "The mid-turn probe did not reach a terminal status inside its bounded wait." };
			return stageEvidence;
		}

		const filler = "STAGE0D_COMPACTION_FILLER ".repeat(1_200);
		const fills: CommandResult[] = [];
		for (let index = 0; index < 4; index++) {
			const fill = await run(
				[
					"sdk",
					"session",
					"send",
					stageSessionId,
					"--agent-dir",
					profileAgent,
					"--text",
					filler,
					"--op-ref",
					`stage0d-compaction-fill-${index + 1}-${crypto.randomUUID()}`,
					"--wait",
					"--timeout-ms",
					"90000",
				],
				stageRepo,
				profileEnvironment,
			);
			fills.push(fill);
			const fillPayload = asRecord(jsonOutput(fill).result);
			if (fill.exitCode !== 0 || fillPayload?.status !== "terminal_ok") break;
		}
		stageEvidence.compactionFills = fills.map(compact);
		if (fills.length !== 4 || fills.some(fill => asRecord(jsonOutput(fill).result)?.status !== "terminal_ok")) {
			stageEvidence.compaction = { skipped: "A bounded filler turn did not terminally succeed." };
			return stageEvidence;
		}

		const compaction = await run(
			[
				"sdk",
				"session",
				"raw",
				"control",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--op",
				"compaction.run",
				"--json-input",
				"{}",
				"--timeout-ms",
				"90000",
			],
			stageRepo,
			profileEnvironment,
		);
		const compactionTail = await run(
			[
				"sdk",
				"session",
				"tail",
				stageSessionId,
				"--agent-dir",
				profileAgent,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				"15000",
			],
			stageRepo,
			profileEnvironment,
		);
		stageEvidence.compaction = {
			control: compact(compaction),
			tail: summarizeTail(compactionTail),
			vocabulary: summarizeTailVocabulary(compactionTail),
		};
	} catch (error) {
		stageEvidence.error = error instanceof Error ? error.message : String(error);
	} finally {
		if (stageSessionId) {
			try {
				const closed = await run(
					[
						"sdk",
						"session",
						"raw",
						"global",
						"--agent-dir",
						profileAgent,
						"--op",
						"session.close",
						"--idempotency-key",
						`stage0d-profile-close-${crypto.randomUUID()}`,
						"--json-input",
						JSON.stringify({ sessionId: stageSessionId, cwd: stageRepo }),
					],
					stageRepo,
					profileEnvironment,
				);
				stageEvidence.close = compact(closed);
			} catch (error) {
				stageEvidence.closeError = error instanceof Error ? error.message : String(error);
			}
		}
		if (stageServe) {
			try {
				stageServe.kill("SIGTERM");
			} catch {
				// The foreground relay may already have exited after an endpoint failure.
			}
			const [exitCode, stdout, stderr] = await Promise.all([
				stageServe.exited,
				stageServeStdout ?? Promise.resolve(""),
				stageServeStderr ?? Promise.resolve(""),
			]);
			stageEvidence.serveCleanup = {
				exitCode,
				stdout: stdout.slice(0, 700),
				stderr: stderr.slice(0, 700),
				socketRemoved: await waitForSocketRemoval(stageSocket),
			};
		}
	}
	return stageEvidence;
}

/**
 * Stage 0e isolates runtime state while retaining only the parent process
 * environment for provider authentication. It never reads or copies credential
 * files or renders environment values; its one explicit scratch provider shape
 * contains no credential material and is removed with the temporary root.
 */
async function runStage0eInheritedEnvironmentProbe(): Promise<Record<string, unknown>> {
	const modelId = "layofflabs-anthropic/claude-opus-5";
	const providerKeys = ["OPENAI_API_KEY"].filter(key => typeof process.env[key] === "string" && process.env[key] !== "");
	const stageEvidence: Record<string, unknown> = {
		mode: "scratch broker and session state with inherited provider environment",
		stateRoot: "<temporary>/sdk-agent",
		workspace: "<temporary>/sdk-repo",
		providerEnvironment: { presentKeys: providerKeys, valuesRendered: false, valuesPersisted: false },
		model: {
			id: modelId,
			thinkingLevel: "medium",
			source: "parent modelRoles.default and parent gjc --list-models",
			providerDefinition: "minimal scratch LayoffLabs Anthropic Messages shape; key reference only",
		},
	};
	if (providerKeys.length === 0) {
		stageEvidence.skipped = "No supported parent provider API-key variable was present; no credential source was guessed.";
		return stageEvidence;
	}

	let stageBroker: ReturnType<typeof Bun.spawn> | undefined;
	let stageBrokerStderr: Promise<string> | undefined;
	let stageSessionId: string | undefined;
	const tail = async (timeoutMs = 30_000): Promise<CommandResult> =>
		await run(
			[
				"sdk",
				"session",
				"tail",
				stageSessionId!,
				"--agent-dir",
				sdkAgent,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				String(timeoutMs),
			],
			sdkRepo,
			sdkInheritedEnvironment,
		);
	const control = async (operation: string, input: JsonRecord, timeoutMs = 30_000): Promise<CommandResult> =>
		await run(
			[
				"sdk",
				"session",
				"raw",
				"control",
				stageSessionId!,
				"--agent-dir",
				sdkAgent,
				"--op",
				operation,
				"--json-input",
				JSON.stringify(input),
				"--timeout-ms",
				String(timeoutMs),
			],
			sdkRepo,
			sdkInheritedEnvironment,
		);
	const poll = async (
		opRef: string,
		mode: "in_flight" | "terminal",
		timeoutMs: number,
	): Promise<Record<string, unknown>> => {
		const observations: Array<Record<string, unknown>> = [];
		const deadline = Date.now() + timeoutMs;
		while (Date.now() < deadline) {
			const status = await run(
				[
					"sdk",
					"session",
					"status",
					stageSessionId!,
					opRef,
					"--agent-dir",
					sdkAgent,
					"--timeout-ms",
					"3000",
				],
				sdkRepo,
				sdkInheritedEnvironment,
			);
			const response = jsonOutput(status);
			const statusBody = asRecord(asRecord(response.result)?.status);
			const value = statusBody?.status;
			observations.push({ exitCode: status.exitCode, status: value, error: response.error });
			if (status.exitCode !== 0) return { outcome: "unavailable", observations };
			if (value === "terminal_ok" || value === "failed") return { outcome: "terminal", status: value, observations };
			if (value === "unknown") return { outcome: "unknown", observations };
			if (mode === "in_flight" && value === "in_flight") return { outcome: "in_flight", observations };
			await Bun.sleep(250);
		}
		return { outcome: "timeout", observations };
	};

	const scratchModels = [
		"providers:",
		"  layofflabs-anthropic:",
		"    baseUrl: https://api.layofflabs.com/v1",
		"    apiKeyEnv: OPENAI_API_KEY",
		"    api: anthropic-messages",
		"    auth: apiKey",
		"    models:",
		"      - id: claude-opus-5",
		"        reasoning: true",
		"        thinking:",
		"          minLevel: minimal",
		"          maxLevel: xhigh",
		"          mode: anthropic-adaptive",
		"          defaultLevel: high",
		"          levels: [minimal, low, medium, high, xhigh]",
		"        input: [text]",
		"        contextWindow: 1000000",
		"        maxTokens: 128000",
		"",
	].join("\n");
	await writeFile(join(sdkAgent, "models.yml"), scratchModels, { mode: 0o600 });
	stageEvidence.scratchProviderConfig = {
		provider: "layofflabs-anthropic",
		model: "claude-opus-5",
		api: "anthropic-messages",
		credentialReference: "OPENAI_API_KEY",
		credentialValueCopied: false,
	};

	try {
		stageBroker = Bun.spawn([gjc, "sdk", "broker-internal", "--agent-dir", sdkAgent], {
			cwd: sdkRepo,
			env: sdkInheritedEnvironment,
			stdout: "ignore",
			stderr: "pipe",
		});
		stageBrokerStderr = pipedText(stageBroker.stderr);
		const discovery = await waitForBroker();
		stageEvidence.broker = {
			protocolVersion: discovery.protocolVersion,
			pid: discovery.pid,
			inheritsProviderEnvironment: true,
		};
		await Bun.sleep(500);

		const createArgs = [
			"sdk",
			"session",
			"raw",
			"global",
			"--agent-dir",
			sdkAgent,
			"--op",
			"session.create",
			"--idempotency-key",
			`stage0e-create-${crypto.randomUUID()}`,
			"--json-input",
			JSON.stringify({ cwd: sdkRepo }),
		];
		const createdRun = await runSdkCreate(createArgs, sdkInheritedEnvironment);
		const created = createdRun.result;
		stageEvidence.create = { attempts: createdRun.attempts.map(compact), final: compact(created) };
		const createPayload = asRecord(jsonOutput(created).result);
		if (typeof createPayload?.sessionId !== "string") {
			stageEvidence.result = "No scratch lifecycle session ID was issued after five idempotent attempts; no model or turn operation was attempted.";
			return stageEvidence;
		}
		stageSessionId = createPayload.sessionId;

		const modelSet = await control("model.set", { id: modelId, thinkingLevel: "medium" });
		stageEvidence.modelSet = compact(modelSet);
		if (modelSet.exitCode !== 0 || jsonOutput(modelSet).ok !== true) {
			stageEvidence.result = "The configured parent-default provider model was not accepted by the scratch session; no prompt was submitted.";
			return stageEvidence;
		}

		const successfulMarker = "STAGE0E_SDK_SUCCESS";
		const simpleOpRef = `stage0e-simple-${crypto.randomUUID()}`;
		const simple = await run(
			[
				"sdk",
				"session",
				"send",
				stageSessionId,
				"--agent-dir",
				sdkAgent,
				"--text",
				`Reply with exactly: ${successfulMarker}`,
				"--op-ref",
				simpleOpRef,
				"--wait",
				"--timeout-ms",
				"120000",
			],
			sdkRepo,
			sdkInheritedEnvironment,
		);
		const simpleTail = await tail();
		const simpleVocabulary = summarizeTailVocabulary(simpleTail);
		stageEvidence.successfulTurn = {
			send: compact(simple),
			tail: simpleVocabulary,
			assistantMarkerObserved: simpleTail.stdout.includes(successfulMarker),
		};
		const simplePayload = asRecord(jsonOutput(simple).result);
		if (simple.exitCode !== 0 || simplePayload?.status !== "terminal_ok" || !simpleTail.stdout.includes(successfulMarker)) {
			stageEvidence.result = "The inherited-environment scratch turn did not produce a terminal successful assistant marker.";
			return stageEvidence;
		}

		const steerMarker = "STAGE0E_STEER_ECHO";
		const steerOpRef = `stage0e-steer-turn-${crypto.randomUUID()}`;
		const longerPrompt =
			"Without using tools, write a detailed 1,800-word explanation of why an integration probe must establish identity, liveness, terminal status, and cleanup independently. Use several sections, then end with STAGE0E_STEER_BASELINE.";
		const steerSeed = await run(
			[
				"sdk",
				"session",
				"send",
				stageSessionId,
				"--agent-dir",
				sdkAgent,
				"--text",
				longerPrompt,
				"--op-ref",
				steerOpRef,
			],
			sdkRepo,
			sdkInheritedEnvironment,
		);
		const steerEvidence: Record<string, unknown> = { seed: compact(steerSeed), marker: steerMarker };
		stageEvidence.steer = steerEvidence;
		const steerSeedPayload = asRecord(jsonOutput(steerSeed).result);
		if (steerSeed.exitCode !== 0 || !["accepted", "in_flight"].includes(String(steerSeedPayload?.status))) {
			steerEvidence.skipped = "The longer turn was not accepted; no mid-turn steering control was sent.";
			stageEvidence.compaction = { skipped: "The longer-turn precondition was not met." };
			return stageEvidence;
		}
		const inFlight = await poll(steerOpRef, "in_flight", 15_000);
		steerEvidence.inFlight = inFlight;
		if (inFlight.outcome !== "in_flight") {
			steerEvidence.skipped = "The longer turn did not reach an observable in-flight state before its bounded window.";
			stageEvidence.compaction = { skipped: "Mid-turn state could not be established safely." };
			return stageEvidence;
		}
		const steering = await control("turn.steer", {
			text: `For this probe, include this exact final line: ${steerMarker}`,
			clientRef: `stage0e-steer-${crypto.randomUUID()}`,
		});
		steerEvidence.control = compact(steering);
		const steerTerminal = await poll(steerOpRef, "terminal", 120_000);
		const steerTail = await tail();
		steerEvidence.terminal = steerTerminal;
		steerEvidence.tail = summarizeTailVocabulary(steerTail);
		steerEvidence.echoObserved = steerTail.stdout.includes(steerMarker);
		if (steerTerminal.outcome !== "terminal" || steerTerminal.status !== "terminal_ok") {
			stageEvidence.compaction = { skipped: "The steered turn did not terminally succeed." };
			return stageEvidence;
		}

		const compactionAttempts: Array<Record<string, unknown>> = [];
		let compactionSucceeded = false;
		let compactionTail: CommandResult | undefined;
		for (let index = 1; index <= 6 && !compactionSucceeded; index++) {
			const filler = await run(
				[
					"sdk",
					"session",
					"send",
					stageSessionId,
					"--agent-dir",
					sdkAgent,
					"--text",
					`Treat the following as retained context filler only and reply exactly: STAGE0E_FILL_ACK_${index}\n\n${"STAGE0E_COMPACTION_FILLER ".repeat(1_200)}`,
					"--op-ref",
					`stage0e-compaction-fill-${index}-${crypto.randomUUID()}`,
					"--wait",
					"--timeout-ms",
					"120000",
				],
				sdkRepo,
				sdkInheritedEnvironment,
			);
			const attempt: Record<string, unknown> = { fill: compact(filler) };
			compactionAttempts.push(attempt);
			const fillPayload = asRecord(jsonOutput(filler).result);
			if (filler.exitCode !== 0 || fillPayload?.status !== "terminal_ok") {
				attempt.reason = "Filler did not terminally succeed; compaction was not requested after partial history.";
				break;
			}
			const compaction = await control("compaction.run", {}, 120_000);
			attempt.control = compact(compaction);
			compactionSucceeded = compaction.exitCode === 0 && jsonOutput(compaction).ok === true;
			if (compactionSucceeded) compactionTail = await tail(60_000);
		}
		const compactionVocabulary = compactionTail ? summarizeTailVocabulary(compactionTail) : undefined;
		const kindSequence = (compactionVocabulary?.kindSequence as string[] | undefined) ?? [];
		const eventTypeSequence = (compactionVocabulary?.eventTypeSequence as string[] | undefined) ?? [];
		const compactionEventKinds = [...new Set([...kindSequence, ...eventTypeSequence].filter(kind => /compact/i.test(kind)))];
		stageEvidence.compaction = {
			attempts: compactionAttempts,
			succeeded: compactionSucceeded,
			...(compactionVocabulary ? { tail: compactionVocabulary } : {}),
			compactionEventKinds,
			noNamedCompactionEvent: compactionSucceeded && compactionEventKinds.length === 0,
		};
	} catch (error) {
		stageEvidence.error = error instanceof Error ? error.message : String(error);
	} finally {
		if (stageSessionId) {
			const closed = await run(
				[
					"sdk",
					"session",
					"raw",
					"global",
					"--agent-dir",
					sdkAgent,
					"--op",
					"session.close",
					"--idempotency-key",
					`stage0e-close-${crypto.randomUUID()}`,
					"--json-input",
					JSON.stringify({ sessionId: stageSessionId, cwd: sdkRepo }),
				],
				sdkRepo,
				sdkInheritedEnvironment,
			);
			stageEvidence.close = compact(closed);
		}
		if (stageBroker) {
			try {
				stageBroker.kill("SIGTERM");
			} catch {
				// The owned broker may have exited after a lifecycle failure.
			}
			stageEvidence.brokerCleanup = {
				exitCode: await stageBroker.exited,
				stderr: (await (stageBrokerStderr ?? Promise.resolve(""))).slice(0, 700),
			};
		}
	}
	return stageEvidence;
}


if (Bun.argv.includes("--stage0e")) {
	let stageEvidence: Record<string, unknown>;
	let stageFailure = false;
	try {
		stageEvidence = await runStage0eInheritedEnvironmentProbe();
	} catch (error) {
		stageFailure = true;
		stageEvidence = { error: error instanceof Error ? error.message : String(error) };
	} finally {
		await rm(root, { recursive: true, force: true });
		console.log(JSON.stringify({ stage0e: stageEvidence!, cleanup: { temporaryRootRemoved: true } }, null, 2));
	}
	process.exit(stageFailure ? 1 : 0);
}

if (Bun.argv.includes("--stage0d")) {
	let stageEvidence: Record<string, unknown>;
	let stageFailure = false;
	try {
		stageEvidence = await runStage0dProfileServeProbe();
	} catch (error) {
		stageFailure = true;
		stageEvidence = { error: error instanceof Error ? error.message : String(error) };
	} finally {
		await rm(root, { recursive: true, force: true });
		console.log(JSON.stringify({ stage0d: stageEvidence!, cleanup: { temporaryRootRemoved: true } }, null, 2));
	}
	process.exit(stageFailure ? 1 : 0);
}

if (Bun.argv.includes("--stage0c")) {
	let stageEvidence: Record<string, unknown>;
	let stageFailure = false;
	try {
		stageEvidence = await runStage0cProfileProbe();
	} catch (error) {
		stageFailure = true;
		stageEvidence = { error: error instanceof Error ? error.message : String(error) };
	} finally {
		await rm(root, { recursive: true, force: true });
		console.log(JSON.stringify({ stage0c: stageEvidence!, cleanup: { temporaryRootRemoved: true } }, null, 2));
	}
	process.exit(stageFailure ? 1 : 0);
}

let brokerProcess: ReturnType<typeof Bun.spawn> | undefined;
let brokerStderr: Promise<string> | undefined;
let serveProcess: ReturnType<typeof Bun.spawn> | undefined;
let serveStdout: Promise<string> | undefined;
let serveStderr: Promise<string> | undefined;
const lifecycleSessionIds = new Set<string>();
const cleanup: Record<string, unknown> = { sessionCloses: [] };
let evidence: Record<string, unknown> | undefined;

try {
	const turnArgs = [
		"-p",
		"--mode",
		"json",
		"--no-tools",
		"--no-mcp",
		"--no-rules",
		"--no-lsp",
		"--session-dir",
		sessions,
	];
	const version = await run(["--version"]);
	const rootHelp = await run(["--help"]);
	const sdkHelp = await run(["sdk", "--help"]);
	const sdkSessionHelp = await run(["sdk", "session", "--help"]);
	const first = await run([...turnArgs, "say exactly: spike-alpha"]);
	const id = sessionId(first.stdout);
	const filesAfterFirst = await readdir(sessions);
	const resumed = await run(["--resume", id, ...turnArgs, "say exactly: spike-beta"]);
	const resumedId = sessionId(resumed.stdout);

	// Give the deliberately invalid resume an isolated agent directory too: GJC's
	// crash recorder must not write to the caller's normal agent directory.
	const bogus = await run(
		["--resume", "00000000-0000-0000-0000-000000000000", ...turnArgs, "say exactly: bogus"],
		repo,
		{ ...sanitizedEnvironment, GJC_CODING_AGENT_DIR: join(root, "bogus-agent") },
	);
	const continued = await run(["--continue", ...turnArgs, "say exactly: spike-continue"]);
	const continuedId = sessionId(continued.stdout);
	// This is deliberately not an SDK-broker session: it reads the installed
	// profile in place for credentials while stores its transcript under root.
	const credentialedTurnArgs = [...turnArgs.slice(0, -1), realSessions];
	const credentialedTurn = await run(
		[...credentialedTurnArgs, "Reply with exactly: P2B_SUCCESS"],
		repo,
		credentialedEnvironment,
	);
	const credentialedTurnSummary = summarizeCredentialedTurn(credentialedTurn);


	// Own the broker foreground process so the probe never leaves a detached broker.
	brokerProcess = Bun.spawn([gjc, "sdk", "broker-internal", "--agent-dir", sdkAgent], {
		cwd: sdkRepo,
		env: sdkEnvironment,
		stdout: "ignore",
		stderr: "pipe",
	});
	brokerStderr = pipedText(brokerProcess.stderr);
	const brokerDiscovery = await waitForBroker();
	// Discovery is published before the child startup queue has fully settled.
	await Bun.sleep(500);
	const sdkInput = JSON.stringify({ cwd: sdkRepo });
	const createBaseArgs = ["sdk", "session", "raw", "global", "--agent-dir", sdkAgent, "--op", "session.create"];
	const createArgs = [...createBaseArgs, "--idempotency-key", "spike-external-key", "--json-input", sdkInput];
	const createdRun = await runSdkCreate(createArgs);
	const created = createdRun.result;
	const createdAgain = await run(createArgs, sdkRepo, sdkEnvironment);
	const createdId = lifecycleSessionId(created);
	lifecycleSessionIds.add(createdId);
	// Replay the already-proven lifecycle key rather than starting another host:
	// this keeps the P2 probe isolated to one session and makes cleanup exact.
	const concurrent: CommandResult[] = [];
	for (let index = 0; index < 5; index++) {
		concurrent.push(await run(createArgs, sdkRepo, sdkEnvironment));
	}
	const sdkIds = concurrent.map(lifecycleSessionId);
	const oneShots = await Promise.all(
		Array.from({ length: 5 }, (_, index) => run([...turnArgs, `say exactly: parallel-${index + 1}`])),
	);
	const oneShotIds = oneShots.map(result => sessionId(result.stdout));

	const socketEntriesBefore = (await readdir(root)).filter(entry => entry.endsWith(".sock"));
	serveProcess = Bun.spawn([gjc, "sdk", "serve", "--socket", socketPath, "--session", createdId], {
		cwd: sdkRepo,
		env: sdkEnvironment,
		stdout: "pipe",
		stderr: "pipe",
	});
	serveStdout = pipedText(serveProcess.stdout);
	serveStderr = pipedText(serveProcess.stderr);
	const socket = await waitForSocket(socketPath);
	const socketReadinessFrame = await probeSocketConnection(socketPath);
	const socketEntriesWhileServing = (await readdir(root)).filter(entry => entry.endsWith(".sock"));
	serveProcess.kill("SIGTERM");
	const [serveExitCode, servedStdout, servedStderr] = await Promise.all([
		serveProcess.exited,
		serveStdout,
		serveStderr,
	]);
	serveProcess = undefined;
	serveStdout = undefined;
	serveStderr = undefined;
	const socketRemoved = await waitForSocketRemoval(socketPath);
	// `session.checkpoint` exposes only a structured record through the CLI. The
	// local endpoint exchange below returns the signed opaque resume token without
	// writing or rendering that credential material.
	const directCheckpoint = checkpointCursor(
		await requestScratchEndpoint(createdId, { type: "query_request", query: "session.checkpoint", input: {} }),
	);


	const checkpoint = await run(
		["sdk", "session", "raw", "query", createdId, "--agent-dir", sdkAgent, "--query", "session.checkpoint", "--json-input", "{}"],
		sdkRepo,
		sdkEnvironment,
	);
	const sent = await run(
		[
			"sdk",
			"session",
			"send",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--text",
			"say exactly: p2-sdk-send",
			"--op-ref",
			"p2-send-ref",
			"--wait",
			"--timeout-ms",
			"15000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const duplicate = await run(
		[
			"sdk",
			"session",
			"send",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--text",
			"different p2-sdk-send",
			"--op-ref",
			"p2-send-ref",
			"--wait",
			"--timeout-ms",
			"15000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const steer = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"turn.steer",
			"--json-input",
			JSON.stringify({ text: "p2 steer", clientRef: "p2-steer-ref" }),
		],
		sdkRepo,
		sdkEnvironment,
	);
	const operatorInput = JSON.stringify({ mode: "terminal", scope: "turn", operator: true });
	const operatorWithoutConfirm = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"turn.abort",
			"--idempotency-key",
			"p2-operator-no-confirm",
			"--json-input",
			operatorInput,
		],
		sdkRepo,
		sdkEnvironment,
	);
	const operatorWithConfirm = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"turn.abort",
			"--idempotency-key",
			"p2-operator-confirm",
			"--confirm",
			"--json-input",
			operatorInput,
		],
		sdkRepo,
		sdkEnvironment,
	);
	const compactionSmall = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"compaction.run",
			"--json-input",
			"{}",
			"--timeout-ms",
			"5000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const tailCursor = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--cursor",
			"p2-invalid-checkpoint-token",
			"--until-idle",
			"--strict",
			"--all-events",
			"--timeout-ms",
			"1500",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const serializedCheckpointCursor = JSON.stringify(directCheckpoint.checkpoint);
	const tailSerializedCheckpoint = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--cursor",
			serializedCheckpointCursor,
			"--until-idle",
			"--strict",
			"--all-events",
			"--timeout-ms",
			"3000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const tailSignedCursorStrict = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--cursor",
			directCheckpoint.token,
			"--until-idle",
			"--strict",
			"--all-events",
			"--timeout-ms",
			"3000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const tailSignedCursorResume = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--cursor",
			directCheckpoint.token,
			"--until-idle",
			"--all-events",
			"--timeout-ms",
			"3000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const cursorResync = retentionResync(tailSignedCursorStrict) ?? retentionResync(tailSignedCursorResume);
	const cursorResyncReplay = cursorResync
		? await requestScratchEndpoint(createdId, {
				type: "event_replay",
				sinceGeneration: cursorResync.generation,
				sinceSeq: cursorResync.seq,
			})
		: { type: "event_replay_result", ok: false, error: { code: "resync_unavailable" } };

	const tailStrict = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--until-idle",
			"--strict",
			"--all-events",
			"--timeout-ms",
			"3000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const tailUntilIdle = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--until-idle",
			"--all-events",
			"--timeout-ms",
			"3000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	// A manual compaction needs a cut point beyond the keep window. Keep the
	// material local and bounded; every submission is terminally reconciled before
	// the control is issued.
	const compactionFiller = "P2B_COMPACTION_FILLER ".repeat(1_200);
	const compactionFills: CommandResult[] = [];
	for (let index = 0; index < 4; index++) {
		compactionFills.push(
			await run(
				[
					"sdk",
					"session",
					"send",
					createdId,
					"--agent-dir",
					sdkAgent,
					"--text",
					compactionFiller,
					"--op-ref",
					`p2b-compaction-fill-${index + 1}`,
					"--wait",
					"--timeout-ms",
					"15000",
				],
				sdkRepo,
				sdkEnvironment,
			),
		);
	}
	const compactionFilled = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"compaction.run",
			"--json-input",
			"{}",
			"--timeout-ms",
			"15000",
		],
		sdkRepo,
		sdkEnvironment,
	);
	const tailAfterCompaction = await run(
		[
			"sdk",
			"session",
			"tail",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--until-idle",
			"--all-events",
			"--timeout-ms",
			"5000",
		],
		sdkRepo,
		sdkEnvironment,
	);

	const modelRebind = await run(
		[
			"sdk",
			"session",
			"raw",
			"control",
			createdId,
			"--agent-dir",
			sdkAgent,
			"--op",
			"model.set",
			"--json-input",
			JSON.stringify({ id: "openai/gpt-5.4" }),
		],
		sdkRepo,
		sdkEnvironment,
	);

	evidence = {
		root: "<temporary directory removed after probe>",
		version: compact(version),
		rootHelp: compact(rootHelp),
		sdkHelp: compact(sdkHelp),
		sdkSessionHelp: compact(sdkSessionHelp),
		first: { ...compact(first), sessionId: id, sessionFiles: filesAfterFirst },
		resume: { ...compact(resumed), sessionId: resumedId, sameSession: id === resumedId },
		bogusResume: compact(bogus),
		continue: { ...compact(continued), sessionId: continuedId, sameSession: id === continuedId },
		sdkCreate: {
			attempts: createdRun.attempts.map(compact),
			first: jsonOutput(created),
			second: jsonOutput(createdAgain),
		},
		sdkRepeatedCreate: {
			mode: "sequential",
			exitCodes: concurrent.map(result => result.exitCode),
			sessionIds: sdkIds,
			uniqueSessionIds: [...new Set(sdkIds)].length,
		},
		parallelOneShots: {
			exitCodes: oneShots.map(result => result.exitCode),
			sessionIds: oneShotIds,
			uniqueSessionIds: [...new Set(oneShotIds)].length,
		},
		credentialedScratchTurn: {
			command: compact(credentialedTurn).command,
			summary: credentialedTurnSummary,
			sessionDirectory: "<temporary directory removed after probe>",
			credentialSource: "installed profile read in place; no credentials copied into the scratch root",
		},

		p2: {
			broker: {
				protocolVersion: brokerDiscovery.protocolVersion,
				pid: brokerDiscovery.pid,
			},
			serve: {
				command: `gjc sdk serve --socket ${socketPath} --session ${createdId}`,
				requestedSocketPath: socketPath,
				socketEntriesBefore,
				socketEntriesWhileServing,
				isSocket: true,
				mode: socket.mode,
				connectable: true,
				authFailureFrame: socketReadinessFrame.trim(),
				exitCode: serveExitCode,
				stdout: servedStdout,
				stderr: servedStderr,
				socketRemoved,
			},
			checkpoint: compact(checkpoint),
			send: compact(sent),
			duplicateOpRef: compact(duplicate),
			turnSteer: compact(steer),
			operatorWithoutConfirm: compact(operatorWithoutConfirm),
			operatorWithConfirm: compact(operatorWithConfirm),
			compactionSmall: compact(compactionSmall),
			tailCursor: compact(tailCursor),
			tailStrict: compact(tailStrict),
			tailUntilIdle: compact(tailUntilIdle),
			tailUntilIdleSummary: summarizeTail(tailUntilIdle),
			modelRebind: compact(modelRebind),
			compactionFilled: compact(compactionFilled),
			compactionFillSubmissions: compactionFills.map(compact),
			tailAfterCompaction: summarizeTail(tailAfterCompaction),
			cursorResume: {
				checkpointToken: directCheckpoint.metadata,
				serializedCheckpointCursor: compact(tailSerializedCheckpoint),
				signedCursorStrict: summarizeTail(tailSignedCursorStrict),
				signedCursorResume: summarizeTail(tailSignedCursorResume),
				resync: cursorResync,
				resyncReplay: summarizeEndpointReplay(cursorResyncReplay),
			},
		},
		latencyMs: { oneShot: first.elapsedMs, resumed: resumed.elapsedMs },
	};
	if (id !== resumedId || id !== continuedId) throw new Error("resume or --continue forked the session");
	if (bogus.exitCode === 0) throw new Error("bogus resume silently succeeded");
	if (new Set(sdkIds).size !== 1 || concurrent.some(result => result.exitCode !== 0))
		throw new Error("SDK repeated create was not idempotent");
	if (new Set(oneShotIds).size !== 5 || oneShots.some(result => result.exitCode !== 0))
		throw new Error("parallel one-shots did not complete cleanly");
} finally {
	if (serveProcess) {
		try {
			serveProcess.kill("SIGTERM");
		} catch {
			// The process may have exited between the lifecycle check and cleanup.
		}
		const [exitCode, stdout, stderr] = await Promise.all([
			serveProcess.exited,
			serveStdout ?? Promise.resolve(""),
			serveStderr ?? Promise.resolve(""),
		]);
		cleanup.serveFallback = { exitCode, stdout, stderr, socketRemoved: await waitForSocketRemoval(socketPath) };
	}

	const sessionCloses = cleanup.sessionCloses as Array<Record<string, unknown>>;
	for (const sessionId of lifecycleSessionIds) {
		const closed = await run(
			[
				"sdk",
				"session",
				"raw",
				"global",
				"--agent-dir",
				sdkAgent,
				"--op",
				"session.close",
				"--idempotency-key",
				`spike-close-${sessionId}`,
				"--json-input",
				JSON.stringify({ sessionId, cwd: sdkRepo }),
			],
			sdkRepo,
			sdkEnvironment,
		);
		sessionCloses.push({ sessionId, ...compact(closed) });
	}

	if (brokerProcess) {
		try {
			brokerProcess.kill("SIGTERM");
		} catch {
			// The broker may have exited between its last use and cleanup.
		}
		cleanup.broker = {
			exitCode: await brokerProcess.exited,
			stderr: await (brokerStderr ?? Promise.resolve("")),
		};
	}
	if (evidence) console.log(JSON.stringify({ ...evidence, cleanup }, null, 2));
	await rm(root, { recursive: true, force: true });
}
