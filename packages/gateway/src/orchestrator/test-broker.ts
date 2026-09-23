import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import type { GlobalGjcClientDependencies, SpawnFn } from "./broker";

type StubOperation = {
	readonly sessionId: string;
	readonly opRef: string;
	readonly commandId: string;
	readonly turnId: string;
	readonly text: string;
	readonly startedAt: number;
	terminalAt?: number;
	reply?: string;
	state: "in_flight" | "terminal_ok";
};

type Relay = {
	readonly sessionId: string;
	readonly connectionId: string;
	readonly controller: ReadableStreamDefaultController<Uint8Array>;
	helloReceived: boolean;
};

/**
 * Deterministic daemon-process seam retained for crash-recovery E2E tests.
 * It is imported only by `test/daemon-entry.ts`; production `main.ts` has no
 * test-broker dependency or environment-selected implementation path.
 *
 * It simulates the host's relay contract: `turn.prompt` submitted on a relay
 * makes THAT relay the turn's owner, and only the owner receives the turn's
 * `agent_start`, `event/message_end` and `agent_end` frames, stamped with the
 * turn's commandId/turnId — exactly what `gjc sdk serve --stdio` forwards.
 */
export function testOnlyBrokerDependencies(): GlobalGjcClientDependencies {
	const sessions = new Map<string, string>();
	const repositories = new Map<string, string>();
	const operations = new Map<string, StubOperation>();
	const relays = new Set<Relay>();
	let connections = 0;
	const emit = (relay: Relay, frame: Record<string, unknown>) => {
		try {
			relay.controller.enqueue(new TextEncoder().encode(`${JSON.stringify(frame)}\n`));
		} catch {
			/* relay closed */
		}
	};
	const runTurn = (relay: Relay, operation: StubOperation) => {
		const correlation = { commandId: operation.commandId, turnId: operation.turnId };
		setTimeout(() => {
			emit(relay, { type: "agent_start", sessionId: operation.sessionId, ...correlation });
			operation.reply = stubReply(operation.text);
			emit(relay, {
				type: "event",
				kind: "message_end",
				payload: {
					event_type: "message_end",
					event: {
						type: "message_end",
						message: {
							role: "assistant",
							id: `${operation.opRef}:assistant`,
							content: [{ type: "text", text: operation.reply }],
						},
					},
				},
				...correlation,
			});
			operation.terminalAt = Date.now();
			operation.state = "terminal_ok";
			emit(relay, {
				type: "agent_end",
				sessionId: operation.sessionId,
				...correlation,
				outcome: { reason: "end_turn" },
			});
		}, 50);
	};
	const statusOf = (opRef: string | undefined, sessionId: string | undefined) => {
		const operation = typeof opRef === "string" ? operations.get(opRef) : undefined;
		if (!operation || operation.sessionId !== sessionId) return { status: "unknown" };
		return {
			status: operation.state,
			commandId: operation.commandId,
			turnId: operation.turnId,
			clientRef: opRef,
			acceptedAt: operation.startedAt,
			startedAt: operation.startedAt,
			receiptState: "present",
			...(operation.state === "terminal_ok"
				? { terminalAt: operation.terminalAt, outcome: { reason: "end_turn" } }
				: {}),
		};
	};
	const handleRelayLine = (relay: Relay, line: string) => {
		let frame: Record<string, unknown>;
		try {
			frame = JSON.parse(line) as Record<string, unknown>;
		} catch {
			return;
		}
		if (frame.type === "hello") {
			relay.helloReceived = true;
			emit(relay, {
				type: "hello",
				protocolVersion: 3,
				capabilities: ["tool_activity_v2"],
				connectionId: relay.connectionId,
			});
			return;
		}
		const id = typeof frame.id === "string" ? frame.id : "";
		if (frame.type === "control_request") {
			const input = (frame.input ?? {}) as { text?: unknown; clientRef?: unknown };
			if (frame.operation === "turn.prompt") {
				const opRef = typeof input.clientRef === "string" ? input.clientRef : "";
				if (!opRef || typeof input.text !== "string")
					return emit(relay, { type: "control_response", id, ok: false, error: { code: "invalid_request" } });
				if (operations.has(opRef))
					return emit(relay, { type: "control_response", id, ok: false, error: { code: "client_ref_conflict" } });
				const operation: StubOperation = {
					sessionId: relay.sessionId,
					opRef,
					commandId: `stub-command-${opRef}`,
					turnId: `stub-turn-${opRef}`,
					text: input.text,
					startedAt: Date.now(),
					state: "in_flight",
				};
				operations.set(opRef, operation);
				emit(relay, {
					type: "control_response",
					id,
					ok: true,
					result: { commandId: operation.commandId, turnId: operation.turnId, accepted: true, clientRef: opRef },
				});
				runTurn(relay, operation);
				return;
			}
			if (frame.operation === "turn.steer")
				return emit(relay, {
					type: "control_response",
					id,
					ok: true,
					result: { accepted: true, status: "accepted", clientRef: input.clientRef },
				});
			return emit(relay, { type: "control_response", id, ok: false, error: { code: "unsupported_operation" } });
		}
		if (frame.type === "query_request") {
			if (frame.query === "turn.result") {
				const input = (frame.input ?? {}) as { clientRef?: unknown };
				const opRef = typeof input.clientRef === "string" ? input.clientRef : undefined;
				return emit(relay, { type: "query_response", id, ok: true, result: statusOf(opRef, relay.sessionId) });
			}
			return emit(relay, { type: "query_response", id, ok: false, error: { code: "unsupported_query" } });
		}
	};
	const spawn = ({ cmd }: { cmd: readonly string[] }) => {
		const sessionId = argument(cmd, "--session");
		if (!sessionId || !repositories.has(sessionId)) throw new Error("unknown fake relay session");
		return relayChild(sessionId, `connection:${++connections}`, relays, handleRelayLine);
	};
	const command: CliRunner = async (rawArgs) => {
		if (violatesSessionArgvContract(rawArgs)) return usage();
		const args = withoutAgentDir(rawArgs);
		if (args[0] === "--version") return success("gjc/0.17.2\n");
		if (args[0] !== "sdk") return failure("stub_unsupported");
		if (args[1] === "serve") return { exitCode: 1, stdout: "", stderr: "" };
		if (args[1] !== "session") return failure("stub_unsupported");

		if (args[2] === "list") return success({ sessions: [] });
		if (args[2] === "--scope") return success({ sessions: [] });
		if (args.includes("session.create")) {
			const key = argument(args, "--idempotency-key");
			const input = JSON.parse(argument(args, "--json-input") ?? "{}") as { cwd?: unknown };
			if (!key || typeof input.cwd !== "string") return failure("invalid_request");
			const sessionId = sessions.get(key) ?? crypto.randomUUID();
			if (repositories.has(sessionId) && repositories.get(sessionId) !== input.cwd)
				return failure("client_ref_conflict");
			sessions.set(key, sessionId);
			repositories.set(sessionId, input.cwd);
			return success({ sessionId });
		}
		if (args[2] === "status") {
			const sessionId = args[3];
			const opRef = args[4];
			return success({
				operationRef: opRef,
				status: statusOf(opRef, sessionId),
				summary: { completed: operations.get(opRef ?? "")?.state === "terminal_ok" },
			});
		}
		if (args.includes("turn.result")) {
			const sessionId = args[args.indexOf("query") + 1];
			const input = JSON.parse(argument(args, "--json-input") ?? "{}") as { clientRef?: unknown };
			const opRef = typeof input.clientRef === "string" ? input.clientRef : undefined;
			const operation = opRef ? operations.get(opRef) : undefined;
			const status = statusOf(opRef, sessionId) as Record<string, unknown>;
			const content =
				operation?.state === "terminal_ok" && operation.reply !== undefined
					? {
							content: {
								version: 1,
								type: "text",
								text: operation.reply,
								byteLength: new TextEncoder().encode(operation.reply).length,
								truncated: false,
							},
						}
					: {};
			return success({ kind: "prompt", ...status, ...content });
		}
		if (args.includes("transcript.list")) {
			const sessionId = args[args.indexOf("query") + 1];
			if (!sessionId || !repositories.has(sessionId)) return failure("session_unavailable");
			const items = [...operations.values()]
				.filter((operation) => operation.sessionId === sessionId && operation.state === "terminal_ok")
				.sort((left, right) => left.terminalAt! - right.terminalAt!)
				.map((operation) => ({
					role: "assistant",
					ts: new Date(operation.terminalAt!).toISOString(),
					body: operation.reply,
				}));
			return {
				exitCode: 0,
				stdout: JSON.stringify({ type: "query_response", ok: true, page: { items, complete: true } }),
				stderr: "",
			};
		}
		if (args.includes("session.last_assistant")) {
			const sessionId = args[args.indexOf("query") + 1];
			const operation = [...operations.values()].reverse().find((entry) => entry.sessionId === sessionId);
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					type: "query_response",
					ok: true,
					page: { items: operation?.state === "terminal_ok" ? [operation.reply] : [], complete: true },
				}),
				stderr: "",
			};
		}
		if (args.includes("model.set")) return success({ changed: true });
		if (args.includes("session.close")) return success({ closed: true });
		if (args.includes("session.resume")) return success({ resumed: true });
		if (args[2] === "inspect") {
			const sessionId = args[3];
			const repo = sessionId ? repositories.get(sessionId) : undefined;
			if (!repo) return failure("session_unavailable");
			return success({ session: { sessionId, live: true, deleted: false, locator: { repo } } });
		}
		return failure("stub_unsupported");
	};
	return {
		command,
		spawn: spawn as unknown as SpawnFn,
		healthProbe: async () => true,
		executable: "/test-only/gjc",
		agentDir: "/test-only/gjc-agent",
		discovery: async () => ({ pid: 1, url: "ws://127.0.0.1:1", token: "test-only", heartbeatAt: Date.now() }),
	};
}

function success(result: unknown): CliResult {
	return { exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" };
}

function failure(code: string): CliResult {
	return { exitCode: 0, stdout: JSON.stringify({ ok: false, error: { code } }), stderr: "" };
}

/** The gjc 0.17.4 registry's parser rejection: exit 2, structured `usage` on stderr, empty stdout. */
function usage(): CliResult {
	return {
		exitCode: 2,
		stdout: "",
		stderr: `ERROR ${JSON.stringify({ code: "usage", category: "usage", message: "The command arguments are invalid." })}\n`,
	};
}

/** Scoped `sdk session` leaves; every other leaf resolves one session by ID and rejects `--repo`. */
const REPO_SCOPED_SESSION_LEAVES = new Set(["list", "tail"]);

/**
 * The argv shape gjc 0.17.4 rejects: `--agent-dir` between `session` and the
 * leaf, or `--repo` on an exact-session leaf (`inspect`, `send`, `status`,
 * `raw query`, ...). Enforced here so tests fail on the argv a real 0.17.4
 * runtime refuses, instead of pinning it.
 */
export function violatesSessionArgvContract(args: readonly string[]): boolean {
	if (args[0] !== "sdk" || args[1] !== "session") return false;
	if (args[2]?.startsWith("--agent-dir")) return true;
	return !REPO_SCOPED_SESSION_LEAVES.has(args[2] ?? "") && args.some((arg) => /^--repo(=|$)/.test(arg));
}

function argument(args: readonly string[], name: string): string | undefined {
	const index = args.indexOf(name);
	return index >= 0 ? args[index + 1] : undefined;
}

function withoutAgentDir(args: readonly string[]): string[] {
	const stripped: string[] = [];
	for (let index = 0; index < args.length; index++) {
		if (args[index] === "--agent-dir") {
			index++;
			continue;
		}
		stripped.push(args[index]!);
	}
	return stripped;
}

function stubReply(_prompt: string): string {
	return process.env.GAJAEWAY_TEST_STUB_REPLY ?? "stub reply";
}

/** A fake `Bun.spawn` child whose stdin lines drive the relay and whose stdout carries its frames. */
function relayChild(
	sessionId: string,
	connectionId: string,
	relays: Set<Relay>,
	onLine: (relay: Relay, line: string) => void,
): ReturnType<typeof Bun.spawn> {
	let finish = () => {};
	let relay!: Relay;
	let closed = false;
	const unregister = () => {
		if (closed) return;
		closed = true;
		relays.delete(relay);
		finish();
	};
	const stdout = new ReadableStream<Uint8Array>({
		start(controller) {
			relay = { sessionId, connectionId, controller, helloReceived: false };
			relays.add(relay);
		},
		cancel() {
			unregister();
		},
	});
	let buffer = "";
	const stdin = {
		write(chunk: string) {
			buffer += chunk;
			let newline = buffer.indexOf("\n");
			while (newline >= 0) {
				const line = buffer.slice(0, newline);
				buffer = buffer.slice(newline + 1);
				if (line.trim()) onLine(relay, line);
				newline = buffer.indexOf("\n");
			}
			return chunk.length;
		},
		flush() {},
		end() {},
	};
	const exited = new Promise<number>((resolve) => {
		finish = () => resolve(0);
	});
	const kill = () => {
		if (closed) return;
		try {
			relay.controller.close();
		} catch {
			/* already closed */
		}
		unregister();
	};
	return { exited, kill, stdout, stdin } as unknown as ReturnType<typeof Bun.spawn>;
}
