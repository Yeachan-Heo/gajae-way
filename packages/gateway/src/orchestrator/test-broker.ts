import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import type { GlobalGjcClientDependencies, SpawnFn } from "./broker";

type StubOperation = {
	readonly sessionId: string;
	readonly opRef: string;
	readonly text: string;
	readonly startedAt: number;
	terminalAt?: number;
	reply?: string;
	state: "in_flight" | "terminal_ok";
	tailDelivered: boolean;
};

/**
 * Deterministic daemon-process seam retained for crash-recovery E2E tests.
 * It is imported only by `test/daemon-entry.ts`; production `main.ts` has no
 * test-broker dependency or environment-selected implementation path. All runtime
 * discovery and commands are in-memory; no global user session is touched.
 * Only an operation's terminal transition publishes assistant output to its
 * session's gateway-owned stdio relays.
 */
export function testOnlyBrokerDependencies(): GlobalGjcClientDependencies {
	const sessions = new Map<string, string>();
	const repositories = new Map<string, string>();
	const operations = new Map<string, StubOperation>();
	const relays = new Map<string, Set<ReadableStreamDefaultController<Uint8Array>>>();
	const spawn = ({ cmd }: { cmd: readonly string[] }) => {
		const sessionId = argument(cmd, "--session");
		if (!sessionId || !repositories.has(sessionId)) throw new Error("unknown fake relay session");
		const controllers = relays.get(sessionId) ?? new Set<ReadableStreamDefaultController<Uint8Array>>();
		relays.set(sessionId, controllers);
		return relayChild(controllers, () => {
			if (controllers.size === 0) relays.delete(sessionId);
		});
	};
	const command: CliRunner = async (rawArgs) => {
		const args = withoutAgentDir(rawArgs);
		if (args[0] === "--version") return success("gjc/0.15.6\n");
		if (args[0] !== "sdk" || args[1] !== "session") return failure("stub_unsupported");

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
		if (args[2] === "send") {
			const sessionId = args[3];
			const opRef = argument(args, "--op-ref");
			const text = argument(args, "--text") ?? "";
			if (!sessionId || !opRef) return failure("invalid_request");
			if (!repositories.has(sessionId) || repositories.get(sessionId) !== argument(args, "--repo"))
				return failure("session_unavailable");
			if (operations.has(opRef)) return failure("client_ref_conflict");
			const operation: StubOperation = {
				sessionId,
				opRef,
				text,
				startedAt: Date.now(),
				state: "in_flight",
				tailDelivered: false,
			};
			operations.set(opRef, operation);
			setTimeout(() => {
				operation.reply = stubReply(operation.text);
				operation.terminalAt = Date.now();
				operation.state = "terminal_ok";
				const ts = new Date(operation.terminalAt).toISOString();
				const frames = [
					{
						type: "transcript",
						id: `${opRef}:assistant`,
						sessionId,
						opRef,
						ts,
						role: "assistant",
						content: [{ type: "text", text: operation.reply }],
					},
					{ type: "agent_end", id: `${opRef}:end`, sessionId, opRef, ts },
				];
				const bytes = new TextEncoder().encode(frames.map((frame) => JSON.stringify(frame)).join("\n") + "\n");
				for (const controller of relays.get(sessionId) ?? []) controller.enqueue(bytes);
			}, 50);
			return success({ sessionId, commandId: `stub-command-${opRef}` });
		}
		if (args[2] === "status") {
			const sessionId = args[3];
			const opRef = args[4];
			const operation = typeof opRef === "string" ? operations.get(opRef) : undefined;
			if (!operation || operation.sessionId !== sessionId)
				return success({ operationRef: opRef, status: { status: "unknown" }, summary: { completed: false } });
			return success({
				operationRef: opRef,
				status: {
					status: operation.state,
					commandId: `stub-command-${opRef}`,
					clientRef: opRef,
					acceptedAt: operation.startedAt,
					startedAt: operation.startedAt,
					receiptState: "present",
					...(operation.state === "terminal_ok"
						? { terminalAt: operation.terminalAt, outcome: { reason: "end_turn" } }
						: {}),
				},
				summary: { completed: operation.state === "terminal_ok" },
			});
		}
		if (args[2] === "tail") {
			const sessionId = args[3];
			const operation = [...operations.values()].reverse().find((entry) => entry.sessionId === sessionId);
			if (!operation || operation.state !== "terminal_ok") return success({ items: [], terminal: false });
			if (operation.tailDelivered) return success({ items: [], terminal: true });
			operation.tailDelivered = true;
			return success({
				items: [
					{
						kind: "transcript",
						payload: {
							role: "assistant",
							opRef: operation.opRef,
							ts: new Date(operation.terminalAt!).toISOString(),
							content: [{ type: "text", text: operation.reply }],
						},
					},
					{ kind: "agent_end", payload: { opRef: operation.opRef } },
				],
				terminal: true,
			});
		}
		if (args.includes("transcript.list")) {
			const sessionId = args[args.indexOf("query") + 1];
			if (!sessionId || !repositories.has(sessionId) || repositories.get(sessionId) !== argument(args, "--repo"))
				return failure("session_unavailable");
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
		if (args.includes("turn.steer")) return success({ status: "accepted" });
		if (args.includes("session.close")) return success({ closed: true });
		if (args.includes("session.resume")) return success({ resumed: true });
		if (args[2] === "inspect") {
			const sessionId = args[3];
			const repo = sessionId ? repositories.get(sessionId) : undefined;
			if (!repo || repo !== argument(args, "--repo")) return failure("session_unavailable");
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

function relayChild(
	controllers: Set<ReadableStreamDefaultController<Uint8Array>>,
	onClose: () => void,
): ReturnType<typeof Bun.spawn> {
	let finish = () => {};
	let controller: ReadableStreamDefaultController<Uint8Array>;
	let closed = false;
	const unregister = () => {
		if (closed) return;
		closed = true;
		controllers.delete(controller);
		onClose();
		finish();
	};
	const stdout = new ReadableStream<Uint8Array>({
		start(value) {
			controller = value;
			controllers.add(value);
		},
		cancel() {
			unregister();
		},
	});
	const exited = new Promise<number>((resolve) => {
		finish = () => resolve(0);
	});
	const kill = () => {
		if (closed) return;
		controller.close();
		unregister();
	};
	return { exited, kill, stdout } as unknown as ReturnType<typeof Bun.spawn>;
}
