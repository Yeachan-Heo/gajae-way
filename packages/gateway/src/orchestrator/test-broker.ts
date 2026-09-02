import type { CliResult, CliRunner } from "@gajaeway/subsession";
import type { BrokerSupervisorDependencies, SpawnFn } from "./broker";

type StubOperation = {
	readonly sessionId: string;
	readonly opRef: string;
	readonly text: string;
	state: "in_flight" | "terminal_ok";
	tailDelivered: boolean;
};

/**
 * Deterministic daemon-process seam retained for crash-recovery E2E tests.
 * It is imported only by `test/daemon-entry.ts`; production `main.ts` has no
 * test-broker dependency or environment-selected implementation path.
 */
export function testOnlyBrokerDependencies(): BrokerSupervisorDependencies {
	const sessions = new Map<string, string>();
	const operations = new Map<string, StubOperation>();
	let sessionCount = 0;
	const command: CliRunner = async (rawArgs) => {
		const args = withoutAgentDir(rawArgs);
		if (args[0] === "--version") return success("gjc/0.15.6\n");
		if (args[0] !== "sdk" || args[1] !== "session") return failure("stub_unsupported");

		if (args[2] === "list") return success({ sessions: [] });
		if (args[2] === "--scope") return success({ sessions: [] });
		if (args.includes("session.create")) {
			const key = argument(args, "--idempotency-key") ?? `stub-create-${++sessionCount}`;
			const sessionId = sessions.get(key) ?? `stub-session-${++sessionCount}`;
			sessions.set(key, sessionId);
			return success({ sessionId });
		}
		if (args[2] === "send") {
			const sessionId = args[3];
			const opRef = argument(args, "--op-ref");
			const text = argument(args, "--text") ?? "";
			if (!sessionId || !opRef) return failure("invalid_request");
			if (operations.has(opRef)) return failure("client_ref_conflict");
			const operation: StubOperation = { sessionId, opRef, text, state: "in_flight", tailDelivered: false };
			operations.set(opRef, operation);
			setTimeout(() => {
				operation.state = "terminal_ok";
			}, 50);
			return success({ sessionId, commandId: `stub-command-${opRef}` });
		}
		if (args[2] === "status") {
			const sessionId = args[3];
			const opRef = args[4];
			const operation = typeof opRef === "string" ? operations.get(opRef) : undefined;
			const status = operation && operation.sessionId === sessionId ? operation.state : "unknown";
			return success({ operationRef: opRef, status: { status }, summary: { completed: status === "terminal_ok" } });
		}
		if (args[2] === "tail") {
			const sessionId = args[3];
			const operation = [...operations.values()].reverse().find((entry) => entry.sessionId === sessionId);
			if (!operation || operation.state !== "terminal_ok") return success({ items: [], terminal: false });
			if (operation.tailDelivered) return success({ items: [], terminal: true });
			operation.tailDelivered = true;
			return success({
				items: [
					{ kind: "transcript", payload: { role: "assistant", content: [{ text: stubReply(operation.text) }] } },
					{ kind: "agent_end", payload: {} },
				],
				terminal: true,
			});
		}
		if (args.includes("session.last_assistant")) {
			const sessionId = args[args.indexOf("query") + 1];
			const operation = [...operations.values()].reverse().find((entry) => entry.sessionId === sessionId);
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					type: "query_response",
					ok: true,
					page: { items: [operation ? stubReply(operation.text) : ""], complete: true },
				}),
				stderr: "",
			};
		}
		if (args.includes("model.set")) return success({ changed: true });
		if (args.includes("turn.steer")) return success({ status: "accepted" });
		if (args.includes("session.close")) return success({ closed: true });
		if (args.includes("session.resume")) return success({ resumed: true });
		if (args[2] === "inspect") return success({ session: undefined });
		return failure("stub_unsupported");
	};
	return {
		command,
		spawn: inertChild as SpawnFn,
		healthProbe: async () => true,
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

function inertChild(): ReturnType<typeof Bun.spawn> {
	let exit = () => {};
	const exited = new Promise<number>((resolve) => {
		exit = () => resolve(0);
	});
	return { exited, kill: exit } as unknown as ReturnType<typeof Bun.spawn>;
}
