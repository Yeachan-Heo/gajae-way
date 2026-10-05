import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { formatFailureNotice } from "../src/orchestrator/rebind";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "persona" } as const;
const KEY = "loopback/loopback/persona";

function enqueue(messageId: string, body: string): void {
	const accepted = database?.inboundEnqueue({
		messageId,
		originKey: KEY,
		originRefJson: JSON.stringify(ORIGIN),
		body,
		source: "platform",
	});
	expect(accepted).toBe(true);
}

async function eventually(predicate: () => boolean, message: string, timeoutMs = 2000): Promise<void> {
	const startTime = Date.now();
	while (Date.now() - startTime < timeoutMs) {
		if (predicate()) return;
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	expect(predicate(), message).toBe(true);
}

async function setupHarness(port: ScriptedSessionPort) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-agent-error-retry-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
	});
}

test("provider_transport error retries successfully", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("provider-retry", "network error test");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with provider_transport
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	// Wait for retry to be scheduled and processed
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	// Retry should have been dispatched
	const second = port.sends[1];
	expect(second).toBeDefined();
	expect(failures).toHaveLength(0);

	// Succeed on retry
	port.complete(second?.opRef ?? "", "success after retry");
	await manager.tick(KEY);
	expect(failures).toHaveLength(0);
});

test("agent_runtime post_start error sends continuation prompt", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("agent-runtime-retry", "agent error test");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with agent_runtime post_start
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "prompt_failed",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	// Wait for continuation retry to be scheduled
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	// Continuation should have been sent (not requeue)
	const continuation = port.sends[1];
	expect(continuation).toBeDefined();
	expect(continuation?.text).toContain("error"); // Should mention error in continuation
	expect(failures).toHaveLength(0);

	// Succeed on retry
	port.complete(continuation?.opRef ?? "", "recovered response");
	await manager.tick(KEY);
	expect(failures).toHaveLength(0);
});

test("non-retryable agent_error code fails immediately", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("agent-error-code", "agent runtime error");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with agent_error code (retryable)
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "agent_error",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	// Continuation should be sent (not immediate failure)
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);
	const continuation = port.sends[1];
	expect(continuation).toBeDefined(); // Continuation sent, not immediate failure
	expect(failures).toHaveLength(0); // No failure yet
});

test("non-retryable errors fail immediately without retry", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("non-retryable", "quota exceeded");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with non-retryable error
	port.fail(first.opRef, "Quota exceeded", {
		code: "provider_http_429",
		outcome: { kind: "failed", phase: "post_start", category: "provider_rejected", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 100));
	await manager.tick(KEY);

	// Failure should be delivered immediately
	await eventually(() => failures.length === 1, "failure not delivered");
	expect(failures[0]).toContain("provider_http_429");
	expect(port.sends).toHaveLength(1); // No retry dispatched
});

test("continuation prompt content does not request re-running tools", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("no-duplicate-tools", "agent error test");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with agent_runtime error
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "prompt_failed",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	const continuation = port.sends[1];
	expect(continuation).toBeDefined();

	// Verify continuation prompt explicitly warns against re-running tools
	expect(continuation?.text).toContain("tool"); // Should mention tools
	expect(continuation?.text).toContain("duplicate"); // Should warn about duplication
	expect(failures).toHaveLength(0); // No failure yet
});

test("tool stalled/hung error is NOT retried", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("tool-stalled", "tool hung test");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with tool_timeout error (non-retryable)
	port.fail(first?.opRef ?? "", "Tool timed out", {
		code: "tool_timeout",
		outcome: { kind: "failed", phase: "tool_call", category: "tool_failed", provenance: "tool_stalled" },
	});

	await new Promise((resolve) => setTimeout(resolve, 100));
	await manager.tick(KEY);

	// Should fail immediately without retry
	await eventually(() => failures.length === 1, "failure not delivered");
	expect(failures[0]).toContain("tool_timeout");
	expect(port.sends).toHaveLength(1); // No retry dispatched
});

test("when both retries fail, failure message posted exactly once", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("exhausted-retries", "retry exhaustion test");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	expect(first).toBeDefined();

	// Fail with provider_transport error on first attempt
	port.fail(first?.opRef ?? "", "Provider down", {
		code: "provider_down",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	// First retry should have been dispatched
	const retry1 = port.sends[1];
	expect(retry1).toBeDefined();
	expect(failures).toHaveLength(0); // No failure yet

	// Fail the first retry
	port.fail(retry1?.opRef ?? "", "Provider still down", {
		code: "provider_down",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 2500));
	await manager.tick(KEY);

	// Second retry should have been dispatched
	const retry2 = port.sends[2];
	expect(retry2).toBeDefined();
	expect(failures).toHaveLength(0); // Still no failure

	// Fail the second retry (exhausted)
	port.fail(retry2?.opRef ?? "", "Provider permanently down", {
		code: "provider_down",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 100));
	await manager.tick(KEY);

	// Failure should be delivered exactly once
	await eventually(() => failures.length === 1, "failure not delivered", 5000);
	expect(failures).toHaveLength(1);
	expect(failures[0]).toContain("provider_down");
	expect(port.sends).toHaveLength(3); // Original + 2 retries
});
