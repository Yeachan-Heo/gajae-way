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

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
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

test("agent error retry succeeds on 2nd attempt with provider_transport error", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	const logs: string[] = [];
	let setupDone = false;

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});
	setupDone = true;

	enqueue("retry-success", "message that will fail then succeed");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;
	expect(first).toBeDefined();

	// Fail with a retryable provider_transport error
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	// Wait for failure detection and retry scheduling
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	// The failure should NOT be delivered yet
	expect(failures).toHaveLength(0);

	// Wait for the retry to fire (scheduled with backoff)
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	// A new dispatch should have been triggered with a new opRef
	const second = port.sends[1];
	expect(second).toBeDefined();
	expect(second!.opRef).not.toBe(first.opRef); // Different opRef (new attempt)

	// Now succeed on the retry
	port.complete(second!.opRef, "success after retry");
	await manager.tick(KEY);

	await eventually(
		() => database!.inboundTurnRow(second!.opRef)?.turn_state === "done",
		"retry turn did not complete successfully",
	);

	// No failure should have been delivered
	expect(failures).toHaveLength(0);
});

test("exhausted retries posts one failure", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	const logs: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => {
			logs.push(line);
		},
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("retry-exhaustion", "message that will fail multiple times");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;

	// Fail 1st attempt with provider_transport (retryAttempt=0, should retry)
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	// Wait for failure detection and retry scheduling
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	const second = port.sends[1]!;

	// Fail 2nd attempt with provider_transport (retryAttempt=1, should retry again)
	port.fail(second.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await eventually(() => logs.filter((l) => l.startsWith("agent_error_retry ")).length >= 2, "second retry not scheduled");
	await new Promise((resolve) => setTimeout(resolve, 3000));
	await manager.tick(KEY);

	const third = port.sends[2]!;

	// Fail 3rd attempt with provider_transport (retryAttempt=2, MAX_AGENT_ERROR_RETRY_ATTEMPTS=2, so 2 < 2 is false, should NOT retry)
	port.fail(third.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await manager.tick(KEY);

	// Now the failure should be delivered (exactly once)
	await eventually(() => failures.length === 1, "failure not delivered after retries exhausted");
	expect(failures[0]).toContain("provider_unavailable");
	// The message in the notice is the one provided to port.fail, not the SDK-generated one
	expect(failures[0]).toContain("Provider unavailable");
});

test("non-retryable provider_rejected errors fail immediately", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	const logs: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("non-retryable", "message that will be rejected");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;

	// Fail with a non-retryable provider_rejected error (quota/refusal, not transient)
	port.fail(first.opRef, "Provider rejected the request", {
		code: "provider_http_429",
		outcome: { kind: "failed", phase: "post_start", category: "provider_rejected", provenance: "agent_failed" },
	});

	// Wait for failure detection
	await new Promise((resolve) => setTimeout(resolve, 100));
	await manager.tick(KEY);

	// Failure should be delivered immediately (no retries)
	await eventually(() => failures.length === 1, "failure not delivered for non-retryable error");
	expect(failures[0]).toContain("provider_http_429");

	// No retry should have been scheduled
	expect(logs.some((l) => l.startsWith("agent_error_retry "))).toBe(false);
});

test("post_start agent_runtime prompt_failed is retried", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	const logs: string[] = [];

	await setupHarness(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("agent-runtime-retry", "message that will fail with agent error");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;

	// Fail with a retryable post_start agent_runtime error
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "prompt_failed",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	// Wait for failure detection and retry scheduling
	await new Promise((resolve) => setTimeout(resolve, 1500));
	await manager.tick(KEY);

	expect(failures).toHaveLength(0); // No failure delivered yet

	const secondSend = port.sends[1];
	expect(secondSend).toBeDefined();

	// Complete the retry successfully
	port.complete(secondSend!.opRef, "recovered response");
	await manager.tick(KEY);

	// No failure should have been posted
	expect(failures).toHaveLength(0);
});
