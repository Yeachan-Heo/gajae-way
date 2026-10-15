import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, type PersonaSessionManagerOptions } from "../src/orchestrator/persona-session";
import { formatFailureNotice } from "../src/orchestrator/rebind";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: Store | undefined;
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
	const accepted = database?.transaction((tx) =>
		database!.inbox.enqueue(tx, {
			messageId,
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			source: "platform",
		}),
	);
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

async function setupHarness(port: ScriptedSessionPort, options: Partial<PersonaSessionManagerOptions> = {}) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-agent-error-retry-"));
	database = await Store.open(join(home, "gateway.db"));
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		...options,
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
	if (!first) throw new Error("initial retryable request was not dispatched");

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

test("agent_runtime post_start failures are not replayed", async () => {
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
	if (!first) throw new Error("initial runtime-failure request was not dispatched");

	// Fail with agent_runtime post_start
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "prompt_failed",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "runtime failure not delivered");
	expect(failures[0]).toContain("prompt_failed");
	expect(port.sends).toHaveLength(1);
});

test("agent_error quota failures are classified without retry", async () => {
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
	if (!first) throw new Error("initial quota-failure request was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_quota_exhausted");

	// Issue #331 observed quota failures reported with this agent_runtime code.
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "agent_error",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});

	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "quota failure not delivered");
	expect(failures[0]).toContain("provider_quota_exhausted");
	expect(failures[0]).toContain("HTTP 402");
	expect(port.sends).toHaveLength(1);
});

test("a provider refusal reported as provider_transport is delivered once, never retried", async () => {
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

	enqueue("provider-refusal", "summarize this exploit write-up");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial refused request was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_refusal", undefined, "cyber");

	// A refusal is deterministic: replaying the same request only earns the same refusal.
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "agent_error",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "refusal not delivered");
	expect(failures[0]).toContain("provider_refusal");
	expect(failures[0]).toContain("(cyber)");
	expect(port.sends).toHaveLength(1);
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
	if (!first) throw new Error("initial rejected request was not dispatched");

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

test("provider failure after visible assistant output is not replayed", async () => {
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

	enqueue("visible-output", "work that produced a partial answer");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial visible-output request was not dispatched");

	port.emitAssistant(first.sessionId, "partial answer", null, first.opRef);
	await manager.tick(KEY);
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "failure after visible output not delivered");
	expect(port.sends).toHaveLength(1);
});

test("provider failure after completed tool activity is not replayed", async () => {
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

	enqueue("completed-tool", "work that executed a tool");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial tool-activity request was not dispatched");
	port.emitTool(first.sessionId, { toolName: "post_message" });
	port.emitToolEnd(first.sessionId, "post_message");
	await manager.tick(KEY);

	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "failure after tool activity not delivered");
	expect(port.sends).toHaveLength(1);
});

test("provider failure after an accepted steer is not replayed", async () => {
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

	enqueue("steered-trigger", "original request");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial steered request was not dispatched");
	enqueue("accepted-correction", "important correction");
	await manager.notifyInbound(KEY);
	await eventually(() => port.steers.length === 1, "correction was not steered");
	expect(database!.inbox.turnRows(first.opRef)).toEqual(
		expect.arrayContaining([expect.objectContaining({ message_id: "accepted-correction", turn_role: "steer" })]),
	);

	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "failure after accepted steer not delivered");
	expect(port.sends).toHaveLength(1);
});

test("provider quota evidence suppresses transport retry", async () => {
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

	enqueue("provider-quota", "billing failure");
	await manager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial provider-quota request was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_quota_exhausted");
	port.fail(first.opRef, "Provider quota exhausted", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager.tick(KEY);
	await eventually(() => failures.length === 1, "quota failure not delivered");
	expect(failures[0]).toContain("provider_quota_exhausted");
	expect(port.sends).toHaveLength(1);
});

test("provider retry budget is independent of prior submission retries", async () => {
	let sendCount = 0;
	const failures: string[] = [];
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			sendCount++;
			if (sendCount === 1) {
				scripted.fail(input.opRef, "Prompt submission failed", {
					code: "internal",
					outcome: { kind: "failed", phase: "submission", category: "agent_runtime", provenance: "agent_failed" },
				});
				return;
			}
			scripted.fail(input.opRef, "Provider unavailable", {
				code: "provider_unavailable",
				outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
			});
		},
	});

	await setupHarness(port, {
		submissionRetryDelayMs: 1,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
	});

	enqueue("submission-then-provider", "retry budget isolation");
	await manager!.notifyInbound(KEY);
	await eventually(
		() => failures.length === 1 && manager!.state(KEY) === "idle",
		"provider retries did not exhaust after a submission retry",
		10_000,
	);
	expect(port.sends).toHaveLength(4);
	expect(failures).toHaveLength(1);
});

test("permission panels make provider replay unsafe", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	let permissionRequests = 0;

	await setupHarness(port, {
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error }) => {
				failures.push(formatFailureNotice(error));
			},
		}),
		onReverseRequest: () => {
			permissionRequests++;
		},
	});

	enqueue("permission-before-failure", "permission-gated action");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("permission-gated request was not dispatched");
	await port.emitPermissionRequest(first.sessionId);
	expect(permissionRequests).toBe(1);
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager!.tick(KEY);
	await eventually(() => failures.length === 1, "permission-gated failure was not delivered");
	expect(port.sends).toHaveLength(1);
});

test("a queued retry cannot clear a turn created by reset", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const timers: { work: () => void; delayMs: number; canceled: boolean }[] = [];
	let unblockDiscard!: () => void;
	let signalDiscardStarted!: () => void;
	const discardGate = new Promise<void>((resolve) => {
		unblockDiscard = resolve;
	});
	const discardStarted = new Promise<void>((resolve) => {
		signalDiscardStarted = resolve;
	});

	await setupHarness(port, {
		setTimeout: (work, delayMs) => {
			const timer = { work, delayMs, canceled: false };
			timers.push(timer);
			return timer;
		},
		clearTimeout: (timer) => {
			const scheduled = timers.find((candidate) => candidate === timer);
			if (scheduled) scheduled.canceled = true;
		},
		onInboundDiscard: async (messageIds) => {
			if (messageIds.length === 0) return;
			signalDiscardStarted();
			await discardGate;
		},
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});

	enqueue("retry-before-reset", "old request");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial request was not dispatched");
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});
	await manager!.tick(KEY);
	await eventually(() => timers.some((timer) => timer.delayMs === 1_000), "provider retry was not scheduled");

	enqueue("discarded-during-reset", "stale request");
	await new Promise((resolve) => setTimeout(resolve, 20));
	const resetting = manager!.reset(KEY, JSON.stringify(ORIGIN));
	await discardStarted;
	const retryTimer = timers.find((timer) => timer.delayMs === 1_000 && !timer.canceled);
	if (!retryTimer) throw new Error("provider retry timer was not captured");
	// The timer observes the old turn before reset finishes, then queues behind it.
	retryTimer.work();
	enqueue("replacement-after-reset", "replacement request");
	unblockDiscard();
	await resetting;
	await manager!.tick(KEY);
	await eventually(() => port.sends.length === 2, "reset replacement was not dispatched");
	expect(port.sends.map((send) => send.text)).toEqual(["old request", "replacement request"]);
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
	if (!first) throw new Error("initial stalled-tool request was not dispatched");

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
	if (!first) throw new Error("initial exhaustion-test request was not dispatched");

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
