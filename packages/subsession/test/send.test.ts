import { describe, expect, test } from "bun:test";
import type { CliResult, ControllerOptions } from "../src/cli";
import { GjcCliError } from "../src/cli";
import {
	assertValidOpRef,
	classifyState,
	isOpRefRejection,
	MAX_OP_REF_LENGTH,
	newOpRef,
	OpRefError,
	OpRefLedger,
	OpRefRejectedError,
	pollStatus,
	reconcileOrSend,
	type SendReceipt,
	sendPrompt,
} from "../src/send";

const WORKTREE = "/wt/subsession-runtime";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";

const ok = (result: unknown): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});

function controller(result: unknown, calls: string[][] = []): ControllerOptions & { calls: string[][] } {
	return {
		repo: WORKTREE,
		calls,
		run: async (args) => {
			calls.push([...args]);
			return ok(result);
		},
	};
}

describe("assertValidOpRef", () => {
	test("accepts a minted ref", () => {
		expect(() => assertValidOpRef("gw-subsession-runtime-01hqwe")).not.toThrow();
	});

	test.each([
		["", "empty"],
		["Gw-Upper", "uppercase"],
		["-leading", "leading dash"],
		["has space", "space"],
		["has/slash", "slash"],
	])("rejects %p (%s)", (candidate) => {
		expect(() => assertValidOpRef(candidate)).toThrow(OpRefError);
	});

	test(`rejects anything longer than ${MAX_OP_REF_LENGTH} chars`, () => {
		expect(() => assertValidOpRef(`gw-${"a".repeat(MAX_OP_REF_LENGTH)}`)).toThrow(/at most 128/);
		expect(() => assertValidOpRef(`gw-${"a".repeat(MAX_OP_REF_LENGTH - 3)}`)).not.toThrow();
	});
});

describe("newOpRef", () => {
	test("uses the gw-<taskKey>-<ulid> shape", () => {
		expect(newOpRef("pr-a", () => "01hqwertyuiop")).toBe("gw-pr-a-01hqwertyuiop");
	});

	test("two refs for the same task differ", () => {
		expect(newOpRef("pr-a")).not.toBe(newOpRef("pr-a"));
	});

	test("generated refs are always valid", () => {
		for (let index = 0; index < 50; index += 1) {
			expect(() => assertValidOpRef(newOpRef("pr-a"))).not.toThrow();
		}
	});
});

describe("OpRefLedger", () => {
	test("rejects reuse inside one session", () => {
		const ledger = new OpRefLedger();
		ledger.issue(SESSION, "gw-a-1");
		expect(() => ledger.issue(SESSION, "gw-a-1")).toThrow(/already used/);
	});

	test("the same ref in another session is not a collision", () => {
		const ledger = new OpRefLedger();
		ledger.issue(SESSION, "gw-a-1");
		expect(() => ledger.issue("other-session", "gw-a-1")).not.toThrow();
		expect(ledger.has("other-session", "gw-a-1")).toBe(true);
	});
});

describe("sendPrompt", () => {
	test("builds the documented CLI invocation with an explicit op-ref", async () => {
		const calls: string[][] = [];
		const options = controller({ commandId: "cmd-1", turnId: "turn-1" }, calls);
		const receipt = await sendPrompt(options, {
			sessionId: SESSION,
			text: "do the thing",
			taskKey: "pr-a",
			opRef: "gw-pr-a-01hq",
		});
		expect(calls[0]).toEqual([
			"sdk",
			"session",
			"send",
			SESSION,
			"--repo",
			WORKTREE,
			"--text",
			"do the thing",
			"--op-ref",
			"gw-pr-a-01hq",
		]);
		expect(receipt).toMatchObject({
			sessionId: SESSION,
			operationRef: "gw-pr-a-01hq",
			commandId: "cmd-1",
			turnId: "turn-1",
			taskKey: "pr-a",
		});
		expect(Date.parse(receipt.acceptedAt)).toBeGreaterThan(0);
	});

	test("adds the bounded wait only when a wait budget is given", async () => {
		const calls: string[][] = [];
		const options = controller({}, calls);
		await sendPrompt(options, {
			sessionId: SESSION,
			text: "x",
			taskKey: "pr-a",
			waitTimeoutMs: 30_000,
		});
		expect(calls[0]?.slice(-3)).toEqual(["--wait", "--timeout-ms", "30000"]);
	});

	test("mints a fresh op-ref when the caller omits one, and records it", async () => {
		const ledger = new OpRefLedger();
		const options = controller({});
		const receipt = await sendPrompt(options, {
			sessionId: SESSION,
			text: "x",
			taskKey: "pr-a",
			ledger,
		});
		expect(receipt.operationRef.startsWith("gw-pr-a-")).toBe(true);
		expect(ledger.has(SESSION, receipt.operationRef)).toBe(true);
	});

	test("refuses to reuse an op-ref already issued for the session", async () => {
		const ledger = new OpRefLedger();
		ledger.issue(SESSION, "gw-pr-a-dup");
		await expect(
			sendPrompt(controller({}), {
				sessionId: SESSION,
				text: "x",
				taskKey: "pr-a",
				opRef: "gw-pr-a-dup",
				ledger,
			}),
		).rejects.toThrow(/already used/);
	});

	test("refuses an empty prompt", async () => {
		await expect(sendPrompt(controller({}), { sessionId: SESSION, text: "   ", taskKey: "pr-a" })).rejects.toThrow(
			OpRefError,
		);
	});
});

describe("classifyState", () => {
	test.each(["completed", "failed", "cancelled", "error"])("%s is terminal", (state) => {
		expect(classifyState(state).terminal).toBe(true);
	});

	test("wait_timeout is NOT terminal: the turn is still running", () => {
		expect(classifyState("wait_timeout").terminal).toBe(false);
	});

	test.each(["running", "active", "pending", "accepted"])("%s is not terminal", (state) => {
		expect(classifyState(state).terminal).toBe(false);
	});

	test("an unknown state is treated as still running", () => {
		expect(classifyState("something-new").terminal).toBe(false);
	});
});

describe("pollStatus", () => {
	const receipt: SendReceipt = {
		sessionId: SESSION,
		operationRef: "gw-pr-a-01hq",
		acceptedAt: new Date(0).toISOString(),
		taskKey: "pr-a",
	};

	test("reconciles a stored receipt by sessionId and op-ref", async () => {
		const calls: string[][] = [];
		const options = controller({ state: "running" }, calls);
		const outcome = await pollStatus(options, receipt);
		expect(calls[0]).toEqual(["sdk", "session", "status", SESSION, "gw-pr-a-01hq", "--repo", WORKTREE]);
		expect(outcome).toMatchObject({ state: "running", terminal: false });
	});

	test("marks a completed op terminal", async () => {
		const outcome = await pollStatus(controller({ status: "completed" }), receipt);
		expect(outcome).toMatchObject({ state: "completed", terminal: true });
	});

	test("an absent state is not treated as done", async () => {
		const outcome = await pollStatus(controller({}), receipt);
		expect(outcome).toMatchObject({ state: "unknown", terminal: false });
	});
});

describe("op-ref rejection", () => {
	test("a duplicate clientRef surfaces as OpRefRejectedError, not a generic CLI failure", async () => {
		const options: ControllerOptions = {
			repo: WORKTREE,
			run: async () => ({
				exitCode: 0,
				stdout: JSON.stringify({
					ok: false,
					error: { code: "client_ref_duplicate", message: "clientRef already used" },
				}),
				stderr: "",
			}),
		};
		await expect(
			sendPrompt(options, { sessionId: SESSION, text: "x", taskKey: "pr-a", opRef: "gw-pr-a-1" }),
		).rejects.toBeInstanceOf(OpRefRejectedError);
	});

	test("the rejection tells the caller to reconcile rather than remint", async () => {
		const error = new OpRefRejectedError("gw-pr-a-1", null);
		expect(error.message).toMatch(/reconcile it with status/);
	});

	test("an unrelated CLI failure is not misread as an op-ref rejection", async () => {
		const options: ControllerOptions = {
			repo: WORKTREE,
			run: async () => ({
				exitCode: 0,
				stdout: JSON.stringify({ ok: false, error: { code: "broker_down" } }),
				stderr: "",
			}),
		};
		await expect(sendPrompt(options, { sessionId: SESSION, text: "x", taskKey: "pr-a" })).rejects.not.toBeInstanceOf(
			OpRefRejectedError,
		);
	});

	test("isOpRefRejection needs both an op-ref subject and a duplication verb", () => {
		expect(isOpRefRejection(new GjcCliError("clientRef already used", 0, ""))).toBe(true);
		expect(isOpRefRejection(new GjcCliError("session already deleted", 0, ""))).toBe(false);
		expect(isOpRefRejection(new Error("clientRef already used"))).toBe(false);
	});
});

describe("reconcileOrSend", () => {
	const stored: SendReceipt = {
		sessionId: SESSION,
		operationRef: "gw-pr-a-01hq",
		acceptedAt: new Date(0).toISOString(),
		taskKey: "pr-a",
	};

	test("resumes a stored receipt instead of resending after a restart", async () => {
		const calls: string[][] = [];
		const options = controller({ state: "running" }, calls);
		const result = await reconcileOrSend(options, {
			sessionId: SESSION,
			text: "x",
			taskKey: "pr-a",
			existing: stored,
		});
		expect(result.kind).toBe("resumed");
		expect(calls[0]?.[2]).toBe("status");
		expect(calls.flat()).not.toContain("send");
	});

	test("does not resend even when the stored op already finished", async () => {
		const calls: string[][] = [];
		const options = controller({ state: "completed" }, calls);
		const result = await reconcileOrSend(options, {
			sessionId: SESSION,
			text: "x",
			taskKey: "pr-a",
			existing: stored,
		});
		expect(result).toMatchObject({ kind: "resumed", outcome: { terminal: true } });
		expect(calls).toHaveLength(1);
	});

	test("sends only when there is no prior record", async () => {
		const calls: string[][] = [];
		const options = controller({ commandId: "c" }, calls);
		const result = await reconcileOrSend(options, { sessionId: SESSION, text: "x", taskKey: "pr-a" });
		expect(result.kind).toBe("sent");
		expect(calls[0]?.[2]).toBe("send");
	});
});
