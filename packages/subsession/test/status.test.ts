import { describe, expect, test } from "bun:test";
import type { CliResult, ControllerOptions } from "../src/cli";
import {
	fetchOpState,
	isPromptStatus,
	isTerminalStatus,
	judgeOperation,
	type PromptStatusBody,
	parseStatusReport,
	projectOpState,
	requiresOperatorHold,
	type SupervisorOpState,
} from "../src/status";

const WORKTREE = "/wt/subsession-runtime";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";

const envelope = (result: unknown): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});

function statusBody(overrides: Partial<PromptStatusBody> = {}): PromptStatusBody {
	return { status: "in_flight", receiptState: "absent", ...overrides };
}

describe("isPromptStatus", () => {
	test("accepts exactly the five canonical statuses", () => {
		for (const status of ["accepted", "in_flight", "terminal_ok", "failed", "unknown"]) {
			expect(isPromptStatus(status)).toBe(true);
		}
	});

	test.each([
		"completed",
		"cancelled",
		"error",
		"running",
		"wait_timeout",
	])("rejects %p, which is not a top-level status", (status) => {
		expect(isPromptStatus(status)).toBe(false);
	});
});

describe("parseStatusReport", () => {
	test("reads the documented envelope shape", () => {
		const report = parseStatusReport(
			envelope({
				version: 1,
				operationRef: "gw-pr-a-1",
				status: {
					status: "in_flight",
					commandId: "cmd",
					turnId: "turn",
					clientRef: "gw-pr-a-1",
					acceptedAt: 1,
					startedAt: 2,
					receiptState: "absent",
				},
				summary: { completed: false },
			}),
		);
		expect(report.operationRef).toBe("gw-pr-a-1");
		expect(report.status).toMatchObject({ status: "in_flight", turnId: "turn", startedAt: 2 });
		expect(report.summaryCompleted).toBe(false);
	});

	test("keeps the outcome body so the stop reason stays readable", () => {
		const report = parseStatusReport(
			envelope({
				operationRef: "r",
				status: {
					status: "terminal_ok",
					terminalAt: 9,
					receiptState: "present",
					outcome: { kind: "stopped", reason: "cancelled", provenance: "client_cancel" },
				},
				summary: { completed: true },
			}),
		);
		expect(report.status.outcome).toEqual({
			kind: "stopped",
			reason: "cancelled",
			provenance: "client_cancel",
		});
	});

	test("refuses a status outside the contract instead of coercing it", () => {
		expect(() => parseStatusReport(envelope({ status: { status: "completed" } }))).toThrow(/non-canonical status/);
	});

	test("drops a receiptState that is not one of the four", () => {
		const report = parseStatusReport(envelope({ status: { status: "accepted", receiptState: "weird" } }));
		expect(report.status.receiptState).toBeUndefined();
	});

	test("keeps the error envelope so a deadline kill stays diagnosable (#9)", () => {
		const report = parseStatusReport(
			envelope({
				operationRef: "r",
				status: {
					status: "failed",
					acceptedAt: 1787803784311,
					startedAt: 1787803784312,
					terminalAt: 1787805584444,
					error: { code: "prompt_deadline_exceeded", message: "Prompt deadline exceeded." },
				},
				summary: { completed: true },
			}),
		);
		expect(report.status.error).toEqual({
			code: "prompt_deadline_exceeded",
			message: "Prompt deadline exceeded.",
		});
		expect(report.status.terminalAt).toBe(1787805584444);
	});

	test("an envelope without an error body leaves the field absent", () => {
		const report = parseStatusReport(envelope({ status: { status: "failed", receiptState: "present" } }));
		expect(report.status.error).toBeUndefined();
	});
});

describe("projectOpState", () => {
	test.each<[SupervisorOpState, PromptStatusBody]>([
		["accepted", statusBody({ status: "accepted" })],
		["running", statusBody({ status: "in_flight" })],
		["failed", statusBody({ status: "failed", receiptState: "present" })],
		["completed", statusBody({ status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } })],
		[
			"cancelled",
			statusBody({
				status: "terminal_ok",
				receiptState: "present",
				outcome: { kind: "stopped", reason: "cancelled", provenance: "client_cancel" },
			}),
		],
		[
			"stopped_incomplete",
			statusBody({ status: "terminal_ok", receiptState: "present", outcome: { reason: "max_tokens" } }),
		],
		[
			"stopped_incomplete",
			statusBody({ status: "terminal_ok", receiptState: "present", outcome: { reason: "refusal" } }),
		],
		["terminal_uncertain", statusBody({ status: "unknown", receiptState: "unknown" })],
		[
			"terminal_missing_receipt",
			statusBody({ status: "terminal_ok", receiptState: "missing", outcome: { reason: "end_turn" } }),
		],
	])("projects %s", (expected, body) => {
		expect(projectOpState(body)).toBe(expected);
	});

	test("a cancel is never projected as completed", () => {
		const state = projectOpState(
			statusBody({ status: "terminal_ok", receiptState: "present", outcome: { reason: "cancelled" } }),
		);
		expect(state).not.toBe("completed");
	});

	test("a terminal transition without a readable reason is not a success", () => {
		expect(projectOpState(statusBody({ status: "terminal_ok", receiptState: "present" }))).toBe("stopped_incomplete");
	});

	test("an unknown receipt on a terminal op is held, not claimed", () => {
		expect(
			projectOpState(statusBody({ status: "terminal_ok", receiptState: "unknown", outcome: { reason: "end_turn" } })),
		).toBe("terminal_uncertain");
	});
});

describe("isTerminalStatus / requiresOperatorHold", () => {
	test("only terminal_ok and failed are terminal", () => {
		expect(isTerminalStatus("terminal_ok")).toBe(true);
		expect(isTerminalStatus("failed")).toBe(true);
		expect(isTerminalStatus("accepted")).toBe(false);
		expect(isTerminalStatus("in_flight")).toBe(false);
		expect(isTerminalStatus("unknown")).toBe(false);
	});

	test("uncertain and receipt-missing states stop automation", () => {
		expect(requiresOperatorHold("terminal_uncertain")).toBe(true);
		expect(requiresOperatorHold("terminal_missing_receipt")).toBe(true);
		expect(requiresOperatorHold("completed")).toBe(false);
		expect(requiresOperatorHold("running")).toBe(false);
	});
});

describe("judgeOperation", () => {
	const liveSession = { live: true, deleted: false };

	test("live session with an in-flight op is running", () => {
		const judgement = judgeOperation(liveSession, statusBody({ status: "in_flight" }));
		expect(judgement).toMatchObject({ state: "running", recheck: false, hold: false });
	});

	test("live session with a terminal op reports the op as done", () => {
		const judgement = judgeOperation(
			liveSession,
			statusBody({ status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } }),
		);
		expect(judgement.state).toBe("completed");
	});

	test("stopped session with a retained terminal op is still terminal", () => {
		const judgement = judgeOperation(
			{ live: false, deleted: false },
			statusBody({ status: "failed", receiptState: "present" }),
		);
		expect(judgement).toMatchObject({ state: "failed", recheck: false });
		expect(judgement.detail).toMatch(/retained/);
	});

	test("stopped session with an active op is a stale snapshot needing re-read", () => {
		const judgement = judgeOperation({ live: false, deleted: false }, statusBody({ status: "in_flight" }));
		expect(judgement.recheck).toBe(true);
		expect(judgement.detail).toMatch(/inspect -> status -> inspect/);
	});

	test("unavailable session with an unknown op never authorises auto-recreation", () => {
		const judgement = judgeOperation(
			{ live: false, deleted: true },
			statusBody({ status: "unknown", receiptState: "unknown" }),
		);
		expect(judgement).toMatchObject({ state: "terminal_uncertain", hold: true, recheck: true });
		expect(judgement.detail).toMatch(/do not recreate/i);
	});

	test("unknown is neither running nor terminal", () => {
		const judgement = judgeOperation(liveSession, statusBody({ status: "unknown" }));
		expect(judgement.state).toBe("terminal_uncertain");
		expect(judgement.hold).toBe(true);
		expect(isTerminalStatus("unknown")).toBe(false);
	});

	test("ambiguous authority or a locator mismatch fails closed", () => {
		expect(judgeOperation({ ...liveSession, ambiguous: true }, statusBody({ status: "in_flight" }))).toMatchObject({
			state: "terminal_uncertain",
			hold: true,
		});
		expect(
			judgeOperation({ ...liveSession, locatorMatches: false }, statusBody({ status: "in_flight" })),
		).toMatchObject({ hold: true });
	});
});

describe("fetchOpState", () => {
	test("queries status by sessionId and op-ref", async () => {
		const calls: string[][] = [];
		const options: ControllerOptions = {
			repo: WORKTREE,
			run: async (args) => {
				calls.push([...args]);
				return envelope({ operationRef: "gw-1", status: { status: "accepted" } });
			},
		};
		const report = await fetchOpState(options, SESSION, "gw-1");
		expect(calls[0]).toEqual(["sdk", "session", "status", SESSION, "gw-1"]);
		expect(report.status.status).toBe("accepted");
	});

	test("summary.completed is carried but never used as a success verdict", async () => {
		const options: ControllerOptions = {
			repo: WORKTREE,
			run: async () =>
				envelope({
					operationRef: "gw-1",
					status: { status: "failed", receiptState: "present" },
					summary: { completed: true },
				}),
		};
		const report = await fetchOpState(options, SESSION, "gw-1");
		expect(report.summaryCompleted).toBe(true);
		expect(projectOpState(report.status)).toBe("failed");
	});
});
