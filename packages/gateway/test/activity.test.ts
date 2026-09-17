import { describe, expect, test } from "bun:test";
import type { TailFrame } from "../src/orchestrator/tail-runner";
import { deriveActivity } from "../src/server/activity";

function frame(rawKind: string, payload: Record<string, unknown> = {}, extra: Partial<TailFrame> = {}): TailFrame {
	return { kind: "event", rawKind, payload, steerEcho: false, idle: false, ...extra };
}

describe("deriveActivity", () => {
	test("a tool start names the tool and prefers the traced intent over the arguments", () => {
		expect(
			deriveActivity(
				frame("tool_execution_start", { toolName: "bash", intent: "Running the tests", args: { command: "bun test" } }),
				undefined,
			),
		).toEqual({ kind: "tool", label: "bash", detail: "Running the tests" });
	});

	test("without an intent the first recognisable argument is the detail", () => {
		expect(
			deriveActivity(frame("tool_execution_start", { toolName: "read", args: { path: "src/main.ts" } }), undefined),
		).toEqual({
			kind: "tool",
			label: "read",
			detail: "src/main.ts",
		});
		expect(
			deriveActivity(frame("tool_execution_start", { toolName: "edit", args: { _i: "Fixing the typo" } }), undefined),
		).toEqual({
			kind: "tool",
			label: "edit",
			detail: "Fixing the typo",
		});
	});

	test("labels and details are single-line and bounded because they land in a chat message", () => {
		const activity = deriveActivity(
			frame("tool_execution_start", {
				toolName: `x${"y".repeat(100)}`,
				intent: `line one\nline two\u0007${"z".repeat(200)}`,
			}),
			undefined,
		);
		expect(activity?.label.length).toBe(48);
		expect(activity?.label.endsWith("…")).toBe(true);
		expect(activity?.detail?.includes("\n")).toBe(false);
		expect(activity?.detail?.includes(String.fromCharCode(7))).toBe(false);
		expect(activity?.detail?.length).toBe(120);
	});

	test("the tail_activity projection only counts a started phase", () => {
		expect(deriveActivity(frame("tool_activity", { toolName: "bash", phase: "completed" }), undefined)).toBeUndefined();
		expect(deriveActivity(frame("tool_activity", { toolName: "bash", phase: "started" }), undefined)).toEqual({
			kind: "tool",
			label: "bash",
		});
	});

	test("a tool end means the model is thinking; assistant text means it is writing", () => {
		const tool = { kind: "tool" as const, label: "bash" };
		expect(deriveActivity(frame("tool_execution_end", { toolName: "bash" }), tool)).toEqual({
			kind: "thinking",
			label: "thinking",
		});
		expect(deriveActivity(frame("transcript", { role: "assistant" }, { assistantText: "hello" }), tool)).toEqual({
			kind: "writing",
			label: "writing",
		});
		// A steer echo is the user's text, not the persona writing.
		expect(
			deriveActivity(frame("transcript", { role: "user" }, { assistantText: "hi", steerEcho: true }), tool),
		).toEqual(tool);
	});

	test("unrelated frames keep the previous activity; a live draft while a tool runs keeps the tool", () => {
		const tool = { kind: "tool" as const, label: "bash", detail: "x" };
		expect(deriveActivity(frame("activity", { toolCalls: 3 }), tool)).toEqual(tool);
		expect(deriveActivity(frame("turn_stream", { phase: "delta" }), tool)).toEqual(tool);
		expect(deriveActivity(frame("turn_stream", { phase: "delta" }), undefined)).toEqual({
			kind: "writing",
			label: "writing",
		});
		expect(deriveActivity(frame("activity", {}), undefined)).toBeUndefined();
	});
});
