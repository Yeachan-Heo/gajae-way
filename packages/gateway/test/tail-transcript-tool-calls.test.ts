import { expect, test } from "bun:test";
import { decodeStreamLine } from "../src/orchestrator/tail-runner";
import { deriveActivity } from "../src/server/activity";

// Exact shapes captured from gjc 0.16.7 (`sdk session tail --all-events`,
// 2026-09-17). The host records tool use as content blocks on transcript rows
// and emits no tool_activity / tool_execution_* frames at all.
const assistantToolRow = JSON.stringify({
	type: "event",
	kind: "transcript",
	generation: 1,
	seq: 41,
	payload: {
		id: "6b1c0e2a",
		role: "assistant",
		ts: "2026-09-17T08:46:10.000Z",
		content: [
			{ type: "thinking", text: "need to look at the file" },
			{
				type: "toolCall",
				id: "toolu_01Hsj3zX4B9qFjbKfQ8TQ58L",
				name: "bash",
				arguments: { command: "ls -l", cwd: "/w" },
			},
		],
	},
});
const toolResultRow = JSON.stringify({
	type: "event",
	kind: "transcript",
	generation: 1,
	seq: 42,
	payload: { id: "4d355c06", role: "toolResult", ts: "2026-09-17T08:46:11.000Z", textSummary: "total 12" },
});
const assistantTextRow = JSON.stringify({
	type: "event",
	kind: "transcript",
	generation: 1,
	seq: 43,
	payload: {
		id: "9f0d1c3b",
		role: "assistant",
		ts: "2026-09-17T08:46:12.000Z",
		content: [{ type: "text", text: "첫 파일 봤어요" }],
	},
});

test("an assistant transcript row with a toolCall block counts as a tool start", () => {
	const [frame] = decodeStreamLine(assistantToolRow);
	expect(frame).toBeDefined();
	// The counter in server.ts reads exactly these two things.
	expect(frame?.payload.toolCallStarted).toBe(true);
	expect(frame?.rawKind).toBe("tool_execution_start");
	// Attribution/dedupe still see a transcript row with its ring identity.
	expect(frame?.kind).toBe("transcript");
	expect(frame?.eventId).toBe("1:41");
	// A row that is only a tool call carries no deliverable text.
	expect(frame?.assistantText).toBeUndefined();
});

test("the activity hint names the tool from the block, so presence leaves the queued phase", () => {
	const [frame] = decodeStreamLine(assistantToolRow);
	expect(deriveActivity(frame!, undefined)).toEqual({ kind: "tool", label: "bash", detail: "ls -l" });
	const [result] = decodeStreamLine(toolResultRow);
	expect(result?.rawKind).toBe("tool_execution_end");
	expect(deriveActivity(result!, { kind: "tool", label: "bash" })).toEqual({ kind: "thinking", label: "thinking" });
});

test("a plain assistant text row is unchanged: deliverable text, no tool start", () => {
	const [frame] = decodeStreamLine(assistantTextRow);
	expect(frame?.rawKind).toBe("transcript");
	expect(frame?.payload.toolCallStarted).toBeUndefined();
	expect(frame?.assistantText).toBe("첫 파일 봤어요");
});

test("the projection recognises the block spellings hosts have used", () => {
	for (const type of ["toolCall", "tool_call", "tool_use"]) {
		const line = JSON.stringify({
			type: "event",
			kind: "transcript",
			payload: { role: "assistant", content: [{ type, name: "read", input: { path: "a.md" } }] },
		});
		const [frame] = decodeStreamLine(line);
		expect(frame?.payload.toolCallStarted).toBe(true);
		expect(deriveActivity(frame!, undefined)).toEqual({ kind: "tool", label: "read", detail: "a.md" });
	}
});
