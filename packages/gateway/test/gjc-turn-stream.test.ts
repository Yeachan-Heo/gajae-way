import { expect, test } from "bun:test";
import { GjcTurnStream } from "../src/orchestrator/gjc-client";

const line = (value: unknown) => `${JSON.stringify(value)}\n`;

test("counts tool executions and captures the final assistant text across chunk splits", () => {
	const stream = new GjcTurnStream();
	stream.feed(line({ type: "session", id: "s-1" }));
	stream.feed(line({ type: "tool_execution_start", toolName: "bash" }));
	stream.feed(line({ type: "tool_execution_end" }));
	stream.feed(line({ type: "tool_execution_start", toolName: "read" }));
	// Interim assistant message: superseded by the later one.
	stream.feed(
		line({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "thinking aloud" }] } }),
	);
	// A chunk boundary in the middle of a JSON line must not corrupt parsing.
	const final = line({
		type: "message_end",
		message: { role: "assistant", content: [{ type: "text", text: "final answer" }] },
	});
	stream.feed(final.slice(0, 25));
	stream.feed(final.slice(25));
	expect(stream.toolCalls).toBe(2);
	expect(stream.finalText).toBe("final answer");
});

test("ignores non-JSON noise and non-assistant messages", () => {
	const stream = new GjcTurnStream();
	stream.feed("plain stderr-ish noise\n");
	stream.feed(
		line({ type: "message_end", message: { role: "toolResult", content: [{ type: "text", text: "tool output" }] } }),
	);
	stream.feed(
		line({ type: "message_end", message: { role: "user", content: [{ type: "text", text: "prompt echo" }] } }),
	);
	expect(stream.toolCalls).toBe(0);
	expect(stream.finalText).toBeUndefined();
});

test("tracks output tokens: delta estimate between messages, exact usage on message end", () => {
	const stream = new GjcTurnStream();
	stream.feed(`${JSON.stringify({ type: "message_update", assistantMessageEvent: { delta: "x".repeat(40) } })}\n`);
	expect(stream.outputTokens).toBe(10);
	stream.feed(
		`${JSON.stringify({ type: "message_end", message: { role: "assistant", usage: { output: 25 }, content: [{ type: "text", text: "done" }] } })}\n`,
	);
	expect(stream.outputTokens).toBe(25);
	stream.feed(`${JSON.stringify({ type: "message_update", assistantMessageEvent: { delta: "abcd" } })}\n`);
	expect(stream.outputTokens).toBe(26);
});
