import type { ChatProgressActivity } from "@gajaeway/protocol";
import type { TailFrame } from "../orchestrator/tail-runner";

/**
 * What the persona is doing right now, derived from the live tail.
 *
 * The status hint used to say "working… (2m, 3 tools)" and nothing else, which
 * tells a reader that the turn is alive but not what it is alive doing. The
 * gjc host streams tool lifecycle frames (`tool_execution_start` with the tool
 * name, its args, and - when intent tracing is on - the model's own one-line
 * reason for the call) and assistant text; this folds them into one small,
 * bounded activity record an adapter can render next to the counters.
 *
 * Bounded on purpose: labels come from the model/tool side and end up in a
 * chat message, so they are truncated and stripped of control characters here,
 * once, rather than in every adapter.
 */

const LABEL_MAX = 48;
const DETAIL_MAX = 120;

/** The tool frame kinds the host emits (packages/agent AgentSessionEvent). */
const TOOL_START = new Set(["tool_execution_start", "tool_activity"]);
const TOOL_END = new Set(["tool_execution_end"]);

export function deriveActivity(
	frame: TailFrame,
	previous: ChatProgressActivity | undefined,
): ChatProgressActivity | undefined {
	const payload = frame.payload;
	const kind = frame.rawKind;
	if (
		TOOL_START.has(kind) &&
		(payload.toolCallStarted === true || payload.phase === undefined || isStartPhase(payload.phase))
	) {
		const label = clean(payload.toolName, LABEL_MAX) ?? "tool";
		const detail = clean(payload.intent, DETAIL_MAX) ?? summarizeArgs(payload.args);
		return { kind: "tool", label, ...(detail ? { detail } : {}) };
	}
	if (TOOL_END.has(kind)) {
		// The model gets the result back and thinks about it next.
		return { kind: "thinking", label: "thinking" };
	}
	if (frame.assistantText && !frame.steerEcho) return { kind: "writing", label: "writing" };
	if (kind === "turn_stream" && payload.phase !== "finalized") {
		// Live drafts and reasoning summaries: the model is composing.
		return previous?.kind === "tool" ? previous : { kind: "writing", label: "writing" };
	}
	return previous;
}

function isStartPhase(phase: unknown): boolean {
	return phase === "started" || phase === "start";
}

/** A short, safe rendering of a tool's arguments when no intent was traced. */
function summarizeArgs(args: unknown): string | undefined {
	if (typeof args !== "object" || args === null) return undefined;
	const record = args as Record<string, unknown>;
	// Prefer the fields humans recognise; fall back to the first short string.
	for (const key of ["_i", "intent", "command", "path", "pattern", "query", "url", "description"]) {
		const value = clean(record[key], DETAIL_MAX);
		if (value) return value;
	}
	for (const value of Object.values(record)) {
		const text = clean(value, DETAIL_MAX);
		if (text) return text;
	}
	return undefined;
}

function clean(value: unknown, max: number): string | undefined {
	if (typeof value !== "string") return undefined;
	// One line, no control characters: this lands inside a chat message.
	let line = "";
	let gap = false;
	for (const char of value) {
		const code = char.codePointAt(0) ?? 0;
		if (code < 0x20 || code === 0x7f) {
			gap = true;
			continue;
		}
		if (gap && line.length > 0) line += " ";
		gap = false;
		line += char;
	}
	line = line.trim();
	if (!line) return undefined;
	return line.length > max ? `${line.slice(0, max - 1)}…` : line;
}
