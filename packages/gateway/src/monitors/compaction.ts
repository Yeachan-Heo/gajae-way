/**
 * Monitor session safety net (issue #68).
 *
 * What this is NOT: the primary defence against an unbounded monitor session.
 * That is gjc's own native auto-compaction. Measured: the `-p --mode json`
 * non-interactive path goes through `AgentSession.prompt()` just like the
 * interactive one, so native auto-compaction already applies to monitor
 * authoring turns, and the GJC SDK additionally exposes a `compaction.run`
 * control action for an explicit request.
 *
 * What actually broke in production was therefore not "no compaction" but
 * "compaction fired too late": the session overflowed with 0 entries left and
 * the authoring turn came back empty, so dispatch settled as `internal_error`.
 * That host now runs gjc 0.15.5 with adaptive compaction (base 70%, floor 45%),
 * which restored the first line of defence.
 *
 * This module is the SECOND line: it detects the case where native compaction
 * silently fails again, and only then rolls the session as a last resort.
 * Invariants:
 * - A healthy monitor is NEVER rolled. Turn count is observational only.
 * - A roll requires consecutive context-class authoring failures AND a native
 *   compaction attempt that did not succeed.
 * - Nothing here calls an LLM: the digest is pure text assembly.
 */

/** Newest authored notes carried across a roll. */
export const MONITOR_DIGEST_MAX_NOTES = 5;
/** Per-note character ceiling inside the digest. */
export const MONITOR_DIGEST_MAX_NOTE_LENGTH = 400;
/** Hard ceiling for the whole digest, so the "compact" prompt can never itself grow unbounded. */
export const MONITOR_DIGEST_MAX_LENGTH = 2400;

/**
 * Consecutive context-class authoring failures before the safety net rolls.
 *
 * 2, not 1: a single empty response can be a transient provider hiccup, and
 * rolling on it would throw away a healthy session's context for nothing. Two
 * in a row with native compaction unable to help is the signal that the
 * session itself is the problem. Config field `monitorContextFailureRollThreshold`.
 */
export const MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD = 2;

/**
 * Outcome of asking the runtime to compact a session natively.
 *
 * `unavailable` is the honest default: the gateway currently has no wired path
 * to `compaction.run`, and reporting `succeeded` when nothing ran would disarm
 * the safety net precisely when it is needed.
 */
export type NativeCompactionStatus = "succeeded" | "failed" | "skipped" | "unavailable";

export interface NativeCompactionResult {
	readonly status: NativeCompactionStatus;
}

/**
 * THE ONE place the gateway asks for native compaction.
 *
 * Wiring target: delegate `run()` to the GJC SDK `compaction.run` control
 * action for the given session id, mapping its result to `succeeded` /
 * `failed` / `skipped`. Until that is wired, `unavailableCompactionPort` is the
 * only implementation and it says so out loud.
 */
export interface CompactionPort {
	/** Requests native compaction of `sessionId`. MUST NOT throw; report `failed` instead. */
	run(sessionId: string): Promise<NativeCompactionResult>;
}

/**
 * Default port: reports `unavailable`, always. Deliberately not a fake success
 * and deliberately not a local re-implementation of compaction.
 */
export const unavailableCompactionPort: CompactionPort = {
	run: async () => ({ status: "unavailable" }),
};

/**
 * Authoring failure classes.
 *
 * `context` — evidence that the session's context is the problem: an empty
 * response, an explicit context-length rejection, or a zero-token completion.
 * These are what a compaction failure looks like from the outside.
 * `other` — everything else (malformed JSON, contract violations, transport
 * errors). They say nothing about context size and MUST NOT arm a roll.
 */
export type AuthoringFailureClass = "context" | "other";

/** Substrings that identify a context-exhaustion failure across providers. */
const CONTEXT_FAILURE_MARKERS = [
	"authoring response is empty",
	"context_too_large",
	"context length",
	"context window",
	"maximum context",
	"prompt is too long",
	"zero-token",
	"0-token",
];

/**
 * Classifies an authoring failure. Pure, message-based: the raw message is
 * inspected here and then discarded — only the class escapes, never the text.
 */
export function classifyAuthoringFailure(error: unknown): AuthoringFailureClass {
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	return CONTEXT_FAILURE_MARKERS.some((marker) => message.includes(marker)) ? "context" : "other";
}

/**
 * Structured fallback reasons. Public-safe: they name the trigger and the
 * native-compaction outcome that failed to prevent it, and carry no provider
 * text. There is deliberately no code for a successful native compaction —
 * that path does not roll.
 */
export type SessionRollReason =
	| "context_failures_native_compaction_unavailable"
	| "context_failures_native_compaction_failed"
	| "context_failures_native_compaction_skipped";

const ROLL_REASON_BY_STATUS: Partial<Record<NativeCompactionStatus, SessionRollReason>> = {
	unavailable: "context_failures_native_compaction_unavailable",
	failed: "context_failures_native_compaction_failed",
	skipped: "context_failures_native_compaction_skipped",
};

/**
 * The whole roll decision, pure and in one expression.
 *
 * Returns the structured reason to roll, or undefined to keep the session.
 * Note what is absent: turn count. A session that keeps answering is never
 * rolled no matter how many turns it has taken.
 */
export function decideSessionRoll(input: {
	readonly consecutiveContextFailures: number;
	readonly threshold: number;
	readonly nativeCompaction: NativeCompactionStatus;
}): SessionRollReason | undefined {
	if (input.consecutiveContextFailures < input.threshold) return undefined;
	return ROLL_REASON_BY_STATUS[input.nativeCompaction];
}

export interface MonitorDigestNote {
	readonly eventType: string;
	readonly firedAt: string;
	readonly note: string;
}

export interface MonitorDigestInput {
	readonly monitorName: string;
	readonly instruction?: string | undefined;
	/** Newest first. Older entries beyond the note budget are dropped, not summarised. */
	readonly notes: readonly MonitorDigestNote[];
	readonly maxNotes?: number;
	readonly maxNoteLength?: number;
	readonly maxTotalLength?: number;
}

const HEADER =
	"Context compaction: your previous session for this monitor was rolled to keep its context bounded. The prior transcript is gone; the digest below is the continuity you have. Continue from it, do not restate it.";

function clip(text: string, max: number): string {
	const collapsed = text.replace(/\s+/g, " ").trim();
	return collapsed.length <= max ? collapsed : `${collapsed.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

/**
 * Renders the compact digest injected into the first authoring prompt of a
 * rolled monitor session. Pure text assembly — no LLM call, no database, no
 * clock — so the bound arithmetic and truncation are unit-testable on their own
 * and a roll can never cost an extra model turn.
 */
export function buildMonitorCompactionDigest(input: MonitorDigestInput): string {
	const maxNotes = input.maxNotes ?? MONITOR_DIGEST_MAX_NOTES;
	const maxNoteLength = input.maxNoteLength ?? MONITOR_DIGEST_MAX_NOTE_LENGTH;
	const maxTotalLength = input.maxTotalLength ?? MONITOR_DIGEST_MAX_LENGTH;
	const lines = [HEADER, `Monitor: ${clip(input.monitorName, 200)}`];
	const instruction = input.instruction?.trim();
	if (instruction) lines.push(`Standing instruction: ${clip(instruction, 1200)}`);
	const notes = input.notes.slice(0, Math.max(0, maxNotes));
	if (notes.length) {
		lines.push("Recent authored notes (newest first):");
		// No brackets in the rendered note: the authoring turn must answer with a
		// JSON array, and a digest line that looks like one invites a parser (or a
		// model) to read the digest as the response.
		for (const note of notes) lines.push(`- ${note.firedAt} ${note.eventType}: ${clip(note.note, maxNoteLength)}`);
	} else {
		lines.push("Recent authored notes: none yet.");
	}
	const digest = lines.join("\n");
	// Truncate whole lines from the OLDEST note upward: the header, the monitor
	// identity and the standing instruction are the contract and must survive.
	if (digest.length <= maxTotalLength) return digest;
	const marker = "- (older notes omitted)";
	const kept = [...lines];
	while (kept.length > 2 && [...kept, marker].join("\n").length > maxTotalLength) kept.pop();
	const trimmed = [...kept, marker].join("\n");
	// Even the contract lines alone can exceed the ceiling (a 4000-char
	// instruction); a hard clip is the last resort so the bound always holds.
	return trimmed.length <= maxTotalLength ? trimmed : clip(trimmed, maxTotalLength);
}
