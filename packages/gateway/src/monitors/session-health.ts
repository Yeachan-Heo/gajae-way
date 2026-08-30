/**
 * Monitor authoring session health (issue #68).
 *
 * Measured problem: two production monitor sessions grew to 14.7 MB and 13.9 MB
 * of transcript. The last healthy turn ran at ~918K context tokens; every turn
 * after it returned `context_too_large` or zero tokens. Not one compaction entry
 * had been written.
 *
 * Diagnosis (from the gjc side): the non-interactive `-p --mode json` path does
 * go through `AgentSession.prompt()`, so auto-compaction exists and is enabled
 * by default (`compaction.enabled = true`, `strategy = context-full`). The host
 * was running gjc 0.15.3, and adaptive compaction landed in 0.15.4; the model
 * catalog also advertises a 372000-token context window while provider usage was
 * observed past 900K, so the budget the strategy trusts and the budget the
 * provider enforces disagree.
 *
 * Therefore compaction is the runtime's job. The gateway's order of preference
 * is fixed:
 *
 * 1. observe monitor session health (turn count, and whether an authoring
 *    failure was context-family or not);
 * 2. on context pressure, ASK THE RUNTIME to compact through its own control
 *    surface (`compaction.run`, behind CompactionPort);
 * 3. only when that native attempt is explicitly `failed`/`skipped`, or no
 *    control channel exists at all (`unavailable`), AND context-family failures
 *    have recurred to the threshold, roll the session with a digest.
 *
 * Step 3 is the last resort. A healthy session is never rolled, however many
 * turns it accumulates: a turn ceiling would throw away working context to
 * protect against a failure the runtime is already handling.
 *
 * The gateway NEVER summarises with an LLM. The digest below is pure string
 * assembly over the monitor's own instruction and its already-authored notes —
 * a second summary contract would drift from GJC's.
 *
 * Everything in this module is pure (no database, clock, config, or model
 * calls), so the classification, the fallback gate and the digest bounds are
 * unit-testable on their own.
 */

import type { CompactionStatus } from "../orchestrator/compaction";
import { GjcRuntimeError, NO_ASSISTANT_TEXT_CODE } from "../orchestrator/rebind";

/**
 * Consecutive context-family authoring failures that constitute proven
 * compaction failure.
 *
 * Rationale for 2: one such failure can be a provider-side blip or a single
 * oversized payload, and rolling on it would discard a healthy session. Two in a
 * row on the same session is the observed signature of the real defect (every
 * subsequent turn failed identically for hours), and waiting longer just extends
 * the outage — a 10-minute monitor loses 10 minutes per extra attempt.
 */
export const MONITOR_CONTEXT_FAILURE_THRESHOLD = 2;

/**
 * Structured codes that mean "this session's context is the problem", not "this
 * request was bad". Matched by CODE ONLY, never by message wording, because
 * message text is not a contract.
 *
 * - `no_assistant_text`: the gateway's own code for a clean turn that produced
 *   zero tokens — the exact shape observed on the exhausted sessions.
 * - the rest are the runtime/provider codes for an over-budget context.
 */
export const CONTEXT_EXHAUSTION_CODES: ReadonlySet<string> = new Set([
	NO_ASSISTANT_TEXT_CODE,
	"context_too_large",
	"context_length_exceeded",
	"context_window_exceeded",
]);

/**
 * Native-compaction results that leave the session unrecovered, and therefore
 * permit the last-resort digest roll. `compacted` is deliberately absent: if the
 * runtime says it compacted, the gateway must not roll.
 */
const UNRECOVERED_BY_NATIVE: ReadonlySet<CompactionStatus> = new Set<CompactionStatus>([
	"failed",
	"skipped",
	"unavailable",
]);

/** No native attempt has been made for this session yet. */
export type NativeCompactionState = CompactionStatus | "not_attempted";

/**
 * The fallback gate, as one pure decision. Both conditions are required:
 * context-family failures have recurred to the threshold, AND the native
 * compaction attempt left the session unrecovered.
 */
export function shouldRollAfterNativeCompaction(input: {
	readonly consecutiveContextFailures: number;
	readonly threshold: number;
	readonly native: NativeCompactionState;
}): boolean {
	if (input.consecutiveContextFailures < input.threshold) return false;
	return input.native !== "not_attempted" && UNRECOVERED_BY_NATIVE.has(input.native);
}

/**
 * The public-safe reason code recorded when the fallback fires. It names BOTH
 * facts an operator needs: that the context was exhausted, and what the native
 * compaction attempt did about it (`…after_native_unavailable`,
 * `…after_native_failed`, `…after_native_skipped`).
 */
export function fallbackReasonCode(native: NativeCompactionState): string {
	return `context_exhausted_after_native_${native}`;
}

/**
 * True when this failure is context-family. Code-based: an error without a
 * structured code is NOT treated as context exhaustion, because guessing from
 * wording is how a JSON-shape failure would start rolling healthy sessions.
 */
export function isContextExhaustionFailure(error: unknown): boolean {
	if (!(error instanceof GjcRuntimeError)) return false;
	const code = error.code?.trim().toLowerCase();
	return code !== undefined && CONTEXT_EXHAUSTION_CODES.has(code);
}

/**
 * An authoring response with no content is the same evidence as a zero-token
 * turn: the session answered nothing. Callers treat it as context-family.
 */
export function isEmptyAuthoringResponse(response: string): boolean {
	return response.trim().length === 0;
}

/**
 * Raised when an authoring turn returns no content. It carries the same
 * structured code as a zero-token turn from the runtime, so one code-based
 * classifier covers both shapes.
 */
export class EmptyAuthoringResponseError extends GjcRuntimeError {
	constructor() {
		super("monitor authoring turn returned an empty response", {
			code: NO_ASSISTANT_TEXT_CODE,
			message: "the authoring turn returned an empty response",
		});
		this.name = "EmptyAuthoringResponseError";
	}
}

/** Newest authored notes carried across a fallback roll. */
export const MONITOR_DIGEST_MAX_NOTES = 5;
/** Per-note character ceiling inside the digest. */
export const MONITOR_DIGEST_MAX_NOTE_LENGTH = 400;
/** Hard ceiling for the whole digest, so the recovery prompt cannot itself be oversized. */
export const MONITOR_DIGEST_MAX_LENGTH = 2400;

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
	"Session recovery: your previous session for this monitor stopped answering because its context could not be compacted, so it was replaced. The prior transcript is gone; the digest below is the continuity you have. Continue from it, do not restate it.";

function clip(text: string, max: number): string {
	const collapsed = text.replace(/\s+/g, " ").trim();
	return collapsed.length <= max ? collapsed : `${collapsed.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

/**
 * Renders the compact digest injected into the first authoring prompt of a
 * rolled monitor session: (a) the monitor's instruction contract and (b) a
 * bounded summary of its most recent authored notes. Pure.
 */
export function buildMonitorSessionDigest(input: MonitorDigestInput): string {
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
