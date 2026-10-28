import type { ProtocolFailureReason } from "@gajae-gateway/protocol";

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
 * silently fails again and rolls as a last resort. Separate bounded checks
 * handle explicit protocol and stuck-session evidence without requesting
 * compaction.
 * Invariants:
 * - A healthy monitor is NEVER rolled. Turn count is observational only.
 * - A context-triggered roll requires consecutive context-class authoring
 *   failures AND a native-compaction attempt that did not succeed.
 * - Other rolls require repeated protocol failures or positive stuck-session
 *   evidence; they never masquerade as context exhaustion.
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
 * Consecutive protocol-class failures before the safety net rolls the session.
 * Higher than the context threshold on purpose: an off-contract answer can be a
 * one-off, but a whole streak of them is a session that no longer follows its
 * own contract. Measured trigger: 19 identical failures in one day with no
 * remediation at all (jip-gajae `sns-threads`, 2026-09-02).
 */
export const MONITOR_PROTOCOL_FAILURE_ROLL_THRESHOLD = 3;

/**
 * Consecutive `session_busy` dispatch failures before the safety net rolls the
 * session. Each one is a full bounded busy wait (10 minutes) in which the
 * runtime never went idle, so two in a row is a session that has stalled on a
 * turn it will not finish — not a slow turn. Measured trigger (issue #263):
 * one event-type session answered 139 dispatches with `session_busy` over 24h
 * (`session stall` logged the whole time, `last_activity_at` frozen), every
 * slot reached `failed_no_retry`, and nothing ever released the session
 * because a busy refusal was filed as an executor failure with no remedy.
 */
export const MONITOR_BUSY_FAILURE_ROLL_THRESHOLD = 2;

/**
 * Consecutive exact prior-run-busy terminal failures before the safety net rolls.
 * One can be a transient while a previous run settles; the second confirms a
 * session that keeps refusing every later prompt.
 */
export const MONITOR_PRIOR_RUN_BUSY_FAILURE_ROLL_THRESHOLD = 2;

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
 * Authoring failure classes. THREE axes, deliberately the minimum that keeps
 * the roll trigger honest:
 *
 * - `context` — evidence that the session's context is the problem: an empty
 *   response, an explicit context-length rejection, or a zero-token completion.
 *   This is what a compaction failure looks like from the outside, and it is
 *   the ONLY class the roll decision looks at.
 * - `executor` — the work around the turn failed: a worker timeout (the aside
 *   collection worker's 300s cap is the canonical case), an external tool
 *   failure, a lock that made the run skip. The model may never have been
 *   asked. Misreading these as context exhaustion would roll healthy sessions
 *   every time an unrelated tool was slow.
 * - `protocol` — the answer arrived but broke the contract: unparseable JSON,
 *   wrong shape, missing/duplicate/unknown events. A non-empty answer proves
 *   the context still works, so this is the opposite of context evidence.
 *
 * Unrecognised runtime failures fall into `executor`: they are unexplained
 * machinery, not proof about context size, and the safety net must stay
 * holstered without positive evidence.
 */
export type AuthoringFailureClass = "context" | "executor" | "protocol";

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

/** Substrings that identify a contract violation in an answer that DID arrive. */
const PROTOCOL_FAILURE_MARKERS = [
	"authoring response is not an array",
	"authoring response entry missing",
	"authoring response contains unknown event",
	"authoring response duplicates event",
	"authoring response omits event",
	"json",
	"unexpected token",
	"unexpected end of",
];

/**
 * Substrings that identify executor-side failure. Listed for readability and
 * for the aside-timeout case specifically; unrecognised failures land here
 * anyway.
 */
const EXECUTOR_FAILURE_MARKERS = [
	"timeout",
	"timed out",
	"aside",
	"tool failed",
	"external tool",
	"lock",
	"skipped",
	"econnrefused",
	"socket",
	"spawn",
];

/** True when the failure is the aside collection worker giving up (its 300s cap). */
export function isAsideTimeoutFailure(error: unknown): boolean {
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	if (message.includes("aside_timeout")) return true;
	return message.includes("aside") && (message.includes("timeout") || message.includes("timed out"));
}

/**
 * Executor sub-kinds, reported as structured operator-facing reasons. The class
 * axis stays at three (context / executor / protocol); this is the breakdown
 * WITHIN executor, because "the worker timed out" and "the worker timed out and
 * left external work running" need different operator action.
 *
 * `executor_orphaned_external_work` is the measured failure mode: the child CLI
 * process was killed on the wrapper's timeout (300s/360s) while the external
 * daemon's job kept running, so the next tick stacked another one on top. Drill
 * observation: aside exec collection took 7–13 minutes, the wrapper killed the
 * child at 300/360s, the Aside daemon task stayed `running`, and 3 sessions and
 * 10 tabs accumulated.
 *
 * That is not a session problem, so it must NEVER touch the session: no streak,
 * no compaction request, no roll. The gateway's whole job here is to name it.
 */
export type ExecutorFailureReason = "executor_orphaned_external_work" | "executor_timeout" | "executor_failed";

/**
 * Substrings that identify an orphaned external executor: the child died, the
 * external work did not.
 */
const ORPHANED_EXECUTOR_MARKERS = [
	"orphaned_executor",
	"orphaned executor",
	"daemon task still running",
	"daemon job still running",
	"external work still running",
	"task still running",
	"still running after",
	"orphaned",
];

/**
 * True when the failure left external executor work running behind a dead child
 * process. Checked before every other executor marker: an orphaned run is the
 * actionable signal, and the same message usually also looks like a timeout.
 */
export function isOrphanedExecutorFailure(error: unknown): boolean {
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	if (ORPHANED_EXECUTOR_MARKERS.some((marker) => message.includes(marker))) return true;
	// The generic shape of the drill: a killed/terminated child alongside a
	// daemon that is still alive.
	const childDied = /kill|terminated|exited|abort/.test(message);
	const externalAlive = /daemon|external/.test(message) && /running|alive|active|pending/.test(message);
	return childDied && externalAlive;
}

/**
 * Names the executor sub-kind. Pure and message-based; the message is inspected
 * here and discarded, only the coded reason escapes.
 */
export function classifyExecutorFailure(error: unknown): ExecutorFailureReason {
	if (isOrphanedExecutorFailure(error)) return "executor_orphaned_external_work";
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	if (isAsideTimeoutFailure(error) || message.includes("timeout") || message.includes("timed out"))
		return "executor_timeout";
	return "executor_failed";
}

/**
 * Protocol sub-kinds, reported as structured operator-facing reasons.
 *
 * Measured gap (jip-gajae host, 2026-09-02): `sns-threads` failed 19/19 ticks in
 * one day with `authoring_response_invalid` while `sns-x` was delivering from
 * the same gateway. The class alone did not say WHICH contract rule the answer
 * broke, and the answer text may not be logged (it can carry secrets), so the
 * failure was undiagnosable from the outside and nothing ever remediated it.
 *
 * These reasons are derived from the classifier's own markers, so they name the
 * violated rule without ever emitting the offending text.
 */
/** Marker -> reason, in check order. First match wins. */
const PROTOCOL_REASON_MARKERS: ReadonlyArray<readonly [string, ProtocolFailureReason]> = [
	["authoring response is not an array", "protocol_response_not_array"],
	["authoring response is not a json array", "protocol_response_not_array"],
	["authoring response json is unparseable", "protocol_unparseable_json"],
	["authoring response entry missing", "protocol_entry_missing_field"],
	["authoring response contains unknown event", "protocol_unknown_event"],
	["authoring response duplicates event", "protocol_duplicate_event"],
	["authoring response omits event", "protocol_omitted_event"],
	["unexpected token", "protocol_unparseable_json"],
	["unexpected end of", "protocol_unparseable_json"],
	["json", "protocol_unparseable_json"],
];

/**
 * Names the protocol sub-kind. Pure and message-based, exactly like
 * `classifyExecutorFailure`: the message is inspected here and discarded, only
 * the coded reason escapes.
 */
export function classifyProtocolFailure(error: unknown): ProtocolFailureReason {
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	for (const [marker, reason] of PROTOCOL_REASON_MARKERS) if (message.includes(marker)) return reason;
	return "protocol_off_contract";
}

/**
 * Classifies an authoring failure. Pure, message-based: the raw message is
 * inspected here and then discarded — only the class escapes, never the text.
 *
 * Order matters. An executor timeout must never be read as context exhaustion,
 * so the executor markers are checked BEFORE the context markers when they are
 * unambiguous (aside timeout), and a protocol violation outranks the generic
 * executor markers because "JSON" strings often mention sockets and tools.
 */
export function classifyAuthoringFailure(error: unknown): AuthoringFailureClass {
	const message = (error instanceof Error ? error.message : String(error)).toLowerCase();
	// Unambiguous executor failures first: an orphaned external run or an aside
	// timeout says nothing at all about the session's context.
	if (isOrphanedExecutorFailure(error) || isAsideTimeoutFailure(error)) return "executor";
	if (PROTOCOL_FAILURE_MARKERS.some((marker) => message.includes(marker))) return "protocol";
	if (CONTEXT_FAILURE_MARKERS.some((marker) => message.includes(marker))) return "context";
	if (EXECUTOR_FAILURE_MARKERS.some((marker) => message.includes(marker))) return "executor";
	// No positive evidence: treat as executor machinery, never as context.
	return "executor";
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
	| "context_failures_native_compaction_skipped"
	| "protocol_failures_off_contract"
	| "session_busy_stalled"
	| "session_prior_run_busy_stalled"
	| "session_host_unavailable"
	// The runtime accepted the prompt but no agent run ever started
	// (`phase=submission`): a session wedged on a prior run fails every later
	// prompt identically, so it earns the same one-shot roll as an unavailable
	// host. See `isWedgedSubmissionError` in propagate.ts.
	| "session_submission_wedged";

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

const NON_SUBSTANTIVE_ECHO_WORDS = new Set([
	"a",
	"an",
	"and",
	"as",
	"at",
	"before",
	"but",
	"by",
	"can",
	"do",
	"does",
	"for",
	"from",
	"here",
	"in",
	"is",
	"it",
	"no",
	"not",
	"of",
	"on",
	"or",
	"only",
	"please",
	"should",
	"task",
	"the",
	"that",
	"this",
	"to",
	"was",
	"were",
	"what",
	"we",
	"will",
	"with",
	"your",
	"run",
	"event",
]);

const META_ECHO_WORDS = new Set([
	"check",
	"checked",
	"confirm",
	"confirms",
	"confirmed",
	"describe",
	"describes",
	"described",
	"find",
	"finds",
	"found",
	"happened",
	"inspect",
	"inspected",
	"look",
	"mention",
	"mentioned",
	"note",
	"noted",
	"review",
	"reviewed",
	"run",
	"ran",
	"say",
	"says",
	"schedule",
	"scheduled",
	"state",
	"states",
	"summarize",
	"summarized",
	"target",
	"verify",
	"verified",
]);

const NO_WORK_DISCLAIMER_PATTERNS = [
	/\b(?:do|does|did|has|have|had|is|are|was|were)\s+not\s+(?:confirm|verify|check|run|execute|inspect|review|report|show)\b/i,
	/\bnot\s+(?:confirmed|verified|checked|run|executed|inspected|reviewed|completed|done)\b/i,
	/\b(?:only|merely)\s+(?:describe|describes|summarize|summarizes|summarise|summarises|repeat|repeats|restate|restates|state|states|report|reports|note|notes)\b/i,
	/\b(?:nothing|no\s+work|no\s+review|no\s+checks?|no\s+commands?|no\s+results?)\s+(?:(?:has|have|was|were)\s+)?(?:been\s+)?(?:done|checked|run|executed|performed)\b/i,
];

const NON_COMMAND_START_WORDS = new Set([
	...NON_SUBSTANTIVE_ECHO_WORDS,
	...META_ECHO_WORDS,
	"activity",
	"daily",
	"deploy",
	"deployment",
	"deployments",
	"failure",
	"failures",
	"final",
	"friction",
	"initial",
	"latency",
	"operation",
	"previous",
	"recent",
	"report",
	"runtime",
	"scheduled",
	"system",
]);

const MAX_NOVEL_META_WORDS = 5;
const MAX_NOVEL_CONTENT_RATIO = 0.2;
const MIN_CONTRACT_COVERAGE = 0.7;

export interface AuthoredEchoEventContext {
	readonly eventType?: string;
	readonly eventId?: string;
	readonly payload?: unknown;
	readonly firedAt?: string;
}

function serializeEchoPayload(payload: unknown): string {
	if (typeof payload === "string") return payload;
	try {
		return JSON.stringify(payload) ?? "";
	} catch {
		return String(payload);
	}
}

const PAYLOAD_COUNT_LABELS = new Map([
	["test", "tests"],
	["tests", "tests"],
	["issue", "issues"],
	["issues", "issues"],
	["pr", "prs"],
	["prs", "prs"],
	["pull request", "pull requests"],
	["pull requests", "pull requests"],
	["file", "files"],
	["files", "files"],
	["error", "errors"],
	["errors", "errors"],
	["failure", "failures"],
	["failures", "failures"],
	["warning", "warnings"],
	["warnings", "warnings"],
	["blocker", "blockers"],
	["blockers", "blockers"],
	["commit", "commits"],
	["commits", "commits"],
	["change", "changes"],
	["changes", "changes"],
	["update", "updates"],
	["updates", "updates"],
	["result", "results"],
	["results", "results"],
	["passed", "passed"],
	["passing", "passing"],
	["failed", "failed"],
	["found", "found"],
	["millisecond", "ms"],
	["milliseconds", "ms"],
	["ms", "ms"],
	["second", "seconds"],
	["seconds", "seconds"],
	["minute", "minutes"],
	["minutes", "minutes"],
]);

function structuredPayloadCountMarkers(payload: unknown, seen = new WeakSet<object>()): string[] {
	if (payload === null || typeof payload !== "object") return [];
	if (seen.has(payload)) return [];
	seen.add(payload);
	if (Array.isArray(payload)) return payload.flatMap((value) => structuredPayloadCountMarkers(value, seen));

	const markers: string[] = [];
	for (const [key, value] of Object.entries(payload)) {
		if (typeof value === "number" && Number.isFinite(value)) {
			const label = key
				.replace(/([a-z])([A-Z])/g, "$1 $2")
				.replace(/[_-]+/g, " ")
				.replace(/\s*(?:count|total)$/i, "")
				.trim()
				.toLowerCase();
			const normalizedLabel = PAYLOAD_COUNT_LABELS.get(label);
			if (normalizedLabel) markers.push(`${value} ${normalizedLabel}`);
		}
		markers.push(...structuredPayloadCountMarkers(value, seen));
	}
	return markers;
}

/** Detect generic command invocations without accepting descriptive task prose as commands. */
function authoredCommandMarkers(text: string): string[] {
	const marker: string[] = [];
	for (const [, executable, argumentsText] of text.matchAll(
		/\b(?:ran|run|executed|execute|invoked|invoke|called)\s+([A-Za-z0-9_./+-]+)(?:\s+((?:"[^"\n]*"|'[^'\n]*'|[A-Za-z0-9_./=+-]+)(?:\s+(?:"[^"\n]*"|'[^'\n]*'|[A-Za-z0-9_./=+-]+))*))?(?=\s*[:.;,!?\n]|$)/gi,
	)) {
		if (executable && !NON_COMMAND_START_WORDS.has(executable.toLowerCase()))
			marker.push(argumentsText ? `${executable} ${argumentsText}` : executable);
	}
	return marker;
}

function authoredEvidenceMarkers(text: string): string[] {
	const quantifiedResults =
		text.match(
			/\b\d+(?:,\d{3})*(?:\.\d+)?%?\s*(?:tests?|issues?|prs?|pull\s+requests?|files?|errors?|failures?|warnings?|blockers?|commits?|changes?|updates?|results?|passed|passing|failed|found|ms|seconds?|minutes?)\b/gi,
		) ?? [];
	const quantifiedValues = new Set(
		quantifiedResults
			.map((marker) => marker.match(/^\d+(?:,\d{3})*(?:\.\d+)?/)?.[0])
			.filter((value): value is string => value !== undefined),
	);
	const standaloneResultMarkers = (
		text.match(/\b(?:passed|passing|failed|found|changed|updated|blocked|resolved|reopened)\s+\d+\b/gi) ?? []
	).filter((marker) => {
		const value = marker.match(/\d+/)?.[0];
		return value === undefined || !quantifiedValues.has(value);
	});
	const scalarResultMarkers = [
		...text.matchAll(/\b(?:is|are|was|were|reached|equals?)\s+(\d+(?:,\d{3})*(?:\.\d+)?%?)\b/gi),
		...text.matchAll(/["'][\w.-]+["']\s*:\s*(\d+(?:,\d{3})*(?:\.\d+)?%?)\b/g),
		...text.matchAll(/\b[A-Za-z][\w .-]*\s*[:=]\s*(\d+(?:,\d{3})*(?:\.\d+)?%?)\b/g),
	].flatMap(([, value]) => (value ? [value] : []));
	const markers = [
		...(text.match(/(?:https?:\/\/|www\.)[^\s<>()[\]{}"'`]+/gi) ?? []),
		...(text.match(/#\d+/g) ?? []),
		...(text.match(/\b[0-9a-f]{7,40}\b/gi) ?? []),
		...(text.match(/\b(?:[\w.-]+\/)*[\w.-]+\.[A-Za-z0-9]{1,12}\b/g) ?? []),
		...(text.match(/\b[A-Z][A-Z0-9]*-\d+\b/gi) ?? []),
		...quantifiedResults,
		...standaloneResultMarkers,
		...scalarResultMarkers,
		...authoredCommandMarkers(text),
		...(text.match(/`[^`\n]+`/g) ?? []),
	];
	return markers.map((marker) => marker.toLowerCase().replace(/[),.;!?}\]"'`]+$/g, ""));
}

function hasNovelAuthoredEvidence(note: string, contractEvidence: Set<string>): boolean {
	const clauses = note.split(/[.!?;]\s+|\s+\b(?:but|however|although|yet)\b\s+/i);
	return clauses.some((clause) => {
		if (NO_WORK_DISCLAIMER_PATTERNS.some((pattern) => pattern.test(clause))) return false;
		return authoredEvidenceMarkers(clause).some((marker) => !contractEvidence.has(marker));
	});
}

function authoredEchoTokens(text: string): string[] {
	const normalizedTimestamps = text.replace(
		/\b(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?Z?\b/gi,
		"$1 $2 $3 $4 $5",
	);
	return normalizedTimestamps.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
}

/** Detect contract-only notes while allowing paraphrase words and real, novel work evidence. */
export function isAuthoredEchoSuspected(
	contractText: string | undefined,
	note: string,
	eventContext?: AuthoredEchoEventContext,
): boolean {
	if (!note) return false;
	// Score instruction and event type independently so either can expose a short echo.
	const contractSources = [contractText, eventContext?.eventType].filter((part): part is string => Boolean(part));
	if (contractSources.length === 0) return false;
	const payloadText = eventContext?.payload === undefined ? undefined : serializeEchoPayload(eventContext.payload);
	const payloadCountMarkers =
		eventContext?.payload === undefined ? [] : structuredPayloadCountMarkers(eventContext.payload);
	const fullContract = contractSources.join("\n");
	// Event identity, timing and payload can explain copied markers, but must not inflate task coverage.
	const evidenceContract = [
		fullContract,
		eventContext?.eventId,
		eventContext?.firedAt,
		payloadText,
		...payloadCountMarkers,
	]
		.filter((part): part is string => Boolean(part))
		.join("\n");
	const contractContentSources = contractSources.map(
		(source) => new Set(authoredEchoTokens(source).filter((token) => !NON_SUBSTANTIVE_ECHO_WORDS.has(token))),
	);
	const contractContent = new Set(contractContentSources.flatMap((source) => [...source]));
	// Event metadata is contract context, but it must not dilute task coverage or create novelty.
	const eventMetadataText = [eventContext?.eventId, eventContext?.firedAt, payloadText, ...payloadCountMarkers]
		.filter(Boolean)
		.join("\n");
	const eventMetadataContent = new Set(authoredEchoTokens(eventMetadataText));
	const noteContent = new Set(
		authoredEchoTokens(note).filter(
			(token) =>
				!NON_SUBSTANTIVE_ECHO_WORDS.has(token) && (!eventMetadataContent.has(token) || contractContent.has(token)),
		),
	);
	if (contractContent.size === 0 || noteContent.size === 0) return false;

	const contractEvidence = new Set([
		...authoredEvidenceMarkers(evidenceContract),
		...(evidenceContract.match(/\b\d+(?:,\d{3})*(?:\.\d+)?%?\b/g) ?? []),
	]);
	if (hasNovelAuthoredEvidence(note, contractEvidence)) return false;
	if (NO_WORK_DISCLAIMER_PATTERNS.some((pattern) => pattern.test(note))) return true;

	const novelTokens = [...noteContent].filter((token) => !contractContent.has(token));
	const novelMetaWords = novelTokens.filter((token) => META_ECHO_WORDS.has(token)).length;
	const novelContentRatio = (novelTokens.length - novelMetaWords) / noteContent.size;
	const hasCoveredContractSource = contractContentSources.some((source) => {
		if (source.size === 0) return false;
		const sharedTokens = [...noteContent].filter((token) => source.has(token)).length;
		return sharedTokens / source.size >= MIN_CONTRACT_COVERAGE;
	});
	return (
		hasCoveredContractSource && novelMetaWords <= MAX_NOVEL_META_WORDS && novelContentRatio <= MAX_NOVEL_CONTENT_RATIO
	);
}
