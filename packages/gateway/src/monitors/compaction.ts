/**
 * Monitor session compaction (issue #68).
 *
 * Measured problem: `SESSION_TURN_LIMIT` rotation in server.ts is driven by the
 * inbound chat path only. The monitor authoring path never counted a turn, so a
 * monitor session stayed at epoch 0 / turn_count 0 forever while every
 * `gjc --resume` replayed the whole transcript. A 10-minute monitor authors ~144
 * turns a day; after roughly a day the authoring turn returned empty text and
 * dispatch settled as `internal_error`.
 *
 * Plain rotation is the wrong remedy: throwing the session away discards
 * everything the lane established. Compaction keeps the context BOUNDED while
 * preserving continuity — the roll carries a digest of the monitor's standing
 * contract plus its most recent authored notes into the first prompt of the new
 * session.
 *
 * The digest builder below is pure: no database, no clock, no config, so the
 * bound arithmetic and truncation are unit-testable on their own.
 */

/**
 * Authoring turns per monitor session epoch before it is compacted.
 *
 * Rationale for 24: a 10-minute monitor produces 6 authoring turns per hour
 * (~144/day). 24 turns is therefore ~4 hours of accumulated transcript — small
 * enough that prefill stays flat, large enough that a roll happens ~6 times a
 * day rather than every few turns (each roll costs one digest instead of the
 * live transcript). Deliberately far below the chat-path `SESSION_TURN_LIMIT`
 * (50): chat turns are human-paced and self-limiting, monitor turns are not.
 */
export const MONITOR_SESSION_TURN_LIMIT = 24;

/** Newest authored notes carried across a roll. */
export const MONITOR_DIGEST_MAX_NOTES = 5;
/** Per-note character ceiling inside the digest. */
export const MONITOR_DIGEST_MAX_NOTE_LENGTH = 400;
/** Hard ceiling for the whole digest, so the "compact" prompt can never itself grow unbounded. */
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
	"Context compaction: your previous session for this monitor was rolled to keep its context bounded. The prior transcript is gone; the digest below is the continuity you have. Continue from it, do not restate it.";

function clip(text: string, max: number): string {
	const collapsed = text.replace(/\s+/g, " ").trim();
	return collapsed.length <= max ? collapsed : `${collapsed.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

/**
 * Renders the compact digest injected into the first authoring prompt of a
 * rolled monitor session. Pure. Returns the digest text; the caller decides
 * whether a roll happened.
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
