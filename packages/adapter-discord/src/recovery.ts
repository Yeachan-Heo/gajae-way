import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { adapterHome } from "./config";
import type { DiscordInboundMessage } from "./main";

/**
 * Restart/reconnect recovery for Discord (issue #33): messages the adapter missed while
 * offline are replayed through the same decideInbound -> chat.send path as live events.
 *
 * Exactly-once rests on two durable layers that already existed: the gateway's
 * inbound_messages/conversation_context tables dedupe on the platform message id, and this
 * module's per-conversation recovery watermark keeps the refetch window tight.
 *
 * The watermark is *recovery* progress only: it records the highest id a recovery pass
 * actually walked past (delivered, known-duplicate, or deliberately skipped). Live sends
 * never write it, because a live message is no proof that the older messages behind it were
 * ever seen — advancing on live traffic is exactly how issue #33 recurs after a partial
 * backfill.
 */

/**
 * How a failed recovery send is classified. Only `terminal-message` may ever be discarded:
 * `retryable` and `write-path-unknown` leave the gap intact so nothing is lost when the
 * gateway write path might still be degraded or partially applied.
 */
export type RecoveryFailureClass = "retryable" | "terminal-message" | "write-path-unknown";

/** Durable record of a message recovery gave up on. Persisted next to the cursors. */
export interface RecoveryDeadLetter {
	readonly messageId: string;
	readonly conversationId: string;
	readonly classification: RecoveryFailureClass;
	readonly attempts: number;
	/** ISO-8601 timestamp of the discard. */
	readonly at: string;
	readonly summary: string;
}

export interface RecoveryCursorState {
	/** Highest message id a recovery pass walked past, per recoverable conversation. */
	readonly recoveredThrough: Readonly<Record<string, string>>;
	/**
	 * Watermarks for conversations recovery does not currently iterate: channels removed
	 * from config, plus thread/DM ids that never get backfill. Kept (not deleted) so a
	 * channel re-added to config resumes instead of replaying the bootstrap window, and
	 * bounded to RECOVERY_QUARANTINE_CAP entries, oldest insertion pruned first.
	 */
	readonly quarantined: Readonly<Record<string, string>>;
	/** Bounded discard log, oldest pruned first (RECOVERY_DEAD_LETTER_CAP). */
	readonly deadLetters: readonly RecoveryDeadLetter[];
}

export type RecoveryGateResult = "acked" | "duplicate" | "unavailable";

const EMPTY_STATE: RecoveryCursorState = { recoveredThrough: {}, quarantined: {}, deadLetters: [] };

export function recoveryCursorPath(home: string = adapterHome()): string {
	return join(home, "adapters", "discord", "recovery-cursor.json");
}

/** Snowflake-keyed in-flight/acked gate shared by the live and recovery send paths. */
export class RecoveryGate {
	readonly #pending = new Map<string, Promise<RecoveryGateResult>>();
	readonly #acked = new Set<string>();

	constructor(readonly limit = 10_000) {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("recovery gate limit must be a positive integer");
	}

	/**
	 * Joins an in-flight attempt for this id when one exists (live and recovery converge on
	 * one send), otherwise runs `attempt`. Only an acked attempt marks the id; a rejected
	 * send leaves it unmarked so a later recovery retries instead of trusting a poisoned
	 * LRU entry.
	 */
	join(messageId: string, attempt: () => Promise<RecoveryGateResult>): Promise<RecoveryGateResult> {
		const pending = this.#pending.get(messageId);
		if (pending) return pending;
		if (this.#acked.has(messageId)) return Promise.resolve("duplicate");
		const attemptPromise = Promise.resolve()
			.then(attempt)
			.then(
				(verdict) => {
					this.#pending.delete(messageId);
					if (verdict === "acked") {
						this.#acked.add(messageId);
						this.forgetBeyond(this.limit);
					}
					return verdict;
				},
				(error: unknown) => {
					this.#pending.delete(messageId);
					throw error;
				},
			);
		this.#pending.set(messageId, attemptPromise);
		return attemptPromise;
	}

	/** True when the gateway already acknowledged this id in this process. */
	acked(messageId: string): boolean {
		return this.#acked.has(messageId);
	}

	/** Bounded memory: forget acked ids beyond the limit (least-recently added first). */
	forgetBeyond(limit: number): void {
		while (this.#acked.size > limit) this.#acked.delete(this.#acked.values().next().value as string);
	}
}

export async function loadRecoveryCursors(path: string): Promise<RecoveryCursorState> {
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(path, "utf8"));
	} catch (error) {
		if ((error as { code?: unknown }).code === "ENOENT") {
			// Missing cursor = first run for this install; bootstrap lookback bounds the gap.
			return EMPTY_STATE;
		}
		if (error instanceof SyntaxError) return EMPTY_STATE;
		throw error;
	}
	if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return EMPTY_STATE;
	const record = raw as { recoveredThrough?: unknown; quarantined?: unknown; deadLetters?: unknown };
	return {
		recoveredThrough: readWatermarks(record.recoveredThrough),
		quarantined: readWatermarks(record.quarantined),
		deadLetters: readDeadLetters(record.deadLetters),
	};
}

function readWatermarks(entries: unknown): Record<string, string> {
	const watermarks: Record<string, string> = {};
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value === "string" && /^\d+$/.test(value)) watermarks[id] = value;
		}
	}
	return watermarks;
}

function readDeadLetters(entries: unknown): RecoveryDeadLetter[] {
	if (!Array.isArray(entries)) return [];
	const letters: RecoveryDeadLetter[] = [];
	for (const entry of entries) {
		if (typeof entry !== "object" || entry === null) continue;
		const candidate = entry as Record<string, unknown>;
		if (typeof candidate.messageId !== "string" || typeof candidate.conversationId !== "string") continue;
		if (candidate.classification !== "terminal-message") continue;
		letters.push({
			messageId: candidate.messageId,
			conversationId: candidate.conversationId,
			classification: "terminal-message",
			attempts: typeof candidate.attempts === "number" ? candidate.attempts : 0,
			at: typeof candidate.at === "string" ? candidate.at : "",
			summary: typeof candidate.summary === "string" ? candidate.summary : "",
		});
	}
	return letters.slice(Math.max(0, letters.length - RECOVERY_DEAD_LETTER_CAP));
}

/** Atomic write (tmp + rename) so a crash mid-write never truncates the cursor file. */
export async function saveRecoveryCursors(path: string, state: RecoveryCursorState): Promise<void> {
	const payload = `${JSON.stringify(
		{
			recoveredThrough: state.recoveredThrough,
			quarantined: state.quarantined,
			deadLetters: state.deadLetters,
		},
		null,
		"\t",
	)}\n`;
	await mkdir(dirname(path), { recursive: true, mode: 0o700 });
	const tmp = `${path}.${process.pid}.tmp`;
	await writeFile(tmp, payload, { encoding: "utf8", mode: 0o600 });
	await rename(tmp, path);
}

/**
 * Partitions every known watermark by whether recovery currently iterates it. Nothing is
 * dropped for being unrecoverable: keys move into `quarantined` so a channel removed from
 * config and later re-added resumes from its watermark instead of replaying the bootstrap
 * window, and a quarantined key becomes live again the moment its channel is configured.
 *
 * Unbounded growth (thread/DM ids, long-dead channels) is the thing that is bounded:
 * `quarantined` keeps at most `cap` entries and drops the oldest insertions first.
 */
export function retainRecoveryCursors(
	state: RecoveryCursorState,
	recoverable: Iterable<string>,
	cap = RECOVERY_QUARANTINE_CAP,
): RecoveryCursorState {
	const allowed = new Set(recoverable);
	const recoveredThrough: Record<string, string> = {};
	const quarantined: Record<string, string> = {};
	// Quarantined first so already-parked keys stay older than freshly parked ones.
	for (const [id, value] of [...Object.entries(state.quarantined), ...Object.entries(state.recoveredThrough)]) {
		if (allowed.has(id)) recoveredThrough[id] = value;
		else quarantined[id] = value;
	}
	const parked = Object.keys(quarantined);
	for (const id of parked.slice(0, Math.max(0, parked.length - cap))) delete quarantined[id];
	return { recoveredThrough, quarantined, deadLetters: state.deadLetters };
}

/** Appends a discard record, keeping at most `cap` entries (oldest dropped first). */
export function recordDeadLetter(
	state: RecoveryCursorState,
	entry: RecoveryDeadLetter,
	cap = RECOVERY_DEAD_LETTER_CAP,
): RecoveryCursorState {
	const deadLetters = [...state.deadLetters, entry];
	return { ...state, deadLetters: deadLetters.slice(Math.max(0, deadLetters.length - cap)) };
}

/** Protocol error codes that can only ever fail for this exact message payload. */
const TERMINAL_ERROR_CODES = new Set([
	"payload_too_large",
	"invalid_params",
	"malformed_frame",
	"unsupported_frame_type",
]);
/** Protocol error codes that describe a transient link/gateway condition. */
const RETRYABLE_ERROR_CODES = new Set([
	"gateway_shutting_down",
	"negotiation_required",
	"incompatible_profile_version",
	"missing_required_capability",
]);
const RETRYABLE_ERRNO = /^(ECONN|EPIPE|ETIMEDOUT|ENOTCONN|EAGAIN|EBUSY|SQLITE_BUSY|SQLITE_LOCKED)/;
const TERMINAL_TEXT =
	/payload too large|message too long|content too long|malformed|unsupported (payload|frame|attachment)|invalid params|validation (failed|rejected)/i;
const RETRYABLE_TEXT =
	/timed out|timeout|econnreset|econnrefused|epipe|enotconn|socket hang up|sqlite_busy|sqlite_locked|database is locked|queue (is )?(saturated|full)|backpressure|temporarily|try again|unavailable|\b5\d{2}\b/i;

/**
 * Classifies a failed `chat.send`. The default is deliberately `write-path-unknown`: a
 * failure nobody recognized may have partially applied on the gateway write path, so the
 * caller must retry it and MUST NOT advance the cursor past it. A successful liveness probe
 * is never evidence that a send is safe to abandon.
 */
export function classifyRecoveryFailure(error: unknown): RecoveryFailureClass {
	const code = (error as { code?: unknown } | null | undefined)?.code;
	if (typeof code === "string") {
		if (TERMINAL_ERROR_CODES.has(code)) return "terminal-message";
		if (RETRYABLE_ERROR_CODES.has(code) || RETRYABLE_ERRNO.test(code)) return "retryable";
	}
	const text = error instanceof Error ? error.message : typeof error === "string" ? error : "";
	if (TERMINAL_TEXT.test(text)) return "terminal-message";
	if (RETRYABLE_TEXT.test(text)) return "retryable";
	return "write-path-unknown";
}

/** Short, bounded, secret-free error summary for the dead-letter record. */
export function summarizeRecoveryFailure(error: unknown): string {
	const code = (error as { code?: unknown } | null | undefined)?.code;
	const text = error instanceof Error ? error.message : String(error);
	return `${typeof code === "string" ? `${code}: ` : ""}${text}`.slice(0, 200);
}

/** Discord snowflakes are time-ordered 64-bit ints; compare numerically, not lexically. */
export function snowflakeIsAfter(a: string, b: string): boolean {
	return BigInt(a) > BigInt(b);
}

/**
 * Snowflake for `timestamp`, used as the bootstrap cursor when a conversation has no
 * stored ack point. Discord encodes ms-since-epoch in the top 42 bits.
 */
export function snowflakeFromTimestamp(timestampMs: number): string {
	return (BigInt(timestampMs - 1_420_070_400_000) << 22n).toString();
}

export const RECOVERY_PAGE_LIMIT = 100;
export const RECOVERY_MAX_PAGES = 5;
export const RECOVERY_BOOTSTRAP_LOOKBACK_MS = 24 * 60 * 60 * 1000;
/**
 * Per-message send budget during recovery. A message that fails this many times in one pass
 * is either retried later (retryable / write-path-unknown) or, only when every failure was
 * terminal for this exact message, dead-lettered and skipped forward so it cannot
 * head-of-line block the channel's backfill forever.
 */
export const RECOVERY_MAX_ATTEMPTS = 3;
/** Backoff floor between per-message send attempts inside one recovery pass. */
export const RECOVERY_ATTEMPT_BACKOFF_MS = 500;
/** Backoff floor/ceiling for re-running a failed or truncated recovery pass. */
export const RECOVERY_RETRY_BASE_MS = 1_000;
export const RECOVERY_RETRY_MAX_MS = 60_000;
/** Retention bound for watermarks of conversations recovery does not iterate. */
export const RECOVERY_QUARANTINE_CAP = 100;
/** Retention bound for the durable discard log. */
export const RECOVERY_DEAD_LETTER_CAP = 50;

/** The slice of a discord.js text-based channel recovery needs: forward paged history. */
export interface RecoverableChannel {
	readonly messages: {
		/**
		 * Fetches messages after `after` with the given limit. Discord's REST `after`
		 * filter paginates forward from the anchor; the recovery loop sorts numerically
		 * anyway, so a Collection shape (entries, not values) or a reverse-ordered page
		 * cannot break the replay. Fake channels in tests return plain arrays.
		 */
		fetch(options: {
			after?: string;
			limit: number;
		}): Promise<Iterable<DiscordInboundMessage> | { entries(): Iterable<[string, DiscordInboundMessage]> }>;
	};
}

/**
 * Per-message delivery verdict handed back by the caller's send path. `skip` means the
 * caller deliberately gave up on this message (poison message past its attempt budget, or
 * nothing to engage with): the cursor advances past it so the rest of the gap can drain.
 */
export type RecoveryDelivery = "acked" | "duplicate" | "unavailable" | "skip";

export interface RecoveryOutcome {
	/** Highest message id this run walked past (delivered, duplicate, or skipped). */
	readonly advancedTo: string;
	readonly delivered: number;
	/** Messages the run walked past without a send (empty content or caller `skip`). */
	readonly skipped: number;
	/** True when the gap exceeded the page bound; older messages may remain unrecovered. */
	readonly truncated: boolean;
	/** True when a delivery was unavailable; the caller must retry from `advancedTo`. */
	readonly failed: boolean;
}

export interface RecoveryOptions {
	readonly cursor?: string;
	readonly nowMs: number;
	readonly pageLimit?: number;
	readonly maxPages?: number;
	deliver(message: DiscordInboundMessage): Promise<RecoveryDelivery>;
	/**
	 * Treats empty/whitespace-only content (thread starters, system entries) as intentional
	 * non-text: skipped without a send, cursor advances past them. Default: skip.
	 */
	readonly skipEmptyContent?: boolean;
}

/**
 * Replays the bounded gap after `cursor` in ascending id order, delivering each message and
 * advancing only past acknowledged (or known-duplicate) ones. Stops early on an unavailable
 * delivery; reports `truncated` when `maxPages` full pages were consumed.
 */
export async function recoverConversation(
	channel: RecoverableChannel,
	options: RecoveryOptions,
): Promise<RecoveryOutcome> {
	const pageLimit = options.pageLimit ?? RECOVERY_PAGE_LIMIT;
	const maxPages = options.maxPages ?? RECOVERY_MAX_PAGES;
	const skipEmpty = options.skipEmptyContent ?? true;
	let cursor = options.cursor ?? snowflakeFromTimestamp(options.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS);
	let delivered = 0;
	let skipped = 0;
	for (let page = 0; page < maxPages; page++) {
		const fetched = await channel.messages.fetch({ after: cursor, limit: pageLimit });
		// discord.js returns a Collection (entries); fake channels return arrays. Normalize
		// to values first, then sort numerically so either API ordering is safe.
		const rawBatch = [
			...(Symbol.iterator in fetched
				? fetched
				: (fetched as { entries(): Iterable<[string, DiscordInboundMessage]> }).entries()),
		].map((entry) => (Array.isArray(entry) ? (entry[1] as DiscordInboundMessage) : (entry as DiscordInboundMessage)));
		const batch = rawBatch.filter((message) => snowflakeIsAfter(message.id, cursor));
		if (rawBatch.length >= pageLimit && batch.length === 0) {
			// A broken/repeated page must not spin until the bound while pretending to
			// make progress. Leave the cursor unchanged so the next reconnect retries it.
			return { advancedTo: cursor, delivered, skipped, truncated: true, failed: false };
		}
		batch.sort((a, b) => {
			const ai = BigInt(a.id);
			const bi = BigInt(b.id);
			return ai < bi ? -1 : ai > bi ? 1 : 0;
		});
		if (batch.length === 0) return { advancedTo: cursor, delivered, skipped, truncated: false, failed: false };
		for (const message of batch) {
			if (skipEmpty && message.content.trim() === "") {
				// Thread starters / system entries carry no text: skip without a send so the
				// cursor still advances and later messages keep backfilling.
				cursor = message.id;
				skipped++;
				continue;
			}
			const verdict = await options.deliver(message);
			if (verdict === "unavailable") {
				return { advancedTo: cursor, delivered, skipped, truncated: false, failed: true };
			}
			cursor = message.id;
			if (verdict === "skip") skipped++;
			else delivered++;
		}
		if (batch.length < pageLimit) return { advancedTo: cursor, delivered, skipped, truncated: false, failed: false };
	}
	return { advancedTo: cursor, delivered, skipped, truncated: true, failed: false };
}
