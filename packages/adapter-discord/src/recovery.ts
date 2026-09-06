import { mkdir, open, readFile, rename } from "node:fs/promises";
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
 * How a failed recovery send is classified. Only `terminal-message` may ever be discarded,
 * and only with per-payload evidence (this message's own content/size) plus proof that the
 * failure is NOT uniform across messages: `retryable` and `write-path-unknown` leave the gap
 * intact so nothing is lost when the gateway write path might be degraded or contract-broken.
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

/**
 * Per-conversation discard aggregate. Survives dead-letter eviction, so a mass-discard event
 * stays auditable even after the bounded record list has rolled over.
 */
export interface RecoveryDeadLetterDigest {
	readonly conversationId: string;
	readonly classification: RecoveryFailureClass;
	readonly count: number;
	readonly firstMessageId: string;
	readonly lastMessageId: string;
	readonly firstAt: string;
	readonly lastAt: string;
}

/** A parked watermark plus the insertion sequence its retention bound sorts on. */
export interface QuarantinedWatermark {
	readonly watermark: string;
	readonly seq: number;
}

/** Cross-pass attempt accounting for one missed message id. */
export interface RecoveryAttemptRecord {
	readonly conversationId: string;
	/** Failed send attempts across every pass. */
	readonly attempts: number;
	/** Subset of `attempts` classified `terminal-message`; only these can reach discard. */
	readonly terminalAttempts: number;
	readonly seq: number;
	readonly summary: string;
}

/** A DM conversation observed live and eligible for bounded restart recovery. */
export interface KnownDmConversation {
	readonly lastSeenAt: string;
	readonly seq: number;
}

export interface RecoveryCursorState {
	/** Highest message id a recovery pass walked past, per recoverable conversation. */
	readonly recoveredThrough: Readonly<Record<string, string>>;
	/**
	 * Watermarks for conversations recovery does not currently iterate: channels removed
	 * from config, plus thread/DM ids that never get backfill. Kept (not deleted) so a
	 * channel re-added to config resumes instead of replaying the bootstrap window, and
	 * bounded to RECOVERY_QUARANTINE_CAP entries, lowest `seq` (oldest) pruned first.
	 */
	readonly quarantined: Readonly<Record<string, QuarantinedWatermark>>;
	/** DMs cannot be enumerated from Discord, so live inbound records a bounded recovery set. */
	readonly knownDms: Readonly<Record<string, KnownDmConversation>>;
	/**
	 * Cross-pass per-message attempt ledger, so a message that alternates terminal and
	 * transient failures still reaches its discard threshold instead of blocking its
	 * channel forever. Bounded by RECOVERY_ATTEMPT_LEDGER_CAP, lowest `seq` pruned first.
	 */
	readonly attempts: Readonly<Record<string, RecoveryAttemptRecord>>;
	/** Bounded discard log, oldest pruned first (RECOVERY_DEAD_LETTER_CAP). */
	readonly deadLetters: readonly RecoveryDeadLetter[];
	/** Per-conversation discard aggregates; never evicted. */
	readonly deadLetterDigest: Readonly<Record<string, RecoveryDeadLetterDigest>>;
	/** Monotonic allocator for the `seq` fields above. */
	readonly sequence: number;
}

export type RecoveryGateResult = "acked" | "duplicate" | "unavailable";

const EMPTY_STATE: RecoveryCursorState = {
	recoveredThrough: {},
	quarantined: {},
	knownDms: {},
	attempts: {},
	deadLetters: [],
	deadLetterDigest: {},
	sequence: 0,
};

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
	const record = raw as Record<string, unknown>;
	const quarantined = readQuarantined(record.quarantined);
	const knownDms = readKnownDms(record.knownDms);
	const attempts = readAttempts(record.attempts);
	const seqs = [
		...Object.values(quarantined).map((entry) => entry.seq),
		...Object.values(knownDms).map((entry) => entry.seq),
		...Object.values(attempts).map((entry) => entry.seq),
	];
	const stored = typeof record.sequence === "number" && Number.isSafeInteger(record.sequence) ? record.sequence : 0;
	return {
		recoveredThrough: readWatermarks(record.recoveredThrough),
		quarantined,
		knownDms,
		attempts,
		deadLetters: readDeadLetters(record.deadLetters),
		deadLetterDigest: readDigest(record.deadLetterDigest),
		// Never hand back a sequence below anything already stored, or fresh entries would
		// collide with old ones and the retention order would be wrong after a restart.
		sequence: Math.max(stored, ...seqs, 0),
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

function readQuarantined(entries: unknown): Record<string, QuarantinedWatermark> {
	const parked: Record<string, QuarantinedWatermark> = {};
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value !== "object" || value === null) continue;
			const entry = value as Record<string, unknown>;
			if (typeof entry.watermark !== "string" || !/^\d+$/.test(entry.watermark)) continue;
			parked[id] = {
				watermark: entry.watermark,
				seq: typeof entry.seq === "number" && Number.isSafeInteger(entry.seq) ? entry.seq : 0,
			};
		}
	}
	return parked;
}

function readKnownDms(entries: unknown): Record<string, KnownDmConversation> {
	const known: Record<string, KnownDmConversation> = {};
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value !== "object" || value === null) continue;
			const entry = value as Record<string, unknown>;
			if (typeof entry.lastSeenAt !== "string" || !Number.isFinite(Date.parse(entry.lastSeenAt))) continue;
			known[id] = {
				lastSeenAt: entry.lastSeenAt,
				seq: typeof entry.seq === "number" && Number.isSafeInteger(entry.seq) ? entry.seq : 0,
			};
		}
	}
	return known;
}

/** Prunes expired and oldest known DMs without observing a new conversation. */
export function pruneKnownDms(
	state: RecoveryCursorState,
	nowMs: number,
	cap = RECOVERY_KNOWN_DM_CAP,
	ttlMs = RECOVERY_KNOWN_DM_TTL_MS,
): RecoveryCursorState {
	const cutoff = nowMs - ttlMs;
	const retained = Object.entries(state.knownDms)
		.filter(([, entry]) => Date.parse(entry.lastSeenAt) >= cutoff)
		.sort((a, b) => b[1].seq - a[1].seq)
		.slice(0, cap);
	const knownDms = Object.fromEntries(retained);
	if (Object.keys(knownDms).length === Object.keys(state.knownDms).length) return state;
	return { ...state, knownDms };
}

/** Records a live DM and prunes stale/oldest entries so Discord's non-enumerable DM set stays bounded. */
export function rememberKnownDm(
	state: RecoveryCursorState,
	conversationId: string,
	nowMs: number,
	cap = RECOVERY_KNOWN_DM_CAP,
	ttlMs = RECOVERY_KNOWN_DM_TTL_MS,
): RecoveryCursorState {
	const sequence = state.sequence + 1;
	return pruneKnownDms(
		{
			...state,
			knownDms: {
				...state.knownDms,
				[conversationId]: { lastSeenAt: new Date(nowMs).toISOString(), seq: sequence },
			},
			sequence,
		},
		nowMs,
		cap,
		ttlMs,
	);
}

function readAttempts(entries: unknown): Record<string, RecoveryAttemptRecord> {
	const ledger: Record<string, RecoveryAttemptRecord> = {};
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value !== "object" || value === null) continue;
			const entry = value as Record<string, unknown>;
			if (typeof entry.conversationId !== "string") continue;
			ledger[id] = {
				conversationId: entry.conversationId,
				attempts: typeof entry.attempts === "number" ? entry.attempts : 0,
				terminalAttempts: typeof entry.terminalAttempts === "number" ? entry.terminalAttempts : 0,
				seq: typeof entry.seq === "number" && Number.isSafeInteger(entry.seq) ? entry.seq : 0,
				summary: typeof entry.summary === "string" ? entry.summary : "",
			};
		}
	}
	return ledger;
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

function readDigest(entries: unknown): Record<string, RecoveryDeadLetterDigest> {
	const digest: Record<string, RecoveryDeadLetterDigest> = {};
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value !== "object" || value === null) continue;
			const entry = value as Record<string, unknown>;
			if (typeof entry.count !== "number" || entry.count <= 0) continue;
			digest[id] = {
				conversationId: typeof entry.conversationId === "string" ? entry.conversationId : id,
				classification: entry.classification === "retryable" ? "retryable" : "terminal-message",
				count: entry.count,
				firstMessageId: typeof entry.firstMessageId === "string" ? entry.firstMessageId : "",
				lastMessageId: typeof entry.lastMessageId === "string" ? entry.lastMessageId : "",
				firstAt: typeof entry.firstAt === "string" ? entry.firstAt : "",
				lastAt: typeof entry.lastAt === "string" ? entry.lastAt : "",
			};
		}
	}
	return digest;
}

/**
 * Atomic write (tmp + rename) plus an fsync of the temp file and of its directory, so a
 * completed save survives a host crash and never leaves a truncated file behind. Directory
 * fsync is best-effort: platforms that reject fsync on a directory fd still get the atomic
 * rename and the file-level fsync.
 */
export async function saveRecoveryCursors(path: string, state: RecoveryCursorState): Promise<void> {
	const payload = `${JSON.stringify(state, null, "\t")}\n`;
	const directory = dirname(path);
	await mkdir(directory, { recursive: true, mode: 0o700 });
	const tmp = `${path}.${process.pid}.tmp`;
	const handle = await open(tmp, "w", 0o600);
	try {
		await handle.writeFile(payload, "utf8");
		await handle.sync();
	} finally {
		await handle.close();
	}
	await rename(tmp, path);
	try {
		const dirHandle = await open(directory, "r");
		try {
			await dirHandle.sync();
		} finally {
			await dirHandle.close();
		}
	} catch {
		// Best-effort: the rename is already atomic, only the directory entry's durability
		// window widens on platforms that refuse fsync on a directory handle.
	}
}

/**
 * Partitions every known watermark by whether recovery currently iterates it. Nothing is
 * dropped for being unrecoverable: keys move into `quarantined` so a channel removed from
 * config and later re-added resumes from its watermark instead of replaying the bootstrap
 * window, and a quarantined key becomes live again the moment its channel is configured.
 *
 * Unbounded growth (thread/DM ids, long-dead channels) is the thing that is bounded:
 * `quarantined` keeps at most `cap` entries and drops the lowest `seq` first. Ordering is
 * explicit metadata, never JS object key order (numeric-looking ids reorder there).
 */
export function retainRecoveryCursors(
	state: RecoveryCursorState,
	recoverable: Iterable<string>,
	cap = RECOVERY_QUARANTINE_CAP,
): RecoveryCursorState {
	const allowed = new Set(recoverable);
	const recoveredThrough: Record<string, string> = {};
	const quarantined: Record<string, QuarantinedWatermark> = {};
	let sequence = state.sequence;
	for (const [id, value] of Object.entries(state.recoveredThrough)) {
		if (allowed.has(id)) recoveredThrough[id] = value;
		else quarantined[id] = { watermark: value, seq: ++sequence };
	}
	for (const [id, entry] of Object.entries(state.quarantined)) {
		// A previously parked key keeps its original seq (it is genuinely older); a key that
		// became recoverable again graduates back to the live map.
		if (allowed.has(id)) recoveredThrough[id] ??= entry.watermark;
		else quarantined[id] = entry;
	}
	const parked = Object.entries(quarantined).sort((a, b) => a[1].seq - b[1].seq);
	for (const [id] of parked.slice(0, Math.max(0, parked.length - cap))) delete quarantined[id];
	return { ...state, recoveredThrough, quarantined, sequence };
}

/**
 * Records one failed send attempt for `messageId`, accumulating across passes so an
 * alternating terminal/transient failure still reaches the discard threshold.
 *
 * Cap eviction never resets an ACTIVE entry: the message just recorded, each conversation's
 * head-of-line blocker (its numerically lowest message id, the one recovery keeps retrying)
 * and each conversation's most recent entry are protected. Only inactive entries are evicted,
 * lowest `seq` first, because dropping an active entry would silently reset the very counts
 * that let a blocked channel eventually drain. If every entry is active the ledger is left
 * over its cap and the overflow is logged loudly instead.
 */
export function recordAttempt(
	state: RecoveryCursorState,
	messageId: string,
	conversationId: string,
	classification: RecoveryFailureClass,
	summary: string,
	cap = RECOVERY_ATTEMPT_LEDGER_CAP,
): RecoveryCursorState {
	const previous = state.attempts[messageId];
	const sequence = previous ? state.sequence : state.sequence + 1;
	const attempts: Record<string, RecoveryAttemptRecord> = {
		...state.attempts,
		[messageId]: {
			conversationId,
			attempts: (previous?.attempts ?? 0) + 1,
			terminalAttempts: (previous?.terminalAttempts ?? 0) + (classification === "terminal-message" ? 1 : 0),
			seq: previous?.seq ?? sequence,
			summary,
		},
	};
	let overflow = Object.keys(attempts).length - cap;
	if (overflow > 0) {
		const active = activeLedgerIds(attempts, messageId);
		const evictable = Object.entries(attempts)
			.filter(([id]) => !active.has(id))
			.sort((a, b) => a[1].seq - b[1].seq);
		for (const [id] of evictable) {
			if (overflow <= 0) break;
			delete attempts[id];
			overflow--;
		}
		if (overflow > 0) {
			console.error(
				`Discord recovery attempt ledger is ${overflow} entry(s) over its ${cap}-entry cap with every entry still active; keeping them rather than resetting a blocked message's attempt counts.`,
			);
		}
	}
	return { ...state, attempts, sequence };
}

/** Ledger ids that must survive cap eviction: the current one, plus per-conversation
 * head-of-line and most-recent entries. */
function activeLedgerIds(attempts: Record<string, RecoveryAttemptRecord>, current: string): Set<string> {
	const active = new Set<string>([current]);
	const headOfLine = new Map<string, string>();
	const newest = new Map<string, { id: string; seq: number }>();
	for (const [id, entry] of Object.entries(attempts)) {
		const head = headOfLine.get(entry.conversationId);
		if (head === undefined || snowflakeIsAfter(head, id)) headOfLine.set(entry.conversationId, id);
		const latest = newest.get(entry.conversationId);
		if (latest === undefined || entry.seq > latest.seq) newest.set(entry.conversationId, { id, seq: entry.seq });
	}
	for (const id of headOfLine.values()) active.add(id);
	for (const entry of newest.values()) active.add(entry.id);
	return active;
}

/** Drops the attempt ledger entry for a message that finally landed (or was discarded). */
export function clearAttempt(state: RecoveryCursorState, messageId: string): RecoveryCursorState {
	if (!state.attempts[messageId]) return state;
	const attempts = { ...state.attempts };
	delete attempts[messageId];
	return { ...state, attempts };
}

/**
 * Appends a discard record (at most `cap` entries, oldest dropped first) and updates the
 * per-conversation digest, which is never evicted so mass discards stay auditable. Recording
 * is idempotent per message id within the retained window: a crash between the discard
 * persist and the end of the pass re-sends the id, and the replay must not double-count it.
 */
export function recordDeadLetter(
	state: RecoveryCursorState,
	entry: RecoveryDeadLetter,
	cap = RECOVERY_DEAD_LETTER_CAP,
): RecoveryCursorState {
	if (state.deadLetters.some((existing) => existing.messageId === entry.messageId)) return state;
	const deadLetters = [...state.deadLetters, entry];
	const previous = state.deadLetterDigest[entry.conversationId];
	const digest: RecoveryDeadLetterDigest = {
		conversationId: entry.conversationId,
		classification: entry.classification,
		count: (previous?.count ?? 0) + 1,
		firstMessageId: previous?.firstMessageId ?? entry.messageId,
		lastMessageId: entry.messageId,
		firstAt: previous?.firstAt ?? entry.at,
		lastAt: entry.at,
	};
	return {
		...state,
		deadLetters: deadLetters.slice(Math.max(0, deadLetters.length - cap)),
		deadLetterDigest: { ...state.deadLetterDigest, [entry.conversationId]: digest },
	};
}

/**
 * Protocol error codes that can only fail because of THIS message's own payload.
 * `payload_too_large` is raised by the local encodeFrame for this frame's bytes, so it is
 * per-payload evidence. Codes like invalid_params / malformed_frame / unsupported_frame_type
 * are deliberately NOT here: the gateway raises invalid_params for contract violations
 * ("non-loopback chat.send requires engagement", "requires a valid origin"), which fail every
 * message deterministically — treating those as terminal would discard a whole backfill.
 */
const TERMINAL_ERROR_CODES = new Set(["payload_too_large"]);
/** Protocol error codes that describe a transient link/gateway condition. */
const RETRYABLE_ERROR_CODES = new Set([
	"gateway_shutting_down",
	"negotiation_required",
	"incompatible_profile_version",
	"missing_required_capability",
]);
const RETRYABLE_ERRNO = /^(ECONN|EPIPE|ETIMEDOUT|ENOTCONN|EAGAIN|EBUSY|SQLITE_BUSY|SQLITE_LOCKED)/;
/**
 * Content faults that name THIS message's own payload (size/encoding of its text). Contract
 * wording like "invalid params" or "requires engagement" is excluded on purpose: it describes
 * the caller's contract, not the message, and hits every message alike.
 */
const TERMINAL_TEXT = /payload too large|message too long|content too long|content exceeds|frame exceeds/i;
const RETRYABLE_TEXT =
	/timed out|timeout|econnreset|econnrefused|epipe|enotconn|socket hang up|sqlite_busy|sqlite_locked|database is locked|queue (is )?(saturated|full)|backpressure|temporarily|try again|unavailable|\b5\d{2}\b/i;

/**
 * Classifies a failed `chat.send`. The default is deliberately `write-path-unknown`: a
 * failure nobody recognized may have partially applied on the gateway write path, so the
 * caller must retry it and MUST NOT advance the cursor past it. `terminal-message` means only
 * "the evidence points at this payload"; the caller must still prove the failure is not
 * uniform across messages before discarding anything (see recoverConversation).
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
 * Cross-pass terminal-failure budget for one message id. Once a message has burned this many
 * `terminal-message` attempts it becomes a discard *candidate*; the discard only commits when
 * a later message in the same pass succeeds (proof the failure is not uniform).
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
/** Retention bound for the cross-pass per-message attempt ledger. */
export const RECOVERY_ATTEMPT_LEDGER_CAP = 200;
/**
 * How many consecutive discard candidates (distinct message ids, no success in between) mean
 * "this looks uniform, not message-specific". At that point the pass bails out without
 * discarding anything and without advancing the cursor.
 */
export const RECOVERY_UNIFORM_FAILURE_LIMIT = 3;

/** The slice of a discord.js text-based channel recovery needs: forward paged history. */
/** Maximum non-enumerable DM conversations retained for restart recovery. */
export const RECOVERY_KNOWN_DM_CAP = 100;
/** DMs with no live inbound for this long leave the restart-recovery set. */
export const RECOVERY_KNOWN_DM_TTL_MS = 30 * 24 * 60 * 60 * 1000;
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
 * Per-message delivery verdict handed back by the caller's send path.
 *
 * - `acked`: a chat.send just landed. This is the ONLY write-path evidence recovery accepts.
 * - `duplicate`: some earlier attempt already covered this id — a previous pass, or just the
 *   in-process RecoveryGate acked cache. It says nothing about the write path right now and
 *   can never justify a discard.
 * - `skip`: nothing to send (no engagement, empty content); the cursor advances.
 * - `discard-candidate`: this message burned its terminal-failure budget and looks
 *   message-specific. It is NOT discarded here: it is held until a fresh `acked` lands later
 *   in the same pass, which is the only proof the failure is not uniform.
 */
export type RecoveryDelivery = "acked" | "duplicate" | "unavailable" | "skip" | "discard-candidate";

export interface RecoveryOutcome {
	/** Highest message id this run walked past (delivered, duplicate, skipped or discarded). */
	readonly advancedTo: string;
	readonly delivered: number;
	/** Messages already acknowledged by an earlier live or recovery attempt. */
	readonly duplicates: number;
	/** Messages the run walked past without a send (empty content or caller `skip`). */
	readonly skipped: number;
	/** Discards committed this run (candidate + a later FRESH acked in the same pass). */
	readonly discarded: number;
	/** Distinct candidates still held at the end of the run: not discarded, not advanced past. */
	readonly held: number;
	/** True when the gap exceeded the page bound; older messages may remain unrecovered. */
	readonly truncated: boolean;
	/** True when the run could not finish (unavailable delivery, uniform failure, fetch error). */
	readonly failed: boolean;
	/** Set when the history fetch itself threw; the caller logs it and retries the channel. */
	readonly fetchError?: string;
}

export interface RecoveryOptions {
	readonly cursor?: string;
	readonly nowMs: number;
	readonly pageLimit?: number;
	readonly maxPages?: number;
	readonly uniformFailureLimit?: number;
	deliver(message: DiscordInboundMessage): Promise<RecoveryDelivery>;
	/** Called once per committed discard, before the cursor moves past the message. */
	onDiscard?(message: DiscordInboundMessage): void;
	/**
	 * Treats empty/whitespace-only content (thread starters, system entries) as intentional
	 * non-text: skipped without a send, cursor advances past them. Default: skip.
	 */
	readonly skipEmptyContent?: boolean;
}

/**
 * Replays the bounded gap after `cursor` in ascending id order, delivering each message and
 * advancing only past finished ones. Stops early on an unavailable delivery or when discard
 * candidates pile up (uniform failure); reports `truncated` when `maxPages` full pages were
 * consumed. A throwing history fetch is contained here: it ends this conversation's run with
 * `failed` + `fetchError`, never the caller's whole pass.
 */
export async function recoverConversation(
	channel: RecoverableChannel,
	options: RecoveryOptions,
): Promise<RecoveryOutcome> {
	const pageLimit = options.pageLimit ?? RECOVERY_PAGE_LIMIT;
	const maxPages = options.maxPages ?? RECOVERY_MAX_PAGES;
	const uniformLimit = options.uniformFailureLimit ?? RECOVERY_UNIFORM_FAILURE_LIMIT;
	const skipEmpty = options.skipEmptyContent ?? true;
	let cursor = options.cursor ?? snowflakeFromTimestamp(options.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS);
	// The durable cursor stays behind held candidates. The scan cursor still advances through
	// the fetched window so one blocked tail message cannot make every later page refetch the
	// already-processed prefix in the same pass.
	let scanCursor = cursor;
	let delivered = 0;
	let duplicates = 0;
	let skipped = 0;
	let discarded = 0;
	// Candidates whose discard is not yet justified, keyed by message id so a refetched page
	// can neither inflate the uniform-failure limit nor fire onDiscard twice for one id. They
	// sit between the cursor and the message being delivered, so nothing advances past them
	// until a FRESH acked send lands later in this same pass.
	const held = new Map<string, DiscordInboundMessage>();
	const commitHeld = (): void => {
		for (const candidate of held.values()) {
			options.onDiscard?.(candidate);
			discarded++;
		}
		held.clear();
	};
	for (let page = 0; page < maxPages; page++) {
		let fetched: Awaited<ReturnType<RecoverableChannel["messages"]["fetch"]>>;
		try {
			fetched = await channel.messages.fetch({ after: scanCursor, limit: pageLimit });
		} catch (error) {
			return {
				advancedTo: cursor,
				delivered,
				duplicates,
				skipped,
				discarded,
				held: held.size,
				truncated: false,
				failed: true,
				fetchError: error instanceof Error ? error.message : String(error),
			};
		}
		// discord.js returns a Collection (entries); fake channels return arrays. Normalize
		// to values first, then sort numerically so either API ordering is safe.
		const rawBatch = [
			...(Symbol.iterator in fetched
				? fetched
				: (fetched as { entries(): Iterable<[string, DiscordInboundMessage]> }).entries()),
		].map((entry) => (Array.isArray(entry) ? (entry[1] as DiscordInboundMessage) : (entry as DiscordInboundMessage)));
		const batch = rawBatch.filter((message) => snowflakeIsAfter(message.id, scanCursor));
		if (rawBatch.length >= pageLimit && batch.length === 0) {
			// A broken/repeated page must not spin until the bound while pretending to
			// make progress. Leave the cursor unchanged so the next reconnect retries it.
			return {
				advancedTo: cursor,
				delivered,
				duplicates,
				skipped,
				discarded,
				held: held.size,
				truncated: true,
				failed: held.size > 0,
			};
		}
		batch.sort((a, b) => {
			const ai = BigInt(a.id);
			const bi = BigInt(b.id);
			return ai < bi ? -1 : ai > bi ? 1 : 0;
		});
		if (batch.length === 0) {
			return {
				advancedTo: cursor,
				delivered,
				duplicates,
				skipped,
				discarded,
				held: held.size,
				truncated: false,
				failed: held.size > 0,
			};
		}
		for (const message of batch) {
			if (skipEmpty && message.content.trim() === "") {
				// Thread starters / system entries carry no text: skip without a send. This is
				// not delivery evidence, so it cannot justify a held discard: keep holding.
				if (held.size === 0) cursor = message.id;
				scanCursor = message.id;
				skipped++;
				continue;
			}
			const verdict = await options.deliver(message);
			if (verdict === "unavailable") {
				return {
					advancedTo: cursor,
					delivered,
					duplicates,
					skipped,
					discarded,
					held: held.size,
					truncated: false,
					failed: true,
				};
			}
			if (verdict === "discard-candidate") {
				held.set(message.id, message);
				scanCursor = message.id;
				if (held.size >= uniformLimit) {
					// The same terminal-looking failure on `uniformLimit` DISTINCT ids with no
					// fresh ack in between: that is a write-path problem, not a payload problem.
					// Drop the held candidates without discarding and leave the cursor alone.
					return {
						advancedTo: cursor,
						delivered,
						duplicates,
						skipped,
						discarded,
						held: held.size,
						truncated: false,
						failed: true,
					};
				}
				continue;
			}
			if (verdict === "skip" || verdict === "duplicate") {
				// Neither is proof that the write path works right now. `skip` sent nothing at
				// all, and `duplicate` only says some earlier attempt — possibly in a previous
				// pass, possibly just the in-process gate's acked cache — already covered this
				// id. Held candidates keep being held and the durable cursor stays behind them,
				// but the in-pass scan moves forward so later pages do not refetch this prefix.
				scanCursor = message.id;
				if (held.size === 0) cursor = message.id;
				if (verdict === "duplicate") duplicates++;
				else skipped++;
				continue;
			}
			// A fresh chat.send just returned acked, in this pass, after the held candidates
			// failed. That is the only accepted evidence that the write path works, so those
			// candidates really are message-specific: commit their discards and advance.
			// This id is never discarded on the strength of its own ack: a refetched page can
			// put an id in `held` and then land it on a later attempt.
			held.delete(message.id);
			commitHeld();
			cursor = message.id;
			scanCursor = message.id;
			delivered++;
		}
		if (batch.length < pageLimit) {
			return {
				advancedTo: cursor,
				delivered,
				duplicates,
				skipped,
				discarded,
				held: held.size,
				truncated: false,
				failed: held.size > 0,
			};
		}
	}
	return {
		advancedTo: cursor,
		delivered,
		duplicates,
		skipped,
		discarded,
		held: held.size,
		truncated: true,
		failed: held.size > 0,
	};
}
