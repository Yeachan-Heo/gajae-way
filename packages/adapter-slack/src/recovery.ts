import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import { SlackApiError, type SlackHistoryPage } from "./api";
import { adapterHome } from "./config";
import type { SlackInboundMessage } from "./main";
import { isSlackDmChannel } from "./origin";

export const RECOVERY_BOOTSTRAP_LOOKBACK_MS = 24 * 60 * 60 * 1000;
export const RECOVERY_PAGE_LIMIT = 200;
export const RECOVERY_MAX_PAGES = 10;
export const RECOVERY_KNOWN_DM_CAP = 100;
export const RECOVERY_KNOWN_DM_TTL_MS = 30 * 24 * 60 * 60 * 1000;
export const RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS = 3;
export const RECOVERY_RETRY_BASE_MS = 1_000;
export const RECOVERY_RETRY_MAX_MS = 60_000;
/** Threads the persona took part in that recovery keeps revisiting after their parent fell behind the watermark. */
export const RECOVERY_PARTICIPATED_THREAD_CAP = 200;
export const RECOVERY_PARTICIPATED_THREAD_TTL_MS = 7 * 24 * 60 * 60 * 1000;
/**
 * How many times one message may fail with a payload-specific (terminal)
 * classification before it is dead-lettered so the channel can move on. Only
 * terminal failures count; a link outage never burns this budget.
 */
export const RECOVERY_MAX_ATTEMPTS = 3;
export const RECOVERY_DEAD_LETTER_CAP = 50;
export const RECOVERY_ATTEMPT_LEDGER_CAP = 200;

/**
 * How a failed recovery send is classified, mirroring the Discord adapter.
 * `terminal-message` is the gateway refusing THIS payload (invalid_params,
 * an oversize body); `retryable` is the link or the gateway being unavailable;
 * `write-path-unknown` is everything else. Only terminal failures may ever
 * consume a message's attempt budget.
 */
export type RecoveryFailureClass = "retryable" | "terminal-message" | "write-path-unknown";

export interface RecoveryDeadLetter {
	readonly messageId: string;
	readonly conversationId: string;
	readonly classification: RecoveryFailureClass;
	readonly attempts: number;
	readonly reason: string;
	readonly at: string;
}

/** Per-conversation discard aggregate; never evicted, so a long-running problem stays visible. */
export interface RecoveryDeadLetterDigest {
	readonly conversationId: string;
	readonly classification: RecoveryFailureClass;
	readonly count: number;
	readonly firstAt: string;
	readonly lastAt: string;
	readonly lastReason: string;
}

export interface RecoveryAttemptRecord {
	readonly conversationId: string;
	readonly attempts: number;
	readonly classification: RecoveryFailureClass;
	readonly reason: string;
	readonly lastAt: string;
}

export interface RecoveryCursorState {
	readonly recoveredThrough: Readonly<Record<string, string>>;
	/**
	 * A scan that hit its page bound could not deliver the whole gap. The oldest ts it
	 * DID walk past is kept here so the next pass resumes below it instead of refetching
	 * the same newest suffix forever; the durable watermark still only moves once the
	 * gap is closed.
	 */
	readonly continuation: Readonly<Record<string, { readonly olderThan: string; readonly through: string }>>;
	readonly knownDms: Readonly<Record<string, { readonly lastSeenAt: string }>>;
	/** Thread roots (`channel:ts`) the persona replied in; revisited independently of the channel watermark. */
	readonly participatedThreads: Readonly<Record<string, { readonly lastSeenAt: string; readonly through?: string }>>;
	/**
	 * Thread roots whose reply walk was cut short by the page bound; drained on
	 * later passes until complete. `through` is the root's own reply cursor so
	 * an unengaged root still makes progress instead of re-walking its prefix.
	 */
	readonly pendingThreads: Readonly<Record<string, { readonly since: string; readonly through?: string }>>;
	readonly quarantined: Readonly<
		Record<string, { readonly reason: string; readonly failures: number; readonly since: string }>
	>;
	readonly attempts: Readonly<Record<string, RecoveryAttemptRecord>>;
	readonly deadLetters: readonly RecoveryDeadLetter[];
	readonly deadLetterDigest: Readonly<Record<string, RecoveryDeadLetterDigest>>;
}

export const EMPTY_RECOVERY_STATE: RecoveryCursorState = {
	recoveredThrough: {},
	continuation: {},
	knownDms: {},
	participatedThreads: {},
	pendingThreads: {},
	quarantined: {},
	attempts: {},
	deadLetters: [],
	deadLetterDigest: {},
};

export function recoveryCursorPath(home: string = adapterHome()): string {
	return join(home, "adapters", "slack", "recovery-cursor.json");
}

export async function loadRecoveryCursors(path: string): Promise<RecoveryCursorState> {
	let text: string;
	try {
		text = await readFile(path, "utf8");
	} catch (error) {
		if ((error as { code?: string }).code === "ENOENT") return EMPTY_RECOVERY_STATE;
		throw error;
	}
	try {
		const state = JSON.parse(text);
		const record = (value: unknown): value is Record<string, unknown> =>
			typeof value === "object" && value !== null && !Array.isArray(value);
		const isTs = (value: unknown): boolean => typeof value === "string" && /^\d+\.\d+$/.test(value);
		const isClass = (value: unknown): boolean =>
			value === "retryable" || value === "terminal-message" || value === "write-path-unknown";
		if (
			!record(state) ||
			!record(state.recoveredThrough) ||
			!record(state.knownDms) ||
			!record(state.quarantined) ||
			!Array.isArray(state.deadLetters) ||
			(state.continuation !== undefined &&
				(!record(state.continuation) ||
					!Object.values(state.continuation).every(
						(entry) => record(entry) && isTs(entry.olderThan) && isTs(entry.through),
					))) ||
			(state.participatedThreads !== undefined &&
				(!record(state.participatedThreads) ||
					!Object.values(state.participatedThreads).every(
						(entry) =>
							record(entry) &&
							typeof entry.lastSeenAt === "string" &&
							(entry.through === undefined || isTs(entry.through)),
					))) ||
			(state.pendingThreads !== undefined &&
				(!record(state.pendingThreads) ||
					!Object.values(state.pendingThreads).every(
						(entry) =>
							record(entry) && typeof entry.since === "string" && (entry.through === undefined || isTs(entry.through)),
					))) ||
			(state.attempts !== undefined &&
				(!record(state.attempts) ||
					!Object.values(state.attempts).every(
						(entry) =>
							record(entry) &&
							typeof entry.conversationId === "string" &&
							Number.isInteger(entry.attempts) &&
							isClass(entry.classification),
					))) ||
			(state.deadLetterDigest !== undefined &&
				(!record(state.deadLetterDigest) ||
					!Object.values(state.deadLetterDigest).every(
						(entry) => record(entry) && Number.isInteger(entry.count) && isClass(entry.classification),
					))) ||
			!Object.values(state.recoveredThrough).every(isTs) ||
			!Object.values(state.knownDms).every(
				(entry) =>
					record(entry) && typeof entry.lastSeenAt === "string" && Number.isFinite(Date.parse(entry.lastSeenAt)),
			) ||
			!Object.values(state.quarantined).every(
				(entry) =>
					record(entry) &&
					typeof entry.reason === "string" &&
					Number.isInteger(entry.failures) &&
					(entry.failures as number) >= 0 &&
					typeof entry.since === "string",
			) ||
			!state.deadLetters.every(
				(entry) =>
					record(entry) &&
					[entry.messageId, entry.conversationId, entry.reason, entry.at].every((value) => typeof value === "string"),
			)
		) {
			throw new Error("Invalid Slack recovery cursor state");
		}
		return { ...EMPTY_RECOVERY_STATE, ...(state as unknown as Partial<RecoveryCursorState>) } as RecoveryCursorState;
	} catch (error) {
		throw new Error(`Cannot load Slack recovery cursors at ${path}`, { cause: error });
	}
}

/** Atomic replacement keeps interrupted saves from destroying the last recovery watermark. */
export async function saveRecoveryCursors(path: string, state: RecoveryCursorState): Promise<void> {
	await mkdir(dirname(path), { recursive: true, mode: 0o700 });
	const temporary = `${path}.${randomUUID()}.tmp`;
	try {
		const file = await open(temporary, "wx", 0o600);
		try {
			await file.writeFile(`${JSON.stringify(state, null, "\t")}\n`);
			await file.sync();
		} finally {
			await file.close();
		}
		await rename(temporary, path);
	} finally {
		await unlink(temporary).catch((error: NodeJS.ErrnoException) => {
			if (error.code !== "ENOENT") throw error;
		});
	}
}

export function pruneKnownDms(state: RecoveryCursorState, nowMs: number): RecoveryCursorState {
	const entries = Object.entries(state.knownDms)
		.filter(([, entry]) => Date.parse(entry.lastSeenAt) > nowMs - RECOVERY_KNOWN_DM_TTL_MS)
		.sort((a, b) => Date.parse(b[1].lastSeenAt) - Date.parse(a[1].lastSeenAt))
		.slice(0, RECOVERY_KNOWN_DM_CAP);
	return { ...state, knownDms: Object.fromEntries(entries) };
}

export function rememberKnownDm(state: RecoveryCursorState, channel: string, nowMs: number): RecoveryCursorState {
	return pruneKnownDms(
		isSlackDmChannel(channel)
			? {
					...state,
					knownDms: { ...state.knownDms, [channel]: { lastSeenAt: new Date(nowMs).toISOString() } },
				}
			: state,
		nowMs,
	);
}

export function pruneParticipatedThreads(state: RecoveryCursorState, nowMs: number): RecoveryCursorState {
	const entries = Object.entries(state.participatedThreads)
		.filter(([, entry]) => Date.parse(entry.lastSeenAt) > nowMs - RECOVERY_PARTICIPATED_THREAD_TTL_MS)
		.sort((a, b) => Date.parse(b[1].lastSeenAt) - Date.parse(a[1].lastSeenAt))
		.slice(0, RECOVERY_PARTICIPATED_THREAD_CAP);
	return { ...state, participatedThreads: Object.fromEntries(entries) };
}

/**
 * Records a thread the persona is part of. History only returns a thread's PARENT,
 * and only while that parent is above the channel watermark; once the watermark
 * moves past it, later replies in that thread would never be discovered from the
 * channel scan alone. These roots are therefore revisited on their own.
 */
export function rememberParticipatedThread(
	state: RecoveryCursorState,
	threadKey: string,
	nowMs: number,
): RecoveryCursorState {
	const prior = state.participatedThreads[threadKey];
	return pruneParticipatedThreads(
		{
			...state,
			participatedThreads: {
				...state.participatedThreads,
				[threadKey]: {
					...(prior?.through ? { through: prior.through } : {}),
					lastSeenAt: new Date(nowMs).toISOString(),
				},
			},
		},
		nowMs,
	);
}

/** Records one failed recovery send; returns the updated state and whether the budget is spent. */
export function recordAttempt(
	state: RecoveryCursorState,
	messageId: string,
	conversationId: string,
	classification: RecoveryFailureClass,
	reason: string,
	nowMs: number,
): { readonly state: RecoveryCursorState; readonly exhausted: boolean } {
	const prior = state.attempts[messageId];
	// Only payload-specific failures burn the budget: a link outage is not this message's fault.
	const attempts = (prior?.attempts ?? 0) + (classification === "terminal-message" ? 1 : 0);
	const entries = Object.entries({
		...state.attempts,
		[messageId]: {
			conversationId,
			attempts,
			classification,
			reason: reason.slice(0, 200),
			lastAt: new Date(nowMs).toISOString(),
		},
	})
		.sort((a, b) => Date.parse(a[1].lastAt) - Date.parse(b[1].lastAt))
		.slice(-RECOVERY_ATTEMPT_LEDGER_CAP);
	return {
		state: { ...state, attempts: Object.fromEntries(entries) },
		exhausted: classification === "terminal-message" && attempts >= RECOVERY_MAX_ATTEMPTS,
	};
}

export function clearAttempt(state: RecoveryCursorState, messageId: string): RecoveryCursorState {
	if (!state.attempts[messageId]) return state;
	const { [messageId]: _cleared, ...attempts } = state.attempts;
	return { ...state, attempts };
}

/** Dead-letters a message: bounded log plus a per-conversation digest that is never evicted. */
export function recordDeadLetter(state: RecoveryCursorState, entry: RecoveryDeadLetter): RecoveryCursorState {
	const digest = state.deadLetterDigest[entry.conversationId];
	return {
		...clearAttempt(state, entry.messageId),
		deadLetters: [...state.deadLetters, entry].slice(-RECOVERY_DEAD_LETTER_CAP),
		deadLetterDigest: {
			...state.deadLetterDigest,
			[entry.conversationId]: {
				conversationId: entry.conversationId,
				classification: entry.classification,
				count: (digest?.count ?? 0) + 1,
				firstAt: digest?.firstAt ?? entry.at,
				lastAt: entry.at,
				lastReason: entry.reason,
			},
		},
	};
}

/**
 * Classifies a failed chat.send. The gateway's typed `invalid_params` (and any
 * frame-size refusal) is about THIS payload; a closed link or a timeout says
 * nothing about the message. Anything else is unknown and never discardable.
 */
export function classifyRecoveryFailure(error: unknown): RecoveryFailureClass {
	const code = (error as { code?: unknown } | null | undefined)?.code;
	if (code === "invalid_params" || code === "payload_too_large") return "terminal-message";
	const message = error instanceof Error ? error.message : String(error);
	if (/not connected|connection closed|timed out|client closed/i.test(message)) return "retryable";
	return "write-path-unknown";
}

export function tsIsAfter(a: string, b: string): boolean {
	const [as = "0", af = ""] = a.split(".");
	const [bs = "0", bf = ""] = b.split(".");
	const secondsA = BigInt(as);
	const secondsB = BigInt(bs);
	return secondsA !== secondsB
		? secondsA > secondsB
		: af.padEnd(Math.max(af.length, bf.length), "0") > bf.padEnd(Math.max(af.length, bf.length), "0");
}

export function tsFromTimestamp(ms: number): string {
	return `${Math.floor(ms / 1000)}.000000`;
}

/**
 * - `acked`: the gateway just accepted the send; the only write-path proof.
 * - `duplicate`: the gateway (or an earlier pass) already knew the id.
 * - `skip`: nothing to send; the cursor walks past it.
 * - `unavailable`: the send did not happen; the cursor stops here.
 * - `discard`: the message burned its terminal-failure budget and was dead-lettered
 *   by the caller; the cursor walks past it so one poison message cannot pin a channel.
 */
export type RecoveryDelivery = "acked" | "duplicate" | "unavailable" | "skip" | "discard";
export interface RecoveryHistoryPort {
	history(
		channel: string,
		options: { oldest: string; latest?: string; cursor?: string; limit: number },
	): Promise<SlackHistoryPage>;
	replies(
		channel: string,
		threadTs: string,
		options: { oldest: string; cursor?: string; limit: number },
	): Promise<SlackHistoryPage>;
}
export interface RecoveryOptions {
	readonly cursor?: string;
	/** Resume a truncated scan below this ts (exclusive) instead of from the newest page. */
	readonly latest?: string;
	readonly nowMs: number;
	readonly pageLimit?: number;
	readonly maxPages?: number;
	readonly botUserId: string;
	/**
	 * `stale` is true when the room has demonstrably moved past this message: it is
	 * older than STALE_BACKFILL_MS and the bot itself posted in the same
	 * conversation after it. The adapter records such a message as context only;
	 * it must not open a turn.
	 */
	deliver(message: SlackInboundMessage, stale: boolean): Promise<RecoveryDelivery>;
}

/**
 * A backfilled message older than this, with a later bot post in the same
 * conversation, is history the persona already acted on - not a question
 * waiting for an answer. Short enough that a genuine catch-up after a brief
 * outage still answers; long enough that a message a human has watched the
 * bot answer around is never answered again.
 */
export const STALE_BACKFILL_MS = 10 * 60_000;

/**
 * Which backfilled messages are stale, decided over the whole fetched window
 * so no extra API call is needed: a message is stale when it is at least
 * STALE_BACKFILL_MS old at `nowMs` AND the bot posted later in the same
 * conversation (thread, or top level for a root). The bot's own rows are the
 * evidence; they are still skipped for delivery afterwards.
 */
export function staleBackfillIds(
	ordered: readonly SlackInboundMessage[],
	botUserId: string,
	nowMs: number,
): ReadonlySet<string> {
	const latestBotTsByConversation = new Map<string, string>();
	for (const message of ordered) {
		if (message.user !== botUserId) continue;
		const key = message.thread_ts ?? "";
		const prior = latestBotTsByConversation.get(key);
		if (!prior || tsIsAfter(message.ts, prior)) latestBotTsByConversation.set(key, message.ts);
	}
	const stale = new Set<string>();
	for (const message of ordered) {
		if (message.user === botUserId) continue;
		if (nowMs - Number(message.ts) * 1000 < STALE_BACKFILL_MS) continue;
		const botAfter = latestBotTsByConversation.get(message.thread_ts ?? "");
		if (botAfter && tsIsAfter(botAfter, message.ts)) stale.add(message.ts);
	}
	return stale;
}
export interface RecoveryOutcome {
	readonly advancedTo?: string;
	/** On a truncated scan: the oldest ts walked past and the newest, so the caller can resume below/up to them. */
	readonly continuation?: { readonly olderThan: string; readonly through: string };
	/**
	 * Thread roots whose reply walk hit the page bound. Their remaining replies are
	 * not in this outcome and must be drained by a per-root walk later, whether or
	 * not the persona engaged in anything fetched so far.
	 */
	readonly truncatedThreads?: readonly string[];
	readonly delivered: number;
	readonly duplicates: number;
	readonly skipped: number;
	readonly discarded: number;
	readonly truncated: boolean;
	readonly failed: boolean;
	readonly fetchError?: string;
	readonly permanent?: boolean;
}

/** Live traffic never advances this watermark: seeing a new message proves nothing about the gap. */
export async function recoverConversation(
	port: RecoveryHistoryPort,
	channel: string,
	options: RecoveryOptions,
): Promise<RecoveryOutcome> {
	const oldest = options.cursor ?? tsFromTimestamp(options.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS);
	const limit = Math.min(RECOVERY_PAGE_LIMIT, Math.max(1, Math.floor(options.pageLimit ?? RECOVERY_PAGE_LIMIT)));
	const maxPages = Math.max(1, Math.floor(options.maxPages ?? RECOVERY_MAX_PAGES));
	const messages = new Map<string, SlackInboundMessage>();
	let truncated = false;
	let advancedTo: string | undefined;
	let delivered = 0;
	let duplicates = 0;
	let skipped = 0;
	let discarded = 0;
	// Slack pages newest-first, so a truncated window is the newest suffix of the gap.
	// Its oldest fetched ts is where the next pass resumes (as `latest`), and its
	// newest is what the watermark may finally become once the gap closes.
	let oldestFetched: string | undefined;
	let newestFetched: string | undefined;
	// A reply walk that hit its bound is reported per root; it never marks the
	// channel scan itself truncated, because the channel's own pages were complete.
	const truncatedThreads: string[] = [];
	const outcome = (failed: boolean): RecoveryOutcome => ({
		advancedTo: truncated ? undefined : advancedTo,
		...(truncated && oldestFetched && newestFetched && !failed
			? { continuation: { olderThan: oldestFetched, through: options.latest ?? newestFetched } }
			: {}),
		...(truncatedThreads.length > 0 ? { truncatedThreads } : {}),
		delivered,
		duplicates,
		skipped,
		discarded,
		truncated,
		failed,
	});
	const collect = async (
		fetchPage: (cursor?: string) => Promise<SlackHistoryPage>,
		threadTs?: string,
	): Promise<void> => {
		let cursor: string | undefined;
		const cursors = new Set<string>();
		for (let page = 0; page < maxPages; page++) {
			const result = await fetchPage(cursor);
			for (const raw of result.messages) {
				if (typeof raw.ts !== "string" || !/^\d+\.\d+$/.test(raw.ts)) throw new SlackApiError(200, "invalid_response");
				if (raw.ts === threadTs || !tsIsAfter(raw.ts, oldest)) continue;
				if (!threadTs && options.latest && !tsIsAfter(options.latest, raw.ts)) continue;
				if (!threadTs) {
					if (!oldestFetched || tsIsAfter(oldestFetched, raw.ts)) oldestFetched = raw.ts;
					if (!newestFetched || tsIsAfter(raw.ts, newestFetched)) newestFetched = raw.ts;
				}
				messages.set(raw.ts, { ...raw, channel, ...(threadTs ? { thread_ts: threadTs } : {}) } as SlackInboundMessage);
			}
			if (!result.has_more && !result.next_cursor) return;
			if (!result.next_cursor || cursors.has(result.next_cursor) || page + 1 === maxPages) {
				if (threadTs) truncatedThreads.push(threadTs);
				else truncated = true;
				return;
			}
			cursor = result.next_cursor;
			cursors.add(cursor);
		}
	};
	try {
		await collect((cursor) =>
			port.history(channel, { oldest, ...(options.latest ? { latest: options.latest } : {}), cursor, limit }),
		);
		for (const message of [...messages.values()]) {
			const raw = message as SlackInboundMessage & { reply_count?: number };
			if ((raw.reply_count ?? 0) > 0 || message.thread_ts === message.ts) {
				await collect((cursor) => port.replies(channel, message.ts, { oldest, cursor, limit }), message.ts);
			}
		}
	} catch (error) {
		return {
			...outcome(true),
			fetchError: error instanceof SlackApiError ? error.code : String(error),
			permanent:
				error instanceof SlackApiError &&
				["channel_not_found", "not_in_channel", "missing_scope", "is_archived", "invalid_auth"].includes(error.code),
		};
	}
	// Slack pages newest-first. Sort the entire bounded window before acknowledging anything;
	// an incomplete window cannot advance a durable cursor past older, unfetched messages.
	const ordered = [...messages.values()].sort((a, b) => (tsIsAfter(a.ts, b.ts) ? 1 : tsIsAfter(b.ts, a.ts) ? -1 : 0));
	const stale = staleBackfillIds(ordered, options.botUserId, options.nowMs);
	for (const message of ordered) {
		let result: RecoveryDelivery;
		try {
			result = message.user === options.botUserId ? "skip" : await options.deliver(message, stale.has(message.ts));
		} catch {
			return outcome(true);
		}
		if (result === "unavailable") return outcome(true);
		if (result === "acked") delivered++;
		else if (result === "duplicate") duplicates++;
		else if (result === "discard") discarded++;
		else skipped++;
		advancedTo = message.ts;
	}
	return outcome(false);
}

/**
 * Walks ONE thread's replies newer than `cursor` (or the bootstrap window), in ts
 * order, independently of the channel scan. Used for threads the persona took
 * part in: `conversations.history` only lists thread parents, so once the channel
 * watermark passes a parent, later replies underneath it are invisible to the
 * channel scan and must be found here.
 */
export async function recoverThread(
	port: Pick<RecoveryHistoryPort, "replies">,
	channel: string,
	threadTs: string,
	options: Omit<RecoveryOptions, "latest">,
): Promise<RecoveryOutcome> {
	const oldest = options.cursor ?? tsFromTimestamp(options.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS);
	const limit = Math.min(RECOVERY_PAGE_LIMIT, Math.max(1, Math.floor(options.pageLimit ?? RECOVERY_PAGE_LIMIT)));
	const maxPages = Math.max(1, Math.floor(options.maxPages ?? RECOVERY_MAX_PAGES));
	const messages = new Map<string, SlackInboundMessage>();
	let truncated = false;
	let advancedTo: string | undefined;
	let delivered = 0;
	let duplicates = 0;
	let skipped = 0;
	let discarded = 0;
	const outcome = (failed: boolean): RecoveryOutcome => ({
		// Replies page oldest-first, so a truncated walk has delivered a contiguous
		// prefix and the cursor may safely advance to its end.
		advancedTo,
		delivered,
		duplicates,
		skipped,
		discarded,
		truncated,
		failed,
	});
	try {
		let cursor: string | undefined;
		for (let page = 0; page < maxPages; page++) {
			const result = await port.replies(channel, threadTs, { oldest, cursor, limit });
			for (const raw of result.messages) {
				if (typeof raw.ts !== "string" || !/^\d+\.\d+$/.test(raw.ts)) throw new SlackApiError(200, "invalid_response");
				if (raw.ts === threadTs || !tsIsAfter(raw.ts, oldest)) continue;
				messages.set(raw.ts, { ...raw, channel, thread_ts: threadTs } as SlackInboundMessage);
			}
			if (!result.has_more && !result.next_cursor) break;
			if (!result.next_cursor || page + 1 === maxPages) {
				truncated = true;
				break;
			}
			cursor = result.next_cursor;
		}
	} catch (error) {
		return {
			...outcome(true),
			fetchError: error instanceof SlackApiError ? error.code : String(error),
			permanent:
				error instanceof SlackApiError &&
				[
					"channel_not_found",
					"not_in_channel",
					"missing_scope",
					"is_archived",
					"invalid_auth",
					"thread_not_found",
				].includes(error.code),
		};
	}
	const ordered = [...messages.values()].sort((a, b) => (tsIsAfter(a.ts, b.ts) ? 1 : tsIsAfter(b.ts, a.ts) ? -1 : 0));
	const stale = staleBackfillIds(ordered, options.botUserId, options.nowMs);
	for (const message of ordered) {
		let result: RecoveryDelivery;
		try {
			result = message.user === options.botUserId ? "skip" : await options.deliver(message, stale.has(message.ts));
		} catch {
			return outcome(true);
		}
		if (result === "unavailable") return outcome(true);
		if (result === "acked") delivered++;
		else if (result === "duplicate") duplicates++;
		else if (result === "discard") discarded++;
		else skipped++;
		advancedTo = message.ts;
	}
	return outcome(false);
}

/** A single retry loop prevents reconnect storms from creating parallel recovery passes. */
export class RecoveryScheduler {
	private running = false;
	private followUp = false;
	private stopped = false;
	private waiting = false;
	private backoff = RECOVERY_RETRY_BASE_MS;
	private timer?: ReturnType<typeof setTimeout>;
	private wake?: () => void;

	constructor(
		private readonly run: () => Promise<boolean>,
		private readonly sleep?: (ms: number) => Promise<void>,
		private readonly random: () => number = Math.random,
	) {}

	get retryPending(): boolean {
		return this.waiting;
	}

	/** Test seam: resolves once no pass is running and no follow-up is queued. */
	async idle(): Promise<void> {
		while (this.running) await new Promise((resolve) => setTimeout(resolve, 1));
	}

	trigger(): void {
		if (this.stopped) return;
		if (this.running) {
			this.followUp = true;
			this.wake?.();
			return;
		}
		this.running = true;
		void this.loop();
	}

	stop(): void {
		this.stopped = true;
		this.followUp = false;
		this.wake?.();
	}

	private async loop(): Promise<void> {
		try {
			while (!this.stopped) {
				this.followUp = false;
				let completed = false;
				try {
					completed = await this.run();
				} catch {
					// A failed pass is incomplete, not a reason to abandon future recovery.
				}
				if (this.stopped) return;
				if (completed) this.backoff = RECOVERY_RETRY_BASE_MS;
				if (this.followUp) continue;
				if (completed) return;
				const delay = Math.min(RECOVERY_RETRY_MAX_MS, this.backoff + Math.floor(this.random() * this.backoff * 0.2));
				this.backoff = Math.min(RECOVERY_RETRY_MAX_MS, this.backoff * 2);
				this.waiting = true;
				try {
					await new Promise<void>((resolve) => {
						this.wake = resolve;
						if (this.sleep) void this.sleep(delay).then(resolve, resolve);
						else this.timer = setTimeout(resolve, delay);
					});
				} finally {
					if (this.timer !== undefined) clearTimeout(this.timer);
					this.timer = undefined;
					this.wake = undefined;
					this.waiting = false;
				}
			}
		} finally {
			this.running = false;
		}
	}
}
