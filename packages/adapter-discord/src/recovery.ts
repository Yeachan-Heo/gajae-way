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
 * module's per-conversation last-acked cursor keeps the refetch window tight. The cursor
 * advances only after the gateway acknowledges a send (or reports a known duplicate); an
 * unavailable gateway leaves the message unacked so the next reconnect retries it.
 */

export interface RecoveryCursorState {
	readonly conversations: Readonly<Record<string, string>>;
}

export type RecoveryGateResult = "acked" | "duplicate" | "unavailable";

const EMPTY_STATE: RecoveryCursorState = { conversations: {} };

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
	const conversations: Record<string, string> = {};
	const entries = (raw as { conversations?: unknown }).conversations;
	if (typeof entries === "object" && entries !== null && !Array.isArray(entries)) {
		for (const [id, value] of Object.entries(entries)) {
			if (typeof value === "string" && /^\d+$/.test(value)) conversations[id] = value;
		}
	}
	return { conversations };
}

/** Atomic write (tmp + rename) so a crash mid-write never truncates the cursor file. */
export async function saveRecoveryCursors(path: string, state: RecoveryCursorState): Promise<void> {
	const payload = `${JSON.stringify({ conversations: state.conversations }, null, "\t")}\n`;
	await mkdir(dirname(path), { recursive: true, mode: 0o700 });
	const tmp = `${path}.${process.pid}.tmp`;
	await writeFile(tmp, payload, { encoding: "utf8", mode: 0o600 });
	await rename(tmp, path);
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

/** Per-message delivery verdict handed back by the caller's send path. */
export type RecoveryDelivery = "acked" | "duplicate" | "unavailable";

export interface RecoveryOutcome {
	/** Highest message id known acked (or already duplicated) after this run. */
	readonly advancedTo: string;
	readonly delivered: number;
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
			return { advancedTo: cursor, delivered, truncated: true, failed: false };
		}
		batch.sort((a, b) => {
			const ai = BigInt(a.id);
			const bi = BigInt(b.id);
			return ai < bi ? -1 : ai > bi ? 1 : 0;
		});
		if (batch.length === 0) return { advancedTo: cursor, delivered, truncated: false, failed: false };
		for (const message of batch) {
			if (skipEmpty && message.content.trim() === "") {
				// Thread starters / system entries carry no text: skip without a send so the
				// cursor still advances and later messages keep backfilling.
				cursor = message.id;
				continue;
			}
			const verdict = await options.deliver(message);
			if (verdict === "unavailable") {
				return { advancedTo: cursor, delivered, truncated: false, failed: true };
			}
			cursor = message.id;
			delivered++;
		}
		if (batch.length < pageLimit) return { advancedTo: cursor, delivered, truncated: false, failed: false };
	}
	return { advancedTo: cursor, delivered, truncated: true, failed: false };
}
