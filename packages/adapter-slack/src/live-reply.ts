import { SlackApiError, type SlackWebApi } from "./api";
import { chunkSlackMessage, SLACK_MESSAGE_LIMIT } from "./mrkdwn";

/** Slack refusals that mean the live message can no longer be edited. */
const UNEDITABLE_CODES = new Set(["message_not_found", "cant_update_message"]);

/** Backstop only: entries close on the terminal delivery or the final progress tick. */
export const LIVE_REPLY_TTL_MS = 30 * 60_000;
/** Bound on tracked turns; the least recently touched is evicted, like the edit outbox. */
export const LIVE_REPLY_MAX_ENTRIES = 64;

/** One ledger delivery, in first-seen order, with its applied state. */
type Part = {
	readonly deliveryId: string;
	readonly text: string;
	applied: boolean;
};

type Entry = {
	readonly channel: string;
	/** Empty until the opening post landed; a failed open removes the entry. */
	ts: string;
	readonly parts: Part[];
	/** False once this entry will never be edited again (turn over, overflow, refusal). */
	open: boolean;
	lastTouched: number;
	/** Serializes the turn's platform writes; keeps running after a failed part. */
	chain: Promise<unknown>;
};

export type LiveDeliveryResult =
	| { readonly kind: "posted" }
	| { readonly kind: "folded" }
	| { readonly kind: "replayed" }
	| { readonly kind: "untracked" };

export interface LiveDeliveryInput {
	readonly turnId: string;
	readonly deliveryId: string;
	readonly channel: string;
	readonly threadTs?: string;
	/** The part's delivery text, already converted to mrkdwn. */
	readonly text: string;
	/** True on the turn's terminal delivery; the entry stops folding afterwards. */
	readonly final: boolean;
}

/**
 * Live replies (opt-in): one turn's streaming parts, folded into one message.
 *
 * Without this, every part of a turn — mid-turn speech and the terminal reply —
 * posts as its own message, so a working persona scatters a burst of separate
 * messages across the thread. With `liveReplies` enabled, the adapter folds
 * them: the first mid-turn part posts the live message, every later part is
 * edited into it (chat.update), and the terminal reply lands as the final edit.
 * A single-part turn (most turns) never opens an entry and posts exactly as
 * before.
 *
 * Ledger semantics are untouched: each part is still its own delivery row, and
 * the caller confirms or fails it from this tracker's outcome. The edit is a
 * full-text render of every applied part, so it is idempotent under the
 * ledger's at-least-once replay — a redelivered part that already applied
 * re-confirms without a platform call, and one that did not renders in its
 * stored position rather than appending out of order. This is why a replayed
 * part carries no `[recovered - may be a duplicate]` prefix on the fold path:
 * the prefix exists because a re-POST can duplicate; a full-text edit cannot.
 * The fallback post path (entry sealed or lost) keeps today's prefix.
 *
 * Folding degrades, never loses content: a render that would exceed Slack's
 * message limit, a Slack refusal to edit (message deleted, permissions
 * changed), or a turn that already ended seals the entry, and the rest of the
 * turn posts as ordinary messages.
 */
export class LiveReplyTracker {
	readonly #entries = new Map<string, Entry>();

	constructor(
		readonly api: Pick<SlackWebApi, "postMessage" | "updateMessage">,
		readonly log: Pick<Console, "error"> = console,
		readonly now: () => number = Date.now,
	) {}

	/**
	 * Offers one text part of a turn. Must not be called for reactions. A part
	 * that is folded, posted, or already applied resolves with the outcome the
	 * caller should settle; `untracked` means the caller posts the part itself.
	 */
	deliver(input: LiveDeliveryInput): Promise<LiveDeliveryResult> {
		this.#sweep();
		const entry = this.#entries.get(input.turnId);
		if (!entry) {
			// A terminal part with no live message is a single-part turn: there is
			// nothing to fold, and opening an entry for it would be pure overhead.
			if (input.final || input.text === "") return Promise.resolve({ kind: "untracked" });
			// Reserve synchronously so a concurrent part of the same new turn
			// queues behind this open instead of posting a second live message.
			const reserved: Entry = {
				channel: input.channel,
				ts: "",
				parts: [],
				open: true,
				lastTouched: this.now(),
				chain: Promise.resolve(),
			};
			this.#entries.set(input.turnId, reserved);
			const open = reserved.chain.then(() => this.#open(reserved, input));
			reserved.chain = open.catch(() => {});
			return open;
		}
		const run = entry.chain.then(() => this.#append(entry, input));
		entry.chain = run.catch(() => {});
		return run;
	}

	/**
	 * The turn settled — terminal delivery or final progress tick. No further
	 * edit is attempted, but the entry stays so a replayed part can still be
	 * answered (re-confirmed when applied, posted when not) until TTL eviction.
	 */
	close(turnId: string): void {
		const entry = this.#entries.get(turnId);
		if (entry) entry.open = false;
	}

	/** True while the turn has an open live message; tests and diagnostics. */
	tracked(turnId: string): boolean {
		const entry = this.#entries.get(turnId);
		return entry?.open === true && entry.ts !== "";
	}

	async #open(entry: Entry, input: LiveDeliveryInput): Promise<LiveDeliveryResult> {
		// Posting the first part is the same path as an ordinary delivery:
		// chunked, threaded, paced by the limiter. The live tail is the last
		// chunk; later parts edit that message.
		const chunks = chunkSlackMessage(input.text);
		let tail: { readonly ts: string; readonly text: string } | undefined;
		try {
			for (const chunk of chunks) {
				const sent = await this.api.postMessage(input.channel, chunk, input.threadTs);
				tail = { ts: sent.ts, text: chunk };
			}
		} catch (error) {
			// The reservation must not pin the turn to untracked mode: dropping
			// it lets the ledger's retry of this part open a fresh live message.
			this.#abandon(input.turnId, entry);
			throw error;
		}
		if (!tail) {
			this.#abandon(input.turnId, entry);
			return { kind: "untracked" };
		}
		entry.ts = tail.ts;
		entry.parts.push({ deliveryId: input.deliveryId, text: tail.text, applied: true });
		this.#touch(input.turnId, entry);
		this.#sweep();
		return { kind: "posted" };
	}

	/** A failed opening post removes the reservation so the next part starts clean. */
	#abandon(turnId: string, entry: Entry): void {
		if (this.#entries.get(turnId) === entry) this.#entries.delete(turnId);
	}

	async #append(entry: Entry, input: LiveDeliveryInput): Promise<LiveDeliveryResult> {
		// The opening post failed and dropped the reservation, or a newer
		// reservation replaced this orphan: this part posts outside the tracker.
		if (this.#entries.get(input.turnId) !== entry || entry.ts === "") return { kind: "untracked" };
		this.#touch(input.turnId, entry);
		// Applied first, sealed second: a replay of an already-visible part
		// re-confirms even after the turn ended (its confirm may have been lost).
		const prior = entry.parts.find((part) => part.deliveryId === input.deliveryId);
		if (prior?.applied) return { kind: "replayed" };
		if (!entry.open) return { kind: "untracked" };
		const part: Part = prior ?? { deliveryId: input.deliveryId, text: input.text, applied: false };
		if (prior === undefined) entry.parts.push(part);
		// A part that never applied renders in its stored position, so a ledger
		// retry of a middle part lands between its neighbours, not after them.
		const rendered = entry.parts
			.filter((candidate) => candidate.applied || candidate === part)
			.map((candidate) => candidate.text)
			.join("\n\n");
		if (rendered.length > SLACK_MESSAGE_LIMIT) {
			entry.open = false;
			return { kind: "untracked" };
		}
		try {
			await this.api.updateMessage(entry.channel, entry.ts, rendered);
		} catch (error) {
			if (error instanceof SlackApiError && UNEDITABLE_CODES.has(error.code)) {
				entry.open = false;
				this.log.error(
					`Slack live reply ${entry.channel}:${entry.ts} is no longer editable (${error.code}); folding stopped for turn ${input.turnId}.`,
				);
				return { kind: "untracked" };
			}
			throw error;
		}
		part.applied = true;
		if (input.final) entry.open = false;
		return { kind: "folded" };
	}

	#touch(turnId: string, entry: Entry): void {
		entry.lastTouched = this.now();
		// Re-insert so Map order is least-recently-touched first for eviction. An
		// entry evicted mid-flight (a capacity sweep beat the turn's write) stays
		// gone rather than resurrecting past the cap.
		if (!this.#entries.has(turnId)) return;
		this.#entries.delete(turnId);
		this.#entries.set(turnId, entry);
	}

	#sweep(): void {
		const now = this.now();
		for (const [turnId, entry] of this.#entries)
			if (now - entry.lastTouched > LIVE_REPLY_TTL_MS) this.#entries.delete(turnId);
		while (this.#entries.size > LIVE_REPLY_MAX_ENTRIES) {
			const oldest = this.#entries.keys().next().value as string | undefined;
			if (oldest === undefined) break;
			this.#entries.delete(oldest);
			this.log.error(`Slack live reply table full; dropped the oldest turn (${oldest}).`);
		}
	}
}
