import type { SlackWebApi } from "./api";
import { parseSlackMessageId, slackMessageId } from "./origin";

/** Bot-rooted threads whose root text is remembered for the reply header. */
export const ROOT_TEXT_CACHE_LIMIT = 500;
/**
 * Cold-root lookups in flight at once. They share `conversations.replies` quota with
 * recovery, so a burst across many threads is shed instead of queued: the next reply
 * in a shed thread asks again.
 */
export const ROOT_TEXT_MAX_IN_FLIGHT = 4;
/**
 * Cold-root lookups started per window. `conversations.replies` is Tier 3 (about 50 a
 * minute) and recovery walks threads with it, so lookups keep well under half of it;
 * one past the budget is shed and the next engaged reply under that root asks again.
 */
export const ROOT_TEXT_LOOKUPS_PER_WINDOW = 20;
export const ROOT_TEXT_WINDOW_MS = 60_000;
/** Kept well under the protocol's "short single-line" excerpt; the header clips further. */
export const ROOT_TEXT_LIMIT = 300;

/**
 * Text of thread roots the bot posted, for `replyTo.excerpt` on replies in their threads.
 *
 * A thread rooted at the bot's own message has its own session, which never saw the root
 * (the bot usually posted it from another session). Quoting the root in the reply header
 * tells that session what the thread is about. The protocol forbids delaying inbound
 * handling to fetch an excerpt, so `excerpt` only answers from memory: roots this adapter
 * posted (plain, live, or panel posts) are remembered as they post. An unknown root is
 * fetched in the background by `warm`, which the adapter calls only once the gateway
 * engaged a reply under it: a message the gateway refuses never spends Slack quota.
 */
export class SlackRootTexts {
	readonly #texts = new Map<string, string>();
	readonly #inFlight = new Map<string, Promise<void>>();
	/** Start times of lookups inside the current window, oldest first. */
	readonly #started: number[] = [];

	constructor(
		readonly api: Pick<SlackWebApi, "conversationsReplies">,
		readonly normalize: (text: string) => string = (text) => text,
		readonly now: () => number = Date.now,
	) {}

	/** Records a root the bot just posted at the top level of a channel. */
	remember(channel: string, ts: string, text: string): void {
		this.#store(slackMessageId(channel, ts), text);
	}

	/** The remembered root text, or undefined while it is unknown. Never fetches. */
	excerpt(rootId: string): string | undefined {
		return this.#texts.get(rootId) || undefined;
	}

	/** Starts a background lookup of an unknown root, for later replies under it. */
	warm(rootId: string): void {
		if (!this.#texts.has(rootId)) this.#fetch(rootId);
	}

	/** Resolves once background fetches settle. */
	async idle(): Promise<void> {
		await Promise.all(this.#inFlight.values());
	}

	#fetch(rootId: string): void {
		if (this.#inFlight.has(rootId) || this.#inFlight.size >= ROOT_TEXT_MAX_IN_FLIGHT) return;
		const now = this.now();
		while (this.#started.length > 0 && now - (this.#started[0] ?? now) >= ROOT_TEXT_WINDOW_MS) this.#started.shift();
		if (this.#started.length >= ROOT_TEXT_LOOKUPS_PER_WINDOW) return;
		this.#started.push(now);
		const fetched = this.#load(rootId).finally(() => this.#inFlight.delete(rootId));
		this.#inFlight.set(rootId, fetched);
	}

	async #load(rootId: string): Promise<void> {
		const root = parseSlackMessageId(rootId);
		if (!root) return;
		try {
			const page = await this.api.conversationsReplies(root.channel, root.ts, { limit: 1 });
			const text = page.messages.find((message) => message.ts === root.ts)?.text;
			this.#store(rootId, typeof text === "string" ? this.normalize(text) : "");
		} catch {
			// Best effort: the next reply in this thread retries.
		}
	}

	#store(rootId: string, text: string): void {
		const flattened = [...text.replace(/\s+/g, " ").trim()];
		const excerpt =
			flattened.length > ROOT_TEXT_LIMIT ? flattened.slice(0, ROOT_TEXT_LIMIT).join("") : flattened.join("");
		this.#texts.delete(rootId);
		if (this.#texts.size >= ROOT_TEXT_CACHE_LIMIT) {
			const oldest = this.#texts.keys().next().value;
			if (oldest !== undefined) this.#texts.delete(oldest);
		}
		this.#texts.set(rootId, excerpt);
	}
}
