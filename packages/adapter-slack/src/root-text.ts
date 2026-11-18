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
/** Kept well under the protocol's "short single-line" excerpt; the header clips further. */
export const ROOT_TEXT_LIMIT = 300;

/**
 * Text of thread roots the bot posted, for `replyTo.excerpt` on replies in their threads.
 *
 * A thread rooted at the bot's own message has its own session, which never saw the root
 * (the bot usually posted it from another session). Quoting the root in the reply header
 * tells that session what the thread is about. The protocol forbids delaying inbound
 * handling to fetch an excerpt, so `excerpt` only answers from memory: roots this adapter
 * posted (plain, live, or panel posts) are remembered as they post, and an unknown root is
 * fetched in the background so later replies in that thread carry it.
 */
export class SlackRootTexts {
	readonly #texts = new Map<string, string>();
	readonly #inFlight = new Map<string, Promise<void>>();

	constructor(
		readonly api: Pick<SlackWebApi, "conversationsReplies">,
		readonly normalize: (text: string) => string = (text) => text,
	) {}

	/** Records a root the bot just posted at the top level of a channel. */
	remember(channel: string, ts: string, text: string): void {
		this.#store(slackMessageId(channel, ts), text);
	}

	/** The remembered root text, or undefined while it is unknown (a fetch is then started). */
	excerpt(rootId: string): string | undefined {
		const known = this.#texts.get(rootId);
		if (known !== undefined) return known || undefined;
		this.#fetch(rootId);
		return undefined;
	}

	/** Resolves once background fetches settle. */
	async idle(): Promise<void> {
		await Promise.all(this.#inFlight.values());
	}

	#fetch(rootId: string): void {
		if (this.#inFlight.has(rootId) || this.#inFlight.size >= ROOT_TEXT_MAX_IN_FLIGHT) return;
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
