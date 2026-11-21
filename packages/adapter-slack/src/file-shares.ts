import type { OriginRef } from "@gajae-gateway/protocol";

/** File shares awaiting their own Slack message event. */
export const FILE_SHARE_PENDING_LIMIT = 200;
/** A share whose event has not arrived by then is forgotten; its reactions take the channel fallback. */
export const FILE_SHARE_PENDING_TTL_MS = 10 * 60_000;

interface PendingShare {
	readonly origin: OriginRef;
	readonly at: number;
}

/**
 * The logical origin of each file the adapter uploads, until Slack's own message event for its share arrives.
 *
 * `files.completeUploadExternal` returns no message ts, so the share's message is known only from the
 * bot's own message event. Where that message sits physically (its `thread_ts`) does not say which
 * session posted it: a channel-origin turn may reply inside a thread. The upload records the delivery's
 * origin under the Slack file id it reserved, before completing, and the event claims it by
 * `files[].id`. Ids are unique, so concurrent uploads of one name into one thread cannot swap origins.
 * An own share with no recorded upload stays unknown and keeps the channel fallback.
 */
export class SlackFileShares {
	readonly #pending = new Map<string, PendingShare>();

	constructor(readonly now: () => number = Date.now) {}

	/** Records an upload's reserved file id; the returned function withdraws it when the upload fails. */
	expect(fileId: string, origin: OriginRef): () => void {
		this.#prune();
		if (this.#pending.size >= FILE_SHARE_PENDING_LIMIT) {
			const oldest = this.#pending.keys().next().value;
			if (oldest !== undefined) this.#pending.delete(oldest);
		}
		const entry: PendingShare = { origin, at: this.now() };
		this.#pending.delete(fileId);
		this.#pending.set(fileId, entry);
		return () => {
			if (this.#pending.get(fileId) === entry) this.#pending.delete(fileId);
		};
	}

	/** The origin of the live upload that reserved this file id, consumed. */
	claim(fileId: string): OriginRef | undefined {
		this.#prune();
		const entry = this.#pending.get(fileId);
		if (!entry) return undefined;
		this.#pending.delete(fileId);
		return entry.origin;
	}

	#prune(): void {
		const cutoff = this.now() - FILE_SHARE_PENDING_TTL_MS;
		for (const [fileId, entry] of this.#pending) {
			if (entry.at > cutoff) break;
			this.#pending.delete(fileId);
		}
	}
}
