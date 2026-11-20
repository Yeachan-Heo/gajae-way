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
 * The logical origin of each file share the adapter uploads, until Slack's own message event for it arrives.
 *
 * `files.completeUploadExternal` returns no message ts, so the share's message is known only from the
 * bot's own message event. Where that message sits physically (its `thread_ts`) does not say which
 * session posted it: a channel-origin turn may reply inside a thread. So the upload records the
 * delivery's own origin under the place and file name it posts to, and the event claims it. An own
 * file share with no recorded upload stays unknown and keeps the channel fallback.
 */
export class SlackFileShares {
	readonly #pending = new Map<string, PendingShare[]>();
	#size = 0;

	constructor(readonly now: () => number = Date.now) {}

	/** Records an upload about to post; the returned function withdraws it when the upload fails. */
	expect(origin: OriginRef, channel: string, threadTs: string | undefined, name: string): () => void {
		this.#prune();
		if (this.#size >= FILE_SHARE_PENDING_LIMIT) this.#dropOldest();
		const key = shareKey(channel, threadTs, name);
		const entry: PendingShare = { origin, at: this.now() };
		const queue = this.#pending.get(key) ?? [];
		queue.push(entry);
		this.#pending.set(key, queue);
		this.#size++;
		return () => this.#remove(key, entry);
	}

	/** The origin of the oldest live upload matching this own file-share message, consumed. */
	claim(channel: string, threadTs: string | undefined, name: string): OriginRef | undefined {
		this.#prune();
		const key = shareKey(channel, threadTs, name);
		const entry = this.#pending.get(key)?.[0];
		if (!entry) return undefined;
		this.#remove(key, entry);
		return entry.origin;
	}

	#remove(key: string, entry: PendingShare): void {
		const queue = this.#pending.get(key);
		const index = queue?.indexOf(entry) ?? -1;
		if (!queue || index < 0) return;
		queue.splice(index, 1);
		this.#size--;
		if (queue.length === 0) this.#pending.delete(key);
	}

	#prune(): void {
		const cutoff = this.now() - FILE_SHARE_PENDING_TTL_MS;
		for (const [key, queue] of this.#pending)
			for (const entry of [...queue]) if (entry.at <= cutoff) this.#remove(key, entry);
	}

	#dropOldest(): void {
		let oldest: { key: string; entry: PendingShare } | undefined;
		for (const [key, queue] of this.#pending) {
			const entry = queue[0];
			if (entry && (!oldest || entry.at < oldest.entry.at)) oldest = { key, entry };
		}
		if (oldest) this.#remove(oldest.key, oldest.entry);
	}
}

function shareKey(channel: string, threadTs: string | undefined, name: string): string {
	return JSON.stringify([channel, threadTs ?? "", name]);
}
