import {
	type ChatProgressPayload,
	type OriginRef,
	PRESENCE_ALL_MARKERS,
	PRESENCE_PHASE_MARKERS,
	type PresenceMarker,
	type PresenceState,
	presenceInitial,
	presenceMarkersFor,
	presenceTransition,
} from "@gajaeway/protocol";
import type { SlackWebApi } from "./api";
import { parseSlackMessageId } from "./origin";

export const WORKING_STATUS_STALE_MS = 90_000;

type Timer = { unref?(): void };
type Entry = {
	readonly channel: string;
	readonly ts: string;
	state: PresenceState;
	/** Markers we believe are on the message right now. */
	readonly shown: Set<string>;
	busy: boolean;
};

/**
 * Presence as a reaction gradient on the triggering message.
 *
 * Instead of posting and editing a "working…" message, the adapter reacts to
 * the message it is answering: a phase marker (⏳ queued, 🔧 tool, 💭 thinking,
 * ✍️ writing), a clock face that advances every minute, and an effort digit
 * for tool calls / tokens. Markers are swapped only when their bucket changes
 * and at most once per coalescing window, and every marker is removed when the
 * reply lands (or the turn goes stale). No chat.postMessage, no chat.update.
 */
export class WorkingStatus {
	readonly #entries = new Map<string, Entry>();
	readonly #staleTimers = new Map<string, Timer>();

	constructor(
		readonly api: Pick<SlackWebApi, "addReaction" | "removeReaction">,
		readonly log: Pick<Console, "error"> = console,
		readonly setTimer: (fn: () => void, ms: number) => Timer = setTimeout,
		readonly clearTimer: (timer: unknown) => void = (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
		readonly now: () => number = Date.now,
	) {}

	/**
	 * An addressed turn was accepted for `messageId` (channel:ts). The queued
	 * marker goes on immediately: it is the room's only sign the message was
	 * seen until the first progress tick. Best-effort, never awaited by callers.
	 */
	arm(origin: OriginRef, messageId: string): void {
		if (origin.platform !== "slack") return;
		const target = parseSlackMessageId(messageId);
		if (!target) return;
		const key = origin.conversationId;
		const prior = this.#entries.get(key);
		// A newer turn in the same conversation takes over the gradient; clean the old one.
		if (prior && (prior.channel !== target.channel || prior.ts !== target.ts)) void this.clear(key);
		const entry: Entry = {
			channel: target.channel,
			ts: target.ts,
			state: presenceInitial(this.now()),
			shown: new Set(),
			busy: false,
		};
		this.#entries.set(key, entry);
		this.#armStale(key);
		void this.#apply(key, entry, [], presenceMarkersFor(entry.state.snapshot));
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "slack") return;
		const key = progress.origin.conversationId;
		const entry = this.#entries.get(key);
		if (!entry) return;
		this.#armStale(key);
		const swap = presenceTransition(entry.state, progress, this.now());
		if (!swap) return;
		entry.state = swap.state;
		await this.#apply(key, entry, swap.remove, swap.add);
	}

	async clear(conversationId: string): Promise<void> {
		const timer = this.#staleTimers.get(conversationId);
		if (timer) this.clearTimer(timer);
		this.#staleTimers.delete(conversationId);
		const entry = this.#entries.get(conversationId);
		if (!entry) return;
		this.#entries.delete(conversationId);
		// Remove what we know we added; a marker already gone is not an error.
		for (const name of entry.shown) {
			await this.api.removeReaction(entry.channel, entry.ts, name).catch(() => {});
		}
		entry.shown.clear();
	}

	#armStale(key: string): void {
		const prior = this.#staleTimers.get(key);
		if (prior) this.clearTimer(prior);
		const timer = this.setTimer(() => {
			this.#staleTimers.delete(key);
			void this.clear(key);
		}, WORKING_STATUS_STALE_MS);
		timer.unref?.();
		this.#staleTimers.set(key, timer);
	}

	async #apply(
		key: string,
		entry: Entry,
		remove: readonly PresenceMarker[],
		add: readonly PresenceMarker[],
	): Promise<void> {
		// Serialize swaps per message so a slow add cannot interleave with a clear.
		if (entry.busy) return;
		entry.busy = true;
		try {
			for (const marker of remove) {
				if (this.#entries.get(key) !== entry) return;
				await this.api.removeReaction(entry.channel, entry.ts, marker.slackName);
				entry.shown.delete(marker.slackName);
			}
			for (const marker of add) {
				if (this.#entries.get(key) !== entry) return;
				await this.api.addReaction(entry.channel, entry.ts, marker.slackName, "cosmetic");
				entry.shown.add(marker.slackName);
			}
		} catch (error) {
			this.log.error(`Slack presence failed for ${key}: ${error instanceof Error ? error.message : String(error)}`);
		} finally {
			entry.busy = false;
			// Cleared while a swap was in flight: whatever landed must come off.
			if (this.#entries.get(key) !== entry && entry.shown.size > 0) {
				for (const name of [...entry.shown]) {
					await this.api.removeReaction(entry.channel, entry.ts, name).catch(() => {});
					entry.shown.delete(name);
				}
			}
		}
	}
}

/** True for a reaction name the adapter itself puts on messages as presence. */
export function isPresenceReaction(slackName: string): boolean {
	return PRESENCE_ALL_MARKERS.some((marker) => marker.slackName === slackName);
}

/** Phase marker names, for tests and docs. */
export const PRESENCE_PHASE_NAMES = Object.fromEntries(
	Object.entries(PRESENCE_PHASE_MARKERS).map(([phase, marker]) => [phase, marker.slackName]),
) as Record<keyof typeof PRESENCE_PHASE_MARKERS, string>;
