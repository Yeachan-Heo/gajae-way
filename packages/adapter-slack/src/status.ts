import {
	type ChatProgressPayload,
	type OriginRef,
	PRESENCE_ALL_MARKERS,
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
	/** Coalescing state: what the gradient should show, decided by presenceTransition. */
	state: PresenceState;
	/** Marker names we know are on the message; only successful API calls change it. */
	readonly shown: Set<string>;
	/** True while the gradient is wanted at all; false once cleared (desired = nothing). */
	wanted: boolean;
	reconciling: boolean;
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
 *
 * Desired state and applied state are kept apart: `state` is what should be
 * visible, `shown` is what the API confirmed. A single reconcile loop per
 * message diffs the two and issues the adds/removes; anything that changes the
 * desired set while a reconcile is in flight is picked up by the loop's next
 * pass, so a slow Slack call can delay a swap but never lose it, and a failed
 * call leaves the marker un-shown so it is retried rather than believed.
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
		if (prior && prior.channel === target.channel && prior.ts === target.ts) {
			// Same message re-armed (an accepted edit): keep the ownership record
			// - the markers already on the message are still ours - and just
			// restart the gradient from queued.
			prior.wanted = true;
			prior.state = presenceInitial(this.now());
			this.#armStale(key);
			void this.#reconcile(key, prior);
			return;
		}
		// A different message in the same conversation takes over; the old
		// gradient is retired through its own reconcile (desired = nothing).
		if (prior) this.#retire(key, prior);
		const entry: Entry = {
			channel: target.channel,
			ts: target.ts,
			state: presenceInitial(this.now()),
			shown: new Set(),
			wanted: true,
			reconciling: false,
		};
		this.#entries.set(key, entry);
		this.#armStale(key);
		void this.#reconcile(key, entry);
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "slack") return;
		const key = progress.origin.conversationId;
		const entry = this.#entries.get(key);
		if (!entry || !entry.wanted) return;
		this.#armStale(key);
		const swap = presenceTransition(entry.state, progress, this.now());
		if (!swap) return;
		entry.state = swap.state;
		await this.#reconcile(key, entry);
	}

	async clear(conversationId: string): Promise<void> {
		const timer = this.#staleTimers.get(conversationId);
		if (timer) this.clearTimer(timer);
		this.#staleTimers.delete(conversationId);
		const entry = this.#entries.get(conversationId);
		if (!entry) return;
		this.#entries.delete(conversationId);
		await this.#retire(conversationId, entry);
	}

	/** Marks the gradient unwanted and drives the reconcile that takes every marker off. */
	async #retire(key: string, entry: Entry): Promise<void> {
		entry.wanted = false;
		await this.#reconcile(key, entry);
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

	/**
	 * Drives `shown` towards the desired set. One loop per entry; a call that
	 * finds the loop running returns immediately and the running loop re-diffs
	 * after each pass until nothing is left to do.
	 */
	async #reconcile(key: string, entry: Entry): Promise<void> {
		if (entry.reconciling) return;
		entry.reconciling = true;
		try {
			for (let pass = 0; pass < 8; pass++) {
				const desired = new Set(entry.wanted ? presenceMarkersFor(entry.state.snapshot).map((m) => m.slackName) : []);
				const remove = [...entry.shown].filter((name) => !desired.has(name));
				const add = [...desired].filter((name) => !entry.shown.has(name));
				if (remove.length === 0 && add.length === 0) return;
				for (const name of remove) {
					try {
						await this.api.removeReaction(entry.channel, entry.ts, name);
						entry.shown.delete(name);
					} catch (error) {
						// Not fatal to anything - but a marker we could not remove is still
						// on the message, and that is worth knowing about.
						this.log.error(`Slack presence could not remove :${name}: on ${key}: ${errorText(error)}`);
						entry.shown.delete(name);
					}
				}
				for (const name of add) {
					if (!entry.wanted) break;
					try {
						await this.api.addReaction(entry.channel, entry.ts, name, "cosmetic");
						entry.shown.add(name);
					} catch (error) {
						this.log.error(`Slack presence could not add :${name}: on ${key}: ${errorText(error)}`);
						// Leave it un-shown; a later pass may succeed. Stop this pass so a
						// hard failure does not hammer the API for every marker.
						return;
					}
				}
			}
		} finally {
			entry.reconciling = false;
		}
	}
}

function errorText(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/** True for a reaction name the adapter itself puts on messages as presence. */
export function isPresenceReaction(slackName: string): boolean {
	return PRESENCE_ALL_MARKERS.some((marker) => marker.slackName === slackName);
}
