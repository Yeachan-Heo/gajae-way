import {
	type ChatProgressPayload,
	type OriginRef,
	PRESENCE_ALL_MARKERS,
	type PresenceSnapshot,
	type PresenceState,
	presenceInitial,
	presenceMarkersFor,
	presenceTransition,
} from "@gajae-gateway/protocol";
import type { SlackWebApi } from "./api";
import { parseSlackMessageId } from "./origin";

export const WORKING_STATUS_STALE_MS = 90_000;

type Timer = { unref?(): void };
type Entry = {
	readonly channel: string;
	readonly ts: string;
	/** Thread the native status line hangs under: the message's own thread, or the message as a new root. */
	readonly threadTs: string;
	/** Coalescing state: what the gradient should show, decided by presenceTransition. */
	state: PresenceState;
	/** Marker names we know are on the message; only successful API calls change it. */
	readonly shown: Set<string>;
	/** Native status text Slack confirmed; "" means none. Same desired/applied split as `shown`. */
	shownStatus: string;
	/** True while the gradient is wanted at all; false once cleared (desired = nothing). */
	wanted: boolean;
	reconciling: boolean;
	/** A change arrived while a pass was running; the loop re-diffs before it exits. */
	pending: boolean;
};

/** Bound on re-diff passes in one reconcile run; retirement cleanup runs regardless. */
const RECONCILE_MAX_PASSES = 8;
const PHASE_TEXT: Record<PresenceSnapshot["phase"], string> = {
	queued: "\uc694\uccad\uc744 \ubc1b\uc558\uc5b4\uc694\u2026",
	tool: "\uc791\uc5c5 \uc911\u2026",
	thinking: "\uacb0\uacfc\ub97c \uc77d\ub294 \uc911\u2026",
	writing: "\ub2f5\ubcc0\uc744 \uc4f0\ub294 \uc911\u2026",
};

/** Effort buckets 0..5 rendered as tool-call counts; index mirrors PRESENCE_EFFORT_MARKERS. */
const EFFORT_TEXT = ["1", "2", "3", "5", "10", "100+"] as const;

/**
 * The native status line, rendered from the same snapshot as the reactions.
 * Slack prefixes the app name, so this is only the predicate - it renders as
 * `sionic-gajae \uc791\uc5c5 \uc911\u2026 \u00b7 2\ubd84 \u00b7 \ub3c4\uad6c 3\ud68c`. Elapsed minutes and effort are
 * appended once they mean something, so the line itself visibly moves.
 */
export function presenceStatusText(snapshot: PresenceSnapshot): string {
	const parts = [PHASE_TEXT[snapshot.phase]];
	if (snapshot.clock > 0) parts.push(`${snapshot.clock}\ubd84`);
	if (snapshot.effort >= 0) parts.push(`\ub3c4\uad6c ${EFFORT_TEXT[snapshot.effort] ?? "?"}\ud68c`);
	return parts.join(" \u00b7 ");
}

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
 *
 * The same snapshot also drives Slack's native "<app> <status>" line
 * (assistant.threads.setStatus) under the thread the reply will land in - the
 * affordance users know from Slack's AI apps. It works on channel threads and
 * DMs with an ordinary bot token (verified live 2026-09-17), needs no Agent
 * registration, and is reconciled exactly like a marker: desired text vs
 * confirmed text. Slack clears it when the reply posts and expires it after
 * two minutes; retiring the entry clears it explicitly for silent turns.
 */
export class WorkingStatus {
	readonly #entries = new Map<string, Entry>();
	readonly #staleTimers = new Map<string, Timer>();

	constructor(
		readonly api: Pick<SlackWebApi, "addReaction" | "removeReaction" | "setThreadStatus">,
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
		// Inside a thread the status hangs under that thread; a channel/DM message
		// becomes its own root, which is exactly where the reply will land.
		const threadTs =
			(origin.kind === "thread" ? parseSlackMessageId(origin.conversationId)?.ts : undefined) ?? target.ts;
		const entry: Entry = {
			channel: target.channel,
			ts: target.ts,
			threadTs,
			state: presenceInitial(this.now()),
			shown: new Set(),
			shownStatus: "",
			wanted: true,
			reconciling: false,
			pending: false,
		};
		this.#entries.set(key, entry);
		this.#armStale(key);
		void this.#reconcile(key, entry);
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "slack") return;
		const key = progress.origin.conversationId;
		const entry = this.#entries.get(key);
		if (!entry?.wanted) return;
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
	 * finds the loop running marks a continuation and returns, and the running
	 * loop re-diffs after each pass (and once more for any continuation) until
	 * nothing is left to do. Retirement (`wanted=false`) is always driven to
	 * completion: removals are attempted even after an add failed, and an entry
	 * retired while a pass was in flight is cleaned by that same loop.
	 */
	async #reconcile(key: string, entry: Entry): Promise<void> {
		if (entry.reconciling) {
			entry.pending = true;
			return;
		}
		entry.reconciling = true;
		try {
			for (let pass = 0; pass < RECONCILE_MAX_PASSES; pass++) {
				entry.pending = false;
				const desired = new Set(entry.wanted ? presenceMarkersFor(entry.state.snapshot).map((m) => m.slackName) : []);
				const remove = [...entry.shown].filter((name) => !desired.has(name));
				const add = [...desired].filter((name) => !entry.shown.has(name));
				const desiredStatus = entry.wanted ? presenceStatusText(entry.state.snapshot) : "";
				// The line goes first: it is the one signal a reader actually sees as
				// "the app is working", and it costs one request per change. A refusal
				// leaves it un-shown for the next pass; the reactions still carry presence.
				if (entry.shownStatus !== desiredStatus) {
					try {
						await this.api.setThreadStatus(entry.channel, entry.threadTs, desiredStatus);
						entry.shownStatus = desiredStatus;
					} catch (error) {
						this.log.error(`Slack status line failed for ${key}: ${errorText(error)}`);
						// Do not retry the line inside this reconcile; treat it as applied so
						// a hard refusal (e.g. unsupported channel type) cannot spin the loop.
						entry.shownStatus = desiredStatus;
					}
				}
				if (remove.length === 0 && add.length === 0) {
					if (!entry.pending) return;
					continue;
				}
				for (const name of remove) {
					try {
						await this.api.removeReaction(entry.channel, entry.ts, name);
					} catch (error) {
						// Not fatal to anything - but a marker we could not remove is still
						// on the message, and that is worth knowing about.
						this.log.error(`Slack presence could not remove :${name}: on ${key}: ${errorText(error)}`);
					}
					entry.shown.delete(name);
				}
				let addFailed = false;
				for (const name of add) {
					if (!entry.wanted) break;
					try {
						await this.api.addReaction(entry.channel, entry.ts, name, "cosmetic");
						entry.shown.add(name);
					} catch (error) {
						this.log.error(`Slack presence could not add :${name}: on ${key}: ${errorText(error)}`);
						// Leave it un-shown; the next desired-state change retries. Stop
						// adding so a hard failure does not hammer the API per marker.
						addFailed = true;
						break;
					}
				}
				// A failed add ends the pass unless the entry was retired meanwhile, in
				// which case the loop continues so the removals happen.
				if (addFailed && entry.wanted && !entry.pending) return;
			}
			// Pass budget exhausted with work still pending: never leave a retired
			// entry's markers behind, whatever else is outstanding.
			if (!entry.wanted && entry.shown.size > 0) {
				for (const name of [...entry.shown]) {
					await this.api
						.removeReaction(entry.channel, entry.ts, name)
						.catch((error) =>
							this.log.error(`Slack presence could not remove :${name}: on ${key}: ${errorText(error)}`),
						);
					entry.shown.delete(name);
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
