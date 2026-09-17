import type { ChatProgressActivity, ChatProgressPayload } from "./catalog";

/**
 * Presence as a reaction gradient.
 *
 * A working turn used to be shown as a temporary "⏳ working… (2m, 3 tools)"
 * message that was edited on every tick. Two problems: every edit is a request
 * against the reply budget, and a message that keeps rewriting itself reads
 * like a bot, not like someone working. The room already has a native, cheap,
 * ambient signal for "I saw this and I am on it": reactions on the message
 * that triggered the turn. So presence is a small SET of marker reactions on
 * that message - one for the phase, one for how long it has been going, one
 * for how much it has done - each swapped only when its bucket changes, and
 * all removed when the reply lands.
 *
 * The markers are the adapter's own and deliberately disjoint from the
 * persona's `[REACT]` allowlist, so a human reading the room can tell "the
 * bot is working" from "the bot reacted", and the gateway's reaction caps and
 * dedupe never see them.
 *
 * Reading the gradient: `⏳ 🔧 🕐 🔟` = queued→running a tool, ~1 minute in,
 * about ten tool calls. The phase marker changes as the turn moves; the clock
 * face advances every minute (up to twelve); the effort digit steps through
 * tool-call / token buckets. Nothing here needs a message edit.
 */

export type PresencePhase = "queued" | "tool" | "thinking" | "writing";

export interface PresenceMarker {
	/** Unicode spelling, what Discord `message.react()` takes. */
	readonly unicode: string;
	/** Slack reaction name. */
	readonly slackName: string;
}

export const PRESENCE_PHASE_MARKERS: Readonly<Record<PresencePhase, PresenceMarker>> = {
	queued: { unicode: "⏳", slackName: "hourglass_flowing_sand" },
	tool: { unicode: "🔧", slackName: "wrench" },
	thinking: { unicode: "💭", slackName: "thought_balloon" },
	writing: { unicode: "✍️", slackName: "writing_hand" },
};

/** Clock faces, one per elapsed minute (index 0 = under a minute, no marker). */
export const PRESENCE_CLOCK_MARKERS: readonly PresenceMarker[] = [
	{ unicode: "🕐", slackName: "clock1" },
	{ unicode: "🕑", slackName: "clock2" },
	{ unicode: "🕒", slackName: "clock3" },
	{ unicode: "🕓", slackName: "clock4" },
	{ unicode: "🕔", slackName: "clock5" },
	{ unicode: "🕕", slackName: "clock6" },
	{ unicode: "🕖", slackName: "clock7" },
	{ unicode: "🕗", slackName: "clock8" },
	{ unicode: "🕘", slackName: "clock9" },
	{ unicode: "🕙", slackName: "clock10" },
	{ unicode: "🕚", slackName: "clock11" },
	{ unicode: "🕛", slackName: "clock12" },
];

/**
 * Effort buckets: tool calls so far, falling back to output tokens when the
 * runtime reports no tool counter (gjc's stdio path filters tool activity).
 * Keycap digits read as "how much" at a glance; the last bucket is open-ended.
 */
export const PRESENCE_EFFORT_MARKERS: readonly PresenceMarker[] = [
	{ unicode: "1️⃣", slackName: "one" },
	{ unicode: "2️⃣", slackName: "two" },
	{ unicode: "3️⃣", slackName: "three" },
	{ unicode: "5️⃣", slackName: "five" },
	{ unicode: "🔟", slackName: "keycap_ten" },
	{ unicode: "💯", slackName: "100" },
];

/** Every marker the adapter may own on a message, for cleanup and for inbound filtering. */
export const PRESENCE_ALL_MARKERS: readonly PresenceMarker[] = [
	...Object.values(PRESENCE_PHASE_MARKERS),
	...PRESENCE_CLOCK_MARKERS,
	...PRESENCE_EFFORT_MARKERS,
];

/** The phase a progress event puts the turn in; no activity yet means still queued. */
export function presencePhaseFor(activity: ChatProgressActivity | undefined): PresencePhase {
	return activity?.kind ?? "queued";
}

/** Elapsed-minute bucket: 0 below one minute, else the clock index (1..12), capped. */
export function presenceClockBucket(elapsedMs: number): number {
	return Math.min(PRESENCE_CLOCK_MARKERS.length, Math.floor(elapsedMs / 60_000));
}

/** Effort bucket index into PRESENCE_EFFORT_MARKERS, or -1 for "nothing yet". */
export function presenceEffortBucket(progress: Pick<ChatProgressPayload, "toolCalls" | "outputTokens">): number {
	const calls = progress.toolCalls > 0 ? progress.toolCalls : Math.floor(progress.outputTokens / 500);
	if (calls <= 0) return -1;
	if (calls < 2) return 0;
	if (calls < 3) return 1;
	if (calls < 5) return 2;
	if (calls < 10) return 3;
	if (calls < 25) return 4;
	return 5;
}

/** The full set of markers a progress event asks to be visible. */
export interface PresenceSnapshot {
	readonly phase: PresencePhase;
	readonly clock: number;
	readonly effort: number;
}

export function presenceSnapshot(
	progress: Pick<ChatProgressPayload, "activity" | "elapsedMs" | "toolCalls" | "outputTokens">,
): PresenceSnapshot {
	return {
		phase: presencePhaseFor(progress.activity),
		clock: presenceClockBucket(progress.elapsedMs),
		effort: presenceEffortBucket(progress),
	};
}

export function presenceMarkersFor(snapshot: PresenceSnapshot): readonly PresenceMarker[] {
	const clock = snapshot.clock > 0 ? PRESENCE_CLOCK_MARKERS[snapshot.clock - 1] : undefined;
	const effort = snapshot.effort >= 0 ? PRESENCE_EFFORT_MARKERS[snapshot.effort] : undefined;
	return [PRESENCE_PHASE_MARKERS[snapshot.phase], ...(clock ? [clock] : []), ...(effort ? [effort] : [])];
}

/** Minimum spacing between swaps on one message; each swap is up to two requests per marker. */
export const PRESENCE_MIN_SWAP_MS = 15_000;

export interface PresenceState {
	readonly snapshot: PresenceSnapshot;
	readonly swappedAt: number;
}

export interface PresenceSwap {
	readonly state: PresenceState;
	readonly remove: readonly PresenceMarker[];
	readonly add: readonly PresenceMarker[];
}

/**
 * Decides what changes on the message for a progress event. Returns `null`
 * when nothing should be sent: an identical snapshot, or a change inside the
 * coalescing window (the next tick will carry it). Otherwise the markers to
 * remove and to add, so a phase change costs one remove + one add and a clock
 * tick alone costs one of each - never a re-post of everything.
 */
export function presenceTransition(
	state: PresenceState,
	progress: Pick<ChatProgressPayload, "activity" | "elapsedMs" | "toolCalls" | "outputTokens">,
	now: number,
): PresenceSwap | null {
	const next = presenceSnapshot(progress);
	const before = presenceMarkersFor(state.snapshot);
	const after = presenceMarkersFor(next);
	const remove = before.filter((marker) => !after.includes(marker));
	const add = after.filter((marker) => !before.includes(marker));
	if (remove.length === 0 && add.length === 0) return null;
	if (now - state.swappedAt < PRESENCE_MIN_SWAP_MS) return null;
	return { state: { snapshot: next, swappedAt: now }, remove, add };
}

/** Initial state for a freshly accepted turn: the queued marker only. */
export function presenceInitial(now: number): PresenceState {
	return { snapshot: { phase: "queued", clock: 0, effort: -1 }, swappedAt: now };
}
