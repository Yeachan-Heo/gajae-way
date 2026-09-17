import type { ChatProgressPayload, OriginRef } from "@gajaeway/protocol";
import type { SlackWebApi } from "./api";
import { parseSlackMessageId } from "./origin";

export const WORKING_STATUS_STALE_MS = 90_000;
/**
 * Minimum spacing between edits of one status message. chat.update counts
 * against the same per-channel budget as replies (Tier 3, ~50/min), so a hint
 * that re-renders on every 10s progress tick across a few busy conversations
 * is exactly how a workspace ends up rate-limited. Elapsed time is shown at
 * minute granularity beyond the first minute for the same reason: a change
 * nobody can read is not worth a request.
 */
export const WORKING_STATUS_MIN_EDIT_MS = 15_000;

type Timer = { unref?(): void };
type StatusMessage = { readonly channel: string; readonly ts: string };
type Entry = { message?: StatusMessage; text?: string; editedAt?: number };

/** Show only reported activity: zero counters are noise, not proof the turn did nothing. */
export function workingStatusText(
	progress: Pick<ChatProgressPayload, "elapsedMs" | "toolCalls" | "outputTokens" | "activity">,
): string {
	const total = Math.floor(progress.elapsedMs / 1000);
	const minutes = Math.floor(total / 60);
	const seconds = total % 60;
	// Sub-minute turns count seconds (in 5s steps); longer ones read as minutes.
	const parts = [minutes > 0 ? `${minutes}m` : `${seconds - (seconds % 5)}s`];
	if (progress.toolCalls > 0) parts.push(`${progress.toolCalls} tool${progress.toolCalls === 1 ? "" : "s"}`);
	if (progress.outputTokens > 0)
		parts.push(
			progress.outputTokens >= 1000
				? `${(progress.outputTokens / 1000).toFixed(1)}k tok`
				: `${progress.outputTokens} tok`,
		);
	return `⏳ working… (${parts.join(", ")})${activitySuffix(progress.activity)}`;
}

/**
 * The "what" next to the "how long": `· bash — running the tests`. The label
 * and detail arrive bounded and single-line from the gateway; mrkdwn control
 * characters are escaped here because this text is posted as-is.
 */
export function activitySuffix(activity: ChatProgressPayload["activity"]): string {
	if (!activity) return "";
	const label = escapeMrkdwn(activity.label);
	if (activity.kind !== "tool") return ` · ${label}…`;
	return activity.detail ? ` · \`${label}\` — ${escapeMrkdwn(activity.detail)}` : ` · \`${label}\``;
}

function escapeMrkdwn(text: string): string {
	return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/`/g, "'");
}

/** One temporary, amended Slack message per addressed conversation; never compete with delivery. */
export class WorkingStatus {
	readonly #messages = new Map<string, Entry>();
	readonly #staleTimers = new Map<string, Timer>();
	// Overheard public turns usually end in silence: a spinner there is noise for everyone present.
	readonly #addressed = new Set<string>();

	constructor(
		readonly api: Pick<SlackWebApi, "postMessage" | "updateMessage" | "deleteMessage">,
		readonly log: Pick<Console, "error"> = console,
		readonly setTimer: (fn: () => void, ms: number) => Timer = setTimeout,
		readonly clearTimer: (timer: unknown) => void = (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
		readonly now: () => number = Date.now,
	) {}

	/**
	 * An addressed turn was accepted. Slack has no typing indicator, so the hint
	 * is posted right here rather than on the first progress tick: the gateway
	 * withholds progress for the first seconds of a turn, and a queued or stalled
	 * turn might never tick at all, which left the room with no sign anything
	 * was happening. Best-effort and never awaited by the caller.
	 */
	arm(origin: OriginRef): void {
		if (origin.platform !== "slack") return;
		this.#addressed.add(origin.conversationId);
		void this.#render(origin, "⏳ working…");
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "slack") return;
		if (!this.#addressed.has(progress.origin.conversationId)) return;
		await this.#render(progress.origin, workingStatusText(progress));
	}

	async #render(origin: OriginRef, text: string): Promise<void> {
		const { conversationId } = origin;
		// A wedged turn or dead gateway must not leave a status behind forever.
		const prior = this.#staleTimers.get(conversationId);
		if (prior) this.clearTimer(prior);
		const timer = this.setTimer(() => {
			this.#staleTimers.delete(conversationId);
			void this.clear(conversationId);
		}, WORKING_STATUS_STALE_MS);
		timer.unref?.();
		this.#staleTimers.set(conversationId, timer);
		const existing = this.#messages.get(conversationId);
		if (existing && !existing.message) return; // a post is in flight; the next tick edits
		const entry = existing ?? {};
		try {
			if (entry.message) {
				// Coalesce: an edit is a request against the reply budget, so only send
				// one when the rendered text actually changed and the last edit is old
				// enough. A skipped tick costs nothing - the next one carries the update.
				if (entry.text === text) return;
				if (entry.editedAt !== undefined && this.now() - entry.editedAt < WORKING_STATUS_MIN_EDIT_MS) return;
				entry.editedAt = this.now();
				entry.text = text;
				await this.api.updateMessage(entry.message.channel, entry.message.ts, text);
				return;
			}
			const thread = origin.kind === "thread" ? parseSlackMessageId(conversationId) : undefined;
			if (origin.kind === "thread" && !thread) throw new Error("Slack status thread has an invalid message id");
			this.#messages.set(conversationId, entry);
			const posted = await this.api.postMessage(thread?.channel ?? conversationId, text, thread?.ts, "cosmetic");
			if (this.#messages.get(conversationId) === entry) {
				entry.message = posted;
				entry.text = text;
				entry.editedAt = this.now();
				return;
			}
			// Clear ran while the post was in flight: remove its late result, not a newer turn's status.
			await this.api.deleteMessage(posted.channel, posted.ts).catch(() => {});
		} catch (error) {
			if (!entry.message && this.#messages.get(conversationId) === entry) this.#messages.delete(conversationId);
			this.log.error(
				`Slack working status failed for ${conversationId}: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}

	async clear(conversationId: string): Promise<void> {
		this.#addressed.delete(conversationId);
		const timer = this.#staleTimers.get(conversationId);
		if (timer) this.clearTimer(timer);
		this.#staleTimers.delete(conversationId);
		const entry = this.#messages.get(conversationId);
		this.#messages.delete(conversationId);
		if (!entry?.message) return;
		try {
			await this.api.deleteMessage(entry.message.channel, entry.message.ts);
		} catch {
			// The Slack message may already be gone; this is cosmetic either way.
		}
	}
}
