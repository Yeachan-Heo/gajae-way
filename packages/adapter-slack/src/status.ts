import type { ChatProgressPayload, OriginRef } from "@gajaeway/protocol";
import type { SlackWebApi } from "./api";
import { parseSlackMessageId } from "./origin";

export const WORKING_STATUS_STALE_MS = 90_000;

type Timer = { unref?(): void };
type StatusMessage = { readonly channel: string; readonly ts: string };
type Entry = { message?: StatusMessage };

/** Show only reported activity: zero counters are noise, not proof the turn did nothing. */
export function workingStatusText(
	progress: Pick<ChatProgressPayload, "elapsedMs" | "toolCalls" | "outputTokens">,
): string {
	const total = Math.floor(progress.elapsedMs / 1000);
	const minutes = Math.floor(total / 60);
	const seconds = total % 60;
	const parts = [minutes > 0 ? `${minutes}m ${String(seconds).padStart(2, "0")}s` : `${seconds}s`];
	if (progress.toolCalls > 0) parts.push(`${progress.toolCalls} tool${progress.toolCalls === 1 ? "" : "s"}`);
	if (progress.outputTokens > 0)
		parts.push(
			progress.outputTokens >= 1000
				? `${(progress.outputTokens / 1000).toFixed(1)}k tok`
				: `${progress.outputTokens} tok`,
		);
	return `⏳ working… (${parts.join(", ")})`;
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
				await this.api.updateMessage(entry.message.channel, entry.message.ts, text);
				return;
			}
			const thread = origin.kind === "thread" ? parseSlackMessageId(conversationId) : undefined;
			if (origin.kind === "thread" && !thread) throw new Error("Slack status thread has an invalid message id");
			this.#messages.set(conversationId, entry);
			const posted = await this.api.postMessage(thread?.channel ?? conversationId, text, thread?.ts);
			if (this.#messages.get(conversationId) === entry) {
				entry.message = posted;
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
