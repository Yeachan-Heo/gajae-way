import type { ChatProgressPayload } from "@gajaeway/protocol";
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

	arm(conversationId: string): void {
		this.#addressed.add(conversationId);
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "slack") return;
		const { conversationId } = progress.origin;
		if (!this.#addressed.has(conversationId)) return;
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
			const text = workingStatusText(progress);
			if (entry.message) {
				await this.api.updateMessage(entry.message.channel, entry.message.ts, text);
				return;
			}
			const thread = progress.origin.kind === "thread" ? parseSlackMessageId(conversationId) : undefined;
			if (progress.origin.kind === "thread" && !thread)
				throw new Error("Slack status thread has an invalid message id");
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
