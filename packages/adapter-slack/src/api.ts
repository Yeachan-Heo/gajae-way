import type { SlackConversationLike, SlackUserLike } from "./author";

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

/**
 * A response Slack sent but this client could not interpret. Deliberately NOT a
 * SlackApiError: `deliveryFailureIsAmbiguous` treats it as an unknown outcome.
 */
export class SlackUnreadableResponseError extends Error {
	constructor(
		readonly status: number,
		cause?: unknown,
	) {
		super(`Slack returned an unreadable response (HTTP ${status})`, cause === undefined ? undefined : { cause });
		this.name = "SlackUnreadableResponseError";
	}
}

export class SlackApiError extends Error {
	constructor(
		readonly status: number,
		readonly code: string,
		message?: string,
	) {
		super(message ?? `Slack API request failed: ${code}`);
		this.name = "SlackApiError";
	}
}

export interface SlackAuthIdentity {
	readonly user_id: string;
	readonly user: string;
	readonly team_id: string;
	readonly team: string;
	readonly bot_id?: string;
}

export interface SlackHistoryPage {
	readonly messages: readonly Record<string, unknown>[];
	readonly has_more: boolean;
	readonly next_cursor?: string;
}

type HistoryResponse = SlackHistoryPage & { readonly response_metadata?: { readonly next_cursor?: string } };

/**
 * Slack said "slow down" and kept saying it for the whole retry budget. NOT a
 * SlackApiError: the write was never refused on its merits, so the delivery is
 * ambiguous and stays in the ledger for a later attempt instead of being
 * recorded as a definitive failure.
 */
export class SlackRateLimitedError extends Error {
	constructor(
		readonly retryAfterMs: number,
		readonly attempts: number,
	) {
		super(`Slack rate limited after ${attempts} attempts; retry after ${retryAfterMs}ms`);
		this.name = "SlackRateLimitedError";
	}
}

/** Bounded, Retry-After-driven retry for HTTP 429 / `ratelimited`. */
export const RATE_LIMIT_MAX_RETRIES = 3;
export const RATE_LIMIT_MAX_WAIT_MS = 30_000;
const RATE_LIMIT_DEFAULT_WAIT_MS = 1_000;

export interface SlackWebApiOptions {
	readonly fetcher?: FetchLike;
	readonly sleep?: (ms: number) => Promise<void>;
	/** Outbound pacing shared by every write on a channel; absent means unpaced. */
	readonly limiter?: OutboundLimiter;
}

/**
 * Per-channel outbound pacing. Slack's chat.postMessage tier is about one
 * message per second per channel, and cosmetic traffic (working-status edits)
 * must never crowd out a reply: deliveries take the next slot first, cosmetics
 * wait, and a cosmetic that has waited longer than its usefulness is dropped
 * by its caller rather than sent late.
 */
export class OutboundLimiter {
	readonly #next = new Map<string, number>();
	readonly #queued = new Map<string, number>();
	constructor(
		readonly minIntervalMs = 1_000,
		readonly now: () => number = Date.now,
		readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
	) {}

	/** Waits for this channel's next slot; `priority` writes jump ahead of pending cosmetics. */
	async acquire(channel: string, priority: "delivery" | "cosmetic" = "delivery"): Promise<void> {
		const queued = this.#queued.get(channel) ?? 0;
		// A cosmetic behind a queue of deliveries yields its slot to them.
		const slotsAhead = priority === "cosmetic" ? queued : 0;
		const at = Math.max(this.now(), this.#next.get(channel) ?? 0) + slotsAhead * this.minIntervalMs;
		this.#next.set(channel, at + this.minIntervalMs);
		if (priority === "delivery") this.#queued.set(channel, queued + 1);
		try {
			const wait = at - this.now();
			if (wait > 0) await this.sleep(wait);
		} finally {
			if (priority === "delivery") this.#queued.set(channel, Math.max(0, (this.#queued.get(channel) ?? 1) - 1));
		}
	}

	/** How long a cosmetic write would wait right now; callers skip stale cosmetics. */
	pendingMs(channel: string): number {
		return Math.max(0, (this.#next.get(channel) ?? 0) - this.now());
	}
}

export class SlackWebApi {
	readonly fetcher: FetchLike;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly limiter: OutboundLimiter | undefined;

	constructor(
		readonly botToken: string,
		options: FetchLike | SlackWebApiOptions = {},
	) {
		const resolved = typeof options === "function" ? { fetcher: options } : options;
		this.fetcher = resolved.fetcher ?? fetch;
		this.#sleep = resolved.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
		this.limiter = resolved.limiter;
	}

	async call<T>(method: string, parameters: Record<string, unknown> = {}, token?: string): Promise<T> {
		// Response URLs are already credentials: never forward the bot token to them.
		const responseUrl = method.startsWith("https://");
		let response: Response | undefined;
		let retryAfterMs = 0;
		for (let attempt = 0; ; attempt++) {
			response = await this.fetcher(responseUrl ? method : `https://slack.com/api/${method}`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					...(responseUrl ? {} : { Authorization: `Bearer ${token ?? this.botToken}` }),
				},
				body: JSON.stringify(parameters),
			});
			// 429 carries Retry-After in seconds; honour it, bounded, then give up as
			// ambiguous. Never a definitive refusal: Slack did not judge the payload.
			if (response.status !== 429) break;
			const header = Number(response.headers.get("retry-after"));
			retryAfterMs = Math.min(
				RATE_LIMIT_MAX_WAIT_MS,
				Number.isFinite(header) && header > 0 ? header * 1000 : RATE_LIMIT_DEFAULT_WAIT_MS,
			);
			if (attempt >= RATE_LIMIT_MAX_RETRIES) throw new SlackRateLimitedError(retryAfterMs, attempt + 1);
			await this.#sleep(retryAfterMs);
		}
		// Slash-command responses may return plain text rather than a Web API envelope.
		if (responseUrl) {
			if (!response.ok) throw new SlackApiError(response.status, `http_${response.status}`);
			return undefined as T;
		}
		// Delivery ambiguity is decided by the error class: a SlackApiError is Slack
		// saying no, everything else is "we do not know". An unreadable or truncated
		// body is the second kind - Slack may well have accepted the write - so it
		// must NOT become a SlackApiError, or the ledger would record a definitive
		// non-delivery for a message the room can already see.
		let body: unknown;
		try {
			body = await response.json();
		} catch (error) {
			throw new SlackUnreadableResponseError(response.status, error);
		}
		if (isObject(body) && body.ok === false) {
			// `ratelimited` in the body without a 429 status is the same signal.
			if (body.error === "ratelimited") throw new SlackRateLimitedError(RATE_LIMIT_DEFAULT_WAIT_MS, 1);
			throw new SlackApiError(response.status, typeof body.error === "string" ? body.error : `http_${response.status}`);
		}
		if (!response.ok) throw new SlackApiError(response.status, `http_${response.status}`);
		// Success needs an affirmative `ok: true`; `{}` is not evidence that anything happened.
		if (!isObject(body) || body.ok !== true) throw new SlackUnreadableResponseError(response.status);
		return body as T;
	}

	async postMessage(
		channel: string,
		text: string,
		threadTs?: string,
		priority: "delivery" | "cosmetic" = "delivery",
	): Promise<{ readonly ts: string; readonly channel: string }> {
		await this.limiter?.acquire(channel, priority);
		return this.call("chat.postMessage", {
			channel,
			text,
			mrkdwn: true,
			unfurl_links: false,
			...(threadTs === undefined ? {} : { thread_ts: threadTs }),
		});
	}

	async updateMessage(channel: string, ts: string, text: string): Promise<unknown> {
		await this.limiter?.acquire(channel, "cosmetic");
		return this.call("chat.update", { channel, ts, text });
	}

	async deleteMessage(channel: string, ts: string): Promise<unknown> {
		await this.limiter?.acquire(channel, "cosmetic");
		return this.call("chat.delete", { channel, ts });
	}

	async addReaction(channel: string, timestamp: string, name: string): Promise<void> {
		await this.limiter?.acquire(channel, "delivery");
		try {
			await this.call("reactions.add", { channel, timestamp, name });
		} catch (error) {
			if (!(error instanceof SlackApiError) || error.code !== "already_reacted") throw error;
		}
	}

	authTest(): Promise<SlackAuthIdentity> {
		return this.call("auth.test");
	}

	async usersInfo(user: string): Promise<SlackUserLike> {
		return (await this.call<{ user: SlackUserLike }>("users.info", { user })).user;
	}

	async conversationsInfo(channel: string): Promise<SlackConversationLike> {
		return (await this.call<{ channel: SlackConversationLike }>("conversations.info", { channel })).channel;
	}

	async conversationsHistory(
		channel: string,
		options: { oldest?: string; latest?: string; cursor?: string; limit?: number; inclusive?: boolean } = {},
	): Promise<SlackHistoryPage> {
		return historyPage(await this.call<HistoryResponse>("conversations.history", { channel, ...options }));
	}

	async conversationsReplies(
		channel: string,
		ts: string,
		options: { oldest?: string; cursor?: string; limit?: number } = {},
	): Promise<SlackHistoryPage> {
		return historyPage(await this.call<HistoryResponse>("conversations.replies", { channel, ts, ...options }));
	}

	connectionsOpen(appToken: string): Promise<{ readonly url: string }> {
		return this.call("apps.connections.open", {}, appToken);
	}

	respond(responseUrl: string, payload: Record<string, unknown>): Promise<void> {
		if (!responseUrl.startsWith("https://"))
			return Promise.reject(new SlackApiError(0, "invalid_response", "Slack response URL must use HTTPS"));
		return this.call(responseUrl, payload);
	}
}

/** Transport failure cannot prove whether Slack accepted the write before disconnecting. */
export function deliveryFailureIsAmbiguous(error: unknown): boolean {
	return !(error instanceof SlackApiError);
}

function historyPage(body: HistoryResponse): SlackHistoryPage {
	const cursor = body.response_metadata?.next_cursor ?? body.next_cursor;
	return { messages: body.messages, has_more: body.has_more, ...(cursor ? { next_cursor: cursor } : {}) };
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
