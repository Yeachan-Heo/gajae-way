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
	readonly #channels = new Map<string, ChannelLane>();
	constructor(
		readonly minIntervalMs = 1_000,
		readonly now: () => number = Date.now,
		readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
	) {}

	/**
	 * Waits for this channel's next slot. Slots are handed out one at a time,
	 * and at each hand-out every waiting delivery goes before any waiting
	 * cosmetic - priority is decided at dispatch, not by arrival order, so a
	 * burst of status traffic can never delay a reply that arrived after it.
	 */
	async acquire(channel: string, priority: "delivery" | "cosmetic" = "delivery"): Promise<void> {
		const lane = this.#lane(channel);
		await new Promise<void>((resolve) => {
			(priority === "delivery" ? lane.deliveries : lane.cosmetics).push(resolve);
			void this.#drain(channel, lane);
		});
	}

	/** How long a new write on this channel would wait right now; 0 once the lane is idle. */
	pendingMs(channel: string): number {
		const lane = this.#channels.get(channel);
		if (!lane) return 0;
		const queued = lane.deliveries.length + lane.cosmetics.length;
		return Math.max(0, lane.nextAt - this.now()) + queued * this.minIntervalMs;
	}

	#lane(channel: string): ChannelLane {
		let lane = this.#channels.get(channel);
		if (!lane) {
			lane = { nextAt: 0, deliveries: [], cosmetics: [], draining: false };
			this.#channels.set(channel, lane);
		}
		return lane;
	}

	async #drain(channel: string, lane: ChannelLane): Promise<void> {
		if (lane.draining) return;
		lane.draining = true;
		try {
			while (lane.deliveries.length > 0 || lane.cosmetics.length > 0) {
				const wait = lane.nextAt - this.now();
				if (wait > 0) await this.sleep(wait);
				const next = lane.deliveries.shift() ?? lane.cosmetics.shift();
				if (!next) break;
				lane.nextAt = Math.max(this.now(), lane.nextAt) + this.minIntervalMs;
				next();
			}
		} finally {
			lane.draining = false;
			// An empty lane is retired once its cooldown has elapsed, so the map is
			// bounded by activity. The cooldown is still running right here, so the
			// retirement is scheduled and re-checks the lane is the same and still idle.
			if (lane.deliveries.length === 0 && lane.cosmetics.length === 0) {
				const retireAt = lane.nextAt;
				void this.sleep(Math.max(0, retireAt - this.now())).then(() => {
					const current = this.#channels.get(channel);
					if (
						current === lane &&
						!lane.draining &&
						lane.deliveries.length === 0 &&
						lane.cosmetics.length === 0 &&
						lane.nextAt <= this.now()
					)
						this.#channels.delete(channel);
				});
			}
		}
	}
}

interface ChannelLane {
	nextAt: number;
	readonly deliveries: Array<() => void>;
	readonly cosmetics: Array<() => void>;
	draining: boolean;
}

/**
 * Read methods that only accept form/query parameters. Everything the adapter
 * writes with accepts JSON, so the set is the read surface, not a guess.
 */
const SLACK_FORM_ENCODED_METHODS: ReadonlySet<string> = new Set([
	"conversations.history",
	"conversations.replies",
	"conversations.info",
	"users.info",
	"reactions.get",
]);

/** Form-encodes the flat parameter objects this client sends; undefined is omitted, not sent as "undefined". */
export function formEncode(parameters: Record<string, unknown>): string {
	const body = new URLSearchParams();
	for (const [key, value] of Object.entries(parameters)) {
		if (value === undefined || value === null) continue;
		body.set(key, typeof value === "string" ? value : String(value));
	}
	return body.toString();
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
		// Slack honours a JSON body only on write methods (chat.*, reactions.add/remove,
		// apps.connections.open, auth.test). Read methods - conversations.*, users.info,
		// reactions.get - ignore it and answer `invalid_arguments` / `user_not_found` /
		// `no_item_specified` as if nothing was sent. That made every recovery backfill
		// fail on its first `conversations.replies` and left missed messages unrecovered
		// (live, 2026-09-17: "Slack recovery incomplete … invalid_arguments" on loop).
		const encoding = responseUrl || !SLACK_FORM_ENCODED_METHODS.has(method) ? "json" : "form";
		let response: Response | undefined;
		let retryAfterMs = 0;
		for (let attempt = 0; ; attempt++) {
			response = await this.fetcher(responseUrl ? method : `https://slack.com/api/${method}`, {
				method: "POST",
				headers: {
					"content-type": encoding === "json" ? "application/json" : "application/x-www-form-urlencoded",
					...(responseUrl ? {} : { Authorization: `Bearer ${token ?? this.botToken}` }),
				},
				body: encoding === "json" ? JSON.stringify(parameters) : formEncode(parameters),
			});
			// A 429, or a 200 whose body says `ratelimited`, both mean "slow down".
			// Honour Retry-After (seconds), bounded, then give up as ambiguous. Never a
			// definitive refusal: Slack did not judge the payload.
			const limited = response.status === 429 || (await bodySaysRateLimited(response));
			if (!limited) break;
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

	async addReaction(
		channel: string,
		timestamp: string,
		name: string,
		priority: "delivery" | "cosmetic" = "delivery",
	): Promise<void> {
		await this.limiter?.acquire(channel, priority);
		try {
			await this.call("reactions.add", { channel, timestamp, name });
		} catch (error) {
			if (!(error instanceof SlackApiError) || error.code !== "already_reacted") throw error;
		}
	}

	/** Removes our own reaction; one that is already gone counts as removed. */
	async removeReaction(channel: string, timestamp: string, name: string): Promise<void> {
		await this.limiter?.acquire(channel, "cosmetic");
		try {
			await this.call("reactions.remove", { channel, timestamp, name });
		} catch (error) {
			if (!(error instanceof SlackApiError) || error.code !== "no_reaction") throw error;
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

/**
 * Peeks at a cloned body for `{"ok":false,"error":"ratelimited"}` without
 * consuming the response the caller still has to parse. Anything unreadable
 * is "not rate limited" - the normal path will classify it.
 */
async function bodySaysRateLimited(response: Response): Promise<boolean> {
	if (response.status !== 200) return false;
	try {
		const body = (await response.clone().json()) as unknown;
		return isObject(body) && body.ok === false && body.error === "ratelimited";
	} catch {
		return false;
	}
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
