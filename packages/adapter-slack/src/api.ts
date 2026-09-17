import type { SlackConversationLike, SlackUserLike } from "./author";

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

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

export class SlackWebApi {
	constructor(
		readonly botToken: string,
		readonly fetcher: FetchLike = fetch,
	) {}

	async call<T>(method: string, parameters: Record<string, unknown> = {}, token?: string): Promise<T> {
		// Response URLs are already credentials: never forward the bot token to them.
		const responseUrl = method.startsWith("https://");
		const response = await this.fetcher(responseUrl ? method : `https://slack.com/api/${method}`, {
			method: "POST",
			headers: {
				"content-type": "application/json",
				...(responseUrl ? {} : { Authorization: `Bearer ${token ?? this.botToken}` }),
			},
			body: JSON.stringify(parameters),
		});
		// Slash-command responses may return plain text rather than a Web API envelope.
		if (responseUrl) {
			if (!response.ok) throw new SlackApiError(response.status, `http_${response.status}`);
			return undefined as T;
		}
		let body: unknown;
		try {
			body = await response.json();
		} catch {
			throw new SlackApiError(response.status, "invalid_response");
		}
		if (!response.ok || (isObject(body) && body.ok === false)) {
			throw new SlackApiError(
				response.status,
				isObject(body) && typeof body.error === "string" ? body.error : `http_${response.status}`,
			);
		}
		if (!isObject(body)) throw new SlackApiError(response.status, "invalid_response");
		return body as T;
	}

	postMessage(
		channel: string,
		text: string,
		threadTs?: string,
	): Promise<{ readonly ts: string; readonly channel: string }> {
		return this.call("chat.postMessage", {
			channel,
			text,
			mrkdwn: true,
			unfurl_links: false,
			...(threadTs === undefined ? {} : { thread_ts: threadTs }),
		});
	}

	updateMessage(channel: string, ts: string, text: string): Promise<unknown> {
		return this.call("chat.update", { channel, ts, text });
	}

	deleteMessage(channel: string, ts: string): Promise<unknown> {
		return this.call("chat.delete", { channel, ts });
	}

	async addReaction(channel: string, timestamp: string, name: string): Promise<void> {
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
