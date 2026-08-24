import type { DiscordMessage, DiscordPlatform } from "./platform";

export const DISCORD_ACK_BUDGET_MS = 2_000;

/**
 * openclaw's reaction doctrine: a reaction is the lightweight "I saw this, I
 * acknowledge you" signal, with 👀 reserved for acknowledgement and at most one
 * reaction per message. This is the default request-detected acknowledgement.
 */
export const DISCORD_ACK_REACTION = "\u{1F440}";

export interface DiscordAcknowledgement {
	readonly messageId: string;
	readonly channelId: string;
	readonly accepted: boolean;
	readonly acknowledgedAt: number;
	readonly elapsedMs: number;
	/** Whether the request-detected reaction was placed. */
	readonly reacted: boolean;
}

export interface DiscordAcknowledgementOptions {
	readonly budgetMs?: number;
	readonly now?: () => number;
	/** Reaction placed on the detected request; `false` disables it. Defaults to 👀. */
	readonly reaction?: string | false;
}

export class DiscordAcknowledgementError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordAcknowledgementError";
	}
}

/**
 * Runs only after the caller observes durable `main.submit` acceptance. The
 * gateway exposes no earlier accepted-claim response, so the typing budget
 * begins at that observable acceptance boundary rather than the inbound Discord
 * dispatch. This permits slow successful admissions to receive late typing
 * without acknowledging a fenced or rejected message.
 */
export async function acknowledgeDiscordMessage(
	platform: DiscordPlatform,
	message: DiscordMessage,
	options: DiscordAcknowledgementOptions = {},
): Promise<DiscordAcknowledgement> {
	const now = options.now ?? Date.now;
	const budgetMs = options.budgetMs ?? DISCORD_ACK_BUDGET_MS;
	if (!Number.isSafeInteger(budgetMs) || budgetMs < 1) {
		throw new DiscordAcknowledgementError("Discord acknowledgement budget must be a positive safe integer.");
	}
	const startedAt = now();
	// React FIRST so the requester sees the request was picked up, then start
	// typing. A failed reaction must not fail the acknowledgement: reactions are a
	// social signal, while typing is the contract the budget exists for.
	const reaction = options.reaction === undefined ? DISCORD_ACK_REACTION : options.reaction;
	let reacted = false;
	if (reaction) {
		try {
			await withinBudget(platform.react(message.channelId, message.id, reaction), budgetMs, message.id);
			reacted = true;
		} catch {
			reacted = false;
		}
	}
	await withinBudget(platform.ackTyping(message.channelId), budgetMs, message.id);
	const acknowledgedAt = now();
	if (acknowledgedAt > startedAt + budgetMs) {
		throw new DiscordAcknowledgementError(`Discord acknowledgement exceeded the ${budgetMs}ms budget for message ${message.id}.`);
	}
	return {
		messageId: message.id,
		channelId: message.channelId,
		accepted: true,
		acknowledgedAt,
		elapsedMs: acknowledgedAt - startedAt,
		reacted,
	};
}

function withinBudget(promise: Promise<void>, remainingMs: number, messageId: string): Promise<void> {
	return new Promise((resolve, reject) => {
		const timeout = setTimeout(
			() => reject(new DiscordAcknowledgementError(`Discord acknowledgement exceeded its deadline for message ${messageId}.`)),
			remainingMs,
		);
		void promise.then(
			() => {
				clearTimeout(timeout);
				resolve();
			},
			error => {
				clearTimeout(timeout);
				reject(error);
			},
		);
	});
}
