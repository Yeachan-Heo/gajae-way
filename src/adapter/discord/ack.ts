import type { DiscordMessage, DiscordPlatform } from "./platform";

export const DISCORD_ACK_BUDGET_MS = 2_000;

export interface DiscordAcknowledgement {
	readonly messageId: string;
	readonly channelId: string;
	readonly accepted: boolean;
	readonly acknowledgedAt: number;
	readonly elapsedMs: number;
}

export interface DiscordAcknowledgementOptions {
	readonly budgetMs?: number;
	readonly now?: () => number;
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
