import type { DiscordMessage, DiscordMessageHandler, DiscordPlatform } from "../../src/adapter/discord/platform";

export class DiscordFixtureClock {
	#now: number;

	constructor(initialNow = 0) {
		this.#now = initialNow;
	}

	now = (): number => this.#now;

	advance(milliseconds: number): void {
		if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) throw new Error("Fixture clock advance must be a non-negative safe integer.");
		this.#now += milliseconds;
	}
}

export interface DiscordFixtureMessage {
	readonly id: string;
	readonly channelId: string;
	readonly text: string;
	readonly authorId?: string;
	readonly authorBot?: boolean;
	readonly acceptedAt?: number;
}

export interface DiscordFixtureSend {
	readonly channelId: string;
	readonly text: string;
	readonly nonce: string;
	readonly id: string;
	readonly at: number;
	readonly duplicate: boolean;
}

export interface DiscordFixtureAcknowledgement {
	readonly channelId: string;
	readonly at: number;
}

export interface DiscordFixtureReaction {
	readonly channelId: string;
	readonly messageId: string;
	readonly emoji: string;
	readonly at: number;
}

export interface DiscordFixtureOptions {
	readonly now?: () => number;
	readonly sendId?: (input: { channelId: string; text: string; nonce: string; ordinal: number }) => string;
}

/**
 * In-process DiscordPlatform for deterministic adapter protocol drills. A
 * repeated nonce returns the original platform id and is captured as a
 * duplicate attempt without publishing a second fixture post.
 */
export class DiscordFixture implements DiscordPlatform {
	readonly messages: DiscordFixtureMessage[] = [];
	readonly sends: DiscordFixtureSend[] = [];
	readonly sendAttempts: DiscordFixtureSend[] = [];
	readonly acknowledgements: DiscordFixtureAcknowledgement[] = [];
	readonly reactions: DiscordFixtureReaction[] = [];
	readonly #handlers = new Set<DiscordMessageHandler>();
	readonly #now: () => number;
	#sendId: (input: { channelId: string; text: string; nonce: string; ordinal: number }) => string;
	readonly #nonceIds = new Map<string, string>();
	readonly #queuedSendIds: string[] = [];
	#connected = false;

	constructor(options: DiscordFixtureOptions = {}) {
		this.#now = options.now ?? Date.now;
		this.#sendId = options.sendId ?? (input => `discord-send-${input.ordinal}`);
	}

	get connected(): boolean {
		return this.#connected;
	}

	async connect(): Promise<void> {
		this.#connected = true;
	}

	async disconnect(): Promise<void> {
		this.#connected = false;
	}

	/** Simulates an unexpected gateway disconnect; reconnect is explicit. */
	simulateDisconnect(): void {
		this.#connected = false;
	}

	simulateReconnect(): void {
		this.#connected = true;
	}

	onMessage(callback: DiscordMessageHandler): () => void {
		this.#handlers.add(callback);
		return () => this.#handlers.delete(callback);
	}

	assignNextSendId(id: string): void {
		if (!id) throw new Error("Fixture send id must not be empty.");
		this.#queuedSendIds.push(id);
	}

	setSendIdAllocator(allocator: DiscordFixtureOptions["sendId"]): void {
		if (!allocator) throw new Error("Fixture send id allocator is required.");
		this.#sendId = allocator;
	}

	async emitMessage(input: DiscordFixtureMessage): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		const message: DiscordMessage = {
			id: input.id,
			channelId: input.channelId,
			text: input.text,
			...(input.authorId ? { authorId: input.authorId } : {}),
			...(input.authorBot !== undefined ? { authorBot: input.authorBot } : {}),
			acceptedAt: input.acceptedAt ?? this.#now(),
		};
		this.messages.push(input);
		for (const handler of [...this.#handlers]) await handler(message);
	}

	async send(channelId: string, text: string, nonce: string): Promise<string> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		const priorId = this.#nonceIds.get(nonce);
		const id = priorId ?? this.#queuedSendIds.shift() ?? this.#sendId({ channelId, text, nonce, ordinal: this.sendAttempts.length + 1 });
		const attempt: DiscordFixtureSend = { channelId, text, nonce, id, at: this.#now(), duplicate: priorId !== undefined };
		this.sendAttempts.push(attempt);
		if (priorId) return priorId;
		this.#nonceIds.set(nonce, id);
		this.sends.push(attempt);
		return id;
	}

	async ackTyping(channelId: string): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		this.acknowledgements.push({ channelId, at: this.#now() });
	}

	async react(channelId: string, messageId: string, emoji: string): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		this.reactions.push({ channelId, messageId, emoji, at: this.#now() });
	}
}
