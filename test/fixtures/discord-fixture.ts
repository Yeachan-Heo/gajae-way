import type { DiscordMessage, DiscordMessageHandler, DiscordPlatform } from "../../src/adapter/discord/platform";

interface DiscordFixtureTimer {
	readonly at: number;
	readonly callback: () => void | Promise<void>;
}

interface DeferredTypingAcknowledgement {
	resolve(): void;
	reject(error: Error): void;
}

interface DeferredSend {
	resolve(): void;
}


export class DiscordFixtureClock {
	#now: number;
	#nextTimerId = 1;
	readonly #timers = new Map<number, DiscordFixtureTimer>();
	readonly #timerErrors: Error[] = [];


	constructor(initialNow = 0) {
		this.#now = initialNow;
	}

	now = (): number => this.#now;

	get scheduledTimerCount(): number {
		return this.#timers.size;
	}




	advance(milliseconds: number): void {
		assertDuration(milliseconds);
		this.#now += milliseconds;
	}

	setTimeout(callback: () => void | Promise<void>, milliseconds: number): number {
		assertDuration(milliseconds);
		const timerId = this.#nextTimerId;
		this.#nextTimerId += 1;
		this.#timers.set(timerId, { at: this.#now + milliseconds, callback });
		return timerId;
	}

	clearTimeout(timer: unknown): void {
		if (typeof timer === "number") this.#timers.delete(timer);
	}

	/** Fires due callbacks without awaiting asynchronous work; use flushAsync() to drain settled continuations. */
	advanceTimersBy(milliseconds: number): void {
		assertDuration(milliseconds);
		const deadline = this.#now + milliseconds;
		for (;;) {
			const next = this.nextTimerBefore(deadline);
			if (!next) {
				this.#now = deadline;
				return;
			}
			const [timerId, timer] = next;
			this.#timers.delete(timerId);
			this.#now = timer.at;
			this.runTimer(timer.callback);
		}
	}

	async flushAsync(): Promise<void> {
		for (let turn = 0; turn < 4; turn += 1) await Promise.resolve();
		const error = this.#timerErrors.shift();
		if (error) throw error;
	}


	private nextTimerBefore(deadline: number): [number, DiscordFixtureTimer] | undefined {
		let next: [number, DiscordFixtureTimer] | undefined;
		for (const timer of this.#timers) {
			if (timer[1].at > deadline) continue;
			if (!next || timer[1].at < next[1].at || (timer[1].at === next[1].at && timer[0] < next[0])) next = timer;
		}
		return next;
	}

	private runTimer(callback: () => void | Promise<void>): void {
		try {
			void Promise.resolve(callback()).catch(error => this.#timerErrors.push(asError(error)));
		} catch (error) {
			this.#timerErrors.push(asError(error));
		}
	}

}

function assertDuration(milliseconds: number): void {
	if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) throw new Error("Fixture clock advance must be a non-negative safe integer.");
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
	readonly acknowledgementAttempts: DiscordFixtureAcknowledgement[] = [];
	readonly reactions: DiscordFixtureReaction[] = [];
	connectCount = 0;
	disconnectCount = 0;

	readonly #handlers = new Set<DiscordMessageHandler>();
	readonly #now: () => number;
	#sendId: (input: { channelId: string; text: string; nonce: string; ordinal: number }) => string;
	readonly #nonceIds = new Map<string, string>();
	readonly #queuedSendIds: string[] = [];
	readonly #queuedMessages: DiscordMessage[] = [];
	readonly #queuedTypingErrors: Error[] = [];
	#deferredTypingCount = 0;
	#deferredSendCount = 0;
	readonly #pendingTyping: DeferredTypingAcknowledgement[] = [];
	readonly #pendingSends: DeferredSend[] = [];



	#connected = false;

	constructor(options: DiscordFixtureOptions = {}) {
		this.#now = options.now ?? Date.now;
		this.#sendId = options.sendId ?? (input => `discord-send-${input.ordinal}`);
	}

	get pendingTypingCount(): number {
		return this.#pendingTyping.length;
	}

	get pendingSendCount(): number {
		return this.#pendingSends.length;
	}

	get connected(): boolean {
		return this.#connected;
	}

	get queuedMessageCount(): number {
		return this.#queuedMessages.length;
	}

	get messageHandlerCount(): number {
		return this.#handlers.size;
	}

	async connect(): Promise<void> {
		this.#connected = true;
		this.connectCount += 1;
		for (const message of this.#queuedMessages.splice(0)) await this.dispatch(message);
	}

	async disconnect(): Promise<void> {
		this.#connected = false;
		this.disconnectCount += 1;
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

	failNextTyping(error = new Error("fixture typing acknowledgement failed")): void {
		this.#queuedTypingErrors.push(error);
	}

	deferNextTyping(): void {
		this.#deferredTypingCount += 1;
	}

	resolveNextTyping(): void {
		const pending = this.#pendingTyping.shift();
		if (!pending) throw new Error("No deferred fixture typing acknowledgement is pending.");
		pending.resolve();
	}

	rejectNextTyping(error = new Error("fixture deferred typing acknowledgement failed")): void {
		const pending = this.#pendingTyping.shift();
		if (!pending) throw new Error("No deferred fixture typing acknowledgement is pending.");
		pending.reject(error);
	}

	deferNextSend(): void {
		this.#deferredSendCount += 1;
	}

	releaseNextSend(): void {
		const pending = this.#pendingSends.shift();
		if (!pending) throw new Error("No deferred fixture send is pending.");
		pending.resolve();
	}


	/** Queues an inbound event until the next gateway connection without acknowledging it. */
	queueMessage(input: DiscordFixtureMessage): void {
		this.#queuedMessages.push(this.recordMessage(input));
	}

	async emitMessage(input: DiscordFixtureMessage): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		await this.dispatch(this.recordMessage(input));
	}

	private recordMessage(input: DiscordFixtureMessage): DiscordMessage {
		const message: DiscordMessage = {
			id: input.id,
			channelId: input.channelId,
			text: input.text,
			...(input.authorId ? { authorId: input.authorId } : {}),
			...(input.authorBot !== undefined ? { authorBot: input.authorBot } : {}),
			acceptedAt: input.acceptedAt ?? this.#now(),
		};
		this.messages.push(input);
		return message;
	}

	private async dispatch(message: DiscordMessage): Promise<void> {
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
		if (this.#deferredSendCount > 0) {
			this.#deferredSendCount -= 1;
			return await new Promise<string>(resolve => {
				this.#pendingSends.push({
					resolve: () => {
						this.sends.push(attempt);
						resolve(id);
					},
				});
			});
		}
		this.sends.push(attempt);
		return id;
	}


	async ackTyping(channelId: string): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		const acknowledgement = { channelId, at: this.#now() };
		this.acknowledgementAttempts.push(acknowledgement);
		const error = this.#queuedTypingErrors.shift();
		if (error) throw error;
		if (this.#deferredTypingCount > 0) {
			this.#deferredTypingCount -= 1;
			return await new Promise<void>((resolve, reject) => {
				this.#pendingTyping.push({
					resolve: () => {
						this.acknowledgements.push(acknowledgement);
						resolve();
					},
					reject,
				});
			});
		}
		this.acknowledgements.push(acknowledgement);
	}


	async react(channelId: string, messageId: string, emoji: string): Promise<void> {
		if (!this.#connected) throw new Error("Discord fixture gateway is disconnected.");
		this.reactions.push({ channelId, messageId, emoji, at: this.#now() });
	}
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}
