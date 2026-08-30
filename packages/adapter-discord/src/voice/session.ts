import type { ChatMessagePayload } from "@gajaeway/protocol";
import {
	type Admission,
	ModalityRegistry,
	OriginTurnBook,
	type SttEvent,
	type SttProvider,
	type SttStream,
	type VoiceDrillRecord,
} from "@gajaeway/voice-core";
import type { VoiceConfig } from "../config";
import {
	createSpeakerReceiver,
	type SpeakerReceiveHandle,
	type VoiceReceiveDecoderFactory,
	type VoiceReceiveIngressEvent,
	type VoiceReceiverLike,
	type VoiceReceiveUtterance,
} from "./receive";

export type VoiceSessionState = "idle" | "joining" | "active" | "draining" | "closed";

export type VoiceCloseReason =
	| "kicked"
	| "connection_lost"
	| "provider_fatal"
	| "gateway_lost"
	| "shutdown"
	| "empty"
	| "idle"
	| "session_max"
	| "command";

/** The connection shape consumed by the lifecycle owner. */
export interface VoiceConnectionLike {
	destroy(): void | Promise<void>;
	onStateChange?(listener: (state: string | { readonly status?: string }) => void): (() => void) | undefined;
}

export interface VoiceConnectionOptions {
	readonly channelId: string;
	readonly guildId?: string;
}

export type VoiceConnectionFactory = (
	options: VoiceConnectionOptions,
) => VoiceConnectionLike | Promise<VoiceConnectionLike>;

/** The player owns the Discord audio resource and any active playback. */
export interface VoicePlayerLike {
	abort(): void | Promise<void>;
	/**
	 * The room's playback handle, when the runtime built one. Kept structural so the
	 * lifecycle owner never depends on the audio implementation.
	 */
	readonly playback?: VoiceRoomPlayback;
}

/** One visible, secret-free diagnostic describing why a room ended. */
export interface VoiceProviderFatalInfo {
	readonly originKey: string;
	readonly channelId: string;
	readonly code: string;
	readonly message: string;
	readonly at: string;
}

/** What a delivery needs from a room's playback: play one reply and report how it ended. */
export interface VoiceRoomPlayback {
	play(message: ChatMessagePayload): Promise<{
		readonly outcome: "completed" | "barge_in" | "failed";
		readonly audioPlayed?: boolean;
		readonly remainder?: string;
		readonly charIndex?: number;
		readonly atMs?: number;
		readonly textFallback?: boolean;
		readonly error?: unknown;
	}>;
}

export type VoicePlayerFactory = (
	connection: VoiceConnectionLike,
	options: VoiceConnectionOptions,
) => VoicePlayerLike | Promise<VoicePlayerLike>;

interface VoiceReleasableResource {
	unsubscribe?(): void | Promise<void>;
	release?(): void | Promise<void>;
	close?(): void | Promise<void>;
	destroy?(): void | Promise<void>;
}

export interface VoiceSubscriptionLike {
	unsubscribe?(): void | Promise<void>;
	release?(): void | Promise<void>;
	close?(): void | Promise<void>;
	destroy?(): void | Promise<void>;
}

export interface VoiceDecoderLike {
	release?(): void | Promise<void>;
	close?(): void | Promise<void>;
	destroy?(): void | Promise<void>;
}
export type VoiceReceiverFactory = (connection: VoiceConnectionLike) => VoiceReceiverLike | Promise<VoiceReceiverLike>;

export interface VoiceSpeakerResources {
	readonly subscription?: VoiceSubscriptionLike;
	readonly decoder?: VoiceDecoderLike;
	readonly displayName?: string;
	/** Tests and platform bindings may provide an already-open stream. */
	readonly stream?: SttStream;
	/** Internal receive pipeline retained across a provider reconnect. */
	readonly receive?: SpeakerReceiveHandle;
}

export interface VoiceSynthesisLike {
	abort?(): void | Promise<void>;
}

export type VoiceTimerHandle = unknown;

export interface VoiceClock {
	now(): number;
	setTimeout(callback: () => void, delayMs: number): VoiceTimerHandle;
	clearTimeout(handle: VoiceTimerHandle): void;
}

export interface VoiceDrillLogSink {
	write(record: VoiceDrillRecord): void | Promise<void>;
	flush(): void | Promise<void>;
	close(): void | Promise<void>;
}

export interface VoiceRoomSessionOptions {
	readonly config: VoiceConfig;
	readonly originKey?: string;
	readonly channelId: string;
	readonly guildId?: string;
	readonly botUserId?: string;
	readonly connectionFactory: VoiceConnectionFactory;
	readonly playerFactory: VoicePlayerFactory;
	readonly sttProvider: SttProvider;
	/** Optional Discord receiver binding; omitted in provider/lifecycle-only tests. */
	readonly receiverFactory?: VoiceReceiverFactory;
	/** Optional Opus decoder binding; omitted when a caller supplies a stream directly. */
	readonly decoderFactory?: VoiceReceiveDecoderFactory;
	/** Receives normalized VAD boundaries for the later voice bridge. */
	readonly onUtterance?: (utterance: VoiceReceiveUtterance) => void | Promise<void>;
	/** Observes every per-frame ingress admission, including reconnect drops. */
	readonly onIngress?: (event: VoiceReceiveIngressEvent) => void;
	readonly clock?: VoiceClock;
	readonly drillLogSink?: VoiceDrillLogSink;
	/**
	 * Reports a provider failure that ended the room, so the channel sees one honest
	 * diagnostic instead of the bot going quiet. Never receives a credential value.
	 */
	readonly onProviderFatal?: (info: VoiceProviderFatalInfo) => void | Promise<void>;
	readonly turnBook?: OriginTurnBook;
	readonly modality?: ModalityRegistry;
}

export interface VoiceStateMemberLike {
	readonly id?: string;
	readonly userId?: string;
	readonly bot?: boolean;
	readonly channelId?: string | null;
}

export interface VoiceStateChangeLike {
	readonly userId: string;
	readonly oldChannelId?: string | null;
	readonly newChannelId?: string | null;
	readonly channelId?: string | null;
	readonly botUserId?: string;
	readonly botId?: string;
	readonly botRemoved?: boolean;
	readonly members?: readonly VoiceStateMemberLike[];
	readonly channelMembers?: readonly VoiceStateMemberLike[];
}

export interface VoiceIngressCounters {
	readonly accepted: number;
	readonly dropped: number;
	readonly rejectedClosed: number;
}

interface SpeakerEntry {
	readonly speakerId: string;
	readonly stream: SttStream;
	readonly subscription?: VoiceSubscriptionLike;
	readonly decoder?: VoiceDecoderLike;
	closed: boolean;
	committed: boolean;
	released: boolean;
	closePromise?: Promise<void>;
	readonly receive?: SpeakerReceiveHandle;
	lastCommittedText: string | undefined;
}

interface LiveTimer {
	active: boolean;
	hasHandle: boolean;
	handle: VoiceTimerHandle;
}

interface ReconnectEntry {
	readonly entry: SpeakerEntry;
	readonly previousText: string | undefined;
	timer?: LiveTimer;
	started: boolean;
}

const VOICE_STT_SAMPLE_RATE_HZ = 16_000;

const defaultClock: VoiceClock = {
	now: () => Date.now(),
	setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
	clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

const STATE_RANK: Readonly<Record<VoiceSessionState, number>> = {
	idle: 0,
	joining: 1,
	active: 2,
	draining: 3,
	closed: 4,
};

export class VoiceSessionClosedError extends Error {
	constructor() {
		super("voice session is closed");
		this.name = "VoiceSessionClosedError";
	}
}

export class VoiceRejoinRequiredError extends Error {
	constructor() {
		super("voice session requires an explicit rejoin after a provider failure");
		this.name = "VoiceRejoinRequiredError";
	}
}

/**
 * Owns every resource associated with one Discord voice room.  All termination paths call
 * close(), which latches the first reason before doing any asynchronous work.
 */
export class VoiceRoomSession {
	readonly config: VoiceConfig;
	readonly channelId: string;
	readonly guildId: string | undefined;
	readonly originKey: string;

	readonly #connectionFactory: VoiceConnectionFactory;
	readonly #playerFactory: VoicePlayerFactory;
	readonly #sttProvider: SttProvider;
	readonly #clock: VoiceClock;
	readonly #drillLogSink: VoiceDrillLogSink | undefined;
	readonly #onProviderFatal: ((info: VoiceProviderFatalInfo) => void | Promise<void>) | undefined;
	#fatalReported = false;
	#fatalCode = "provider_fatal";
	#fatalMessage = "The voice provider failed.";
	readonly #receiverFactory: VoiceReceiverFactory | undefined;
	readonly #decoderFactory: VoiceReceiveDecoderFactory | undefined;
	readonly #onUtterance: ((utterance: VoiceReceiveUtterance) => void | Promise<void>) | undefined;
	readonly #onIngress: ((event: VoiceReceiveIngressEvent) => void) | undefined;
	readonly #botUserId: string | undefined;
	readonly #turnBook: OriginTurnBook;
	readonly #modality: ModalityRegistry;
	readonly #speakers = new Map<string, SpeakerEntry>();
	readonly #reconnects = new Map<string, ReconnectEntry>();
	readonly #retryAttempts = new Map<string, number>();
	readonly #ingressCounters = new Map<string, { accepted: number; dropped: number; rejectedClosed: number }>();
	readonly #seenSpeakers = new Set<string>();
	readonly #modalityIds = new Set<string>();
	readonly #timers = new Set<LiveTimer>();
	readonly #pendingOpens = new Set<Promise<SttStream | undefined>>();

	#state: VoiceSessionState = "idle";
	#closeReason: VoiceCloseReason | undefined;
	#closePromise: Promise<void> | undefined;
	#admissionClosed = false;
	#fatalUntilRejoin = false;
	#reopened = false;
	#connection: VoiceConnectionLike | undefined;
	#receiver: VoiceReceiverLike | undefined;
	#receiverOff: (() => void) | undefined;
	#connectionOff: (() => void) | undefined;
	#connectionDestroyed = false;
	#player: VoicePlayerLike | undefined;
	#playerAborted = false;
	#currentSynthesis: VoiceSynthesisLike | undefined;
	#synthesisAborted = false;
	#drillLogClosed = false;
	#idleTimer: LiveTimer | undefined;
	#sessionTimer: LiveTimer | undefined;
	#startedAtMs = 0;
	#lastActivityMs = 0;

	constructor(options: VoiceRoomSessionOptions) {
		this.config = options.config;
		this.channelId = options.channelId;
		this.guildId = options.guildId;
		this.originKey = options.originKey ?? `discord/channel/${options.channelId}`;
		this.#botUserId = options.botUserId;
		this.#connectionFactory = options.connectionFactory;
		this.#playerFactory = options.playerFactory;
		this.#sttProvider = options.sttProvider;
		this.#clock = options.clock ?? defaultClock;
		this.#drillLogSink = options.drillLogSink;
		this.#onProviderFatal = options.onProviderFatal;
		this.#receiverFactory = options.receiverFactory;
		this.#decoderFactory = options.decoderFactory;
		this.#onUtterance = options.onUtterance;
		this.#onIngress = options.onIngress;
		this.#turnBook = options.turnBook ?? new OriginTurnBook(options.config.outstanding.maxEntries);
		this.#modality = options.modality ?? new ModalityRegistry();
	}

	get state(): VoiceSessionState {
		return this.#state;
	}

	get closeReason(): VoiceCloseReason | undefined {
		return this.#closeReason;
	}

	get activeStreams(): number {
		return this.#speakers.size;
	}
	get ingressCounters(): ReadonlyMap<string, VoiceIngressCounters> {
		return new Map(
			[...this.#ingressCounters.entries()].map(([speakerId, counters]) => [speakerId, { ...counters }] as const),
		);
	}

	getSpeakerIngressCounters(speakerId: string): VoiceIngressCounters {
		const counters = this.#ingressCounters.get(speakerId);
		return counters === undefined ? { accepted: 0, dropped: 0, rejectedClosed: 0 } : { ...counters };
	}

	get liveTimerCount(): number {
		return this.#timers.size;
	}

	get timers(): number {
		return this.liveTimerCount;
	}

	get reopened(): boolean {
		return this.#reopened;
	}

	get fatalUntilRejoin(): boolean {
		return this.#fatalUntilRejoin;
	}

	get turnBook(): OriginTurnBook {
		return this.#turnBook;
	}

	/** The room's playback handle, present once the runtime built one for this room. */
	get playback(): VoiceRoomPlayback | undefined {
		return this.#player?.playback;
	}

	get modality(): ModalityRegistry {
		return this.#modality;
	}

	/** Opens the connection and player once. Repeated joins return the same lifecycle owner. */
	async join(): Promise<this> {
		if (this.#state === "closed") throw new VoiceSessionClosedError();
		if (this.#state !== "idle") return this;
		this.#transition("joining");
		this.#startedAtMs = this.#clock.now();
		this.#lastActivityMs = this.#startedAtMs;
		await this.#performJoin();
		return this;
	}

	async #performJoin(): Promise<void> {
		try {
			const connection = await this.#connectionFactory({ channelId: this.channelId, guildId: this.guildId });
			if (this.#admissionClosed) {
				await invokeSafely(() => connection.destroy());
				return;
			}
			this.#connection = connection;
			if (this.#receiverFactory !== undefined) {
				this.#receiver = await this.#receiverFactory(connection);
				this.#attachReceiverSignals();
			}
			const off = connection.onStateChange?.((state) => this.handleConnectionState(state));
			this.#connectionOff = typeof off === "function" ? off : undefined;
			const player = await this.#playerFactory(connection, { channelId: this.channelId, guildId: this.guildId });
			if (this.#admissionClosed) {
				this.#player = player;
				await this.#abortPlayback();
				await this.#destroyConnection();
				return;
			}
			this.#player = player;
			this.#scheduleLifecycleTimers();
			this.#transition("active");
		} catch (error) {
			if (!this.#admissionClosed) await this.close("connection_lost");
			throw error;
		}
	}
	#attachReceiverSignals(): void {
		const speaking = this.#receiver?.speaking;
		if (speaking === undefined) return;
		const onStart = (speakerId: string): void => {
			if (speakerId === this.#botUserId || this.#admissionClosed) return;
			void this.openSpeaker(speakerId).catch(() => undefined);
		};
		speaking.on("start", onStart);
		const remove = speaking.off ?? speaking.removeListener;
		if (remove !== undefined) this.#receiverOff = () => remove.call(speaking, "start", onStart);
	}

	/**
	 * Opens one speaker's bounded STT stream. A stream supplied by a decoder binding is accepted
	 * directly; otherwise the injected provider is opened with the fixed 16 kHz STT contract.
	 */
	async openSpeaker(speakerId: string, resources: VoiceSpeakerResources = {}): Promise<SttStream | undefined> {
		if (speakerId.length === 0 || this.#admissionClosed || this.#fatalUntilRejoin) return undefined;
		if (this.#state !== "joining" && this.#state !== "active") return undefined;
		const existing = this.#speakers.get(speakerId);
		if (existing !== undefined) return existing.stream;
		const operation = this.#performOpenSpeaker(speakerId, resources);
		this.#pendingOpens.add(operation);
		try {
			return await operation;
		} finally {
			this.#pendingOpens.delete(operation);
		}
	}

	async #performOpenSpeaker(
		speakerId: string,
		resources: VoiceSpeakerResources,
		previousText?: string,
	): Promise<SttStream | undefined> {
		let receive = resources.receive;
		let decoder = resources.decoder;
		let subscription = resources.subscription;
		let createdReceive = false;
		try {
			if (
				receive === undefined &&
				resources.stream === undefined &&
				this.#receiver !== undefined &&
				this.#decoderFactory !== undefined
			) {
				const receiveDecoder = await this.#decoderFactory();
				decoder = receiveDecoder;
				receive = createSpeakerReceiver({
					receiver: this.#receiver,
					speakerId,
					decoder: receiveDecoder,
					clock: this.#clock,
					energyGate: this.config.energyGate,
					silenceEndMs: this.config.silenceEndMs,
					maxQueuedFrames: this.config.ingress.maxQueuedFramesPerSpeaker,
					onAdmission: (event) => this.#recordIngress(event),
					onUtterance: (utterance) => this.#onUtterance?.(utterance),
					sttMachine: {
						speakerId,
						sampleRateHz: VOICE_STT_SAMPLE_RATE_HZ,
						maxQueuedFrames: this.config.ingress.maxQueuedFramesPerSpeaker,
						transcriptWaitMs: this.config.transcriptWaitMs,
						reconnectInitialBackoffMs: this.config.reconnect.initialBackoffMs,
						reconnectMaxBackoffMs: this.config.reconnect.maxBackoffMs,
						reconnectMaxAttempts: this.config.reconnect.maxAttempts,
					},
				});
				subscription = receive.subscription;
				createdReceive = true;
			}
		} catch (error) {
			if (!this.#admissionClosed) void this.close("provider_fatal");
			throw error;
		}

		receive?.providerOpened();
		let pendingRetryable: Extract<SttEvent, { readonly kind: "retryable" }> | undefined;
		const sink = (event: SttEvent): void => {
			receive?.handleSttEvent(event);
			if (
				event.kind === "retryable" &&
				this.#speakers.get(speakerId) === undefined &&
				this.#reconnects.get(speakerId) === undefined
			) {
				pendingRetryable = event;
				return;
			}
			this.#handleSttEvent(speakerId, event);
		};
		let stream: SttStream | undefined = resources.stream;
		try {
			stream ??= await this.#sttProvider.open(
				{
					speakerId,
					sampleRateHz: VOICE_STT_SAMPLE_RATE_HZ,
					maxQueuedFrames: this.config.ingress.maxQueuedFramesPerSpeaker,
					...(previousText === undefined ? {} : { previousText }),
				},
				sink,
			);
		} catch (error) {
			if (createdReceive) await receive?.close("provider_fatal");
			if (!this.#admissionClosed) void this.close("provider_fatal");
			throw error;
		}

		if (stream === undefined) {
			if (createdReceive) await receive?.close("provider_fatal");
			return undefined;
		}
		if (this.#admissionClosed || this.#fatalUntilRejoin) {
			await this.#closeRawStream(stream);
			if (createdReceive) await receive?.close("teardown");
			await this.#releaseResources({ ...resources, subscription, decoder, receive });
			return undefined;
		}

		const duplicate = this.#speakers.get(speakerId);
		if (duplicate !== undefined) {
			await this.#closeRawStream(stream);
			if (createdReceive) await receive?.close("teardown");
			await this.#releaseResources({ ...resources, subscription, decoder, receive });
			return duplicate.stream;
		}

		receive?.attachStream(stream);
		const entry: SpeakerEntry = {
			speakerId,
			stream,
			subscription,
			decoder,
			receive,
			closed: false,
			committed: false,
			released: false,
			lastCommittedText: previousText,
		};
		this.#speakers.set(speakerId, entry);
		if (this.#seenSpeakers.has(speakerId)) this.#reopened = true;
		this.#seenSpeakers.add(speakerId);
		if (pendingRetryable !== undefined) void this.#startReconnect(speakerId);
		return stream;
	}

	#handleSttEvent(speakerId: string, event: SttEvent): void {
		const entry = this.#speakers.get(speakerId) ?? this.#reconnects.get(speakerId)?.entry;
		if (event.kind === "committed" && entry !== undefined) entry.lastCommittedText = event.text;
		if (event.kind === "ready") this.#retryAttempts.delete(speakerId);
		if (event.kind === "fatal") {
			this.#fatalCode = event.code;
			this.#fatalMessage = event.message;
			void this.close("provider_fatal");
			return;
		}
		if (event.kind === "retryable") void this.#startReconnect(speakerId);
	}

	#recordIngress(event: VoiceReceiveIngressEvent): void {
		const previous = this.#ingressCounters.get(event.speakerId) ?? { accepted: 0, dropped: 0, rejectedClosed: 0 };
		const next = { ...previous };
		if (event.admission === "accepted") next.accepted += 1;
		if (event.admission === "dropped_oldest") next.dropped += 1;
		if (event.admission === "rejected_closed") {
			next.dropped += 1;
			next.rejectedClosed += 1;
		}
		this.#ingressCounters.set(event.speakerId, next);
		this.#onIngress?.(event);
	}

	async #startReconnect(speakerId: string): Promise<void> {
		if (this.#admissionClosed || this.#reconnects.has(speakerId)) return;
		const entry = this.#speakers.get(speakerId);
		if (entry === undefined || entry.closed) return;
		const attempt = (this.#retryAttempts.get(speakerId) ?? 0) + 1;
		this.#retryAttempts.set(speakerId, attempt);
		const reconnect: ReconnectEntry = {
			entry,
			previousText: entry.lastCommittedText,
			started: false,
		};
		this.#reconnects.set(speakerId, reconnect);
		entry.receive?.detachStream();
		await this.#closeSpeakerEntry(entry, "retryable", false);
		if (this.#speakers.get(speakerId) === entry) this.#speakers.delete(speakerId);
		if (attempt > this.config.reconnect.maxAttempts) {
			void this.close("provider_fatal");
			return;
		}
		const delayMs = Math.min(
			this.config.reconnect.maxBackoffMs,
			this.config.reconnect.initialBackoffMs * 2 ** (attempt - 1),
		);
		reconnect.timer = this.#scheduleTimer(delayMs, () => {
			void this.#reopenSpeaker(speakerId, reconnect).catch(() => undefined);
		});
	}

	async #reopenSpeaker(speakerId: string, reconnect: ReconnectEntry): Promise<void> {
		if (reconnect.started) return;
		reconnect.started = true;
		if (this.#admissionClosed || this.#fatalUntilRejoin) return;
		await this.#performOpenSpeaker(
			speakerId,
			{
				subscription: reconnect.entry.subscription,
				decoder: reconnect.entry.decoder,
				receive: reconnect.entry.receive,
			},
			reconnect.previousText,
		);
		if (this.#reconnects.get(speakerId) === reconnect) this.#reconnects.delete(speakerId);
	}

	push(speakerId: string, pcm16: Uint8Array): Admission {
		const atMs = this.#clock.now();
		if (this.#admissionClosed) {
			const admission = "rejected_closed" as const;
			this.#recordIngress({ speakerId, admission, ingress: "dropped", atMs });
			return admission;
		}
		const entry = this.#speakers.get(speakerId);
		if (entry === undefined || entry.closed) {
			const admission = "rejected_closed" as const;
			this.#recordIngress({ speakerId, admission, ingress: "dropped", atMs });
			return admission;
		}
		const admission = entry.stream.push(pcm16);
		this.#recordIngress({
			speakerId,
			admission,
			ingress: admission === "accepted" ? "recorded" : "dropped",
			atMs,
		});
		this.#lastActivityMs = atMs;
		this.#scheduleIdleTimer();
		return admission;
	}

	async removeSpeaker(speakerId: string): Promise<void> {
		const reconnect = this.#reconnects.get(speakerId);
		if (reconnect !== undefined) {
			this.#clearTimer(reconnect.timer);
			this.#reconnects.delete(speakerId);
			await this.#releaseSpeakerResources(reconnect.entry);
		}
		const entry = this.#speakers.get(speakerId);
		if (entry === undefined) return;
		if (this.#speakers.get(speakerId) === entry) this.#speakers.delete(speakerId);
		await this.#closeSpeakerEntry(entry, "speaker_left");
		await this.#releaseSpeakerResources(entry);
	}

	setCurrentSynthesis(synthesis: VoiceSynthesisLike | undefined): void {
		this.#currentSynthesis = synthesis;
		this.#synthesisAborted = false;
	}

	setSynthesis(synthesis: VoiceSynthesisLike | undefined): void {
		this.setCurrentSynthesis(synthesis);
	}

	registerModality(turnId: string, modality: "voice" | "text", atMs = this.#clock.now()): boolean {
		if (this.#admissionClosed) return false;
		const added = this.#modality.register(turnId, modality, atMs);
		if (added) this.#modalityIds.add(turnId);
		return added;
	}

	endModality(turnId: string): boolean {
		this.#modalityIds.delete(turnId);
		return this.#modality.end(turnId);
	}

	admitTurn(
		entry: Omit<Parameters<OriginTurnBook["admit"]>[0], "seq">,
		nowMs = this.#clock.now(),
	): "tracked" | "saturated" {
		return this.#turnBook.admit(entry, nowMs);
	}

	settleTurn(turnId: string, nowMs = this.#clock.now()): void {
		this.#turnBook.settle(turnId, nowMs);
		this.endModality(turnId);
	}

	writeDrillRecord(record: VoiceDrillRecord): Promise<void> {
		if (this.#drillLogSink === undefined || !this.config.drillLog.enabled || this.#drillLogClosed) {
			return Promise.resolve();
		}
		return Promise.resolve(this.#drillLogSink.write(record));
	}

	/** Voice-state trigger: bot removal wins over empty-room detection. */
	handleVoiceStateChange(change: VoiceStateChangeLike): void {
		const botId = change.botUserId ?? change.botId ?? this.#botUserId;
		const newChannelId = change.newChannelId ?? change.channelId ?? null;
		if (
			change.botRemoved === true ||
			(botId !== undefined && change.userId === botId && newChannelId !== this.channelId)
		) {
			void this.close("kicked");
			return;
		}

		const members = change.members ?? change.channelMembers;
		if (members !== undefined) {
			const nonBotMember = members.some((member) => {
				const memberId = member.userId ?? member.id;
				if (member.bot === true || (botId !== undefined && memberId === botId)) return false;
				return member.channelId === undefined || member.channelId === null || member.channelId === this.channelId;
			});
			if (!nonBotMember) {
				void this.close("empty");
				return;
			}
		}

		if (change.userId !== botId && newChannelId !== this.channelId) void this.removeSpeaker(change.userId);
	}

	handleConnectionState(state: string | { readonly status?: string }): void {
		const value = typeof state === "string" ? state : (state.status ?? "");
		const normalized = value.toLowerCase();
		if (normalized === "disconnected" || normalized === "destroyed") void this.close("connection_lost");
	}

	onConnectionState(state: string | { readonly status?: string }): void {
		this.handleConnectionState(state);
	}

	handleGatewayLost(): void {
		void this.close("gateway_lost");
	}

	handleProviderFatal(): void {
		void this.close("provider_fatal");
	}

	idleTimerTick(): void {
		if (this.#admissionClosed) return;
		const nowMs = this.#clock.now();
		if (nowMs - this.#lastActivityMs >= this.config.idleLeaveMs) {
			void this.close("idle");
		} else {
			this.#scheduleIdleTimer();
		}
	}

	sessionMaxTimerTick(): void {
		if (this.#admissionClosed) return;
		const nowMs = this.#clock.now();
		if (nowMs - this.#startedAtMs >= this.config.sessionMaxMs) {
			void this.close("session_max");
		} else {
			this.#scheduleSessionTimer();
		}
	}

	/** Clears the fatal latch as the explicit signal that a new voice join is intended. */
	/** Exactly one diagnostic per fatal room end, cleared only by an explicit rejoin. */
	#reportProviderFatal(): void {
		if (this.#fatalReported || this.#onProviderFatal === undefined) return;
		this.#fatalReported = true;
		void this.#onProviderFatal({
			originKey: this.originKey,
			channelId: this.channelId,
			code: this.#fatalCode,
			message: this.#fatalMessage,
			at: new Date(this.#clock.now()).toISOString(),
		});
	}

	rejoin(): boolean {
		if (!this.#fatalUntilRejoin) return false;
		this.#fatalUntilRejoin = false;
		this.#fatalReported = false;
		return true;
	}

	/** The sole termination path. The first call wins the reason and teardown promise. */
	close(reason: VoiceCloseReason): Promise<void> {
		if (this.#closePromise !== undefined) return this.#closePromise;
		this.#closeReason = reason;
		this.#admissionClosed = true;
		if (reason === "provider_fatal") {
			this.#fatalUntilRejoin = true;
			this.#reportProviderFatal();
		}
		this.#transition("draining");
		const teardown = this.#teardown();
		this.#closePromise = teardown;
		return teardown;
	}

	async #teardown(): Promise<void> {
		// (1) admission is latched by close() before this method starts.
		// (2) abort active playback before touching speaker streams.
		await this.#abortPlayback();
		await this.#awaitPendingOpens();

		// (3) commit and close every stream exactly once.
		for (const entry of [...this.#speakers.values()]) await this.#closeSpeakerEntry(entry, "teardown");
		// (4) release decoder/subscription resources and connection listeners.
		for (const reconnect of [...this.#reconnects.values()]) await this.#releaseSpeakerResources(reconnect.entry);
		this.#reconnects.clear();
		this.#receiverOff?.();
		this.#receiverOff = undefined;
		this.#receiver = undefined;
		for (const entry of [...this.#speakers.values()]) await this.#releaseSpeakerResources(entry);
		this.#connectionOff?.();
		this.#connectionOff = undefined;
		// (5) destroy the voice connection.
		await this.#destroyConnection();
		// (6) clear all lifecycle timers.
		this.#clearTimers();
		// (7) flush and close drill-log output.
		await this.#closeDrillLog();
		// (8) clear modality and origin-turn state.
		this.#clearStateMaps();
		this.#reopened = false;
		this.#transition("closed");
		// (9) final lifecycle invariant.
		if (this.activeStreams !== 0 || this.liveTimerCount !== 0 || this.#reopened) {
			throw new Error("voice session teardown invariant failed");
		}
	}

	async #awaitPendingOpens(): Promise<void> {
		if (this.#pendingOpens.size === 0) return;
		await Promise.allSettled([...this.#pendingOpens]);
	}

	#scheduleLifecycleTimers(): void {
		this.#scheduleIdleTimer();
		this.#scheduleSessionTimer();
	}

	#scheduleIdleTimer(): void {
		if (this.#admissionClosed) return;
		this.#clearTimer(this.#idleTimer);
		const delayMs = Math.max(0, this.#lastActivityMs + this.config.idleLeaveMs - this.#clock.now());
		this.#idleTimer = this.#scheduleTimer(delayMs, () => this.idleTimerTick());
	}

	#scheduleSessionTimer(): void {
		if (this.#admissionClosed) return;
		this.#clearTimer(this.#sessionTimer);
		const delayMs = Math.max(0, this.#startedAtMs + this.config.sessionMaxMs - this.#clock.now());
		this.#sessionTimer = this.#scheduleTimer(delayMs, () => this.sessionMaxTimerTick());
	}

	#scheduleTimer(delayMs: number, callback: () => void): LiveTimer {
		const timer: LiveTimer = { active: true, hasHandle: false, handle: undefined };
		this.#timers.add(timer);
		const handle = this.#clock.setTimeout(() => {
			if (!timer.active) return;
			timer.active = false;
			this.#timers.delete(timer);
			callback();
		}, delayMs);
		timer.handle = handle;
		timer.hasHandle = true;
		if (!timer.active) this.#timers.delete(timer);
		return timer;
	}

	#clearTimer(timer: LiveTimer | undefined): void {
		if (timer === undefined || !timer.active) return;
		timer.active = false;
		this.#timers.delete(timer);
		if (timer.hasHandle) this.#clock.clearTimeout(timer.handle);
	}

	#clearTimers(): void {
		for (const timer of [...this.#timers]) this.#clearTimer(timer);
		this.#idleTimer = undefined;
		this.#sessionTimer = undefined;
	}

	async #abortPlayback(): Promise<void> {
		if (!this.#playerAborted && this.#player !== undefined) {
			this.#playerAborted = true;
			await invokeSafely(() => this.#player?.abort());
		}
		if (!this.#synthesisAborted && this.#currentSynthesis?.abort !== undefined) {
			this.#synthesisAborted = true;
			await invokeSafely(() => this.#currentSynthesis?.abort?.());
		}
	}

	async #closeSpeakerEntry(
		entry: SpeakerEntry,
		reason: "teardown" | "speaker_left" | "retryable",
		commit = true,
	): Promise<void> {
		if (entry.closePromise !== undefined) {
			await entry.closePromise;
			return;
		}
		entry.closed = true;
		entry.closePromise = (async () => {
			if (commit && !entry.committed) {
				entry.committed = true;
				await invokeSafely(() => entry.stream.commit(reason === "retryable" ? "teardown" : reason));
			}
			await invokeSafely(() => entry.stream.close(reason));
		})();
		await entry.closePromise;
	}

	async #closeRawStream(stream: SttStream): Promise<void> {
		await invokeSafely(() => stream.commit("teardown"));
		await invokeSafely(() => stream.close("teardown"));
	}

	async #releaseSpeakerResources(entry: SpeakerEntry): Promise<void> {
		if (entry.released) return;
		entry.released = true;
		const receive = entry.receive;
		if (receive !== undefined) await invokeSafely(() => receive.close("teardown"));
		await this.#releaseResources({ subscription: entry.subscription, decoder: entry.decoder });
	}

	async #releaseResources(resources: VoiceSpeakerResources): Promise<void> {
		if (resources.subscription !== undefined)
			await invokeReleasable(resources.subscription, ["unsubscribe", "release", "close", "destroy"]);
		if (resources.decoder !== undefined) await invokeReleasable(resources.decoder, ["release", "close", "destroy"]);
	}

	async #destroyConnection(): Promise<void> {
		if (this.#connectionDestroyed || this.#connection === undefined) return;
		this.#connectionDestroyed = true;
		await invokeSafely(() => this.#connection?.destroy());
	}

	async #closeDrillLog(): Promise<void> {
		if (this.#drillLogClosed) return;
		this.#drillLogClosed = true;
		if (this.#drillLogSink === undefined || !this.config.drillLog.enabled) return;
		await invokeSafely(() => this.#drillLogSink?.flush());
		await invokeSafely(() => this.#drillLogSink?.close());
	}

	#clearStateMaps(): void {
		this.#speakers.clear();
		for (const turnId of this.#modalityIds) this.#modality.end(turnId);
		this.#modalityIds.clear();
		while (this.#turnBook.isBusy()) this.#turnBook.settle("", this.#clock.now());
	}

	#transition(next: VoiceSessionState): void {
		if (STATE_RANK[next] < STATE_RANK[this.#state]) return;
		this.#state = next;
	}
}

export interface VoiceJoinRequest extends VoiceConnectionOptions {
	readonly originKey: string;
	readonly botUserId?: string;
	readonly rejoin?: boolean;
}

export interface VoiceSessionManagerOptions {
	readonly config: VoiceConfig;
	readonly connectionFactory: VoiceConnectionFactory;
	readonly playerFactory: VoicePlayerFactory;
	readonly sttProvider: SttProvider;
	readonly receiverFactory?: VoiceReceiverFactory;
	readonly decoderFactory?: VoiceReceiveDecoderFactory;
	readonly onUtterance?: (utterance: VoiceReceiveUtterance) => void | Promise<void>;
	readonly onIngress?: (event: VoiceReceiveIngressEvent) => void;
	readonly clock?: VoiceClock;
	readonly drillLogSink?: VoiceDrillLogSink | ((request: VoiceJoinRequest) => VoiceDrillLogSink | undefined);
	readonly onProviderFatal?: (info: VoiceProviderFatalInfo) => void | Promise<void>;
	readonly sessionFactory?: (options: VoiceRoomSessionOptions) => VoiceRoomSession;
}

/** Keeps one lifecycle owner per canonical origin key. */
export class VoiceSessionManager {
	readonly config: VoiceConfig;
	readonly #options: VoiceSessionManagerOptions;
	readonly #sessions = new Map<string, VoiceRoomSession>();

	constructor(options: VoiceSessionManagerOptions) {
		this.config = options.config;
		this.#options = options;
	}

	get(originKey: string): VoiceRoomSession | undefined {
		return this.#sessions.get(originKey);
	}

	async join(request: VoiceJoinRequest): Promise<VoiceRoomSession> {
		if (!this.config.enabled) throw new Error("voice is disabled");
		if (request.originKey.length === 0) throw new TypeError("originKey must be non-empty");
		const existing = this.#sessions.get(request.originKey);
		if (existing !== undefined && existing.state !== "closed") return existing;
		if (existing?.state === "closed" && existing.fatalUntilRejoin && request.rejoin !== true) {
			throw new VoiceRejoinRequiredError();
		}
		if (existing?.state === "closed" && request.rejoin === true) existing.rejoin();

		const sink =
			typeof this.#options.drillLogSink === "function"
				? this.#options.drillLogSink(request)
				: this.#options.drillLogSink;
		const sessionOptions: VoiceRoomSessionOptions = {
			config: this.config,
			originKey: request.originKey,
			channelId: request.channelId,
			guildId: request.guildId,
			botUserId: request.botUserId,
			connectionFactory: this.#options.connectionFactory,
			playerFactory: this.#options.playerFactory,
			sttProvider: this.#options.sttProvider,
			receiverFactory: this.#options.receiverFactory,
			decoderFactory: this.#options.decoderFactory,
			onUtterance: this.#options.onUtterance,
			onIngress: this.#options.onIngress,
			clock: this.#options.clock,
			drillLogSink: sink,
			onProviderFatal: this.#options.onProviderFatal,
		};
		const session = this.#options.sessionFactory?.(sessionOptions) ?? new VoiceRoomSession(sessionOptions);
		this.#sessions.set(request.originKey, session);
		try {
			await session.join();
			return session;
		} catch (error) {
			if (this.#sessions.get(request.originKey) === session) this.#sessions.delete(request.originKey);
			throw error;
		}
	}

	async rejoin(request: VoiceJoinRequest): Promise<VoiceRoomSession> {
		return this.join({ ...request, rejoin: true });
	}

	async leave(originKey: string): Promise<void> {
		const session = this.#sessions.get(originKey);
		if (session === undefined || session.state === "closed") return;
		await session.close("command");
	}

	/** Live rooms, so a delivery can be matched to whichever room owns its turn. */
	get live(): readonly VoiceRoomSession[] {
		return [...this.#sessions.values()];
	}

	async leaveAll(reason: VoiceCloseReason): Promise<void> {
		await Promise.all([...this.#sessions.values()].map((session) => session.close(reason)));
	}
}

async function invokeSafely(action: () => void | Promise<void>): Promise<void> {
	try {
		await action();
	} catch {
		// Teardown is best effort per resource; later resources still have to be released.
	}
}

type ReleasableMethod = "unsubscribe" | "release" | "close" | "destroy";

async function invokeReleasable(
	resource: VoiceReleasableResource,
	methods: readonly ReleasableMethod[],
): Promise<void> {
	for (const name of methods) {
		const method = resource[name];
		if (typeof method !== "function") continue;
		await invokeSafely(() => method.call(resource));
		return;
	}
}
