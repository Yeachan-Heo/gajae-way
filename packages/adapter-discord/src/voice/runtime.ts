/**
 * Real bindings that turn the pure voice logic and the provider port into a live
 * Discord voice interface.
 *
 * Everything impure lives here: joining a guild voice channel, the native Opus
 * decoder, the provider WebSocket, and the append-only drill log. The lifecycle
 * owner (`VoiceRoomSession`) and the pure logic in `@gajaeway/voice-core` stay
 * free of these details, which is what keeps them testable without audio.
 */

import { appendFile, mkdir } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import type { AudioPlayer } from "@discordjs/voice";
import {
	createAudioPlayer,
	entersState,
	joinVoiceChannel,
	NoSubscriberBehavior,
	VoiceConnectionStatus,
} from "@discordjs/voice";
import type { VoiceDrillRecord } from "@gajaeway/voice-core";
import { serializeDrillRecord } from "@gajaeway/voice-core";
import type { LoadedDiscordAdapterConfig, VoiceConfig } from "../config";
import { voiceOriginKey } from "./commands";
import { createVoicePlayback } from "./playback";
import {
	ElevenLabsSttProvider,
	type ElevenLabsSttSocket,
	type ElevenLabsSttSocketFactoryOptions,
} from "./providers/elevenlabs-stt";
import {
	ElevenLabsTtsProvider,
	type ElevenLabsTtsSocket,
	type ElevenLabsTtsSocketFactoryOptions,
} from "./providers/elevenlabs-tts";
import {
	type VoiceCloseReason,
	type VoiceConnectionLike,
	type VoiceDrillLogSink,
	type VoicePlayerLike,
	type VoiceRoomPlayback,
	VoiceSessionManager,
} from "./session";

/** Discord's voice gateway needs this long to finish signalling before audio flows. */
const VOICE_READY_TIMEOUT_MS = 20_000;

/** The guild lookup the connection factory needs; satisfied by discord.js `Client`. */
/** Posting the fatal diagnostic needs a channel; the text adapter keeps working regardless. */
export interface VoiceChannelSource {
	channels: { fetch(id: string): Promise<unknown> };
}

export interface VoiceGuildSource {
	guilds: {
		fetch(id: string): Promise<{
			readonly id: string;
			readonly voiceAdapterCreator: Parameters<typeof joinVoiceChannel>[0]["adapterCreator"];
		}>;
	};
}

/** A live WebSocket for the provider, using Bun's header-capable constructor. */
export function createElevenLabsSttSocket(options: ElevenLabsSttSocketFactoryOptions): Promise<ElevenLabsSttSocket> {
	return new Promise((resolve, reject) => {
		let socket: WebSocket;
		try {
			socket = new WebSocket(options.url, { headers: options.headers } as unknown as string[]);
		} catch (error) {
			reject(error instanceof Error ? error : new Error(String(error)));
			return;
		}
		const messageListeners: ((payload: unknown) => void)[] = [];
		const errorListeners: ((error: unknown) => void)[] = [];
		const closeListeners: ((reason: unknown) => void)[] = [];
		socket.addEventListener("message", (event) => {
			for (const listener of messageListeners) listener(event);
		});
		socket.addEventListener("error", (event) => {
			for (const listener of errorListeners) listener(event);
		});
		socket.addEventListener("close", (event) => {
			for (const listener of closeListeners) listener({ code: String(event.code), reason: event.reason });
		});
		socket.addEventListener("open", () =>
			resolve({
				send: (payload) => socket.send(payload),
				close: () => socket.close(),
				onMessage: (listener) => messageListeners.push(listener),
				onError: (listener) => errorListeners.push(listener),
				onClose: (listener) => closeListeners.push(listener),
			}),
		);
	});
}

function isSendableChannel(value: unknown): value is { send(text: string): Promise<unknown> } {
	return typeof value === "object" && value !== null && "send" in value && typeof value.send === "function";
}

/** The TTS socket uses the same live WebSocket boundary as the STT socket. */
export function createElevenLabsTtsSocket(options: ElevenLabsTtsSocketFactoryOptions): Promise<ElevenLabsTtsSocket> {
	return createElevenLabsSttSocket(options);
}

/**
 * Subscribes the player to the connection so its audio actually reaches the room.
 * A connection that cannot be subscribed yields no subscription rather than throwing:
 * playback then reports a failure and the reply falls back to text.
 */
function subscribePlayer(connection: VoiceConnectionLike, player: AudioPlayer): { unsubscribe?(): void } | undefined {
	const subscribe = (connection as { subscribe?: (player: AudioPlayer) => { unsubscribe?(): void } | undefined })
		.subscribe;
	return typeof subscribe === "function" ? subscribe.call(connection, player) : undefined;
}

/** Appends one JSON record per line so a drill log survives an abrupt process exit. */
export function createDrillLogSink(config: VoiceConfig): VoiceDrillLogSink | undefined {
	if (!config.drillLog.enabled) return undefined;
	const day = new Date().toISOString().slice(0, 10);
	const directory = isAbsolute(config.drillLog.path) ? config.drillLog.path : join(process.cwd(), config.drillLog.path);
	const file = join(directory, `${day}-discord-live-voice-utterances.jsonl`);
	let pending: Promise<void> = mkdir(directory, { recursive: true }).then(() => undefined);
	return {
		write(record: VoiceDrillRecord): Promise<void> {
			pending = pending.then(() => appendFile(file, `${serializeDrillRecord(record)}\n`, "utf8"));
			return pending;
		},
		flush(): Promise<void> {
			return pending;
		},
		async close(): Promise<void> {
			await pending;
		},
	};
}

export interface VoiceRuntime {
	readonly sessions: VoiceSessionManager;
	/**
	 * Resolves a reply's modality across live rooms. An unknown turn id resolves to
	 * undefined, which is the fail-safe keeping that delivery on the text path: after a
	 * restart the map is empty, so a redelivered reply can never be spoken again.
	 */
	resolveModality(turnId: string): "voice" | undefined;
	/** Records one `[BREAK]` fragment so a multi-part reply is not evicted early. */
	recordDeliveryPart(turnId: string, atMs: number, final?: boolean): boolean;
	/** The playback of the room owning this conversation, when one is live. */
	playbackFor(conversationId: string): VoiceRoomPlayback | undefined;
	/** Closes every live room with one reason, e.g. a lost gateway or process shutdown. */
	closeAll(reason: VoiceCloseReason): Promise<void>;
}

/**
 * Builds the voice runtime, or returns undefined when voice is not configured.
 * A missing credential is a configuration error, not a silent downgrade.
 */
export function createVoiceRuntime(
	config: LoadedDiscordAdapterConfig,
	discord: VoiceGuildSource & Partial<VoiceChannelSource>,
	log: Pick<Console, "error"> = console,
): VoiceRuntime | undefined {
	const voice = config.voice;
	if (!voice?.enabled) return undefined;
	const apiKey = config.elevenLabsApiKey;
	if (apiKey === undefined || apiKey.trim() === "") {
		throw new Error("Voice is enabled but the ElevenLabs credential file produced no key.");
	}

	const sessions = new VoiceSessionManager({
		config: voice,
		connectionFactory: async ({ channelId, guildId }): Promise<VoiceConnectionLike> => {
			if (guildId === undefined) throw new Error("A voice channel outside a guild cannot be joined.");
			const guild = await discord.guilds.fetch(guildId);
			const connection = joinVoiceChannel({
				channelId,
				guildId,
				adapterCreator: guild.voiceAdapterCreator,
				selfDeaf: false,
				selfMute: false,
			});
			await entersState(connection, VoiceConnectionStatus.Ready, VOICE_READY_TIMEOUT_MS);
			return {
				destroy: () => connection.destroy(),
				onStateChange: (listener) => {
					const handler = (_from: unknown, to: { status: VoiceConnectionStatus }) => listener({ status: to.status });
					connection.on("stateChange", handler);
					return () => {
						connection.off("stateChange", handler);
					};
				},
			};
		},
		playerFactory: (connection): VoicePlayerLike => {
			// Pause instead of stopping when nobody listens: the room may briefly empty
			// mid-reply, and the lifecycle owner decides when playback really ends.
			const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Pause } });
			const subscription = subscribePlayer(connection, player);
			const playback = createVoicePlayback({
				config: voice,
				tts: new ElevenLabsTtsProvider({ config: voice, apiKey, socketFactory: createElevenLabsTtsSocket }),
				subscribe: () => subscription,
				playerFactory: () => player as never,
			});
			return {
				abort: () => {
					player.stop(true);
				},
				playback,
			};
		},
		sttProvider: new ElevenLabsSttProvider({ config: voice, apiKey, socketFactory: createElevenLabsSttSocket }),
		// A vendor outage closes voice only. The room is told once, in plain text, why the
		// bot stopped listening and how to bring it back; the text adapter is untouched.
		onProviderFatal: async (info) => {
			const text = `🔇 voice stopped: ${info.code} at ${info.at}. Text still works; run /voice join to try again.`;
			try {
				const channel = await discord.channels?.fetch(info.channelId);
				if (isSendableChannel(channel)) await channel.send(text);
				else log.error(`Voice diagnostic could not be posted to ${info.channelId}: ${text}`);
			} catch (error) {
				log.error(
					`Voice diagnostic could not be posted to ${info.channelId}: ${error instanceof Error ? error.message : String(error)}`,
				);
			}
		},
		drillLogSink: () => createDrillLogSink(voice),
	});

	return {
		sessions,
		resolveModality: (turnId) => {
			for (const session of sessions.live) {
				if (session.modality.resolve(turnId) === "voice") return "voice";
			}
			return undefined;
		},
		recordDeliveryPart: (turnId, atMs, final) => {
			for (const session of sessions.live) {
				if (session.modality.resolve(turnId) === "voice") return session.modality.recordPart(turnId, atMs, final);
			}
			return false;
		},
		playbackFor: (conversationId) => sessions.get(voiceOriginKey(conversationId))?.playback,
		closeAll: (reason) => sessions.leaveAll(reason),
	};
}
