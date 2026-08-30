import type {
	ChatContextEntry,
	ChatContextParams,
	ChatContextResult,
	ChatTurnEndPayload,
	OriginRef,
	EngagementContext as ProtocolEngagementContext,
} from "@gajaeway/protocol";
import {
	buildVoiceUnreadPayload,
	type HeldVoiceUtterance,
	type MergeUtterance,
	mergeUtterances,
	type OriginTurnBook,
	shouldAdmitTurn,
	type EngagementContext as VoiceEngagementContext,
	type VoiceUnreadPayload,
} from "@gajaeway/voice-core";
import type { VoiceConfig } from "../config";

/** The subset of the adapter clock needed by the bridge. */
export interface VoiceBridgeClock {
	now(): number;
}

/** The gateway response fields needed to decide whether a voice turn was accepted. */
export interface VoiceTurnAdmission {
	readonly engaged?: boolean;
	readonly turnId?: string | null;
	readonly admit?: boolean;
}

export interface VoiceTurnBridgeOptions {
	readonly config: Pick<VoiceConfig, "mergeWindowMs" | "unread">;
	/** Canonical origin used by both chat.send and chat.context. */
	readonly origin?: OriginRef;
	/** Opaque origin identity retained for callers that own several bridges. */
	readonly originKey?: string;
	readonly voiceChannelId: string;
	readonly turnBook: OriginTurnBook;
	readonly submitTurn: (
		text: string,
		engagement: ProtocolEngagementContext,
		messageId: string,
	) => Promise<VoiceTurnAdmission | undefined>;
	readonly submitContext: (params: ChatContextParams) => Promise<ChatContextResult | undefined>;
	readonly clock?: VoiceBridgeClock;
	readonly mergeUtterances?: typeof mergeUtterances;
	readonly buildVoiceUnreadPayload?: typeof buildVoiceUnreadPayload;
	/** Called at most once for every admitted turn id. */
	readonly onTurnStart?: (turnId: string) => void | Promise<void>;
	/** Optional modality mirror used by the delivery router. */
	readonly modality?: {
		register(turnId: string, modality: "voice" | "text", atMs: number): boolean;
		end(turnId: string): boolean;
	};
}

export interface VoiceTurnBridgeResult {
	readonly turnsSubmitted: number;
	readonly turnsAdmitted: number;
	readonly contextSubmitted: number;
	readonly heldEntries: number;
	readonly context?: VoiceUnreadPayload;
}

export interface VoiceTurnBridge {
	readonly originKey: string | undefined;
	handleUtterances(utterances: readonly MergeUtterance[] | MergeUtterance): Promise<VoiceTurnBridgeResult>;
	handleTurnEnd(event: string | Pick<ChatTurnEndPayload, "turnId">): void;
	announceOnce(turnId: string): boolean;
}

const defaultClock: VoiceBridgeClock = { now: () => Date.now() };

function defaultOrigin(voiceChannelId: string): OriginRef {
	return { platform: "discord", kind: "channel", conversationId: voiceChannelId };
}

function coreEngagement(value: VoiceEngagementContext | undefined, speakerId: string): VoiceEngagementContext {
	return value ?? { authorId: speakerId };
}

function protocolEngagement(value: VoiceEngagementContext, speakerId: string): ProtocolEngagementContext {
	const candidate = value as VoiceEngagementContext & Partial<ProtocolEngagementContext>;
	if (typeof candidate.mentioned === "boolean" && typeof candidate.group === "boolean") {
		return candidate as ProtocolEngagementContext;
	}
	return {
		mentioned: false,
		group: true,
		authorId: candidate.authorId || speakerId,
		...(candidate.authorName === undefined ? {} : { authorName: candidate.authorName }),
		...(candidate.authorHandle === undefined ? {} : { authorHandle: candidate.authorHandle }),
		...(candidate.channelLabel === undefined ? {} : { channelLabel: candidate.channelLabel }),
		...(candidate.serverLabel === undefined ? {} : { serverLabel: candidate.serverLabel }),
	};
}

function stableVoiceMessageId(voiceChannelId: string, speakerId: string, endedAtMs: number): string {
	return `voice:${voiceChannelId}:${speakerId}:${endedAtMs}`;
}

function heldUtterance(
	voiceChannelId: string,
	utterance: {
		readonly speakerId: string;
		readonly text: string;
		readonly endedAtMs: number;
		readonly engagement?: VoiceEngagementContext;
	},
): HeldVoiceUtterance {
	return {
		voiceChannelId,
		speakerId: utterance.speakerId,
		endedAtEpochMs: utterance.endedAtMs,
		body: utterance.text,
		engagement: coreEngagement(utterance.engagement, utterance.speakerId),
	};
}

function lastUtterance<T>(values: readonly T[]): T | undefined {
	return values.length === 0 ? undefined : values[values.length - 1];
}

/**
 * Bridges normalized voice utterances to the two gateway ingress ports. The bridge is scoped to
 * one origin: the turn book is consulted before every merged group, while all held groups from
 * one call are submitted as one context batch so the gateway can apply its cap atomically.
 */
export function createVoiceTurnBridge(options: VoiceTurnBridgeOptions): VoiceTurnBridge {
	if (options.voiceChannelId.trim() === "") throw new RangeError("voiceChannelId must not be blank");
	const clock = options.clock ?? defaultClock;
	const origin = options.origin ?? defaultOrigin(options.voiceChannelId);
	const merge = options.mergeUtterances ?? mergeUtterances;
	const buildUnread = options.buildVoiceUnreadPayload ?? buildVoiceUnreadPayload;
	const announced = new Set<string>();

	const bridge: VoiceTurnBridge = {
		originKey: options.originKey,
		handleUtterances: async (utterances) => {
			const batch = Array.isArray(utterances) ? utterances : [utterances];
			if (batch.length === 0) {
				return { turnsSubmitted: 0, turnsAdmitted: 0, contextSubmitted: 0, heldEntries: 0 };
			}
			const merged = merge(batch, options.config.mergeWindowMs);
			const held: HeldVoiceUtterance[] = [];
			let turnsSubmitted = 0;
			let turnsAdmitted = 0;

			for (const turn of merged) {
				if (options.turnBook.isBusy()) {
					for (const utterance of turn.utterances) held.push(heldUtterance(options.voiceChannelId, utterance));
					continue;
				}

				const trigger = lastUtterance(turn.utterances);
				if (trigger === undefined) continue;
				const messageId = stableVoiceMessageId(options.voiceChannelId, trigger.speakerId, trigger.endedAtMs);
				const response = await options.submitTurn(
					turn.text,
					protocolEngagement(coreEngagement(trigger.engagement, trigger.speakerId), trigger.speakerId),
					messageId,
				);
				turnsSubmitted += 1;
				const admission = response ?? {};
				if (!shouldAdmitTurn(admission)) continue;
				const acceptedAtMs = clock.now();
				options.turnBook.admit({ messageId, turnId: admission.turnId, modality: "voice", acceptedAtMs }, acceptedAtMs);
				options.modality?.register(admission.turnId, "voice", acceptedAtMs);
				turnsAdmitted += 1;
				bridge.announceOnce(admission.turnId);
			}

			if (held.length === 0) {
				return { turnsSubmitted, turnsAdmitted, contextSubmitted: 0, heldEntries: 0 };
			}

			const prepared = buildUnread(held, options.config.unread);
			const entries: readonly ChatContextEntry[] = prepared.entries.map((entry) => ({
				...entry,
				engagement: protocolEngagement(entry.engagement, entry.engagement.authorId),
			}));
			const params: ChatContextParams = { origin, entries, cap: options.config.unread };
			await options.submitContext(params);
			return {
				turnsSubmitted,
				turnsAdmitted,
				contextSubmitted: 1,
				heldEntries: held.length,
				context: { ...prepared, entries },
			};
		},
		handleTurnEnd: (event) => {
			const turnId = typeof event === "string" ? event : event.turnId;
			const settledAtMs = clock.now();
			options.turnBook.settle(turnId, settledAtMs);
			options.modality?.end(turnId);
		},
		announceOnce: (turnId) => {
			if (announced.has(turnId)) return false;
			announced.add(turnId);
			void options.onTurnStart?.(turnId);
			return true;
		},
	};
	return bridge;
}
