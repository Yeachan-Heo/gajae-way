/**
 * Builds the context-only payload used for held voice utterances.
 *
 * The cap is applied after deterministic chronological ordering, so retaining the latest
 * items is independent of arrival order and same-millisecond speakers remain reproducible.
 */

import type { EngagementContext } from "./ports";

export interface VoiceUnreadCap {
	readonly maxItems: number;
	readonly maxCharsPerItem: number;
}

export interface HeldVoiceUtterance {
	readonly voiceChannelId: string;
	readonly speakerId?: string;
	readonly speakerUserId?: string;
	readonly endedAtEpochMs?: number;
	readonly endedAtMs?: number;
	readonly body?: string;
	readonly text?: string;
	readonly transcript?: string;
	readonly engagement: EngagementContext;
}

export interface VoiceContextEntry {
	readonly messageId: string;
	readonly text: string;
	readonly engagement: EngagementContext;
	readonly at: string;
}

export interface VoiceUnreadPayload {
	readonly entries: readonly VoiceContextEntry[];
	readonly dropped: number;
	readonly truncated: number;
}

interface PreparedEntry {
	readonly messageId: string;
	readonly atMs: number;
	readonly body: string;
	readonly engagement: EngagementContext;
	readonly inputIndex: number;
}

function validateCap(cap: VoiceUnreadCap): void {
	if (!Number.isSafeInteger(cap.maxItems) || cap.maxItems < 1) {
		throw new RangeError("maxItems must be a positive safe integer");
	}
	if (!Number.isSafeInteger(cap.maxCharsPerItem) || cap.maxCharsPerItem < 1) {
		throw new RangeError("maxCharsPerItem must be a positive safe integer");
	}
}

function prepareEntry(input: HeldVoiceUtterance, inputIndex: number): PreparedEntry {
	const speakerId = input.speakerId ?? input.speakerUserId;
	const atMs = input.endedAtEpochMs ?? input.endedAtMs;
	const body = input.body ?? input.text ?? input.transcript;
	if (typeof input.voiceChannelId !== "string" || input.voiceChannelId.length === 0) {
		throw new TypeError("voiceChannelId must be a non-empty string");
	}
	if (typeof speakerId !== "string" || speakerId.length === 0) {
		throw new TypeError("speakerId must be a non-empty string");
	}
	if (typeof atMs !== "number" || !Number.isFinite(atMs)) {
		throw new TypeError("endedAtEpochMs must be finite");
	}
	if (typeof body !== "string") {
		throw new TypeError("voice utterance body must be a string");
	}
	if (input.engagement === undefined) {
		throw new TypeError("voice utterance engagement is required");
	}
	return {
		messageId: `voice:${input.voiceChannelId}:${speakerId}:${atMs}`,
		atMs,
		body,
		engagement: input.engagement,
		inputIndex,
	};
}

function truncateBody(body: string, maxChars: number): { readonly text: string; readonly truncated: boolean } {
	const characters = Array.from(body);
	if (characters.length <= maxChars) return { text: body, truncated: false };
	const visibleCharacters = Math.max(0, maxChars - 1);
	return { text: `${characters.slice(0, visibleCharacters).join("")}…`, truncated: true };
}

/**
 * Creates entries ready for the `chat.context` request. The gateway separately determines
 * its recorded insert count when stable ids are retried, so this payload reports only cap
 * effects that the builder can know: dropped and truncated entries.
 */
export function buildVoiceUnreadPayload(
	utterances: readonly HeldVoiceUtterance[],
	cap: VoiceUnreadCap,
): VoiceUnreadPayload {
	validateCap(cap);

	const prepared = utterances.map((utterance, inputIndex) => prepareEntry(utterance, inputIndex));
	prepared.sort((left, right) => {
		const timeDifference = left.atMs - right.atMs;
		if (timeDifference !== 0) return timeDifference;
		if (left.messageId < right.messageId) return -1;
		if (left.messageId > right.messageId) return 1;
		return left.inputIndex - right.inputIndex;
	});

	const dropped = Math.max(0, prepared.length - cap.maxItems);
	const retained = prepared.slice(dropped);
	let truncated = 0;
	const entries: VoiceContextEntry[] = retained.map((entry) => {
		const body = truncateBody(entry.body, cap.maxCharsPerItem);
		if (body.truncated) truncated += 1;
		return {
			messageId: entry.messageId,
			text: body.text,
			engagement: entry.engagement,
			at: new Date(entry.atMs).toISOString(),
		};
	});

	return { entries, dropped, truncated };
}
