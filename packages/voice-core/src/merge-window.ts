/**
 * Deterministic grouping for utterances that finish close enough to share one turn.
 *
 * Grouping is anchored at the first ending timestamp in each group. This makes the
 * caller-supplied window a real boundary rather than an unbounded sliding chain.
 */

import type { EngagementContext } from "./ports";

/** Input shape accepted from the adapter's utterance boundary. */
export interface MergeUtterance {
	readonly speakerId?: string;
	readonly speakerUserId?: string;
	readonly text?: string;
	readonly transcript?: string;
	readonly body?: string;
	readonly endedAtMs?: number;
	readonly endedAtEpochMs?: number;
	readonly engagement?: EngagementContext;
}

export interface MergedSpeakerLine {
	readonly speakerId: string;
	readonly text: string;
	readonly utterances: readonly NormalizedMergeUtterance[];
	readonly engagement?: EngagementContext;
}

export interface NormalizedMergeUtterance {
	readonly speakerId: string;
	readonly text: string;
	readonly endedAtMs: number;
	readonly engagement?: EngagementContext;
}

export interface MergedVoiceTurn {
	readonly startedAtMs: number;
	readonly endedAtMs: number;
	readonly utterances: readonly NormalizedMergeUtterance[];
	readonly lines: readonly MergedSpeakerLine[];
	readonly text: string;
}

interface IndexedUtterance extends NormalizedMergeUtterance {
	readonly inputIndex: number;
}

function normalizeUtterance(input: MergeUtterance): NormalizedMergeUtterance {
	const speakerId = input.speakerId ?? input.speakerUserId;
	const text = input.text ?? input.transcript ?? input.body;
	const endedAtMs = input.endedAtMs ?? input.endedAtEpochMs;
	if (typeof speakerId !== "string" || speakerId.length === 0) {
		throw new TypeError("utterance speakerId must be a non-empty string");
	}
	if (typeof text !== "string") {
		throw new TypeError("utterance text must be a string");
	}
	if (typeof endedAtMs !== "number" || !Number.isFinite(endedAtMs)) {
		throw new TypeError("utterance endedAtMs must be finite");
	}
	return { speakerId, text, endedAtMs, engagement: input.engagement };
}

function appendText(previous: string, next: string): string {
	if (previous.length === 0) return next;
	if (next.length === 0) return previous;
	return `${previous} ${next}`;
}

function buildTurn(group: readonly IndexedUtterance[]): MergedVoiceTurn {
	const lineBySpeaker = new Map<
		string,
		{ text: string; utterances: NormalizedMergeUtterance[]; engagement?: EngagementContext }
	>();
	for (const utterance of group) {
		const existing = lineBySpeaker.get(utterance.speakerId);
		if (existing === undefined) {
			lineBySpeaker.set(utterance.speakerId, {
				text: utterance.text,
				utterances: [utterance],
				engagement: utterance.engagement,
			});
		} else {
			existing.text = appendText(existing.text, utterance.text);
			existing.utterances.push(utterance);
			if (existing.engagement === undefined && utterance.engagement !== undefined) {
				existing.engagement = utterance.engagement;
			}
		}
	}

	const lines: MergedSpeakerLine[] = [];
	for (const [speakerId, line] of lineBySpeaker) {
		lines.push({
			speakerId,
			text: line.text,
			utterances: line.utterances,
			engagement: line.engagement,
		});
	}
	return {
		startedAtMs: group[0]?.endedAtMs ?? 0,
		endedAtMs: group[group.length - 1]?.endedAtMs ?? 0,
		utterances: group,
		lines,
		text: lines.map((line) => `${line.speakerId}: ${line.text}`).join("\n"),
	};
}

/**
 * Groups chronologically ending utterances into turns. The window is inclusive at its
 * boundary; a later utterance starts a fresh turn and cannot pull an earlier group open.
 */
export function mergeUtterances(
	utterances: readonly MergeUtterance[],
	mergeWindowMs: number,
): readonly MergedVoiceTurn[] {
	if (!Number.isFinite(mergeWindowMs) || mergeWindowMs < 0) {
		throw new RangeError("mergeWindowMs must be finite and non-negative");
	}

	const normalized: IndexedUtterance[] = utterances.map((input, inputIndex) => ({
		...normalizeUtterance(input),
		inputIndex,
	}));
	normalized.sort((left, right) => {
		const timestampDifference = left.endedAtMs - right.endedAtMs;
		return timestampDifference !== 0 ? timestampDifference : left.inputIndex - right.inputIndex;
	});

	const turns: MergedVoiceTurn[] = [];
	let current: IndexedUtterance[] = [];
	let anchorMs: number | undefined;
	for (const utterance of normalized) {
		if (anchorMs === undefined || utterance.endedAtMs - anchorMs > mergeWindowMs) {
			if (current.length > 0) turns.push(buildTurn(current));
			current = [utterance];
			anchorMs = utterance.endedAtMs;
		} else {
			current.push(utterance);
		}
	}
	if (current.length > 0) turns.push(buildTurn(current));
	return turns;
}
