/**
 * Provider-neutral speaker metadata assembly.
 *
 * Discord resolves these values before this package is called. The precedence is
 * deliberately identical to the text adapter: guild nickname, member display
 * name, global name, then handle. Blank values are skipped rather than emitted.
 */

import type { EngagementContext } from "./ports";

export interface SpeakerMetadataInput {
	readonly authorId: string;
	readonly guildNickname?: string | null;
	readonly memberDisplayName?: string | null;
	readonly globalName?: string | null;
	readonly handle?: string | null;
	readonly channelLabel?: string | null;
	readonly serverLabel?: string | null;
}

function firstNonBlank(candidates: readonly (string | null | undefined)[]): string | undefined {
	for (const candidate of candidates) {
		if (typeof candidate === "string" && candidate.trim() !== "") return candidate;
	}
	return undefined;
}

/** Resolves the display value without mutating or trimming the platform value. */
export function resolveSpeakerDisplayName(input: SpeakerMetadataInput): string | undefined {
	return firstNonBlank([input.guildNickname, input.memberDisplayName, input.globalName, input.handle]);
}

/**
 * Builds the same engagement shape used by text turns. Optional labels are kept
 * only when meaningful, so callers never need to special-case whitespace.
 */
export function assembleEngagementContext(input: SpeakerMetadataInput): EngagementContext {
	if (input.authorId.trim() === "") throw new RangeError("authorId must not be blank");
	const authorName = resolveSpeakerDisplayName(input);
	const authorHandle = firstNonBlank([input.handle]);
	const channelLabel = firstNonBlank([input.channelLabel]);
	const serverLabel = firstNonBlank([input.serverLabel]);
	return {
		authorId: input.authorId,
		...(authorName === undefined ? {} : { authorName }),
		...(authorHandle === undefined ? {} : { authorHandle }),
		...(channelLabel === undefined ? {} : { channelLabel }),
		...(serverLabel === undefined ? {} : { serverLabel }),
	};
}
