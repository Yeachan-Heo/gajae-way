/**
 * Speaker attribution for the turn header.
 *
 * Adapters send both a per-surface display name (`authorName`) and the raw
 * platform handle (`authorHandle`). The persona should address people by the
 * name the room shows, so the display name leads; the handle is appended only
 * when it differs, which keeps identity recoverable without making the header
 * the place a nickname gets lost.
 */

export type SpeakerEngagement =
	| {
			readonly authorId?: string;
			readonly authorName?: string;
			readonly authorHandle?: string;
	  }
	| undefined;

export function composeSpeakerLabel(engagement: SpeakerEngagement): string | undefined {
	const displayName = nonBlank(engagement?.authorName);
	const handle = nonBlank(engagement?.authorHandle);

	if (!displayName) {
		// No usable name: fall back to the handle before the opaque id, since a
		// handle is still something a human can look up.
		return handle ?? nonBlank(engagement?.authorId);
	}
	return handle && handle !== displayName ? `${displayName} (@${handle})` : displayName;
}

function nonBlank(value: string | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
