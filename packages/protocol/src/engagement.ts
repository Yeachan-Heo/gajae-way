export const ENGAGEMENT_MODES = ["open", "mention-open", "closed"] as const;
export type EngagementMode = (typeof ENGAGEMENT_MODES)[number];

export const ENGAGEMENT_AUDIENCES = ["all", "human-only", "bot-only"] as const;
export type EngagementAudience = (typeof ENGAGEMENT_AUDIENCES)[number];

/** Per-channel engagement policy shared by gateway and platform adapters. */
export interface ChannelEngagementPolicy {
	/** Unset remains the safe `closed` gateway default. */
	readonly engagement?: EngagementMode;
	/** Unset preserves historical `open` behavior: humans use the mode, bots use the closed gate. */
	readonly audience?: EngagementAudience;
}

export interface ChannelEngagementInput {
	readonly policy?: ChannelEngagementPolicy;
	readonly authorIsBot: boolean;
	/** A real platform mention or a native reply addressed to this bot/session. */
	readonly addressed: boolean;
	/** Existing owner/allowlist authorization used by the closed gate. */
	readonly authorized: boolean;
}

export interface ChannelEngagementDecision {
	readonly engaged: boolean;
	/** True only when a bot was admitted by an explicitly widened audience rather than the closed gate. */
	readonly botAudienceAdmission: boolean;
}

/**
 * Canonical channel policy evaluation.
 *
 * An explicit `human-only`/`bot-only` audience is a strict exclusion filter:
 * authors outside the audience are declined, never routed to the closed gate.
 * An omitted audience preserves legacy behavior: humans use the mode while
 * bots fall back to the closed mention-and-allowlist gate. `closed` always
 * ignores audience and uses the closed gate for every author.
 */
export function evaluateChannelEngagement(input: ChannelEngagementInput): ChannelEngagementDecision {
	const mode = input.policy?.engagement ?? "closed";
	const audience = input.policy?.audience;
	if (mode === "closed") {
		return { engaged: input.addressed && input.authorized, botAudienceAdmission: false };
	}
	if (audience === "bot-only" || audience === "human-only") {
		const audienceMatches = audience === "bot-only" ? input.authorIsBot : !input.authorIsBot;
		if (!audienceMatches) return { engaged: false, botAudienceAdmission: false };
		const engaged = mode === "open" || input.addressed;
		return { engaged, botAudienceAdmission: engaged && input.authorIsBot };
	}
	if (audience === "all") {
		const engaged = mode === "open" || input.addressed;
		return { engaged, botAudienceAdmission: engaged && input.authorIsBot };
	}
	// No explicit audience: humans use the mode, bots use the closed gate.
	if (input.authorIsBot) {
		return { engaged: input.addressed && input.authorized, botAudienceAdmission: false };
	}
	return { engaged: mode === "open" || input.addressed, botAudienceAdmission: false };
}
