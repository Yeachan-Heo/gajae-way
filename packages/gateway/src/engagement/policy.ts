import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export function decideEngagement(
	origin: { readonly platform: string; readonly kind: string; readonly conversationId: string },
	engagement: EngagementContext | undefined,
	config: GatewayConfig,
): { readonly engaged: boolean } {
	// `loopback` is a local, already-trusted origin and stays exempt.
	if (origin.platform === "loopback") return { engaged: true };
	// DMs used to return engaged right here, before the allowlist, before
	// `ownerTarget`, before the channel gate below. That made the private
	// surface the only unauthenticated one: anyone sharing a guild with the
	// bot could reach a full persona turn, with its tool authority, unseen.
	if (origin.kind === "dm") return { engaged: dmEngaged(engagement, config) };
	if (!engagement?.group) return { engaged: false };
	const configured =
		config.channels?.[`${origin.platform}:${origin.conversationId}`] ??
		(origin.platform === "discord" ? config.channels?.[origin.conversationId] : undefined);
	// Three explicit gates. The previous shape had two states and the second one
	// silently changed meaning depending on whether `mentionAllowlist` happened to
	// be populated - a security-relevant setting flipping on the presence of an
	// unrelated field. Unset now means `closed`, the safe default.
	//
	// Bot authors never get the free pass an `open` channel gives humans: every bot
	// status/progress/chatter message was burning a full serialized gjc turn, which
	// queued real owner messages behind minutes of noise (live finding: 67% of
	// inbound was sibling-bot chatter). A bot must mention us to get a turn; its
	// message stays recorded as unread context either way.
	const gate = configured?.engagement ?? "closed";
	if (gate === "open" && !engagement.authorIsBot) return { engaged: true };
	if (!engagement.mentioned) return { engaged: false };
	// `open-mention-only`: anyone may address the persona, but only by addressing it.
	if (gate === "open-mention-only") return { engaged: true };
	// `closed` (and `open` for a bot author): addressed AND authorised. An empty
	// allowlist means owner-only rather than everyone - the previous code fell
	// through to "anyone who mentions us", which is the opposite of failing closed.
	const allowlist = config.mentionAllowlist;
	if (!allowlist || allowlist.length === 0) {
		const ownerId = ownerPeerId(config);
		return { engaged: ownerId !== undefined && engagement.authorId === ownerId };
	}
	if (!allowlist.includes(engagement.authorId)) return { engaged: false };
	return { engaged: true };
}

/**
 * Whether the persona should treat this message as addressed to it. Adapters
 * report the raw `mentioned` fact only; an `open` channel counts as addressed
 * for human authors under the CURRENT config, so an open→closed reload changes
 * the notice on the next turn without an adapter restart.
 */
export function isAddressed(
	origin: { readonly platform: string; readonly kind: string; readonly conversationId: string },
	engagement: Partial<Pick<EngagementContext, "mentioned" | "group" | "authorIsBot">> | undefined,
	config: GatewayConfig,
): boolean {
	if (engagement?.mentioned) return true;
	if (origin.kind === "dm" || origin.platform === "loopback") return true;
	if (!engagement?.group || engagement.authorIsBot) return false;
	const configured =
		config.channels?.[`${origin.platform}:${origin.conversationId}`] ??
		(origin.platform === "discord" ? config.channels?.[origin.conversationId] : undefined);
	return configured?.engagement === "open";
}

function ownerPeerId(config: GatewayConfig): string | undefined {
	const owner = config.ownerTarget?.origin;
	return owner && "peerId" in owner ? (owner as { peerId?: string }).peerId : undefined;
}

/**
 * Direct-message authorisation. Fails closed: an absent policy is `allowlist`,
 * and an absent or empty allowlist narrows to the owner rather than widening to
 * everyone. Unauthorised DMs are still recorded as unread context by the
 * caller - we decline the turn, we do not discard the message.
 */
function dmEngaged(engagement: EngagementContext | undefined, config: GatewayConfig): boolean {
	const policy = config.dmPolicy ?? "allowlist";
	if (policy === "open") return true;
	const authorId = engagement?.authorId;
	if (authorId === undefined) return false;
	const ownerId = ownerPeerId(config);
	if (ownerId !== undefined && authorId === ownerId) return true;
	if (policy === "owner-only") return false;
	const allowlist = config.mentionAllowlist;
	if (!allowlist || allowlist.length === 0) return false;
	return allowlist.includes(authorId);
}
