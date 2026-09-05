import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export function decideEngagement(
	origin: {
		readonly platform: string;
		readonly kind: string;
		readonly conversationId: string;
		readonly parentId?: string;
	},
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
	const configured = channelPolicy(origin, config);
	const gate = configured?.engagement ?? "closed";
	if (engagement.authorIsBot) {
		// Bot collaboration is always bounded by the existing author allowlist. Per-channel
		// botEngagement widens WHICH messages from trusted bots engage, never WHICH bots.
		if (!authorAllowed(engagement.authorId, config)) return { engaged: false };
		if (configured?.botEngagement === "open") return { engaged: true };
		if (
			configured?.botEngagement === "reply-or-mention" &&
			(engagement.mentioned || engagement.replyTo?.fromSelf === true)
		)
			return { engaged: true };
		return { engaged: engagement.mentioned };
	}
	if (gate === "open") return { engaged: true };
	if (!engagement.mentioned) return { engaged: false };
	// `open-mention-only`: any human may address the persona explicitly.
	if (gate === "open-mention-only") return { engaged: true };
	return { engaged: authorAllowed(engagement.authorId, config) };
}

function channelPolicy(
	origin: { readonly platform: string; readonly conversationId: string; readonly parentId?: string },
	config: GatewayConfig,
) {
	const byId = (conversationId: string) =>
		config.channels?.[`${origin.platform}:${conversationId}`] ??
		(origin.platform === "discord" ? config.channels?.[conversationId] : undefined);
	const direct = byId(origin.conversationId);
	const parent = origin.platform === "discord" && origin.parentId ? byId(origin.parentId) : undefined;
	return parent || direct ? { ...parent, ...direct } : undefined;
}

function authorAllowed(authorId: string, config: GatewayConfig): boolean {
	const allowlist = config.mentionAllowlist;
	if (!allowlist || allowlist.length === 0) {
		const ownerId = ownerPeerId(config);
		return ownerId !== undefined && authorId === ownerId;
	}
	return allowlist.includes(authorId);
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
