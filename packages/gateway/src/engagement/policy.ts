import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export function decideEngagement(
	origin: { readonly platform: string; readonly kind: string; readonly conversationId: string },
	engagement: EngagementContext | undefined,
	config: GatewayConfig,
): { readonly engaged: boolean } {
	if (origin.platform === "loopback" || origin.kind === "dm") return { engaged: true };
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
		const owner = config.ownerTarget?.origin;
		const ownerId = owner && "peerId" in owner ? (owner as { peerId?: string }).peerId : undefined;
		return { engaged: ownerId !== undefined && engagement.authorId === ownerId };
	}
	if (!allowlist.includes(engagement.authorId)) return { engaged: false };
	return { engaged: true };
}
