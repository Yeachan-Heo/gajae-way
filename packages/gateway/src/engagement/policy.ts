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
	// An explicitly opened channel is a room the persona inhabits: it hears every
	// HUMAN (silence tokens keep it from answering everything), so the allowlist
	// does not gate humans there. Bot authors never get that free pass: every bot
	// status/progress/chatter message was burning a full serialized gjc turn, which
	// is what queued real owner messages behind minutes of noise turns (live
	// gajaeway-play finding: 67% of inbound was sibling-bot chatter). A bot must
	// mention us explicitly to get a turn; its message stays recorded as unread
	// conversation context either way. Mention-triggered turns in ordinary group
	// surfaces are commands, and commands are allowlisted: an unlisted author's
	// mention stays context, never a turn (owner directive: prompt-injection
	// posture — non-owners are untrusted).
	if (configured?.engagement === "open" && !engagement.authorIsBot) return { engaged: true };
	if (!engagement.mentioned) return { engaged: false };
	const allowlist = config.mentionAllowlist;
	if (allowlist && allowlist.length > 0 && !allowlist.includes(engagement.authorId)) return { engaged: false };
	return { engaged: true };
}
