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
	// An explicitly opened channel is a room the persona inhabits: it hears everyone
	// (silence tokens keep it from answering everything), so the allowlist does not
	// gate it. Mention-triggered turns in ordinary group surfaces are commands, and
	// commands are allowlisted: an unlisted author's mention stays context, never a
	// turn (owner directive: prompt-injection posture — non-owners are untrusted).
	if (configured?.engagement === "open") return { engaged: true };
	if (!engagement.mentioned) return { engaged: false };
	const allowlist = config.mentionAllowlist;
	if (allowlist && allowlist.length > 0 && !allowlist.includes(engagement.authorId)) return { engaged: false };
	return { engaged: true };
}
