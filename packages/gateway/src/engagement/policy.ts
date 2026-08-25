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
	return { engaged: engagement.mentioned || configured?.engagement === "open" };
}
