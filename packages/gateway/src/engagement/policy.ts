import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export function decideEngagement(
	origin: { readonly platform: string; readonly kind: string; readonly conversationId: string },
	engagement: EngagementContext | undefined,
	config: GatewayConfig,
): { readonly engaged: boolean } {
	if (origin.platform === "loopback" || origin.kind === "dm") return { engaged: true };
	if (!engagement?.group) return { engaged: false };
	return { engaged: engagement.mentioned || config.channels?.[origin.conversationId]?.engagement === "open" };
}
