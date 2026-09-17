import { type EngagementContext, evaluateChannelEngagement, type OriginRef, originKey } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export const MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS = 1;

/**
 * Bounds opt-in bot collaboration per conversation. One bot-authored turn may
 * run through an open/mention-open audience; another cannot run until a human
 * message arrives. Declined messages still enter the context ledger.
 */
export class BotAudienceTurnGuard {
	readonly #counts = new Map<string, number>();

	canAdmit(originKey: string): boolean {
		return (this.#counts.get(originKey) ?? 0) < MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS;
	}

	recordBotAdmission(originKey: string): void {
		this.#counts.set(originKey, (this.#counts.get(originKey) ?? 0) + 1);
	}

	recordHumanMessage(originKey: string): void {
		this.#counts.delete(originKey);
	}
}

export interface EngagementDecision {
	readonly engaged: boolean;
	readonly botAudienceAdmission: boolean;
}

export function decideEngagement(
	origin: Pick<OriginRef, "platform" | "kind" | "conversationId" | "parentId">,
	engagement: EngagementContext | undefined,
	config: GatewayConfig,
	/**
	 * True when the persona is already talking in THIS thread (see
	 * `threadFollowUpEngaged`). A thread is its own origin, so the mention that
	 * opened it is the addressing act for the whole thread: re-mentioning on every
	 * line is noise nobody types, and without this a reply to the persona's own
	 * answer was dropped by the closed/mention-open gate (live, Slack,
	 * 2026-09-17). Authorisation is NOT relaxed: a closed channel still admits
	 * only owner/allowlist authors, and audience rules still decide bots.
	 */
	threadFollowUp = false,
): EngagementDecision {
	if (origin.platform === "loopback") return { engaged: true, botAudienceAdmission: false };
	if (origin.kind === "dm") return { engaged: dmEngaged(engagement, config), botAudienceAdmission: false };
	if (!engagement?.group) return { engaged: false, botAudienceAdmission: false };
	const policy = resolveChannelPolicy(origin, config);
	return evaluateChannelEngagement({
		policy,
		authorIsBot: engagement.authorIsBot === true,
		addressed: engagement.mentioned || (origin.kind === "thread" && threadFollowUp),
		authorized: closedAuthorAuthorized(engagement.authorId, config),
	});
}

/** The inbound-ledger surface the follow-up signal needs; narrowed so tests need no database. */
export interface ThreadEngagementStore {
	originTriggeredTurn(originKey: string): boolean;
	messageTriggeredTurn(originKey: string, messageId: string): boolean;
}

/**
 * Durable evidence that the persona is already talking in THIS thread.
 *
 * Two shapes count, because a Slack thread is entered two different ways:
 * - a mention written inside the thread binds a trigger turn to the thread
 *   origin itself;
 * - a channel mention is answered INTO a new thread rooted at the triggering
 *   message, and that trigger belongs to the CHANNEL origin. A thread's
 *   conversation id is exactly that root's platform message id (`channel:ts`),
 *   so the root is looked up under the parent channel origin.
 *
 * Without the second shape the feature would miss the common case: the persona
 * opens a thread by answering a mention, and the next line in that thread is
 * refused because the thread origin itself had never been triggered (verified
 * live, 2026-09-17).
 */
export function threadFollowUpEngaged(
	origin: Pick<OriginRef, "platform" | "kind" | "conversationId" | "parentId">,
	threadOriginKey: string,
	store: ThreadEngagementStore,
): boolean {
	if (origin.kind !== "thread") return false;
	if (store.originTriggeredTurn(threadOriginKey)) return true;
	if (!origin.parentId) return false;
	const parentKey = originKey({
		platform: origin.platform,
		kind: "channel",
		conversationId: origin.parentId,
	});
	return store.messageTriggeredTurn(parentKey, origin.conversationId);
}

export function resolveChannelPolicy(
	origin: Pick<OriginRef, "platform" | "conversationId" | "parentId">,
	config: GatewayConfig,
) {
	const ids = [origin.conversationId, origin.parentId].filter((id): id is string => id !== undefined);
	for (const id of ids) {
		const namespaced = config.channels?.[`${origin.platform}:${id}`];
		if (namespaced) return namespaced;
		if (origin.platform === "discord") {
			const legacy = config.channels?.[id];
			if (legacy) return legacy;
		}
	}
	return undefined;
}

function closedAuthorAuthorized(authorId: string, config: GatewayConfig): boolean {
	const allowlist = config.mentionAllowlist;
	if (!allowlist || allowlist.length === 0) return ownerPeerId(config) === authorId;
	return allowlist.includes(authorId);
}

function ownerPeerId(config: GatewayConfig): string | undefined {
	const owner = config.ownerTarget?.origin;
	return owner && "peerId" in owner ? (owner as { peerId?: string }).peerId : undefined;
}

/**
 * Direct-message authorisation. Fails closed: an absent policy is `allowlist`,
 * and an absent or empty allowlist narrows to the owner rather than widening to
 * everyone. Unauthorised DMs are still recorded as unread context by the caller.
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
