import { type EngagementContext, evaluateChannelEngagement, type OriginRef, originKey } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

/**
 * Minimum spacing between two bot-authored turns admitted by an opt-in audience
 * in the same conversation, when no human spoke in between.
 *
 * This is a RATE bound, not a per-human-message quota. The hazard the guard
 * exists for is a machine-speed loop: two bots answering each other as fast as
 * the runtime can dispatch. Spacing kills that (at most one admission per
 * window per conversation) without breaking the case the `audience` opt-in was
 * added for — a scheduled driver bot whose follow-up cadence is minutes apart.
 *
 * The previous bound was "one bot-authored turn until a human speaks", which
 * silently starved exactly that case: a 25-minute follow-up cron lost every
 * tick but the first, measured at 3h34m of unanswered addressed mentions in one
 * channel (2026-09-17, live). Keep this window well under the shortest
 * legitimate driver cadence.
 */
export const BOT_AUDIENCE_TURN_COOLDOWN_MS = 5 * 60_000;

/** Minimal durable metadata surface used by the engagement guard. */
export interface BotAudienceTurnStore {
	metaGet(key: string): string | undefined;
	metaSet(key: string, value: string): void;
	metaDelete?(key: string): void;
}

const BOT_AUDIENCE_CHARGE_PREFIX = "bot-audience-charged-at:";
const BOT_AUDIENCE_DECLINES_KEY = "bot-audience-declines";

function botAudienceChargeKey(originKey: string): string {
	return `${BOT_AUDIENCE_CHARGE_PREFIX}${originKey}`;
}

/**
 * Bounds opt-in bot collaboration per conversation: a bot-authored turn admitted
 * by an open/mention-open audience charges its conversation for
 * `BOT_AUDIENCE_TURN_COOLDOWN_MS`, and a further bot-authored trigger inside
 * that window is declined. A human message clears the charge immediately, and
 * so does a turn that settled without an answer — an attempt that delivered
 * nothing cannot have provoked anything, so it must not be paid for.
 *
 * Declined messages still enter the context ledger. Runtime instances receive
 * the gateway's `meta` store so a restart cannot refund a live charge; direct
 * unit tests may omit it for process-local use.
 */
export class BotAudienceTurnGuard {
	readonly #chargedAt = new Map<string, number>();
	readonly #hydrated = new Set<string>();
	readonly #store: BotAudienceTurnStore | undefined;
	readonly #now: () => number;
	#declines: number | undefined;

	constructor(store?: BotAudienceTurnStore, now: () => number = Date.now) {
		this.#store = store;
		this.#now = now;
	}

	canAdmit(originKey: string): boolean {
		const chargedAt = this.#chargeFor(originKey);
		return chargedAt === undefined || this.#now() - chargedAt >= BOT_AUDIENCE_TURN_COOLDOWN_MS;
	}

	recordBotAdmission(originKey: string): void {
		this.#setCharge(originKey, this.#now());
	}

	/** Refund the admission after a terminal turn produced no reply. */
	releaseBotAdmission(originKey: string): void {
		this.#clearCharge(originKey);
	}

	recordHumanMessage(originKey: string): void {
		this.#clearCharge(originKey);
	}

	/** Count operator-visible declines without producing a chat message. */
	recordBotAudienceDecline(): void {
		const next = this.#declineCount() + 1;
		this.#declines = next;
		this.#store?.metaSet(BOT_AUDIENCE_DECLINES_KEY, String(next));
	}

	botAudienceDeclines(): number {
		return this.#declineCount();
	}

	/** Epoch-ms timestamp of the live charge, or undefined when the conversation is free. */
	#chargeFor(originKey: string): number | undefined {
		if (this.#hydrated.has(originKey)) return this.#chargedAt.get(originKey);
		this.#hydrated.add(originKey);
		const raw = this.#store?.metaGet(botAudienceChargeKey(originKey));
		if (raw === undefined) return undefined;
		const parsed = Number(raw);
		const now = this.#now();
		// Corrupt or future-dated durable state fails closed to "charged now": it
		// must not hand an operator's spent budget back as an open audience, and
		// unlike the old unbounded counter it heals itself after one window.
		const chargedAt = Number.isSafeInteger(parsed) && parsed >= 0 && parsed <= now ? parsed : now;
		if (chargedAt === parsed) this.#chargedAt.set(originKey, chargedAt);
		else this.#setCharge(originKey, chargedAt);
		return chargedAt;
	}

	#setCharge(originKey: string, chargedAt: number): void {
		this.#hydrated.add(originKey);
		this.#chargedAt.set(originKey, chargedAt);
		this.#store?.metaSet(botAudienceChargeKey(originKey), String(chargedAt));
	}

	#clearCharge(originKey: string): void {
		this.#hydrated.add(originKey);
		this.#chargedAt.delete(originKey);
		const key = botAudienceChargeKey(originKey);
		if (this.#store?.metaDelete) this.#store.metaDelete(key);
		else this.#store?.metaSet(key, "0");
	}

	#declineCount(): number {
		if (this.#declines !== undefined) return this.#declines;
		const raw = this.#store?.metaGet(BOT_AUDIENCE_DECLINES_KEY);
		const parsed = raw === undefined ? 0 : Number(raw);
		this.#declines = Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
		return this.#declines;
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
