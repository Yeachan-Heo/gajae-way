import { type EngagementContext, evaluateChannelEngagement, type OriginRef, originKey } from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";

export const MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS = 1;

/** Durable per-origin state for the bot-audience admission guard. */
export interface BotAudienceTurnState {
	readonly admissions: readonly string[];
	readonly count: number;
}

/** Minimal durable metadata surface used by the engagement guard. */
export interface BotAudienceTurnStore {
	metaGet(key: string): string | undefined;
	metaSet(key: string, value: string): void;
	metaDelete?(key: string): void;
}

const BOT_AUDIENCE_STATE_PREFIX = "bot-audience-state:";
const BOT_AUDIENCE_DECLINES_KEY = "bot-audience-declines";

function botAudienceStateKey(originKey: string): string {
	return `${BOT_AUDIENCE_STATE_PREFIX}${originKey}`;
}

function parseState(raw: string | undefined): BotAudienceTurnState {
	if (raw === undefined) return { admissions: [], count: 0 };
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!parsed || typeof parsed !== "object") return { admissions: [], count: MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS };
		const value = parsed as { admissions?: unknown; count?: unknown };
		const admissions = Array.isArray(value.admissions)
			? value.admissions.filter((entry): entry is string => typeof entry === "string")
			: [];
		if (typeof value.count !== "number" || !Number.isSafeInteger(value.count))
			return { admissions: [], count: MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS };
		const count = value.count;
		if (count < 0) return { admissions: [], count: MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS };
		return {
			admissions: admissions.slice(0, MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS),
			count: Math.max(0, Math.min(MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS, count)),
		};
	} catch {
		return { admissions: [], count: MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS };
	}
}

/**
 * Bounds opt-in bot collaboration per conversation. One bot-authored turn may
 * run through an open/mention-open audience; another cannot run until a human
 * message arrives. Declined messages still enter the context ledger.
 */
export class BotAudienceTurnGuard {
	readonly #store: BotAudienceTurnStore | undefined;
	readonly #memory = new Map<string, BotAudienceTurnState>();
	#declines: number | undefined;

	constructor(store?: BotAudienceTurnStore) {
		this.#store = store;
	}

	canAdmit(originKey: string): boolean {
		return this.#state(originKey).count < MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS;
	}

	recordBotAdmission(originKey: string, admissionId?: string): void {
		const state = this.#state(originKey);
		if (admissionId !== undefined && state.admissions.includes(admissionId)) return;
		const admissions = admissionId === undefined ? state.admissions : [...state.admissions, admissionId];
		this.#save(originKey, {
			admissions: admissions.slice(-MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS),
			count: Math.min(MAX_CONSECUTIVE_BOT_AUDIENCE_TURNS, state.count + 1),
		});
	}

	/** Refund only the admission whose terminal reply slot remained unsatisfied. */
	releaseUnansweredAdmission(originKey: string, admissionId?: string): void {
		const state = this.#state(originKey);
		if (state.count === 0) return;
		if (admissionId !== undefined && state.admissions.length > 0 && !state.admissions.includes(admissionId)) return;
		const admissions =
			admissionId === undefined ? state.admissions.slice(1) : state.admissions.filter((id) => id !== admissionId);
		const count = Math.max(0, state.count - 1);
		if (count === 0) this.#delete(originKey);
		else this.#save(originKey, { admissions, count });
	}

	recordHumanMessage(originKey: string): void {
		this.#delete(originKey);
	}

	/** Count operator-visible declines; callers pass false for unaddressed bot chatter. */
	recordBotAudienceDecline(addressed = true): void {
		if (!addressed) return;
		const next = this.botAudienceDeclines() + 1;
		this.#declines = next;
		this.#store?.metaSet(BOT_AUDIENCE_DECLINES_KEY, String(next));
	}

	botAudienceDeclines(): number {
		if (this.#declines !== undefined) return this.#declines;
		const parsed = Number.parseInt(this.#store?.metaGet(BOT_AUDIENCE_DECLINES_KEY) ?? "0", 10);
		this.#declines = Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
		return this.#declines;
	}

	#state(originKey: string): BotAudienceTurnState {
		if (this.#store) return parseState(this.#store.metaGet(botAudienceStateKey(originKey)));
		return this.#memory.get(originKey) ?? { admissions: [], count: 0 };
	}

	#save(originKey: string, state: BotAudienceTurnState): void {
		const value = JSON.stringify(state);
		if (this.#store) this.#store.metaSet(botAudienceStateKey(originKey), value);
		else this.#memory.set(originKey, state);
	}

	#delete(originKey: string): void {
		if (this.#store?.metaDelete) this.#store.metaDelete(botAudienceStateKey(originKey));
		else if (this.#store)
			this.#store.metaSet(botAudienceStateKey(originKey), JSON.stringify({ admissions: [], count: 0 }));
		else this.#memory.delete(originKey);
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
	// An adapter that already knows the room moved past this message records it
	// and asks for nothing more. Checked before every other gate, loopback
	// included, because it is a statement about the message, not the author.
	if (engagement?.contextOnly === true) return { engaged: false, botAudienceAdmission: false };
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
