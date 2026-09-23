import { type EngagementContext, evaluateChannelEngagement, type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../config";

/** Rolling window the runaway rate limiter measures bot admissions over. */
export const BOT_AUDIENCE_RATE_WINDOW_MS = 60_000;
/**
 * Bot admissions allowed per origin inside one window when nothing is
 * configured. Collaboration between agents runs far below it; a bot-to-bot
 * reply loop crosses it within seconds, which is exactly what it exists to stop.
 */
export const DEFAULT_BOT_AUDIENCE_TURNS_PER_WINDOW = 30;
/** Bound on the retained admission ids so a long bot conversation cannot grow the meta row without limit. */
const MAX_TRACKED_ADMISSIONS = 64;

/** Durable per-origin state for the bot-audience admission guard. */
export interface BotAudienceTurnState {
	/** Ids of admitted bot turns still eligible for a refund. */
	readonly admissions: readonly string[];
	/** Consecutive bot turns since the last human message. */
	readonly count: number;
	/** Admission timestamps inside the rate window; a human message does NOT clear these. */
	readonly recent: readonly number[];
}

/** Resolved per-origin bot-audience budget. */
export interface BotAudienceLimits {
	/** Consecutive bot turns allowed before a human message is required. Undefined is unlimited. */
	readonly maxConsecutiveTurns?: number;
	/** Bot admissions allowed per origin inside {@link BOT_AUDIENCE_RATE_WINDOW_MS}. Always finite. */
	readonly maxTurnsPerWindow: number;
}

export type BotAudienceDeclineReason = "budget_spent" | "rate_limited";

export type BotAudienceAdmission =
	| { readonly admit: true }
	| { readonly admit: false; readonly reason: BotAudienceDeclineReason };

/** Minimal durable metadata surface used by the engagement guard. */
export interface BotAudienceTurnStore {
	metaGet(key: string): string | undefined;
	metaSet(key: string, value: string): void;
	metaDelete?(key: string): void;
}

const BOT_AUDIENCE_STATE_PREFIX = "bot-audience-state:";
const BOT_AUDIENCE_DECLINES_KEY = "bot-audience-declines";
const BOT_AUDIENCE_RATE_LIMITED_KEY = "bot-audience-rate-limited";

function botAudienceStateKey(originKey: string): string {
	return `${BOT_AUDIENCE_STATE_PREFIX}${originKey}`;
}

const EMPTY_STATE: BotAudienceTurnState = { admissions: [], count: 0, recent: [] };

/**
 * Unreadable state fails closed on the consecutive budget: a corrupt row must
 * not hand out an unbounded run of bot turns. `count` is deliberately large
 * rather than 1 so every configured cap treats it as spent; the next human
 * message clears it.
 */
const UNREADABLE_STATE: BotAudienceTurnState = { admissions: [], count: Number.MAX_SAFE_INTEGER, recent: [] };

function parseState(raw: string | undefined): BotAudienceTurnState {
	if (raw === undefined) return EMPTY_STATE;
	try {
		const parsed: unknown = JSON.parse(raw);
		if (!parsed || typeof parsed !== "object") return UNREADABLE_STATE;
		const value = parsed as { admissions?: unknown; count?: unknown; recent?: unknown };
		const admissions = Array.isArray(value.admissions)
			? value.admissions.filter((entry): entry is string => typeof entry === "string")
			: [];
		const recent = Array.isArray(value.recent)
			? value.recent.filter((entry): entry is number => typeof entry === "number" && Number.isFinite(entry))
			: [];
		if (typeof value.count !== "number" || !Number.isSafeInteger(value.count) || value.count < 0)
			return UNREADABLE_STATE;
		return {
			admissions: admissions.slice(-MAX_TRACKED_ADMISSIONS),
			count: value.count,
			recent,
		};
	} catch {
		return UNREADABLE_STATE;
	}
}

function withinWindow(recent: readonly number[], now: number): readonly number[] {
	return recent.filter((at) => at > now - BOT_AUDIENCE_RATE_WINDOW_MS && at <= now);
}

/**
 * Bounds opt-in bot collaboration per conversation on two independent axes.
 *
 * - A configurable consecutive-turn budget, unlimited unless a channel or the
 *   global default sets one. Multi-agent threads are a normal usage pattern, so
 *   `audience: "all"` means "bot messages are turns", not "one bot turn".
 * - An always-on rolling-window rate limit, which is what actually stops two
 *   bots answering each other forever. It is not reset by a human message.
 *
 * Declined messages still enter the context ledger.
 */
export class BotAudienceTurnGuard {
	readonly #store: BotAudienceTurnStore | undefined;
	readonly #memory = new Map<string, BotAudienceTurnState>();
	#declines: number | undefined;
	#rateLimited: number | undefined;

	constructor(store?: BotAudienceTurnStore) {
		this.#store = store;
	}

	canAdmit(originKey: string, limits: BotAudienceLimits, now = Date.now()): BotAudienceAdmission {
		const state = this.#state(originKey);
		if (limits.maxConsecutiveTurns !== undefined && state.count >= limits.maxConsecutiveTurns)
			return { admit: false, reason: "budget_spent" };
		if (withinWindow(state.recent, now).length >= limits.maxTurnsPerWindow)
			return { admit: false, reason: "rate_limited" };
		return { admit: true };
	}

	/** Consecutive bot turns already admitted for this origin, for operator diagnosis. */
	consecutiveTurns(originKey: string): number {
		return this.#state(originKey).count;
	}

	/** Bot turns admitted for this origin inside the current rate window. */
	windowedTurns(originKey: string, now = Date.now()): number {
		return withinWindow(this.#state(originKey).recent, now).length;
	}

	recordBotAdmission(originKey: string, admissionId?: string, now = Date.now()): void {
		const state = this.#state(originKey);
		if (admissionId !== undefined && state.admissions.includes(admissionId)) return;
		const admissions = admissionId === undefined ? state.admissions : [...state.admissions, admissionId];
		this.#save(originKey, {
			admissions: admissions.slice(-MAX_TRACKED_ADMISSIONS),
			count: state.count + 1,
			recent: [...withinWindow(state.recent, now), now].slice(-MAX_TRACKED_ADMISSIONS),
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
		// The refunded turn produced no reply, so it also gives back its rate slot.
		const recent = state.recent.slice(0, -1);
		if (count === 0 && recent.length === 0) this.#delete(originKey);
		else this.#save(originKey, { admissions, count, recent });
	}

	/** A human message clears the consecutive budget. The runaway rate window survives it. */
	recordHumanMessage(originKey: string, now = Date.now()): void {
		const state = this.#state(originKey);
		const recent = withinWindow(state.recent, now);
		if (recent.length === 0) {
			this.#delete(originKey);
			return;
		}
		if (state.count === 0 && state.admissions.length === 0 && recent.length === state.recent.length) return;
		this.#save(originKey, { admissions: [], count: 0, recent });
	}

	/**
	 * Count operator-visible declines; callers pass false for unaddressed bot
	 * chatter. A rate-limited decline is always counted on its own axis, because
	 * the runaway guard firing is an operational event regardless of addressing.
	 */
	recordBotAudienceDecline(addressed = true, reason: BotAudienceDeclineReason = "budget_spent"): void {
		if (reason === "rate_limited") {
			const limited = this.botAudienceRateLimited() + 1;
			this.#rateLimited = limited;
			this.#store?.metaSet(BOT_AUDIENCE_RATE_LIMITED_KEY, String(limited));
		}
		if (!addressed) return;
		const next = this.botAudienceDeclines() + 1;
		this.#declines = next;
		this.#store?.metaSet(BOT_AUDIENCE_DECLINES_KEY, String(next));
	}

	botAudienceRateLimited(): number {
		if (this.#rateLimited !== undefined) return this.#rateLimited;
		const parsed = Number.parseInt(this.#store?.metaGet(BOT_AUDIENCE_RATE_LIMITED_KEY) ?? "0", 10);
		this.#rateLimited = Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
		return this.#rateLimited;
	}

	botAudienceDeclines(): number {
		if (this.#declines !== undefined) return this.#declines;
		const parsed = Number.parseInt(this.#store?.metaGet(BOT_AUDIENCE_DECLINES_KEY) ?? "0", 10);
		this.#declines = Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
		return this.#declines;
	}

	#state(originKey: string): BotAudienceTurnState {
		if (this.#store) return parseState(this.#store.metaGet(botAudienceStateKey(originKey)));
		return this.#memory.get(originKey) ?? EMPTY_STATE;
	}

	#save(originKey: string, state: BotAudienceTurnState): void {
		const value = JSON.stringify(state);
		if (this.#store) this.#store.metaSet(botAudienceStateKey(originKey), value);
		else this.#memory.set(originKey, state);
	}

	#delete(originKey: string): void {
		if (this.#store?.metaDelete) this.#store.metaDelete(botAudienceStateKey(originKey));
		else if (this.#store) this.#store.metaSet(botAudienceStateKey(originKey), JSON.stringify(EMPTY_STATE));
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

/**
 * Per-origin bot budget: channel entry first, then the global default, then the
 * built-in. Only the rate limit has a built-in value; the consecutive-turn cap
 * is unlimited unless configured.
 */
export function resolveBotAudienceLimits(
	origin: Pick<OriginRef, "platform" | "conversationId" | "parentId">,
	config: GatewayConfig,
): BotAudienceLimits {
	const channel = resolveChannelPolicy(origin, config);
	const maxConsecutiveTurns = channel?.botAudienceMaxConsecutiveTurns ?? config.botAudience?.maxConsecutiveTurns;
	const maxTurnsPerWindow =
		channel?.botAudienceMaxTurnsPerWindow ??
		config.botAudience?.maxTurnsPerWindow ??
		DEFAULT_BOT_AUDIENCE_TURNS_PER_WINDOW;
	return {
		...(maxConsecutiveTurns === undefined ? {} : { maxConsecutiveTurns }),
		maxTurnsPerWindow,
	};
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
