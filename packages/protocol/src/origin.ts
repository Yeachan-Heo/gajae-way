/**
 * Canonical origin identity (ARCH-007): the PUBLIC, platform-neutral
 * normalization lives here in the protocol so SDK-only adapters can construct
 * canonical OriginRefs. The gateway owns only the binding from an OriginRef
 * to an internal gjc session (never exposed here).
 *
 * One conversational origin == one strictly isolated session (spec fact 9).
 */

export const ORIGIN_PLATFORMS = ["loopback", "discord", "telegram", "slack", "monitor"] as const;
export type OriginPlatform = (typeof ORIGIN_PLATFORMS)[number];

/**
 * The platforms with real messages behind them: the ones an adapter can deliver
 * to, react on, and report reactions from. `loopback` is the owner REPL and
 * `monitor` is event-driven work; neither has a message a persona could react to.
 */
export const CHAT_PLATFORMS = ["discord", "telegram", "slack"] as const;
export type ChatPlatform = (typeof CHAT_PLATFORMS)[number];

export function isChatPlatform(platform: OriginPlatform): platform is ChatPlatform {
	return (CHAT_PLATFORMS as readonly string[]).includes(platform);
}

/** "a discord, telegram or slack origin": the wording every chat-only verb rejects with. */
export function describeChatPlatforms(): string {
	const [last, ...rest] = [...CHAT_PLATFORMS].reverse();
	return `${rest.reverse().join(", ")} or ${last}`;
}

export const ORIGIN_KINDS = ["dm", "channel", "thread", "topic", "loopback", "eventtype"] as const;
export type OriginKind = (typeof ORIGIN_KINDS)[number];

export interface OriginRef {
	readonly platform: OriginPlatform;
	readonly kind: OriginKind;
	/** Platform-scoped stable conversation id (channel id, chat id, "loopback"). */
	readonly conversationId: string;
	/** Present for thread/topic origins: the parent conversation id. */
	readonly parentId?: string;
	/** Present for DM origins: the platform-scoped peer id. */
	readonly peerId?: string;
}

export class OriginRefError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "OriginRefError";
	}
}

const SEGMENT_RE = /^[A-Za-z0-9_.:@+-]+$/;

function requireSegment(value: string, field: string): string {
	if (value.length === 0 || value.length > 256 || !SEGMENT_RE.test(value)) {
		throw new OriginRefError(`invalid origin segment for ${field}: ${JSON.stringify(value)}`);
	}
	return value;
}

/** Validate an OriginRef's structural invariants. Returns the same ref. */
export function validateOriginRef(ref: OriginRef): OriginRef {
	if (!ORIGIN_PLATFORMS.includes(ref.platform)) {
		throw new OriginRefError(`unknown platform: ${String(ref.platform)}`);
	}
	if (!ORIGIN_KINDS.includes(ref.kind)) {
		throw new OriginRefError(`unknown origin kind: ${String(ref.kind)}`);
	}
	requireSegment(ref.conversationId, "conversationId");
	if (ref.kind === "thread" || ref.kind === "topic") {
		if (!ref.parentId) throw new OriginRefError(`${ref.kind} origin requires parentId`);
		requireSegment(ref.parentId, "parentId");
	} else if (ref.kind === "eventtype" && ref.parentId !== undefined) {
		// A monitor's executing session is scoped by its owning monitor id.
		requireSegment(ref.parentId, "parentId");
	} else if (ref.parentId !== undefined) {
		throw new OriginRefError(`${ref.kind} origin must not carry parentId`);
	}
	if (ref.kind === "dm") {
		if (!ref.peerId) throw new OriginRefError("dm origin requires peerId");
		requireSegment(ref.peerId, "peerId");
	} else if (ref.peerId !== undefined) {
		throw new OriginRefError(`${ref.kind} origin must not carry peerId`);
	}
	if (ref.platform === "loopback" && ref.kind !== "loopback") {
		throw new OriginRefError("loopback platform only supports loopback kind");
	}
	if (ref.kind === "loopback" && ref.platform !== "loopback") {
		throw new OriginRefError("loopback kind only valid on loopback platform");
	}
	if (ref.platform === "monitor" && ref.kind !== "eventtype") {
		throw new OriginRefError("monitor platform only supports eventtype kind");
	}
	if (ref.kind === "eventtype" && ref.platform !== "monitor") {
		throw new OriginRefError("eventtype kind only valid on monitor platform");
	}
	// Slack threads are identified by a channel:ts pair; Telegram forum topics are
	// the only `topic` kind, so a slack topic would be a mis-normalized thread.
	if (ref.platform === "slack" && ref.kind === "topic") {
		throw new OriginRefError("slack platform does not support topic kind; use thread");
	}
	return ref;
}

/**
 * THE single canonical origin key. Deterministic pure function of the origin
 * (spec/plan: surface-routing single-source lesson). Treat it as opaque outside
 * the canonical protocol-boundary parser below.
 */
export function originKey(ref: OriginRef): string {
	validateOriginRef(ref);
	const parts = [ref.platform, ref.kind, ref.conversationId];
	if (ref.parentId) parts.push(`parent=${ref.parentId}`);
	if (ref.peerId) parts.push(`peer=${ref.peerId}`);
	return parts.join("/");
}

/** Parse only the exact originKey grammar; never decode, normalize or guess. */
export function parseOriginKey(key: string): OriginRef {
	const parts = key.split("/");
	const [platform, kind, conversationId, qualifier] = parts;
	if (parts.length < 3 || parts.length > 4 || !platform || !kind || !conversationId)
		throw new OriginRefError("invalid canonical origin key");
	const ref: OriginRef = {
		platform: platform as OriginPlatform,
		kind: kind as OriginKind,
		conversationId,
		...(qualifier?.startsWith("parent=") ? { parentId: qualifier.slice(7) } : {}),
		...(qualifier?.startsWith("peer=") ? { peerId: qualifier.slice(5) } : {}),
	};
	validateOriginRef(ref);
	if (originKey(ref) !== key) throw new OriginRefError("noncanonical origin key");
	return ref;
}

/** The single loopback origin used by the P0 `gajaeway chat` REPL. */
export const LOOPBACK_ORIGIN: OriginRef = {
	platform: "loopback",
	kind: "loopback",
	conversationId: "loopback",
};

/** Origin of the event-type session executing events of one declared type (P4). */
export function eventTypeOrigin(eventType: string): OriginRef {
	return { platform: "monitor", kind: "eventtype", conversationId: eventType };
}

/**
 * Origin of ONE monitor's executing session for one event type. The monitor id
 * is part of the key: two monitors that declare the same event type must never
 * share a session, its history, its instruction, or its failure domain (#177).
 */
export function monitorSessionOrigin(monitorId: string, eventType: string): OriginRef {
	return { ...eventTypeOrigin(eventType), parentId: monitorId };
}

/** The catch-all session origin for undeclared event types (spec fact 19). */
export const CATCH_ALL_EVENT_ORIGIN: OriginRef = {
	platform: "monitor",
	kind: "eventtype",
	conversationId: "catch-all",
};
