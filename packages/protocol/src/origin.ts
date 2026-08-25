/**
 * Canonical origin identity (ARCH-007): the PUBLIC, platform-neutral
 * normalization lives here in the protocol so SDK-only adapters can construct
 * canonical OriginRefs. The gateway owns only the binding from an OriginRef
 * to an internal gjc session (never exposed here).
 *
 * One conversational origin == one strictly isolated session (spec fact 9).
 */

export const ORIGIN_PLATFORMS = ["loopback", "discord", "telegram", "monitor"] as const;
export type OriginPlatform = (typeof ORIGIN_PLATFORMS)[number];

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
	return ref;
}

/**
 * THE single canonical origin key. Deterministic pure function of the origin
 * (spec/plan: surface-routing single-source lesson). Never parse this string
 * back into parts; it is an opaque identity.
 */
export function originKey(ref: OriginRef): string {
	validateOriginRef(ref);
	const parts = [ref.platform, ref.kind, ref.conversationId];
	if (ref.parentId) parts.push(`parent=${ref.parentId}`);
	if (ref.peerId) parts.push(`peer=${ref.peerId}`);
	return parts.join("/");
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

/** The catch-all session origin for undeclared event types (spec fact 19). */
export const CATCH_ALL_EVENT_ORIGIN: OriginRef = {
	platform: "monitor",
	kind: "eventtype",
	conversationId: "catch-all",
};
