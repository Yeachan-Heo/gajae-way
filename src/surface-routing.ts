import type { OwnerSurface } from "./profile";

/**
 * THE single source of truth for surface identity and thread/topic derivation.
 *
 * This convention used to be re-implemented independently in at least four
 * places: the gateway's admission resolver, the Discord ingress route builder,
 * the Discord egress route resolver, and (for Telegram topics) an in-memory
 * platform map. They drifted, and the drift was not theoretical - the admission
 * resolver hardcoded `platform === "discord"`, so Telegram forum-topic surfaces
 * derived by the adapter could never be admitted at all.
 *
 * Every producer and consumer of a derived child surface id MUST go through this
 * module so the format, the platform rules, and the parent lookup cannot diverge
 * again.
 */

/** Separator between a parent surface and its derived child (thread/topic). */
export const THREAD_SURFACE_SEPARATOR = "/thread:";

/** Platform surface kinds that may parent a derived thread/topic child. */
const THREAD_PARENT_KINDS = new Set(["channel"]);

export interface ResolvedSurface {
	/** The surface the message is attributed to (may be a derived child). */
	readonly surface: OwnerSurface;
	/** The configured surface that owns quarantine/policy decisions. */
	readonly quarantineSurface: OwnerSurface;
	/** True when `surface` was derived rather than explicitly configured. */
	readonly derived: boolean;
}

/**
 * Builds a derived child surface id.
 *
 * The child id must remain a bare platform id (no separators of its own) so
 * `parseThreadSurfaceId` can round-trip it and so admission's prefix rule stays
 * unambiguous.
 */
export function threadSurfaceId(parentSurfaceId: string, threadId: string): string {
	if (!parentSurfaceId.trim()) throw new Error("A thread surface requires a parent surface id.");
	if (!isPlatformThreadId(threadId)) throw new Error("A thread surface id must be a bare numeric platform id.");
	return `${parentSurfaceId}${THREAD_SURFACE_SEPARATOR}${threadId}`;
}

/** Splits a derived child surface id, or returns undefined when it is not one. */
export function parseThreadSurfaceId(surfaceId: string): { readonly parentSurfaceId: string; readonly threadId: string } | undefined {
	const index = surfaceId.indexOf(THREAD_SURFACE_SEPARATOR);
	if (index <= 0) return undefined;
	const parentSurfaceId = surfaceId.slice(0, index);
	const threadId = surfaceId.slice(index + THREAD_SURFACE_SEPARATOR.length);
	if (!parentSurfaceId || !isPlatformThreadId(threadId)) return undefined;
	return { parentSurfaceId, threadId };
}

/** A platform thread/topic id: numeric and free of separators. */
export function isPlatformThreadId(value: string): boolean {
	return /^\d+$/.test(value);
}

/**
 * Resolves a submitted surface id against the configured catalog.
 *
 * An exact configured surface wins. Otherwise the id may be a derived
 * thread/topic child of exactly one configured channel surface, on ANY platform -
 * ephemeral thread ids are deliberately not written into the registry, so this
 * prefix rule is what admits them without dynamic registry mutation.
 */
export function resolveSurface(surfaceId: string, knownSurfaces: ReadonlyMap<string, OwnerSurface>): ResolvedSurface | undefined {
	const exact = knownSurfaces.get(surfaceId);
	if (exact) return { surface: exact, quarantineSurface: exact, derived: false };
	const parsed = parseThreadSurfaceId(surfaceId);
	if (!parsed) return undefined;
	const parent = knownSurfaces.get(parsed.parentSurfaceId);
	if (!parent || !THREAD_PARENT_KINDS.has(parent.kind)) return undefined;
	return {
		// A derived child inherits its parent's declared redaction class: a thread
		// under a conversation channel IS a conversation, and inventing a different
		// class here would apply the wrong deny list.
		surface: { id: surfaceId, platform: parent.platform, kind: "thread", sessionKind: parent.sessionKind },
		quarantineSurface: parent,
		derived: true,
	};
}
