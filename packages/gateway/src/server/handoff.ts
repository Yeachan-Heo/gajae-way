import {
	checkHandoffChain,
	composeHandoffDigest,
	composeHandoffFailure,
	composeHandoffPayload,
	composeHandoffPointer,
	type HandoffDigestEntry,
	type HandoffProvenance,
	type HandoffRefusal,
	handoffEventId,
	handoffOriginLabel,
	type OriginRef,
	originKey,
	validateOriginRef,
} from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";
import type { GatewayDatabase } from "../store/db";

/**
 * Session-to-session handoff dispatch (issue #72).
 *
 * A handoff is accepted when its target inbound row is durably inserted. The
 * target actor is notified only after that boundary and owns all later binding,
 * steering, delivery, and recovery. In particular, a target that is busy or
 * temporarily unavailable does not turn an accepted source handoff into a
 * failure notice.
 */

export interface HandoffTarget {
	readonly origin: OriginRef;
	/** What the source room's pointer calls this destination. */
	readonly label: string;
}

/**
 * Binds a written target to a real origin. Two ways, both auditable:
 *
 * 1. a `handoffTargets` alias — an operator-declared origin, the intended path;
 * 2. a conversation the gateway already has a session for, named by its
 *    canonical origin key or (unambiguously) by its bare conversation id.
 *
 * There is deliberately no "construct an origin from the id and hope": an
 * origin needs a platform and a kind, and guessing them produces a plausible
 * key that nothing is bound to.
 */
export function resolveHandoffTarget(
	target: string,
	deps: { readonly config: GatewayConfig; readonly database: GatewayDatabase },
): HandoffTarget | HandoffRefusal {
	const wanted = target.trim();
	if (!wanted) return { code: "unresolved_target", detail: "the handoff token carried an empty target" };
	const alias = deps.config.handoffTargets?.[wanted];
	if (alias) {
		const origin = validateOriginRef(alias);
		return { origin, label: `${wanted} (${handoffOriginLabel(origin)})` };
	}
	const known: Array<{ key: string; origin: OriginRef }> = [];
	for (const row of deps.database.sessionIdentityRows()) {
		if (!row.origin_ref_json) continue;
		try {
			known.push({ key: row.origin_key, origin: validateOriginRef(JSON.parse(row.origin_ref_json) as OriginRef) });
		} catch {
			// A corrupt session row must not make every handoff unresolvable.
		}
	}
	const exact = known.find((entry) => entry.key === wanted);
	if (exact) return { origin: exact.origin, label: handoffOriginLabel(exact.origin) };
	const byConversation = known.filter((entry) => entry.origin.conversationId === wanted);
	if (byConversation.length === 1) {
		const only = byConversation[0] as { key: string; origin: OriginRef };
		return { origin: only.origin, label: handoffOriginLabel(only.origin) };
	}
	if (byConversation.length > 1)
		return {
			code: "ambiguous_target",
			detail: `"${wanted}" matches ${byConversation.length} conversations (${byConversation.map((entry) => entry.key).join(", ")}); use a handoffTargets alias or the full origin key`,
		};
	const aliases = Object.keys(deps.config.handoffTargets ?? {});
	return {
		code: "unresolved_target",
		detail: `"${wanted}" is not a configured handoff alias${aliases.length ? ` (known: ${aliases.join(", ")})` : " (none configured)"} and no session is bound to it`,
	};
}

export interface HandoffRequest {
	/** Target as written in the token. */
	readonly target: string;
	/** The relaying session's own words: what the target should know and do. */
	readonly body: string;
	readonly sourceOrigin: OriginRef;
	readonly sourceLabel: string;
	readonly sourceMessageId: string;
	readonly requester: string;
	readonly requestedAt: string;
	/** The chain carried by the inbound relayed event, empty for a human turn. */
	readonly incomingChain: readonly string[];
	/** Source material for the bounded digest, oldest first. */
	readonly digestEntries: readonly HandoffDigestEntry[];
}

export type HandoffOutcome =
	| {
			/** The target inbound row was accepted and notification was scheduled. */
			readonly kind: "relayed";
			readonly targetOriginKey: string;
			readonly handoffMessageId: string;
			readonly notice: string;
	  }
	| {
			/** The same causal event was already accepted; it must not notify twice. */
			readonly kind: "duplicate";
			readonly targetOriginKey: string;
			readonly handoffMessageId: string;
			readonly notice: string;
	  }
	| {
			readonly kind: "refused";
			readonly refusal: HandoffRefusal;
			readonly notice: string;
	  };

export interface HandoffDeps {
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	/** Notifies the target's PersonaSessionManager actor after durable acceptance. */
	readonly notifyTarget: (targetOriginKey: string) => Promise<void>;
	/** Optional logging seam; notification failure never changes acceptance. */
	readonly log?: (line: string) => void;
}

/**
 * The first-line token shape, used only to suppress an interim frame. It does
 * not resolve a target or enforce a chain; those semantics belong exclusively
 * to the finalized terminal reply path in server.ts.
 */
export function isHandoffTokenPrefix(text: string): boolean {
	return /^[ \t]*\[HANDOFF:[^\]\n]*\][ \t]*(?:\n|$)/.test(text);
}

/**
 * Enqueue one handoff event. Durable insertion is the acceptance and
 * idempotency boundary; target execution is intentionally asynchronous and
 * remains owned by PersonaSessionManager.
 */
export function dispatchHandoff(deps: HandoffDeps, request: HandoffRequest): HandoffOutcome {
	const resolved = resolveHandoffTarget(request.target, deps);
	if ("code" in resolved) return { kind: "refused", refusal: resolved, notice: composeHandoffFailure(resolved) };
	const sourceKey = originKey(request.sourceOrigin);
	const targetKey = originKey(resolved.origin);
	const chain = [...request.incomingChain, sourceKey];
	const refusal = checkHandoffChain(chain, targetKey);
	if (refusal) return { kind: "refused", refusal, notice: composeHandoffFailure(refusal) };
	const provenance: HandoffProvenance = {
		sourceOriginKey: sourceKey,
		sourceLabel: request.sourceLabel,
		sourceMessageId: request.sourceMessageId,
		requester: request.requester,
		requestedAt: request.requestedAt,
		chain,
	};
	const payload = composeHandoffPayload({
		provenance,
		body: request.body,
		digest: composeHandoffDigest(request.digestEntries),
		targetOriginKey: targetKey,
	});
	const handoffMessageId = handoffEventId({
		sourceOriginKey: sourceKey,
		sourceMessageId: request.sourceMessageId,
		targetOriginKey: targetKey,
	});
	const pointer = composeHandoffPointer(resolved.label, targetKey);
	const accepted = deps.database.inboundEnqueue({
		messageId: handoffMessageId,
		originKey: targetKey,
		originRefJson: JSON.stringify(resolved.origin),
		body: payload,
		engagementJson: JSON.stringify({
			// The relayed event is addressed to the target's own session. The
			// handoff payload itself explicitly grants no authority from the source.
			mentioned: true,
			group: resolved.origin.kind !== "dm",
			handoff: provenance,
		}),
	});
	if (!accepted) return { kind: "duplicate", targetOriginKey: targetKey, handoffMessageId, notice: pointer };
	try {
		const notification = deps.notifyTarget(targetKey);
		void notification.catch((error: unknown) => {
			const detail = error instanceof Error ? error.message : String(error);
			const line = `gateway handoff target notification failed target=${targetKey} message=${handoffMessageId} detail=${detail}; accepted row remains durable`;
			if (deps.log) deps.log(line);
			else console.error(line);
		});
	} catch (error) {
		const detail = error instanceof Error ? error.message : String(error);
		const line = `gateway handoff target notification failed target=${targetKey} message=${handoffMessageId} detail=${detail}; accepted row remains durable`;
		if (deps.log) deps.log(line);
		else console.error(line);
	}
	return { kind: "relayed", targetOriginKey: targetKey, handoffMessageId, notice: pointer };
}

/** Provenance carried by an inbound row, or undefined for an ordinary message. */
export function inboundHandoffProvenance(engagement: unknown): HandoffProvenance | undefined {
	const candidate = (engagement as { handoff?: unknown } | undefined)?.handoff;
	if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return undefined;
	const value = candidate as Record<string, unknown>;
	if (
		typeof value.sourceOriginKey !== "string" ||
		typeof value.sourceLabel !== "string" ||
		typeof value.sourceMessageId !== "string" ||
		typeof value.requester !== "string" ||
		typeof value.requestedAt !== "string" ||
		!Array.isArray(value.chain) ||
		!value.chain.every((entry) => typeof entry === "string")
	)
		return undefined;
	return candidate as unknown as HandoffProvenance;
}
