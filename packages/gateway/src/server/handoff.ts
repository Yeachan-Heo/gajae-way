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
 * The gateway half of the `[HANDOFF:<target>]` contract: bind the target origin,
 * enforce the loop bound, and hand ONE durable inbound event to the target
 * origin's own queue so the target session answers in its own room under the
 * existing per-origin turn serialization.
 *
 * Deliberately separate from server.ts: target binding and the refusal taxonomy
 * are the load-bearing parts and they are testable without a socket.
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
 * 2. a conversation the persona already has a session for, named by its canonical
 *    origin key or (unambiguously) by its bare conversation id.
 *
 * There is deliberately no "construct an origin from the id and hope": an origin
 * needs a platform and a kind, and guessing them produces a plausible key that
 * nothing is bound to — the silent-loss shape this issue exists to kill.
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
	/**
	 * The chain carried IN by the message this turn answered, empty for a turn
	 * that started from a human message. The hop's own origin is appended here,
	 * which is what makes the loop check local.
	 */
	readonly incomingChain: readonly string[];
	/** Source material for the bounded digest, oldest first. */
	readonly digestEntries: readonly HandoffDigestEntry[];
}

export type HandoffOutcome =
	| {
			/** Enqueued and the target turn ran. */
			readonly kind: "relayed";
			readonly targetOriginKey: string;
			readonly handoffMessageId: string;
			readonly notice: string;
	  }
	| {
			/** The same handoff event was already accepted: the target turn is NOT run again. */
			readonly kind: "duplicate";
			readonly targetOriginKey: string;
			readonly handoffMessageId: string;
			readonly notice: string;
	  }
	| {
			readonly kind: "refused";
			readonly refusal: HandoffRefusal;
			readonly notice: string;
	  }
	| {
			/** Bound and enqueued, but the target turn failed. The source must not read "moved". */
			readonly kind: "target_failed";
			readonly targetOriginKey: string;
			readonly handoffMessageId: string;
			readonly notice: string;
	  };

export interface HandoffDeps {
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	/** Runs the target origin's queue, under that origin's own turn serialization. */
	readonly runTargetTurn: (targetOriginKey: string, handoffMessageId: string) => Promise<void>;
}

/**
 * The whole dispatch. Every exit produces a notice for the SOURCE room: a
 * pointer when the work moved, a loud failure when it did not. Nothing here can
 * end with the human believing the work moved when it did not.
 */
export async function dispatchHandoff(deps: HandoffDeps, request: HandoffRequest): Promise<HandoffOutcome> {
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
	// The durable insert is the acceptance boundary AND the idempotency gate: a
	// replayed or reconciled handoff hits the same causal id and is rejected here,
	// so the target turn runs exactly once.
	const accepted = deps.database.inboundEnqueue({
		messageId: handoffMessageId,
		originKey: targetKey,
		originRefJson: JSON.stringify(resolved.origin),
		body: payload,
		engagementJson: JSON.stringify({
			// The relayed event is addressed to the target session by its own sibling,
			// so it is answered rather than gated as room chatter. Authority still does
			// not travel: `handoff` marks the body second-hand, and the target's own
			// channel policy governs everything it does with it.
			mentioned: true,
			group: resolved.origin.kind !== "dm",
			handoff: provenance,
		}),
	});
	if (!accepted) return { kind: "duplicate", targetOriginKey: targetKey, handoffMessageId, notice: pointer };
	try {
		await deps.runTargetTurn(targetKey, handoffMessageId);
	} catch (error) {
		return {
			kind: "target_failed",
			targetOriginKey: targetKey,
			handoffMessageId,
			notice: `[handoff failed] target_turn_failed: ${resolved.label} (${targetKey}) accepted the handoff but its turn failed: ${error instanceof Error ? error.message : String(error)}. The work did not move.`,
		};
	}
	return { kind: "relayed", targetOriginKey: targetKey, handoffMessageId, notice: pointer };
}

/** Provenance carried by an inbound row, or undefined for an ordinary message. */
export function inboundHandoffProvenance(engagement: unknown): HandoffProvenance | undefined {
	const candidate = (engagement as { handoff?: unknown } | undefined)?.handoff as HandoffProvenance | undefined;
	if (!candidate || typeof candidate !== "object") return undefined;
	if (typeof candidate.sourceOriginKey !== "string" || !Array.isArray(candidate.chain)) return undefined;
	return candidate;
}
