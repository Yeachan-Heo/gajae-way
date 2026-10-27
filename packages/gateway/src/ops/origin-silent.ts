import { createHash } from "node:crypto";
import {
	type ChatMessagePayload,
	type EngagementContext,
	type OriginHoldReason,
	type OriginRef,
	originKey as originKeyOf,
	type SilentOriginView,
} from "@gajae-gateway/protocol";
import { buildDeliveryPayload } from "../delivery/delivery";
import { threadMuteKey } from "../engagement/policy";
import { sanitizeDiagnostic } from "../orchestrator/rebind";
import type { Store } from "../store";

/**
 * Origin liveness, not process liveness (#688). On 2026-10-26 two Discord
 * channels went 2+ hours without a reply while the healthcheck, readiness and
 * `gajaeway status` all stayed green: every check asked whether the gateway was
 * alive, none whether an origin was being answered. A user message older than
 * this, with nothing delivered on its origin since it arrived, is silence.
 */
export const ORIGIN_SILENT_MS = 15 * 60_000;

const INCIDENT_PREFIX = "origin_silent:";
/** Exclusive upper bound for meta keys starting with INCIDENT_PREFIX. */
const INCIDENT_PREFIX_END = "origin_silent;";
/** A malformed incident row is moved here as evidence instead of being read as a valid incident. */
const CORRUPT_INCIDENT_PREFIX = "origin_silent_corrupt:";

/** Read-only: origins whose user messages have gone unanswered past `thresholdMs`. */
export function findSilentOrigins(
	database: Store,
	nowMs: number,
	thresholdMs: number = ORIGIN_SILENT_MS,
): SilentOriginView[] {
	const cutoff = new Date(nowMs - thresholdMs).toISOString();
	const byOrigin = new Map<string, { oldestMessageId: string; oldestReceivedAt: string; unanswered: number }>();
	for (const row of database.inbox.unansweredMessages(cutoff)) {
		if (inMutedThread(database, row)) continue;
		const entry = byOrigin.get(row.origin_key);
		if (entry) entry.unanswered += 1;
		else
			byOrigin.set(row.origin_key, {
				oldestMessageId: row.message_id,
				oldestReceivedAt: row.received_at,
				unanswered: 1,
			});
	}
	return [...byOrigin].map(([originKey, entry]) => {
		const block = blockingTurn(database, originKey);
		return {
			originKey,
			oldestMessageId: entry.oldestMessageId,
			oldestPendingAgeMs: Math.max(0, nowMs - Date.parse(entry.oldestReceivedAt)),
			unanswered: entry.unanswered,
			blockingOpRef: block.opRef,
			holdReason: block.reason,
		};
	});
}

/** The intake's own mute rule (#625): a `[SHUTUP]` thread never raises silence, including Slack replies kept on the channel session. */
function inMutedThread(
	database: Store,
	row: { readonly origin_key: string; readonly origin_ref_json: string; readonly engagement_json: string | null },
): boolean {
	let origin: OriginRef;
	let engagement: EngagementContext | undefined;
	try {
		origin = JSON.parse(row.origin_ref_json) as OriginRef;
		engagement = row.engagement_json === null ? undefined : (JSON.parse(row.engagement_json) as EngagementContext);
	} catch {
		// An unreadable row cannot prove it was muted; silence is reported rather than hidden.
		return false;
	}
	const key = threadMuteKey(origin, row.origin_key, engagement);
	return key !== undefined && database.inbox.isThreadMuted(key);
}

/** The nonterminal trigger owning the origin's current epoch, and why it is not moving. */
function blockingTurn(
	database: Store,
	originKey: string,
): { readonly opRef: string | null; readonly reason: OriginHoldReason } {
	const epoch = database.bindings.getSessionRecord(originKey)?.epoch;
	const turns = database.inbox.nonterminalTurns(originKey);
	// A retired-epoch hold no longer fences admission; only the current epoch's turn blocks.
	const turn = epoch === undefined ? turns.at(-1) : turns.find((candidate) => candidate.epoch === epoch);
	if (!turn) return { opRef: null, reason: "dispatch_pending" };
	if (turn.state === "accepted") return { opRef: turn.opRef, reason: "turn_running" };
	if (!database.inbox.sendAttempted(turn.opRef)) return { opRef: turn.opRef, reason: "send_not_attempted" };
	if (database.bindings.promptBusyRefused(turn.opRef)) return { opRef: turn.opRef, reason: "session_busy_wait" };
	return { opRef: turn.opRef, reason: "operation_state_unknown" };
}

interface Incident {
	readonly messageId: string;
	readonly alertedAt: string;
}

export interface OriginSilentTransition {
	readonly kind: "silent" | "cleared";
	readonly originKey: string;
	/** Deterministic: one alert per origin per incident, one cleared notice per incident. */
	readonly deliveryId: string;
	/** The owner DM, already in the delivery ledger; undefined when no owner target is configured. */
	readonly payload: ChatMessagePayload | undefined;
}

/**
 * Owner alerting for `origin_silent`: one alert per origin per incident and one
 * cleared notice when the origin answers again. Each incident transition and
 * its owner DM commit in ONE transaction, so a crash can neither record an
 * incident whose alert was never queued nor drop an incident whose cleared
 * notice was never queued; deterministic delivery ids keep the ledger row
 * single across retries.
 */
export class OriginSilentAlerter {
	readonly #database: Store;
	readonly #owner: () => OriginRef | undefined;
	readonly #thresholdMs: number;
	readonly #log: (line: string) => void;

	constructor(
		database: Store,
		owner: () => OriginRef | undefined,
		options: { readonly thresholdMs?: number; readonly log?: (line: string) => void } = {},
	) {
		this.#database = database;
		this.#owner = owner;
		this.#thresholdMs = options.thresholdMs ?? ORIGIN_SILENT_MS;
		this.#log = options.log ?? ((line) => console.warn(line));
	}

	/** Records incident transitions with their owner DMs and returns them for broadcast. */
	sweep(nowMs: number): OriginSilentTransition[] {
		const silent = findSilentOrigins(this.#database, nowMs, this.#thresholdMs);
		const silentKeys = new Set(silent.map((origin) => origin.originKey));
		const owner = this.#owner();
		const transitions: OriginSilentTransition[] = [];
		const database = this.#database;
		database.transaction((tx) => {
			const core = database.core;
			const queue = (kind: "silent" | "cleared", originKey: string, deliveryId: string, notice: string) => {
				const payload = owner ? buildDeliveryPayload(deliveryId, owner, notice, deliveryId) : undefined;
				if (payload && owner)
					database.outbox.create(tx, {
						deliveryId,
						turnId: deliveryId,
						originKey: originKeyOf(owner),
						payloadJson: JSON.stringify(payload),
					});
				transitions.push({ kind, originKey, deliveryId, payload });
			};
			for (const origin of silent) {
				const key = `${INCIDENT_PREFIX}${origin.originKey}`;
				if (core.metaGet(key) !== undefined) continue;
				const incident: Incident = { messageId: origin.oldestMessageId, alertedAt: new Date(nowMs).toISOString() };
				core.metaSet(key, JSON.stringify(incident));
				queue(
					"silent",
					origin.originKey,
					alertDeliveryId("silent", origin.originKey, incident.messageId),
					silentNotice(origin),
				);
			}
			for (const key of core.metaKeysInRange(INCIDENT_PREFIX, INCIDENT_PREFIX_END)) {
				const originKey = key.slice(INCIDENT_PREFIX.length);
				if (silentKeys.has(originKey)) continue;
				const raw = core.metaGet(key) ?? "";
				const incident = parseIncident(raw);
				core.metaDelete(key);
				if (!incident) {
					// Never read a malformed row as a valid incident: keep it as evidence,
					// and still tell the owner the origin answers again.
					core.metaSet(`${CORRUPT_INCIDENT_PREFIX}${originKey}`, raw);
					this.#log(`origin_silent_incident_corrupt origin=${originKey} raw=${sanitizeDiagnostic(raw).slice(0, 160)}`);
				}
				queue(
					"cleared",
					originKey,
					alertDeliveryId("cleared", originKey, incident?.messageId ?? `corrupt:${raw}`),
					`[origin silent cleared] ${displayKey(originKey)} 응답이 다시 나가고 있습니다.`,
				);
			}
		});
		return transitions;
	}
}

function silentNotice(origin: SilentOriginView): string {
	const minutes = Math.floor(origin.oldestPendingAgeMs / 60_000);
	const op = origin.blockingOpRef ? ` op=${origin.blockingOpRef}` : "";
	return `[origin silent] ${displayKey(origin.originKey)} 사용자 메시지 ${origin.unanswered}건이 ${minutes}분째 응답이 없습니다. reason=${origin.holdReason}${op}. 확인: gajaeway ops cycle`;
}

function displayKey(originKey: string): string {
	return sanitizeDiagnostic(originKey).slice(0, 160);
}

function parseIncident(raw: string): Incident | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		return undefined;
	}
	if (typeof parsed !== "object" || parsed === null) return undefined;
	const { messageId, alertedAt } = parsed as Partial<Incident>;
	return typeof messageId === "string" && typeof alertedAt === "string" ? { messageId, alertedAt } : undefined;
}

function alertDeliveryId(kind: "silent" | "cleared", originKey: string, messageId: string): string {
	return `gw-s-${createHash("sha256").update(`${kind}|${originKey}|${messageId}`).digest("hex").slice(0, 32)}`;
}
