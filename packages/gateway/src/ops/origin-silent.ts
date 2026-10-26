import { createHash } from "node:crypto";
import type { OriginHoldReason, SilentOriginView } from "@gajae-gateway/protocol";
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

/** Read-only: origins whose user messages have gone unanswered past `thresholdMs`. */
export function findSilentOrigins(
	database: Store,
	nowMs: number,
	thresholdMs: number = ORIGIN_SILENT_MS,
): SilentOriginView[] {
	const cutoff = new Date(nowMs - thresholdMs).toISOString();
	return database.inbox.unansweredOrigins(cutoff).map((row) => {
		const block = blockingTurn(database, row.origin_key);
		return {
			originKey: row.origin_key,
			oldestMessageId: row.message_id,
			oldestPendingAgeMs: Math.max(0, nowMs - Date.parse(row.received_at)),
			unanswered: row.unanswered,
			blockingOpRef: block.opRef,
			holdReason: block.reason,
		};
	});
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

export interface OriginSilentAlert {
	readonly kind: "silent" | "cleared";
	readonly originKey: string;
	/** Deterministic: one alert per origin per incident, one cleared notice per incident. */
	readonly deliveryId: string;
	readonly notice: string;
}

/**
 * Owner alerting for `origin_silent`: one alert per origin per incident and one
 * cleared notice when the origin answers again. Incidents are durable meta rows,
 * so a restart neither repeats an alert nor loses the cleared notice, and the
 * delivery ids are deterministic so a retried send is deduplicated by the ledger.
 */
export class OriginSilentAlerter {
	readonly #database: Store;
	readonly #thresholdMs: number;

	constructor(database: Store, thresholdMs: number = ORIGIN_SILENT_MS) {
		this.#database = database;
		this.#thresholdMs = thresholdMs;
	}

	/** Records incident transitions and returns the owner notices they call for. */
	sweep(nowMs: number): OriginSilentAlert[] {
		const silent = findSilentOrigins(this.#database, nowMs, this.#thresholdMs);
		const silentKeys = new Set(silent.map((origin) => origin.originKey));
		const alerts: OriginSilentAlert[] = [];
		this.#database.transaction(() => {
			const core = this.#database.core;
			for (const origin of silent) {
				const key = `${INCIDENT_PREFIX}${origin.originKey}`;
				if (core.metaGet(key) !== undefined) continue;
				const incident: Incident = { messageId: origin.oldestMessageId, alertedAt: new Date(nowMs).toISOString() };
				core.metaSet(key, JSON.stringify(incident));
				alerts.push({
					kind: "silent",
					originKey: origin.originKey,
					deliveryId: alertDeliveryId("silent", origin.originKey, incident.messageId),
					notice: silentNotice(origin),
				});
			}
			for (const key of core.metaKeysInRange(INCIDENT_PREFIX, INCIDENT_PREFIX_END)) {
				const originKey = key.slice(INCIDENT_PREFIX.length);
				if (silentKeys.has(originKey)) continue;
				const incident = parseIncident(core.metaGet(key));
				core.metaDelete(key);
				alerts.push({
					kind: "cleared",
					originKey,
					deliveryId: alertDeliveryId("cleared", originKey, incident?.messageId ?? key),
					notice: `[origin silent cleared] ${displayKey(originKey)} 응답이 다시 나가고 있습니다.`,
				});
			}
		});
		return alerts;
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

function parseIncident(raw: string | undefined): Incident | undefined {
	if (raw === undefined) return undefined;
	try {
		const parsed = JSON.parse(raw) as Partial<Incident>;
		return typeof parsed.messageId === "string" && typeof parsed.alertedAt === "string"
			? { messageId: parsed.messageId, alertedAt: parsed.alertedAt }
			: undefined;
	} catch {
		return undefined;
	}
}

function alertDeliveryId(kind: "silent" | "cleared", originKey: string, messageId: string): string {
	return `gw-s-${createHash("sha256").update(`${kind}|${originKey}|${messageId}`).digest("hex").slice(0, 32)}`;
}
