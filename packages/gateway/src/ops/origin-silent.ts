import { createHash } from "node:crypto";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import type { DeliveryService } from "../delivery/delivery";
import type { Store } from "../store";
import { ORIGIN_SILENT_THRESHOLD_MS, silentHoldReason } from "./cycle";

/** How often the gateway looks for origins a human is waiting on (#688). */
export const ORIGIN_SILENT_CHECK_INTERVAL_MS = 60_000;

const ALERT_PREFIX = "origin_silent_alert:";
const ALERT_END = "origin_silent_alert;";

interface AlertRecord {
	readonly originKey: string;
	readonly messageId: string;
}

function noticeId(record: AlertRecord, kind: "alert" | "cleared"): string {
	return `gw-s-${createHash("sha256").update(`${record.originKey}|${record.messageId}|${kind}`).digest("hex").slice(0, 32)}`;
}

function alertKey(originKey: string): string {
	return `${ALERT_PREFIX}${createHash("sha256").update(originKey).digest("hex")}`;
}

/**
 * Alerts the owner once per silent-origin incident and once when it clears
 * (#688). An incident is the origin's oldest unanswered message, so the same
 * stuck message never re-alerts, across restarts too: the open incident lives
 * in meta and every notice has a deterministic delivery id.
 */
export function alertSilentOrigins(input: {
	readonly database: Store;
	readonly delivery: Pick<DeliveryService, "prepare">;
	readonly ownerTarget: OriginRef | undefined;
	readonly send: (payload: ChatMessagePayload) => void;
	readonly nowMs: number;
	readonly thresholdMs?: number;
}): void {
	const { database, delivery, ownerTarget, send, nowMs } = input;
	if (!ownerTarget) return;
	const thresholdMs = input.thresholdMs ?? ORIGIN_SILENT_THRESHOLD_MS;
	const silent = database.inbox.silentOrigins(new Date(nowMs - thresholdMs).toISOString());
	const open = new Map<string, AlertRecord>();
	for (const key of database.core.metaKeysInRange(ALERT_PREFIX, ALERT_END)) {
		const raw = database.core.metaGet(key);
		if (raw) open.set(key, JSON.parse(raw) as AlertRecord);
	}
	for (const row of silent) {
		const key = alertKey(row.originKey);
		const current = open.get(key);
		open.delete(key);
		if (current?.messageId === row.messageId) continue;
		const record = { originKey: row.originKey, messageId: row.messageId };
		const minutes = Math.round((nowMs - Date.parse(row.receivedAt)) / 60_000);
		const reason = silentHoldReason(row.turnState);
		const text = `[origin silent] ${row.originKey}: 사람 메시지에 ${minutes}분째 답이 없습니다 (reason=${reason}${row.turnOpRef ? ` opRef=${row.turnOpRef}` : ""}). 확인: gajaeway ops cycle`;
		database.core.metaSet(key, JSON.stringify(record));
		const id = noticeId(record, "alert");
		const payload = delivery.prepare(id, ownerTarget, text, undefined, id);
		if (payload) send(payload);
	}
	// Every incident still open but no longer silent has recovered.
	for (const [key, record] of open) {
		database.core.metaDelete(key);
		const id = noticeId(record, "cleared");
		const payload = delivery.prepare(
			id,
			ownerTarget,
			`[origin recovered] ${record.originKey}: 다시 답하고 있습니다.`,
			undefined,
			id,
		);
		if (payload) send(payload);
	}
}
