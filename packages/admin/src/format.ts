/**
 * Display formatting for the console projection.
 *
 * Every string an operator reads is built here, on the server, so the browser
 * only ever maps a finished string onto a DOM node. The one exception is
 * anything that ticks - relative ages - which the client recomputes from an ISO
 * timestamp, because a server-rendered "11m ago" starts lying the moment it is
 * sent.
 */

import { type OriginRef, originKey, type TriggerSpec } from "@gajae-gateway/protocol";
import { parseCron } from "./cron";

/**
 * `4d 6h`, `3m 12s`, `41s`, `0s`. Two units at most: precision the owner cannot
 * use is noise. A zero second unit is dropped, because `5m` is what a person
 * says and `5m 0s` reads like a broken template.
 */
export function formatDuration(ms: number): string {
	const total = Math.max(0, Math.round(ms / 1000));
	const days = Math.floor(total / 86_400);
	const hours = Math.floor((total % 86_400) / 3600);
	const minutes = Math.floor((total % 3600) / 60);
	const seconds = total % 60;
	if (days > 0) return hours > 0 ? `${days}d ${hours}h` : `${days}d`;
	if (hours > 0) return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
	if (minutes > 0) return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
	return `${seconds}s`;
}

/** Local wall clock, `14:02`. */
export function formatClock(at: Date): string {
	const hours = String(at.getHours()).padStart(2, "0");
	const minutes = String(at.getMinutes()).padStart(2, "0");
	return `${hours}:${minutes}`;
}

/** Local wall clock with seconds, `14:02:31`, for the stream freshness stamp. */
export function formatClockSeconds(at: Date): string {
	return `${formatClock(at)}:${String(at.getSeconds()).padStart(2, "0")}`;
}

export function parseIso(value: string | null | undefined): Date | null {
	if (!value) return null;
	const at = new Date(value);
	return Number.isNaN(at.getTime()) ? null : at;
}

/** `1493…5762`: enough to recognise, short enough not to dominate a phone row. */
export function shortId(id: string, keep = 4): string {
	return id.length <= keep * 2 + 1 ? id : `${id.slice(0, keep)}…${id.slice(-keep)}`;
}

/**
 * The best human name available for an origin. No verb returns channel or server
 * labels (gap G4), so this is structural rather than friendly - but structural
 * and honest beats `discord/channel/1493635653441945762` verbatim.
 *
 * A running gateway is free to grow origin platforms and kinds ahead of this
 * console's copy of the protocol - the live deployment already serves
 * `work/task` origins that `ORIGIN_KINDS` does not know - so an unrecognised
 * origin is named from its own fields instead of being rejected. A console that
 * 500s on one unfamiliar row is worse than a console that names it plainly.
 */
export function originLabel(origin: OriginRef): string {
	switch (origin.kind) {
		case "loopback":
			return "loopback console";
		case "dm":
			return `${origin.platform} DM · ${shortId(origin.peerId ?? origin.conversationId)}`;
		case "eventtype":
			return `monitor event · ${origin.conversationId}`;
		case "channel":
		case "thread":
		case "topic":
			return `${origin.platform} ${origin.kind} · ${shortId(origin.conversationId)}`;
		default:
			return `${origin.platform} ${String(origin.kind)} · ${shortId(origin.conversationId)}`;
	}
}

/**
 * A stable identity for a row. `originKey` validates, and validation is the
 * wrong behaviour here for the reason above, so an origin the protocol copy
 * rejects falls back to the same deterministic shape rather than throwing.
 */
export function originId(origin: OriginRef): string {
	try {
		return originKey(origin);
	} catch {
		const parts = [origin.platform, origin.kind, origin.conversationId];
		if (origin.parentId) parts.push(`parent=${origin.parentId}`);
		if (origin.peerId) parts.push(`peer=${origin.peerId}`);
		return parts.join("/");
	}
}

const DAY_WORDS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function dayOfWeekWords(days: Set<number>): string | null {
	if (days.size === 7) return null;
	const sorted = [...days].sort((a, b) => a - b);
	if (sorted.length === 5 && sorted.join(",") === "1,2,3,4,5") return "weekdays";
	if (sorted.length === 2 && sorted.join(",") === "0,6") return "weekends";
	return sorted.map((day) => DAY_WORDS[day] ?? String(day)).join(" ");
}

/**
 * A cron expression as a sentence when it is simple enough to say, and the raw
 * expression when it is not. Guessing at a complex schedule is worse than
 * showing it.
 */
export function cronSummary(schedule: string): string {
	const cron = parseCron(schedule);
	if (!cron) return `cron ${schedule} (unparseable)`;
	const days = dayOfWeekWords(cron.dayOfWeek);
	const scope = days ? `${days} ` : cron.dayOfMonth.size === 31 ? "daily " : "";
	if (cron.hour.size === 1 && cron.minute.size === 1) {
		const [hour] = cron.hour;
		const [minute] = cron.minute;
		return `${scope}${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
	}
	if (cron.hour.size === 24 && cron.minute.size === 1) {
		const [minute] = cron.minute;
		return `${scope}hourly at :${String(minute).padStart(2, "0")}`;
	}
	return `cron ${schedule}`;
}

/** One line naming what makes a monitor fire, whatever its trigger kind. */
export function triggerSummary(trigger: TriggerSpec): string {
	switch (trigger.kind) {
		case "cron":
			return cronSummary(trigger.schedule);
		case "webhook":
			return `webhook ${trigger.route}`;
		case "watcher":
			return `watches ${trigger.root}`;
		case "script":
			return `runs ${trigger.command.join(" ")} every ${formatDuration(trigger.intervalMs)}`;
	}
}

/** `3,240` - thousands separators, because 3240 and 32400 are hard to tell apart at a glance. */
export function formatCount(value: number): string {
	return value.toLocaleString("en-US");
}

export function pluralise(count: number, singular: string, plural = `${singular}s`): string {
	return `${formatCount(count)} ${count === 1 ? singular : plural}`;
}
