/**
 * The console projection.
 *
 * One rule governs this module: every string the operator reads is produced
 * here, and the browser only ever maps a finished string onto a DOM node. That
 * keeps ranking, severity policy and wording in exactly one place, and it is why
 * the client needs no knowledge of the protocol at all.
 *
 * Rows are uniform `RowView`s - a stable key, a tone, a flat field map, plus the
 * two things that must be recomputed in the browser because they change without
 * new data arriving (ages, and a meter's position). The client reconciles by key
 * and writes only fields that actually differ, so scroll position, text
 * selection and open `<details>` all survive a refresh.
 */

import type {
	GatewayStatusResult,
	MonitorEventRecord,
	MonitorRecord,
	SessionListResult,
	TriggerSpec,
} from "@gajaeway/protocol";
import {
	ATTENTION_GAPS,
	type AttentionItem,
	buildAttention,
	type CoverageGap,
	type MonitorSnapshot,
} from "./attention";
import { nextCronFire } from "./cron";
import {
	formatClock,
	formatClockSeconds,
	formatCount,
	formatDuration,
	originId,
	originLabel,
	parseIso,
	pluralise,
	shortId,
	triggerSummary,
} from "./format";
import { type TrackedTurn, TURN_CEILING_MS, type TurnTracker } from "./turns";

export type Tone = "ok" | "warn" | "danger" | "muted" | "active";

export type Meter = { readonly value: number; readonly max: number };

export type RowView = {
	readonly key: string;
	readonly tone: Tone;
	/** Row-level modifier the stylesheet keys off (`data-state`). */
	readonly state?: string;
	readonly fields: Readonly<Record<string, string>>;
	/** Per-field tone overrides, for status chips inside one row. */
	readonly tones?: Readonly<Record<string, Tone>>;
	/** Field name -> ISO instant; the client renders a ticking age into it. */
	readonly ages?: Readonly<Record<string, string>>;
	readonly meters?: Readonly<Record<string, Meter>>;
};

/**
 * A panel is never a blank box. `empty` is a designed sentence, `error` names the
 * failure, `blocked` names the protocol gap that would unblock it, and `stale`
 * is applied by the client when the stream has gone quiet.
 */
export type PanelState = "ready" | "empty" | "error" | "blocked";

export type Panel = {
	readonly state: PanelState;
	readonly note: string;
	readonly rows: readonly RowView[];
	readonly gaps: readonly CoverageGap[];
};

export type ConsoleSnapshot = {
	readonly at: string;
	readonly gateway: { readonly reachable: boolean; readonly error: string | null };
	readonly status: RowView;
	readonly attention: Panel;
	readonly live: Panel;
	readonly conversation: Panel;
	readonly sessions: Panel;
	readonly monitors: Panel;
	/** Tier 3 only: the raw protocol results, behind an explicit disclosure. */
	readonly raw: Readonly<Record<string, unknown>>;
};

export type GatewayRequest = (method: string, params?: unknown) => Promise<unknown>;

type ReadOutcome<T> = { readonly value: T | null; readonly error: string | null };

async function read<T>(request: GatewayRequest, method: string, params?: unknown): Promise<ReadOutcome<T>> {
	try {
		return { value: (await request(method, params)) as T, error: null };
	} catch (error) {
		return { value: null, error: error instanceof Error ? error.message : String(error) };
	}
}

const LIVE_GAPS: readonly CoverageGap[] = [
	{ gap: "G1", missing: "turns already in flight when the console connected — chat.progress is not replayable" },
	{ gap: "G7", missing: "delegated work — work.run is synchronous and returns no id, state or progress" },
];

const CONVERSATION_GAPS: readonly CoverageGap[] = [
	{ gap: "G2", missing: "session.transcript — conversation_context and authored_outputs are unexposed" },
];

const SESSION_GAPS: readonly CoverageGap[] = [
	{ gap: "G4", missing: "channel and server labels, and each session's last-turn outcome" },
];

const MONITOR_GAPS: readonly CoverageGap[] = [
	{ gap: "G6", missing: "enable/disable without delete-and-recreate, and a gateway-computed next fire" },
];

/** How many monitors the projection will inspect individually per pass. */
const MONITOR_INSPECT_LIMIT = 24;

function attentionRow(item: AttentionItem): RowView {
	return {
		key: item.key,
		tone: item.tone,
		fields: {
			title: item.title,
			detail: item.detail,
			meta: item.meta,
			age: item.at ? "" : "—",
		},
		...(item.at ? { ages: { age: item.at } } : {}),
	};
}

function turnRow(turn: TrackedTurn, state: "running" | "stalled" | "finished", now: Date): RowView {
	// Evidence of work, not assertion of work. A turn that finished inside the
	// gateway's first heartbeat window produced no counters at all, and printing
	// its zeros would read as "it did nothing" rather than "we never saw".
	const evidence = turn.observed
		? `${pluralise(turn.toolCalls, "tool call")} · ${formatCount(turn.outputTokens)} tokens out`
		: "no progress heartbeat was seen for this turn";
	const elapsed = state === "finished" ? turn.elapsedMs : Math.max(turn.elapsedMs, now.getTime() - turn.startedAt);
	const ceiling = formatDuration(TURN_CEILING_MS);
	// The gateway reports the elapsed time it actually observed, and a live run can
	// exceed the ceiling before the kill lands. Saying "approaching" at that point
	// would be the console reassuring the owner about a run that is already over
	// budget, so the two cases are named separately.
	const over = elapsed >= TURN_CEILING_MS;
	const near = elapsed > TURN_CEILING_MS * 0.8;

	const stateLabel =
		state === "finished"
			? turn.outcome === "silent"
				? "◐ replied [SILENT] — nothing sent"
				: turn.outcome === "failed"
					? "✕ turn failed"
					: turn.deliveryId
						? "✓ replied, delivery ledgered"
						: "✓ replied"
			: state === "stalled"
				? "⚠ no progress — last known counters below"
				: over
					? `▲ past the ${ceiling} ceiling`
					: near
						? "▲ approaching timeout"
						: "● working";

	return {
		key: turn.turnId,
		tone:
			state === "stalled" || (state !== "finished" && over)
				? "warn"
				: state === "finished"
					? turn.outcome === "failed"
						? "danger"
						: "ok"
					: "active",
		state,
		fields: {
			title: originLabel(turn.origin),
			turn: `turn ${shortId(turn.turnId)}`,
			stateLabel,
			elapsed: turn.observed ? `${formatDuration(elapsed)} / ${ceiling}` : "duration not observed",
			evidence,
			lastEvent: "",
		},
		ages: { lastEvent: new Date(turn.lastEventAt).toISOString() },
		...(state === "finished"
			? {}
			: { meters: { progress: { value: Math.min(elapsed, TURN_CEILING_MS), max: TURN_CEILING_MS } } }),
	};
}

function sessionRow(session: SessionListResult["sessions"][number]): RowView {
	const last = parseIso(session.lastActivityAt);
	const bootstrap = session.bootstrap ?? {
		epoch: session.epoch,
		pending: true,
		appliedAt: null,
		includedSections: [],
		byteCount: 0,
		truncated: false,
		diagnostics: ["projection_unavailable"],
	};
	return {
		key: originId(session.origin),
		tone: "muted",
		fields: {
			title: originLabel(session.origin),
			key: originId(session.origin),
			epoch: `epoch ${formatCount(session.epoch)}`,
			bootstrap: bootstrap.pending
				? `bootstrap pending for epoch ${formatCount(bootstrap.epoch)}`
				: `bootstrap applied · ${formatCount(bootstrap.byteCount)} bytes · ${bootstrap.includedSections.join(", ") || "metadata only"}${bootstrap.truncated ? " · truncated" : ""}`,
			activity: last ? "" : "no activity yet",
			created: "",
		},
		ages: {
			...(last ? { activity: session.lastActivityAt as string } : {}),
			created: session.createdAt,
		},
	};
}

function lastOutcome(events: readonly MonitorEventRecord[] | null): { label: string; tone: Tone; at: string | null } {
	if (!events) return { label: "◌ events unread", tone: "muted", at: null };
	const [latest] = events;
	if (!latest) return { label: "◌ never fired", tone: "muted", at: null };
	switch (latest.stage) {
		case "delivered":
			return { label: "✓ delivered", tone: "ok", at: latest.firedAt };
		case "authored":
			return { label: "◐ authored, delivery pending", tone: "warn", at: latest.firedAt };
		case "authored_no_delivery":
			return { label: "○ authored, no delivery target", tone: "muted", at: latest.firedAt };
		case "failed_no_retry":
			return { label: "✕ failed, retries exhausted", tone: "danger", at: latest.firedAt };
		case "failed":
			return { label: "✕ failed", tone: "danger", at: latest.firedAt };
		default:
			return { label: `◐ ${latest.stage}`, tone: "warn", at: latest.firedAt };
	}
}

function nextFireLabel(trigger: TriggerSpec, now: Date): string {
	if (trigger.kind !== "cron") return "on demand";
	const next = nextCronFire(trigger.schedule, now);
	if (!next) return "schedule never matches";
	const weekday = next.toLocaleDateString("en-US", { weekday: "short" });
	const clock = `${String(next.getHours()).padStart(2, "0")}:${String(next.getMinutes()).padStart(2, "0")}`;
	return `in ${formatDuration(next.getTime() - now.getTime())} · ${weekday} ${clock}`;
}

function monitorRow(monitor: MonitorRecord, events: readonly MonitorEventRecord[] | null, now: Date): RowView {
	const outcome = lastOutcome(events);
	return {
		key: monitor.monitorId,
		tone: monitor.enabled ? outcome.tone : "muted",
		state: monitor.enabled ? "enabled" : "disabled",
		fields: {
			name: monitor.name,
			trigger: triggerSummary(monitor.trigger),
			next: monitor.enabled ? nextFireLabel(monitor.trigger, now) : "paused — will not fire",
			emits: monitor.eventTypes.join(", ") || "no declared types",
			target: monitor.channelTarget ? originLabel(monitor.channelTarget.origin) : "no channel target",
			outcome: outcome.label,
			outcomeAge: outcome.at ? "" : "—",
			id: shortId(monitor.monitorId, 6),
		},
		tones: { outcome: outcome.tone },
		...(outcome.at ? { ages: { outcomeAge: outcome.at } } : {}),
	};
}

function statusRow(
	status: GatewayStatusResult | null,
	statusError: string | null,
	sessionCount: number | null,
	working: number,
	attention: readonly AttentionItem[],
	now: Date,
): RowView {
	const startedAt = parseIso(status?.startedAt);
	const danger = attention.some((item) => item.tone === "danger");
	const delivery = status?.delivery;
	const deliveryLabel = !delivery
		? "delivery ledger not reported"
		: delivery.pending === 0
			? "deliveries clear"
			: `${pluralise(delivery.pending, "delivery", "deliveries")} pending · oldest ${formatDuration(delivery.oldestPendingAgeMs ?? 0)}`;
	const context = status?.contextDiff;
	const contextLabel = !context
		? "conversation diff not reported"
		: `${formatCount(context.unread)} unread · ${formatCount(context.expired)} expired · ${formatCount(context.truncated)} truncated`;

	return {
		key: "status",
		tone: status ? (danger ? "danger" : "ok") : "danger",
		state: status ? "alive" : "unreachable",
		fields: {
			alive: status
				? startedAt
					? `alive ${formatDuration(now.getTime() - startedAt.getTime())}`
					: "alive"
				: "gateway unreachable",
			sessions: sessionCount === null ? "sessions unknown" : pluralise(sessionCount, "session"),
			working: working === 0 ? "idle" : `${formatCount(working)} working`,
			attention: attention.length === 0 ? "nothing needs you" : `⚠ ${pluralise(attention.length, "item")} needs you`,
			delivery: deliveryLabel,
			context: contextLabel,
			profile: status ? `profile ${status.profileVersion}` : (statusError ?? "no answer from the socket"),
			stream: `data ${formatClockSeconds(now)}`,
		},
		tones: {
			alive: status ? "ok" : "danger",
			attention: attention.length === 0 ? "muted" : danger ? "danger" : "warn",
			delivery: !delivery ? "muted" : delivery.pending === 0 ? "ok" : "warn",
			context: !context ? "muted" : context.unread > 0 || context.expired > 0 || context.truncated > 0 ? "warn" : "ok",
			working: working === 0 ? "muted" : "active",
		},
	};
}

export type SnapshotDeps = {
	readonly request: GatewayRequest;
	readonly turns: TurnTracker;
	readonly now?: () => Date;
};

/**
 * Project rows, or degrade the panel that owns them.
 *
 * The gateway's vocabulary can legitimately move ahead of this console's copy of
 * the protocol, and one unfamiliar record must never be able to take the whole
 * page down. Formatting is written not to throw; this is the backstop that makes
 * that a property of the page rather than a hope about the formatter.
 */
function project<T>(items: readonly T[], row: (item: T) => RowView): { rows: RowView[]; error: string | null } {
	const rows: RowView[] = [];
	for (const item of items) {
		try {
			rows.push(row(item));
		} catch (error) {
			return {
				rows,
				error: `this panel could not be rendered: ${error instanceof Error ? error.message : String(error)}`,
			};
		}
	}
	return { rows, error: null };
}

/**
 * A resolved answer of the wrong shape is as much a failure as a rejected one,
 * and it must degrade a panel rather than take the page down. `read` catches the
 * rejection; these two narrow the success.
 */
function asList<T>(value: unknown): readonly T[] {
	return Array.isArray(value) ? (value as readonly T[]) : [];
}

function asMonitorSnapshot(value: unknown): MonitorSnapshot | null {
	if (typeof value !== "object" || value === null) return null;
	const monitor = (value as { monitor?: unknown }).monitor;
	if (typeof monitor !== "object" || monitor === null) return null;
	if (typeof (monitor as { monitorId?: unknown }).monitorId !== "string") return null;
	return {
		monitor: monitor as MonitorRecord,
		recentEvents: asList<MonitorEventRecord>((value as { recentEvents?: unknown }).recentEvents),
	};
}

export async function buildSnapshot(deps: SnapshotDeps): Promise<ConsoleSnapshot> {
	const now = deps.now?.() ?? new Date();
	const [status, sessions, monitors] = await Promise.all([
		read<GatewayStatusResult>(deps.request, "gateway.status"),
		read<SessionListResult>(deps.request, "session.list"),
		read<{ monitors: readonly MonitorRecord[] }>(deps.request, "monitor.list"),
	]);

	const monitorList = asList<MonitorRecord>(monitors.value?.monitors).filter(
		(monitor) => typeof monitor?.monitorId === "string",
	);
	const inspected = await Promise.all(
		monitorList.slice(0, MONITOR_INSPECT_LIMIT).map((monitor) =>
			read<{ monitor: MonitorRecord; recentEvents: readonly MonitorEventRecord[] }>(deps.request, "monitor.inspect", {
				monitorId: monitor.monitorId,
			}),
		),
	);
	const eventsById = new Map<string, readonly MonitorEventRecord[]>();
	const monitorSnapshots: MonitorSnapshot[] = [];
	for (const outcome of inspected) {
		const snapshot = asMonitorSnapshot(outcome.value);
		if (!snapshot) continue;
		eventsById.set(snapshot.monitor.monitorId, snapshot.recentEvents);
		monitorSnapshots.push(snapshot);
	}

	const attention = buildAttention(status.value, monitorSnapshots, now);

	deps.turns.prune();
	const turnRows = deps.turns.list().map((turn) => turnRow(turn, deps.turns.stateOf(turn), now));

	const sessionList = asList<SessionListResult["sessions"][number]>(sessions.value?.sessions).filter(
		(session) => typeof session?.origin?.platform === "string",
	);
	const projectedSessions = project(sessionList, sessionRow);
	const projectedMonitors = project(monitorList, (monitor) =>
		monitorRow(monitor, eventsById.get(monitor.monitorId) ?? null, now),
	);

	const reachable = status.value !== null;

	return {
		at: now.toISOString(),
		gateway: { reachable, error: reachable ? null : status.error },
		status: statusRow(
			status.value,
			status.error,
			sessions.value === null ? null : sessionList.length,
			deps.turns.activeCount,
			attention,
			now,
		),
		attention: {
			state: attention.length === 0 ? "empty" : "ready",
			note: "Nothing needs you.",
			rows: attention.map(attentionRow),
			gaps: ATTENTION_GAPS,
		},
		live: {
			state: turnRows.length === 0 ? "empty" : "ready",
			note: "No turn is running. Work that finished more than ten minutes ago has moved to history.",
			rows: turnRows,
			gaps: LIVE_GAPS,
		},
		conversation: {
			state: "blocked",
			note: "The conversation projection needs a transcript verb. Nothing is shown rather than something invented.",
			rows: [],
			gaps: CONVERSATION_GAPS,
		},
		sessions: {
			state:
				(sessions.error ?? projectedSessions.error) ? "error" : projectedSessions.rows.length === 0 ? "empty" : "ready",
			note: sessions.error ?? projectedSessions.error ?? "No session has been opened yet.",
			rows: projectedSessions.rows,
			gaps: SESSION_GAPS,
		},
		monitors: {
			state:
				(monitors.error ?? projectedMonitors.error) ? "error" : projectedMonitors.rows.length === 0 ? "empty" : "ready",
			note: monitors.error ?? projectedMonitors.error ?? "No monitor is registered. Create one from Operations below.",
			rows: projectedMonitors.rows,
			gaps: MONITOR_GAPS,
		},
		raw: {
			"gateway.status": status.value ?? { error: status.error },
			"session.list": sessions.value ?? { error: sessions.error },
			"monitor.list": monitors.value ?? { error: monitors.error },
		},
	};
}

/** Seven days, for the removal consequence panel's fire count. */
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

export type MutationConsequence = {
	readonly targetName: string;
	readonly headline: string;
	readonly facts: readonly { readonly label: string; readonly value: string }[];
	readonly warning: string;
	/** The action button's own text, so it restates the target it will act on. */
	readonly actionLabel: string;
};

/**
 * What actually happens if the operator goes through with it, computed from a
 * live read rather than restated from the form. This is the whole point of the
 * Review step: "this monitor has fired 43 times and posts to #ops" is what makes
 * the decision real.
 */
export async function buildMonitorConsequence(
	request: GatewayRequest,
	monitorId: string,
	operation: { readonly summary: string; readonly action?: string; readonly consequence?: string },
	now: Date,
): Promise<MutationConsequence> {
	const inspected = (await request("monitor.inspect", { monitorId })) as {
		monitor: MonitorRecord;
		recentEvents: readonly MonitorEventRecord[];
	};
	const monitor = inspected.monitor;
	const events = inspected.recentEvents;
	const recent = events.filter((event) => {
		const at = parseIso(event.firedAt);
		return at !== null && now.getTime() - at.getTime() <= SEVEN_DAYS_MS;
	});
	const latest = parseIso(events[0]?.firedAt);
	const bounded = events.length >= 100 ? " (of the last 100 events kept)" : "";

	return {
		targetName: monitor.name,
		headline: `${operation.summary}: ${monitor.name}`,
		facts: [
			{ label: "Fires", value: triggerSummary(monitor.trigger) },
			{ label: "Emits", value: monitor.eventTypes.join(", ") || "no declared types" },
			{
				label: "Posts to",
				value: monitor.channelTarget ? originLabel(monitor.channelTarget.origin) : "nowhere — no channel target",
			},
			{
				label: "History",
				value:
					recent.length === 0
						? `no events in the last 7 days${bounded}`
						: `fired ${pluralise(recent.length, "time")} in the last 7 days${bounded}, last ${
								latest ? `${latest.toLocaleDateString("en-CA")} ${formatClock(latest)}` : "unknown"
							}`,
			},
			{ label: "State", value: monitor.enabled ? "enabled" : "disabled" },
		],
		warning: operation.consequence ?? "",
		actionLabel: `${operation.action ?? operation.summary} ${monitor.name}`,
	};
}
