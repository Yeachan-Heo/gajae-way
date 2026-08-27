import type { GatewayStatusResult, MonitorEventRecord, MonitorRecord } from "@gajaeway/protocol";
import type { AuditEntry } from "../src/gate";
import { type AdminApp, type AdminServerOptions, createAdminApp, type GatewayRequest } from "../src/server";

export const FIXED_NOW = new Date("2026-08-27T14:02:31.000Z");

export const STATUS: GatewayStatusResult = {
	profileVersion: "v0.1",
	capabilities: ["gateway.core"],
	pid: 4242,
	startedAt: new Date(FIXED_NOW.getTime() - 4 * 86_400_000 - 6 * 3_600_000).toISOString(),
	schemaVersion: 8,
	sessions: { active: 2 },
	delivery: { pending: 0, oldestPendingAgeMs: null },
};

export const MONITOR: MonitorRecord = {
	monitorId: "mon-weekday-review-0001",
	name: "weekday-review",
	trigger: { kind: "cron", schedule: "30 8 * * 1-5" },
	eventTypes: ["review.due"],
	burstPolicy: "coalesce",
	channelTarget: { origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" } },
	enabled: true,
	createdAt: new Date(FIXED_NOW.getTime() - 20 * 86_400_000).toISOString(),
};

export function monitorEvent(overrides: Partial<MonitorEventRecord> = {}): MonitorEventRecord {
	return {
		eventId: "evt-0001",
		monitorId: MONITOR.monitorId,
		eventType: "review.due",
		firedAt: new Date(FIXED_NOW.getTime() - 3_600_000).toISOString(),
		stage: "delivered",
		...overrides,
	};
}

export const SESSIONS = {
	sessions: [
		{
			origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" } as const,
			createdAt: new Date(FIXED_NOW.getTime() - 9 * 86_400_000).toISOString(),
			lastActivityAt: new Date(FIXED_NOW.getTime() - 11 * 60_000).toISOString(),
			epoch: 2,
		},
		{
			origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const,
			createdAt: new Date(FIXED_NOW.getTime() - 86_400_000).toISOString(),
			lastActivityAt: null,
			epoch: 1,
		},
	],
};

export type Call = { readonly method: string; readonly params?: unknown };

export type Harness = {
	readonly app: AdminApp;
	fetch(path: string, init?: RequestInit): Promise<Response>;
	readonly calls: Call[];
	readonly audit: AuditEntry[];
	emit(event: string, payload: unknown): void;
	stop(): void;
};

export type HarnessOptions = {
	readonly request?: GatewayRequest;
	readonly monitors?: readonly MonitorRecord[];
	readonly events?: readonly MonitorEventRecord[];
	readonly status?: GatewayStatusResult | null;
	readonly gate?: AdminServerOptions["gate"];
	readonly now?: () => Date;
};

/**
 * A stub gateway answering every verb the console actually reads. Anything the
 * console asks for that is not listed here is a bug in the console, not in the
 * fixture, so the default is to throw.
 */
export function harness(options: HarnessOptions = {}): Harness {
	const calls: Call[] = [];
	const audit: AuditEntry[] = [];
	const monitors = options.monitors ?? [MONITOR];
	const events = options.events ?? [monitorEvent()];
	let listeners: ((event: string, payload: unknown) => void)[] = [];

	const request: GatewayRequest =
		options.request ??
		(async (method, params) => {
			calls.push({ method, params });
			switch (method) {
				case "gateway.status":
					if (options.status === null) throw new Error("gateway socket closed");
					return options.status ?? STATUS;
				case "session.list":
					return SESSIONS;
				case "monitor.list":
					return { monitors };
				case "monitor.inspect": {
					const monitorId = (params as { monitorId?: string } | undefined)?.monitorId;
					const monitor = monitors.find((candidate) => candidate.monitorId === monitorId);
					if (!monitor) throw new Error("unknown monitorId");
					return { monitor, recentEvents: events.filter((event) => event.monitorId === monitorId) };
				}
				default:
					return { method, echoed: params ?? null };
			}
		});

	const app = createAdminApp({
		request,
		now: options.now ?? (() => FIXED_NOW),
		reconcileMs: 0,
		gate: {
			audit: (entry) => {
				audit.push(entry);
			},
			...options.gate,
		},
		events: (handler) => {
			listeners.push(handler);
			return () => {
				listeners = listeners.filter((candidate) => candidate !== handler);
			};
		},
	});

	return {
		app,
		calls,
		audit,
		fetch: (path, init) => app.handler(new Request(`http://admin.test${path}`, init)),
		emit: (event, payload) => {
			for (const listener of listeners) listener(event, payload);
		},
		stop: () => app.stop(),
	};
}

export const post = (body: unknown): RequestInit => ({
	method: "POST",
	headers: { "content-type": "application/json" },
	body: JSON.stringify(body),
});
