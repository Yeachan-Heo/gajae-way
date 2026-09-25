import { describe, expect, test } from "bun:test";
import { TurnTracker } from "../src/turns";
import { buildMonitorConsequence, buildSnapshot, type ConsoleSnapshot } from "../src/view";
import { FIXED_NOW, MONITOR, monitorEvent, SESSIONS, STATUS } from "./fixture";

type Stub = { readonly request: (method: string, params?: unknown) => Promise<unknown>; readonly calls: string[] };

function stub(overrides: Record<string, unknown | (() => never)> = {}): Stub {
	const calls: string[] = [];
	const defaults: Record<string, unknown> = {
		"gateway.status": STATUS,
		"session.list": SESSIONS,
		"monitor.list": { monitors: [MONITOR] },
		"monitor.inspect": { monitor: MONITOR, recentEvents: [monitorEvent()] },
	};
	return {
		calls,
		request: async (method) => {
			calls.push(method);
			const answer = method in overrides ? overrides[method] : defaults[method];
			if (typeof answer === "function") return (answer as () => never)();
			if (answer === undefined) throw new Error(`unexpected verb ${method}`);
			return answer;
		},
	};
}

function snapshot(overrides?: Record<string, unknown | (() => never)>): Promise<ConsoleSnapshot> {
	const backend = stub(overrides);
	return buildSnapshot({
		request: backend.request,
		turns: new TurnTracker(() => FIXED_NOW.getTime()),
		now: () => FIXED_NOW,
	});
}

describe("status bar", () => {
	test("answers alive, sessions, working and needs-me in that order", async () => {
		const state = await snapshot();
		expect(state.status.fields).toMatchObject({
			alive: "alive 4d 6h",
			sessions: "2 sessions",
			working: "idle",
			attention: "nothing needs you",
			delivery: "deliveries clear",
			context: "3 unread · 287 expired · 12 truncated",
			profile: "profile v0.1",
		});
		expect(state.status.tone).toBe("ok");
	});

	test("carries no diagnostics: pid and schemaVersion are not glance-level facts", async () => {
		const state = await snapshot();
		const rendered = Object.values(state.status.fields).join(" ");
		expect(rendered).not.toContain("4242");
		expect(rendered).not.toContain("schemaVersion");
	});

	test("an unreachable gateway says so instead of showing a stale-looking alive", async () => {
		const state = await snapshot({
			"gateway.status": () => {
				throw new Error("gateway socket closed");
			},
		});
		expect(state.gateway).toEqual({ reachable: false, error: "gateway socket closed" });
		expect(state.status.fields.alive).toBe("gateway unreachable");
		expect(state.status.fields.profile).toBe("gateway socket closed");
		expect(state.status.tone).toBe("danger");
	});

	test("delivery pressure is stated with an age, not a bare count", async () => {
		const state = await snapshot({
			"gateway.status": {
				...STATUS,
				delivery: { pending: 2, oldestPendingAgeMs: 11 * 60_000, expired: 0, recentExpired: [], recentPending: [] },
			},
		});
		expect(state.status.fields.delivery).toBe("2 deliveries pending · oldest 11m");
		expect(state.status.fields.attention).toBe("⚠ 1 item needs you");
		expect(state.status.tone).toBe("danger");
	});

	test("expired count is visible in the status bar and raises attention", async () => {
		const state = await snapshot({
			"gateway.status": {
				...STATUS,
				delivery: {
					pending: 0,
					oldestPendingAgeMs: null,
					expired: 2,
					recentExpired: [
						{
							deliveryId: "delivery-1",
							originKey: "discord/channel/room",
							attempts: 5,
							expiredAt: "2026-08-27T14:00:00.000Z",
							lastError: "not_found",
						},
					],
					recentPending: [],
				},
			},
		});
		expect(state.status.fields.delivery).toBe("2 expired deliveries");
		expect(state.status.tone).toBe("danger");
		expect(state.attention.rows[0]?.fields.title).toBe("2 deliveries expired");
		expect(state.attention.rows[0]?.fields.detail).toBe("delivery-1 · discord/channel/room");
	});

	test("a gateway without a delivery ledger says the ledger is unreported, not zero", async () => {
		const state = await snapshot({ "gateway.status": { ...STATUS, delivery: undefined } });
		expect(state.status.fields.delivery).toBe("delivery ledger not reported");
		expect(state.status.tones?.delivery).toBe("muted");
	});
});

describe("panels", () => {
	test("every panel carries a designed note, never a blank box", async () => {
		const state = await snapshot();
		for (const panel of [state.attention, state.live, state.conversation, state.sessions, state.monitors]) {
			expect(panel.note.length).toBeGreaterThan(0);
			expect(["ready", "empty", "error", "blocked"]).toContain(panel.state);
		}
	});

	test("an empty attention queue is a designed sentence, not an absence", async () => {
		const state = await snapshot();
		expect(state.attention.state).toBe("empty");
		expect(state.attention.note).toBe("Nothing needs you.");
	});

	test("a failed read degrades one panel and leaves the rest correct", async () => {
		const state = await snapshot({
			"monitor.list": () => {
				throw new Error("monitor registry unavailable");
			},
		});
		expect(state.monitors.state).toBe("error");
		expect(state.monitors.note).toBe("monitor registry unavailable");
		expect(state.sessions.state).toBe("ready");
		expect(state.sessions.rows).toHaveLength(2);
	});

	test("an empty session list reads as a sentence", async () => {
		const state = await snapshot({ "session.list": { sessions: [] } });
		expect(state.sessions.state).toBe("empty");
		expect(state.sessions.note).toBe("No session has been opened yet.");
	});

	test("the conversation panel is blocked and names the gap rather than inventing data", async () => {
		const state = await snapshot();
		expect(state.conversation.state).toBe("blocked");
		expect(state.conversation.rows).toEqual([]);
		expect(state.conversation.gaps.map((gap) => gap.gap)).toEqual(["G2"]);
	});

	test("the live-work panel declares that it cannot backfill turns it did not witness", async () => {
		const state = await snapshot();
		expect(state.live.state).toBe("empty");
		expect(state.live.gaps.map((gap) => gap.gap)).toEqual(["G1", "G7"]);
	});
});

describe("session rows", () => {
	test("origins are humanised structurally, never dumped verbatim", async () => {
		const state = await snapshot();
		expect(state.sessions.rows[0]?.fields.title).toBe("discord channel · 1493…5762");
		expect(state.sessions.rows[1]?.fields.title).toBe("loopback console");
	});

	test("the full origin key stays available as the row's identity", async () => {
		const state = await snapshot();
		expect(state.sessions.rows[0]?.key).toBe("discord/channel/1493635653441945762");
		expect(state.sessions.rows[0]?.ages?.activity).toBe(SESSIONS.sessions[0]?.lastActivityAt ?? "");
	});

	test("a session that never spoke says so rather than showing an epoch-zero age", async () => {
		const state = await snapshot();
		expect(state.sessions.rows[1]?.fields.activity).toBe("no activity yet");
		expect(state.sessions.rows[1]?.ages?.activity).toBeUndefined();
	});

	test("an origin the protocol copy does not know is still rendered, not fatal", async () => {
		// The running deployment already serves `work/task` origins, which this
		// branch's ORIGIN_PLATFORMS/ORIGIN_KINDS do not contain. A console that
		// 500s on one unfamiliar row is worse than one that names it plainly.
		const state = await snapshot({
			"session.list": {
				sessions: [
					{
						origin: { platform: "work", kind: "task", conversationId: "smoke-1" },
						createdAt: "2026-08-26T07:36:32.288Z",
						lastActivityAt: "2026-08-26T07:36:32.290Z",
						epoch: 0,
					},
				],
			},
		});
		expect(state.sessions.state).toBe("ready");
		expect(state.sessions.rows[0]?.fields.title).toBe("work task · smoke-1");
		expect(state.sessions.rows[0]?.key).toBe("work/task/smoke-1");
	});

	test("a row that cannot be projected degrades its own panel and keeps the others", async () => {
		const state = await snapshot({
			"monitor.list": {
				monitors: [
					{
						...MONITOR,
						get trigger(): never {
							throw new Error("corrupt trigger");
						},
					},
				],
			},
		});
		expect(state.monitors.state).toBe("error");
		expect(state.monitors.note).toBe("this panel could not be rendered: corrupt trigger");
		expect(state.sessions.state).toBe("ready");
		expect(state.status.fields.alive).toBe("alive 4d 6h");
	});
});

describe("monitor rows", () => {
	test("a cron is summarised as a sentence with a computed next fire", async () => {
		const state = await snapshot();
		const row = state.monitors.rows[0];
		expect(row?.fields.trigger).toBe("weekdays 08:30");
		expect(row?.fields.next).toMatch(/^in \d/);
		expect(row?.fields.emits).toBe("review.due");
		expect(row?.fields.target).toBe("discord channel · 1493…5762");
		expect(row?.fields.outcome).toBe("✓ delivered");
		expect(row?.tone).toBe("ok");
	});

	test("a disabled monitor says it will not fire instead of showing a next time", async () => {
		const state = await snapshot({
			"monitor.list": { monitors: [{ ...MONITOR, enabled: false }] },
			"monitor.inspect": { monitor: { ...MONITOR, enabled: false }, recentEvents: [monitorEvent()] },
		});
		expect(state.monitors.rows[0]?.fields.next).toBe("paused — will not fire");
		expect(state.monitors.rows[0]?.state).toBe("disabled");
	});

	test("a monitor that never fired says so rather than showing a bogus outcome", async () => {
		const state = await snapshot({ "monitor.inspect": { monitor: MONITOR, recentEvents: [] } });
		expect(state.monitors.rows[0]?.fields.outcome).toBe("◌ never fired");
		expect(state.monitors.rows[0]?.fields.outcomeAge).toBe("—");
	});

	test("a monitor whose events could not be read admits it instead of claiming health", async () => {
		const state = await snapshot({
			"monitor.inspect": () => {
				throw new Error("nope");
			},
		});
		expect(state.monitors.rows[0]?.fields.outcome).toBe("◌ events unread");
	});

	test("a failed last event colours the row danger", async () => {
		const state = await snapshot({
			"monitor.inspect": { monitor: MONITOR, recentEvents: [monitorEvent({ stage: "failed" })] },
		});
		expect(state.monitors.rows[0]?.tone).toBe("danger");
		expect(state.monitors.rows[0]?.fields.outcome).toBe("✕ failed");
	});
});

describe("raw results", () => {
	test("the raw protocol payloads exist, but only under the raw key", async () => {
		const state = await snapshot();
		expect(Object.keys(state.raw)).toEqual(["gateway.status", "session.list", "monitor.list"]);
		const surfaces = JSON.stringify([state.status, state.attention, state.live, state.sessions, state.monitors]);
		expect(surfaces).not.toContain("schemaVersion");
		expect(surfaces).not.toContain("4242");
	});
});

describe("buildMonitorConsequence", () => {
	test("names the target and its real facts, computed from a live read", async () => {
		const backend = stub();
		const consequence = await buildMonitorConsequence(
			backend.request,
			MONITOR.monitorId,
			{ summary: "Remove a monitor", action: "Remove", consequence: "This cannot be undone." },
			FIXED_NOW,
		);
		expect(consequence.targetName).toBe("weekday-review");
		expect(consequence.headline).toBe("Remove a monitor: weekday-review");
		expect(consequence.facts.map((fact) => fact.label)).toEqual(["Fires", "Emits", "Posts to", "History", "State"]);
		expect(consequence.warning).toBe("This cannot be undone.");
		// The button restates what it will act on, in the imperative.
		expect(consequence.actionLabel).toBe("Remove weekday-review");
		expect(backend.calls).toEqual(["monitor.inspect"]);
	});

	test("counts only the last seven days, and says when the count is bounded", async () => {
		const old = monitorEvent({
			eventId: "old",
			firedAt: new Date(FIXED_NOW.getTime() - 30 * 86_400_000).toISOString(),
		});
		const consequence = await buildMonitorConsequence(
			stub({ "monitor.inspect": { monitor: MONITOR, recentEvents: [monitorEvent(), old] } }).request,
			MONITOR.monitorId,
			{ summary: "Remove a monitor", action: "Remove", consequence: "gone" },
			FIXED_NOW,
		);
		expect(consequence.facts.find((fact) => fact.label === "History")?.value).toContain(
			"fired 1 time in the last 7 days",
		);
		// A wall clock, not a raw ISO string: no primary surface prints serialisation.
		expect(consequence.facts.find((fact) => fact.label === "History")?.value).toMatch(
			/last \d{4}-\d{2}-\d{2} \d{2}:\d{2}$/,
		);
	});

	test("a monitor with no recent events says nothing fired rather than omitting the row", async () => {
		const consequence = await buildMonitorConsequence(
			stub({ "monitor.inspect": { monitor: MONITOR, recentEvents: [] } }).request,
			MONITOR.monitorId,
			{ summary: "Remove a monitor", action: "Remove", consequence: "gone" },
			FIXED_NOW,
		);
		expect(consequence.facts.find((fact) => fact.label === "History")?.value).toBe("no events in the last 7 days");
	});

	test("a monitor with no channel target says nowhere, not an empty cell", async () => {
		const consequence = await buildMonitorConsequence(
			stub({ "monitor.inspect": { monitor: { ...MONITOR, channelTarget: null }, recentEvents: [] } }).request,
			MONITOR.monitorId,
			{ summary: "Remove a monitor", action: "Remove", consequence: "gone" },
			FIXED_NOW,
		);
		expect(consequence.facts.find((fact) => fact.label === "Posts to")?.value).toBe("nowhere — no channel target");
	});
});
