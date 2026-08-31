import { describe, expect, test } from "bun:test";
import { projectRuntimeCycle, type RuntimeCycleSources } from "../src/ops/cycle";

const generatedAt = "2026-08-26T00:00:00.000Z";

function sources(overrides: Partial<RuntimeCycleSources> = {}): RuntimeCycleSources {
	const defaults: RuntimeCycleSources = {
		sessionRows: [],
		inboundCounts: new Map(),
		inboundPendingByOrigin: new Map(),
		contextByOrigin: new Map(),
		contextDiff: {
			unread: 0,
			expired: 0,
			truncated: 0,
			omittedOldestAt: null,
			omittedNewestAt: null,
			floorAt: null,
		},
		inFlightInbound: 0,
		pendingInbound: 0,
		unknownInboundStates: [],
		deliveryCounts: new Map(),
		unknownDeliveryStates: [],
		unsettledByOrigin: new Map(),
		memoryIntents: new Map(),
		monitorStages: new Map(),
		memoryClosing: false,
		instanceId: "test-instance",
	};
	const merged = { ...defaults, ...overrides };
	// Mirror the DB snapshot seam: the census total derives from the counts.
	return { ...merged, pendingInbound: overrides.pendingInbound ?? merged.inboundCounts.get("pending") ?? 0 };
}

const boundSession = {
	origin_key: "discord/dm/c1/peer=p1",
	origin_ref_json: JSON.stringify({ platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" }),
	gjc_session_id: "sess-1234567890abcdef",
	epoch: 3,
	created_at: "2026-08-01T00:00:00.000Z",
	last_activity_at: "2026-08-02T00:00:00.000Z",
	last_bootstrapped_epoch: 3,
	bootstrap_applied_at: "2026-08-01T00:01:00.000Z",
	bootstrap_sections_json: '["Current conversation metadata"]',
	bootstrap_byte_count: 512,
	bootstrap_truncated: 0,
	bootstrap_diagnostics_json: "[]",
};

describe("runtime cycle projection", () => {
	test("empty durable state projects idle with no gates", () => {
		const result = projectRuntimeCycle(sources(), generatedAt);
		expect(result.phase).toBe("idle");
		expect(result.gates).toEqual([]);
		expect(result.sessions).toEqual([]);
		expect(result.generatedAt).toBe(generatedAt);
		expect(result.instanceId).toBe("test-instance");
	});

	test("healthy bound session with no work projects idle and carries identity/provenance", () => {
		const result = projectRuntimeCycle(sources({ sessionRows: [boundSession] }), generatedAt);
		expect(result.phase).toBe("idle");
		expect(result.gates).toEqual([]);
		expect(result.sessions).toHaveLength(1);
		expect(result.sessions[0]).toMatchObject({
			originKey: "discord/dm/c1/peer=p1",
			epoch: 3,
			sessionId: "sess-1234567890abcdef",
			pendingInbound: 0,
			unsettledDeliveries: 0,
			oldestUnsettledAgeMs: null,
		});
		expect(result.sessions[0].origin).toMatchObject({ platform: "discord", kind: "dm" });
	});

	test("mid-rebind session (empty gjc session id) gates as stale identity, never healthy", () => {
		const result = projectRuntimeCycle(
			sources({ sessionRows: [{ ...boundSession, gjc_session_id: "", epoch: 4 }] }),
			generatedAt,
		);
		// Fail-closed: epoch is visible but identity is unbound; the operator must see it.
		expect(result.gates).toContain("stale_session_identity");
		expect(result.phase).toBe("degraded");
		expect(result.sessions[0].sessionId).toBe("");
		expect(result.sessions[0].epoch).toBe(4);
	});

	test("claimed inbound message projects dispatching", () => {
		const result = projectRuntimeCycle(sources({ inFlightInbound: 1 }), generatedAt);
		expect(result.phase).toBe("dispatching");
		expect(result.gates).toEqual([]);
	});

	test("pending (unclaimed) inbound also projects dispatching", () => {
		const result = projectRuntimeCycle(sources({ inboundCounts: new Map([["pending", 2]]) }), generatedAt);
		expect(result.phase).toBe("dispatching");
		expect(result.inFlightInbound).toBe(0);
	});

	test("unsettled delivery projects delivering; settled alone does not", () => {
		const delivering = projectRuntimeCycle(
			sources({ unsettledByOrigin: new Map([["k", { n: 1, oldestMs: 5_000 }]]) }),
			generatedAt,
		);
		expect(delivering.phase).toBe("delivering");
		const settled = projectRuntimeCycle(
			sources({
				deliveryCounts: new Map([
					["confirmed", 7],
					["expired", 2],
				]),
			}),
			generatedAt,
		);
		expect(settled.phase).toBe("idle");
		expect(settled.deliveries).toEqual({
			pending: 0,
			inflight: 0,
			confirmed: 7,
			failedAmbiguous: 0,
			expired: 2,
		});
	});

	test("pending inbound for an origin with no session row still counts in the census", () => {
		// First-message case: inboundEnqueue is durable before ensureSession ever runs,
		// so a session-summing census would read pending=0 while the phase says dispatching.
		const result = projectRuntimeCycle(sources({ inboundCounts: new Map([["pending", 3]]) }), generatedAt);
		expect(result.phase).toBe("dispatching");
		expect(result.pendingInbound).toBe(3);
		expect(result.inFlightInbound).toBe(0);
	});

	test("in-flight memory closure projects draining", () => {
		const result = projectRuntimeCycle(sources({ memoryClosing: true }), generatedAt);
		expect(result.phase).toBe("draining");
		expect(result.memoryClosing).toBe(true);
	});

	test("durable unsettled memory intents also project draining across a restart", () => {
		// queueDepth is in-memory and lost on restart; the durable census is not.
		const result = projectRuntimeCycle(
			sources({
				memoryIntents: new Map([
					["written", 1],
					["committed", 2],
				]),
			}),
			generatedAt,
		);
		expect(result.phase).toBe("draining");
		expect(result.memoryClosing).toBe(true);
		expect(result.memoryIntents).toMatchObject({ written: 1, committed: 2 });
	});

	test("quarantined memory intent gates memory_closure_blocked and degrades", () => {
		const result = projectRuntimeCycle(sources({ memoryIntents: new Map([["quarantined", 1]]) }), generatedAt);
		expect(result.gates).toContain("memory_closure_blocked");
		expect(result.phase).toBe("degraded");
	});

	test("failed monitor event gates monitor_settlement_failed and degrades", () => {
		const result = projectRuntimeCycle(
			sources({
				monitorStages: new Map([
					["failed", 1],
					["delivered", 5],
				]),
			}),
			generatedAt,
		);
		expect(result.gates).toContain("monitor_settlement_failed");
		expect(result.phase).toBe("degraded");
		expect(result.monitorEvents).toEqual([
			{ stage: "delivered", count: 5 },
			{ stage: "failed", count: 1 },
		]);
	});

	test("failed_ambiguous deliveries are unsettled work but not a gate by themselves", () => {
		const result = projectRuntimeCycle(
			sources({
				deliveryCounts: new Map([["failed_ambiguous", 1]]),
				unsettledByOrigin: new Map([["k", { n: 1, oldestMs: 60_000 }]]),
			}),
			generatedAt,
		);
		// Ambiguity is honest at-least-once semantics, not unknown settlement.
		expect(result.gates).toEqual([]);
		expect(result.phase).toBe("delivering");
		expect(result.deliveries.failedAmbiguous).toBe(1);
	});

	test("unknown delivery state gates delivery_settlement_unknown — never silently healthy", () => {
		const result = projectRuntimeCycle(
			sources({
				deliveryCounts: new Map([
					["pending", 1],
					["settled", 1],
				]),
				unknownDeliveryStates: ["settled"],
			}),
			generatedAt,
		);
		expect(result.gates).toContain("delivery_settlement_unknown");
		expect(result.phase).toBe("degraded");
	});

	test("unknown inbound queue state also gates delivery_settlement_unknown", () => {
		const result = projectRuntimeCycle(sources({ unknownInboundStates: ["poof"] }), generatedAt);
		expect(result.gates).toContain("delivery_settlement_unknown");
		expect(result.phase).toBe("degraded");
	});

	test("degraded dominates concurrent busy work — never reported as merely dispatching", () => {
		const result = projectRuntimeCycle(
			sources({
				inFlightInbound: 2,
				unsettledByOrigin: new Map([["k", { n: 3, oldestMs: 1_000 }]]),
				memoryIntents: new Map([["quarantined", 1]]),
			}),
			generatedAt,
		);
		expect(result.phase).toBe("degraded");
		expect(result.gates).toEqual(["memory_closure_blocked"]);
	});

	test("per-origin pending inbound and unsettled delivery ages attach to the right session", () => {
		const result = projectRuntimeCycle(
			sources({
				sessionRows: [boundSession],
				inboundPendingByOrigin: new Map([["discord/dm/c1/peer=p1", 2]]),
				unsettledByOrigin: new Map([["discord/dm/c1/peer=p1", { n: 1, oldestMs: 42_000 }]]),
			}),
			generatedAt,
		);
		expect(result.sessions[0].pendingInbound).toBe(2);
		expect(result.sessions[0].unsettledDeliveries).toBe(1);
		expect(result.sessions[0].oldestUnsettledAgeMs).toBe(42_000);
	});

	test("aggregate and per-origin context drift project without exposing message bodies", () => {
		const context = {
			unread: 4,
			expired: 287,
			truncated: 12,
			omittedOldestAt: "2026-08-27T00:00:00.000Z",
			omittedNewestAt: "2026-08-28T01:00:00.000Z",
			floorAt: "2026-08-28T02:00:00.000Z",
		};
		const result = projectRuntimeCycle(
			sources({
				sessionRows: [boundSession],
				contextByOrigin: new Map([[boundSession.origin_key, context]]),
				contextDiff: { ...context, floorAt: null },
			}),
			generatedAt,
		);
		expect(result.contextDiff).toEqual({ ...context, floorAt: null });
		expect(result.sessions[0]?.contextDiff).toEqual(context);
		expect(JSON.stringify(result)).not.toContain("private body");
	});

	test("an unparseable stored origin ref still surfaces the session instead of hiding it", () => {
		const result = projectRuntimeCycle(
			sources({ sessionRows: [{ ...boundSession, origin_ref_json: "{not json" }] }),
			generatedAt,
		);
		expect(result.sessions).toHaveLength(1);
		expect(result.sessions[0].originKey).toBe(boundSession.origin_key);
	});

	test("a bound terminal-origin row projects identifiers only (AC-16)", () => {
		const terminalSession = {
			...boundSession,
			origin_key: "loopback/loopback/terminal",
			origin_ref_json: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "terminal" }),
			gjc_session_id: "stub-session-9d3f",
		};
		const result = projectRuntimeCycle(sources({ sessionRows: [terminalSession] }), generatedAt);
		const row = result.sessions[0];

		expect(row?.originKey).toBe("loopback/loopback/terminal");
		expect(row?.sessionId).toBe("stub-session-9d3f");
		expect(row?.origin?.conversationId).toBe("terminal");
		expect(result.gates).toEqual([]);
		// Identifier-only: no lease holder, child pid, PTY state, or delivery health
		// may leak onto the view for this origin.
		for (const forbidden of ["lease", "holder", "childPid", "pty", "deliveryHealth"]) {
			expect(Object.keys(row ?? {})).not.toContain(forbidden);
		}
	});

	test("a terminal row mid-rebind is reported as stale identity, never healthy (AC-16)", () => {
		const rebinding = {
			...boundSession,
			origin_key: "loopback/loopback/terminal",
			origin_ref_json: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "terminal" }),
			gjc_session_id: "",
		};
		const result = projectRuntimeCycle(sources({ sessionRows: [rebinding] }), generatedAt);
		expect(result.gates).toContain("stale_session_identity");
		expect(result.sessions[0]?.sessionId).toBe("");
	});
});
