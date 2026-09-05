/**
 * I6a/I6c acceptance: scoped serviceability strikes (only broker-scope
 * daemon-resource outcomes of the current generation strike; cursor/pin errors,
 * unrelated successes and retired-generation errors never do) and the Q4 host
 * audit predicate (every negative in the plan -> none signalled; the one
 * positive -> reaped).
 */
import { expect, test } from "bun:test";
import { type BrokerOutcome, classifyFailure, ServiceabilityTracker } from "../src/orchestrator/broker-outcomes";
import {
	type AuditEvidence,
	auditHost,
	type HostCandidate,
	type IndexSnapshot,
	parseBrokerReapPolicy,
} from "../src/orchestrator/host-audit";

const daemon = { pid: 100, url: "ws://127.0.0.1:1/a" };
const outcome = (over: Partial<BrokerOutcome>): BrokerOutcome => ({
	scope: "session",
	op: "session.inspect",
	code: undefined,
	messageClass: "other",
	daemon,
	generation: 1,
	at: 0,
	ok: false,
	...over,
});

test("classifyFailure: capacity messages are broker-scope; cursor/pin codes and cursor-bearing invalid_input are session-local", () => {
	expect(
		classifyFailure({
			op: "session.inspect",
			code: "invalid_input",
			message: "session.list cursor capacity is exhausted",
			hasCursor: false,
		}),
	).toEqual({ scope: "broker", messageClass: "capacity" });
	expect(classifyFailure({ op: "x", code: "reconciliation_capacity", message: undefined, hasCursor: false })).toEqual({
		scope: "broker",
		messageClass: "capacity",
	});
	for (const code of ["snapshot_capacity_exceeded", "invalid_cursor", "cursor_expired"])
		expect(classifyFailure({ op: "session.tail", code, message: undefined, hasCursor: true })).toEqual({
			scope: "session",
			messageClass: "cursor",
		});
	expect(
		classifyFailure({ op: "session.tail", code: "invalid_input", message: "cursor does not match", hasCursor: true }),
	).toEqual({ scope: "session", messageClass: "cursor" });
	expect(
		classifyFailure({ op: "session.inspect", code: "session_unavailable", message: undefined, hasCursor: false }),
	).toEqual({ scope: "session", messageClass: "other" });
});

test("three broker-scope capacity failures within 60 s retire once; cooldown blocks a second retirement", () => {
	let now = 0;
	const tracker = new ServiceabilityTracker({ now: () => now });
	const capacity = () => outcome({ scope: "broker", messageClass: "capacity", code: "invalid_input" });
	expect(tracker.observe(capacity(), 1, new Set()).retire).toBe(false);
	expect(tracker.observe(capacity(), 1, new Set()).retire).toBe(false);
	expect(tracker.observe(capacity(), 1, new Set())).toMatchObject({ retire: true, strikes: 3, reason: "capacity" });
	for (let i = 0; i < 5; i++)
		expect(tracker.observe(capacity(), 1, new Set())).toMatchObject({ retire: false, reason: "cooldown" });
	now = 11 * 60_000;
	expect(tracker.observe(capacity(), 1, new Set()).retire).toBe(false);
});

test("per-session cursor/pin failures with healthy peers never strike", () => {
	const tracker = new ServiceabilityTracker();
	for (let i = 0; i < 10; i++)
		expect(
			tracker.observe(
				outcome({ scope: "session", messageClass: "cursor", code: "snapshot_capacity_exceeded", sessionId: `s${i}` }),
				1,
				new Set(),
			).retire,
		).toBe(false);
	expect(tracker.strikes).toBe(0);
});

test("interleaved unrelated successes do not reset a strike run, but a matching success does", () => {
	let now = 0;
	const tracker = new ServiceabilityTracker({ now: () => now });
	const capacity = () => outcome({ scope: "broker", messageClass: "capacity" });
	tracker.observe(capacity(), 1, new Set());
	tracker.observe(capacity(), 1, new Set());
	// A successful transcript query is not the same op class as the failing listing.
	tracker.observe(outcome({ ok: true, op: "query:transcript.list", scope: "session" }), 1, new Set());
	expect(tracker.strikes).toBe(2);
	// A successful inspect/list against the same daemon resets.
	tracker.observe(outcome({ ok: true, op: "session.inspect", scope: "session" }), 1, new Set());
	expect(tracker.strikes).toBe(0);
	tracker.observe(capacity(), 1, new Set());
	expect(tracker.observe(capacity(), 1, new Set()).retire).toBe(false);
});

test("a late error from a retired generation or a replaced daemon never strikes the current one", () => {
	const tracker = new ServiceabilityTracker();
	const capacity = (generation: number, d = daemon) =>
		outcome({ scope: "broker", messageClass: "capacity", generation, daemon: d });
	tracker.observe(capacity(2), 2, new Set());
	tracker.observe(capacity(2), 2, new Set());
	expect(tracker.observe(capacity(1), 2, new Set()).retire).toBe(false);
	// Identity change resets the run.
	tracker.observe(capacity(2, { pid: 200, url: "ws://127.0.0.1:2/b" }), 2, new Set());
	expect(tracker.strikes).toBe(1);
});

test("session_unavailable counts as broker-scope only for >= 2 distinct sessions the index reports live", () => {
	const tracker = new ServiceabilityTracker();
	const live = new Set(["a", "b", "c"]);
	const unavailable = (sessionId: string) => outcome({ code: "session_unavailable", sessionId });
	expect(tracker.observe(unavailable("a"), 1, live).retire).toBe(false);
	expect(tracker.strikes).toBe(0);
	expect(tracker.observe(unavailable("zzz-not-live"), 1, live).retire).toBe(false);
	expect(tracker.strikes).toBe(0);
	tracker.observe(unavailable("b"), 1, live);
	expect(tracker.strikes).toBe(1);
	tracker.observe(unavailable("c"), 1, live);
	expect(tracker.observe(unavailable("a"), 1, live).retire).toBe(true);
});

// ---- host audit (Q4) ---------------------------------------------------------

const DIR = "/private/agent";
const base = (over: Partial<HostCandidate> = {}): HostCandidate => ({
	pid: 500,
	ppid: 100,
	args: "gjc sdk session-host-internal",
	agentDirEnv: DIR,
	startedAtMs: 1_000,
	incarnation: "inc-1",
	...over,
});
const snapshot = (rows: IndexSnapshot["rows"], observedAt = 10_000): IndexSnapshot => ({
	observedAt,
	rows,
	stalenessMs: 15_000,
});
const evidence = (over: Partial<AuditEvidence> = {}): AuditEvidence => ({
	protectedSessions: new Set(),
	activeHostPids: new Set(),
	backfillDone: true,
	...over,
});
const now = 100_000;

test("positive: ownership-proven, incarnation-equal, dead-and-stale, unreferenced host is reaped", () => {
	const verdict = auditHost({
		candidate: base(),
		privateAgentDir: DIR,
		daemonPid: 100,
		snapshot: snapshot([
			{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 60_000, incarnation: "inc-1" },
		]),
		evidence: evidence(),
		policy: "index-dead-only",
		nowMs: now,
	});
	expect(verdict).toEqual({ kind: "reap", pid: 500, basis: "index-dead" });
});

test("negatives: none of the plan's cases is signalled", () => {
	const cases: Array<[string, Parameters<typeof auditHost>[0], string]> = [
		[
			"stale heartbeat + live accepted op",
			{
				candidate: base(),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([
					{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 60_000, incarnation: "inc-1" },
				]),
				evidence: evidence({ protectedSessions: new Set(["s1"]) }),
				policy: "index-dead-only",
				nowMs: now,
			},
			"admitted_work",
		],
		[
			"snapshot-only live host after compaction",
			{
				candidate: base(),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([{ sessionId: "s1", pid: 500, live: true, lastHeartbeatAt: now, incarnation: "inc-1" }]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"index_live",
		],
		[
			"heartbeat recent",
			{
				candidate: base(),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([
					{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 1_000, incarnation: "inc-1" },
				]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"heartbeat_recent",
		],
		[
			"PPID=1 live host",
			{
				candidate: base({ ppid: 1 }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([{ sessionId: "s1", pid: 500, live: true, lastHeartbeatAt: now, incarnation: "inc-1" }]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"index_live",
		],
		[
			"PID reuse (started after snapshot, no row)",
			{
				candidate: base({ startedAtMs: 20_000 }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"started_after_snapshot",
		],
		[
			"private-dir prefix collision",
			{
				candidate: base({ agentDirEnv: `${DIR}-other` }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"ownership_unproven",
		],
		[
			"malformed/unindexed evidence with active attempt host",
			{
				candidate: base(),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([]),
				evidence: evidence({ activeHostPids: new Set([500]) }),
				policy: "index-dead-only",
				nowMs: now,
			},
			"active_attempt_host",
		],
		[
			"incarnation not derivable",
			{
				candidate: base({ incarnation: undefined }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([
					{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 60_000, incarnation: "inc-1" },
				]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"incarnation_not_derivable",
		],
		[
			"incarnation mismatch",
			{
				candidate: base({ incarnation: "inc-9" }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([
					{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 60_000, incarnation: "inc-1" },
				]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"incarnation_mismatch",
		],
		[
			"foreign gjc process (no env)",
			{
				candidate: base({ agentDirEnv: undefined }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"ownership_unproven",
		],
		[
			"the daemon itself",
			{
				candidate: base({ pid: 100 }),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: snapshot([]),
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"not_a_host",
		],
		[
			"index unavailable",
			{
				candidate: base(),
				privateAgentDir: DIR,
				daemonPid: 100,
				snapshot: undefined,
				evidence: evidence(),
				policy: "index-dead-only",
				nowMs: now,
			},
			"index_unavailable",
		],
	];
	for (const [name, input, reason] of cases) {
		const verdict = auditHost(input);
		expect([name, verdict.kind, verdict.kind === "skip" ? verdict.reason : ""]).toEqual([name, "skip", reason]);
	}
});

test("first boot after upgrade is observe-only: unknown => protect, would-reap reported", () => {
	const verdict = auditHost({
		candidate: base(),
		privateAgentDir: DIR,
		daemonPid: 100,
		snapshot: snapshot([
			{ sessionId: "s1", pid: 500, live: false, lastHeartbeatAt: now - 60_000, incarnation: "inc-1" },
		]),
		evidence: evidence({ backfillDone: false }),
		policy: "index-dead-only",
		nowMs: now,
	});
	expect(verdict).toEqual({ kind: "observe", pid: 500, wouldReap: true, reason: "would_reap:index-dead" });
});

test("policy `all` reaps every env-proven host and nothing else; the parser refuses other values", () => {
	expect(
		auditHost({
			candidate: base(),
			privateAgentDir: DIR,
			daemonPid: 100,
			snapshot: undefined,
			evidence: evidence(),
			policy: "all",
			nowMs: now,
		}),
	).toEqual({ kind: "reap", pid: 500, basis: "policy-all" });
	expect(
		auditHost({
			candidate: base({ agentDirEnv: "/elsewhere" }),
			privateAgentDir: DIR,
			daemonPid: 100,
			snapshot: undefined,
			evidence: evidence(),
			policy: "all",
			nowMs: now,
		}),
	).toMatchObject({ kind: "skip", reason: "ownership_unproven" });
	expect(parseBrokerReapPolicy(undefined)).toBe("index-dead-only");
	expect(parseBrokerReapPolicy("all")).toBe("all");
	expect(() => parseBrokerReapPolicy("kill-everything")).toThrow(/brokerReap/);
});
