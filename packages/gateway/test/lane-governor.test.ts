import { afterEach, expect, setSystemTime, test } from "bun:test";
import { ProtocolError } from "@gajae-gateway/protocol";
import {
	appendAttempt,
	closeAttempt,
	createLaneJobRecord,
	type LaneJobRecord,
	newOpRef,
} from "@gajae-gateway/subsession";
import { LaneGovernor, laneJobIdentity } from "../src/orchestrator/lane-governor";
import { BrokerAuthorityError, GatewayDatabase } from "../src/store/db";
import { initializeTestBrokerAuthority, ScriptedSessionPort } from "./session-port.fake";

const NOW = Date.parse("2026-09-01T12:00:00.000Z");
const SESSION_ID = "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0";
let database: GatewayDatabase;
afterEach(() => {
	setSystemTime();
	database?.close();
});

function bind(name: string, activityAt: number, sessionId = `sess-${name}`): void {
	database.putSession(`work/task/${name}`, sessionId);
	try {
		setSystemTime(new Date(activityAt));
		database.updateActivity(`work/task/${name}`, "{}");
	} finally {
		setSystemTime();
	}
}

function persistJob(name: string, state: "done" | "aborted" | "running", open = false): void {
	const identity = laneJobIdentity(name);
	let record: LaneJobRecord = createLaneJobRecord({
		jobId: identity.jobId,
		branch: `work/${name}`,
		worktreePath: "/tmp/worker-repo",
		now: () => new Date(NOW),
	});
	if (open) {
		record = appendAttempt(record, {
			opRef: newOpRef("governor-test"),
			sessionId: SESSION_ID,
			startedAt: new Date(NOW - 120_000).toISOString(),
		});
	}
	record = { ...record, state };
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });
}

test("activeLanes includes only bound work origins and derives idle time from activity", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 30_000);
	bind("future", NOW + 1_000);
	database.putSession("work/task/no-activity", "sess-no-activity");
	database.putSession("work/task/unbound", "");
	database.putSession("discord/channel/a", "sess-other");
	const governor = new LaneGovernor({ database, sessionPort: new ScriptedSessionPort(), now: () => NOW });
	const lanes = governor.activeLanes();
	expect(lanes.map((lane) => lane.name).sort()).toEqual(["a", "future", "no-activity"]);
	expect(lanes.find((lane) => lane.name === "a")).toEqual({
		name: "a",
		sessionKey: "work/task/a",
		sessionId: "sess-a",
		lastActivityAt: new Date(NOW - 30_000).toISOString(),
		idleMs: 30_000,
		state: "unknown",
		attemptOpen: false,
	});
	expect(lanes.find((lane) => lane.name === "future")?.idleMs).toBe(0);
	expect(lanes.find((lane) => lane.name === "no-activity")?.idleMs).toBe(Number.POSITIVE_INFINITY);
});

test("admission admits existing lanes at capacity and reports new-name candidates idlest first", async () => {
	database = await GatewayDatabase.open(":memory:");
	const governor = new LaneGovernor({ database, sessionPort: new ScriptedSessionPort(), maxLanes: 2, now: () => NOW });
	bind("fresh", NOW - 1_000);
	expect(() => governor.assertAdmission("old")).not.toThrow();
	bind("old", NOW - 60_000);
	expect(() => governor.assertAdmission("fresh")).not.toThrow();
	let failure: unknown;
	try {
		governor.assertAdmission("new");
	} catch (error) {
		failure = error;
	}
	expect(failure).toBeInstanceOf(ProtocolError);
	expect(failure).toMatchObject({
		code: "lane_capacity",
		detail: {
			active: 2,
			maxLanes: 2,
			candidates: [
				{ name: "old", idleMs: 60_000, state: "unknown" },
				{ name: "fresh", idleMs: 1_000, state: "unknown" },
			],
		},
	});
});

test("retire closes the session in its job repo, clears the binding, bumps epoch, and logs", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW);
	persistJob("a", "done");
	const before = database.getSessionRecord("work/task/a");
	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({ database, sessionPort: port, now: () => NOW, log: (line) => logs.push(line) });
	expect(await governor.retire("a", "operator")).toEqual({
		retired: true,
		sessionKey: "work/task/a",
		sessionId: "sess-a",
		closed: true,
	});
	expect(port.closes).toEqual([{ sessionId: "sess-a", repo: "/tmp/worker-repo" }]);
	expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before!.epoch + 1 });
	expect(governor.activeLanes()).toEqual([]);
	expect(logs).toContain("lane_retired name=a session=sess-a reason=operator closed=true");
});

test("retire refuses unknown names and open attempts without closing or changing epochs", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("busy", NOW - 120_000, SESSION_ID);
	persistJob("busy", "running", true);
	const before = database.getSessionRecord("work/task/busy");
	const port = new ScriptedSessionPort();
	const governor = new LaneGovernor({ database, sessionPort: port, now: () => NOW });
	expect(await governor.retire("missing", "operator")).toMatchObject({
		retired: false,
		sessionKey: "work/task/missing",
	});
	expect(await governor.retire("busy", "operator")).toMatchObject({
		retired: false,
		sessionKey: "work/task/busy",
		reason: expect.stringContaining("attempt still open"),
	});
	expect(port.closes).toEqual([]);
	expect(database.getSessionRecord("work/task/busy")).toEqual(before);
	expect(database.getSessionRecord("work/task/missing")).toBeUndefined();
});

test("a failed close keeps the lane bound unless the broker proves the session gone", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW);
	const before = database.getSessionRecord("work/task/a");
	class FailingClosePort extends ScriptedSessionPort {
		override async close(input: { sessionId: string; repo: string }): Promise<void> {
			this.closes.push(input);
			throw new Error("broker unavailable");
		}
	}
	const port = new FailingClosePort();
	const logs: string[] = [];
	const governor = new LaneGovernor({ database, sessionPort: port, now: () => NOW, log: (line) => logs.push(line) });
	// Still live according to the broker: the slot is NOT released.
	port.setSessionState("sess-a", { live: true });
	const retained = await governor.retire("a", "operator");
	expect(retained.retired).toBe(false);
	expect(retained.retired === false && retained.reason).toMatch(/not proven gone/);
	expect(database.getSessionRecord("work/task/a")).toEqual(before);
	expect(governor.activeLanes().map((lane) => lane.name)).toEqual(["a"]);
	expect(logs.some((line) => line.startsWith("lane_close_failed name=a") && line.endsWith("action=retained"))).toBe(
		true,
	);
	expect(logs.some((line) => line.startsWith("lane_retired"))).toBe(false);
	// Broker says the session is dead: the binding may clear.
	port.setSessionState("sess-a", { live: false });
	expect(await governor.retire("a", "operator")).toEqual({
		retired: true,
		sessionKey: "work/task/a",
		sessionId: "sess-a",
		closed: false,
	});
	expect(port.closes).toHaveLength(2);
	expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before!.epoch + 1 });
	expect(governor.activeLanes()).toEqual([]);
	expect(logs).toContain("lane_retired name=a session=sess-a reason=operator closed=false");
});

test("a corrupt lane-job record fails closed: counted, never retired", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000);
	const identity = laneJobIdentity("a");
	database.putLaneJob({
		jobId: identity.jobId,
		laneKey: identity.laneKey,
		state: "running",
		createdAt: new Date(NOW).toISOString(),
		updatedAt: new Date(NOW).toISOString(),
		lane: { branch: "work/a", worktreePath: "/tmp/worker-repo" },
		json: "{not json",
	});
	const port = new ScriptedSessionPort();
	// Set session as live to prevent sweep second pass from releasing it
	port.setSessionState("sess-a", { live: true });
	const governor = new LaneGovernor({ database, sessionPort: port, maxLanes: 1, idleRetireMs: 60_000, now: () => NOW });
	const lane = governor.activeLanes()[0];
	expect(lane).toMatchObject({ name: "a", state: "corrupt", attemptOpen: true });
	expect(() => governor.assertAdmission("b")).toThrow(ProtocolError);
	const outcome = await governor.retire("a", "operator");
	expect(outcome.retired).toBe(false);
	expect(outcome.retired === false && outcome.reason).toMatch(/corrupt/);
	expect(await governor.sweep()).toBe(0);
	expect(port.closes).toEqual([]);
	expect(database.getSessionRecord("work/task/a")?.sessionId).toBe("sess-a");
});

test("an attempt the ledger ended without broker proof is retired only once the broker reports it terminal", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, SESSION_ID);
	const identity = laneJobIdentity("a");
	const opRef = newOpRef("governor-reaped");
	let record: LaneJobRecord = createLaneJobRecord({
		jobId: identity.jobId,
		branch: "work/a",
		worktreePath: "/tmp/worker-repo",
		now: () => new Date(NOW - 300_000),
	});
	record = appendAttempt(record, { opRef, sessionId: SESSION_ID, startedAt: new Date(NOW - 240_000).toISOString() });
	record = closeAttempt({
		record,
		opRef,
		endState: "attempt_ended",
		errorCode: "gateway_turn_reaped",
		endedAt: new Date(NOW - 120_000).toISOString(),
	});
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });
	const port = new ScriptedSessionPort();
	port.setSessionState(SESSION_ID, { live: true, repo: "/tmp/worker-repo" });
	const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 60_000, now: () => NOW });
	// The scripted broker has no record of the op: `unknown` on a live session is not terminal.
	const held = await governor.retire("a", "idle");
	expect(held.retired).toBe(false);
	expect(held.retired === false && held.reason).toMatch(/broker reports unknown/);
	expect(port.closes).toEqual([]);
	expect(await governor.sweep()).toBe(0);
	// Once the session itself is dead, nothing can still be running: settled.
	port.setSessionState(SESSION_ID, { live: false });
	expect((await governor.retire("a", "idle")).retired).toBe(true);
});

test("sweep leaves a lane alone when it was rebound between nomination and the lock", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, "sess-old");
	const port = new ScriptedSessionPort();
	const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 60_000, now: () => NOW });
	// Simulate a work.run that rebinds the lane while the sweep's snapshot is in flight.
	const outcome = await governor.retire("a", "idle", { sessionId: "sess-other", reason: "idle", now: NOW });
	expect(outcome.retired).toBe(false);
	expect(outcome.retired === false && outcome.reason).toMatch(/rebound/);
	expect(port.closes).toEqual([]);
	expect(database.getSessionRecord("work/task/a")?.sessionId).toBe("sess-old");
});

test("sweep retires idle and terminal jobs but preserves fresh lanes and open attempts", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("idle", NOW - 60_001);
	bind("boundary", NOW - 60_000);
	bind("fresh", NOW - 59_999);
	bind("done", NOW);
	bind("aborted", NOW);
	bind("busy", NOW - 120_000, SESSION_ID);
	persistJob("done", "done");
	persistJob("aborted", "aborted");
	persistJob("busy", "running", true);
	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});
	expect(await governor.sweep()).toBe(4);
	expect(
		governor
			.activeLanes()
			.map((lane) => lane.name)
			.sort(),
	).toEqual(["busy", "fresh"]);
	expect(port.closes.map((close) => close.sessionId).sort()).toEqual([
		"sess-aborted",
		"sess-boundary",
		"sess-done",
		"sess-idle",
	]);
	expect(logs).toContain("lane_retired name=done session=sess-done reason=job_done closed=true");
	expect(logs).toContain("lane_retired name=idle session=sess-idle reason=idle closed=true");
	expect(await governor.sweep()).toBe(0);
});

test("a ledger `failed` attempt is local evidence only: retirement waits for broker terminality", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, SESSION_ID);
	const identity = laneJobIdentity("a");
	const opRef = newOpRef("governor-failed");
	let record: LaneJobRecord = createLaneJobRecord({
		jobId: identity.jobId,
		branch: "work/a",
		worktreePath: "/tmp/worker-repo",
		now: () => new Date(NOW - 300_000),
	});
	record = appendAttempt(record, { opRef, sessionId: SESSION_ID, startedAt: new Date(NOW - 240_000).toISOString() });
	// The gateway recorded `failed` because its status poll threw after an
	// accepted send; the broker operation itself may still be running.
	record = closeAttempt({ record, opRef, endState: "failed", endedAt: new Date(NOW - 120_000).toISOString() });
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });
	class InFlightPort extends ScriptedSessionPort {
		override async status(input: { sessionId: string; repo: string; opRef: string }) {
			return { operationRef: input.opRef, status: { status: "in_flight" as const }, summaryCompleted: false };
		}
	}
	const port = new InFlightPort();
	port.setSessionState(SESSION_ID, { live: true, repo: "/tmp/worker-repo" });
	const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 60_000, now: () => NOW });
	const held = await governor.retire("a", "operator");
	expect(held.retired).toBe(false);
	expect(held.retired === false && held.reason).toMatch(/ended failed in the ledger but the broker reports in_flight/);
	expect(port.closes).toEqual([]);
	expect(await governor.sweep()).toBe(0);
});

test("an existing lane-job row with an empty body is corrupt, not absent", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000);
	const identity = laneJobIdentity("a");
	database.putLaneJob({
		jobId: identity.jobId,
		laneKey: identity.laneKey,
		state: "running",
		createdAt: new Date(NOW).toISOString(),
		updatedAt: new Date(NOW).toISOString(),
		lane: { branch: "work/a", worktreePath: "/tmp/worker-repo" },
		json: "",
	});
	const port = new ScriptedSessionPort();
	// Set session as live to prevent sweep second pass from releasing it
	port.setSessionState("sess-a", { live: true });
	const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 60_000, now: () => NOW });
	expect(governor.activeLanes()[0]).toMatchObject({ state: "corrupt", attemptOpen: true });
	expect((await governor.retire("a", "operator")).retired).toBe(false);
	expect(await governor.sweep()).toBe(0);
	expect(port.closes).toEqual([]);
});

test("sweep leaves a lane alone when it was reused on the same session between nomination and the lock", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, "sess-a");
	const port = new ScriptedSessionPort();
	// Set session as live to prevent sweep second pass from releasing it
	port.setSessionState("sess-a", { live: true });
	let clock = NOW;
	const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 60_000, now: () => clock });
	// Hold the lane lock while the sweep nominates `a` as idle, then make the
	// lane freshly active (same session id) before the sweep's retire runs.
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const holder = port.runExclusive("work/task/a", async () => {
		await gate;
		clock = NOW + 1_000;
		database.updateActivity("work/task/a", "{}");
	});
	const sweeping = governor.sweep(NOW);
	release();
	await holder;
	expect(await sweeping).toBe(0);
	expect(port.closes).toEqual([]);
	expect(database.getSessionRecord("work/task/a")?.sessionId).toBe("sess-a");
});

test("quarantined historical names refuse admission and retirement before recovery or SDK controls", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("old", NOW, SESSION_ID);
	persistJob("old", "running", true);
	const history = database.laneJobJson(laneJobIdentity("old").jobId);
	database.cutoverBrokerAuthority({
		expectedAuthority: null,
		targetAuthority: { canonicalAgentDir: "/tmp/global-agent", identity: "shared-broker" },
		evidence: "test operator quarantined old work",
		disposition: "quarantine",
	});
	const port = new ScriptedSessionPort();
	let recoveryCalls = 0;
	let lockCalls = 0;
	port.runExclusive = async (_key, work) => {
		lockCalls++;
		return work();
	};
	const governor = new LaneGovernor({ database, sessionPort: port });
	governor.setRecoveryGate(async () => {
		recoveryCalls++;
	});
	expect(() => governor.assertAdmission("old")).toThrow(ProtocolError);
	try {
		governor.assertAdmission("old");
	} catch (error) {
		expect(error).toMatchObject({
			code: "verb_failed",
			detail: {
				reasonCode: "broker_authority_quarantined",
				jobId: laneJobIdentity("old").jobId,
				name: "old",
			},
		});
	}
	expect(await governor.retire("old", "operator")).toEqual({
		retired: false,
		sessionKey: "work/task/old",
		reason: "broker_authority_quarantined",
	});
	expect(recoveryCalls).toBe(0);
	expect(lockCalls).toBe(0);
	expect(port.closes).toEqual([]);
	expect(database.laneJobJson(laneJobIdentity("old").jobId)).toBe(history);
	expect(() => governor.assertAdmission("fresh")).not.toThrow();
});

test("#340: broker mode - lane with no job record must use binding repo from workLaneRepoBySessionId", async () => {
	// Broker mode setup
	const AUTHORITY = { canonicalAgentDir: "/broker/agent", identity: "test-broker" };
	database = await GatewayDatabase.open(":memory:");
	database.assertBrokerAuthority(AUTHORITY, { initializeEmpty: true });

	// Lane A: no job record in broker mode - CRITICAL #340 case
	const LANE_A_REPO = "/binding/repo/a";
	const sessionIdA = "sess-broker-a-no-job";
	const bindingA = {
		sessionId: sessionIdA,
		originKey: "work/task/a",
		epoch: 0,
		repo: LANE_A_REPO,
		authority: AUTHORITY,
	};
	database.recordOwnedBinding(bindingA);

	// Lane B: with job record in broker mode
	const LANE_B_REPO = "/binding/repo/b";
	const sessionIdB = "sess-broker-b-has-job";
	const bindingB = {
		sessionId: sessionIdB,
		originKey: "work/task/b",
		epoch: 0,
		repo: LANE_B_REPO,
		authority: AUTHORITY,
	};
	database.recordOwnedBinding(bindingB);

	// Add activity to both (update sessions created by recordOwnedBinding)
	setSystemTime(new Date(NOW - 70_000));
	database.updateActivity("work/task/a", "{}");
	database.updateActivity("work/task/b", "{}");
	setSystemTime();

	// Add job record for lane B ONLY (lane A has NO job - the critical #340 case)
	const idB = laneJobIdentity("b");
	let recB = createLaneJobRecord({
		jobId: idB.jobId,
		branch: "work/b",
		worktreePath: "/tmp/job-b-worktree",
		now: () => new Date(NOW),
	});
	recB = { ...recB, state: "done" };
	database.putLaneJob({ ...recB, laneKey: idB.laneKey, json: JSON.stringify(recB) });

	// Port that enforces broker authority: throws if close() repo doesn't match binding
	class BrokerEnforcingPort extends ScriptedSessionPort {
		override async close(input: { sessionId: string; repo: string }): Promise<void> {
			this.closes.push(input);
			// Enforce broker ownership:
			// - Lane A (no job): MUST use binding repo (this is the #340 fix)
			// - Lane B (has job): would use job worktree; allow either binding or job repo
			if (input.sessionId === sessionIdA && input.repo !== LANE_A_REPO) {
				throw new BrokerAuthorityError("unowned_session");
			}
		}
	}

	const port = new BrokerEnforcingPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// CRITICAL: Both lanes must retire successfully
	// Lane A (no job record) MUST use binding repo via workLaneRepoBySessionId
	// If binding lookup is replaced with undefined, close will throw unowned_session
	const retired = await governor.sweep();
	expect(retired).toBe(2);

	// Verify correct repos were used in close() calls
	expect(port.closes.length).toBe(2);
	const closeA = port.closes.find((c) => c.sessionId === sessionIdA);
	const closeB = port.closes.find((c) => c.sessionId === sessionIdB);

	// CRITICAL #340 TEST:
	// Lane A (no job record) must use binding repo via workLaneRepoBySessionId
	// If this lookup is replaced with undefined, close will throw unowned_session
	expect(closeA?.repo).toBe(LANE_A_REPO);

	// Lane B can use either job worktree or binding repo
	expect(closeB).toBeDefined();

	expect(logs.some((line) => line.includes("lane_retired name=a"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=b"))).toBe(true);
});

test("#340: per-lane error handling continues sweep for remaining lanes", async () => {
	database = await GatewayDatabase.open(":memory:");

	// Lane A: will fail during lock
	bind("a", NOW - 70_000);
	// Lane B: should retire despite A's error
	bind("b", NOW - 70_000);
	persistJob("b", "done");

	class FailingLockPort extends ScriptedSessionPort {
		override async runExclusive(key: string, work: () => Promise<any>): Promise<any> {
			if (key === "work/task/a") {
				throw new Error("lock acquisition failed");
			}
			return await super.runExclusive(key, work);
		}
	}

	const port = new FailingLockPort();
	// Set session state: A is live (lock fails, doesn't get retired in first pass)
	// B's state defaults to live=false (will be retired)
	port.setSessionState("sess-a", { live: true });
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	const retired = await governor.sweep();
	// B retired by first pass (done job), A retained by lock failure and live session
	expect(retired).toBe(1);

	expect(port.closes.length).toBe(1);
	expect(port.closes[0].sessionId).toBe("sess-b");

	// A stays bound because lock failed and session is live
	expect(database.getSessionRecord("work/task/a")?.sessionId).not.toBe("");
	// B is retired
	expect(database.getSessionRecord("work/task/b")?.sessionId).toBe("");

	expect(logs.some((line) => line.includes("lane_retire_failed name=a"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=b"))).toBe(true);
});

test("#340: per-lane error handling continues sweep when retire() throws", async () => {
	database = await GatewayDatabase.open(":memory:");

	// Lane A: will fail during lock acquisition
	bind("a", NOW - 70_000);
	// Lane B: should retire despite A's error
	bind("b", NOW - 70_000);
	persistJob("b", "done");

	class FailingLockPort extends ScriptedSessionPort {
		override async runExclusive(key: string, work: () => Promise<any>): Promise<any> {
			if (key === "work/task/a") {
				throw new Error("lock acquisition failed");
			}
			return await super.runExclusive(key, work);
		}
	}

	const port = new FailingLockPort();
	// A is live (lock fails, doesn't get retired); B defaults to dead
	port.setSessionState("sess-a", { live: true });
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	const retired = await governor.sweep();
	expect(retired).toBe(1); // Only B retired

	expect(port.closes.length).toBe(1);
	expect(port.closes[0].sessionId).toBe("sess-b");

	// A stays bound (lock failed, live session)
	expect(database.getSessionRecord("work/task/a")?.sessionId).not.toBe("");
	// B is retired
	expect(database.getSessionRecord("work/task/b")?.sessionId).toBe("");

	expect(logs.some((line) => line.includes("lane_retire_failed name=a"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=b"))).toBe(true);
});

test("#360: session_unavailable status error is treated like unknown: settled when session dead/disowned", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, SESSION_ID);
	const identity = laneJobIdentity("a");
	const opRef = newOpRef("governor-session-unavail");
	let record: LaneJobRecord = createLaneJobRecord({
		jobId: identity.jobId,
		branch: "work/a",
		worktreePath: "/tmp/worker-repo",
		now: () => new Date(NOW - 300_000),
	});
	record = appendAttempt(record, { opRef, sessionId: SESSION_ID, startedAt: new Date(NOW - 240_000).toISOString() });
	record = closeAttempt({
		record,
		opRef,
		endState: "attempt_ended",
		errorCode: "gateway_turn_reaped",
		endedAt: new Date(NOW - 120_000).toISOString(),
	});
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });

	class SessionUnavailablePort extends ScriptedSessionPort {
		override async status(): Promise<any> {
			throw new Error('gjc sdk session status reported failure: {"code":"session_unavailable"}');
		}
	}

	const port = new SessionUnavailablePort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// When session is dead, session_unavailable error should be treated as settled (via liveness)
	port.setSessionState(SESSION_ID, { live: false, repo: "/tmp/worker-repo" });
	const deadOutcome = await governor.retire("a", "operator");
	expect(deadOutcome.retired).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=a"))).toBe(true);
});

test("#360: session_unavailable with live session does not settle", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("a", NOW - 10 * 60_000, SESSION_ID);
	const identity = laneJobIdentity("a");
	const opRef = newOpRef("governor-session-unavail-live");
	let record: LaneJobRecord = createLaneJobRecord({
		jobId: identity.jobId,
		branch: "work/a",
		worktreePath: "/tmp/worker-repo",
		now: () => new Date(NOW - 300_000),
	});
	record = appendAttempt(record, { opRef, sessionId: SESSION_ID, startedAt: new Date(NOW - 240_000).toISOString() });
	record = closeAttempt({
		record,
		opRef,
		endState: "attempt_ended",
		errorCode: "gateway_turn_reaped",
		endedAt: new Date(NOW - 120_000).toISOString(),
	});
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });

	class SessionUnavailablePort2 extends ScriptedSessionPort {
		override async status(): Promise<any> {
			throw new Error('gjc sdk session status reported failure: {"code":"session_unavailable"}');
		}
	}

	const port = new SessionUnavailablePort2();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// When session is live, session_unavailable error should NOT settle
	port.setSessionState(SESSION_ID, { live: true, repo: "/tmp/worker-repo" });
	const liveOutcome = await governor.retire("a", "operator");
	expect(liveOutcome.retired).toBe(false);
	expect(liveOutcome.retired === false && liveOutcome.reason).toMatch(/broker reports/);
});

test("#360: forceRetire refuses live sessions but closes dead ones", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("live", NOW, "sess-live");
	persistJob("live", "running", false);
	bind("dead", NOW, "sess-dead");
	persistJob("dead", "running", false);

	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// Force retire on live session: should refuse
	port.setSessionState("sess-live", { live: true });
	const liveRefused = await governor.forceRetire("live");
	expect(liveRefused.retired).toBe(false);
	expect(liveRefused.retired === false && liveRefused.reason).toMatch(/still live/);
	expect(logs.some((line) => line.includes("lane_force_retire_rejected"))).toBe(true);

	// Force retire on dead session: should succeed
	port.setSessionState("sess-dead", { live: false });
	const deadRetired = await governor.forceRetire("dead");
	expect(deadRetired.retired).toBe(true);
	if (deadRetired.retired) {
		expect(deadRetired.forced).toBe(true);
	}
	expect(logs.some((line) => line.includes("lane_retired name=dead") && line.includes("reason=operator_force"))).toBe(
		true,
	);
	expect(port.closes).toContainEqual({ sessionId: "sess-dead", repo: "/tmp/worker-repo" });
});

test("forceRetire catches and logs recovery-gate rejection instead of leaking a rejected promise", async () => {
	database = await GatewayDatabase.open(":memory:");
	bind("dead", NOW, "sess-dead");
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: new ScriptedSessionPort(),
		now: () => NOW,
		log: (line) => logs.push(line),
	});
	governor.setRecoveryGate(async () => {
		throw new Error("recovery failed");
	});

	await expect(governor.forceRetire("dead")).resolves.toMatchObject({
		retired: false,
		reason: "recovery failed",
	});
	expect(logs).toContain("lane_force_retire_failed name=dead reason=recovery failed");
});

test("#360: sweep releases dead lanes and quarantined lanes don't count toward capacity", async () => {
	database = await GatewayDatabase.open(":memory:");

	// Lane 1: dead session
	bind("dead-lane", NOW - 120_000, "sess-dead");
	persistJob("dead-lane", "running", false); // not open

	// Lane 2: live lane (should not be retired)
	bind("live-lane", NOW - 10_000, "sess-live");
	persistJob("live-lane", "running", false);

	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		maxLanes: 2,
		idleRetireMs: 60_000,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// Both lanes fit within maxLanes
	const lanes = governor.activeLanes();
	expect(lanes.map((l) => l.name).sort()).toEqual(["dead-lane", "live-lane"]);

	// Test that we can still admit since we're at capacity
	expect(() => governor.assertAdmission("dead-lane")).not.toThrow();
	expect(() => governor.assertAdmission("live-lane")).not.toThrow();

	// Test that sweep releases dead lanes
	port.setSessionState("sess-dead", { live: false });
	port.setSessionState("sess-live", { live: true });

	const retiredCount = await governor.sweep();
	expect(retiredCount).toBe(1); // Only dead-lane should be retired by sweep

	expect(logs.some((line) => line.includes("lane_retired name=dead-lane"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=live-lane"))).toBe(false); // live lane not retired

	// Verify only dead-lane was closed
	expect(port.closes).toContainEqual({ sessionId: "sess-dead", repo: "/tmp/worker-repo" });
	expect(port.closes).not.toContainEqual({ sessionId: "sess-live", repo: "/tmp/worker-repo" });
});

test("#360: retireAllDead retires only dead lanes, skipping live and quarantined", async () => {
	database = await GatewayDatabase.open(":memory:");

	// Lane 1: dead
	bind("dead1", NOW - 120_000, "sess-dead1");
	// Lane 2: dead
	bind("dead2", NOW - 120_000, "sess-dead2");
	// Lane 3: live
	bind("live", NOW - 10_000, "sess-live");

	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		now: () => NOW,
		log: (line) => logs.push(line),
	});

	// Set session states
	port.setSessionState("sess-dead1", { live: false });
	port.setSessionState("sess-dead2", { live: false });
	port.setSessionState("sess-live", { live: true });

	const result = await governor.retireAllDead();
	expect(result.count).toBe(2); // Only the two dead lanes
	expect(result.names.sort()).toEqual(["dead1", "dead2"]);

	expect(logs.filter((line) => line.includes("lane_retired")).length).toBe(2);
	expect(logs.some((line) => line.includes("lane_retired name=dead1"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=dead2"))).toBe(true);
	expect(logs.some((line) => line.includes("lane_retired name=live"))).toBe(false);

	expect(port.closes.length).toBe(2);
	expect(port.closes.map((c) => c.sessionId).sort()).toEqual(["sess-dead1", "sess-dead2"]);
});
