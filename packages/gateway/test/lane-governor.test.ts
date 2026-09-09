import { afterEach, expect, setSystemTime, test } from "bun:test";
import { ProtocolError } from "@gajaeway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord, type LaneJobRecord, newOpRef } from "@gajaeway/subsession";
import { LaneGovernor, laneJobIdentity } from "../src/orchestrator/lane-governor";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

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
