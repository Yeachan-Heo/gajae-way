import { afterAll, afterEach, expect, test } from "bun:test";
import type { ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createScheduler, type SchedulerClock } from "../../src/main-session/scheduler";
import { GatewayStateError, GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";
import { ManagedProcessRegistry } from "../helpers/managed-process";

const temporaryDirectories: string[] = [];
const runningHttp: WayCoreHandle[] = [];
const runningRpc: WayCoreHandle[] = [];
const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});

afterAll(async () => {
	for (const core of runningHttp) {
		try {
			core.metricsHttpStop();
		} catch {
			// already stopped
		}
	}
	for (const core of runningRpc) {
		try {
			core.shutdownRpcServer();
		} catch {
			// already stopped
		}
	}
	await Bun.sleep(20);
	await managedProcesses.reapAll();
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

class TestClock implements SchedulerClock {
	#now: number;
	constructor(now: number) {
		this.#now = now;
	}
	now(): number {
		return this.#now;
	}
	advance(ms: number): void {
		this.#now += ms;
	}
	setTimeout(): unknown {
		return undefined;
	}
	clearTimeout(): void {}
}

function openCore(): WayCoreHandle {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-gen2-"));
	temporaryDirectories.push(stateDir);
	return loadWayCore().WayCore.open(stateDir);
}

function committedState(): { core: WayCoreHandle; state: GatewayStateStore } {
	const core = openCore();
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "bootstrap_state", value: "COMMITTED" }],
		deletes: [],
	});
	return { core, state: new GatewayStateStore(core) };
}

function writeProfile(): string {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-gen2-profile-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	return profilePath;
}

function alertEvents(core: WayCoreHandle): Array<{ kind: string; payload: unknown }> {
	const read = core.journalRead("1:0", 200);
	return read.events
		.filter((event) => event.kind === "alert_raised" || event.kind === "alert_cleared")
		.map((event) => ({ kind: event.kind, payload: JSON.parse(event.payloadJson) as unknown }));
}

function derivedJobId(idempotencyKey: string): string {
	return `job:${createHash("sha256").update(idempotencyKey).digest("hex").slice(0, 32)}`;
}

function schedulerFixture() {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-gen2-sched-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);
	const clock = new TestClock(Date.now());
	const scheduler = createScheduler({
		core,
		profile,
		admitSystemEvent: async () => {},
		submitFromRpc: async () => {
			throw new Error("submitFromRpc must not run in this probe");
		},
		turnState: () => "idle",
		tickMs: 1_000,
		clock,
		newId: (() => {
			let counter = 0;
			return () => {
				counter += 1;
				return `gen2-${counter}`;
			};
		})(),
	});
	return { core, scheduler };
}

async function spawnHolder(core: WayCoreHandle): Promise<{
	child: ChildProcess;
	identity: { pid: number; pidStartTime: string; pgid: number; pgidStartTime?: string };
}> {
	const child = managedProcesses.spawnNodeGroup("/bin/sh", ["-c", "exec sleep 60"], { stdio: "ignore" });
	if (!child.pid) throw new Error("holder process did not expose a pid");
	let lastError: unknown;
	for (let attempt = 0; attempt < 100; attempt += 1) {
		try {
			return { child, identity: core.processIdentity(child.pid) };
		} catch (error) {
			lastError = error;
			await Bun.sleep(10);
		}
	}
	throw new Error(
		`holder process did not publish an incarnation: ${lastError instanceof Error ? lastError.message : "unknown"}`,
	);
}

async function acquireLease(core: WayCoreHandle, sessionId: string) {
	const holder = await spawnHolder(core);
	const acquired = core.lockAcquire({
		label: "gen2-redteam",
		waitMs: 0,
		ttlMs: 30_000,
		holder: {
			holderKind: "in_daemon",
			sessionId,
			pid: holder.identity.pid,
			pidStartTime: holder.identity.pidStartTime,
			pgid: holder.identity.pgid,
			...(holder.identity.pgidStartTime ? { pgidStartTime: holder.identity.pgidStartTime } : {}),
			connId: "way.in_daemon_executor.v1",
		},
	});
	return { holder, acquired };
}

async function connectRpc(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// listener startup races
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

function metaValue(core: WayCoreHandle, key: string): string | undefined {
	return core.gatewayMetaRead([key]).entries.find((entry) => entry.key === key)?.value;
}

test("clear_quarantine does not double-emit alert_cleared, including a second cycle", async () => {
	const core = openCore();
	const sessionId = "sess_gen2_lock_cycle";
	const first = await acquireLease(core, sessionId);
	core.lockQuarantineOverride(first.acquired.leaseId, true, true);
	expect(core.alertsRaised()).toContain("lock_quarantined");
	expect(alertEvents(core).filter((event) => event.kind === "alert_raised")).toHaveLength(1);

	await managedProcesses.crashNodeGroup(first.holder.child, "SIGKILL");
	const receipt = core.lockRecordQuarantineReceipt({
		leaseId: first.acquired.leaseId,
		corpus: "corpus",
		processInspected: true,
		gitStatusChecked: true,
		gitLogChecked: true,
		gitFsckChecked: true,
		remoteVerified: true,
	});
	core.lockClearQuarantine(receipt.receiptId, true);
	expect(core.alertsRaised()).not.toContain("lock_quarantined");
	expect(alertEvents(core).filter((event) => event.kind === "alert_cleared")).toHaveLength(1);

	expect(() => core.lockClearQuarantine(receipt.receiptId, true)).toThrow();
	expect(alertEvents(core).filter((event) => event.kind === "alert_cleared")).toHaveLength(1);

	const second = await acquireLease(core, `${sessionId}_2`);
	core.lockQuarantineOverride(second.acquired.leaseId, true, true);
	expect(core.alertsRaised()).toContain("lock_quarantined");
	expect(alertEvents(core).filter((event) => event.kind === "alert_raised")).toHaveLength(2);
	await managedProcesses.crashNodeGroup(second.holder.child, "SIGKILL");
	const secondReceipt = core.lockRecordQuarantineReceipt({
		leaseId: second.acquired.leaseId,
		corpus: "corpus",
		processInspected: true,
		gitStatusChecked: true,
		gitLogChecked: true,
		gitFsckChecked: true,
		remoteVerified: true,
	});
	core.lockClearQuarantine(secondReceipt.receiptId, true);
	expect(alertEvents(core).map((event) => event.kind)).toEqual([
		"alert_raised",
		"alert_cleared",
		"alert_raised",
		"alert_cleared",
	]);
	expect(core.alertsRaised()).not.toContain("lock_quarantined");
});

test("clear_quarantine when never raised emits no alert_cleared", () => {
	const core = openCore();
	expect(() => core.lockClearQuarantine("git-verify-00000000000000000000000000000000", true)).toThrow();
	expect(alertEvents(core)).toEqual([]);
	expect(core.alertsRaised()).not.toContain("lock_quarantined");
});

test("force_release does not raise or clear a quarantine alert", async () => {
	const core = openCore();
	const { holder, acquired } = await acquireLease(core, "sess_gen2_force_release");
	await managedProcesses.crashNodeGroup(holder.child, "SIGKILL");
	const released = core.lockForceRelease(acquired.leaseId, true);
	expect(released.released).toBe(true);
	expect(alertEvents(core)).toEqual([]);
	expect(core.alertsRaised()).not.toContain("lock_quarantined");
	expect(metaValue(core, "alert_lock_quarantined") ?? "clear").not.toBe("raised");
});

test("approveProfile clears fail-closed flags and way.status.alerts for profile_drift only", async () => {
	const { core, state } = committedState();
	runningRpc.push(core);
	state.markFailedClosed("profile_drift", 1_000);
	expect(core.alertsRaised()).toContain("failed_closed");

	const profile = loadWayProfile(writeProfile());
	state.approveProfile(profile, "receipt-gen2", 2_000);

	expect(metaValue(core, "alert_failed_closed")).toBe("clear");
	expect(metaValue(core, "failed_closed_since_ms")).toBe("null");
	expect(core.alertsRaised()).not.toContain("failed_closed");

	const socketPath = path.join(core.stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	const client = await connectRpc(socketPath);
	try {
		const status = await client.request("way.status", {});
		expect(status.error).toBeUndefined();
		const rendered = JSON.stringify(status.result ?? {});
		expect(rendered).not.toMatch(/failed_closed/i);
	} finally {
		client.close();
		core.shutdownRpcServer();
	}
});

test("approveProfile refuses when the fail-closed reason is not profile_drift", () => {
	const { core, state } = committedState();
	state.markFailedClosed("growth_protocol_invalid", 1_000);
	const profile = loadWayProfile(writeProfile());
	try {
		state.approveProfile(profile, "receipt-refused", 2_000);
		throw new Error("expected approveProfile to refuse");
	} catch (error) {
		expect(error).toBeInstanceOf(GatewayStateError);
		expect((error as GatewayStateError).reason).toBe("failed_closed_not_profile_drift");
	}
	expect(metaValue(core, "alert_failed_closed")).toBe("raised");
	expect(core.alertsRaised()).toContain("failed_closed");
});

test("catchUpFailedClosedAlert is load-bearing on persist=false and does not double-emit with markFailedClosed", () => {
	const { core, state } = committedState();
	core.gatewayMetaTransaction({
		expected: [],
		puts: [
			{ key: "bootstrap_state", value: "FAILED_CLOSED" },
			{ key: "failed_closed_reason", value: JSON.stringify("growth_protocol_invalid") },
		],
		deletes: [],
	});
	expect(alertEvents(core)).toEqual([]);
	expect(state.catchUpFailedClosedAlert(5_000)).toBe(true);
	expect(alertEvents(core)).toHaveLength(1);
	expect(state.catchUpFailedClosedAlert(6_000)).toBe(false);
	expect(alertEvents(core)).toHaveLength(1);

	const second = committedState();
	second.state.markFailedClosed("profile_drift", 1_000);
	expect(second.state.catchUpFailedClosedAlert(2_000)).toBe(false);
	expect(alertEvents(second.core)).toHaveLength(1);
});

test("enterFailedClosed wraps catchUpFailedClosedAlert so a throw cannot change exit 78", () => {
	const source = fs.readFileSync(path.join(import.meta.dir, "..", "..", "src", "main.ts"), "utf8");
	expect(source).toContain("state.catchUpFailedClosedAlert()");
	expect(source).toContain("await enterFailedClosed(core, state, config, recovery.reason, false)");
	const catchUpIndex = source.indexOf("state.catchUpFailedClosedAlert()");
	const tryIndex = source.lastIndexOf("try {", catchUpIndex);
	const catchIndex = source.indexOf("} catch {", catchUpIndex);
	expect(tryIndex).toBeGreaterThan(-1);
	expect(catchIndex).toBeGreaterThan(catchUpIndex);
	expect(source.slice(tryIndex, catchIndex)).toContain("state.catchUpFailedClosedAlert()");
	expect(source).toMatch(/process\.exit\(78\)|process\.exitCode = 78/);
});

test("derived job_id is stable across stores, explicit job_id wins, and mutations address the derived id", async () => {
	const key = "gen2-idempotency-stable";
	const expected = derivedJobId(key);
	expect(expected).toMatch(/^job:[0-9a-f]{32}$/);

	const createParams = {
		name: "derived-a",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "nudge" },
	};

	const first = schedulerFixture();
	const created = (await first.scheduler.handleRpc("schedule.create", {
		idempotency_key: key,
		...createParams,
	})) as { job_id: string };
	expect(created.job_id).toBe(expected);

	const second = schedulerFixture();
	const recreated = (await second.scheduler.handleRpc("schedule.create", {
		idempotency_key: key,
		...createParams,
		name: "derived-b",
	})) as { job_id: string };
	expect(recreated.job_id).toBe(expected);

	const weird = schedulerFixture();
	const injected = (await weird.scheduler.handleRpc("schedule.create", {
		idempotency_key: "../; DROP TABLE jobs; --",
		...createParams,
		name: "weird-key",
	})) as { job_id: string };
	expect(injected.job_id).toBe(derivedJobId("../; DROP TABLE jobs; --"));
	expect(injected.job_id).toMatch(/^job:[0-9a-f]{32}$/);

	const explicit = schedulerFixture();
	const custom = (await explicit.scheduler.handleRpc("schedule.create", {
		idempotency_key: key,
		job_id: "custom-job-id-wins",
		...createParams,
		name: "explicit",
	})) as { job_id: string };
	expect(custom.job_id).toBe("custom-job-id-wins");

	const updated = (await first.scheduler.handleRpc("schedule.update", {
		idempotency_key: `${key}-update`,
		job_id: expected,
		...createParams,
		name: "derived-renamed",
		spec: String(120_000),
	})) as { job_id: string };
	expect(updated.job_id).toBe(expected);
	expect(first.core.scheduleJobGet(expected).name).toBe("derived-renamed");

	const ran = (await first.scheduler.handleRpc("schedule.run_now", {
		idempotency_key: `${key}-run`,
		job_id: expected,
	})) as { job_id: string; trigger: string };
	expect(ran.job_id).toBe(expected);
	expect(ran.trigger).toBe("manual");

	const deleted = (await first.scheduler.handleRpc("schedule.delete", {
		idempotency_key: `${key}-delete`,
		job_id: expected,
	})) as { deleted: boolean };
	expect(deleted.deleted).toBe(true);
	expect(first.core.scheduleJobList({ limit: 200 }).jobs).toHaveLength(0);
});

test("seeded lock holder, quarantined lease, and fail-closed reason never appear in UDS way.metrics or HTTP /metrics", async () => {
	const { core, state } = committedState();
	runningHttp.push(core);
	runningRpc.push(core);

	const holderSessionId = "sess_gen2red_HOLDER_9f3c_unique";
	const failReason = "gen2red_secret_reason_xyz_unique";
	const { acquired } = await acquireLease(core, holderSessionId);
	core.lockQuarantineOverride(acquired.leaseId, true, true);
	state.markFailedClosed(failReason, Date.now() - 120_000);

	expect(core.lockStatus().quarantined).toBe(true);
	expect(core.lockStatus().holder?.sessionId).toBe(holderSessionId);

	const socketPath = path.join(core.stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setMainSessionStatus("busy", 4, "verified");
	core.setJournalDegraded(true);
	const client = await connectRpc(socketPath);
	let udsBody = "";
	try {
		const metrics = await client.request("way.metrics", {});
		expect(metrics.error).toBeUndefined();
		const payload = metrics.result as Record<string, unknown>;
		for (const field of [
			"gajaeway_state",
			"gajaeway_journal_degraded",
			"gajaeway_turn_busy",
			"gajaeway_follow_up_queue_depth",
			"gajaeway_transcript_verified",
		]) {
			expect(payload).toHaveProperty(field);
		}
		expect(payload.gajaeway_journal_degraded).toBe(true);
		expect(payload.gajaeway_turn_busy).toBe(true);
		expect(payload.gajaeway_follow_up_queue_depth).toBe(4);
		expect(payload.gajaeway_transcript_verified).toBe(true);
		udsBody = JSON.stringify(payload);
	} finally {
		client.close();
		core.shutdownRpcServer();
	}

	const port = core.metricsHttpStart(0);
	const httpBody = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text();
	core.metricsHttpStop();

	expect(httpBody).toContain("gajaeway_lock_held 1");
	expect(httpBody).toContain("gajaeway_lock_quarantined 1");

	for (const secret of [holderSessionId, acquired.leaseId, failReason, "HOLDER_9f3c"]) {
		expect(udsBody).not.toContain(secret);
		expect(httpBody).not.toContain(secret);
	}
	for (const forbidden of ["session_id", "lease_id", "reason"]) {
		expect(httpBody.toLowerCase()).not.toContain(forbidden);
	}
});
