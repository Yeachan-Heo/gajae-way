import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseLaneJobRecord } from "@gajae-gateway/subsession";
import {
	LaneGovernor,
	laneJobIdentity,
	managedLaneWorktreePath,
	workSessionKey,
} from "../src/orchestrator/lane-governor";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
async function until(predicate: () => boolean) {
	for (let i = 0; i < 600 && !predicate(); i++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}
function git(cwd: string, ...args: string[]) {
	const result = Bun.spawnSync(["git", ...args], { cwd });
	if (result.exitCode !== 0) throw new Error(result.stderr.toString());
	return result.stdout.toString().trim();
}
async function fixture(maxLanes = 8) {
	const dir = await realpath(await mkdtemp(join(tmpdir(), "gw-isolated-successor-")));
	const home = join(dir, "home");
	const repo = join(dir, "repo");
	const service = join(dir, "service");
	await Promise.all([home, repo, service].map((path) => mkdir(path)));
	git(repo, "init", "-b", "main");
	git(repo, "config", "user.email", "recovery@example.invalid");
	git(repo, "config", "user.name", "Recovery fixture");
	await writeFile(join(repo, "task.txt"), "reviewed immutable task input\n");
	git(repo, "add", "task.txt");
	git(repo, "commit", "-m", "reviewed input");
	const base = git(repo, "rev-parse", "HEAD");
	const db = await Store.open(join(dir, "nonproduction.db"));
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const id = db.bindings.getSessionRecord(input.originKey)?.sessionId ?? crypto.randomUUID();
			db.transaction((tx) => db.bindings.bind(tx, input.originKey, id));
			return id;
		},
	});
	let probes = 0;
	Object.assign(port, {
		turnRunning: async () => {
			probes++;
			return true;
		},
	});
	const lanes = new LaneGovernor({ database: db, sessionPort: port, laneRoot: home, maxLanes });
	const notices: string[] = [];
	const make = () =>
		new WorkLaneManager({
			database: db,
			port,
			lanes,
			gajaewayHome: home,
			serviceDirectory: service,
			pollMs: 5,
			brokerWait: { deadlineMs: 0 },
			ownerTarget: () => ({ platform: "discord", kind: "channel", conversationId: "recovery-owner" }),
			deliverFallback: (payload) => notices.push(payload.text),
		});
	let manager = make();
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(dir, { recursive: true, force: true });
	});
	const first = await manager.start({ name: "predecessor", text: "original accepted work; never replay", repo, base });
	if (!first.started) throw new Error("unexpected hold");
	const runtime = db.work.workAttemptGet(first.opRef)!;
	const status = port.status.bind(port);
	port.status = async (input) =>
		input.opRef !== first.opRef
			? status(input)
			: {
					operationRef: first.opRef,
					status: {
						status: "failed",
						clientRef: first.opRef,
						commandId: "command-original",
						turnId: "turn-original",
						startedAt: Date.parse(runtime.startedAt),
						terminalAt: Date.parse(runtime.startedAt) + 1,
						receiptState: "missing",
						error: { code: "prompt_deadline_exceeded" },
					},
					summaryCompleted: true,
				};
	await until(() => probes > 1);
	const job = (name = "predecessor") => parseLaneJobRecord(db.work.laneJobJson(laneJobIdentity(name).jobId)!);
	const request = (name = "authorized-successor") => ({
		name,
		repo,
		base,
		text: "New authorized isolated task; review old effects, do not replay the old prompt or publish its branch",
		successorOf: { name: "predecessor", opRef: first.opRef, sessionId: first.sessionId, acknowledgeUnresolved: true },
	});
	return {
		db,
		port,
		home,
		repo,
		base,
		first,
		runtime,
		notices,
		job,
		request,
		get manager() {
			return manager;
		},
		restart: async () => {
			await manager.stop();
			manager = make();
			await manager.recover();
		},
	};
}

test("missing execution end holds the predecessor and an authorized isolated successor actually runs without replay", async () => {
	const f = await fixture();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	await expect(f.manager.start({ name: "predecessor", text: "no replay", resume: true })).rejects.toMatchObject({
		detail: { reasonCode: "attempt_open" },
	});
	// An already designated, independent successor is not retired, resumed, or altered.
	const existing = await f.manager.start({
		name: "designated-successor",
		text: "existing independent work",
		repo: f.repo,
		base: f.base,
	});
	if (!existing.started) throw new Error("unexpected existing hold");
	const existingJob = f.db.work.laneJobJson(existing.jobId);
	const result = await f.manager.start(f.request());
	expect(result.started).toBe(true);
	if (!result.started) throw new Error("unexpected successor hold");
	expect(f.job().state).toBe("awaiting_operator");
	expect(await f.manager.status({ name: "predecessor" })).toMatchObject({ recoveryHold: "execution_end_unproven" });
	const next = f.db.work.workAttemptGet(result.opRef)!;
	expect(next.cwd).not.toBe(f.runtime.cwd);
	expect(git(next.cwd, "rev-parse", "--git-common-dir")).not.toBe(git(f.runtime.cwd, "rev-parse", "--git-common-dir"));
	expect(git(next.cwd, "rev-parse", "HEAD")).toBe(f.base);
	expect(result.sessionId).not.toBe(f.first.sessionId);
	expect(f.port.sends.map((send) => send.text)).toEqual([
		"original accepted work; never replay",
		"existing independent work",
		f.request().text,
	]);
	expect(f.port.sends.at(-1)?.systemPreamble).toContain(f.first.opRef);
	expect(f.port.sends.at(-1)?.systemPreamble).toContain("execution end is UNPROVEN");
	expect(next.successorOf).toMatchObject({
		name: "predecessor",
		opRef: f.first.opRef,
		sessionId: f.first.sessionId,
		baseSha: f.base,
	});
	expect(f.db.work.workAttemptGet(f.first.opRef)).toMatchObject({
		successor: { name: "authorized-successor", opRef: result.opRef, sessionId: result.sessionId },
		terminal: null,
		settledAt: null,
		output: { reads: 0 },
	});
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	await f.restart();
	expect(await f.manager.start(f.request())).toMatchObject({
		started: true,
		alreadyStarted: true,
		opRef: result.opRef,
		sessionId: result.sessionId,
	});
	expect(await f.manager.status({ name: "authorized-successor" })).toMatchObject({
		successorOf: { opRef: f.first.opRef, baseSha: f.base },
	});
	await expect(f.manager.start(f.request("second-successor"))).rejects.toMatchObject({
		detail: { reasonCode: "successor_already_authorized" },
	});
	f.port.complete(result.opRef, "Authorized isolated task finished; predecessor remains unresolved.");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]).toContain("Authorized isolated task finished");
	expect(f.db.work.workAttemptGet(f.first.opRef)?.settledAt).toBeNull();
	expect(f.db.work.laneJobJson(existing.jobId)).toBe(existingJob);
	expect(f.db.bindings.getSessionRecord(workSessionKey("designated-successor"))?.sessionId).toBe(existing.sessionId);
	expect(f.port.sends).toHaveLength(3);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.port.closes).toHaveLength(0);
});

test("linked recovery remains inspectable during a transport gap and never replays an uncertain send", async () => {
	const f = await fixture();
	const originalSend = f.port.send.bind(f.port);
	const originalStatus = f.port.status.bind(f.port);
	let successorOp: string | undefined;
	f.port.send = async (input) => {
		successorOp = input.opRef;
		await originalSend(input);
		throw new Error("accepted acknowledgement lost");
	};
	f.port.status = async (input) => {
		if (input.opRef === successorOp) throw new Error("transport unavailable");
		return originalStatus(input);
	};
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	expect(successorOp).toBeDefined();
	const op = successorOp!;
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(op);
	expect(await f.manager.status({ name: "authorized-successor" })).toMatchObject({
		op: null,
		opUnavailable: true,
		successorOf: { opRef: f.first.opRef },
	});
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain", opRef: op },
	});
	await expect(f.manager.start(f.request("do-not-replay"))).rejects.toMatchObject({
		detail: { reasonCode: "successor_already_authorized" },
	});
	f.port.status = async () => {
		throw new Error("all live queries unavailable");
	};
	expect(await f.manager.status({ name: "predecessor" })).toMatchObject({
		op: null,
		opUnavailable: true,
		successor: { opRef: op },
		recoveryHold: "execution_end_unproven",
	});
	expect(f.port.sends).toHaveLength(2);
	// Reconstruction is safe even when source and successor observation are unavailable.
	await f.restart();
	expect(f.port.sends).toHaveLength(2);
});

test("isolated successor refuses stale identity, mutable base, wrong repository, and non-explicit acknowledgement before sending", async () => {
	const f = await fixture();
	for (const params of [
		{ ...f.request(), base: "main" },
		{ ...f.request(), cwd: f.runtime.cwd },
		{ ...f.request(), resume: true },
		{ ...f.request(), successorOf: { ...f.request().successorOf, acknowledgeUnresolved: false } },
		{ ...f.request(), successorOf: { ...f.request().successorOf, sessionId: crypto.randomUUID() } },
		{ ...f.request(), successorOf: { ...f.request().successorOf, opRef: "different-op" } },
		{ ...f.request(), repo: "/unrelated/repository" },
	])
		await expect(f.manager.start(params)).rejects.toBeDefined();
	await expect(f.manager.run(f.request(), {})).rejects.toBeDefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.binds).toHaveLength(1);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
});

test("two concurrent distinct-name recovery requests cannot dispatch two successors", async () => {
	const f = await fixture();
	const results = await Promise.allSettled([
		f.manager.start(f.request("concurrent-a")),
		f.manager.start(f.request("concurrent-b")),
	]);
	expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
	expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
	expect(f.port.sends).toHaveLength(2);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeDefined();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("capacity refusal preserves the unresolved source without binding or designating a successor", async () => {
	const f = await fixture(1);
	await expect(f.manager.start(f.request())).rejects.toMatchObject({ code: "lane_capacity" });
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.closes).toHaveLength(0);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("a symlinked successor checkout cannot alias the unresolved predecessor", async () => {
	const f = await fixture();
	await symlink(f.runtime.cwd, managedLaneWorktreePath(f.home, "authorized-successor"));
	await expect(f.manager.start(f.request())).rejects.toBeDefined();
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(git(f.runtime.cwd, "rev-parse", "HEAD")).toBe(f.base);
});

test("a no-longer-missing source cannot authorize a fresh successor", async () => {
	const f = await fixture();
	const status = f.port.status.bind(f.port);
	f.port.status = async (input) => {
		const report = await status(input);
		return { ...report, status: { ...report.status, status: "in_flight", receiptState: "absent" } };
	};
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_requires_missing_execution_end" },
	});
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
});
