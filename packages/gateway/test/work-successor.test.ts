import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { access, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import {
	LaneGovernor,
	laneJobIdentity,
	managedLaneRepositoryPath,
	managedLaneWorktreePath,
	workSessionKey,
} from "../src/orchestrator/lane-governor";
import { type CheckStatusClient, WorkLaneManager } from "../src/orchestrator/work-lane";
import { Store } from "../src/store";
import { workAttemptDeliveryId, workAttemptReportId } from "../src/store/groups/work";
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
async function fixture(
	maxLanes = 8,
	initialTrigger?: string,
	options: {
		runPredecessor?: boolean;
		callerOriginKey?: string;
		noOwner?: boolean;
		checkStatusClient?: CheckStatusClient;
	} = {},
) {
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
	const reportOrigin = { platform: "discord", kind: "channel", conversationId: "recovery-owner" } as const;
	let ownerTarget: { platform: "discord"; kind: "channel"; conversationId: string } | undefined = options.noOwner
		? undefined
		: reportOrigin;
	let activeOwnerOp: string | undefined;
	const setOwnerTrigger = (messageId?: string) =>
		db.transaction((tx) => {
			if (activeOwnerOp) db.inbox.turnComplete(tx, activeOwnerOp);
			activeOwnerOp = messageId ? `owner-${messageId}` : undefined;
			if (messageId && activeOwnerOp) {
				db.inbox.enqueue(tx, {
					messageId,
					originKey: "discord/channel/recovery-owner",
					originRefJson: JSON.stringify(reportOrigin),
					body: "operator instruction",
				});
				db.inbox.bindTurn(tx, {
					messageId,
					originKey: "discord/channel/recovery-owner",
					epoch: 0,
					opRef: activeOwnerOp,
					sessionId: crypto.randomUUID(),
				});
			}
		});
	setOwnerTrigger(initialTrigger);
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const id = db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
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
			checkStatusClient: options.checkStatusClient,
			ownerTarget: () => ownerTarget,
			deliverFallback: (payload) => notices.push(payload.text),
		});
	let manager = make();
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(dir, { recursive: true, force: true });
	});
	const callerSessionId = options.callerOriginKey ? crypto.randomUUID() : undefined;
	if (callerSessionId && options.callerOriginKey)
		db.transaction((tx) => db.bindings.bind(tx, options.callerOriginKey!, callerSessionId));
	const initial = {
		name: "predecessor",
		text: "original accepted work; never replay",
		repo,
		base,
		...(callerSessionId ? { callerSessionId } : {}),
	};
	const abort = new AbortController();
	let first: { started: true; opRef: string; sessionId: string };
	if (options.runPredecessor) {
		const run = manager.run(initial, {}, abort.signal).catch(() => undefined);
		cleanups.push(async () => {
			abort.abort();
			await run;
		});
		await until(() => port.sends.length === 1);
		const send = port.sends[0]!;
		first = { started: true, opRef: send.opRef, sessionId: send.sessionId };
		await until(() => db.work.workAttemptGet(first.opRef)?.sendPhase === "accepted");
	} else {
		const started = await manager.start(initial);
		if (!started.started) throw new Error("unexpected hold");
		first = started;
	}
	if (!first.started) throw new Error("unexpected hold");
	const initialState = parseLaneJobRecord(db.work.laneJobJson(laneJobIdentity("predecessor").jobId)!).state;
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
		dir,
		port,
		home,
		repo,
		base,
		first,
		runtime,
		callerSessionId,
		initialState,
		lanes,
		notices,
		job,
		request,
		setOwnerTrigger,
		setOwner: (conversationId: string) => {
			ownerTarget = { ...reportOrigin, conversationId };
		},
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
	f.port.liveness = async () => {
		throw new Error("inspect unavailable");
	};
	await f.restart();
	for (const opRef of [f.first.opRef, op]) {
		expect(f.db.work.workAttemptGet(opRef)).toMatchObject({
			terminal: null,
			settledAt: null,
			decision: "undecided",
			output: { reads: 0 },
		});
	}
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.job("authorized-successor").attempts[0]?.endedAt).toBeUndefined();
	expect(f.notices).toHaveLength(0);
	expect(f.port.sends).toHaveLength(2);
});

test("a prepared linked successor stays open when submission and both reconstruction probes are unavailable", async () => {
	const f = await fixture();
	let successorOp: string | undefined;
	f.port.send = async (input) => {
		successorOp = input.opRef;
		throw new Error("submission outcome unavailable");
	};
	const originalStatus = f.port.status.bind(f.port);
	f.port.status = async (input) =>
		input.opRef === successorOp
			? { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false }
			: originalStatus(input);
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	expect(successorOp).toBeDefined();
	f.port.status = async () => {
		throw new Error("status unavailable");
	};
	f.port.liveness = async () => {
		throw new Error("inspect unavailable");
	};
	await f.restart();
	for (const opRef of [f.first.opRef, successorOp!]) {
		expect(f.db.work.workAttemptGet(opRef)).toMatchObject({
			terminal: null,
			settledAt: null,
			decision: "undecided",
			output: { reads: 0 },
		});
	}
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain", opRef: successorOp },
	});
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.job("authorized-successor").attempts[0]?.endedAt).toBeUndefined();
	expect(f.notices).toHaveLength(0);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.closes).toHaveLength(0);
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

test("later operator messages authorize and retry recovery while retaining the original captured report target", async () => {
	const f = await fixture(8, "M1");
	expect(f.runtime.parent).toMatchObject({ kind: "persona", triggerMessageId: "M1" });
	f.setOwnerTrigger("M2");
	const successor = await f.manager.start(f.request());
	if (!successor.started) throw new Error("unexpected successor hold");
	expect(f.db.work.workAttemptGet(successor.opRef)?.parent).toEqual(f.runtime.parent);
	f.setOwnerTrigger("M3");
	expect(await f.manager.start(f.request())).toMatchObject({
		started: true,
		alreadyStarted: true,
		opRef: successor.opRef,
	});
	f.setOwnerTrigger();
	await f.restart();
	expect(await f.manager.start(f.request())).toMatchObject({
		started: true,
		alreadyStarted: true,
		opRef: successor.opRef,
	});
	expect(f.db.work.workAttemptGet(successor.opRef)?.parent).toEqual(f.runtime.parent);
	expect(f.port.sends).toHaveLength(2);
});

test("a different report owner cannot authorize or reuse a linked successor", async () => {
	const f = await fixture(8, "M1");
	f.setOwner("different-owner");
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	expect(f.port.binds).toHaveLength(1);
	f.setOwner("recovery-owner");
	const successor = await f.manager.start(f.request());
	expect(successor.started).toBe(true);
	f.setOwner("different-owner");
	await expect(f.manager.start(f.request())).rejects.toBeDefined();
	expect(f.port.sends).toHaveLength(2);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeDefined();
});

test("response-only predecessor authorizes only its captured owner and retains a null report route", async () => {
	const f = await fixture(8, "M1", { runPredecessor: true });
	expect(f.runtime.mode).toBe("run");
	expect(f.runtime.parent).toBeNull();
	f.setOwner("different-owner");
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	f.setOwner("recovery-owner");
	const successor = await f.manager.start(f.request());
	if (!successor.started) throw new Error("unexpected successor hold");
	expect(f.db.work.workAttemptGet(successor.opRef)?.parent).toBeNull();
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(successor.opRef);
	await f.restart();
	await expect(f.manager.start(f.request())).resolves.toMatchObject({ alreadyStarted: true, opRef: successor.opRef });
	f.setOwner("different-owner");
	await expect(f.manager.start(f.request())).rejects.toBeDefined();
	f.setOwner("recovery-owner");
	f.port.complete(successor.opRef, "response-only successor completed");
	await until(() => f.db.work.workAttemptGet(successor.opRef)?.settledAt != null);
	expect(f.db.work.workAttemptGet(successor.opRef)?.decision).toBe("no_target");
	expect(f.notices).toEqual([]);
	expect(f.port.sends).toHaveLength(2);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.settledAt).toBeNull();
});

test("response-only caller session owns successor authorization independently of owner target", async () => {
	const f = await fixture(8, undefined, { runPredecessor: true, callerOriginKey: "discord/channel/caller-owner" });
	const callerSessionId = f.callerSessionId!;
	const otherSessionId = crypto.randomUUID();
	f.db.transaction((tx) => f.db.bindings.bind(tx, "discord/channel/other-caller", otherSessionId));
	expect(f.runtime.parent).toBeNull();
	expect(f.runtime.authorizationOwner).toEqual({ kind: "persona", originKey: "discord/channel/caller-owner" });
	for (const wrong of [undefined, otherSessionId, crypto.randomUUID()]) {
		await expect(
			f.manager.start({ ...f.request(), ...(wrong ? { callerSessionId: wrong } : {}) }),
		).rejects.toMatchObject({
			detail: { reasonCode: "successor_report_owner_mismatch" },
		});
	}
	const successor = await f.manager.start({ ...f.request(), callerSessionId });
	if (!successor.started) throw new Error("unexpected successor hold");
	const runtime = f.db.work.workAttemptGet(successor.opRef)!;
	expect(runtime.parent).toBeNull();
	expect(runtime.authorizationOwner).toEqual(f.runtime.authorizationOwner);
	await f.restart();
	await expect(f.manager.start({ ...f.request(), callerSessionId })).resolves.toMatchObject({
		alreadyStarted: true,
		opRef: successor.opRef,
	});
	await expect(f.manager.start({ ...f.request(), callerSessionId: otherSessionId })).rejects.toBeDefined();
	await expect(f.manager.start({ ...f.request("duplicate"), callerSessionId })).rejects.toMatchObject({
		detail: { reasonCode: "successor_already_authorized" },
	});
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(successor.opRef);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.settledAt).toBeNull();
	expect(f.port.sends).toHaveLength(2);
	expect(f.notices).toEqual([]);
});

test("response-only work with no captured owner cannot infer authorization from a later target", async () => {
	const f = await fixture(8, undefined, { runPredecessor: true, noOwner: true });
	expect(f.runtime.parent).toBeNull();
	expect(f.runtime.authorizationOwner).toBeNull();
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	f.setOwner("recovery-owner");
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

function setStoredAuthorizationOwner(dir: string, opRef: string, owner: "missing" | "null") {
	const raw = new Database(join(dir, "nonproduction.db"));
	try {
		const expression =
			owner === "missing"
				? "json_remove(record_json, '$.authorizationOwner')"
				: "json_set(record_json, '$.authorizationOwner', NULL)";
		raw.query(`UPDATE work_attempt_runtime SET record_json = ${expression} WHERE op_ref = ?`).run(opRef);
	} finally {
		raw.close();
	}
}

test("persisted start source without owner field authorizes from its captured report parent", async () => {
	const f = await fixture(8, "M1");
	setStoredAuthorizationOwner(f.dir, f.first.opRef, "missing");
	expect(f.db.work.workAttemptGet(f.first.opRef)?.authorizationOwner).toBeUndefined();
	f.setOwner("different-owner");
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	f.setOwner("recovery-owner");
	const successor = await f.manager.start(f.request());
	if (!successor.started) throw new Error("unexpected successor hold");
	expect(f.db.work.workAttemptGet(successor.opRef)?.parent).toEqual(f.runtime.parent);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(successor.opRef);
	await f.restart();
	await expect(f.manager.start(f.request())).resolves.toMatchObject({ alreadyStarted: true, opRef: successor.opRef });
	await expect(f.manager.start(f.request("duplicate"))).rejects.toMatchObject({
		detail: { reasonCode: "successor_already_authorized" },
	});
	expect(f.port.sends).toHaveLength(2);
});

for (const owner of ["missing", "null"] as const) {
	test(`response-only source with ${owner} stored owner and null report route fails closed`, async () => {
		const f = await fixture(8, undefined, { runPredecessor: true });
		setStoredAuthorizationOwner(f.dir, f.first.opRef, owner);
		expect(f.db.work.workAttemptGet(f.first.opRef)?.parent).toBeNull();
		expect(f.db.work.workAttemptGet(f.first.opRef)?.authorizationOwner).toBe(owner === "missing" ? undefined : null);
		await expect(f.manager.start(f.request())).rejects.toMatchObject({
			detail: { reasonCode: "successor_report_owner_mismatch" },
		});
		expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
		expect(f.port.sends).toHaveLength(1);
	});
}

test("explicit null authorization is not replaced by a non-null report parent", async () => {
	const f = await fixture();
	setStoredAuthorizationOwner(f.dir, f.first.opRef, "null");
	expect(f.db.work.workAttemptGet(f.first.opRef)?.parent).not.toBeNull();
	await expect(f.manager.start(f.request())).rejects.toMatchObject({
		detail: { reasonCode: "successor_report_owner_mismatch" },
	});
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("live missing-receipt observation moves a running accepted attempt into the operator hold", async () => {
	const f = await fixture();
	expect(f.initialState).toBe("running");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.db.work.workAttemptGet(f.first.opRef)).toMatchObject({
		sendPhase: "accepted",
		terminal: null,
		settledAt: null,
	});
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.workerOutputReads).toHaveLength(0);
	expect(f.notices).toHaveLength(0);
});

test("successor base mismatch after worktree creation leaves the source unchanged and cleans the checkout", async () => {
	const f = await fixture();
	git(f.repo, "tag", "-a", "reviewed-tag", "-m", "annotated object", f.base);
	const tagObject = git(f.repo, "rev-parse", "reviewed-tag^{tag}");
	expect(tagObject).not.toBe(f.base);
	const before = f.job();
	await expect(f.manager.start({ ...f.request(), base: tagObject })).rejects.toMatchObject({
		detail: { reasonCode: "successor_base_mismatch" },
	});
	expect(f.job()).toEqual(before);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(f.db.work.laneJobJson(laneJobIdentity("authorized-successor").jobId)).toBeUndefined();
	await expect(access(managedLaneWorktreePath(f.home, "authorized-successor"))).rejects.toBeDefined();
	await expect(access(managedLaneRepositoryPath(f.home, "authorized-successor"))).rejects.toBeDefined();
	expect(f.port.sends).toHaveLength(1);
});

test("successor bind failure after checkout creation leaves no source designation or false escalation", async () => {
	const f = await fixture();
	const before = f.job();
	const bind = f.port.bind.bind(f.port);
	f.port.bind = async (input) => {
		if (input.originKey === workSessionKey("authorized-successor")) throw new Error("injected bind failure");
		return bind(input);
	};
	await expect(f.manager.start(f.request())).rejects.toMatchObject({ detail: { reasonCode: "bind_failed" } });
	// A possibly referenced checkout is retained by the existing startup contract.
	await access(managedLaneWorktreePath(f.home, "authorized-successor"));
	expect(f.job()).toEqual(before);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
	expect(f.job("authorized-successor").attempts).toEqual([]);
	expect(f.port.sends).toHaveLength(1);
});

test("failed successor prepare transaction rolls back its predecessor link and escalation", async () => {
	const f = await fixture();
	const before = f.job();
	const raw = new Database(join(f.dir, "nonproduction.db"));
	try {
		raw.exec(`CREATE TRIGGER fail_successor_escalation BEFORE UPDATE ON lane_jobs
			WHEN NEW.job_id = '${laneJobIdentity("predecessor").jobId}' AND NEW.record_json LIKE '%isolated successor%'
			BEGIN SELECT RAISE(ABORT, 'injected successor escalation failure'); END`);
		await expect(f.manager.start(f.request())).rejects.toThrow("injected successor escalation failure");
		expect(f.port.binds).toHaveLength(2);
		expect(f.job()).toEqual(before);
		expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
		expect(f.job("authorized-successor").attempts).toEqual([]);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		raw.exec("DROP TRIGGER fail_successor_escalation");
		raw.close();
	}
});

test("store rejects an attempted successor using the predecessor checkout", async () => {
	const f = await fixture();
	const name = "alias-attempt";
	const identity = laneJobIdentity(name);
	const sessionKey = workSessionKey(name);
	const binding = await f.port.bind({ originKey: sessionKey, epoch: 0, repo: f.runtime.cwd, codingRegister: true });
	const opRef = "gw-work-alias-attempt";
	const runtime = {
		...f.runtime,
		...identity,
		sessionKey,
		sessionId: binding.sessionId,
		epoch: binding.epoch,
		opRef,
		sendPhase: "prepared" as const,
		sendEvidence: null,
		reportId: workAttemptReportId(f.db.instanceId, identity.jobId, opRef),
		deliveryId: workAttemptDeliveryId(f.db.instanceId, identity.jobId, opRef),
		version: 0,
		successorOf: {
			name: "predecessor",
			opRef: f.first.opRef,
			sessionId: f.first.sessionId,
			baseSha: f.base,
			authorizedAt: f.runtime.startedAt,
		},
	};
	const record = appendAttempt(
		createLaneJobRecord({ jobId: identity.jobId, branch: "work/alias-attempt", worktreePath: f.runtime.cwd }),
		{ opRef, sessionId: binding.sessionId, startedAt: runtime.startedAt },
	);
	expect(() => f.db.transaction((tx) => f.db.work.workAttemptPrepare(tx, runtime, record, true))).toThrow(
		"successor uses a distinct checkout and job with the same parent report route",
	);
	expect(f.db.work.workAttemptGet(opRef)).toBeUndefined();
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor).toBeUndefined();
});

async function successorAwaitingChecks() {
	let status: "pending" | "red" = "pending";
	const checks: CheckStatusClient = {
		async getCheckStatus(headSha) {
			return { prNumber: 701, headSha, status, failingChecks: status === "red" ? ["unit"] : [] };
		},
	};
	const f = await fixture(8, undefined, { checkStatusClient: checks });
	const successor = await f.manager.start(f.request());
	if (!successor.started) throw new Error("unexpected successor hold");
	const initial = f.db.work.workAttemptGet(successor.opRef)!;
	const branch = git(initial.cwd, "branch", "--show-current");
	git(initial.cwd, "push", "-u", "origin", branch);
	f.port.complete(successor.opRef, "first isolated work completed");
	await until(() => f.job("authorized-successor").state === "awaiting_checks");
	return {
		f,
		successor,
		initial,
		setRed: () => {
			status = "red";
		},
	};
}

for (const restart of [false, true]) {
	test(`red-check continuation retains successor provenance during ${restart ? "restart" : "live"} status failure`, async () => {
		const { f, successor, initial, setRed } = await successorAwaitingChecks();
		setRed();
		for (let i = 0; i < 200 && f.port.sends.length < 3; i++) {
			await f.manager.recover();
			await Bun.sleep(5);
		}
		expect(f.port.sends).toHaveLength(3);
		const continuation = f.port.sends[2]!;
		expect(continuation.text).toContain("checks failed");
		expect(continuation.opRef).not.toBe(successor.opRef);
		expect(f.db.work.workAttemptGet(continuation.opRef)?.successorOf).toBeUndefined();
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (input.opRef === continuation.opRef) throw new Error("status transport unavailable");
			return status(input);
		};
		if (restart) {
			const liveness = f.port.liveness.bind(f.port);
			f.port.liveness = async (input) =>
				input.sessionId === continuation.sessionId ? { live: false, disowned: false } : liveness(input);
			await f.restart();
			expect(f.job("authorized-successor").state).toBe("awaiting_operator");
		}
		expect(await f.manager.status({ name: "authorized-successor" })).toMatchObject({
			op: null,
			opUnavailable: true,
			attempt: { opRef: continuation.opRef },
			successorOf: initial.successorOf,
		});
		expect(f.db.work.workAttemptGet(successor.opRef)?.successorOf).toEqual(initial.successorOf);
		expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(successor.opRef);
		expect(f.db.work.workAttemptGet(f.first.opRef)?.settledAt).toBeNull();
		expect(f.db.work.workAttemptGet(continuation.opRef)?.settledAt).toBeNull();
		await expect(f.manager.start(f.request())).resolves.toMatchObject({ alreadyStarted: true, opRef: successor.opRef });
		await expect(f.manager.start(f.request("second-successor"))).rejects.toBeDefined();
		expect(f.port.sends).toHaveLength(3);
	});
}

test("replacement session receives the original successor warning without relinking", async () => {
	const { f, successor, initial } = await successorAwaitingChecks();
	f.port.setSessionState(successor.sessionId, { live: false });
	expect(await f.lanes.forceRetire("authorized-successor")).toMatchObject({ retired: true });
	const replacement = await f.manager.start({
		name: "authorized-successor",
		text: "continue isolated work",
		resume: true,
	});
	if (!replacement.started) throw new Error("replacement was held");
	expect(replacement.sessionId).not.toBe(successor.sessionId);
	expect(f.port.sends.at(-1)?.systemPreamble).toContain(f.first.opRef);
	expect(f.port.sends.at(-1)?.systemPreamble).toContain("execution end is UNPROVEN");
	expect(f.db.work.workAttemptGet(replacement.opRef)?.successorOf).toBeUndefined();
	expect(f.db.work.workAttemptGet(successor.opRef)?.successorOf).toEqual(initial.successorOf);
	expect(f.db.work.workAttemptGet(f.first.opRef)?.successor?.opRef).toBe(successor.opRef);
	expect(await f.manager.status({ name: "authorized-successor" })).toMatchObject({ successorOf: initial.successorOf });
	expect(f.port.sends).toHaveLength(3);
});
