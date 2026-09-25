import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ChatMessagePayload, type OriginRef, ProtocolError } from "@gajae-gateway/protocol";
import { appendAttempt, createLaneJobRecord, GjcCliError, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { LaneGovernor, laneJobIdentity, workSessionKey } from "../src/orchestrator/lane-governor";
import { parseWorkerOutputResponse, type WorkerOutputResult } from "../src/orchestrator/session-port";
import { utf8Prefix, WorkLaneManager, type WorkLaneManagerOptions } from "../src/orchestrator/work-lane";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "work-results" };
async function until(predicate: () => boolean): Promise<void> {
	for (let i = 0; i < 600 && !predicate(); i++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}
function deferred<T>() {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}
async function fixture(
	options: Partial<WorkLaneManagerOptions> = {},
	portOptions: ConstructorParameters<typeof ScriptedSessionPort>[0] = {},
) {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-work-lane-"));
	const db = await GatewayDatabase.open(join(directory, "gateway.db"));
	const port = new ScriptedSessionPort({
		...portOptions,
		onBind: (input) => {
			const sessionId = db.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
			db.putSession(input.originKey, sessionId);
			return sessionId;
		},
	});
	const notices: ChatMessagePayload[] = [];
	const lanes = new LaneGovernor({ database: db, sessionPort: port, maxLanes: 2 });
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes,
		pollMs: 5,
		waitTimeoutMs: 2000,
		deliver: (payload) => notices.push(payload),
		...options,
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const job = (name = "a") => parseLaneJobRecord(db.laneJobJson(laneJobIdentity(name).jobId)!);
	return {
		db,
		port,
		lanes,
		notices,
		directory,
		job,
		get manager() {
			return manager;
		},
		restart: async (extra: Partial<WorkLaneManagerOptions> = {}) => {
			await manager.stop();
			manager = new WorkLaneManager({
				database: db,
				port,
				lanes,
				pollMs: 5,
				deliver: (payload) => notices.push(payload),
				...options,
				...extra,
			});
			await manager.recover();
			return manager;
		},
	};
}
async function started(f: Awaited<ReturnType<typeof fixture>>, name = "a", notify?: OriginRef) {
	const result = await f.manager.start({ name, text: "work", cwd: f.directory, ...(notify ? { notify } : {}) });
	if (!result.started) throw new Error("unexpected hold");
	return result;
}

for (const model of ["startup-model", { preset: "startup-preset" }]) {
	test(`proven startup model skips only the newly bound session's duplicate send model: ${JSON.stringify(model)}`, async () => {
		const f = await fixture();
		const first = await f.manager.start({ name: "a", text: "work", cwd: f.directory, model });
		if (!first.started) throw new Error("unexpected hold");
		expect(f.port.binds[0]?.model).toEqual(model);
		expect(f.port.sends).toHaveLength(1);
		expect(Object.hasOwn(f.port.sends[0]!, "model")).toBe(false);
		f.port.complete(first.opRef, "done");
		await until(() => f.db.workAttemptGet(first.opRef)?.settledAt !== null);

		const bind = f.port.bind.bind(f.port);
		f.port.bind = async (input) => ({ ...(await bind(input)), startupModelApplied: false });
		const nextModel = "later-turn-model";
		const second = await f.manager.start({ name: "a", text: "again", cwd: f.directory, model: nextModel });
		if (!second.started) throw new Error("unexpected hold");
		expect(second.sessionId).toBe(first.sessionId);
		expect(f.port.binds[1]?.model).toBe(nextModel);
		expect(f.port.sends).toHaveLength(2);
		expect(f.port.sends[1]?.model).toBe(nextModel);
	});
}

for (const startupModelApplied of [false, undefined]) {
	test(`binding without startup proof forwards the requested model: ${startupModelApplied}`, async () => {
		const f = await fixture();
		const bind = f.port.bind.bind(f.port);
		f.port.bind = async (input) => {
			const { startupModelApplied: _, ...binding } = await bind(input);
			return startupModelApplied === undefined ? binding : { ...binding, startupModelApplied };
		};
		const model = { preset: "requested-preset" };
		await f.manager.start({ name: "a", text: "work", cwd: f.directory, model });
		expect(f.port.binds[0]?.model).toEqual(model);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.sends[0]?.model).toEqual(model);
	});
}

test("failed send after proven startup model remains uncertain without retry or resend", async () => {
	const f = await fixture();
	let attempts = 0;
	f.port.send = async (input) => {
		attempts++;
		f.port.sendAttempts.push(input);
		throw new Error("session_unavailable");
	};
	await expect(
		f.manager.start({ name: "a", text: "work", cwd: f.directory, model: "startup-model" }),
	).rejects.toMatchObject({ detail: { reasonCode: "send_acceptance_uncertain" } });
	const opRef = f.job().attempts[0]!.opRef;
	expect(f.db.workAttemptGet(opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	await f.restart();
	expect(attempts).toBe(1);
	expect(Object.hasOwn(f.port.sendAttempts[0]!, "model")).toBe(false);
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.db.workAttemptGet(opRef)?.sendEvidence).toBeNull();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("start acknowledges before terminal, one observer survives caller-free completion and refreshes activity", async () => {
	const f = await fixture();
	let attaches = 0;
	const attach = f.port.attachTail.bind(f.port);
	f.port.attachTail = async (input) => {
		attaches++;
		return attach(input);
	};
	const result = await started(f, "a", origin);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	await until(() => attaches === 1);
	await Promise.all([f.manager.recover(), f.manager.recover()]);
	expect(attaches).toBe(1);
	const activity = f.db.workLaneRows()[0]!.last_activity_at;
	await Bun.sleep(10);
	f.port.complete(result.opRef, "done");
	await until(() => f.db.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.db.workLaneRows()[0]!.last_activity_at! > activity!).toBe(true);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]).toMatchObject({ turnId: result.opRef, origin, text: "[lane a] completed: done" });
	expect(f.db.deliveryRows()).toHaveLength(1);
	await f.manager.recover();
	expect(f.notices).toHaveLength(1);
});

test("simultaneous cap admission and same-name resume refuse without overlapping sends", async () => {
	const f = await fixture();
	const results = await Promise.allSettled([started(f, "a"), started(f, "b"), started(f, "c")]);
	expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(2);
	const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
	expect(rejected.reason).toMatchObject({ code: "lane_capacity", detail: { active: 2, maxLanes: 2 } });
	for (const resume of [false, true])
		await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, resume })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "attempt_open" },
		});
	expect(f.port.binds).toHaveLength(2);
	expect(f.port.sends).toHaveLength(2);
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: false });
	expect(await f.lanes.sweep(Date.now() + 1e9)).toBe(0);
});

test("status is pure during a run wait and steer returns the exact supplied clientRef", async () => {
	const f = await fixture();
	const owner = {};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, owner);
	await until(() => f.port.sends.length === 1);
	const op = f.port.sends[0]!.opRef;
	const before = f.db.laneJobJson(laneJobIdentity("a").jobId);
	const activity = f.db.workLaneRows()[0]!.last_activity_at;
	const snapshot = await f.manager.status({ name: "a" });
	expect(snapshot).toMatchObject({ attempt: { opRef: op }, op: { status: "in_flight" } });
	expect(f.db.laneJobJson(snapshot.jobId)).toBe(before);
	expect(f.db.workLaneRows()[0]!.last_activity_at).toBe(activity);
	const steer = await f.manager.steer({ name: "a", text: "correction" });
	expect(steer.steered).toBe(true);
	if (steer.steered) expect(f.port.steers[0]?.clientRef).toBe(steer.clientRef);
	f.port.complete(op, "full answer");
	expect(await run).toMatchObject({ held: false, text: "full answer" });
	expect(f.notices).toHaveLength(0);
	expect(f.port.binds).toHaveLength(1);
	await expect(f.manager.steer({ name: "a", text: "late" })).rejects.toMatchObject({
		detail: { reasonCode: "no_open_attempt" },
	});
});

test("status unknown, malformed and changed-binding edges never rewrite durable evidence", async () => {
	const f = await fixture();
	const result = await started(f);
	const before = f.db.laneJobJson(result.jobId);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	expect((await f.manager.status({ name: "a" })).op?.status).toBe("unknown");
	f.port.status = async () => ({ operationRef: "wrong", status: { status: "terminal_ok" }, summaryCompleted: true });
	await expect(f.manager.status({ name: "a" })).rejects.toMatchObject({ detail: { reasonCode: "status_unavailable" } });
	expect(f.db.laneJobJson(result.jobId)).toBe(before);
	f.db.rebindEpoch(workSessionKey("a"));
	expect(await f.manager.status({ name: "a" })).toMatchObject({ sessionId: "", op: null });
	await expect(f.manager.status({ name: "missing" })).rejects.toMatchObject({
		detail: { reasonCode: "unknown_work_lane" },
	});
});

test("steer distinguishes structured refusal from transport uncertainty without leaking text", async () => {
	const f = await fixture();
	await started(f);
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "busy" });
	};
	expect(await f.manager.steer({ name: "a", text: "change" })).toEqual({
		steered: false,
		reason: "steer_refused:busy",
	});
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "private_policy_code", refused: true });
	};
	expect(await f.manager.steer({ name: "a", text: "change" })).toEqual({
		steered: false,
		reason: "steer_refused:sdk_refused",
	});
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "receipt_identity_mismatch" });
	};
	await expect(f.manager.steer({ name: "a", text: "change" })).rejects.toMatchObject({
		detail: { reasonCode: "steer_acceptance_uncertain" },
	});
	f.port.steer = async () => {
		throw new Error("secret token");
	};
	await expect(f.manager.steer({ name: "a", text: "change" })).rejects.toMatchObject({
		message: "work steer acceptance uncertain",
		detail: { reasonCode: "steer_acceptance_uncertain" },
	});
	expect(f.port.sends).toHaveLength(1);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("caller timeout only detaches and a later completion settles the same operation response-only", async () => {
	const f = await fixture({ waitTimeoutMs: 25, ownerTarget: () => origin });
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
	await expect(run).rejects.toMatchObject({ detail: { reasonCode: "work_wait_timeout" } });
	const op = f.port.sends[0]!.opRef;
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	f.port.complete(op, "late answer");
	await until(() => f.db.workAttemptGet(op)?.settledAt !== null);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices).toHaveLength(0);
});

test("disconnect and AbortSignal release only their own waits, not worker observation", async () => {
	const f = await fixture();
	const ownerA = {};
	const ownerB = {};
	const abort = new AbortController();
	const a = f.manager.run({ name: "a", text: "work", cwd: f.directory }, ownerA).catch((e: unknown) => e);
	const b = f.manager.run({ name: "b", text: "work", cwd: f.directory }, ownerB, abort.signal).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 2);
	f.manager.detachWaiters(ownerA);
	expect(await a).toBeInstanceOf(ProtocolError);
	expect(f.job("b").attempts[0]?.endedAt).toBeUndefined();
	abort.abort();
	expect(await b).toBeInstanceOf(ProtocolError);
	for (const send of f.port.sends) f.port.complete(send.opRef, "finished after caller left");
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.port.sends).toHaveLength(2);
});

test("a disconnect before send receipt cannot leave a newly registered waiter behind", async () => {
	const f = await fixture();
	const receipt = deferred<void>();
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		const result = await send(input);
		await receipt.promise;
		return result;
	};
	const owner = {};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, owner).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 1);
	f.manager.detachWaiters(owner);
	receipt.resolve();
	expect(await run).toBeInstanceOf(ProtocolError);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("notification target is immutable across owner reload; no-target remains no-target", async () => {
	let owner: OriginRef | undefined = origin;
	const f = await fixture({ ownerTarget: () => owner });
	const first = await started(f);
	owner = { ...origin, conversationId: "new-target" };
	f.port.complete(first.opRef, "first");
	await until(() => f.notices.length === 1);
	expect(f.notices[0]?.origin).toEqual(origin);
	owner = undefined;
	const second = await started(f, "b");
	owner = origin;
	f.port.complete(second.opRef, "second");
	await until(() => f.db.workAttemptGet(second.opRef)?.settledAt !== null);
	expect(f.notices).toHaveLength(1);
	expect(f.db.workAttemptGet(second.opRef)?.decision).toBe("no_target");
});

test("proven original silence suppresses before prefix and UTF8 clipping, and survives restart", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.workAttemptGet(result.opRef)?.output.knownSilence).toMatchObject({
		opRef: result.opRef,
		source: "turn.result",
		fullness: "original",
	});
	expect(f.db.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	await f.restart();
	expect(f.notices).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
	expect(utf8Prefix("HEAD" + "가".repeat(1000) + "TAIL")).toBe("HEAD" + "가".repeat(681));
	expect(utf8Prefix("😀가tail", 6)).toBe("😀");
	expect(utf8Prefix("😀가tail", 7)).toBe("😀가");
});

for (const [label, text] of [
	["preamble", "Nothing to report. [SILENT]"],
	["lowercase", "Nothing to report. [silent]"],
	["beyond excerpt", "HEAD:" + "가".repeat(1000) + "[SILENT]"],
] as const) {
	test(`proven original ${label} silence persists before clipping and suppresses start delivery`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, text);
		await until(() => f.db.workAttemptGet(result.opRef)?.settledAt != null);
		const runtime = f.db.workAttemptGet(result.opRef)!;
		expect(runtime.output.disposition).toBe("silent");
		expect(runtime.output.knownSilence).toMatchObject({
			opRef: result.opRef,
			sessionId: result.sessionId,
			epoch: runtime.epoch,
			source: "turn.result",
			fullness: "original",
			attribution: "operation_ref",
			clientRef: result.opRef,
			byteLength: Buffer.byteLength(text),
		});
		expect(runtime.output.knownSilence).toEqual(runtime.output.proof);
		expect(runtime.output.excerpt).toBe(utf8Prefix(text));
		if (label === "beyond excerpt") expect(runtime.output.excerpt).not.toContain("[SILENT]");
		expect(runtime.decision).toBe("suppressed");
		expect(f.notices).toHaveLength(0);
		expect(f.db.deliveryRows()).toHaveLength(0);
		await f.restart();
		expect(f.db.workAttemptGet(result.opRef)?.output.knownSilence).toEqual(runtime.output.knownSilence);
		expect(f.notices).toHaveLength(0);
		expect(f.db.deliveryRows()).toHaveLength(0);
	});

	test(`run returns full original ${label} silence body without a completion notification`, async () => {
		const f = await fixture({ ownerTarget: () => origin });
		const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
		await until(() => f.port.sends.length === 1);
		const opRef = f.port.sends[0]!.opRef;
		f.port.complete(opRef, text);
		expect(await run).toMatchObject({ held: false, text, opRef });
		const runtime = f.db.workAttemptGet(opRef)!;
		expect(runtime.mode).toBe("run");
		expect(runtime.target).toBeNull();
		expect(runtime.output.knownSilence).toMatchObject({
			opRef,
			byteLength: Buffer.byteLength(text),
			fullness: "original",
		});
		expect(runtime.output.disposition).toBe("silent");
		expect(runtime.decision).toBe("suppressed");
		expect(f.notices).toHaveLength(0);
		expect(f.db.deliveryRows()).toHaveLength(0);
	});
}

test("unavailable output preserves completed outcome without prior assistant fallback", async () => {
	const f = await fixture();
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "identity_mismatch" });
	f.port.fetchLastAssistant = async () => {
		throw new Error("must not read unscoped output");
	};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
	await until(() => f.port.sends.length === 1);
	f.port.complete(f.port.sends[0]!.opRef, "untrusted");
	await expect(run).rejects.toMatchObject({ detail: { reasonCode: "output_unavailable" } });
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices).toHaveLength(0);
});

test("three durable output read claims retain 1s/5s eligibility across restart", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	let reads = 0;
	f.port.fetchWorkerOutput = async () => {
		reads++;
		return { status: "absent", code: "output_pending" };
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "not published");
	await until(() => reads === 1);
	expect(f.db.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	await f.restart();
	await Bun.sleep(20);
	expect(reads).toBe(1);
	now += 1000;
	await until(() => reads === 2);
	expect(f.db.workAttemptGet(result.opRef)?.output.reads).toBe(2);
	now += 4999;
	await Bun.sleep(20);
	expect(reads).toBe(2);
	now += 1;
	await until(() => f.db.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(reads).toBe(3);
	expect(f.notices[0]?.text).toBe("[lane a] completed: output_unavailable");
});

for (const [reason, state] of [
	["cancelled", "attempt_ended"],
	["max_tokens", "attempt_ended"],
	["max_turn_requests", "attempt_ended"],
	["refusal", "attempt_ended"],
	["unknown_reason", "attempt_ended"],
] as const) {
	test(`terminal ${reason} is not run success and notifications preserve safe reason`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, "가".repeat(1000));
		f.port.status = async (input) => ({
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState: "present", outcome: { reason } },
			summaryCompleted: true,
		});
		await until(() => f.db.workAttemptGet(result.opRef)?.settledAt !== null);
		expect(f.job().attempts[0]?.endState).toBe(state);
		const suffix = f.notices[0]!.text.slice("[lane a] attempt_ended: ".length);
		expect(suffix.startsWith(`${reason === "unknown_reason" ? "stopped_incomplete" : reason}: `)).toBe(true);
		expect(Buffer.byteLength(suffix)).toBeLessThanOrEqual(2048);
	});
}

test("live restart reattaches exact session/op without bind, resume or replay", async () => {
	const f = await fixture();
	const result = await started(f);
	await f.restart();
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, resume: true })).rejects.toMatchObject({
		detail: { reasonCode: "attempt_open" },
	});
	f.port.complete(result.opRef, "recovered");
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("completed");
});

test("live unknown restart observes without replay or false acceptance", async () => {
	const f = await fixture();
	const result = await started(f);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	await f.restart();
	await Bun.sleep(20);
	expect(f.db.workAttemptGet(result.opRef)?.terminal).toBeNull();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("dead restart settles uncertainty into a sticky hold, never replacement bind", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.setSessionState(result.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	expect(f.db.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("session_dead");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.port.sends).toHaveLength(1);
	expect(await f.manager.start({ name: "a", text: "again", cwd: f.directory })).toMatchObject({
		started: false,
		held: true,
	});
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: session_dead: output_unavailable");
});

test("saved terminal proof wins over dead liveness and output recovery consumes saved budget", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const original = f.port.fetchWorkerOutput.bind(f.port);
	f.port.fetchWorkerOutput = async () => ({ status: "absent", code: "output_pending" });
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "durable result");
	await until(() => f.db.workAttemptGet(result.opRef)?.output.reads === 1);
	f.port.setSessionState(result.sessionId, { live: false });
	f.port.status = async () => {
		throw new Error("offline");
	};
	f.port.fetchWorkerOutput = original;
	now += 1000;
	await f.restart();
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices[0]?.text).toBe("[lane a] completed: durable result");
});

test("historical open attempt recovery uses no current notification target and never replays", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	const { jobId, laneKey } = laneJobIdentity("old");
	const sessionId = crypto.randomUUID();
	f.db.putSession(workSessionKey("old"), sessionId);
	f.port.setSessionState(sessionId, { repo: f.directory, live: false });
	const job = appendAttempt(createLaneJobRecord({ jobId, branch: "work/old", worktreePath: f.directory }), {
		opRef: "old-open-attempt",
		sessionId,
		startedAt: new Date().toISOString(),
	});
	f.db.putLaneJob({ ...job, laneKey, json: JSON.stringify(job) });
	await f.manager.recover();
	await until(() => f.db.workAttemptGet("old-open-attempt")?.settledAt != null);
	expect(f.db.workAttemptGet("old-open-attempt")).toMatchObject({
		mode: "historical",
		target: null,
		decision: "no_target",
	});
	expect(f.notices).toHaveLength(0);
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

test("late output from an obsolete generation cannot settle or fan out", async () => {
	let generation = 1;
	const f = await fixture({ brokerGeneration: () => generation });
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	const original = f.port.fetchWorkerOutput.bind(f.port);
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	await until(() => reading);
	const before = f.db.workAttemptGet(result.opRef)!;
	generation++;
	const recovery = f.manager.onBrokerGeneration();
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await recovery;
	expect(f.db.workAttemptGet(result.opRef)?.output.disposition).toBe("pending");
	expect(f.notices).toHaveLength(0);
	expect(f.db.workAttemptGet(result.opRef)?.version).toBe(before.version);
	f.port.fetchWorkerOutput = original;
});

test("binding epoch fences an already-running output read", async () => {
	const f = await fixture();
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	await until(() => reading);
	const before = f.db.workAttemptGet(result.opRef)!;
	f.db.rebindEpoch(result.sessionKey);
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await Bun.sleep(20);
	expect(f.db.workAttemptGet(result.opRef)?.version).toBe(before.version);
	expect(f.notices).toHaveLength(0);
});

test("stop releases run before draining a blocked finite output read and preserves open durable state", async () => {
	const f = await fixture();
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {}).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 1);
	const op = f.port.sends[0]!.opRef;
	f.port.complete(op, "answer");
	await until(() => reading);
	const stop = f.manager.stop();
	expect(await run).toBeInstanceOf(ProtocolError);
	gate.resolve({ status: "unavailable", code: "cancelled" });
	await stop;
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.db.workAttemptGet(op)?.output.reads).toBe(1);
	expect(f.db.workAttemptGet(op)?.output.disposition).toBe("pending");
	expect(f.notices).toHaveLength(0);
});

test("invalid shared parameters have no bind/send effects and manager registration is unique", async () => {
	const f = await fixture();
	for (const params of [
		{ name: "../bad", text: "x" },
		{ name: "a", text: "" },
		{ name: "a", text: "x", cwd: "relative" },
		{ name: "a", text: "x", resume: "yes" },
		{ name: "a", text: "x", model: { preset: "x", extra: true } },
		{ name: "a", text: "x", notify: null },
	])
		await expect(f.manager.start(params)).rejects.toMatchObject({
			code: "invalid_params",
			message: "invalid work parameters",
		});
	await expect(f.manager.run({ name: "a", text: "x", notify: origin }, {})).rejects.toMatchObject({
		detail: { field: "notify" },
	});
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
	expect(() => new WorkLaneManager({ database: f.db, port: f.port, lanes: f.lanes })).toThrow("already registered");
});
// receiptState=missing is covered by the #248 late-receipt/held tests below:
// a proven same-op final body reconciles it instead of holding.
for (const [receiptState, reasonCode, endState] of [["unknown", "terminal_uncertain", "terminal_uncertain"]] as const) {
	test(`terminal receipt ${receiptState} is a held non-success with a safe notice`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, "partial answer");
		f.port.status = async (input) => ({
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState, outcome: { reason: "end_turn" } },
			summaryCompleted: true,
		});
		await until(() => f.db.workAttemptOpen().length === 0);
		expect(f.job().attempts[0]?.endState).toBe(endState);
		expect(f.job().state).toBe("awaiting_operator");
		expect(f.notices[0]?.text).toBe(`[lane a] attempt_ended: ${reasonCode}: partial answer`);
	});
}

test("late receipt after a missing-receipt terminal is reconciled once, without resend (#248)", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const original = f.port.fetchWorkerOutput.bind(f.port);
	let reads = 0;
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "PR opened");
	const status = f.port.status.bind(f.port);
	// The terminal is observed before the final body lands: the SDK reports
	// receiptState=missing, then enriches the same op to present.
	f.port.status = async (input) => {
		const report = await status(input);
		return { ...report, status: { ...report.status, receiptState: "missing" } };
	};
	f.port.fetchWorkerOutput = async (input) => {
		reads++;
		if (reads === 1) {
			const early = await original(input);
			expect(early.status).toBe("proven");
			return parseWorkerOutputResponse(
				input,
				{
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: {
							kind: "prompt",
							clientRef: input.opRef,
							...input.terminalIdentity,
							status: "terminal_ok",
							terminalAt: input.notBeforeMs + 1,
							receiptState: "missing",
						},
					}),
					stderr: "",
				},
				Date.now(),
			);
		}
		return original(input);
	};
	await until(() => reads === 1);
	expect(f.db.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("terminal_missing_receipt");
	expect(f.db.workAttemptGet(result.opRef)?.settledAt).toBeNull();
	now += 1000;
	await until(() => f.db.workAttemptGet(result.opRef)?.settledAt != null);
	expect(reads).toBe(2);
	expect(f.db.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("end_turn");
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.job().state).not.toBe("awaiting_operator");
	expect(f.notices.map((notice) => notice.text)).toEqual(["[lane a] completed: PR opened"]);
	// Reprocessing the same terminal neither re-notifies nor re-runs the work.
	await f.restart();
	await Bun.sleep(20);
	expect(f.notices).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("a receipt still missing after the output budget stays held with a non-loss diagnostic (#248)", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	let reads = 0;
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "unused");
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "terminal_ok", receiptState: "missing", outcome: { reason: "end_turn", kind: "stopped" } },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async (input) => {
		reads++;
		return parseWorkerOutputResponse(
			input,
			{
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						kind: "prompt",
						clientRef: input.opRef,
						status: "terminal_ok",
						terminalAt: input.notBeforeMs + 1,
						receiptState: "missing",
					},
				}),
				stderr: "",
			},
			Date.now(),
		);
	};
	await until(() => reads === 1);
	now += 1000;
	await until(() => reads === 2);
	now += 5000;
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(reads).toBe(3);
	expect(f.job().attempts[0]?.endState).toBe("terminal_missing_receipt");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices.map((notice) => notice.text)).toEqual([
		`[lane a] attempt_ended: terminal_missing_receipt: final_response_missing opRef=${result.opRef}`,
	]);
	expect(f.port.sends).toHaveLength(1);
});

test("broker deadline ends the attempt but caller timeout never does", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", error: { code: "prompt_deadline_exceeded", message: "secret" } },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("attempt_ended");
	expect(f.db.workAttemptGet(result.opRef)?.terminal?.status?.error).toEqual({ code: "prompt_deadline_exceeded" });
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: prompt_deadline_exceeded: output_unavailable");
});

test("torn send accepts only same-op status evidence; unknown remains open with no resend", async () => {
	const f = await fixture();
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		await send(input);
		throw new Error("lost receipt secret");
	};
	const result = await started(f);
	expect(f.db.workAttemptGet(result.opRef)?.sendEvidence?.source).toBe("status");
	f.port.send = async () => {
		throw new Error("lost receipt secret");
	};
	await expect(f.manager.start({ name: "b", text: "work", cwd: f.directory })).rejects.toMatchObject({
		message: "work send acceptance uncertain",
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	const pending = f.job("b").attempts[0]!;
	expect(f.db.workAttemptGet(pending.opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	expect(f.job("b").attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("definitive send rejection atomically holds and enqueues one safe start-only notice", async () => {
	const f = await fixture();
	f.port.send = async () => {
		throw new GjcCliError("secret", 0, "", { code: "busy" });
	};
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.directory, notify: origin })).rejects.toMatchObject({
		detail: { reasonCode: "send_rejected" },
	});
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("failed");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices[0]?.text).toBe("[lane a] failed: send_rejected: output_unavailable");
	expect(f.db.deliveryRows()).toHaveLength(1);
});

test("CAS loss does not fan out or perform a separate history/activity update", async () => {
	const f = await fixture();
	const settle = f.db.workAttemptSettle.bind(f.db);
	let calls = 0;
	f.db.workAttemptSettle = () => {
		calls++;
		return undefined;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	const history = f.db.laneJobJson(result.jobId);
	const activity = f.db.workLaneRows()[0]!.last_activity_at;
	await until(() => calls > 0);
	expect(f.db.laneJobJson(result.jobId)).toBe(history);
	expect(f.db.workLaneRows()[0]!.last_activity_at).toBe(activity);
	expect(f.notices).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
	f.db.workAttemptSettle = settle;
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.notices).toHaveLength(1);
});

test("persisted original silence survives a crash between output staging and settlement", async () => {
	const f = await fixture();
	const settle = f.db.workAttemptSettle.bind(f.db);
	f.db.workAttemptSettle = () => {
		throw new Error("injected pre-transaction crash");
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.workAttemptGet(result.opRef)?.output.knownSilence != null);
	await f.manager.stop();
	f.db.workAttemptSettle = settle;
	f.port.fetchWorkerOutput = async () => {
		throw new Error("original output no longer accessible");
	};
	await f.restart();
	await until(() => f.db.workAttemptOpen().length === 0);
	expect(f.db.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	expect(f.db.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	expect(f.notices).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
});

test("reopening SQLite recovers the same live operation and immutable notification intent", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-work-reopen-"));
	const path = join(directory, "gateway.db");
	let db = await GatewayDatabase.open(path);
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const id = crypto.randomUUID();
			db.putSession(input.originKey, id);
			return id;
		},
	});
	const notices: ChatMessagePayload[] = [];
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port }),
		pollMs: 5,
		deliver: (payload) => notices.push(payload),
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const result = await manager.start({ name: "a", text: "work", cwd: directory, notify: origin });
	if (!result.started) throw new Error("unexpected hold");
	await manager.stop();
	db.close();
	db = await GatewayDatabase.open(path);
	manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port }),
		pollMs: 5,
		ownerTarget: () => ({ ...origin, conversationId: "changed" }),
		deliver: (payload) => notices.push(payload),
	});
	await manager.recover();
	expect(db.workAttemptOpen()).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
	expect(port.resumes).toHaveLength(0);
	port.complete(result.opRef, "after database reopen");
	await until(() => db.workAttemptOpen().length === 0);
	expect(notices).toHaveLength(1);
	expect(notices[0]?.origin).toEqual(origin);
	expect(notices[0]?.turnId).toBe(result.opRef);
});
for (const status of ["terminal_ok", "failed"] as const) {
	for (const receiptState of ["missing", "unknown", "absent", undefined, "present"] as const) {
		for (const observedAtSend of [true, false]) {
			test(`torn send ${status}/${receiptState} accounting at ${observedAtSend ? "send" : "poll"} never invents acceptance`, async () => {
				const f = await fixture();
				let queries = 0;
				let sends = 0;
				f.port.send = async () => {
					sends++;
					throw new Error("lost send receipt");
				};
				f.port.status = async (input) => {
					queries++;
					return {
						operationRef: input.opRef,
						summaryCompleted: true,
						status:
							!observedAtSend && queries === 1
								? { status: "unknown" }
								: {
										status,
										receiptState,
										outcome: { reason: "end_turn" },
										error: status === "failed" ? { code: "sdk_failed" } : undefined,
									},
					};
				};
				f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
				const response = await f.manager
					.start({ name: "a", text: "work", cwd: f.directory })
					.catch((error: unknown) => error);
				if (observedAtSend && receiptState === "present") expect(response).toMatchObject({ started: true });
				else
					expect(response).toMatchObject({ code: "verb_failed", detail: { reasonCode: "send_acceptance_uncertain" } });
				const opRef = f.job().attempts[0]!.opRef;
				await until(() => f.db.workAttemptGet(opRef)?.settledAt != null);
				const runtime = f.db.workAttemptGet(opRef)!;
				expect(runtime.sendPhase).toBe(receiptState === "present" ? "accepted" : "uncertain");
				if (receiptState === "present") expect(runtime.sendEvidence?.source).toBe("status");
				else expect(runtime.sendEvidence).toBeNull();
				expect(runtime.terminal?.status).toMatchObject({ status });
				expect(f.job().attempts).toHaveLength(1);
				await f.manager.recover();
				expect(sends).toBe(1);
				expect(f.port.sends).toHaveLength(0);
				expect(f.port.binds).toHaveLength(1);
				expect(f.port.resumes).toHaveLength(0);
			});
		}
	}
}

test("completion excerpt retains the leading marker and scalar-safe prefix, not the tail", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "HEAD:" + "😀".repeat(1000) + ":TAIL");
	await until(() => f.db.workAttemptGet(result.opRef)?.settledAt != null);
	const expected = "HEAD:" + "😀".repeat(510);
	expect(f.db.workAttemptGet(result.opRef)?.output.excerpt).toBe(expected);
	expect(f.notices[0]?.text).toBe(`[lane a] completed: ${expected}`);
	expect(f.notices[0]?.text.includes(":TAIL")).toBe(false);
});

test("a queued steer captured for A refuses after A settles and B starts", async () => {
	const f = await fixture();
	const a = await started(f);
	const entered = deferred<void>();
	const release = deferred<void>();
	const exclusive = f.port.runExclusive.bind(f.port);
	const recover = f.manager.recover.bind(f.manager);
	// Recovery is already registered; isolate the steer mutation lock boundary.
	f.manager.recover = async () => {};
	let intercept = true;
	f.port.runExclusive = async <T>(key: string, work: () => Promise<T>): Promise<T> => {
		if (intercept && key === workSessionKey("a")) {
			intercept = false;
			entered.resolve();
			await release.promise;
		}
		return exclusive(key, work);
	};
	const steer = f.manager.steer({ name: "a", text: "for A only" }).catch((error: unknown) => error);
	try {
		await entered.promise;
		f.port.complete(a.opRef, "A complete");
		await until(() => f.db.workAttemptGet(a.opRef)?.settledAt != null);
		const b = await started(f);
		expect(b.opRef).not.toBe(a.opRef);
		const binds = f.port.binds.length;
		const sends = f.port.sends.length;
		release.resolve();
		expect(await steer).toMatchObject({ code: "invalid_params", detail: { reasonCode: "no_open_attempt" } });
		expect(f.port.steers).toHaveLength(0);
		expect(f.port.binds).toHaveLength(binds);
		expect(f.port.sends).toHaveLength(sends);
		expect(sends).toBe(2);
		expect(f.db.workAttemptGet(b.opRef)?.settledAt).toBeNull();
	} finally {
		release.resolve();
		await steer;
		f.port.runExclusive = exclusive;
		f.manager.recover = recover;
	}
});

test("quarantined accepted work reserves its name without querying the shared broker", async () => {
	const f = await fixture();
	const old = await started(f, "old", origin);
	await f.manager.stop();
	const history = f.db.laneJobJson(old.jobId);
	const runtime = f.db.workAttemptGet(old.opRef);
	const authority = { canonicalAgentDir: "/tmp/global-agent", identity: "shared-broker" };
	f.db.cutoverBrokerAuthority({
		expectedAuthority: null,
		targetAuthority: authority,
		evidence: "test operator quarantined private broker work",
		disposition: "quarantine",
	});
	// The shared broker may answer the same old ID; it must never be asked.
	f.port.setSessionState(old.sessionId, { repo: f.directory, live: true });
	const calls: string[] = [];
	for (const method of [
		"status",
		"liveness",
		"inspect",
		"resume",
		"steer",
		"close",
		"attachTail",
		"fetchWorkerOutput",
	] as const) {
		const original = f.port[method];
		Object.assign(f.port, {
			[method]: (...args: unknown[]) => {
				calls.push(method);
				return Reflect.apply(original, f.port, args);
			},
		});
	}
	const binds = f.port.binds.length;
	const sends = f.port.sends.length;
	await f.restart();
	const failure = {
		code: "verb_failed",
		message: "work lane belongs to a quarantined broker authority",
		detail: { reasonCode: "broker_authority_quarantined", jobId: old.jobId, name: "old" },
	};
	await expect(f.manager.status({ name: "old" })).rejects.toMatchObject(failure);
	for (const resume of [false, true]) {
		await expect(f.manager.start({ name: "old", text: "again", cwd: f.directory, resume })).rejects.toMatchObject(
			failure,
		);
		await expect(f.manager.run({ name: "old", text: "again", cwd: f.directory, resume }, {})).rejects.toMatchObject(
			failure,
		);
	}
	await expect(f.manager.steer({ name: "old", text: "change" })).rejects.toMatchObject(failure);
	expect(await f.lanes.retire("old", "operator")).toEqual({
		retired: false,
		sessionKey: workSessionKey("old"),
		reason: "broker_authority_quarantined",
	});
	expect(calls).toEqual([]);
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sends).toHaveLength(sends);
	expect(f.db.laneJobJson(old.jobId)).toBe(history);
	expect(f.db.workAttemptGet(old.opRef)).toEqual(runtime);
	expect(f.db.laneJobRows()).toEqual([]);
	expect(f.db.laneJobRows(true).map((row) => row.job_id)).toEqual([old.jobId]);
	// Explicit test-only provenance models a successful shared SDK bind.
	f.port.bind = async (input) => {
		const binding = { ...input, sessionId: crypto.randomUUID() };
		f.db.recordOwnedBinding({ ...binding, authority });
		return binding;
	};
	const fresh = await started(f, "fresh");
	expect(fresh.jobId).not.toBe(old.jobId);
	expect(f.port.sends).toHaveLength(sends + 1);
	expect(f.notices).toHaveLength(0);
});
