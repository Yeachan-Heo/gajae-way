import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ChatMessagePayload, type OriginRef, originKey, ProtocolError } from "@gajae-gateway/protocol";
import {
	appendAttempt,
	closeAttempt,
	createLaneJobRecord,
	GjcCliError,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import { GjcCliUnavailableError } from "../src/orchestrator/broker";
import { classifyCheckRollup } from "../src/orchestrator/gh-checks";
import { LaneGovernor, laneJobIdentity, workSessionKey } from "../src/orchestrator/lane-governor";
import { parseWorkerOutputResponse, type WorkerOutputResult } from "../src/orchestrator/session-port";
import {
	type CheckStatusClient,
	type CheckStatusSnapshot,
	laneLastCommits,
	laneSystemNotice,
	reportText,
	utf8Prefix,
	WorkLaneManager,
	type WorkLaneManagerOptions,
} from "../src/orchestrator/work-lane";
import { Store } from "../src/store";
import { WorkAttemptStateError } from "../src/store/groups/work";
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
	const gajaewayHome = join(directory, "gajaeway");
	// Create gajaewayHome directory for service directory validation tests
	await mkdir(gajaewayHome, { recursive: true });
	const db = await Store.open(join(directory, "gateway.db"));
	const port = new ScriptedSessionPort({
		...portOptions,
		onBind: (input) => {
			const sessionId = db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
			db.transaction((tx) => db.bindings.bind(tx, input.originKey, sessionId));
			return sessionId;
		},
	});
	const notices: ChatMessagePayload[] = [];
	const lanes = new LaneGovernor({ database: db, sessionPort: port, maxLanes: 2 });
	let implicitOwner: OriginRef | undefined;
	const ownerTarget = options.ownerTarget ?? (() => implicitOwner);
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes,
		gajaewayHome,
		ownerTarget,
		pollMs: 5,
		waitTimeoutMs: 2000,
		deliverFallback: (payload) => notices.push(payload),
		// Use gajaewayHome as the service directory for tests (for issue #480 validation)
		serviceDirectory: gajaewayHome,
		...options,
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const job = (name = "a") => parseLaneJobRecord(db.work.laneJobJson(laneJobIdentity(name).jobId)!);
	return {
		db,
		port,
		lanes,
		notices,
		directory,
		gajaewayHome,
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
				gajaewayHome,
				ownerTarget,
				pollMs: 5,
				deliverFallback: (payload) => notices.push(payload),
				...options,
				...extra,
			});
			await manager.recover();
			return manager;
		},
		setOwnerTarget: (target: OriginRef | undefined) => {
			implicitOwner = target;
		},
	};
}
async function started(f: Awaited<ReturnType<typeof fixture>>, name = "a", parentOrigin?: OriginRef) {
	if (parentOrigin) f.setOwnerTarget(parentOrigin);
	const result = await f.manager.start({ name, text: "work", cwd: f.directory });
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
		await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);

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
	expect(f.db.work.workAttemptGet(opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	await f.restart();
	expect(attempts).toBe(1);
	expect(Object.hasOwn(f.port.sendAttempts[0]!, "model")).toBe(false);
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.db.work.workAttemptGet(opRef)?.sendEvidence).toBeNull();
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
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	await Bun.sleep(10);
	f.port.complete(result.opRef, "done");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at! > activity!).toBe(true);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]).toMatchObject({ turnId: result.opRef, origin, text: "[lane a] completed: done" });
	expect(f.db.outbox.rows()).toHaveLength(1);
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
	const before = f.db.work.laneJobJson(laneJobIdentity("a").jobId);
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	const snapshot = await f.manager.status({ name: "a" });
	expect(snapshot).toMatchObject({ attempt: { opRef: op }, op: { status: "in_flight" } });
	expect(f.db.work.laneJobJson(snapshot.jobId)).toBe(before);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(activity);
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
	const before = f.db.work.laneJobJson(result.jobId);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	expect((await f.manager.status({ name: "a" })).op?.status).toBe("unknown");
	f.port.status = async () => ({ operationRef: "wrong", status: { status: "terminal_ok" }, summaryCompleted: true });
	await expect(f.manager.status({ name: "a" })).rejects.toMatchObject({ detail: { reasonCode: "status_unavailable" } });
	expect(f.db.work.laneJobJson(result.jobId)).toBe(before);
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, workSessionKey("a")));
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
	await until(() => f.db.work.workAttemptGet(op)?.settledAt !== null);
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
	await until(() => f.db.work.workAttemptOpen().length === 0);
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
	await until(() => f.db.work.workAttemptGet(second.opRef)?.settledAt !== null);
	expect(f.notices).toHaveLength(1);
	expect(f.db.work.workAttemptGet(second.opRef)?.decision).toBe("no_target");
});

test("proven original silence suppresses before prefix and UTF8 clipping, and survives restart", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.knownSilence).toMatchObject({
		opRef: result.opRef,
		source: "turn.result",
		fullness: "original",
	});
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	await f.restart();
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
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
		await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
		const runtime = f.db.work.workAttemptGet(result.opRef)!;
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
		expect(f.db.outbox.rows()).toHaveLength(0);
		await f.restart();
		expect(f.db.work.workAttemptGet(result.opRef)?.output.knownSilence).toEqual(runtime.output.knownSilence);
		expect(f.notices).toHaveLength(0);
		expect(f.db.outbox.rows()).toHaveLength(0);
	});

	test(`run returns full original ${label} silence body without a completion notification`, async () => {
		const f = await fixture({ ownerTarget: () => origin });
		const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
		await until(() => f.port.sends.length === 1);
		const opRef = f.port.sends[0]!.opRef;
		f.port.complete(opRef, text);
		expect(await run).toMatchObject({ held: false, text, opRef });
		const runtime = f.db.work.workAttemptGet(opRef)!;
		expect(runtime.mode).toBe("run");
		expect(runtime.parent).toBeNull();
		expect(runtime.output.knownSilence).toMatchObject({
			opRef,
			byteLength: Buffer.byteLength(text),
			fullness: "original",
		});
		expect(runtime.output.disposition).toBe("silent");
		expect(runtime.decision).toBe("suppressed");
		expect(f.notices).toHaveLength(0);
		expect(f.db.outbox.rows()).toHaveLength(0);
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
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	await f.restart();
	await Bun.sleep(20);
	expect(reads).toBe(1);
	now += 1000;
	await until(() => reads === 2);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(2);
	now += 4999;
	await Bun.sleep(20);
	expect(reads).toBe(2);
	now += 1;
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
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
		await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
		expect(f.job().attempts[0]?.endState).toBe(state);
		const suffix = f.notices[0]!.text.slice("[lane a] attempt_ended: ".length);
		expect(suffix.startsWith(`${reason === "unknown_reason" ? "stopped_incomplete" : reason}: `)).toBe(true);
		expect(Buffer.byteLength(suffix)).toBeLessThanOrEqual(2048);
	});
}

test("failing status reads back off and log one diagnostic per distinct reason (#262)", async () => {
	const f = await fixture();
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const result = await started(f);
	let queries = 0;
	let message = "reconciliation unavailable token=sk-secret-value";
	f.port.status = async () => {
		queries++;
		throw new Error(message);
	};
	await Bun.sleep(300);
	// Fixed 5ms polling would have issued ~60 reads; backoff bounds it.
	expect(queries).toBeGreaterThan(0);
	expect(queries).toBeLessThan(12);
	const lines = () =>
		errors.mock.calls
			.map((call) => String(call[0]))
			.filter((line) => line.startsWith("work_reconciliation_unavailable"));
	expect(lines()).toHaveLength(1);
	expect(lines()[0]).toContain(`opRef=${result.opRef}`);
	expect(lines()[0]).toContain("reason=Error: reconciliation unavailable");
	expect(lines()[0]).not.toContain("sk-secret-value");
	message = "offline";
	await until(() => lines().length === 2);
	expect(lines()[1]).toContain("reason=Error: offline");
	expect(f.db.work.workAttemptGet(result.opRef)?.settledAt).toBeNull();
});

test("a rebound lane stops the observer and settles the attempt as session_disowned (#262)", async () => {
	const f = await fixture({ pollMs: 1 });
	const result = await started(f, "a", origin);
	let queries = 0;
	f.port.status = async () => {
		queries++;
		throw new Error("invalid status identity");
	};
	await until(() => queries > 0);
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, result.sessionKey));
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("session_disowned");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: session_disowned: output_unavailable");
	const settledQueries = queries;
	await Bun.sleep(50);
	expect(queries).toBe(settledQueries);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("persistently failing status without liveness proof settles instead of polling forever (#262)", async () => {
	const f = await fixture({ pollMs: 1 });
	const result = await started(f, "a", origin);
	let queries = 0;
	f.port.status = async () => {
		queries++;
		throw new Error("offline");
	};
	f.port.liveness = async () => {
		throw new Error("offline");
	};
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(queries).toBeLessThanOrEqual(10);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("recovery_indeterminate");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	const settledQueries = queries;
	await Bun.sleep(50);
	expect(queries).toBe(settledQueries);
	expect(f.port.sends).toHaveLength(1);
});

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
	await until(() => f.db.work.workAttemptOpen().length === 0);
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
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("dead restart settles uncertainty into a sticky hold, never replacement bind", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.setSessionState(result.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("session_dead");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.port.sends).toHaveLength(1);
	expect(await f.manager.start({ name: "a", text: "again", cwd: f.directory })).toMatchObject({
		started: false,
		held: true,
	});
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: session_dead: output_unavailable");
});

test("force-retiring an open dead lane settles its runtime and permits later work", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	const result = await started(f, "a", origin);
	f.port.setSessionState(result.sessionId, { live: false });

	expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: ["a"] });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime).toBeDefined();
	expect(attempt).toBeDefined();
	expect(runtime?.settledAt).toBe(attempt?.endedAt);
	expect(runtime?.terminal).toMatchObject({ kind: "local", reasonCode: "session_dead" });
	expect(attempt).toMatchObject({ endState: "attempt_ended", errorCode: "host_lost" });
	expect(f.db.work.workAttemptOpen()).toHaveLength(0);

	const next = await f.manager.start({ name: "a", text: "again", cwd: f.directory, resume: true });
	expect(next.started).toBe(true);
	await expect(f.manager.recover()).resolves.toBeUndefined();
});

test("force retirement releases a pending work.run waiter promptly", async () => {
	const f = await fixture({ waitTimeoutMs: 60_000 });
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {}).then(
		(value) => ({ value }),
		(error: unknown) => ({ error }),
	);
	await until(() => f.port.sends.length === 1);
	await Bun.sleep(10);
	const { sessionId } = f.port.sends[0]!;
	f.port.setSessionState(sessionId, { live: false });

	expect(await f.lanes.forceRetire("a")).toMatchObject({ retired: true });
	const outcome = await Promise.race([run, Bun.sleep(250).then(() => null)]);
	expect(outcome).not.toBeNull();
	expect(outcome).toMatchObject({ error: { detail: { reasonCode: "session_dead" } } });
});

test("force retirement preserves a recorded terminal outcome in lane history", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	f.port.fetchWorkerOutput = async () => ({ status: "absent", code: "output_pending" });
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "durable result");
	await until(() => {
		const runtime = f.db.work.workAttemptGet(result.opRef);
		return runtime?.terminal?.reasonCode === "end_turn" && runtime.output.reads > 0;
	});
	f.port.setSessionState(result.sessionId, { live: false });

	expect(await f.lanes.forceRetire("a")).toMatchObject({ retired: true });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime?.terminal).toMatchObject({ kind: "broker", reasonCode: "end_turn" });
	expect(attempt).toMatchObject({ endState: "completed" });
	expect(attempt?.errorCode).toBeUndefined();
});

test("force-retiring a disowned lane records a session_disowned terminal", async () => {
	const f = await fixture();
	const result = await started(f);
	f.port.liveness = async () => ({ live: undefined, disowned: true });

	expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: ["a"] });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime?.settledAt).toBe(attempt?.endedAt);
	expect(runtime?.terminal).toMatchObject({ kind: "local", reasonCode: "session_disowned" });
});

test("one inconsistent runtime is logged with its opRef and does not block other lane recovery", async () => {
	const f = await fixture();
	const bad = await started(f, "a");
	const good = await started(f, "b");
	await f.manager.stop();
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: bad.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt: new Date(Date.now() + 1_000).toISOString(),
	});
	const identity = laneJobIdentity("a");
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: identity.laneKey, json: JSON.stringify(closed) }),
	);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	let recoveredOther = false;
	const status = f.port.status.bind(f.port);
	f.port.status = async (input) => {
		if (input.opRef === good.opRef) recoveredOther = true;
		return status(input);
	};

	await expect(f.restart()).resolves.toBeDefined();
	await until(() => recoveredOther);

	// Verify that the invalid attempt was logged and then quarantined
	const errorLogged = errors.mock.calls.some((call) => {
		const line = String(call[0]);
		return (
			line.includes("work_recovery_invalid_attempt") &&
			line.includes(bad.opRef) &&
			line.includes("attempt.endedAt matches runtime.settledAt")
		);
	});
	expect(errorLogged).toBe(true);

	// Verify that the attempt is quarantined: it should not be queried in recovery
	const isQuarantined = f.db.bindings.isQuarantined("work", f.db.work.workAttemptJobId(bad.opRef)!);
	expect(isQuarantined).toBe(true);
	f.port.complete(good.opRef, "recovered other lane");
	await until(() => f.db.work.workAttemptGet(good.opRef)?.settledAt !== null);
});

test("an inconsistent runtime is quarantined and not re-logged on subsequent recovery calls (issue #407)", async () => {
	const f = await fixture();
	const bad = await started(f, "a");
	const good = await started(f, "b");
	await f.manager.stop();
	const endedAt = new Date(Date.now() + 1_000).toISOString();
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: bad.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt,
	});
	const identity = laneJobIdentity("a");
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: identity.laneKey, json: JSON.stringify(closed) }),
	);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	let recoveredOther = false;
	const status = f.port.status.bind(f.port);
	f.port.status = async (input) => {
		if (input.opRef === good.opRef) recoveredOther = true;
		return status(input);
	};

	await expect(f.restart()).resolves.toBeDefined();
	await until(() => recoveredOther);

	// Verify that the invalid attempt was logged
	const errorLogged = errors.mock.calls.some((call) => {
		const line = String(call[0]);
		return (
			line.includes("work_recovery_invalid_attempt") &&
			line.includes(bad.opRef) &&
			line.includes("attempt.endedAt matches runtime.settledAt")
		);
	});
	expect(errorLogged).toBe(true);

	const badJobId = f.db.work.workAttemptJobId(bad.opRef)!;
	// Verify that the attempt's job was quarantined
	expect(f.db.bindings.isQuarantined("work", badJobId)).toBe(true);

	// Verify that recovery doesn't re-log the same error on subsequent calls
	const errorCountBefore = errors.mock.calls.length;
	await f.manager.recover();
	const errorCountAfter = errors.mock.calls.length;
	// Should not have new work_recovery_invalid_attempt logs
	const newInvalidLogs = [...errors.mock.calls.slice(errorCountBefore)].filter((call) => {
		const line = String(call[0]);
		return line.includes("work_recovery_invalid_attempt") && line.includes(bad.opRef);
	});
	expect(newInvalidLogs.length).toBe(0);

	f.port.complete(good.opRef, "recovered other lane");
	await until(() => f.db.work.workAttemptGet(good.opRef)?.settledAt !== null);
});

test("a lane row torn under a live observer is logged and dropped, not an unhandled rejection (#401)", async () => {
	const rejections: unknown[] = [];
	const onRejection = (reason: unknown) => rejections.push(reason);
	process.on("unhandledRejection", onRejection);
	cleanups.push(async () => {
		process.off("unhandledRejection", onRejection);
	});
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const f = await fixture();
	const torn = await started(f, "a");
	const healthy = await started(f, "b");
	// The #401 shape: the lane row says the attempt ended host_lost while its
	// runtime row is still open, so every runtime read for it throws.
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: torn.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt: new Date(Date.now() + 1_000).toISOString(),
	});
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: laneJobIdentity("a").laneKey, json: JSON.stringify(closed) }),
	);
	expect(() => f.db.work.workAttemptGet(torn.opRef)).toThrow(WorkAttemptStateError);

	await until(() =>
		errors.mock.calls.some((call) => {
			const line = String(call[0]);
			return line.includes("work_observer_failed") && line.includes(torn.opRef);
		}),
	);
	// Several more poll periods: a dropped observer must not keep throwing.
	await Bun.sleep(50);
	expect(rejections).toEqual([]);
	f.port.complete(healthy.opRef, "other lane still served");
	await until(() => f.db.work.workAttemptGet(healthy.opRef)?.settledAt !== null);
	expect(rejections).toEqual([]);
});

test("saved terminal proof wins over dead liveness and output recovery consumes saved budget", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const original = f.port.fetchWorkerOutput.bind(f.port);
	f.port.fetchWorkerOutput = async () => ({ status: "absent", code: "output_pending" });
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "durable result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.output.reads === 1);
	f.port.setSessionState(result.sessionId, { live: false });
	f.port.status = async () => {
		throw new Error("offline");
	};
	f.port.fetchWorkerOutput = original;
	now += 1000;
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices[0]?.text).toBe("[lane a] completed: durable result");
});

test("historical open attempt recovery uses no current notification target and never replays", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	const { jobId, laneKey } = laneJobIdentity("old");
	const sessionId = crypto.randomUUID();
	f.db.transaction((tx) => f.db.bindings.bind(tx, workSessionKey("old"), sessionId));
	f.port.setSessionState(sessionId, { repo: f.directory, live: false });
	const job = appendAttempt(createLaneJobRecord({ jobId, branch: "work/old", worktreePath: f.directory }), {
		opRef: "old-open-attempt",
		sessionId,
		startedAt: new Date().toISOString(),
	});
	f.db.transaction((tx) => f.db.work.putLaneJob(tx, { ...job, laneKey, json: JSON.stringify(job) }));
	await f.manager.recover();
	await until(() => f.db.work.workAttemptGet("old-open-attempt")?.settledAt != null);
	expect(f.db.work.workAttemptGet("old-open-attempt")).toMatchObject({
		mode: "historical",
		parent: null,
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
	const before = f.db.work.workAttemptGet(result.opRef)!;
	generation++;
	const recovery = f.manager.onBrokerGeneration();
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await recovery;
	expect(f.db.work.workAttemptGet(result.opRef)?.output.disposition).toBe("pending");
	expect(f.notices).toHaveLength(0);
	expect(f.db.work.workAttemptGet(result.opRef)?.version).toBe(before.version);
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
	const before = f.db.work.workAttemptGet(result.opRef)!;
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, result.sessionKey));
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await Bun.sleep(20);
	expect(f.db.work.workAttemptGet(result.opRef)?.version).toBe(before.version);
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
	expect(f.db.work.workAttemptGet(op)?.output.reads).toBe(1);
	expect(f.db.work.workAttemptGet(op)?.output.disposition).toBe("pending");
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
		{ name: "a", text: "x", extra: true },
	])
		await expect(f.manager.start(params)).rejects.toMatchObject({
			code: "invalid_params",
			message: "invalid work parameters",
		});
	await expect(f.manager.run({ name: "a", text: "x", extra: true }, {})).rejects.toMatchObject({
		detail: { field: "extra" },
	});
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
	expect(
		() => new WorkLaneManager({ database: f.db, port: f.port, lanes: f.lanes, gajaewayHome: f.gajaewayHome }),
	).toThrow("already registered");
});

test("start and run nested refusal happens before recover and bind", async () => {
	const f = await fixture();
	const parent = await f.manager.start({ name: "parent", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	let recoverCalls = 0;
	f.manager.recover = async () => {
		recoverCalls++;
	};
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	await expect(
		f.manager.run({ name: "child", text: "nested run", cwd: f.directory, callerSessionId: parent.sessionId }, {}),
	).rejects.toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "run", parent: "parent" },
	});
	await expect(
		f.manager.start({ name: "child", text: "nested start", cwd: f.directory, callerSessionId: parent.sessionId }),
	).rejects.toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "start", parent: "parent" },
	});
	expect(f.db.work.laneJobJson(laneJobIdentity("child").jobId)).toBeUndefined();
	expect(recoverCalls).toBe(0);
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
});
test("nested starts refuse self and ancestor cycles before binding", async () => {
	const f = await fixture({ allowNested: () => true });
	const root = await f.manager.start({ name: "root", text: "root task", cwd: f.directory });
	if (!root.started) throw new Error("root did not start");
	const child = await f.manager.start({
		name: "child",
		text: "child task",
		cwd: f.directory,
		callerSessionId: root.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	for (const name of ["child", "root"]) {
		await expect(
			f.manager.start({ name, text: "cycle", cwd: f.directory, callerSessionId: child.sessionId }),
		).rejects.toMatchObject({ code: "invalid_params", detail: { reasonCode: "nested_lane_cycle" } });
	}
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
});

test("issue #480: cwd inside service directory is refused", async () => {
	const f = await fixture();
	const insideServiceDir = join(f.gajaewayHome, "nested");
	await expect(
		f.manager.start({ name: "a", text: "work inside service dir", cwd: insideServiceDir }),
	).rejects.toMatchObject({
		code: "invalid_params",
		message: "lane cwd cannot be inside service directory",
		detail: { reasonCode: "cwd_inside_service_dir" },
	});
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

test("issue #480: repository binding is stored in job record", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-binding-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-binding-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "a",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		if (!result.started) throw new Error("work did not start");
		const job = f.job();
		expect(job.lane.repository).toBe(bareRepo);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: cwd equals service directory is refused", async () => {
	const f = await fixture();
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.gajaewayHome })).rejects.toMatchObject({
		code: "invalid_params",
		message: "lane cwd cannot be inside service directory",
		detail: { reasonCode: "cwd_inside_service_dir" },
	});
});

test("run of the caller's own open lane is attempt_open, not a cycle", async () => {
	const f = await fixture({ allowNested: () => true });
	const parent = await f.manager.start({ name: "same", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	await expect(
		f.manager.run({ name: "same", text: "nested run", cwd: f.directory, callerSessionId: parent.sessionId }, {}),
	).rejects.toMatchObject({ code: "invalid_params", detail: { reasonCode: "attempt_open" } });
});

test("run from a lane with nesting allowed remains response-only", async () => {
	const f = await fixture({ allowNested: () => true });
	const parent = await f.manager.start({ name: "parent", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const run = f.manager.run(
		{ name: "run-child", text: "response-only task", cwd: f.directory, callerSessionId: parent.sessionId },
		{},
	);
	await until(() => f.port.sends.length === 2);
	const opRef = f.port.sends[1]!.opRef;
	f.port.complete(opRef, "response only");
	expect(await run).toMatchObject({ held: false, text: "response only", opRef });
	const runtime = f.db.work.workAttemptGet(opRef)!;
	expect(runtime).toMatchObject({ mode: "run", parent: null, decision: "no_target" });
	expect(f.db.work.laneReportsByParent("parent")).toEqual([]);
	expect(f.db.outbox.rows()).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
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
		await until(() => f.db.work.workAttemptOpen().length === 0);
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
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("terminal_missing_receipt");
	expect(f.db.work.workAttemptGet(result.opRef)?.settledAt).toBeNull();
	now += 1000;
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	expect(reads).toBe(2);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("end_turn");
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
	await until(() => f.db.work.workAttemptOpen().length === 0);
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
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("attempt_ended");
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.status?.error).toEqual({ code: "prompt_deadline_exceeded" });
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
	expect(f.db.work.workAttemptGet(result.opRef)?.sendEvidence?.source).toBe("status");
	f.port.send = async () => {
		throw new Error("lost receipt secret");
	};
	await expect(f.manager.start({ name: "b", text: "work", cwd: f.directory })).rejects.toMatchObject({
		message: "work send acceptance uncertain",
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	const pending = f.job("b").attempts[0]!;
	expect(f.db.work.workAttemptGet(pending.opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	expect(f.job("b").attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("definitive send rejection atomically holds and enqueues one safe start-only notice", async () => {
	const f = await fixture();
	f.port.send = async () => {
		throw new GjcCliError("secret", 0, "", { code: "busy" });
	};
	f.setOwnerTarget(origin);
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.directory })).rejects.toMatchObject({
		detail: { reasonCode: "send_rejected" },
	});
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("failed");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices[0]?.text).toBe("[lane a] failed: send_rejected: output_unavailable");
	expect(f.db.outbox.rows()).toHaveLength(1);
});

test("CAS loss does not fan out or perform a separate history/activity update", async () => {
	const f = await fixture();
	const settle = f.db.work.workAttemptSettle.bind(f.db.work);
	let calls = 0;
	f.db.work.workAttemptSettle = () => {
		calls++;
		return undefined;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	const history = f.db.work.laneJobJson(result.jobId);
	await until(() => calls > 0);
	// Streamed frames may refresh activity before settlement; a lost settlement
	// CAS must not write it (or history) again on any later retry.
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	const seen = calls;
	await until(() => calls > seen);
	expect(f.db.work.laneJobJson(result.jobId)).toBe(history);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(activity);
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
	f.db.work.workAttemptSettle = settle;
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.notices).toHaveLength(1);
});

test("persisted original silence survives a crash between output staging and settlement", async () => {
	const f = await fixture();
	const settle = f.db.work.workAttemptSettle.bind(f.db.work);
	f.db.work.workAttemptSettle = () => {
		throw new Error("injected pre-transaction crash");
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.output.knownSilence != null);
	await f.manager.stop();
	f.db.work.workAttemptSettle = settle;
	f.port.fetchWorkerOutput = async () => {
		throw new Error("original output no longer accessible");
	};
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
});

test("reopening SQLite recovers the same live operation and immutable notification intent", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-work-reopen-"));
	const path = join(directory, "gateway.db");
	let db = await Store.open(path);
	const gajaewayHome = join(directory, "gajaeway");
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const id = crypto.randomUUID();
			db.transaction((tx) => db.bindings.bind(tx, input.originKey, id));
			return id;
		},
	});
	const notices: ChatMessagePayload[] = [];
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port }),
		gajaewayHome,
		ownerTarget: () => origin,
		pollMs: 5,
		deliverFallback: (payload) => notices.push(payload),
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const result = await manager.start({ name: "a", text: "work", cwd: directory });
	if (!result.started) throw new Error("unexpected hold");
	await manager.stop();
	db.close();
	db = await Store.open(path);
	manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port }),
		gajaewayHome,
		pollMs: 5,
		ownerTarget: () => ({ ...origin, conversationId: "changed" }),
		deliverFallback: (payload) => notices.push(payload),
	});
	await manager.recover();
	expect(db.work.workAttemptOpen()).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
	expect(port.resumes).toHaveLength(0);
	port.complete(result.opRef, "after database reopen");
	await until(() => db.work.workAttemptOpen().length === 0);
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
				await until(() => f.db.work.workAttemptGet(opRef)?.settledAt != null);
				const runtime = f.db.work.workAttemptGet(opRef)!;
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
	const source = "HEAD:" + "😀".repeat(1000) + ":TAIL";
	f.port.complete(result.opRef, source);
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	const head = "[lane a] completed: ";
	const excerpt = utf8Prefix(source);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.excerpt).toBe(excerpt);
	expect(f.notices[0]?.text).toBe(head + utf8Prefix(source, 2048 - Buffer.byteLength(head, "utf8")));
	expect(f.notices[0]?.text.includes(":TAIL")).toBe(false);
});

test("report and fallback text stay within 2048 UTF-8 bytes including the lane head", async () => {
	const name = "n".repeat(64);
	for (const scalar of ["界", "😀"]) {
		const output = {
			disposition: "available" as const,
			reads: 0,
			nextReadAt: null,
			excerpt: scalar.repeat(1000),
			proof: null,
			knownSilence: null,
		};
		const head = `[lane ${name}] attempt_ended: recovery_indeterminate: `;
		const text = reportText(name, "attempt_ended", "recovery_indeterminate", "op-ref", output);
		expect(text).toBe(head + utf8Prefix(output.excerpt, 2048 - Buffer.byteLength(head, "utf8")));
		expect(Buffer.byteLength(text, "utf8")).toBeLessThanOrEqual(2048);
	}
	const f = await fixture();
	const result = await started(f, name, origin);
	f.port.complete(result.opRef, "界".repeat(1000));
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	const runtime = f.db.work.workAttemptGet(result.opRef)!;
	expect(f.notices[0]?.text).toBe(reportText(name, "completed", "end_turn", result.opRef, runtime.output));
	expect(Buffer.byteLength(f.notices[0]!.text, "utf8")).toBeLessThanOrEqual(2048);
});

test("AC-K notice names persona parent", () => {
	expect(
		laneSystemNotice({
			name: "worker",
			parent: { kind: "persona", originKey: originKey(origin), origin },
			allowNested: false,
		}),
	).toContain(`Parent: persona conversation ${originKey(origin)}.`);
});

test("AC-K notice names lane parent and nested allowed", () => {
	const notice = laneSystemNotice({
		name: "child",
		parent: { kind: "lane", name: "parent", root: null },
		allowNested: true,
	});
	expect(notice).toContain("Parent: work lane parent.");
	expect(notice).toContain("Nested work.start and work.run are allowed");
});

test("AC-K notice says none for no parent and names both refused verbs", () => {
	const notice = laneSystemNotice({ name: "worker", parent: null, allowNested: false });
	expect(notice).toContain("Parent: none (status-only).");
	expect(notice).toContain("Nested work.start and work.run are refused");
});

test("AC-K first accepted send per epoch carries notice; same-epoch same-parent attempt does not", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const first = await started(f);
	const firstNotice = f.port.sends[0]?.systemPreamble;
	expect(firstNotice).toContain(`persona conversation ${originKey(origin)}`);
	expect(JSON.parse(f.db.core.metaGet(`lane-notice:${workSessionKey("a")}`)!).hash).toBe(
		f.db.work.workAttemptGet(first.opRef)?.noticeHash,
	);
	f.port.complete(first.opRef, "first done");
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	const second = await started(f);
	expect(f.port.sends[1]?.systemPreamble).toBeUndefined();
	f.port.complete(second.opRef, "second done");
	await until(() => f.db.work.workAttemptGet(second.opRef)?.settledAt !== null);
});

test("AC-K parent change in the same epoch re-prepends the notice", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const first = await started(f);
	f.port.complete(first.opRef, "first done");
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	const changedOrigin = { ...origin, conversationId: "different-parent" };
	f.setOwnerTarget(changedOrigin);
	const second = await started(f);
	expect(f.port.sends[1]?.systemPreamble).toContain(`persona conversation ${originKey(changedOrigin)}`);
});

test("AC-K nested-refused notice is prepended to start sends and run sends carry no notice", async () => {
	const f = await fixture();
	const start = await started(f);
	expect(f.port.sends[0]?.systemPreamble).toContain("Nested work.start and work.run are refused");
	expect(f.port.sends[0]?.systemPreamble).toContain("Parent: none (status-only).");
	f.port.complete(start.opRef, "finished");
	await until(() => f.db.work.workAttemptGet(start.opRef)?.settledAt !== null);
	const run = f.manager.run({ name: "a", text: "run text", cwd: f.directory }, {});
	await until(() => f.port.sends.length === 2);
	expect(f.port.sends[1]?.systemPreamble).toBeUndefined();
	f.port.complete(f.port.sends[1]!.opRef, "run result");
	expect(await run).toMatchObject({ held: false, text: "run result" });
});

test("AC-K uncertain send acceptance re-prepends on the next send", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const send = f.port.send.bind(f.port);
	f.port.send = async () => {
		throw new Error("lost send receipt");
	};
	await expect(f.manager.start({ name: "a", text: "first", cwd: f.directory })).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	const first = f.job().attempts[0]!;
	f.port.setSessionState(first.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	f.port.send = send;
	const retry = await f.manager.start({ name: "a", text: "retry", cwd: f.directory, resume: true });
	if (!retry.started) throw new Error("retry was held");
	expect(f.port.sendAttempts.at(-1)?.systemPreamble).toContain(`persona conversation ${originKey(origin)}`);
});

test("personaHold broker_wedged selects fallback inside the settlement transaction", async () => {
	const f = await fixture({ ownerTarget: () => origin, personaHold: () => "broker_wedged" });
	const result = await started(f);
	f.port.complete(result.opRef, "held persona result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	const runtime = f.db.work.workAttemptGet(result.opRef)!;
	expect(runtime.decision).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.db.outbox.rows()[0]?.delivery_id).toBe(runtime.deliveryId);
	expect(f.db.inbox.pendingOldest(originKey(origin))).toBeUndefined();
	expect(f.notices).toHaveLength(1);
});

test("personaHold emitted hold reason selects fallback", async () => {
	const f = await fixture({ ownerTarget: () => origin, personaHold: () => "sdk_unavailable" });
	const result = await started(f);
	f.port.complete(result.opRef, "held persona result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.notices[0]?.text).toBe("[lane a] completed: held persona result");
});
test("lane inbox steers an open nested parent and consumes only after acceptance", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	expect(f.port.sends[1]?.systemPreamble).toContain("Parent: work lane parent.");
	expect(f.port.sends[1]?.systemPreamble).toContain("Nested work.start and work.run are allowed");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	expect(report).toMatchObject({ state: "consumed", claim_kind: "steer", consumed_op_ref: report.claim_ref });
	expect(report.body).toBe("[lane child] completed: child result");
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.steers[0]?.clientRef).toBe(report.claim_ref!);
	expect(f.port.steers[0]?.text).toContain("Report from child work lane child; not from a human.");
	expect(f.port.sendAttempts).toHaveLength(2);
	expect(f.notices).toHaveLength(0);
});

test("torn nested steer replays with the same clientRef after restart and consumes once", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const steer = f.port.steer.bind(f.port);
	let torn = true;
	const refs: string[] = [];
	f.port.steer = async (input) => {
		refs.push(input.clientRef);
		if (torn) throw new Error("steer transport torn");
		return steer(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => refs.length === 1);
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	expect(claimed).toMatchObject({ state: "claimed", claim_kind: "steer" });
	torn = false;
	await f.restart();
	await until(() => f.db.work.laneReportGet(claimed.report_id)?.state === "consumed");
	const claimRef = claimed.claim_ref;
	if (!claimRef) throw new Error("claimed lane report has no claim_ref");
	expect(new Set(refs)).toEqual(new Set([claimRef]));
	expect(f.db.work.laneReportGet(claimed.report_id)?.consumed_op_ref).toBe(claimRef);
	expect(f.port.sendAttempts.filter((attempt) => attempt.opRef.includes("-lr-"))).toHaveLength(0);
});

test("AC-K wake send in a new epoch carries its lane notice", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	let rebound = false;
	const claim = f.db.work.laneReportClaim.bind(f.db.work);
	f.db.work.laneReportClaim = (tx, reportId, kind, ref, targetOpRef) => {
		const row = claim(tx, reportId, kind, ref, targetOpRef);
		if (row?.claim_kind === "wake" && !rebound) {
			rebound = true;
			f.db.bindings.rotateEpoch(tx, workSessionKey("parent"));
		}
		return row;
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child report");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	expect(rebound).toBe(true);
	expect(f.port.sendAttempts[2]?.systemPreamble).toContain("You are work lane parent.");
	expect(f.port.sendAttempts[2]?.systemPreamble).toContain(
		"Parent: persona conversation discord/channel/work-results.",
	);
});

test("idle lane parent wake completes without lock re-entry", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed"),
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error("lane parent wake timed out")), 2000);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	const wake = f.db.work.workAttemptGet(report.consumed_op_ref!)!;
	expect(wake).toMatchObject({ wakeReportId: report.report_id, parent: { kind: "persona" } });
	expect(report).toMatchObject({ state: "consumed", claim_kind: "wake", consumed_op_ref: report.claim_ref });
	expect(f.port.sendAttempts).toHaveLength(3);
	expect(f.port.sendAttempts[2]?.opRef).toBe(report.claim_ref!);
	expect(f.port.sendAttempts[2]?.text).toContain("Report from child work lane child; not from a human.");
});

test("rejected nested wake settles wake_unaccepted and falls back the child exactly once", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const rootInboundBefore = f.db.inbox.pendingCount(originKey(origin));
	let wakeSendAttempts = 0;
	f.notices.length = 0;
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) {
			wakeSendAttempts++;
			throw new GjcCliError("refused", 0, "", { code: "busy" });
		}
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "fallback");
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	const wake = f.db.work.workAttemptGet(report.claim_ref!)!;
	expect(wake.decision).toBe("wake_unaccepted");
	expect(report.state).toBe("fallback");
	expect(f.db.work.workAttemptGet(wake.opRef)?.reportId).toBe(wake.reportId);
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.db.outbox.rows()[0]?.turn_id).toBe(child.opRef);
	expect(f.db.outbox.rows()[0]?.delivery_id).not.toBe(wake.deliveryId);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]?.deliveryId).toBe(f.db.outbox.rows()[0]?.delivery_id);
	expect(f.db.inbox.pendingCount(originKey(origin))).toBe(rootInboundBefore);
	await f.manager.recover();
	expect(f.db.work.laneReportsByParent("parent")[0]?.state).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(wakeSendAttempts).toBe(1);
});

test("uncertain nested wake keeps its claim and consumes after status acceptance without resend", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	let wakeSendAttempts = 0;
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) {
			wakeSendAttempts++;
			throw new Error("wake send receipt lost");
		}
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "claimed");
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	const wakeOpRef = claimed.claim_ref!;
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "accepted", clientRef: input.opRef },
		summaryCompleted: false,
	});
	await f.restart();
	await until(() => f.db.work.laneReportGet(claimed.report_id)?.state === "consumed");
	expect(f.db.work.workAttemptGet(wakeOpRef)?.sendPhase).toBe("accepted");
	expect(f.port.sendAttempts.filter((attempt) => attempt.opRef.includes("-lr-")).length).toBe(0);
	expect(wakeSendAttempts).toBe(1);
});
test("recovered terminal status with receipt present proves wake acceptance without output proof", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) throw new Error("wake send receipt lost");
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "claimed");
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	const wakeOpRef = claimed.claim_ref!;
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "identity_mismatch" });
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: {
			status: "terminal_ok",
			clientRef: input.opRef,
			receiptState: "present",
			outcome: { reason: "end_turn" },
		},
		summaryCompleted: true,
	});
	await f.restart();
	await until(() => f.db.work.workAttemptGet(wakeOpRef)?.settledAt != null);
	const wake = f.db.work.workAttemptGet(wakeOpRef)!;
	expect(wake.sendPhase).toBe("accepted");
	expect(wake.decision).not.toBe("wake_unaccepted");
	expect(f.db.work.laneReportGet(claimed.report_id)?.state).toBe("consumed");
});
test("open-parent steer refusal requeues and the idle parent receives one wake", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	let steerAttempts = 0;
	const steer = f.port.steer.bind(f.port);
	f.port.steer = async (input) => {
		steerAttempts++;
		if (steerAttempts === 1) throw new GjcCliError("busy", 0, "", { code: "busy", refused: true });
		return steer(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "pending");
	expect(steerAttempts).toBe(1);
	f.port.complete(parent.opRef, "parent completed");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	expect(f.db.work.laneReportsByParent("parent")[0]).toMatchObject({ claim_kind: "wake" });
	expect(steerAttempts).toBe(1);
	expect(f.port.sendAttempts).toHaveLength(3);
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
		await until(() => f.db.work.workAttemptGet(a.opRef)?.settledAt != null);
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
		expect(f.db.work.workAttemptGet(b.opRef)?.settledAt).toBeNull();
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
	const history = f.db.work.laneJobJson(old.jobId);
	const runtime = f.db.work.workAttemptGet(old.opRef);
	const authority = { canonicalAgentDir: "/tmp/global-agent", identity: "shared-broker" };
	f.db.transaction((tx) =>
		f.db.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: null,
			targetAuthority: authority,
			evidence: "test operator quarantined private broker work",
			disposition: "quarantine",
		}),
	);
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
	expect(f.db.work.laneJobJson(old.jobId)).toBe(history);
	expect(f.db.work.workAttemptGet(old.opRef)).toEqual(runtime);
	expect(f.db.work.laneJobRows()).toEqual([]);
	expect(f.db.work.laneJobRows(true).map((row) => row.job_id)).toEqual([old.jobId]);
	// Explicit test-only provenance models a successful shared SDK bind.
	f.port.bind = async (input) => {
		const binding = { ...input, sessionId: crypto.randomUUID() };
		f.db.transaction((tx) => f.db.bindings.recordOwnedBinding(tx, { ...binding, authority }));
		return binding;
	};
	const fresh = await started(f, "fresh");
	expect(fresh.jobId).not.toBe(old.jobId);
	expect(f.port.sends).toHaveLength(sends + 1);
	expect(f.notices).toHaveLength(0);
});

function sessionUnavailable(): GjcCliError {
	return new GjcCliError("gjc sdk request failed: session_unavailable", 1, "", { code: "session_unavailable" });
}

test("#308 host loss settles the open attempt as host_lost; resume and retire then work", async () => {
	const f = await fixture({ hostLostGraceMs: 20 });
	const result = await started(f, "a", origin);
	const status = f.port.status.bind(f.port);
	// The host process died: the router disowns the session for every read.
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
	expect(f.job().attempts[0]?.endState).toBe("attempt_ended");
	expect(f.job().attempts[0]?.errorCode).toBe("host_lost");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: host_lost: output_unavailable");
	expect(await f.manager.status({ name: "a" })).toMatchObject({
		attempt: { opRef: result.opRef, endState: "attempt_ended" },
		op: null,
	});
	expect(f.port.sends).toHaveLength(1);
	// Retire proves the host gone from the same router verdict.
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: true, sessionId: result.sessionId });
	f.port.status = status;
	const resumed = await f.manager.start({ name: "a", text: "continue", cwd: f.directory, resume: true });
	expect(resumed).toMatchObject({ started: true });
	expect(f.port.sends).toHaveLength(2);
	expect(f.job().attempts).toHaveLength(2);
});

test("#308 open attempt resumes after host loss without retiring first", async () => {
	const f = await fixture({ hostLostGraceMs: 0 });
	const result = await started(f);
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const resumed = await f.manager.start({ name: "a", text: "continue", cwd: f.directory, resume: true });
	expect(resumed).toMatchObject({ started: true });
});

for (const [label, failure, live] of [
	["broker_unavailable", () => new GjcCliUnavailableError("connect ECONNREFUSED"), false],
	["an unstructured transport failure", () => new Error("gjc sdk request failed: timeout"), false],
	["session_unavailable while inspect still reports the host live", sessionUnavailable, true],
] as const) {
	test(`#308 ${label} never settles an open attempt`, async () => {
		const f = await fixture({ hostLostGraceMs: 0 });
		const result = await started(f);
		let reads = 0;
		f.port.status = async () => {
			reads++;
			throw failure();
		};
		f.port.setSessionState(result.sessionId, { live });
		await until(() => reads >= 5);
		expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
		expect(f.job().attempts[0]?.endedAt).toBeUndefined();
		expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: false });
	});
}

test("#308 host loss must persist across the grace window before settling", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now, hostLostGraceMs: 60_000 });
	const result = await started(f);
	let reads = 0;
	f.port.status = async () => {
		reads++;
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => reads >= 3);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
	now += 60_000;
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
});

test("#308 restart recovery classifies a router-disowned open attempt as host_lost", async () => {
	const f = await fixture();
	const result = await started(f);
	await f.manager.stop();
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
	expect(f.job().state).toBe("attempt_ended");
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: true });
});

test("#308 streamed turn frames advance lane activity past the attempt start", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const result = await started(f);
	const startedAt = f.db.bindings.workLaneRows()[0]!.last_activity_at!;
	expect(startedAt).toBe(f.db.work.workAttemptGet(result.opRef)!.startedAt);
	now += 5_000;
	f.port.emitTool(result.sessionId, { toolName: "bash" });
	await until(() => f.db.bindings.workLaneRows()[0]!.last_activity_at !== startedAt);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(new Date(now).toISOString());
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("transport failure ECONNRESET without errorStatus yields transport cause in lane notice", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", receiptState: "missing" },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	// Mock the transport failure
	f.port.setFailedTransportCause(result.sessionId, {
		kind: "transport",
		nativeErrorCode: "ECONNRESET",
		requestBytes: 1238792,
		endpointClass: "custom",
		retryMaxAttempts: 1,
	});
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const attempt = f.db.work.workAttemptGet(result.opRef);
	expect(attempt?.terminal?.reasonCode).toBe("terminal_missing_receipt");
	expect(attempt?.output?.transportCause).toEqual({
		kind: "transport",
		nativeErrorCode: "ECONNRESET",
		requestBytes: 1238792,
		endpointClass: "custom",
		retryMaxAttempts: 1,
	});
	const notice = f.notices[0]?.text ?? "";
	expect(notice).toContain("final_response_missing");
	expect(notice).toContain("cause=transport");
	expect(notice).toContain("ECONNRESET");
	expect(notice).toContain("requestBytes=1238792");
	expect(notice).toContain("retryMaxAttempts=1");
	expect(notice).toContain("endpointClass=custom");
});

test("transport failure never leaks errorMessage secrets in lane notice", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", receiptState: "missing" },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const attempt = f.db.work.workAttemptGet(result.opRef);
	const notice = f.notices[0]?.text ?? "";
	// Private error messages must never appear in lane notice
	expect(notice).not.toContain("secret");
	expect(notice).not.toContain("private");
	expect(notice).not.toContain("authentication");
});

async function laneRepo(directory: string, branch: string, push: boolean) {
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "t",
		GIT_AUTHOR_EMAIL: "t@t",
		GIT_COMMITTER_NAME: "t",
		GIT_COMMITTER_EMAIL: "t@t",
	};
	const git = (cwd: string, ...args: string[]) => {
		const run = Bun.spawnSync(["git", "-C", cwd, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (run.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${run.stderr.toString()}`);
		return run.stdout.toString().trim();
	};
	const remote = `${directory}-origin.git`;
	cleanups.push(() => rm(remote, { recursive: true, force: true }));
	git(directory, "init", "-q", "--bare", remote);
	git(directory, "init", "-q", "-b", branch);
	git(directory, "remote", "add", "origin", remote);
	git(directory, "commit", "-q", "--allow-empty", "-m", "lane head");
	if (push) git(directory, "push", "-q", "origin", branch);
	return git(directory, "rev-parse", "HEAD");
}

function recordingCheckClient(answer: CheckStatusSnapshot | null) {
	const calls: Array<{ branch: string; cwd: string }> = [];
	const client: CheckStatusClient = {
		async getCheckStatus(branch, cwd) {
			calls.push({ branch, cwd });
			return answer;
		},
	};
	return { calls, client };
}

async function settledReport(f: Awaited<ReturnType<typeof fixture>>) {
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => f.notices.some((notice) => notice.text.includes("[lane a] completed")));
	return f.notices.find((notice) => notice.text.includes("[lane a] completed"))?.text ?? "";
}

test("#481 settled report of an unpushed lane carries its head and pushed=false without a PR lookup", async () => {
	const { calls, client } = recordingCheckClient(null);
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/unpushed", false);
	const text = await settledReport(f);
	expect(text).toContain(`head=${head.slice(0, 12)}`);
	expect(text).toContain("pushed=false");
	expect(text).not.toContain("pr=");
	expect(calls).toHaveLength(0);
});

test("#481 settled report of a pushed lane without an open PR says pushed=true and no checks", async () => {
	const { calls, client } = recordingCheckClient(null);
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/no-pr", true);
	const text = await settledReport(f);
	expect(text).toContain(`head=${head.slice(0, 12)}`);
	expect(text).toContain("pushed=true");
	expect(text).not.toContain("checks=");
	expect(calls).toEqual([{ branch: "fix/no-pr", cwd: f.directory }]);
});

test("#481 settled report of a pushed lane with a red PR names the PR and failing checks", async () => {
	const { client } = recordingCheckClient({ prNumber: 484, status: "red", failingChecks: ["lint", "test"] });
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	await laneRepo(f.directory, "fix/red", true);
	const text = await settledReport(f);
	expect(text).toContain("pushed=true");
	expect(text).toContain("pr=484");
	expect(text).toContain("checks=red(lint,test)");
});

test("#481 check rollup: any failure is red, unfinished is pending, all success is green", () => {
	const run = (name: string, status: string, conclusion = "") => ({ __typename: "CheckRun", name, status, conclusion });
	expect(
		classifyCheckRollup(1, [
			run("lint", "COMPLETED", "FAILURE"),
			run("lint", "COMPLETED", "FAILURE"),
			run("test", "IN_PROGRESS"),
		]),
	).toEqual({ prNumber: 1, status: "red", failingChecks: ["lint"] });
	expect(classifyCheckRollup(2, [run("lint", "COMPLETED", "SUCCESS"), run("test", "QUEUED")]).status).toBe("pending");
	expect(classifyCheckRollup(3, []).status).toBe("pending");
	expect(
		classifyCheckRollup(4, [
			run("lint", "COMPLETED", "SUCCESS"),
			{ __typename: "StatusContext", context: "ci/x", state: "SUCCESS" },
		]).status,
	).toBe("green");
	expect(
		classifyCheckRollup(5, [{ __typename: "StatusContext", context: "ci/x", state: "ERROR" }]).failingChecks,
	).toEqual(["ci/x"]);
});

test("#407: laneLastCommits reads each distinct worktree once, concurrently, null for non-repos", async () => {
	const repo = await mkdtemp(join(tmpdir(), "lane-commit-"));
	const missing = join(repo, "does-not-exist");
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repo, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "t",
					GIT_AUTHOR_EMAIL: "t@t",
					GIT_COMMITTER_NAME: "t",
					GIT_COMMITTER_EMAIL: "t@t",
				},
			});
		git("init", "-q");
		git("commit", "-q", "--allow-empty", "-m", "lane head");
		const spawn = spyOn(Bun, "spawn");
		const commits = await laneLastCommits([repo, missing, repo, repo], 2);
		expect(spawn).toHaveBeenCalledTimes(2);
		spawn.mockRestore();
		expect(commits.get(repo)?.subject).toBe("lane head");
		expect(commits.get(repo)?.sha).toMatch(/^[0-9a-f]{40}$/);
		expect(commits.get(missing)).toBeNull();
	} finally {
		await rm(repo, { recursive: true, force: true });
	}
});

test("work.start with repo creates a worktree in <laneRoot>/lanes/<name> from a bare git repo", async () => {
	// Create a bare git repo to test against
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		// Initialize a regular repo
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");

		// Create a bare clone
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const laneRoot = f.gajaewayHome;
		const result = await f.manager.start({
			name: "worktree-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});

		expect(result.started).toBe(true);
		const job = f.job("worktree-test");
		expect(job.lane.worktreePath).toContain("lanes/worktree-test");
		expect(job.lane.worktreePath.startsWith(join(laneRoot, "lanes"))).toBe(true);
		expect(job.lane.repository).toBe(bareRepo);

		// Verify worktree was created with proper branch
		const branchResult = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
		});
		expect(branchResult.exitCode).toBe(0);
		const branch = branchResult.stdout?.toString().trim();
		expect(branch).toBe("work/worktree-test");
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("work.start with invalid repo throws invalid_params with git stderr", async () => {
	const f = await fixture();
	const invalidRepo = "/nonexistent/repo";

	await expect(
		f.manager.start({
			name: "bad-repo",
			text: "work",
			cwd: f.directory,
			repo: invalidRepo,
		}),
	).rejects.toMatchObject({
		code: "invalid_params",
		detail: {
			reasonCode: "repo_not_git",
		},
	});

	// Verify no worktree was created
	const job = f.db.work.laneJobJson(laneJobIdentity("bad-repo").jobId);
	expect(job).toBeUndefined();
});

test("work.start with repo and base parameter creates worktree from specified base", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-base-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-base-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		// Initialize a regular repo with branches
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");
		git("checkout", "-q", "-b", "dev");
		git("commit", "-q", "--allow-empty", "-m", "dev commit");

		// Create a bare clone
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdio: ["ignore", "ignore", "ignore"],
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "base-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
			base: "dev",
		});

		expect(result.started).toBe(true);
		const job = f.job("base-test");
		expect(job.lane.repository).toBe(bareRepo);

		// Verify worktree was created from dev branch
		const logResult = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "log", "-1", "--format=%s"], {
			stdout: "pipe",
		});
		expect(logResult.stdout?.toString().trim()).toBe("dev commit");
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});
