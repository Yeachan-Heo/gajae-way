import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAttempt, closeAttempt, createLaneJobRecord, newOpRef } from "@gajaeway/subsession";
import { LaneGovernor, laneJobIdentity } from "../src/orchestrator/lane-governor";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

let server: GatewayServer | undefined;
let directory = "";
let closeClient: (() => void) | undefined;
afterEach(async () => {
	await server?.stop();
	closeClient?.();
	closeClient = undefined;
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
});

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function harness(
	maxLanes = 3,
	hooks: {
		beforeBind?: () => Promise<void>;
		onBind?: NonNullable<ConstructorParameters<typeof ScriptedSessionPort>[0]>["onBind"];
		onSend?: NonNullable<ConstructorParameters<typeof ScriptedSessionPort>[0]>["onSend"];
	} = {},
) {
	directory = await mkdtemp(join(tmpdir(), "lane-redteam-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const port = new ScriptedSessionPort({
		onBind:
			hooks.onBind ??
			(async (input) => {
				await hooks.beforeBind?.();
				const id = database.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
				database.putSession(input.originKey, id);
				return id;
			}),
		onSend: hooks.onSend ?? ((input, scripted) => scripted.complete(input.opRef, "done")),
	});
	const socketPath = join(directory, "gateway.sock");
	server = await startUnixServer({
		config: {
			schemaVersion: 1,
			home: directory,
			configPath: join(directory, "config.json"),
			socketPath,
			dbPath: join(directory, "gateway.db"),
			logVerbosity: "info",
			work: { maxLanes },
		},
		database,
		sessionPort: port,
		onStop: () => database.close(),
	});
	const frames: any[] = [];
	let buffer = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffer += Buffer.from(data).toString("utf8");
				let end = buffer.indexOf("\n");
				while (end >= 0) {
					frames.push(JSON.parse(buffer.slice(0, end)));
					buffer = buffer.slice(end + 1);
					end = buffer.indexOf("\n");
				}
			},
		},
	});
	closeClient = () => socket.end();
	async function wait(predicate: (frame: any) => boolean) {
		for (let i = 0; i < 400 && !frames.some(predicate); i++) await Bun.sleep(5);
		const frame = frames.find(predicate);
		expect(frame).toBeDefined();
		return frame;
	}
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await wait((frame) => frame.type === "negotiated");
	let sequence = 0;
	function request(verb: string, params?: unknown) {
		const id = `request-${sequence++}`;
		socket.write(`${JSON.stringify({ v: "0.1", type: "request", id, verb, params })}\n`);
		return wait((frame) => frame.id === id);
	}
	return {
		database,
		port,
		request,
		run: (name: string, extra = {}) => request("work.run", { name, text: "work", cwd: directory, ...extra }),
	};
}

test("R1 concurrent new names cannot overbook the final slot", async () => {
	const binding = deferred();
	const release = deferred();
	let delay = false;
	const h = await harness(2, {
		beforeBind: async () => {
			if (delay) {
				binding.resolve();
				await release.promise;
			}
		},
	});
	await h.run("existing");
	delay = true;
	const first = h.run("first");
	await binding.promise;
	const second = h.run("second");
	// Keep the first bind unpublished while the other name reaches admission.
	await Bun.sleep(30);
	release.resolve();
	const results = await Promise.all([first, second]);
	expect(h.database.workLaneRows().length).toBeLessThanOrEqual(2);
	expect(results.filter((frame) => frame.error?.code === "lane_capacity")).toHaveLength(1);
});

test("R2 retire refuses an open live turn without waiting for it to finish", async () => {
	const started = deferred();
	const release = deferred();
	const h = await harness(2, {
		onSend: async (input, port) => {
			started.resolve();
			await release.promise;
			port.complete(input.opRef, "finished");
		},
	});
	const running = h.run("busy");
	await started.promise;
	const retiring = h.request("work.retire", { name: "busy" });
	let outcome: any;
	try {
		outcome = await Promise.race([retiring, Bun.sleep(100).then(() => ({ timedOut: true }))]);
	} finally {
		release.resolve();
	}
	const completed = await running;
	await retiring;
	expect(outcome).toMatchObject({ result: { retired: false, reason: expect.stringContaining("attempt still open") } });
	expect(completed.result.text).toBe("finished");
	expect(h.port.closes).toEqual([]);
});

test("R3 retirement advances epoch and rerun never references the old session", async () => {
	const h = await harness();
	await h.run("recycled");
	const before = h.database.getSessionRecord("work/task/recycled")!;
	expect(h.database.referencedSessionIds()).toContain(before.sessionId);
	expect((await h.request("work.retire", { name: "recycled" })).result).toMatchObject({
		retired: true,
		sessionId: before.sessionId,
		closed: true,
	});
	expect(h.database.referencedSessionIds()).not.toContain(before.sessionId);
	await h.run("recycled");
	const after = h.database.getSessionRecord("work/task/recycled")!;
	expect(after.epoch).toBe(before.epoch + 1);
	expect(after.sessionId).not.toBe(before.sessionId);
	expect(h.database.referencedSessionIds()).not.toContain(before.sessionId);
});

test("R4 invalid models and name boundaries fail closed; valid model reaches bind and request", async () => {
	const h = await harness();
	for (const model of ["", { preset: "" }, { preset: "x", extra: 1 }, [], null]) {
		expect((await h.run("invalid", { model })).error?.code).toBe("invalid_params");
	}
	expect(h.port.binds).toHaveLength(0);
	for (const model of ["model-id", { preset: "preset-name" }]) {
		expect((await h.run("a".repeat(64), { model })).type).toBe("response");
		expect(h.port.binds.at(-1)?.model).toEqual(model);
		expect(h.port.sendAttempts.at(-1)?.model).toEqual(model);
	}
	expect((await h.run("a".repeat(65))).error?.code).toBe("invalid_params");
	for (const name of ["../bad", "", "a".repeat(65), null])
		expect((await h.request("work.retire", { name })).error?.code).toBe("invalid_params");
});

test("R5 capacity details include every candidate sorted idlest first", async () => {
	const h = await harness(3);
	for (const name of ["old", "middle", "fresh"]) {
		await h.run(name);
		await Bun.sleep(15);
	}
	const result = await h.run("fourth");
	expect(result.error?.code).toBe("lane_capacity");
	const detail = result.error.detail;
	expect(detail).toMatchObject({ active: 3, maxLanes: 3 });
	expect(detail.candidates.map((candidate: any) => candidate.name)).toEqual(["old", "middle", "fresh"]);
	for (let i = 0; i < 2; i++) expect(detail.candidates[i].idleMs).toBeGreaterThan(detail.candidates[i + 1].idleMs);
	expect(detail.candidates.every((candidate: any) => typeof candidate.state === "string")).toBe(true);
	expect((await h.run("old")).type).toBe("response");
});

test("R6 sweep is idempotent and protects an idle lane with an open attempt", async () => {
	const started = deferred();
	const release = deferred();
	const h = await harness(3, {
		onSend: async (input, port) => {
			if (input.text === "wait") {
				started.resolve();
				await release.promise;
			}
			port.complete(input.opRef, "done");
		},
	});
	await h.run("idle");
	const running = h.run("busy", { text: "wait" });
	await started.promise;
	try {
		const governor = new LaneGovernor({
			database: h.database,
			sessionPort: h.port,
			idleRetireMs: 60_000,
			now: () => Date.now() + 120_000,
		});
		expect(await governor.sweep()).toBe(1);
		expect(await governor.sweep()).toBe(0);
		expect(h.database.workLaneRows().map((row) => row.origin_key)).toEqual(["work/task/busy"]);
	} finally {
		release.resolve();
		await running;
	}
});

test("R7 socket ops.cycle saturation gate clears after retirement without stale identity", async () => {
	const h = await harness(2);
	await h.run("a");
	await h.run("b");
	const full = (await h.request("ops.cycle")).result;
	expect(full.lanes).toEqual({ active: 2, max: 2 });
	expect(full.gates).toContain("lane_capacity_exhausted");
	await h.request("work.retire", { name: "a" });
	const freed = (await h.request("ops.cycle")).result;
	expect(freed.lanes).toEqual({ active: 1, max: 2 });
	expect(freed.gates).not.toContain("lane_capacity_exhausted");
	expect(freed.gates).not.toContain("stale_session_identity");
});

test("R7b a failed first bind leaves an unbound worker row with no job and stays a stale-identity gate", async () => {
	const h = await harness(2, {
		onBind: async (input) => {
			// Mirror BrokerSessionPort's create failure: the epoch is rotated for a
			// retry key before the failure propagates, so a row exists unbound.
			h.database.rebindEpoch(input.originKey);
			throw new Error("session.create exploded");
		},
	});
	const failed = await h.run("broken");
	expect(failed.type).toBe("error");
	expect(h.database.getSessionRecord("work/task/broken")).toMatchObject({ sessionId: "" });
	expect(h.database.laneJobRows()).toEqual([]);
	const cycle = (await h.request("ops.cycle")).result;
	expect(cycle.gates).toContain("stale_session_identity");
	expect(cycle.phase).toBe("degraded");
	expect(cycle.lanes.active).toBe(0);
});

test("R8 retirement source never calls session.delete or deleteSession", async () => {
	const governor = await Bun.file(new URL("../src/orchestrator/lane-governor.ts", import.meta.url)).text();
	const serverSource = await Bun.file(new URL("../src/server/server.ts", import.meta.url)).text();
	const handler = serverSource.split('case "work.retire":')[1]?.split('case "ops.cycle":')[0];
	expect(handler).toBeDefined();
	expect(governor).not.toMatch(/session\.delete|deleteSession/);
	expect(handler).not.toMatch(/session\.delete|deleteSession/);
});

function seedJob(
	database: GatewayDatabase,
	name: string,
	state: "awaiting_operator" | "done" | "attempt_ended" | "failed",
	sessionId = crypto.randomUUID(),
) {
	const identity = laneJobIdentity(name);
	let record = createLaneJobRecord({ jobId: identity.jobId, branch: `work/${name}`, worktreePath: process.cwd() });
	const opRef = newOpRef("gen2-redteam");
	if (state === "attempt_ended" || state === "failed") {
		record = appendAttempt(record, { opRef, sessionId, startedAt: new Date(Date.now() - 120_000).toISOString() });
		record = closeAttempt({
			record,
			opRef,
			endState: state,
			errorCode: state === "failed" ? "transport_error" : "gateway_turn_reaped",
			endedAt: new Date().toISOString(),
		});
	}
	record = { ...record, state: state === "failed" ? "awaiting_operator" : state };
	database.putLaneJob({ ...record, laneKey: identity.laneKey, json: JSON.stringify(record) });
	return { record, opRef, identity };
}

// work.run does not forward waitTimeoutMs, and the fake request has no timeout.
// Exercise the durable timeout boundary directly rather than waiting for a production timeout.
for (const unavailable of [false, true]) {
	test(`${unavailable ? "G2" : "G1"} ended gateway wait does not prove broker terminality (${unavailable ? "status unavailable" : "in_flight"})`, async () => {
		const database = await GatewayDatabase.open(":memory:");
		try {
			const sessionId = crypto.randomUUID();
			database.putSession("work/task/a", sessionId);
			const { opRef } = seedJob(database, "a", "attempt_ended", sessionId);
			class UnsettledPort extends ScriptedSessionPort {
				override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
					if (unavailable) throw new Error("broker transport unavailable");
					return { operationRef: input.opRef, status: { status: "in_flight" as const }, summaryCompleted: false };
				}
			}
			const port = new UnsettledPort({ onSend: () => new Promise<void>(() => {}) });
			port.seedOperation(opRef, sessionId);
			port.setSessionState(sessionId, { live: true });
			const before = database.getSessionRecord("work/task/a");
			const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 1 });
			expect(await governor.retire("a", "operator")).toMatchObject({
				retired: false,
				reason: expect.stringContaining(unavailable ? "status unavailable" : "broker reports in_flight"),
			});
			expect(await governor.sweep()).toBe(0);
			expect(port.closes).toEqual([]);
			expect(database.getSessionRecord("work/task/a")).toEqual(before);
		} finally {
			database.close();
		}
	});
}

test("G3 ambiguous close retains identity until broker explicitly disowns session", async () => {
	const database = await GatewayDatabase.open(":memory:");
	try {
		database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a001");
		class AmbiguousPort extends ScriptedSessionPort {
			disowned = false;
			override async close(input: { sessionId: string; repo: string }) {
				this.closes.push(input);
				throw new Error("close transport lost");
			}
			override async liveness() {
				return { live: undefined, disowned: this.disowned };
			}
		}
		const port = new AmbiguousPort();
		const governor = new LaneGovernor({ database, sessionPort: port });
		const before = database.getSessionRecord("work/task/a")!;
		expect(await governor.retire("a", "operator")).toMatchObject({
			retired: false,
			reason: expect.stringContaining("not proven gone"),
		});
		expect(database.getSessionRecord("work/task/a")).toEqual(before);
		expect(governor.activeLanes()).toHaveLength(1);
		port.disowned = true;
		expect(await governor.retire("a", "operator")).toMatchObject({ retired: true, closed: false });
		expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before.epoch + 1 });
		expect(port.closes).toHaveLength(2);
	} finally {
		database.close();
	}
});

test("G4 sweep snapshot cannot retire a replacement bound before lock acquisition", async () => {
	const database = await GatewayDatabase.open(":memory:");
	const release = deferred();
	const port = new ScriptedSessionPort();
	let rebinding: Promise<void> | undefined;
	try {
		database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a001");
		const before = database.getSessionRecord("work/task/a")!;
		rebinding = port.runExclusive("work/task/a", async () => {
			await release.promise;
			database.rebindEpoch("work/task/a");
			database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a002");
		});
		const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 1 });
		const sweeping = governor.sweep();
		release.resolve();
		await rebinding;
		expect(await sweeping).toBe(0);
		expect(database.getSessionRecord("work/task/a")).toEqual({
			sessionId: "0000aaaa-0000-4000-8000-00000000a002",
			epoch: before.epoch + 1,
		});
		expect(port.closes).toEqual([]);
	} finally {
		release.resolve();
		await rebinding;
		database.close();
	}
});

test("G5 admission lock is released while first distinct lane turn is still running", async () => {
	const started = deferred();
	const release = deferred();
	const order: string[] = [];
	const h = await harness(2, {
		onSend: async (input, port) => {
			if (input.text === "slow") {
				started.resolve();
				await release.promise;
			}
			order.push(input.text);
			port.complete(input.opRef, input.text);
		},
	});
	const first = h.run("a", { text: "slow" });
	await started.promise;
	try {
		const second = await Promise.race([h.run("b", { text: "fast" }), Bun.sleep(500).then(() => ({ timedOut: true }))]);
		expect(second).toMatchObject({ result: { text: "fast" } });
		expect(order).toEqual(["fast"]);
		expect(h.database.workLaneRows()).toHaveLength(2);
	} finally {
		release.resolve();
		await first;
	}
	expect(order).toEqual(["fast", "slow"]);
});

test("G6 idle retirement interval starts at turn end, including the exact boundary", async () => {
	const idleRetireMs = 30;
	let startedAt = 0;
	let completedAt = 0;
	const h = await harness(2, {
		onSend: async (input, port) => {
			startedAt = Date.now();
			await Bun.sleep(idleRetireMs * 3);
			completedAt = Date.now();
			port.complete(input.opRef, "done");
		},
	});
	expect((await h.run("a")).result.text).toBe("done");
	const governor = new LaneGovernor({ database: h.database, sessionPort: h.port, idleRetireMs });
	const activityAt = Date.parse(governor.activeLanes()[0]!.lastActivityAt!);
	expect(completedAt - startedAt).toBeGreaterThanOrEqual(idleRetireMs);
	expect(activityAt).toBeGreaterThanOrEqual(completedAt);
	expect(await governor.sweep(activityAt + idleRetireMs - 1)).toBe(0);
	expect(h.port.closes).toEqual([]);
	expect(await governor.sweep(activityAt + idleRetireMs)).toBe(1);
	expect(h.port.closes).toHaveLength(1);
});

test("G7 corrupt bound job consumes socket capacity and identifies corrupt candidate", async () => {
	const h = await harness(1);
	h.database.putSession("work/task/a", crypto.randomUUID());
	const { record, identity } = seedJob(h.database, "a", "awaiting_operator");
	h.database.putLaneJob({ ...record, laneKey: identity.laneKey, json: "{invalid json" });
	const result = await h.run("b");
	expect(result.error).toMatchObject({
		code: "lane_capacity",
		detail: { active: 1, maxLanes: 1, candidates: [{ name: "a", state: "corrupt" }] },
	});
	expect(h.port.binds).toHaveLength(0);
	expect(h.port.closes).toEqual([]);
});

test("G8 socket cycle gates unbound unsettled work but not done work", async () => {
	const h = await harness();
	h.database.putSession("work/task/x", "");
	seedJob(h.database, "x", "awaiting_operator");
	const unsettled = (await h.request("ops.cycle")).result;
	expect(unsettled.gates).toContain("stale_session_identity");
	expect(unsettled.lanes.active).toBe(0);
	seedJob(h.database, "x", "done");
	const done = (await h.request("ops.cycle")).result;
	expect(done.gates).not.toContain("stale_session_identity");
	expect(done.lanes.active).toBe(0);
	expect(h.database.getSessionRecord("work/task/x")?.sessionId).toBe("");
});

test("H1 failed ledger retires when broker proves terminal_ok", async () => {
	const database = await GatewayDatabase.open(":memory:");
	try {
		const sessionId = crypto.randomUUID();
		database.putSession("work/task/a", sessionId);
		const { opRef } = seedJob(database, "a", "failed", sessionId);
		const port = new ScriptedSessionPort();
		port.seedOperation(opRef, sessionId, "terminal_ok", "finished");
		port.setSessionState(sessionId, { live: true });
		const before = database.getSessionRecord("work/task/a")!;
		const governor = new LaneGovernor({ database, sessionPort: port });
		expect(await governor.retire("a", "operator")).toMatchObject({ retired: true, sessionId, closed: true });
		expect(port.closes).toHaveLength(1);
		expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before.epoch + 1 });
	} finally {
		database.close();
	}
});

test("H2 failed ledger with unknown broker operation needs a dead session", async () => {
	for (const live of [true, false]) {
		const database = await GatewayDatabase.open(":memory:");
		try {
			const sessionId = crypto.randomUUID();
			database.putSession("work/task/a", sessionId);
			seedJob(database, "a", "failed", sessionId);
			const port = new ScriptedSessionPort();
			port.setSessionState(sessionId, { live });
			const before = database.getSessionRecord("work/task/a")!;
			const governor = new LaneGovernor({ database, sessionPort: port });
			expect(await governor.retire("a", "operator")).toMatchObject(
				live
					? { retired: false, reason: expect.stringContaining("broker reports unknown") }
					: { retired: true, sessionId, closed: true },
			);
			expect(port.closes).toHaveLength(live ? 0 : 1);
			expect(database.getSessionRecord("work/task/a")).toEqual(
				live ? before : { sessionId: "", epoch: before.epoch + 1 },
			);
		} finally {
			database.close();
		}
	}
});

test("H3 sweep nomination cannot retire a done job replaced by a fresh open attempt", async () => {
	const database = await GatewayDatabase.open(":memory:");
	const release = deferred();
	const port = new ScriptedSessionPort();
	let updating: Promise<void> | undefined;
	try {
		const sessionId = crypto.randomUUID();
		database.putSession("work/task/a", sessionId);
		const { record, identity } = seedJob(database, "a", "done", sessionId);
		const before = database.getSessionRecord("work/task/a");
		updating = port.runExclusive("work/task/a", async () => {
			await release.promise;
			const running = appendAttempt(
				{ ...record, state: "running" },
				{ opRef: newOpRef("gen3-redteam"), sessionId, startedAt: new Date().toISOString() },
			);
			database.putLaneJob({ ...running, laneKey: identity.laneKey, json: JSON.stringify(running) });
		});
		const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 1 });
		expect(governor.activeLanes()[0]?.state).toBe("done");
		const sweeping = governor.sweep();
		release.resolve();
		await updating;
		expect(governor.activeLanes()[0]).toMatchObject({ state: "running", attemptOpen: true });
		expect(await sweeping).toBe(0);
		expect(port.closes).toEqual([]);
		expect(database.getSessionRecord("work/task/a")).toEqual(before);
	} finally {
		release.resolve();
		await updating;
		database.close();
	}
});

test("H4 sweep refuses rebound even when the replacement job is also done", async () => {
	const database = await GatewayDatabase.open(":memory:");
	const release = deferred();
	const port = new ScriptedSessionPort();
	let updating: Promise<void> | undefined;
	try {
		database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a001");
		seedJob(database, "a", "done", "0000aaaa-0000-4000-8000-00000000a001");
		const before = database.getSessionRecord("work/task/a")!;
		updating = port.runExclusive("work/task/a", async () => {
			await release.promise;
			database.rebindEpoch("work/task/a");
			database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a002");
			seedJob(database, "a", "done", "0000aaaa-0000-4000-8000-00000000a002");
		});
		const outcomes: Awaited<ReturnType<LaneGovernor["retire"]>>[] = [];
		class ObservedGovernor extends LaneGovernor {
			override async retire(...args: Parameters<LaneGovernor["retire"]>) {
				const outcome = await super.retire(...args);
				outcomes.push(outcome);
				return outcome;
			}
		}
		const governor = new ObservedGovernor({ database, sessionPort: port });
		const sweeping = governor.sweep();
		release.resolve();
		await updating;
		expect(await sweeping).toBe(0);
		expect(outcomes).toEqual([
			{ retired: false, sessionKey: "work/task/a", reason: expect.stringContaining("rebound") },
		]);
		expect(governor.activeLanes()[0]?.state).toBe("done");
		expect(database.getSessionRecord("work/task/a")).toEqual({
			sessionId: "0000aaaa-0000-4000-8000-00000000a002",
			epoch: before.epoch + 1,
		});
		expect(port.closes).toEqual([]);
	} finally {
		release.resolve();
		await updating;
		database.close();
	}
});

test("H5 empty job JSON consumes capacity and socket retirement refuses corrupt record", async () => {
	const h = await harness(1);
	h.database.putSession("work/task/a", "empty-record-session");
	const { record, identity } = seedJob(h.database, "a", "done");
	h.database.putLaneJob({ ...record, laneKey: identity.laneKey, json: "" });
	const before = h.database.getSessionRecord("work/task/a");
	expect(h.database.laneJobJson(identity.jobId)).toBe("");
	expect((await h.run("b")).error).toMatchObject({
		code: "lane_capacity",
		detail: { active: 1, maxLanes: 1, candidates: [{ name: "a", state: "corrupt" }] },
	});
	expect((await h.request("work.retire", { name: "a" })).result).toMatchObject({
		retired: false,
		reason: expect.stringContaining("corrupt"),
	});
	expect(h.database.getSessionRecord("work/task/a")).toEqual(before);
	expect(h.port.binds).toEqual([]);
	expect(h.port.closes).toEqual([]);
});

test("H6 operator socket retirement of an idle done job needs no sweep nomination", async () => {
	const h = await harness();
	h.database.putSession("work/task/a", "0000aaaa-0000-4000-8000-00000000a003");
	seedJob(h.database, "a", "done", "0000aaaa-0000-4000-8000-00000000a003");
	const before = h.database.getSessionRecord("work/task/a")!;
	const governor = new LaneGovernor({ database: h.database, sessionPort: h.port });
	expect(governor.activeLanes()[0]).toMatchObject({ state: "done", idleMs: Number.POSITIVE_INFINITY });
	expect((await h.request("work.retire", { name: "a" })).result).toMatchObject({
		retired: true,
		sessionId: before.sessionId,
		closed: true,
	});
	expect(h.port.closes).toHaveLength(1);
	expect(h.database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before.epoch + 1 });
});

test("H7 concurrent sweeps close once and bump the epoch exactly once", async () => {
	const database = await GatewayDatabase.open(":memory:");
	try {
		database.putSession("work/task/a", "idle-session");
		const before = database.getSessionRecord("work/task/a")!;
		const port = new ScriptedSessionPort();
		const governor = new LaneGovernor({ database, sessionPort: port, idleRetireMs: 1 });
		expect((await Promise.all([governor.sweep(), governor.sweep()])).sort()).toEqual([0, 1]);
		expect(port.closes).toEqual([{ sessionId: before.sessionId, repo: process.cwd() }]);
		expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: before.epoch + 1 });
		expect(governor.activeLanes()).toEqual([]);
	} finally {
		database.close();
	}
});
