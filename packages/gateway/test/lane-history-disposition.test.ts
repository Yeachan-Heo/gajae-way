import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAttempt, closeAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { LaneGovernor, laneJobIdentity, workSessionKey } from "../src/orchestrator/lane-governor";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { Store } from "../src/store";
import { type WorkAttemptRuntime, workAttemptDeliveryId, workAttemptReportId } from "../src/store/groups/work";
import { initializeTestBrokerAuthority, ScriptedSessionPort } from "./session-port.fake";

const START = "2026-09-08T00:00:00.000Z";
const END = "2026-09-08T00:00:10.000Z";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const handles: Array<{ close(): void }> = [];
const directories: string[] = [];
afterEach(async () => {
	for (const database of handles.splice(0)) database.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function fixture(
	port = new ScriptedSessionPort(),
	brokerGeneration = () => 0,
	ownership: { originKey?: string; repo?: string } | false = {},
) {
	const directory = await mkdtemp(join(tmpdir(), "lane-history-disposition-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	handles.push(database);
	const raw = new Database(path);
	handles.push(raw);
	const authority = initializeTestBrokerAuthority(database, join(directory, "agent"));
	const identity = laneJobIdentity("a");
	const sessionKey = workSessionKey("a");
	if (ownership !== false)
		database.transaction((tx) =>
			database.bindings.recordOwnedBinding(tx, {
				authority,
				sessionId: SESSION,
				originKey: ownership.originKey ?? sessionKey,
				epoch: 0,
				repo: ownership.repo ?? directory,
			}),
		);
	const record = appendAttempt(
		createLaneJobRecord({
			jobId: identity.jobId,
			branch: "work/a",
			worktreePath: directory,
			sessionId: SESSION,
			now: () => new Date(START),
		}),
		{ opRef: "gw-history-a", sessionId: SESSION, startedAt: START },
	);
	const runtime: WorkAttemptRuntime = {
		opRef: "gw-history-a",
		jobId: identity.jobId,
		laneKey: identity.laneKey,
		sessionKey,
		sessionId: SESSION,
		epoch: 0,
		cwd: directory,
		startedAt: START,
		mode: "run",
		sendPhase: "prepared",
		sendEvidence: null,
		terminal: null,
		output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
		parent: null,
		reportId: workAttemptReportId(database.instanceId, identity.jobId, "gw-history-a"),
		wakeReportId: null,
		noticeHash: null,
		deliveryId: workAttemptDeliveryId(database.instanceId, identity.jobId, "gw-history-a"),
		decision: "undecided",
		settledAt: null,
		version: 0,
	};
	const closed = closeAttempt({ record, opRef: runtime.opRef, endState: "completed", endedAt: END });
	database.transaction((tx) => {
		database.work.workAttemptPrepare(tx, runtime, record);
		database.work.workAttemptSettle(tx, runtime.opRef, 0, closed, {
			terminal: {
				kind: "broker",
				observedAt: END,
				reasonCode: "end_turn",
				status: {
					status: "terminal_ok",
					clientRef: runtime.opRef,
					receiptState: "present",
					outcome: { reason: "end_turn" },
				},
			},
			output: { ...runtime.output, disposition: "unavailable" },
			decision: "no_target",
			settledAt: END,
		});
		database.bindings.rotateEpoch(tx, sessionKey);
	});
	port.setSessionState(SESSION, { live: false, repo: directory });
	const governor = new LaneGovernor({
		database,
		sessionPort: port,
		brokerGeneration,
		now: () => Date.parse(END),
		log: () => {},
	});
	const history = () => {
		const json = database.work.laneJobJson(identity.jobId);
		if (json === undefined) throw new Error("fixture history missing");
		return parseLaneJobRecord(json);
	};
	const putHistory = (patch: Partial<typeof closed>) => {
		const next = { ...history(), ...patch };
		database.transaction((tx) =>
			database.work.putLaneJob(tx, { ...next, laneKey: identity.laneKey, json: JSON.stringify(next) }),
		);
	};
	const terminal = () => {
		const value = database.work.workAttemptGet(runtime.opRef)?.terminal;
		if (!value?.status) throw new Error("fixture broker terminal status missing");
		return { ...value, status: value.status };
	};
	const patchRuntime = (patch: Partial<WorkAttemptRuntime>) => {
		const stored = database.work.workAttemptGet(runtime.opRef);
		if (!stored) throw new Error("fixture runtime missing");
		const next = { ...stored, ...patch };
		raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, settled_at = ?, version = ? WHERE op_ref = ?")
			.run(JSON.stringify(next), next.settledAt, next.version, runtime.opRef);
	};
	return {
		database,
		raw,
		authority,
		directory,
		identity,
		sessionKey,
		runtime,
		closed,
		port,
		governor,
		history,
		terminal,
		putHistory,
		patchRuntime,
	};
}

test("operator retirement dispositions proven terminal unbound history without deleting evidence", async () => {
	const f = await fixture();
	await writeFile(join(f.directory, "uncommitted.txt"), "preserve user work");
	const before = f.history();
	const binding = f.database.bindings.getSessionRecord(f.sessionKey);
	const attempt = f.database.work.workAttemptGet(f.runtime.opRef);
	expect(f.governor.activeLanes()).toEqual([]);
	expect(before.state).toBe("attempt_ended");
	expect(await f.governor.retire("a", "operator")).toMatchObject({
		retired: true,
		closed: false,
		disposition: "aborted",
	});
	expect(f.history()).toEqual({
		...before,
		state: "aborted",
		updatedAt: END,
		escalations: [...before.escalations, `${END} operator retired unbound history; attempts and worktree preserved`],
	});
	expect(f.database.bindings.getSessionRecord(f.sessionKey)).toEqual(binding);
	expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(attempt);
	expect(await readFile(join(f.directory, "uncommitted.txt"), "utf8")).toBe("preserve user work");
	expect(f.port.closes).toEqual([]);
	expect(f.port.sends).toEqual([]);
	expect(f.database.journal.latest("lane_job").get(f.identity.jobId)?.state).toBe("aborted");
	const disposed = f.database.work.projectionRows();
	f.database.replayJournal();
	expect(f.database.work.projectionRows()).toEqual(disposed);
	expect((await f.governor.retire("a", "operator")).retired).toBe(false);
	expect(f.database.work.projectionRows()).toEqual(disposed);
});

for (const schemaVersion of [2, 3]) {
	test(`disposition preserves the original validated history payload (schema ${schemaVersion})`, async () => {
		const f = await fixture();
		const before = {
			...f.history(),
			schemaVersion,
			operatorEvidence: { receipt: "preserve-original-evidence" },
			attempts: f.history().attempts.map((attempt) => ({ ...attempt, evidenceRef: "preserve-attempt-evidence" })),
		};
		f.raw.query("UPDATE lane_jobs SET record_json = ? WHERE job_id = ?").run(JSON.stringify(before), f.identity.jobId);
		expect((await f.governor.retire("a", "operator")).retired).toBe(true);
		const json = f.database.work.laneJobJson(f.identity.jobId);
		if (json === undefined) throw new Error("disposed history missing");
		expect(JSON.parse(json)).toEqual({
			...before,
			state: "aborted",
			updatedAt: END,
			escalations: [...before.escalations, `${END} operator retired unbound history; attempts and worktree preserved`],
		});
	});
}

type Fixture = Awaited<ReturnType<typeof fixture>>;
const terminalHistories: Array<{
	name: string;
	endState: NonNullable<Fixture["closed"]["attempts"][number]["endState"]>;
	reasonCode: string;
	status: NonNullable<NonNullable<WorkAttemptRuntime["terminal"]>["status"]>;
}> = [
	{
		name: "completed",
		endState: "completed",
		reasonCode: "end_turn",
		status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
	},
	{
		name: "failed",
		endState: "failed",
		reasonCode: "sdk_failed",
		status: { status: "failed", receiptState: "present", error: { code: "sdk_failed" } },
	},
	{
		name: "terminal_uncertain",
		endState: "terminal_uncertain",
		reasonCode: "terminal_uncertain",
		status: { status: "terminal_ok", receiptState: "unknown" },
	},
	{
		name: "terminal_missing_receipt",
		endState: "terminal_missing_receipt",
		reasonCode: "terminal_missing_receipt",
		status: { status: "terminal_ok", receiptState: "missing" },
	},
	{
		name: "cancelled without deliverable success",
		endState: "attempt_ended",
		reasonCode: "cancelled",
		status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "cancelled" } },
	},
];

for (const state of ["attempt_ended", "awaiting_operator"] as const) {
	for (const { name, endState, reasonCode, status } of terminalHistories) {
		test(`operator dispositions ${state} ${name} history without promoting its outcome`, async () => {
			const f = await fixture();
			f.putHistory({ state, attempts: [{ ...f.closed.attempts[0], endState }] });
			f.patchRuntime({
				terminal: { kind: "broker", observedAt: END, reasonCode, status: { ...status, clientRef: f.runtime.opRef } },
			});
			const before = f.history();
			const runtime = f.database.work.workAttemptGet(f.runtime.opRef);
			const binding = f.database.bindings.getSessionRecord(f.sessionKey);
			expect(await f.governor.retire("a", "operator")).toMatchObject({
				retired: true,
				closed: false,
				disposition: "aborted",
			});
			expect(f.history()).toEqual({
				...before,
				state: "aborted",
				updatedAt: END,
				escalations: [
					...before.escalations,
					`${END} operator retired unbound history; attempts and worktree preserved`,
				],
			});
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(runtime);
			expect(f.database.bindings.getSessionRecord(f.sessionKey)).toEqual(binding);
			const disposed = f.database.work.projectionRows();
			expect((await f.governor.retire("a", "operator")).retired).toBe(false);
			expect(f.database.work.projectionRows()).toEqual(disposed);
			expect(f.port.closes).toEqual([]);
			expect(f.port.sends).toEqual([]);
		});
	}
}

const refusals: Array<[string, (f: Fixture) => void]> = [
	...(["stalled", "running", "awaiting_checks", "done", "aborted"] as const).map(
		(state): [string, (f: Fixture) => void] => [
			`job state ${state}`,
			(f) =>
				f.putHistory({
					state,
					...(state === "awaiting_checks"
						? { awaitingChecks: { prNumber: null, headSha: "a".repeat(40), since: END, retries: 0 } }
						: {}),
				}),
		],
	),
	["exhausted hold budget", (f) => f.putHistory({ stalledContinuations: 3 })],
	[
		"hidden checks cycle",
		(f) => f.putHistory({ awaitingChecks: { prNumber: null, headSha: "a".repeat(40), since: END, retries: 0 } }),
	],
	[
		"open historical attempt",
		(f) => f.putHistory({ attempts: [{ ...f.closed.attempts[0], endedAt: undefined, endState: undefined }] }),
	],
	["empty history", (f) => f.putHistory({ attempts: [] })],
	[
		"missing runtime",
		(f) => {
			f.raw.exec("DELETE FROM work_attempt_runtime");
		},
	],
	[
		"corrupt runtime",
		(f) => {
			f.raw.exec("UPDATE work_attempt_runtime SET record_json = '{}' ");
		},
	],
	[
		"corrupt history",
		(f) => {
			f.raw.exec("UPDATE lane_jobs SET record_json = '{}' ");
		},
	],
	[
		"mismatched state projection",
		(f) => {
			f.raw.exec("UPDATE lane_jobs SET state = 'running'");
		},
	],
	[
		"mismatched repository projection",
		(f) => {
			f.raw.exec("UPDATE lane_jobs SET worktree_path = '/other'");
		},
	],
	[
		"mismatched lane projection",
		(f) => {
			f.raw.exec("UPDATE lane_jobs SET lane_key = 'work-other'");
		},
	],
	[
		"quarantine",
		(f) => {
			f.database.transaction((tx) => f.database.bindings.quarantineInvalidWork(tx, f.identity.jobId));
		},
	],
	["local terminal", (f) => f.patchRuntime({ terminal: { kind: "local", observedAt: END, reasonCode: "end_turn" } })],
	...(["absent", undefined] as const).map((receiptState): [string, (f: Fixture) => void] => [
		`${receiptState ?? "omitted"} receipt state`,
		(f) => {
			f.putHistory({ attempts: [{ ...f.closed.attempts[0], endState: "attempt_ended" }] });
			f.patchRuntime({
				terminal: {
					kind: "broker",
					observedAt: END,
					reasonCode: "stopped_incomplete",
					status: { status: "terminal_ok", clientRef: f.runtime.opRef, receiptState },
				},
			});
		},
	]),
	["indeterminate recovery decision", (f) => f.patchRuntime({ decision: "recovery_indeterminate" })],
	["unsettled runtime", (f) => f.patchRuntime({ decision: "undecided", settledAt: null })],
	[
		"mismatched client reference",
		(f) =>
			f.patchRuntime({
				terminal: {
					...f.terminal(),
					status: {
						status: "terminal_ok",
						clientRef: "gw-other",
						receiptState: "present",
						outcome: { reason: "end_turn" },
					},
				},
			}),
	],
	[
		"missing receipt with mismatched completed attempt",
		(f) =>
			f.patchRuntime({
				terminal: {
					kind: "broker",
					observedAt: END,
					reasonCode: "terminal_missing_receipt",
					status: { status: "terminal_ok", clientRef: f.runtime.opRef, receiptState: "missing" },
				},
			}),
	],
	[
		"unknown receipt with mismatched completed attempt",
		(f) =>
			f.patchRuntime({
				terminal: {
					kind: "broker",
					observedAt: END,
					reasonCode: "terminal_uncertain",
					status: { status: "terminal_ok", clientRef: f.runtime.opRef, receiptState: "unknown" },
				},
			}),
	],
	[
		"inconsistent terminal reason",
		(f) =>
			f.patchRuntime({
				terminal: { ...f.terminal(), reasonCode: "cancelled" },
			}),
	],
	[
		"inconsistent attempt outcome",
		(f) => f.putHistory({ attempts: [{ ...f.closed.attempts[0], endState: "attempt_ended" }] }),
	],
	["live historical session", (f) => f.port.setSessionState(SESSION, { live: true, repo: f.directory })],
	["wrong runtime session", (f) => f.patchRuntime({ sessionId: "22222222-2584-4d13-b7b6-c6ac24a1087f" })],
	["wrong runtime repository", (f) => f.patchRuntime({ cwd: join(f.directory, "other") })],
	["wrong runtime epoch", (f) => f.patchRuntime({ epoch: 9 })],
	[
		"unretired epoch",
		(f) => {
			f.raw.exec("UPDATE sessions SET epoch = 0");
		},
	],
	[
		"missing binding",
		(f) => {
			f.raw.exec("DELETE FROM sessions");
		},
	],
	[
		"foreign job identity",
		(f) => {
			f.raw.exec("UPDATE lane_jobs SET record_json = replace(record_json, 'lanejob-61', 'lanejob-62')");
		},
	],
	[
		"unresolved report",
		(f) => {
			f.database.transaction((tx) =>
				f.database.work.laneReportInsert(
					tx,
					{
						reportId: workAttemptReportId(f.database.instanceId, "lanejob-6368696c64", "gw-child"),
						parentName: "a",
						childName: "child",
						childOpRef: "gw-child",
						body: "report",
						root: null,
					},
					END,
				),
			);
		},
	],
];

for (const receiptState of ["missing", "unknown"] as const) {
	for (const [name, change] of [
		["wrong runtime session", (f: Fixture) => f.patchRuntime({ sessionId: "22222222-2584-4d13-b7b6-c6ac24a1087f" })],
		["wrong runtime repository", (f: Fixture) => f.patchRuntime({ cwd: join(f.directory, "other") })],
		[
			"uncorrelated client reference",
			(f: Fixture) =>
				f.patchRuntime({
					terminal: { ...f.terminal(), status: { ...f.terminal().status, clientRef: "gw-other" } },
				}),
		],
		[
			"absent client reference",
			(f: Fixture) =>
				f.patchRuntime({
					terminal: { ...f.terminal(), status: { ...f.terminal().status, clientRef: undefined } },
				}),
		],
		[
			"inconsistent recorded reason",
			(f: Fixture) => f.patchRuntime({ terminal: { ...f.terminal(), reasonCode: "end_turn" } }),
		],
		[
			"malformed receipt state",
			(f: Fixture) => {
				f.raw.exec(
					"UPDATE work_attempt_runtime SET record_json = json_set(record_json, '$.terminal.status.receiptState', 'invalid')",
				);
			},
		],
		[
			"nonterminal broker status",
			(f: Fixture) => {
				f.raw.exec(
					"UPDATE work_attempt_runtime SET record_json = json_set(record_json, '$.terminal.status.status', 'running')",
				);
			},
		],
	] as const) {
		test(`awaiting_operator refuses ${receiptState} receipt with ${name}`, async () => {
			const f = await fixture();
			const endState = receiptState === "missing" ? "terminal_missing_receipt" : "terminal_uncertain";
			f.putHistory({ state: "awaiting_operator", attempts: [{ ...f.closed.attempts[0], endState }] });
			f.patchRuntime({
				terminal: {
					kind: "broker",
					observedAt: END,
					reasonCode: endState,
					status: { status: "terminal_ok", clientRef: f.runtime.opRef, receiptState },
				},
			});
			change(f);
			const before = f.database.work.projectionRows();
			expect((await f.governor.retire("a", "operator")).retired).toBe(false);
			expect(f.database.work.projectionRows()).toEqual(before);
			expect(f.port.closes).toEqual([]);
			expect(f.port.sends).toEqual([]);
		});
	}
}

for (const [state, name, change] of (["attempt_ended", "awaiting_operator"] as const).flatMap((state) =>
	refusals.map(([name, change]) => [state, name, change] as const),
)) {
	test(`${state} disposition refuses ${name} without mutating history`, async () => {
		const f = await fixture();
		f.putHistory({ state });
		change(f);
		const before = f.database.work.projectionRows();
		expect((await f.governor.retire("a", "operator")).retired).toBe(false);
		expect(f.database.work.projectionRows()).toEqual(before);
		expect(f.port.closes).toEqual([]);
		expect(f.port.sends).toEqual([]);
	});
}

class ProbePort extends ScriptedSessionPort {
	onProbe?: () => void;
	override async liveness(input: { sessionId: string; repo: string }) {
		const result = await super.liveness(input);
		this.onProbe?.();
		return result;
	}
}

for (const [name, change] of [
	["history", (f: Fixture) => f.putHistory({ report: "new evidence" })],
	[
		"epoch",
		(f: Fixture) => {
			f.database.transaction((tx) => f.database.bindings.rotateEpoch(tx, f.sessionKey));
		},
	],
	["runtime version", (f: Fixture) => f.patchRuntime({ version: 5 })],
	[
		"quarantine",
		(f: Fixture) => {
			f.database.transaction((tx) => f.database.bindings.quarantineInvalidWork(tx, f.identity.jobId));
		},
	],
	[
		"authority",
		(f: Fixture) => {
			f.database.transaction((tx) =>
				f.database.bindings.setAuthority(tx, {
					canonicalAgentDir: join(f.directory, "other-agent"),
					identity: `gjc:${join(f.directory, "other-agent")}`,
				}),
			);
		},
	],
	[
		"projection",
		(f: Fixture) => {
			f.raw.exec("UPDATE lane_jobs SET state = 'running'");
		},
	],
	[
		"new binding",
		(f: Fixture) => {
			f.database.transaction((tx) =>
				f.database.bindings.recordOwnedBinding(tx, {
					authority: f.authority,
					sessionId: "bcb7f8a3-30e5-4d76-9e7e-32d2e2f51f9a",
					originKey: f.sessionKey,
					epoch: 1,
					repo: f.directory,
				}),
			);
		},
	],
] as const) {
	test(`historical disposition refuses ${name} changing during the death probe`, async () => {
		const port = new ProbePort();
		const f = await fixture(port);
		port.onProbe = () => change(f);
		expect((await f.governor.retire("a", "operator")).retired).toBe(false);
		expect(f.history().state).toBe("attempt_ended");
		expect(f.port.closes).toEqual([]);
	});
}

test("unavailable death probe refuses disposition without assuming session death", async () => {
	class FailingProbePort extends ScriptedSessionPort {
		override async liveness(): Promise<never> {
			throw new Error("broker unavailable");
		}
	}
	const f = await fixture(new FailingProbePort());
	expect((await f.governor.retire("a", "operator")).retired).toBe(false);
	expect(f.history().state).toBe("attempt_ended");
});

for (const live of [false, true]) {
	test(`all historical sessions must be dead, not just the last attempt (earlier live=${live})`, async () => {
		const f = await fixture();
		const earlier = "bcb7f8a3-30e5-4d76-9e7e-32d2e2f51f9a";
		f.database.transaction((tx) => {
			f.database.bindings.recordOwnedBinding(tx, {
				authority: f.authority,
				sessionId: earlier,
				originKey: f.sessionKey,
				epoch: 1,
				repo: f.directory,
			});
			f.database.bindings.rotateEpoch(tx, f.sessionKey);
		});
		f.putHistory({ sessions: [earlier, SESSION] });
		f.port.setSessionState(earlier, { live, repo: f.directory });
		expect((await f.governor.retire("a", "operator")).retired).toBe(!live);
		expect(f.port.closes).toEqual([]);
	});
}

for (const ownership of [false, { originKey: "work/task/other" }, { repo: "/different" }] as const) {
	test(`historical disposition refuses unproven ownership ${JSON.stringify(ownership)}`, async () => {
		const f = await fixture(new ScriptedSessionPort(), () => 0, ownership);
		const before = f.database.work.projectionRows();
		expect((await f.governor.retire("a", "operator")).retired).toBe(false);
		expect(f.database.work.projectionRows()).toEqual(before);
	});
}

test("broker generation changing during death proof refuses disposition", async () => {
	let generation = 0;
	const port = new ProbePort();
	const f = await fixture(port, () => generation);
	port.onProbe = () => {
		generation++;
	};
	expect((await f.governor.retire("a", "operator")).retired).toBe(false);
	expect(f.history().state).toBe("attempt_ended");
});

for (const liveness of [
	{ live: undefined, disowned: false },
	{ live: false, disowned: true },
] as const) {
	test(`uncertain liveness ${JSON.stringify(liveness)} refuses disposition`, async () => {
		class UncertainPort extends ScriptedSessionPort {
			override async liveness() {
				return liveness;
			}
		}
		const f = await fixture(new UncertainPort());
		expect((await f.governor.retire("a", "operator")).retired).toBe(false);
		expect(f.history().state).toBe("attempt_ended");
	});
}

test("force, bulk and sweep retirement do not disposition unbound history", async () => {
	const f = await fixture();
	const before = f.database.work.projectionRows();
	expect((await f.governor.forceRetire("a")).retired).toBe(false);
	expect(await f.governor.retireAllDead()).toEqual({ count: 0, names: [] });
	expect(await f.governor.sweep()).toBe(0);
	expect((await f.governor.retire("a", "idle")).retired).toBe(false);
	expect(f.database.work.projectionRows()).toEqual(before);
});

test("operator-disposed history cannot be restarted, including explicit resume", async () => {
	const f = await fixture();
	expect((await f.governor.retire("a", "operator")).retired).toBe(true);
	const manager = new WorkLaneManager({
		database: f.database,
		port: f.port,
		lanes: f.governor,
		gajaewayHome: f.directory,
		serviceDirectory: join(f.directory, "service"),
		pollMs: 5,
	});
	try {
		for (const resume of [false, true]) {
			expect(await manager.start({ name: "a", text: "must not run", resume })).toMatchObject({
				started: false,
				held: true,
				state: "aborted",
			});
		}
		expect(f.port.binds).toEqual([]);
		expect(f.port.resumes).toEqual([]);
		expect(f.port.sends).toEqual([]);
		expect(f.history().attempts).toHaveLength(1);
	} finally {
		await manager.stop();
	}
});
