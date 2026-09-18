import { afterAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	appendAttempt,
	applyReconciliation,
	classifyWorkEvidence,
	closeAttempt,
	createLaneJobRecord,
	LaneJobError,
	type LaneJobRecord,
	parseLaneJobRecord,
	planContinuation,
} from "@gajae-gateway/subsession";
import { GatewayDatabase } from "../src/store/db";

const NOW = new Date("2026-08-27T13:00:00.000Z").toISOString();
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";

const directories: string[] = [];

afterAll(async () => {
	for (const directory of directories) await rm(directory, { recursive: true, force: true });
});

/** Opens a database in a persistent directory so repeated calls simulate restarts of the SAME deployment. */
async function openDatabase(): Promise<GatewayDatabase> {
	if (directories.length === 0) {
		directories.push(await mkdtemp(join(tmpdir(), "gajaeway-lanejob-")));
	}
	return GatewayDatabase.open(join(directories[0], "gateway.db"));
}

function storeJob(database: GatewayDatabase, record: LaneJobRecord, laneKey = "project-issue-10-lane-jobs"): void {
	database.putLaneJob({
		jobId: record.jobId,
		laneKey,
		state: record.state,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
		lane: record.lane,
		json: JSON.stringify(record),
	});
}

describe("lane_jobs store (migration v10)", () => {
	test("fresh database opens at the current schema with the lane_jobs table", async () => {
		const database = await openDatabase();
		try {
			expect(database.schemaVersion).toBe(22);
			expect(database.laneJobRows()).toEqual([]);
			expect(database.laneJobJson("lanejob-none")).toBeUndefined();
		} finally {
			database.close();
		}
	});

	test("a full deadline-killed cycle survives a gateway restart", async () => {
		let record = createLaneJobRecord({
			jobId: "lanejob-project-issue-10-lane-jobs",
			branch: "feat/issue-10-lane-jobs-takeover",
			worktreePath: "/wt/lane-jobs",
			sessionId: SESSION,
			baselineSha: "0".repeat(40),
			now: () => new Date(NOW),
		});
		const opRef = "gw-lanejob-01hqrestart0";
		record = appendAttempt(record, { opRef, sessionId: SESSION, startedAt: NOW });
		record = closeAttempt({
			record,
			opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: new Date(Date.parse(NOW) + 1_800_000).toISOString(),
		});
		record = applyReconciliation({
			record,
			repository: { headSha: "a".repeat(40), dirtyFiles: 3, observedAt: NOW },
			classification: "progressed",
		});

		const first = await openDatabase();
		storeJob(first, record);
		first.close(); // the controller/gateway process died here

		const second = await openDatabase();
		try {
			const storedJson = second.laneJobJson("lanejob-project-issue-10-lane-jobs");
			expect(typeof storedJson).toBe("string");
			const revived = parseLaneJobRecord(storedJson as string);
			expect(revived.checkpoints).toHaveLength(1);
			expect(revived.attempts[0].errorCode).toBe("prompt_deadline_exceeded");
			expect(revived.state).toBe("attempt_ended");

			// Repository evidence first, recovered without any reply body.
			const evidence = classifyWorkEvidence({
				headSha: "a".repeat(40),
				baselineSha: "0".repeat(40),
				dirtyFiles: 3,
				observedAt: NOW,
			});
			expect(evidence).toBe("work_committed_report_lost");

			// Continuation stays one deterministic path and is persisted.
			const decision = planContinuation({
				record: revived,
				repository: { headSha: "a".repeat(40), dirtyFiles: 3, observedAt: NOW },
				latestAttempt: revived.attempts[0],
				session: { live: true, deleted: false, locatorMatches: true },
				workComplete: false,
			});
			expect(decision.action).toBe("continue_same_session");

			const continued = appendAttempt(revived, {
				opRef: "gw-lanejob-01hqrestart1",
				sessionId: SESSION,
				startedAt: new Date(Date.parse(NOW) + 1_900_000).toISOString(),
			});
			storeJob(second, continued);

			const rows = second.laneJobRows();
			expect(rows).toHaveLength(1);
			expect(rows[0].state).toBe("running");
		} finally {
			second.close();
		}
	});

	test("a corrupt record_json is surfaced to the caller, never silently repaired", async () => {
		const database = await openDatabase();
		try {
			const valid = createLaneJobRecord({
				jobId: "lanejob-project-issue-10-lane-jobs",
				branch: "feat/issue-10-lane-jobs-takeover",
				worktreePath: "/wt/lane-jobs",
				now: () => new Date(NOW),
			});
			storeJob(database, valid);
			// Simulate on-disk corruption of the stored authority.
			database.putLaneJob({
				jobId: valid.jobId,
				laneKey: "project-issue-10-lane-jobs",
				state: valid.state,
				createdAt: valid.createdAt,
				updatedAt: valid.updatedAt,
				lane: valid.lane,
				json: '{"schemaVersion":1,"jobId":',
			});
			const corrupted = database.laneJobJson(valid.jobId);
			expect(typeof corrupted).toBe("string");
			expect(() => parseLaneJobRecord(corrupted as string)).toThrow(LaneJobError);
		} finally {
			database.close();
		}
	});
});
