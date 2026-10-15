import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { join } from "node:path";
import { Store } from "../src/store";
import { type Barrier, eventually, WorkFixture } from "./fixtures/work-lane-server";

const fixtures: WorkFixture[] = [];
afterEach(async () => {
	for (const fixture of fixtures.splice(0)) await fixture.cleanup();
});

async function fixture(barrier?: Barrier) {
	const f = await WorkFixture.create();
	fixtures.push(f);
	await f.start(barrier);
	return f;
}

async function projections(path: string) {
	const store = await Store.open(path);
	try {
		return { ...store.work.projectionRows(), deliveries: store.outbox.projectionRows() };
	} finally {
		store.close();
	}
}

function journalTypes(path: string, entity: string, key: string): string[] {
	const raw = new Database(path, { readonly: true });
	try {
		return raw
			.query<{ event_type: string }, [string, string]>(
				"SELECT event_type FROM work_journal WHERE entity = ? AND entity_key = ? ORDER BY seq",
			)
			.all(entity, key)
			.map((row) => row.event_type);
	} finally {
		raw.close();
	}
}

/**
 * With the gateway process dead, capture the live projections, wipe every projection row,
 * rebuild them from the journal alone and require field-for-field equality.
 */
async function assertReplayRebuildsLive(f: WorkFixture) {
	const path = join(f.home, "gateway.db");
	const live = await projections(path);
	const raw = new Database(path);
	try {
		raw.exec("DELETE FROM work_attempt_runtime; DELETE FROM lane_jobs; DELETE FROM deliveries;");
	} finally {
		raw.close();
	}
	expect(await projections(path)).toEqual({ attempts: [], laneJobs: [], deliveries: [] });
	const store = await Store.open(path);
	try {
		expect(store.replayJournal()).toEqual({
			attempts: live.attempts.length,
			laneJobs: live.laneJobs.length,
			deliveries: live.deliveries.length,
		});
		expect(store.replayJournal()).toEqual({ attempts: 0, laneJobs: 0, deliveries: 0 });
	} finally {
		store.close();
	}
	expect(await projections(path)).toEqual(live);
	return live;
}

async function settled(f: WorkFixture) {
	return eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.runtimes.length === 1 && snapshot.runtimes[0].settledAt !== null,
		"durable settlement",
	);
}

const BOUNDARIES = [
	"prepared",
	"accepted-before-save",
	"terminal-before",
	"terminal-after",
	"output-claim",
	"settle-before",
	"settle-cas",
	"settle-history",
	"settle-activity",
	"settle-ledger",
	"settle-commit",
] as const satisfies readonly Barrier[];

for (const point of BOUNDARIES) {
	test(`SIGKILL at ${point}: journal replay rebuilds the committed projections, then the live path`, async () => {
		const f = await fixture(point);
		const client = await f.connect();
		client.send({
			v: "0.1",
			type: "request",
			id: "start",
			verb: "work.start",
			params: { name: "journal", text: "journaled work", cwd: f.home },
		});
		const { opRef, sessionId } = (
			await eventually(
				() => f.snapshot(),
				(snapshot) => snapshot.runtimes.length === 1,
				"prepared attempt",
			)
		).runtimes[0];
		await f.control(opRef, { terminal: true, text: `answer at ${point}` });
		await f.barrier(point);
		await f.kill();

		const crashed = await assertReplayRebuildsLive(f);
		expect(crashed.attempts).toHaveLength(1);
		expect(crashed.attempts[0]?.settled_at === null).toBe(point !== "settle-commit");
		expect(crashed.deliveries).toHaveLength(point === "settle-commit" ? 1 : 0);

		// The restarted gateway runs on the replayed projections and must finish exactly as the live path does.
		if (point === "prepared") await f.control(sessionId, { live: "dead" });
		await f.start();
		const after = await settled(f);
		expect(after.jobs[0].attempts).toHaveLength(1);
		expect(after.jobs[0].attempts[0].endState).toBe(point === "prepared" ? "terminal_uncertain" : "completed");
		expect(after.deliveries).toHaveLength(1);
		await f.kill();
		const final = await assertReplayRebuildsLive(f);
		expect(final.attempts[0]?.settled_at).not.toBeNull();
		expect(final.deliveries[0]?.delivery_id).toBe(after.runtimes[0].deliveryId);
	}, 60_000);
}

test("attempt, lane-job and delivery transitions are journaled through a held lane and its retirement", async () => {
	const f = await fixture();
	const client = await f.connect();
	const started = await client.request("work.start", { name: "journal", text: "journaled work", cwd: f.home });
	expect(started.error).toBeUndefined();
	const { opRef } = started.result;
	await f.kill();
	await f.control(opRef, { live: "dead", unknown: true });
	await f.start();
	const operator = await f.connect();
	const held = await settled(f);
	expect(held.jobs[0].state).toBe("awaiting_operator");
	expect((await operator.request("work.retire", { name: "journal" })).result.retired).toBe(true);
	await f.kill();

	const path = join(f.home, "gateway.db");
	const attempt = journalTypes(path, "attempt", opRef);
	expect(attempt[0]).toBe("attempt_started");
	expect(attempt).toContain("attempt_accepted");
	expect(attempt.at(-1)).toMatch(/^attempt_(settled|reconciled)$/);
	const job = journalTypes(path, "lane_job", held.runtimes[0].jobId);
	expect(job[0]).toBe("lane_job_running");
	expect(job.at(-1)).toBe("lane_job_awaiting_operator");
	expect(journalTypes(path, "delivery", held.runtimes[0].deliveryId)[0]).toBe("delivery_queued");

	const final = await assertReplayRebuildsLive(f);
	expect(final.laneJobs[0]?.state).toBe("awaiting_operator");
}, 60_000);
