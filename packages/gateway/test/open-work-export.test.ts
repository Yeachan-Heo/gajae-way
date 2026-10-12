import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { lstat, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeOpenWorkExport } from "../src/ops/open-work-export";
import { Store } from "../src/store";

const roots: string[] = [];

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture() {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-open-work-"));
	roots.push(root);
	const path = join(root, "gateway.db");
	const database = await Store.open(path);
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "open-inbound",
			originKey: "discord:channel:fixture",
			originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "fixture" }),
			body: "replayable work",
		}),
	);
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "replayable-inbound",
			originKey: "discord:channel:fixture",
			originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "fixture" }),
			body: "not quarantined",
		}),
	);
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: "lanejob-invalid-record",
			laneKey: "fixture-lane",
			state: "running",
			createdAt: new Date().toISOString(),
			updatedAt: new Date().toISOString(),
			lane: { branch: "main", worktreePath: root },
			json: "{invalid",
		}),
	);
	database.close();
	const raw = new Database(path);
	raw
		.query("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('work', ?, ?)")
		.run("lanejob-invalid-record", "cutover-invalid");
	raw
		.query("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('inbound', ?, ?)")
		.run("open-inbound", "cutover-inbound");
	raw.close();
	return { root, path, database: await Store.open(path) };
}

test("collectOpenWork applies replayable, quarantined and all scopes without hiding corrupt record_json", async () => {
	const f = await fixture();
	try {
		const replayable = f.database.core.collectOpenWork("replayable");
		expect(replayable.inbound.map((row) => row.messageId)).toEqual(["replayable-inbound"]);
		expect(replayable.work.invalid).toHaveLength(0);

		const quarantined = f.database.core.collectOpenWork("quarantined");
		expect(quarantined.inbound.map((row) => row.messageId)).toEqual(["open-inbound"]);
		expect(quarantined.work.invalid).toHaveLength(1);
		expect(quarantined.work.invalid[0]).toMatchObject({
			kind: "lane_job",
			jobId: "lanejob-invalid-record",
			stateColumn: "running",
			assertion: expect.stringContaining("lane job state is corrupt"),
			recordJson: "{invalid",
			cutoverId: "cutover-invalid",
		});
		expect(quarantined.work.invalid[0]?.recordSha256).toMatch(/^[0-9a-f]{64}$/);

		const all = f.database.core.collectOpenWork("all");
		expect(all.inbound.map((row) => row.messageId)).toEqual(["open-inbound", "replayable-inbound"]);
		expect(all.work.invalid).toHaveLength(1);
	} finally {
		f.database.close();
	}
});

test("open-work export publishes v1 atomically with 0600 mode, backup hash and counts", async () => {
	const f = await fixture();
	try {
		const backupPath = join(f.root, "pre-cutover-2026-10-09.db");
		await writeFile(backupPath, "self-contained backup");
		const report = f.database.core.collectOpenWork("all");
		const receipt = writeOpenWorkExport(backupPath, {
			...report,
			reason: "cutover",
			schemaVersion: f.database.schemaVersion,
			authority: null,
		});
		expect(receipt.path).toBe(join(f.root, "pre-cutover-2026-10-09.open-work.json"));
		expect(receipt.counts).toEqual({
			inbound: 2,
			work: { laneJobs: 0, attempts: 0, laneReports: 0, invalid: 1 },
			monitors: 0,
		});
		expect((await lstat(receipt.path)).mode & 0o777).toBe(0o600);
		expect(receipt.sha256).toMatch(/^[0-9a-f]{64}$/);
		const document = JSON.parse(await readFile(receipt.path, "utf8"));
		expect(document).toMatchObject({
			kind: "gajaeway.open-work-export",
			version: 1,
			reason: "cutover",
			schemaVersion: 32,
			backup: { path: backupPath, sha256: expect.stringMatching(/^[0-9a-f]{64}$/) },
			cutoverIds: ["cutover-inbound", "cutover-invalid"],
			counts: receipt.counts,
		});
		expect(document.work.invalid[0]).toMatchObject({ recordJson: "{invalid", cutoverId: "cutover-invalid" });
		expect((await readdir(f.root)).filter((name) => name.includes("open-work.json.tmp"))).toEqual([]);
	} finally {
		f.database.close();
	}
});

test("export failure leaves no published output or temporary file", async () => {
	const f = await fixture();
	try {
		const absentBackup = join(f.root, "missing.db");
		const report = f.database.core.collectOpenWork("all");
		expect(() =>
			writeOpenWorkExport(absentBackup, {
				...report,
				reason: "cutover",
				schemaVersion: f.database.schemaVersion,
				authority: null,
			}),
		).toThrow();
		expect((await readdir(f.root)).some((name) => name.endsWith(".open-work.json"))).toBe(false);
	} finally {
		f.database.close();
	}
});
