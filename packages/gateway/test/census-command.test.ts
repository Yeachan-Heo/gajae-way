import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease } from "@gajae-gateway/home";
import { loadConfig } from "../src/config";
import { runCensus } from "../src/ops/census";
import { Store } from "../src/store";

const homes: string[] = [];

afterEach(async () => {
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function fixture() {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-census-"));
	homes.push(home);
	await mkdir(join(home, "agent"));
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const path = join(home, "gateway.db");
	const database = await Store.open(path, { canonicalAgentDir: join(home, "agent") });
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "open-inbound",
			originKey: "discord:channel:census",
			originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "census" }),
			body: "pending",
		}),
	);
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: "lanejob-invalid-census",
			laneKey: "census-lane",
			state: "running",
			createdAt: new Date().toISOString(),
			updatedAt: new Date().toISOString(),
			lane: { branch: "main", worktreePath: home },
			json: "{broken",
		}),
	);
	database.close();
	const raw = new Database(path);
	raw
		.query("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('work', ?, ?)")
		.run("lanejob-invalid-census", "cutover-census");
	raw.close();
	return { home, path, config: await loadConfig({ home }) };
}

test("census is a read-only v38 view and reports quarantined invalid work by cutover id", async () => {
	const f = await fixture();
	const before = await readFile(f.path);
	const report = await runCensus(f.config);
	expect(report.schemaVersion).toBe(39);
	expect(report.authority).toEqual({
		canonicalAgentDir: join(f.home, "agent"),
		identity: `gjc:${join(f.home, "agent")}`,
	});
	expect(report.authorityCounts.openInbound).toBe(1);
	expect(report.tableCounts.inbound_messages).toBe(1);
	expect(report.quickCheck).toBe("ok");
	expect(report.leaseOwners).toHaveProperty("gateway");
	expect(report.openQuarantined["cutover-census"]).toMatchObject({ invalid: 1 });
	expect(report.authorityCounts.inspectionError).toContain("lane job state is corrupt");
	expect(await readFile(f.path)).toEqual(before);
});

test("census reports the work_journal row count and its on-disk byte size", async () => {
	const f = await fixture();
	const raw = new Database(f.path, { readonly: true });
	const { rows, jsonBytes } = raw
		.query("SELECT COUNT(*) AS rows, COALESCE(SUM(LENGTH(CAST(row_json AS BLOB))), 0) AS jsonBytes FROM work_journal")
		.get() as { rows: number; jsonBytes: number };
	const { pageSize } = raw.query("SELECT page_size AS pageSize FROM pragma_page_size()").get() as { pageSize: number };
	// SQLite builds without SQLITE_ENABLE_DBSTAT_VTAB take the documented estimate instead of page sizes.
	let hasDbstat = true;
	try {
		raw.query("SELECT 1 FROM dbstat LIMIT 1").get();
	} catch {
		hasDbstat = false;
	}
	raw.close();
	expect(rows).toBeGreaterThan(0);
	const report = await runCensus(f.config);
	expect(report.journalRowCount).toBe(rows);
	if (hasDbstat) {
		expect(report.journalByteSize).toBeGreaterThanOrEqual(jsonBytes);
		expect(report.journalByteSize % pageSize).toBe(0);
	} else {
		expect(report.journalByteSize).toBe(jsonBytes + rows * 80);
	}
});

test("census refuses while a daemon holds the gateway lease", async () => {
	const f = await fixture();
	const lease = await acquireLease(f.home, "gateway");
	try {
		await expect(runCensus(f.config)).rejects.toMatchObject({ code: "lease_held", name: "gateway" });
	} finally {
		await lease.release();
	}
});

test("main wires the census verb to the offline command", async () => {
	const f = await fixture();
	const entry = join(import.meta.dir, "../src/main.ts");
	const child = Bun.spawn([process.execPath, entry, "census"], {
		env: { ...process.env, GAJAEWAY_HOME: f.home },
		stdout: "pipe",
		stderr: "pipe",
	});
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	expect(stderr).toBe("");
	expect(exitCode).toBe(0);
	expect(stdout).toContain('"schemaVersion": 39');
});

test("census refuses a restore journal before opening the database and releases its lease", async () => {
	const f = await fixture();
	const before = await readFile(f.path);
	await writeFile(join(f.home, "restore.journal.json"), "{}\n", { mode: 0o600 });
	await expect(runCensus(f.config)).rejects.toMatchObject({ code: "restore_incomplete" });
	expect(await readFile(f.path)).toEqual(before);
	const lease = await acquireLease(f.home, "gateway");
	await lease.release();
});

test("census handles pre-v36 database without work_journal table", async () => {
	const f = await fixture();
	const raw = new Database(f.path);
	try {
		// Remove v36 migration to simulate pre-v36 database
		raw.exec("DROP TABLE work_journal");
		raw.exec("DELETE FROM schema_migrations WHERE version >= 36");
		raw.close();
		// Census should still work and report null/0 for journal metrics
		const report = await runCensus(f.config);
		expect(report.journalRowCount).toBe(0);
		expect(report.journalByteSize).toBe(0);
	} finally {
		try {
			raw.close();
		} catch {}
	}
});
