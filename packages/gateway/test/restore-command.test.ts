import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { copyFile, lstat, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { writeBackup } from "../src/ops/backup";
import { runRestore } from "../src/ops/restore";
import { Store } from "../src/store";

const roots: string[] = [];
const LIVE_NAMES = ["gateway.db-wal", "gateway.db-shm", "gateway.db"] as const;
const INITIAL_FAULTS = [
	"after-staging",
	"after-journal-preserving",
	"after-preserve-wal",
	"after-preserve-shm",
	"after-preserve-db",
	"after-journal-preserved",
	"after-install-rename",
	"before-journal-installed",
	"after-journal-installed",
	"after-post-check-open",
	"before-journal-delete",
] as const;

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture() {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-restore-"));
	roots.push(root);
	const home = join(root, "home");
	await mkdir(home);
	const livePath = homeLayout(home).db;
	const initial = await Store.open(livePath);
	initial.core.metaSet("fixture", "original-main");
	initial.close();

	const ready = join(root, "wal-writer-ready");
	const writerScript = `
		import { Database } from "bun:sqlite";
		import { writeFileSync } from "node:fs";
		const db = new Database(${JSON.stringify(livePath)});
		db.exec("PRAGMA wal_autocheckpoint = 0");
		db.query("INSERT INTO meta(key, value) VALUES ('fixture_wal_only', 'live-L')").run();
		writeFileSync(${JSON.stringify(ready)}, "ready");
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
	`;
	const writer = Bun.spawn([process.execPath, "--eval", writerScript], { stdout: "ignore", stderr: "ignore" });
	const deadline = Date.now() + 10_000;
	while (!(await Bun.file(ready).exists()) && writer.exitCode === null && Date.now() < deadline) await Bun.sleep(10);
	if (!(await Bun.file(ready).exists())) {
		writer.kill("SIGKILL");
		await writer.exited;
		throw new Error("WAL writer did not reach its barrier");
	}
	writer.kill("SIGKILL");
	await writer.exited;
	const original: Record<string, { dev: number; ino: number; size: number } | null> = {};
	for (const name of LIVE_NAMES) {
		try {
			const info = await lstat(join(home, name));
			original[name] = { dev: info.dev, ino: info.ino, size: info.size };
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			original[name] = null;
		}
	}
	if (!original["gateway.db-wal"] || original["gateway.db-wal"]?.size === 0)
		throw new Error("fixture failed to leave a non-empty WAL");

	const sourcePath = join(root, "backup-source.db");
	const source = await Store.open(sourcePath);
	source.core.metaSet("fixture", "backup-B");
	source.core.metaSet("fixture_backup_only", "B");
	const backup = await writeBackup(source.core, join(root, "backup.db"));
	source.close();
	return { root, home, livePath, backup: backup.path, original };
}

async function sqliteRows(path: string) {
	const db = new Database(path, { readonly: true });
	try {
		return db.query("SELECT key, value FROM meta ORDER BY key").all();
	} finally {
		db.close();
	}
}

async function faultRestore(home: string, backup: string, point: string): Promise<void> {
	const modulePath = new URL("../src/ops/restore.ts", import.meta.url).href;
	const script = `
		import { runRestore } from ${JSON.stringify(modulePath)};
		process.exitCode = await runRestore(${JSON.stringify(backup)}, { home: ${JSON.stringify(home)} });
	`;
	const child = Bun.spawn([process.execPath, "--eval", script], {
		env: { ...process.env, GAJAEWAY_TEST_RESTORE_FAULT: point },
		stdout: "ignore",
		stderr: "ignore",
	});
	const outcome = await Promise.race([
		child.exited.then(() => "exited" as const),
		Bun.sleep(10_000).then(() => "timeout" as const),
	]);
	if (outcome === "timeout") child.kill("SIGKILL");
	await child.exited;
	expect(outcome).toBe("exited");
	expect(child.signalCode).toBe("SIGKILL");
}

async function quietly<T>(run: () => Promise<T>): Promise<T> {
	const log = console.log;
	const error = console.error;
	console.log = () => {};
	console.error = () => {};
	try {
		return await run();
	} finally {
		console.log = log;
		console.error = error;
	}
}

async function verifyRestored(f: Awaited<ReturnType<typeof fixture>>) {
	const actual = await sqliteRows(f.livePath);
	const expected = await sqliteRows(f.backup);
	expect(actual).toEqual(expected);
	const dirs = await readdir(homeLayout(f.home).backupsDir);
	const preserved = join(homeLayout(f.home).backupsDir, dirs.find((name) => name.startsWith("pre-restore-")) as string);
	for (const name of LIVE_NAMES) {
		const before = f.original[name];
		if (!before) continue;
		const after = await lstat(join(preserved, name));
		expect({ dev: after.dev, ino: after.ino, size: after.size }).toEqual(before);
	}
	const preservedCopy = join(f.root, "preserved-copy");
	await mkdir(preservedCopy);
	for (const name of LIVE_NAMES) {
		if (f.original[name]) await copyFile(join(preserved, name), join(preservedCopy, name));
	}
	const preservedDb = await Store.openReadonly(join(preservedCopy, "gateway.db"));
	try {
		expect(preservedDb.core.metaGet("fixture_wal_only")).toBe("live-L");
		expect(preservedDb.core.metaGet("fixture")).toBe("original-main");
	} finally {
		preservedDb.close();
	}
	expect(await Bun.file(join(f.home, "restore.journal.json")).exists()).toBe(false);
}

test("restore keeps WAL-only live work in the preserved generation and installs the backup bytes", async () => {
	const f = await fixture();
	const expectedHash = await crypto.subtle.digest("SHA-256", await Bun.file(f.backup).arrayBuffer());
	const code = await quietly(() => runRestore(f.backup, { home: f.home }));
	expect(code).toBe(0);
	expect(await crypto.subtle.digest("SHA-256", await Bun.file(f.livePath).arrayBuffer())).toEqual(expectedHash);
	await verifyRestored(f);
});

test("restore refuses a held lease, corrupt/newer backups, sidecar sources and non-empty backup WAL", async () => {
	const f = await fixture();
	const liveBefore = await readFile(f.livePath);
	const lease = await acquireLease(f.home, "gateway");
	try {
		expect(await quietly(() => runRestore(f.backup, { home: f.home }))).toBe(1);
	} finally {
		await lease.release();
	}
	const corrupt = join(f.root, "corrupt.db");
	await writeFile(corrupt, "not a database");
	expect(await quietly(() => runRestore(corrupt, { home: f.home }))).toBe(1);
	const newer = join(f.root, "newer.db");
	const newerDb = new Database(newer);
	newerDb.exec(
		"CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES (99, 'now')",
	);
	newerDb.close();
	expect(await quietly(() => runRestore(newer, { home: f.home }))).toBe(1);
	const nonSelfContained = join(f.root, "non-self-contained.db");
	await copyFile(f.backup, nonSelfContained);
	await writeFile(`${nonSelfContained}-wal`, "committed-but-unmerged");
	expect(await quietly(() => runRestore(nonSelfContained, { home: f.home }))).toBe(1);
	expect(await quietly(() => runRestore(f.livePath, { home: f.home }))).toBe(1);
	expect(await quietly(() => runRestore(join(f.home, "gateway.db-wal"), { home: f.home }))).toBe(1);
	expect(await readFile(f.livePath)).toEqual(liveBefore);
});

test("restore replaces corrupt and newer live databases without opening them first", async () => {
	for (const kind of ["corrupt", "newer"] as const) {
		const f = await fixture();
		if (kind === "corrupt") await writeFile(f.livePath, "not a live database");
		else {
			const newerPath = join(f.root, "newer-live.db");
			const newerDb = new Database(newerPath);
			newerDb.exec(
				"CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES (99, 'now')",
			);
			newerDb.close();
			await copyFile(newerPath, f.livePath);
		}
		expect(await quietly(() => runRestore(f.backup, { home: f.home }))).toBe(0);
		expect(await sqliteRows(f.livePath)).toEqual(await sqliteRows(f.backup));
		expect(await Bun.file(join(f.home, "restore.journal.json")).exists()).toBe(false);
	}
});

test("restore rolls back a pre-install interrupted generation on --abort", async () => {
	const f = await fixture();
	await faultRestore(f.home, f.backup, "after-preserve-wal");
	expect(await quietly(() => runRestore(undefined, { home: f.home, abort: true }))).toBe(3);
	for (const name of LIVE_NAMES) {
		const before = f.original[name];
		const after = await lstat(join(f.home, name)).catch((error: unknown) => {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
			throw error;
		});
		if (before === null) expect(after).toBeNull();
		else expect({ dev: after?.dev, ino: after?.ino, size: after?.size }).toEqual(before);
	}
	const restoredOriginal = await Store.openReadonly(f.livePath);
	try {
		expect(restoredOriginal.core.metaGet("fixture_wal_only")).toBe("live-L");
	} finally {
		restoredOriginal.close();
	}
	expect(await Bun.file(join(f.home, "restore.journal.json")).exists()).toBe(false);
});

test("abort returns the original inodes at every pre-install transition", async () => {
	for (const point of [
		"after-journal-preserving",
		"after-preserve-wal",
		"after-preserve-shm",
		"after-preserve-db",
		"after-journal-preserved",
	]) {
		const f = await fixture();
		await faultRestore(f.home, f.backup, point);
		expect(await quietly(() => runRestore(undefined, { home: f.home, abort: true }))).toBe(3);
		for (const name of LIVE_NAMES) {
			const before = f.original[name];
			const after = await lstat(join(f.home, name)).catch((error: unknown) => {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
				throw error;
			});
			if (before === null) expect(after).toBeNull();
			else expect({ dev: after?.dev, ino: after?.ino, size: after?.size }).toEqual(before);
		}
		expect(await Bun.file(join(f.home, "restore.journal.json")).exists()).toBe(false);
	}
});

test("child-process crash injection covers every restore transition and resume is idempotent", async () => {
	for (const point of INITIAL_FAULTS) {
		const f = await fixture();
		await faultRestore(f.home, f.backup, point);
		const code = await quietly(() => runRestore(f.backup, { home: f.home }));
		expect(code).toBe(0);
		await verifyRestored(f);
	}

	const f = await fixture();
	await faultRestore(f.home, f.backup, "after-journal-preserving");
	for (const point of [
		"after-preserve-wal",
		"after-preserve-shm",
		"after-preserve-db",
		"after-journal-preserved",
		"after-install-rename",
		"before-journal-installed",
		"after-journal-installed",
		"after-post-check-open",
		"before-journal-delete",
	]) {
		await faultRestore(f.home, f.backup, point);
	}
	expect(await quietly(() => runRestore(f.backup, { home: f.home }))).toBe(0);
	await verifyRestored(f);
	expect(await quietly(() => runRestore(f.backup, { home: f.home }))).toBe(0);
});

test("restore --abort is refused after install and anomalies keep the journal", async () => {
	const afterInstall = await fixture();
	await faultRestore(afterInstall.home, afterInstall.backup, "after-install-rename");
	expect(await quietly(() => runRestore(undefined, { home: afterInstall.home, abort: true }))).toBe(1);
	expect(await Bun.file(join(afterInstall.home, "restore.journal.json")).exists()).toBe(true);

	const anomaly = await fixture();
	await faultRestore(anomaly.home, anomaly.backup, "after-journal-installed");
	await writeFile(`${anomaly.livePath}-wal`, "foreign non-empty WAL");
	const installedBytes = await readFile(anomaly.livePath);
	expect(await quietly(() => runRestore(anomaly.backup, { home: anomaly.home }))).toBe(4);
	expect(await Bun.file(join(anomaly.home, "restore.journal.json")).exists()).toBe(true);
	expect(await readFile(anomaly.livePath)).toEqual(installedBytes);
});
