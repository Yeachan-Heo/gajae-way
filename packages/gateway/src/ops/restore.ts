import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { copyFile, chmod, lstat, mkdir, open, readdir, realpath, rename, rmdir, stat, unlink, writeFile } from "node:fs/promises";
import { basename, isAbsolute, join, relative, resolve } from "node:path";

import { LeaseHeldError, homeLayout } from "@gajae-gateway/home";
import { gatewayHome } from "../config";
import { withGatewayLease } from "../offline";
import { StoreCore } from "../store/core";
import { LATEST_SCHEMA_VERSION } from "../store/migrations";

type RestorePhase = "preserving" | "preserved" | "installed";
type LiveName = string;
type FileIdentity = { readonly dev: number; readonly ino: number; readonly size: number };
type OriginalSet = Readonly<Record<LiveName, FileIdentity | null>>;

interface RestoreJournal {
	readonly version: 1;
	readonly id: string;
	readonly backup: string;
	readonly backupSha256: string;
	readonly staged: string;
	readonly preservedDir: string;
	readonly phase: RestorePhase;
	readonly original: OriginalSet;
}

function databaseName(home: string): string {
	return basename(homeLayout(home).db);
}

function liveNames(home: string): readonly [string, string, string] {
	const base = databaseName(home);
	return [`${base}-wal`, `${base}-shm`, base];
}

function stagedName(home: string, id: string): string {
	return `${databaseName(home)}.restore-${id}.tmp`;
}

function isStagedName(home: string, name: string): boolean {
	const prefix = `${databaseName(home)}.restore-`;
	const suffix = ".tmp";
	return name.startsWith(prefix) && name.endsWith(suffix) && /^[0-9a-f-]{36}$/.test(name.slice(prefix.length, -suffix.length));
}

class RestoreRefusedError extends Error {}
class RestoreUnrecoverableError extends Error {}

function diagnostic(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function fault(point: string): void {
	if (process.env.GAJAEWAY_TEST_RESTORE_FAULT !== point) return;
	process.kill(process.pid, "SIGKILL");
	Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
}

async function sha256File(path: string): Promise<string> {
	const hash = createHash("sha256");
	for await (const chunk of createReadStream(path)) hash.update(chunk);
	return hash.digest("hex");
}

async function syncPath(path: string): Promise<void> {
	const handle = await open(path, "r+");
	try {
		await handle.sync();
	} finally {
		await handle.close();
	}
}

async function syncDirectory(path: string): Promise<void> {
	const handle = await open(path, "r");
	try {
		await handle.sync();
	} finally {
		await handle.close();
	}
}

async function exists(path: string): Promise<boolean> {
	try {
		await lstat(path);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw error;
	}
}

async function inspectRegular(path: string): Promise<FileIdentity | null> {
	try {
		const info = await lstat(path);
		if (!info.isFile() || info.isSymbolicLink()) throw new RestoreUnrecoverableError(`unexpected_non_regular_file path=${path}`);
		return { dev: info.dev, ino: info.ino, size: info.size };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
		throw error;
	}
}

function sameIdentity(left: FileIdentity | null, right: FileIdentity | null, includeSize = true): boolean {
	return left !== null && right !== null && left.dev === right.dev && left.ino === right.ino && (!includeSize || left.size === right.size);
}

function isRecordedInOriginal(journal: RestoreJournal, identity: FileIdentity | null): boolean {
	return (
		identity !== null &&
		Object.values(journal.original).some(
			(original) => original !== null && original.dev === identity.dev && original.ino === identity.ino,
		)
	);
}

async function readRestoreJournal(path: string, home: string, backupsDir: string): Promise<RestoreJournal> {
	const info = await lstat(path).catch((error: unknown) => {
		throw new RestoreUnrecoverableError(`restore_journal_unavailable ${diagnostic(error)}`);
	});
	if (info.isSymbolicLink() || !info.isFile() || info.nlink !== 1 || (info.mode & 0o777) !== 0o600)
		throw new RestoreUnrecoverableError("restore_journal_invalid file mode or type");
	let value: unknown;
	try {
		value = JSON.parse(await Bun.file(path).text());
	} catch (error) {
		throw new RestoreUnrecoverableError(`restore_journal_invalid ${diagnostic(error)}`);
	}
	if (typeof value !== "object" || value === null) throw new RestoreUnrecoverableError("restore_journal_invalid shape");
	const input = value as Partial<RestoreJournal>;
	const id = input.id;
	if (input.version !== 1 || typeof id !== "string" || !/^[0-9a-f-]{36}$/.test(id))
		throw new RestoreUnrecoverableError("restore_journal_invalid identity");
	if (!isAbsolute(input.backup ?? "") || !/^[0-9a-f]{64}$/.test(input.backupSha256 ?? ""))
		throw new RestoreUnrecoverableError("restore_journal_invalid backup");
	if (input.staged !== join(home, stagedName(home, id)))
		throw new RestoreUnrecoverableError("restore_journal_invalid staged path");
	if (input.preservedDir !== join(backupsDir, `pre-restore-${id}`))
		throw new RestoreUnrecoverableError("restore_journal_invalid preserved path");
	if (!(input.phase === "preserving" || input.phase === "preserved" || input.phase === "installed"))
		throw new RestoreUnrecoverableError("restore_journal_invalid phase");
	const original = input.original as OriginalSet | undefined;
	const names = liveNames(home);
	if (!original || names.some((name) => !(name in original)))
		throw new RestoreUnrecoverableError("restore_journal_invalid original set");
	for (const name of names) {
		const identity = original[name];
		if (
			identity !== null &&
			(!identity || !Number.isSafeInteger(identity.dev) || !Number.isSafeInteger(identity.ino) || !Number.isSafeInteger(identity.size) || identity.size < 0)
		)
			throw new RestoreUnrecoverableError(`restore_journal_invalid original ${name}`);
	}
	return input as RestoreJournal;
}

async function writeRestoreJournal(path: string, home: string, journal: RestoreJournal): Promise<void> {
	const temporary = `${path}.tmp-${randomUUID()}`;
	try {
		await writeFile(temporary, `${JSON.stringify(journal)}\n`, { mode: 0o600 });
		await chmod(temporary, 0o600);
		await syncPath(temporary);
		await rename(temporary, path);
		await syncDirectory(home);
	} catch (error) {
		await unlink(temporary).catch(() => undefined);
		throw error;
	}
}

async function updateJournal(path: string, home: string, journal: RestoreJournal, phase: RestorePhase): Promise<RestoreJournal> {
	const updated: RestoreJournal = { ...journal, phase };
	await writeRestoreJournal(path, home, updated);
	return updated;
}

async function checkBackup(path: string, liveDb: string, backupsDir: string): Promise<{ path: string; sha256: string }> {
	if (!isAbsolute(path)) throw new RestoreRefusedError("restore requires an absolute backup path");
	const resolved = await realpath(path).catch(() => {
		throw new RestoreRefusedError(`backup_missing path=${path}`);
	});
	const info = await lstat(resolved);
	if (!info.isFile() || info.isSymbolicLink()) throw new RestoreRefusedError("backup_not_regular_file");
	const liveNames = [liveDb, `${liveDb}-wal`, `${liveDb}-shm`];
	const liveRealPaths = await Promise.all(liveNames.map(async (name) => realpath(name).catch(() => resolve(name))));
	if (liveRealPaths.includes(resolve(resolved))) throw new RestoreRefusedError("restore_source_is_live_database");
	const relativeToBackups = relative(await realpath(backupsDir), resolved);
	const firstPart = relativeToBackups.split(/[\\/]/)[0] ?? "";
	if (firstPart.startsWith("pre-restore-")) throw new RestoreRefusedError("restore_source_is_preserved_database");
	const sidecar = `${resolved}-wal`;
	if (await exists(sidecar)) {
		const sidecarInfo = await lstat(sidecar);
		if (!sidecarInfo.isFile() || sidecarInfo.size > 0) throw new RestoreRefusedError("backup_not_self_contained");
	}
	const db = await StoreCore.openReadonly(resolved).catch((error: unknown) => {
		throw new RestoreRefusedError(`backup_invalid ${diagnostic(error)}`);
	});
	try {
		const detail = db.integrityCheckDetail();
		if (detail !== "ok") throw new RestoreRefusedError(`backup_integrity_check_failed detail=${detail}`);
		if (db.schemaVersion > LATEST_SCHEMA_VERSION) throw new RestoreRefusedError(`backup_newer_schema version=${db.schemaVersion}`);
	} finally {
		db.close();
	}
	return { path: resolved, sha256: await sha256File(resolved) };
}

async function assertDeviceBoundary(home: string, backupsDir: string): Promise<void> {
	const [databaseDirectory, backupDirectory] = await Promise.all([stat(home), stat(backupsDir)]);
	if (databaseDirectory.dev !== backupDirectory.dev) throw new RestoreRefusedError("preserve_cross_device");
}

async function captureOriginal(home: string): Promise<OriginalSet> {
	const result = {} as Record<LiveName, FileIdentity | null>;
	try {
		for (const name of liveNames(home)) result[name] = await inspectRegular(join(home, name));
	} catch (error) {
		if (error instanceof RestoreUnrecoverableError)
			throw new RestoreRefusedError(`restore_live_entry_refused ${diagnostic(error)}`);
		throw error;
	}
	return result;
}

async function preservationComplete(home: string, journal: RestoreJournal): Promise<boolean> {
	for (const name of liveNames(home)) {
		const original = journal.original[name];
		const live = await inspectRegular(join(home, name));
		const preserved = await inspectRegular(join(journal.preservedDir, name));
		if (original === null) {
			if (live !== null || preserved !== null) return false;
		} else if (live !== null || !sameIdentity(original, preserved)) return false;
	}
	return true;
}

async function installCompleted(home: string, journal: RestoreJournal): Promise<boolean> {
	if (await exists(journal.staged)) return false;
	const [walName, shmName, dbName] = liveNames(home);
	const dbPath = join(home, dbName);
	const liveDb = await inspectRegular(dbPath);
	if (!liveDb || (await sha256File(dbPath)) !== journal.backupSha256 || isRecordedInOriginal(journal, liveDb))
		return false;
	for (const name of [walName, shmName]) {
		if (await inspectRegular(join(home, name))) return false;
	}
	for (const name of [walName, shmName, dbName]) {
		const original = journal.original[name];
		const live = await inspectRegular(join(home, name));
		const preserved = await inspectRegular(join(journal.preservedDir, name));
		if (name === dbName) {
			if (original === null ? preserved !== null : !sameIdentity(original, preserved)) return false;
			continue;
		}
		if (live !== null || (original === null ? preserved !== null : !sameIdentity(original, preserved))) return false;
	}
	return true;
}

async function validatePreservedContents(home: string, journal: RestoreJournal): Promise<void> {
	const info = await lstat(journal.preservedDir).catch((error: unknown) => {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	});
	if (!info) return;
	if (info.isSymbolicLink() || !info.isDirectory() || (info.mode & 0o777) !== 0o700)
		throw new RestoreUnrecoverableError("restore_anomaly preserved_directory_type_or_mode");
	const entries = await readdir(journal.preservedDir);
	const names = liveNames(home);
	if (entries.some((name) => !names.includes(name)))
		throw new RestoreUnrecoverableError(`restore_anomaly unexpected_preserved_entry=${entries.find((name) => !names.includes(name))}`);
}

async function classifyOriginals(home: string, journal: RestoreJournal): Promise<Record<LiveName, "live" | "preserved" | "absent">> {
	await validatePreservedContents(home, journal);
	const completedInstall = journal.phase !== "preserving" && await installCompleted(home, journal);
	const result = {} as Record<LiveName, "live" | "preserved" | "absent">;
	const names = liveNames(home);
	for (const name of names) {
		const original = journal.original[name];
		let live = await inspectRegular(join(home, name));
		const preserved = await inspectRegular(join(journal.preservedDir, name));
		if (name === databaseName(home) && completedInstall) live = null;
		if (original === null) {
			if (live !== null || preserved !== null) throw new RestoreUnrecoverableError(`restore_anomaly ${name}=unexpected`);
			result[name] = "absent";
		} else if (sameIdentity(original, live) && preserved === null) {
			result[name] = "live";
		} else if (live === null && sameIdentity(original, preserved)) {
			result[name] = "preserved";
		} else {
			throw new RestoreUnrecoverableError(`restore_anomaly ${name}=mismatch`);
		}
	}
	return result;
}

async function sidecarsAllowReadOnlyResume(home: string, journal: RestoreJournal): Promise<boolean> {
	const [walName, shmName] = liveNames(home);
	const wal = await inspectRegular(join(home, walName));
	const shm = await inspectRegular(join(home, shmName));
	if (wal === null && shm === null) return true;
	return (
		wal !== null &&
		wal.size === 0 &&
		shm !== null &&
		!isRecordedInOriginal(journal, wal) &&
		!isRecordedInOriginal(journal, shm)
	);
}

async function removeJournal(path: string, home: string): Promise<void> {
	await unlink(path);
	await syncDirectory(home);
}

async function rollback(journalPath: string, home: string, journal: RestoreJournal): Promise<void> {
	const states = await classifyOriginals(home, journal);
	if (journal.phase === "installed" || (journal.phase === "preserved" && await installCompleted(home, journal)))
		throw new RestoreUnrecoverableError("restore_abort_after_install_refused");
	for (const name of liveNames(home)) {
		if (states[name] !== "preserved") continue;
		const target = join(home, name);
		if (await exists(target)) throw new RestoreUnrecoverableError(`restore_rollback_live_name_occupied=${name}`);
		const source = join(journal.preservedDir, name);
		const identity = await inspectRegular(source);
		if (!sameIdentity(journal.original[name], identity))
			throw new RestoreUnrecoverableError(`restore_rollback_identity_mismatch=${name}`);
		await rename(source, target);
		await syncDirectory(home);
		await syncDirectory(journal.preservedDir);
	}
	for (const name of liveNames(home)) {
		const expected = journal.original[name];
		const actual = await inspectRegular(join(home, name));
		if (expected === null ? actual !== null : !sameIdentity(expected, actual))
			throw new RestoreUnrecoverableError(`restore_rollback_verification_failed=${name}`);
	}
	const stagedIdentity = await inspectRegular(journal.staged);
	if (stagedIdentity) await unlink(journal.staged);
	await rmdir(journal.preservedDir).catch((error: unknown) => {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	});
	await syncDirectory(homeLayout(home).backupsDir);
	await removeJournal(journalPath, home);
}

async function preserveOriginals(home: string, journalPath: string, initial: RestoreJournal): Promise<RestoreJournal> {
	let created = false;
	try {
		await mkdir(initial.preservedDir, { mode: 0o700 });
		created = true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
	}
	const directory = await lstat(initial.preservedDir);
	if (directory.isSymbolicLink() || !directory.isDirectory())
		throw new RestoreUnrecoverableError("restore_anomaly preserved_directory_type_or_mode");
	if (created) await chmod(initial.preservedDir, 0o700);
	else if ((directory.mode & 0o777) !== 0o700)
		throw new RestoreUnrecoverableError("restore_anomaly preserved_directory_type_or_mode");
	await validatePreservedContents(home, initial);
	await syncDirectory(homeLayout(home).backupsDir);
	const journal = initial;
	let states = await classifyOriginals(home, journal);
	const [walName, shmName, dbName] = liveNames(home);
	for (const name of [walName, shmName, dbName]) {
		if (states[name] !== "live") continue;
		const source = join(home, name);
		const target = join(journal.preservedDir, name);
		if (await exists(target)) throw new RestoreUnrecoverableError(`restore_preserve_target_occupied=${name}`);
		await rename(source, target);
		await syncDirectory(home);
		await syncDirectory(journal.preservedDir);
		fault(`after-preserve-${name === dbName ? "db" : name === walName ? "wal" : "shm"}`);
		states = await classifyOriginals(home, journal);
	}
	if (!(await preservationComplete(home, journal))) throw new RestoreUnrecoverableError("restore_preservation_boundary_failed");
	const preserved = await updateJournal(journalPath, home, journal, "preserved");
	fault("after-journal-preserved");
	return preserved;
}

async function installStaged(home: string, journalPath: string, journal: RestoreJournal): Promise<RestoreJournal> {
	if (!(await preservationComplete(home, journal))) throw new RestoreUnrecoverableError("restore_install_without_preservation_boundary");
	for (const name of liveNames(home)) {
		if (await exists(join(home, name))) throw new RestoreUnrecoverableError(`restore_install_live_name_occupied=${name}`);
	}
	const staged = await inspectRegular(journal.staged);
	if (!staged || (await sha256File(journal.staged)) !== journal.backupSha256)
		throw new RestoreUnrecoverableError("restore_staged_backup_invalid");
	await rename(journal.staged, homeLayout(home).db);
	await syncDirectory(home);
	fault("after-install-rename");
	fault("before-journal-installed");
	const installed = await updateJournal(journalPath, home, journal, "installed");
	fault("after-journal-installed");
	return installed;
}

async function postCheck(home: string, journalPath: string, journal: RestoreJournal, allowReadOnlySidecars: boolean): Promise<void> {
	const liveDb = homeLayout(home).db;
	const identity = await inspectRegular(liveDb);
	if (!identity || (await sha256File(liveDb)) !== journal.backupSha256)
		throw new RestoreUnrecoverableError("restore_post_check_backup_bytes_mismatch live_database");
	if (!(await sidecarsAllowReadOnlyResume(home, journal)) || (!allowReadOnlySidecars && ((await exists(`${liveDb}-wal`)) || (await exists(`${liveDb}-shm`)))))
		throw new RestoreUnrecoverableError("restore_post_check_live_sidecar_present");
	const database = await StoreCore.openReadonly(liveDb).catch((error: unknown) => {
		throw new RestoreUnrecoverableError(`restore_post_check_open_failed ${diagnostic(error)}`);
	});
	fault("after-post-check-open");
	try {
		const detail = database.integrityCheckDetail();
		if (detail !== "ok") throw new RestoreUnrecoverableError(`restore_post_check_integrity_failed detail=${detail}`);
		if (database.schemaVersion > LATEST_SCHEMA_VERSION)
			throw new RestoreUnrecoverableError(`restore_post_check_newer_schema version=${database.schemaVersion}`);
		console.log(`restore_post_check schema=${database.schemaVersion}`);
	} finally {
		database.close();
	}
	fault("before-journal-delete");
	await removeJournal(journalPath, home);
}

async function pruneUnjournaledStages(home: string): Promise<void> {
	for (const name of await readdir(home)) {
		if (isStagedName(home, name)) await unlink(join(home, name));
	}
}

async function startRestore(home: string, backupPath: string, journalPath: string, backupsDir: string): Promise<RestoreJournal> {
	const layout = homeLayout(home);
	await assertDeviceBoundary(home, backupsDir);
	const backup = await checkBackup(backupPath, layout.db, backupsDir);
	const id = randomUUID();
	const staged = join(home, stagedName(home, id));
	const preservedDir = join(backupsDir, `pre-restore-${id}`);
	const initial: RestoreJournal = {
		version: 1,
		id,
		backup: backup.path,
		backupSha256: backup.sha256,
		staged,
		preservedDir,
		phase: "preserving",
		original: Object.fromEntries(liveNames(home).map((name) => [name, null])),
	};
	try {
		await copyFile(backup.path, staged);
		await chmod(staged, 0o600);
		await syncPath(staged);
		if ((await sha256File(staged)) !== backup.sha256) throw new RestoreRefusedError("restore_staged_copy_mismatch");
		fault("after-staging");
		const original = await captureOriginal(home);
		const journal = { ...initial, original };
		await writeRestoreJournal(journalPath, home, journal);
		fault("after-journal-preserving");
		return journal;
	} catch (error) {
		if (!(await exists(journalPath))) await unlink(staged).catch(() => undefined);
		throw error;
	}
}

async function continueRestore(home: string, journalPath: string, journal: RestoreJournal): Promise<number> {
	try {
		let current = journal;
		if (current.phase === "preserving") {
			await classifyOriginals(home, current);
			const staged = await inspectRegular(current.staged);
			const stagedValid = staged !== null && (await sha256File(current.staged)) === current.backupSha256;
			if (!stagedValid) {
				await rollback(journalPath, home, current);
				console.error("restore_rolled_back staged_backup_unusable");
				return 3;
			}
			current = await preserveOriginals(home, journalPath, current);
		}
		if (current.phase === "preserved") {
			if (await installCompleted(home, current)) {
				fault("before-journal-installed");
				current = await updateJournal(journalPath, home, current, "installed");
				fault("after-journal-installed");
			} else {
				const staged = await inspectRegular(current.staged);
				const stagedValid = staged !== null && (await sha256File(current.staged)) === current.backupSha256;
				if (!stagedValid) {
					if (await inspectRegular(homeLayout(home).db) !== null)
						throw new RestoreUnrecoverableError("restore_anomaly staged_missing_live_database_present");
					await rollback(journalPath, home, current);
					console.error("restore_rolled_back staged_backup_unusable");
					return 3;
				}
				current = await installStaged(home, journalPath, current);
			}
		}
		if (current.phase !== "installed") throw new RestoreUnrecoverableError(`restore_non_final_phase=${current.phase}`);
		await postCheck(home, journalPath, current, journal.phase === "installed");
		console.log("restore_ok");
		return 0;
	} catch (error) {
		console.error(`restore_unrecoverable ${diagnostic(error)}`);
		return 4;
	}
}

export async function runRestore(
	backupPath?: string,
	options: { readonly home?: string; readonly abort?: boolean } = {},
): Promise<number> {
	const home = options.home ?? gatewayHome();
	const layout = homeLayout(home);
	const journalPath = join(home, "restore.journal.json");
	try {
		return await withGatewayLease(home, async () => {
			await mkdir(layout.backupsDir, { recursive: true, mode: 0o700 });
			const backupInfo = await lstat(layout.backupsDir);
			if (!backupInfo.isDirectory() || backupInfo.isSymbolicLink()) throw new RestoreRefusedError("backup_directory_invalid");
			await chmod(layout.backupsDir, 0o700);
			await assertDeviceBoundary(home, layout.backupsDir);
			if (await exists(journalPath)) {
				const journal = await readRestoreJournal(journalPath, home, layout.backupsDir);
				if (backupPath) {
					const supplied = await realpath(backupPath).catch(() => "");
					if (supplied !== journal.backup) throw new RestoreRefusedError("restore_journal_backup_mismatch");
				}
				if (options.abort) {
					if (journal.phase === "installed" || (journal.phase === "preserved" && await installCompleted(home, journal)))
						throw new RestoreRefusedError("restore_abort_after_install_refused");
					await rollback(journalPath, home, journal);
					console.error("restore_rolled_back");
					return 3;
				}
				return continueRestore(home, journalPath, journal);
			}
			if (options.abort) throw new RestoreRefusedError("restore_no_journal_to_abort");
			if (!backupPath) throw new RestoreRefusedError("restore_backup_required");
			await pruneUnjournaledStages(home);
			const journal = await startRestore(home, backupPath, journalPath, layout.backupsDir);
			return continueRestore(home, journalPath, journal);
		});
	} catch (error) {
		if (error instanceof LeaseHeldError || error instanceof RestoreRefusedError) {
			console.error(`restore_refused ${diagnostic(error)}`);
			return 1;
		}
		const code = error instanceof RestoreUnrecoverableError || (await exists(journalPath).catch(() => false)) ? 4 : 1;
		console.error(`${code === 4 ? "restore_unrecoverable" : "restore_refused"} ${diagnostic(error)}`);
		return code;
	}
}
