import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, lstat, open, rename, stat, unlink } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { StoreCore } from "../store/core";

async function syncFile(path: string): Promise<void> {
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

async function assertAbsent(path: string): Promise<void> {
	try {
		await lstat(path);
		throw new Error(`backup_exists path=${path}`);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

async function fileSha256(path: string): Promise<string> {
	const hash = createHash("sha256");
	for await (const chunk of createReadStream(path)) hash.update(chunk);
	return hash.digest("hex");
}

export async function writeBackup(
	database: StoreCore,
	path: string,
): Promise<{ path: string; sha256: string; bytes: number }> {
	if (!isAbsolute(path)) throw new Error("backup target must be an absolute path");
	const target = resolve(path);
	const directory = dirname(target);
	const parent = await stat(directory).catch(() => undefined);
	if (!parent?.isDirectory()) throw new Error("backup target parent is not a directory");
	await assertAbsent(target);
	const temporary = `${target}.tmp-${randomUUID()}`;
	try {
		database.writeBackup(temporary);
		await chmod(temporary, 0o600);
		await syncFile(temporary);
		const check = await StoreCore.openReadonly(temporary);
		let detail: string;
		try {
			detail = check.quickCheckDetail();
		} finally {
			check.close();
		}
		if (detail !== "ok") throw new Error(`backup_integrity_check_failed detail=${detail}`);
		try {
			await lstat(`${temporary}-wal`);
			throw new Error("backup_not_self_contained: temporary backup has a -wal sidecar");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		await rename(temporary, target);
		await syncDirectory(directory);
		const info = await stat(target);
		return { path: target, sha256: await fileSha256(target), bytes: info.size };
	} catch (error) {
		await Promise.all([
			unlink(temporary).catch(() => undefined),
			unlink(`${temporary}-wal`).catch(() => undefined),
			unlink(`${temporary}-shm`).catch(() => undefined),
		]);
		throw error;
	}
}

export async function backupDatabase(
	database: StoreCore,
	liveDatabasePath: string,
	path: unknown,
): Promise<{ path: string; bytes: number }> {
	if (typeof path !== "string" || !isAbsolute(path)) throw new Error("ops.backup requires an absolute path");
	const target = resolve(path);
	if (target === resolve(liveDatabasePath)) throw new Error("ops.backup cannot overwrite the live database");
	const receipt = await writeBackup(database, target);
	return { path: receipt.path, bytes: receipt.bytes };
}

export function integrityDatabase(database: StoreCore): { ok: boolean; detail: string } {
	const detail = database.integrityCheckDetail();
	return { ok: detail === "ok", detail };
}
