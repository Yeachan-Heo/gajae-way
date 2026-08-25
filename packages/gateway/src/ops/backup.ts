import { stat } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import type { GatewayDatabase } from "../store/db";

export async function backupDatabase(
	database: GatewayDatabase,
	liveDatabasePath: string,
	path: unknown,
): Promise<{ path: string; bytes: number }> {
	if (typeof path !== "string" || !isAbsolute(path)) throw new Error("ops.backup requires an absolute path");
	const target = resolve(path);
	if (target === resolve(liveDatabasePath)) throw new Error("ops.backup cannot overwrite the live database");
	let parent: Awaited<ReturnType<typeof stat>>;
	try {
		parent = await stat(dirname(target));
	} catch {
		throw new Error("ops.backup target parent directory does not exist");
	}
	if (!parent.isDirectory()) throw new Error("ops.backup target parent is not a directory");
	database.backupInto(target);
	return { path: target, bytes: (await stat(target)).size };
}

export function integrityDatabase(database: GatewayDatabase): { ok: boolean; detail: string } {
	const detail = database.integrityCheckDetail();
	return { ok: detail === "ok", detail };
}
