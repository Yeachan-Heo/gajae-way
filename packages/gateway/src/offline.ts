import { lstat } from "node:fs/promises";
import { join } from "node:path";
import { acquireLease, type Lease } from "@gajae-gateway/home";

export class RestoreIncompleteError extends Error {
	readonly code = "restore_incomplete";

	constructor(home: string) {
		super(`restore_incomplete journal=${join(home, "restore.journal.json")}`);
		this.name = "RestoreIncompleteError";
	}
}

export async function assertNoRestoreJournal(home: string): Promise<void> {
	try {
		await lstat(join(home, "restore.journal.json"));
		throw new RestoreIncompleteError(home);
	} catch (error) {
		if (error instanceof RestoreIncompleteError) throw error;
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

/** Runs an offline operation under the daemon's exclusive lease without opening its database. */
export async function withGatewayLease<T>(home: string, fn: () => Promise<T>): Promise<T> {
	const lease: Lease = await acquireLease(home, "gateway");
	try {
		return await fn();
	} finally {
		await lease.release();
	}
}
