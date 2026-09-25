import { chmod, lstat, mkdtemp, readdir, rm } from "node:fs/promises";
import { join } from "node:path";

/**
 * Per-run scratch root for `bun test` (#188).
 *
 * Fixtures create temp dirs under `os.tmpdir()` and not all of them clean up;
 * a killed shard never does. Leaked into the shared `/tmp`, they pile up by the
 * hundred per run and outlive the process that owned them. Pointing TMPDIR at
 * one scratch dir per run bounds every leak to that dir, which is removed when
 * the run ends and swept on the next run if the owner was killed first.
 */
export const SCRATCH_PREFIX = "gw-test-";

export function createScratchTmp(root: string): Promise<string> {
	return mkdtemp(join(root, `${SCRATCH_PREFIX}${process.pid}-`));
}

/** Remove `dir` even when fixtures left read-only directories inside it. */
export async function removeScratchTmp(dir: string): Promise<void> {
	try {
		await rm(dir, { recursive: true, force: true });
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code !== "EACCES" && code !== "EPERM") throw error;
		await grantOwnerWrite(dir);
		await rm(dir, { recursive: true, force: true });
	}
}

async function grantOwnerWrite(dir: string): Promise<void> {
	await chmod(dir, 0o700);
	for (const entry of await readdir(dir, { withFileTypes: true })) {
		if (entry.isDirectory()) await grantOwnerWrite(join(dir, entry.name));
	}
}

function alive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

/** Remove scratch dirs under `root` whose owning run is no longer alive. */
export async function sweepDeadScratch(root: string): Promise<void> {
	for (const name of await readdir(root)) {
		const match = new RegExp(`^${SCRATCH_PREFIX}(\\d+)-`).exec(name);
		if (!match || alive(Number(match[1]))) continue;
		const dir = join(root, name);
		const info = await lstat(dir).catch(() => undefined);
		if (!info?.isDirectory() || info.uid !== process.getuid?.()) continue;
		await removeScratchTmp(dir).catch(() => {});
	}
}
