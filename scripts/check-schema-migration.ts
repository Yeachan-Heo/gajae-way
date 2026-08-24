/**
 * Enforces the serialized-migration constraint.
 *
 * `crates/way-core/src/store.rs` migrates with a linear chain of
 * `if current_version < N` blocks, and `migrate` rejects a database whose
 * recorded version exceeds `SCHEMA_VERSION`. A database that reached version
 * N+1 without ever executing the `< N` block therefore never gets that block's
 * tables: independently-guarded integer slots do NOT backfill a skipped
 * version. Two branches each claiming "the next version" is a silent
 * missing-table data-loss bug, not a merge conflict.
 *
 * So this check asserts, against the merge base:
 *   1. `SCHEMA_VERSION` advances by at most 1, and
 *   2. every newly added `if current_version < N` block has an `N` that is
 *      reachable, i.e. no block is added for a version above the new
 *      `SCHEMA_VERSION`, and
 *   3. when `SCHEMA_VERSION` advances, a guarded block for exactly that value
 *      exists.
 */

const STORE_PATH = "crates/way-core/src/store.rs";

function schemaVersionOf(source: string, label: string): number {
	const match = /pub const SCHEMA_VERSION: u32 = (\d+);/u.exec(source);
	if (!match) throw new Error(`could not find SCHEMA_VERSION in ${label}`);
	return Number(match[1]);
}

function guardedVersions(source: string): number[] {
	return [...source.matchAll(/if current_version < (\d+)/gu)].map((match) => Number(match[1])).sort((a, b) => a - b);
}

async function gitShow(ref: string, path: string): Promise<string> {
	const result = Bun.spawnSync(["git", "show", `${ref}:${path}`]);
	if (result.exitCode !== 0) throw new Error(`git show ${ref}:${path} failed: ${result.stderr.toString()}`);
	return result.stdout.toString();
}

function mergeBase(): string {
	const explicit = Bun.env.GAJAEWAY_MERGE_BASE?.trim();
	if (explicit) return explicit;
	for (const candidate of ["origin/main", "main"]) {
		const result = Bun.spawnSync(["git", "merge-base", "HEAD", candidate]);
		if (result.exitCode === 0) return result.stdout.toString().trim();
	}
	throw new Error("could not resolve a merge base; set GAJAEWAY_MERGE_BASE");
}

const base = mergeBase();
const baseSource = await gitShow(base, STORE_PATH);
const headSource = await Bun.file(STORE_PATH).text();

const baseVersion = schemaVersionOf(baseSource, `${base}:${STORE_PATH}`);
const headVersion = schemaVersionOf(headSource, STORE_PATH);
const failures: string[] = [];

if (headVersion < baseVersion) {
	failures.push(`SCHEMA_VERSION moved backwards: ${baseVersion} -> ${headVersion}.`);
}
if (headVersion - baseVersion > 1) {
	failures.push(
		`SCHEMA_VERSION advanced by ${headVersion - baseVersion} (${baseVersion} -> ${headVersion}). ` +
			"Migrations must serialize one version per change, because a database that skips a version never executes its guarded block.",
	);
}

const headGuards = guardedVersions(headSource);
const unreachable = headGuards.filter((version) => version > headVersion);
if (unreachable.length > 0) {
	failures.push(
		`migration blocks guard version(s) ${unreachable.join(", ")} above SCHEMA_VERSION ${headVersion}; ` +
			"such a block can never run on a database that reaches the higher version first.",
	);
}

if (headVersion > baseVersion && !headGuards.includes(headVersion)) {
	failures.push(
		`SCHEMA_VERSION is ${headVersion} but no \`if current_version < ${headVersion}\` block exists, ` +
			"so a fresh or upgrading database would skip this version's schema.",
	);
}

const baseGuards = new Set(guardedVersions(baseSource));
const added = headGuards.filter((version) => !baseGuards.has(version));
if (added.length > 1) {
	failures.push(`this change adds ${added.length} migration blocks (${added.join(", ")}); add exactly one at a time.`);
}

if (failures.length > 0) {
	console.error("Schema migration check failed:");
	for (const failure of failures) console.error(`  - ${failure}`);
	process.exit(1);
}

console.log(
	`Schema migration check passed: SCHEMA_VERSION ${baseVersion} -> ${headVersion} (merge base ${base.slice(0, 12)}), ` +
		`guarded versions ${headGuards.join(", ")}.`,
);
