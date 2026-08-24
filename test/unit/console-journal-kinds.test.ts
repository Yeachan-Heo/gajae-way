import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { GAJAEWAY_JOURNAL_EVENT_KINDS } from "../../src/console/console";
import { loadWayCore } from "../../src/native-loader";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

/**
 * `/journal all` sends no `kinds` filter, so `main.events.read` returns every
 * kind the daemon can emit and the console throws on any kind missing from its
 * allowlist. Adding a journal kind in Rust without updating the console
 * therefore breaks a first-party operator surface at runtime; this test turns
 * that drift into a build-time failure instead.
 */
test("the console renders every journal kind the daemon can emit", () => {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-journal-kinds-"));
	temporaryDirectories.push(stateDir);
	const core = loadWayCore().WayCore.open(stateDir);

	const daemonKinds = core.mainEventKinds();
	const renderable = new Set<string>(GAJAEWAY_JOURNAL_EVENT_KINDS);
	const unrenderable = daemonKinds.filter((kind) => !renderable.has(kind));

	expect(daemonKinds).toContain("schedule_run");
	expect(unrenderable).toEqual([]);
});
