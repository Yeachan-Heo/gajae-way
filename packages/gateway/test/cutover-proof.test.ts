import { expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { parseConfigFile } from "../src/config";

const SOURCE_ROOT = join(import.meta.dir, "../src");

async function sourceFiles(directory: string): Promise<string[]> {
	const entries = await readdir(directory, { withFileTypes: true });
	const files: string[] = [];
	for (const entry of entries) {
		const path = join(directory, entry.name);
		if (entry.isDirectory()) files.push(...(await sourceFiles(path)));
		else if (entry.isFile() && path.endsWith(".ts")) files.push(path);
	}
	return files;
}

async function sourceSnapshot(): Promise<{ readonly files: readonly string[]; readonly text: string }> {
	const files = await sourceFiles(SOURCE_ROOT);
	return { files, text: (await Promise.all(files.map((path) => readFile(path, "utf8")))).join("\n") };
}

test("hard cutover leaves no resume argv, KeyedQueue subject, or turn-abort path", async () => {
	const { files, text } = await sourceSnapshot();
	const relativeFiles = files.map((path) => relative(SOURCE_ROOT, path));

	expect(relativeFiles).not.toContain("server/keyed-queue.ts");
	expect(text).not.toContain("keyed-queue");
	expect(text).not.toContain("--resume");
	expect(text).not.toContain("turn.abort");
	expect(text).not.toContain("turn.replace");
});

test("configuration has no turn-path coexistence switch and rejects the removed timeout", async () => {
	const config = await readFile(join(SOURCE_ROOT, "config.ts"), "utf8");

	expect(config).not.toMatch(
		/\b(?:legacyTurn|persistentTurn|turnPath|turnMode|turnTransport|useLegacyTurn|usePersistentTurn|turnPathFeatureFlag)\b/i,
	);
	for (const field of ["legacyTurn", "persistentTurn", "turnPath", "turnMode", "turnTransport"]) {
		const parsed = parseConfigFile({ schemaVersion: 1, [field]: true });
		expect(parsed).not.toHaveProperty(field);
	}
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 60_000 })).toThrow(
		"turnTimeoutMs was removed with persistent SDK sessions",
	);
});

test("persistent-session lifecycle logs retain their grep-stable formats", async () => {
	const { text } = await sourceSnapshot();
	for (const format of [
		"steer_delivered originKey=",
		"stall_alert originKey=",
		"compaction_event sessionId=",
		"broker_restart generation=",
		"retired_hold originKey=",
	])
		expect(text).toContain(format);
});
