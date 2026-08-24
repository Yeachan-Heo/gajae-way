import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { assembleInjection } from "../../src/main-session/inject";
import { loadWayProfile } from "../../src/profile";

const directories: string[] = [];

afterEach(() => {
	for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

test("profile-parameterized injection preserves order, skips missing dailies, and honors session restrictions", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-inject-"));
	directories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(path.join(corpus, "daily"), { recursive: true });
	fs.mkdirSync(workspace);
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "soul");
	fs.writeFileSync(path.join(corpus, "USER.md"), "user");
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-03.md"), "today");
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-01.md"), "two-days-ago");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "memory");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]

[restricted_files]
conversation = ["MEMORY.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
session_kind = "main"
`,
	);
	const profile = loadWayProfile(profilePath);
	const logs: string[] = [];
	const main = assembleInjection(profile, {
		now: new Date(2026, 0, 3, 12),
		onLog: entry => logs.push(`${entry.kind}:${entry.path}`),
	});
	expect(main.map(file => file.path)).toEqual([
		"SOUL.md",
		"USER.md",
		"daily/2026-01-03.md",
		"daily/2026-01-01.md",
		"MEMORY.md",
	]);
	expect(logs).toEqual(["missing:daily/2026-01-02.md"]);
	const conversation = assembleInjection(profile, { sessionKind: "conversation", now: new Date(2026, 0, 3, 12) });
	expect(conversation.map(file => file.path)).not.toContain("MEMORY.md");
});
