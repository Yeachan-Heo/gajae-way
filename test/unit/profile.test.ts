import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadWayProfile, ProfileRevisionTracker, ProfileValidationError } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

function temporaryProfile(contents: string): { directory: string; profile: string; corpus: string; workspace: string } {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-"));
	temporaryDirectories.push(directory);
	const corpus = path.join(directory, "corpus");
	const workspace = path.join(directory, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profile = path.join(directory, "profile.toml");
	fs.writeFileSync(profile, contents.replaceAll("$CORPUS", corpus).replaceAll("$WORKSPACE", workspace));
	return { directory, profile, corpus, workspace };
}

function profileToml(extra = ""): string {
	return `[corpus]
path = "$CORPUS"
workspace = "$WORKSPACE"

[injection]
files = ["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]

[restricted_files]
conversation = ["MEMORY.md"]

[surfaces.owner]
id = "discord:owner"
platform = "discord"
kind = "dm"
session_kind = "main"

[operator]
id = "operator-1"
name = "Operator"

[poll]
interval_ms = 15000
${extra}`;
}

test("profile digest is canonical across TOML key order and excludes tunables", () => {
	const first = temporaryProfile(profileToml());
	const second = temporaryProfile(`[operator]
name = "Operator"
id = "operator-1"

[surfaces.owner]
kind = "dm"
platform = "discord"
id = "discord:owner"
session_kind = "main"

[injection]
files = ["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]

[corpus]
workspace = "$WORKSPACE"
path = "$CORPUS"

[restricted_files]
conversation = ["MEMORY.md"]

[poll]
interval_ms = 30000
`);
	fs.writeFileSync(
		second.profile,
		fs
			.readFileSync(second.profile, "utf8")
			.replaceAll(second.corpus, first.corpus)
			.replaceAll(second.workspace, first.workspace),
	);
	const left = loadWayProfile(first.profile);
	const right = loadWayProfile(second.profile);
	expect(left.digest).toEqual(right.digest);
	expect(left.projection.injectionFiles).toEqual(["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]);
});

test("profile digest changes for an identity/security injection-order change", () => {
	const fixture = temporaryProfile(profileToml());
	const before = loadWayProfile(fixture.profile);
	fs.writeFileSync(
		fixture.profile,
		profileToml()
			.replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"')
			.replaceAll("$CORPUS", fixture.corpus)
			.replaceAll("$WORKSPACE", fixture.workspace),
	);
	const after = loadWayProfile(fixture.profile);
	expect(after.digest.sha256).not.toBe(before.digest.sha256);
});

test("tunables have a separate hot-reload revision without changing the digest", () => {
	const fixture = temporaryProfile(profileToml());
	const tracker = new ProfileRevisionTracker();
	const before = tracker.load(fixture.profile);
	fs.writeFileSync(
		fixture.profile,
		profileToml("\n[ack]\nbudget = 12\n")
			.replaceAll("$CORPUS", fixture.corpus)
			.replaceAll("$WORKSPACE", fixture.workspace),
	);
	const after = tracker.load(fixture.profile);
	expect(before.digest.sha256).toBe(after.digest.sha256);
	expect(before.tunablesRevision).toBe(1);
	expect(after.tunablesRevision).toBe(2);
});

test("profile loader returns typed validation errors", () => {
	const fixture = temporaryProfile(
		profileToml().replace('files = ["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]', "files = 42"),
	);
	try {
		loadWayProfile(fixture.profile);
		throw new Error("expected profile loader to reject");
	} catch (error) {
		expect(error).toBeInstanceOf(ProfileValidationError);
		expect(error).toMatchObject({ code: "invalid_type", field: "injection.files" });
	}
});
