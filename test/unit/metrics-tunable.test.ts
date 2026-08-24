import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadWayProfile } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

function profileWith(extra: string) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-metrics-tunable-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n${extra}`,
	);
	return loadWayProfile(profilePath);
}

function httpEnabled(profile: ReturnType<typeof loadWayProfile>): boolean {
	const metrics = (profile.tunables.tunables as Record<string, unknown> | undefined)?.metrics as
		| Record<string, unknown>
		| undefined;
	return metrics?.http_enabled === true;
}

/**
 * The endpoint is unauthenticated, so absence of configuration must mean
 * absence of exposure. A default-on telemetry socket would be exposure by
 * omission.
 */
test("a profile with no metrics block leaves the endpoint disabled", () => {
	const profile = profileWith("");
	expect(httpEnabled(profile)).toBe(false);
});

test("an empty metrics block still leaves the endpoint disabled", () => {
	const profile = profileWith("\n[tunables.metrics]\nport = 9465\n");
	expect(httpEnabled(profile)).toBe(false);
});

test("the endpoint is enabled only by an explicit true", () => {
	expect(httpEnabled(profileWith("\n[tunables.metrics]\nhttp_enabled = true\n"))).toBe(true);
	// A truthy-looking string must not enable an unauthenticated surface.
	expect(httpEnabled(profileWith('\n[tunables.metrics]\nhttp_enabled = "true"\n'))).toBe(false);
});

/**
 * `[tunables.metrics]` must stay in the non-digest tunable class: enabling
 * telemetry is an operational decision, not a change to gateway identity, so it
 * must not require a profile-approval ceremony.
 */
test("changing the metrics tunable does not change the profile digest", () => {
	// Both profiles must share one corpus/workspace: those paths ARE digest-bound,
	// so varying them would prove nothing about the tunable.
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-metrics-digest-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const base = `[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`;

	const disabledPath = path.join(root, "disabled.toml");
	const enabledPath = path.join(root, "enabled.toml");
	fs.writeFileSync(disabledPath, `${base}\n[tunables.metrics]\nhttp_enabled = false\n`);
	fs.writeFileSync(enabledPath, `${base}\n[tunables.metrics]\nhttp_enabled = true\nport = 9999\n`);

	const disabled = loadWayProfile(disabledPath);
	const enabled = loadWayProfile(enabledPath);
	expect(httpEnabled(disabled)).toBe(false);
	expect(httpEnabled(enabled)).toBe(true);
	// Same identity, different tunables: no approval ceremony is required.
	expect(enabled.digest.sha256).toBe(disabled.digest.sha256);
});
