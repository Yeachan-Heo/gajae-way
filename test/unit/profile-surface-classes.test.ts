import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadWayProfile } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

function writeProfile(surfacesBlock: string): string {
	return writeProfileIn(sharedRoot(), surfacesBlock);
}

/**
 * A digest comparison MUST hold the corpus and workspace paths constant: those
 * are themselves digest-bound, so writing each profile into a fresh temp dir
 * would make any two digests differ and prove nothing.
 */
let cachedRoot: string | undefined;
function sharedRoot(): string {
	if (cachedRoot) return cachedRoot;
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-surface-class-"));
	temporaryDirectories.push(root);
	fs.mkdirSync(path.join(root, "corpus"));
	fs.mkdirSync(path.join(root, "workspace"));
	cachedRoot = root;
	return root;
}

let profileSeq = 0;
function writeProfileIn(root: string, surfacesBlock: string): string {
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	profileSeq += 1;
	const profilePath = path.join(root, `profile-${profileSeq}.toml`);
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["MEMORY.md"]\n\n${surfacesBlock}`,
	);
	return profilePath;
}

const OWNER = `[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n`;

test("a surface without session_kind fails to load", () => {
	const profilePath = writeProfile(`[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\n`);
	// No inferred default: a wrong default would hand a surface the wrong deny list.
	expect(() => loadWayProfile(profilePath)).toThrow(/session_kind/u);
});

test("a session_kind outside SESSION_KINDS fails to load", () => {
	const profilePath = writeProfile(
		`[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "supervisor"\n`,
	);
	expect(() => loadWayProfile(profilePath)).toThrow(/session_kind/u);
});

test("an owner surface declaring anything but main fails to load", () => {
	const profilePath = writeProfile(
		`[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "conversation"\n`,
	);
	// An owner surface IS the main session; another class would give the main
	// transcript a non-main deny list.
	expect(() => loadWayProfile(profilePath)).toThrow(/main/u);
});

test("a known surface shadowing an owner with a different session_kind fails to load", () => {
	const profilePath = writeProfile(
		`${OWNER}\n[[surfaces.known]]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "conversation"\n`,
	);
	expect(() => loadWayProfile(profilePath)).toThrow(/conflicts/u);
});

/**
 * A4-1: the table form rebuilds the record field by field, so an omitted
 * forward silently drops a declared class rather than failing loudly.
 */
test("session_kind survives the owner_surfaces table-form reconstruction", () => {
	const profilePath = writeProfile(
		`[owner_surfaces.primary]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	expect(profile.ownerSurfaces).toEqual([
		{ id: "discord:owner-dm", platform: "discord", kind: "dm", sessionKind: "main" },
	]);
});

test("the table form still rejects a missing session_kind rather than defaulting", () => {
	const profilePath = writeProfile(
		`[owner_surfaces.primary]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\n`,
	);
	expect(() => loadWayProfile(profilePath)).toThrow(/session_kind/u);
});

/**
 * The authority hole: adding a `[[surfaces.known]]` entry granted follow-up
 * admission authority with no `profile approve` ceremony, because known-surface
 * membership was absent from the digest-bound projection. This test fails on
 * the pre-change tree and is the proof the hole is closed.
 */
test("adding a known surface CHANGES the profile digest", () => {
	const withoutKnown = loadWayProfile(writeProfile(OWNER));
	const withKnown = loadWayProfile(
		writeProfile(
			`${OWNER}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
		),
	);

	expect(withKnown.digest.sha256).not.toBe(withoutKnown.digest.sha256);
	expect(withKnown.digest.version).toBe(3);
});

test("changing only a known surface's session_kind changes the digest", () => {
	const asConversation = loadWayProfile(
		writeProfile(
			`${OWNER}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
		),
	);
	const asLane = loadWayProfile(
		writeProfile(
			`${OWNER}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "lane"\n`,
		),
	);
	// The redaction class is security-relevant, so it must be digest-bound.
	expect(asLane.digest.sha256).not.toBe(asConversation.digest.sha256);
});

/**
 * `kind` is the free-form platform kind and must never be consulted for
 * redaction. Two profiles differing only in `kind` still carry the same class.
 */
test("the free-form platform kind is not the redaction class", () => {
	const profile = loadWayProfile(
		writeProfile(
			`${OWNER}\n[[surfaces.known]]\nid = "slack:room"\nplatform = "slack"\nkind = "conversation"\nsession_kind = "lane"\n`,
		),
	);
	const known = profile.knownSurfaces.find((surface) => surface.id === "slack:room");
	// `kind` says "conversation" but the declared class is "lane".
	expect(known?.kind).toBe("conversation");
	expect(known?.sessionKind).toBe("lane");
	const mapping = profile.projection.surfaceClassMapping.find((entry) => entry.id === "slack:room");
	expect(mapping?.sessionKind).toBe("lane");
});

test("the surface class mapping covers every declared surface, id-sorted", () => {
	const profile = loadWayProfile(
		writeProfile(
			`${OWNER}\n[[surfaces.known]]\nid = "zz:last"\nplatform = "discord"\nkind = "channel"\nsession_kind = "job"\n\n[[surfaces.known]]\nid = "aa:first"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
		),
	);
	expect(profile.projection.surfaceClassMapping).toEqual([
		{ id: "aa:first", sessionKind: "conversation" },
		{ id: "discord:owner-dm", sessionKind: "main" },
		{ id: "zz:last", sessionKind: "job" },
	]);
});
