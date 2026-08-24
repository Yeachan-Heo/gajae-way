import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { isRestricted, recallCandidates, restrictionMask } from "../../src/main-session/inject";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile, SESSION_KINDS } from "../../src/profile";
import { createRpcBridge } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

function fixture(injectionFiles = '["SOUL.md", "MEMORY.md"]') {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-recall-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "the operator prefers direct feedback about zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "a private zebra note that conversation surfaces must not see");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ${injectionFiles}\n\n[restricted_files]\nconversation = ["MEMORY.md"]\nunknown = ["MEMORY.md", "SOUL.md"]\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
	// A missing injection file contributes no recall document, exactly as the
	// daemon's index build treats it.
	const documents = profile.injection.files.flatMap((file, fileIndex) => {
		try {
			return [{ fileIndex, path: file, body: fs.readFileSync(path.resolve(profile.corpusPath, file), "utf8") }];
		} catch {
			return [];
		}
	});
	core.memoryIndexRebuild(
		profile.digest.sha256,
		documents,
		SESSION_KINDS.map((sessionKind) => ({ sessionKind, mask: restrictionMask(profile, sessionKind) })),
	);
	return { core, profile, stateDir };
}

/**
 * The mask must be derived from the injector's own predicate. A second
 * implementation would be free to drift, and a redaction drift is a disclosure.
 */
test("the recall mask is derived from the same predicate the injector uses", () => {
	const { profile } = fixture();

	// main denies nothing; conversation denies MEMORY.md (bit 1); unknown denies both.
	expect(restrictionMask(profile, "main")).toBe("0");
	expect(restrictionMask(profile, "conversation")).toBe("2");
	expect(restrictionMask(profile, "unknown")).toBe("3");

	// And the bits agree with isRestricted itself, file by file.
	for (const sessionKind of SESSION_KINDS) {
		const mask = BigInt(restrictionMask(profile, sessionKind));
		profile.injection.files.forEach((file, index) => {
			const bit = (mask >> BigInt(index)) & 1n;
			expect(bit === 1n).toBe(isRestricted(file, profile.restrictedFiles[sessionKind]));
		});
	}
});

test("a conversation surface gets conversation's deny list, not unknown's", () => {
	const { profile } = fixture();
	// Per-class granularity must match the injector: conversation still sees SOUL.md.
	expect(restrictionMask(profile, "conversation")).not.toBe(restrictionMask(profile, "unknown"));
	expect(isRestricted("SOUL.md", profile.restrictedFiles.conversation)).toBe(false);
	expect(isRestricted("SOUL.md", profile.restrictedFiles.unknown)).toBe(true);
});

test("more than 64 injection files is refused at load rather than truncated", () => {
	const many = JSON.stringify(Array.from({ length: 65 }, (_, index) => `file-${index}.md`));
	// Truncating would leave files past bit 63 permanently UNrestricted.
	expect(() => fixture(many)).toThrow(/64/u);
	const exactly64 = JSON.stringify(Array.from({ length: 64 }, (_, index) => `file-${index}.md`));
	expect(() => fixture(exactly64)).not.toThrow();
});

test("rebuilding the index appends memory_index_rebuilt to the journal", () => {
	const { core } = fixture();
	const kinds = core.journalRead("1:0", 100).events.map((event) => event.kind);
	expect(kinds).toContain("memory_index_rebuilt");
});

test("the mask is a decimal string so a 64-bit value survives the boundary", () => {
	const { profile } = fixture(JSON.stringify(Array.from({ length: 64 }, (_, index) => `file-${index}.md`)));
	const mask = restrictionMask(profile, "main");
	// A JS number would lose precision here; the string round-trips exactly.
	expect(typeof mask).toBe("string");
	expect(() => BigInt(mask)).not.toThrow();
});

test("FTS5 availability is probed so startup can fail closed on a named reason", () => {
	const { core } = fixture();
	// This build must have FTS5; if it ever does not, the daemon enters
	// failed-closed with memory_fts5_unavailable rather than degrading.
	expect(core.memoryFts5Available()).toBe(true);
});

/**
 * The stale-index refusal is asserted through `memory.search` ITSELF over a real
 * RPC server. A meta-only assertion would pass even if search happily answered
 * from the old mask, which is the failure that actually matters.
 */
test("a rotated digest makes memory.search refuse with 1801 instead of answering", async () => {
	const { core, profile, stateDir } = fixture();

	core.gatewayMetaTransaction({
		expected: [{ key: "profile_digest", value: profile.digest.sha256 }],
		puts: [{ key: "profile_digest", value: `${profile.digest.sha256}-rotated` }],
		deletes: [],
	});

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	const client = await RpcClient.connect(socketPath);
	try {
		const response = await client.request("memory.search", { query: "zebra", session_kind: "main" });
		expect(response.error?.code).toBe(1801);
		// Nothing from the stale index leaked alongside the refusal.
		expect(JSON.stringify(response.result ?? null)).not.toContain("zebra");
	} finally {
		client.close();
		core.shutdownRpcServer();
	}
});

/**
 * The `{date}` expansion must be shared by the injector, the mask, and the
 * index. Using raw templates would index nothing for a daily file and would let
 * a rule written against an expanded name silently miss.
 */
test("recall candidates expand {date} exactly as the injector does", () => {
	const { profile } = fixture('["SOUL.md", "daily/{date}.md", "MEMORY.md"]');
	const at = new Date("2026-08-24T12:00:00Z");
	const candidates = recallCandidates(profile, at);

	// Three daily candidates (today plus the prior two days), in order.
	expect(candidates).toHaveLength(5);
	expect(candidates[0]).toBe("SOUL.md");
	expect(candidates.filter((file) => file.startsWith("daily/"))).toHaveLength(3);
	expect(candidates.at(-1)).toBe("MEMORY.md");

	// The mask indexes THAT list, so MEMORY.md is the last bit, not bit 1.
	const mask = BigInt(restrictionMask(profile, "conversation", at));
	expect((mask >> BigInt(candidates.length - 1)) & 1n).toBe(1n);
	expect(mask & 1n).toBe(0n);
});
