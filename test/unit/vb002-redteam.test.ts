import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { assembleInjection, isRestricted, recallCandidates, restrictionMask } from "../../src/main-session/inject";
import { dispatchMcp, type McpRequest, mcpTools, mcpWriteEnabled } from "../../src/mcp/serve";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile, MAX_INJECTION_FILES, SESSION_KINDS } from "../../src/profile";
import { createRpcBridge } from "../../src/rpc-bridge";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";
import { RpcClient } from "../../src/rpc-client";

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");
const temporaryDirectories: string[] = [];
const runningRpc: WayCoreHandle[] = [];

afterAll(async () => {
	for (const core of runningRpc) {
		try {
			core.shutdownRpcServer();
		} catch {
			// Already stopped.
		}
	}
	await Bun.sleep(20);
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

async function connectRpc(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// Listener startup races are expected.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

function writeProfileIn(
	root: string,
	surfacesBlock: string,
	extra = "",
	injectionFiles = '["SOUL.md", "MEMORY.md"]',
): string {
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus, { recursive: true });
	fs.mkdirSync(workspace, { recursive: true });
	const profilePath = path.join(root, `profile-${fs.readdirSync(root).length}.toml`);
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ${injectionFiles}\n\n[restricted_files]\nconversation = ["MEMORY.md"]\nunknown = ["MEMORY.md", "SOUL.md"]\n\n${surfacesBlock}\n${extra}`,
	);
	return profilePath;
}

const OWNER = `[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n`;

function recallFixture(options: { unknownRestrictsAll?: boolean } = {}) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-recall-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "the operator prefers direct feedback about zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "a private zebra note that conversation surfaces must not see");
	const unknownBlock = options.unknownRestrictsAll === false ? "" : 'unknown = ["MEMORY.md", "SOUL.md"]\n';
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["MEMORY.md"]\n${unknownBlock}\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
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

async function withSearch(
	body: (client: RpcClient, ctx: ReturnType<typeof recallFixture>) => Promise<void>,
	options?: Parameters<typeof recallFixture>[0],
): Promise<void> {
	const ctx = recallFixture(options);
	runningRpc.push(ctx.core);
	const socketPath = path.join(ctx.stateDir, "rpc.sock");
	ctx.core.startRpcServer(socketPath, createRpcBridge(ctx.core));
	ctx.core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		await body(client, ctx);
	} finally {
		client.close();
	}
}

class FakeGateway implements JsonRpcClient {
	readonly calls: Array<{ method: string; params: unknown }> = [];
	close(): void {}
	async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
		this.calls.push({ method, params });
		return { jsonrpc: "2.0", id: 1, result: { ok: true, method } } as JsonRpcResponse;
	}
}

async function mcpCall(
	request: McpRequest,
	rpc: JsonRpcClient,
	writeEnabled: boolean,
): Promise<Record<string, unknown>> {
	return (await dispatchMcp(request, { rpc, writeEnabled, serverVersion: "test" })) as Record<string, unknown>;
}

test("crafted FTS5 queries cannot reach a masked document or leak it via snippet", async () => {
	await withSearch(async (client) => {
		for (const query of [
			'zebra" OR *',
			'zebra" OR body:"private',
			"zebra NEAR private",
			"*private",
			"body:private",
			"private OR zebra",
			'"',
			"*",
			"NEAR",
			"file_index:1",
		]) {
			const response = await client.request("memory.search", { query, session_kind: "conversation" });
			expect(response.error).toBeUndefined();
			const hits = (response.result as { hits: Array<{ path: string; snippet: string }> }).hits;
			for (const hit of hits) {
				expect(hit.path).not.toBe("MEMORY.md");
				expect(hit.snippet.toLowerCase()).not.toContain("private");
			}
		}
	});
});

test("cardinality and rank do not leak a restricted document's existence", async () => {
	await withSearch(async (client, ctx) => {
		const masked = await client.request("memory.search", { query: "zebra", session_kind: "conversation" });
		const maskedHits = (masked.result as { hits: Array<{ path: string; rank: number; snippet: string }> }).hits;
		expect(maskedHits.map((hit) => hit.path)).toEqual(["SOUL.md"]);
		expect(Object.keys(masked.result as object)).toEqual(["hits"]);

		const controlDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-control-"));
		temporaryDirectories.push(controlDir);
		const controlState = path.join(controlDir, "state");
		fs.mkdirSync(controlState);
		const control = loadWayCore().WayCore.open(controlState);
		control.gatewayMetaTransaction({
			expected: [],
			puts: [{ key: "profile_digest", value: ctx.profile.digest.sha256 }],
			deletes: [],
		});
		control.memoryIndexRebuild(
			ctx.profile.digest.sha256,
			[{ fileIndex: 0, path: "SOUL.md", body: fs.readFileSync(path.join(ctx.profile.corpusPath, "SOUL.md"), "utf8") }],
			SESSION_KINDS.map((sessionKind) => ({ sessionKind, mask: "0" })),
		);
		runningRpc.push(control);
		const controlSock = path.join(controlState, "rpc.sock");
		control.startRpcServer(controlSock, createRpcBridge(control));
		control.setRpcHealth("running");
		const controlClient = await connectRpc(controlSock);
		try {
			const controlResponse = await controlClient.request("memory.search", {
				query: "zebra",
				session_kind: "conversation",
			});
			const controlHits = (controlResponse.result as { hits: Array<{ path: string; rank: number; snippet: string }> })
				.hits;
			expect(controlHits).toHaveLength(maskedHits.length);
			expect(controlHits[0]?.path).toBe(maskedHits[0]?.path);
			expect(controlHits[0]?.snippet).toBe(maskedHits[0]?.snippet);
			// JSON-RPC round-trips f64, so bit-identical equality is not the contract.
			// A post-filter would shift bm25 by far more than this.
			expect(Math.abs((controlHits[0]?.rank ?? 0) - (maskedHits[0]?.rank ?? 0))).toBeLessThan(1e-6);
		} finally {
			controlClient.close();
		}
	});
});

test("empty, all-ones, and bit-63 masks over a 64-file corpus", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-64-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	const files = Array.from({ length: 64 }, (_, index) => `file-${index}.md`);
	for (const [index, file] of files.entries()) {
		const body = index === 63 ? "bit63secret unique token" : `common token file${index}`;
		fs.writeFileSync(path.join(corpus, file), body);
	}
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ${JSON.stringify(files)}\n\n[restricted_files]\nconversation = ["file-63.md"]\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	expect(profile.injection.files).toHaveLength(64);
	const conversationMask = BigInt(restrictionMask(profile, "conversation"));
	expect((conversationMask >> 63n) & 1n).toBe(1n);

	const core = loadWayCore().WayCore.open(stateDir);
	runningRpc.push(core);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
	core.memoryIndexRebuild(
		profile.digest.sha256,
		files.map((file, fileIndex) => ({
			fileIndex,
			path: file,
			body: fs.readFileSync(path.join(corpus, file), "utf8"),
		})),
		[
			{ sessionKind: "main", mask: "0" },
			{ sessionKind: "conversation", mask: restrictionMask(profile, "conversation") },
			{ sessionKind: "unknown", mask: ((1n << 64n) - 1n).toString() },
		],
	);
	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		const empty = await client.request("memory.search", { query: "bit63secret", session_kind: "main" });
		expect((empty.result as { hits: Array<{ path: string }> }).hits.map((hit) => hit.path)).toEqual(["file-63.md"]);

		const bit63 = await client.request("memory.search", { query: "bit63secret", session_kind: "conversation" });
		expect((bit63.result as { hits: unknown[] }).hits).toEqual([]);

		const allOnes = await client.request("memory.search", { query: "common", session_kind: "unknown" });
		expect((allOnes.result as { hits: unknown[] }).hits).toEqual([]);
	} finally {
		client.close();
	}
});

test("exactly 64 injection files are accepted and 65 are rejected", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-count-"));
	temporaryDirectories.push(root);
	const sixtyFour = JSON.stringify(Array.from({ length: MAX_INJECTION_FILES }, (_, index) => `file-${index}.md`));
	expect(() => loadWayProfile(writeProfileIn(root, OWNER, "", sixtyFour))).not.toThrow();
	const sixtyFive = JSON.stringify(Array.from({ length: MAX_INJECTION_FILES + 1 }, (_, index) => `file-${index}.md`));
	expect(() => loadWayProfile(writeProfileIn(root, OWNER, "", sixtyFive))).toThrow(/64/u);
});

test("a nonexistent or non-string session_kind does not become a permissive mask when unknown denies all", async () => {
	await withSearch(async (client) => {
		const missing = await client.request("memory.search", { query: "zebra" });
		expect((missing.result as { hits: Array<{ path: string }> }).hits).toEqual([]);

		const nonexistent = await client.request("memory.search", { query: "zebra", session_kind: "nonexistent" });
		expect((nonexistent.result as { hits: Array<{ path: string }> }).hits).toEqual([]);

		const numeric = await client.request("memory.search", { query: "zebra", session_kind: 123 });
		expect((numeric.result as { hits: Array<{ path: string }> }).hits).toEqual([]);

		const conversation = await client.request("memory.search", { query: "zebra", session_kind: "conversation" });
		expect((conversation.result as { hits: Array<{ path: string }> }).hits.map((hit) => hit.path)).toEqual(["SOUL.md"]);
	});
});

test("when unknown's deny list is empty, an unresolvable kind inherits that stored mask rather than u64::MAX", async () => {
	await withSearch(
		async (client) => {
			const nonexistent = await client.request("memory.search", { query: "zebra", session_kind: "nonexistent" });
			const hits = (nonexistent.result as { hits: Array<{ path: string }> }).hits.map((hit) => hit.path);
			expect(hits.sort()).toEqual(["MEMORY.md", "SOUL.md"]);
		},
		{ unknownRestrictsAll: false },
	);
});

test("rotating the digest refuses memory.search with 1801 rather than answering from the old mask", async () => {
	await withSearch(async (client, ctx) => {
		ctx.core.gatewayMetaTransaction({
			expected: [{ key: "profile_digest", value: ctx.profile.digest.sha256 }],
			puts: [{ key: "profile_digest", value: `${ctx.profile.digest.sha256}-rotated` }],
			deletes: [],
		});
		const response = await client.request("memory.search", { query: "zebra", session_kind: "conversation" });
		expect(response.error?.code).toBe(1801);
		expect(response.result).toBeUndefined();
	});
});

test("fail-closed refuses memory.search and no other path answers it", async () => {
	const ctx = recallFixture();
	runningRpc.push(ctx.core);
	const socketPath = path.join(ctx.stateDir, "rpc.sock");
	ctx.core.startRpcServer(socketPath, createRpcBridge(ctx.core));
	ctx.core.setRpcHealth("failed_closed", "profile_drift");
	const client = await connectRpc(socketPath);
	try {
		const response = await client.request("memory.search", { query: "zebra", session_kind: "main" });
		expect(response.error?.code).toBe(1000);
		expect(JSON.stringify(response.result ?? null)).not.toContain("zebra");
		const health = await client.request("way.health", {});
		expect(health.error).toBeUndefined();
	} finally {
		client.close();
	}
});

function writeDateProfile(): string {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-date-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "soul zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "memory zebra");
	fs.mkdirSync(path.join(corpus, "daily"));
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-03.md"), "daily zebra");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "daily/{date}.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["daily/{date}.md", "MEMORY.md"]\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	return profilePath;
}

test("{date} expansion is shared by the injector, the recall mask, and the index", () => {
	const profilePath = writeDateProfile();
	const profile = loadWayProfile(profilePath);
	const at = new Date(2026, 0, 3, 12);

	// A rule written against the raw template still matches the template itself.
	expect(isRestricted("daily/{date}.md", profile.restrictedFiles.conversation)).toBe(true);

	const injected = assembleInjection(profile, { sessionKind: "conversation", now: at });
	const candidates = recallCandidates(profile, at);

	// The index walks the SAME expanded list the injector walks, so a daily file
	// is recallable at all rather than silently absent.
	expect(candidates).toContain("daily/2026-01-03.md");
	expect(candidates.length).toBeGreaterThan(profile.injection.files.length);
	// A rule written as the raw template does NOT match an expanded candidate,
	// so the daily file is injected. That is the injector's existing semantics;
	// what matters is that the recall mask agrees with it exactly, rather than
	// the two disagreeing about which position is denied.
	expect(injected.map((file) => file.path)).toContain("daily/2026-01-03.md");
	const mask = BigInt(restrictionMask(profile, "conversation", at));
	candidates.forEach((file, index) => {
		expect(((mask >> BigInt(index)) & 1n) === 1n).toBe(isRestricted(file, profile.restrictedFiles.conversation));
	});
});

test("a known surface cannot gain admission without a digest change", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-digest-"));
	temporaryDirectories.push(root);
	fs.mkdirSync(path.join(root, "corpus"));
	fs.mkdirSync(path.join(root, "workspace"));
	const without = loadWayProfile(writeProfileIn(root, OWNER));
	const withKnown = loadWayProfile(
		writeProfileIn(
			root,
			`${OWNER}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
		),
	);
	expect(withKnown.digest.sha256).not.toBe(without.digest.sha256);
	expect(withKnown.projection.surfaceClassMapping.some((entry) => entry.id === "discord:guest")).toBe(true);

	const cased = loadWayProfile(
		writeProfileIn(
			root,
			`${OWNER}\n[[surfaces.known]]\nid = "Discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
		),
	);
	expect(cased.digest.sha256).not.toBe(withKnown.digest.sha256);

	const ownerShadow = loadWayProfile(
		writeProfileIn(
			root,
			`${OWNER}\n[[surfaces.known]]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n`,
		),
	);
	expect(ownerShadow.digest.sha256).toBe(without.digest.sha256);
	expect(ownerShadow.knownSurfaces).toHaveLength(1);
});

test("an owner surface cannot take a non-main class through the table form", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-owner-"));
	temporaryDirectories.push(root);
	expect(() =>
		loadWayProfile(
			writeProfileIn(
				root,
				`[owner_surfaces.primary]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "conversation"\n`,
			),
		),
	).toThrow(/main/u);
});

test("admission refuses a surface that is not in knownSurfaces", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-admit-"));
	temporaryDirectories.push(root);
	const stateDir = path.join(root, "state");
	fs.mkdirSync(stateDir);
	const profile = loadWayProfile(writeProfileIn(root, OWNER));
	const core = loadWayCore().WayCore.open(stateDir);
	const handler = createMainAdmissionHandler(
		{
			turnState: "idle",
			async admit() {},
		} as never,
		profile,
		core,
	);
	await expect(
		handler.submitFromRpc({ text: "x", surface_id: "discord:guest", idempotency_key: "k" }),
	).rejects.toMatchObject({ code: 1300 });
});

test("MCP tool table cannot be escaped by crafted names, prototype keys, or extra arguments", async () => {
	const gateway = new FakeGateway();
	for (const name of [
		"memory.search",
		"main.submit",
		"main_submit",
		"Main_submit",
		"MAIN_SUBMIT",
		"main-submit",
		"__proto__",
		"constructor",
		"",
	]) {
		const response = await mcpCall(
			{ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: { text: "hi" } } },
			gateway,
			false,
		);
		expect((response.error as { code: number }).code).toBe(-32602);
	}
	expect(gateway.calls).toEqual([]);

	const protoGateway = new FakeGateway();
	const proto = await mcpCall(
		{
			jsonrpc: "2.0",
			id: 2,
			method: "tools/call",
			params: {
				name: "way_status",
				arguments: JSON.parse('{"__proto__": {"name": "main_submit"}, "constructor": {}}'),
			},
		},
		protoGateway,
		false,
	);
	// Prototype-ish keys must not redirect the call onto another method. way_status
	// takes no arguments, so a polluted object either errors as unsupported or
	// still hits way.status with an empty params object.
	if (proto.error) {
		expect((proto.error as { code: number }).code).toBe(-32602);
		expect(protoGateway.calls).toEqual([]);
	} else {
		expect(protoGateway.calls).toEqual([{ method: "way.status", params: {} }]);
		expect(JSON.stringify(protoGateway.calls)).not.toContain("main.submit");
	}
});

test("main_submit cannot be invoked while write_enabled is false by any spelling", async () => {
	const gateway = new FakeGateway();
	for (const name of ["main_submit", "main.submit", "Main_submit", "MAIN_SUBMIT", "main-submit"]) {
		const response = await mcpCall(
			{
				jsonrpc: "2.0",
				id: 1,
				method: "tools/call",
				params: { name, arguments: { text: "hi", surface_id: "owner-dm", idempotency_key: "k" } },
			},
			gateway,
			false,
		);
		expect((response.error as { code: number }).code).toBe(-32602);
	}
	expect(gateway.calls).toEqual([]);
	expect(mcpTools(false).map((tool) => tool.name)).not.toContain("main_submit");
});

test("journal_read and transcript_read never emit a consumer_id even when one is supplied", async () => {
	const gateway = new FakeGateway();
	for (const name of ["journal_read", "transcript_read"] as const) {
		const response = await mcpCall(
			{
				jsonrpc: "2.0",
				id: 1,
				method: "tools/call",
				params: { name, arguments: { cursor: "1:0", consumer_id: "gajaeway-discord" } },
			},
			gateway,
			false,
		);
		expect((response.error as { code: number }).code).toBe(-32602);
	}
	expect(gateway.calls).toEqual([]);
});

test("write_enabled is only the boolean true; package.json has no dependencies and compile.ts adds no MCP target", () => {
	const make = (mcp: unknown) => ({ tunables: { tunables: mcp === undefined ? {} : { mcp } } }) as never;
	expect(mcpWriteEnabled(make({ write_enabled: true }))).toBe(true);
	for (const value of ["true", "TRUE", 1, "1", "yes", { enabled: true }]) {
		expect(mcpWriteEnabled(make({ write_enabled: value }))).toBe(false);
	}
	const manifest = JSON.parse(fs.readFileSync(path.join(repositoryRoot, "package.json"), "utf8")) as Record<
		string,
		unknown
	>;
	expect(manifest.dependencies).toBeUndefined();
	const compile = fs.readFileSync(path.join(repositoryRoot, "scripts/compile.ts"), "utf8");
	expect(compile).not.toMatch(/name: "gajaeway-mcp"/u);
	expect(compile).not.toMatch(/src\/mcp\//u);
});

test("computer-use and coding-agent surfaces are absent from this tree", () => {
	for (const relative of ["packages/coding-agent", "packages/computer-use", "src/computer-use", "src/desktop"]) {
		expect(fs.existsSync(path.join(repositoryRoot, relative))).toBe(false);
	}
});
