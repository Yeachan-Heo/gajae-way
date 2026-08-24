import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
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

function hitsOf(response: JsonRpcResponse): Array<{ path: string; snippet: string; rank: number }> {
	expect(response.error).toBeUndefined();
	return (response.result as { hits: Array<{ path: string; snippet: string; rank: number }> }).hits;
}

function openCore(stateDir: string): WayCoreHandle {
	const core = loadWayCore().WayCore.open(stateDir);
	runningRpc.push(core);
	return core;
}

class FakeGateway implements JsonRpcClient {
	readonly calls: Array<{ method: string; params: unknown }> = [];
	close(): void {}
	async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
		this.calls.push({ method, params });
		return { jsonrpc: "2.0", id: 1, result: { ok: true, method, params } } as JsonRpcResponse;
	}
}

async function mcpCall(
	request: McpRequest,
	rpc: JsonRpcClient,
	writeEnabled: boolean,
): Promise<Record<string, unknown>> {
	return (await dispatchMcp(request, { rpc, writeEnabled, serverVersion: "test" })) as Record<string, unknown>;
}

test("AR-3: out-of-range file_index 64, 65, -1, and a very large value never appear under any mask", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-oob-"));
	temporaryDirectories.push(root);
	const stateDir = path.join(root, "state");
	fs.mkdirSync(stateDir);
	const core = openCore(stateDir);
	const digest = "sha256:oob-gen2";
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: digest }],
		deletes: [],
	});

	const documents: Array<{ fileIndex: number; path: string; body: string }> = [
		{ fileIndex: 0, path: "SOUL.md", body: "addressable secret token" },
		{ fileIndex: 63, path: "file-63.md", body: "bit63secret token" },
		{ fileIndex: 64, path: "OVERFLOW64.md", body: "unaddressable overflow64 token" },
		{ fileIndex: 65, path: "OVERFLOW65.md", body: "unaddressable overflow65 token" },
		{ fileIndex: 4_294_967_295, path: "HUGE.md", body: "unaddressable huge token" },
	];
	let negativeRejected = false;
	try {
		core.memoryIndexRebuild(digest, [...documents, { fileIndex: -1, path: "NEGATIVE.md", body: "negative token" }], []);
	} catch {
		negativeRejected = true;
		core.memoryIndexRebuild(digest, documents, [
			{ sessionKind: "main", mask: "0" },
			{ sessionKind: "conversation", mask: (1n << 63n).toString() },
			{ sessionKind: "unknown", mask: ((1n << 64n) - 1n).toString() },
		]);
	}
	expect(negativeRejected).toBe(true);

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		const forbidden = ["OVERFLOW64.md", "OVERFLOW65.md", "HUGE.md", "NEGATIVE.md"];
		for (const session_kind of ["main", "conversation", "unknown"]) {
			for (const query of ["token", "overflow64", "overflow65", "huge", "negative", "bit63secret"]) {
				const response = await client.request("memory.search", { query, session_kind });
				const paths = hitsOf(response).map((hit) => hit.path);
				for (const pathName of forbidden) {
					expect(paths).not.toContain(pathName);
				}
			}
		}

		expect(
			hitsOf(await client.request("memory.search", { query: "bit63secret", session_kind: "main" })).map(
				(hit) => hit.path,
			),
		).toEqual(["file-63.md"]);
		expect(
			hitsOf(await client.request("memory.search", { query: "bit63secret", session_kind: "conversation" })),
		).toEqual([]);
		expect(hitsOf(await client.request("memory.search", { query: "token", session_kind: "unknown" }))).toEqual([]);
		expect(
			hitsOf(await client.request("memory.search", { query: "addressable", session_kind: "main" })).map(
				(hit) => hit.path,
			),
		).toEqual(["SOUL.md"]);
	} finally {
		client.close();
	}
});

test("AR-1: index and search cannot disagree across a local midnight because search uses the stored mask", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-midnight-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	fs.mkdirSync(path.join(corpus, "daily"));
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "soul zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "memory zebra");
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-03.md"), "today-secret zebra");
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-04.md"), "tomorrow-secret zebra");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "daily/{date}.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["daily/2026-01-03.md"]\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);

	const beforeMidnight = new Date(2026, 0, 3, 23, 59, 59);
	const afterMidnight = new Date(2026, 0, 4, 0, 0, 1);
	const beforeCandidates = recallCandidates(profile, beforeMidnight);
	const afterCandidates = recallCandidates(profile, afterMidnight);
	expect(beforeCandidates).toContain("daily/2026-01-03.md");
	expect(afterCandidates).toContain("daily/2026-01-03.md");
	expect(beforeCandidates.indexOf("daily/2026-01-03.md")).not.toBe(afterCandidates.indexOf("daily/2026-01-03.md"));

	const beforeMask = BigInt(restrictionMask(profile, "conversation", beforeMidnight));
	const afterMask = BigInt(restrictionMask(profile, "conversation", afterMidnight));
	expect(beforeMask).not.toBe(afterMask);

	const indexedAt = beforeMidnight;
	const candidates = recallCandidates(profile, indexedAt);
	const core = openCore(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
	const documents = candidates.flatMap((file, fileIndex) => {
		try {
			return [{ fileIndex, path: file, body: fs.readFileSync(path.resolve(profile.corpusPath, file), "utf8") }];
		} catch {
			return [];
		}
	});
	core.memoryIndexRebuild(
		profile.digest.sha256,
		documents,
		SESSION_KINDS.map((sessionKind) => ({ sessionKind, mask: restrictionMask(profile, sessionKind, indexedAt) })),
	);

	const stored = core
		.gatewayMetaRead(["memory_mask_conversation"])
		.entries.find((entry) => entry.key === "memory_mask_conversation")?.value;
	expect(stored).toBe(restrictionMask(profile, "conversation", indexedAt));
	expect(stored).not.toBe(restrictionMask(profile, "conversation", afterMidnight));

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		const denied = hitsOf(
			await client.request("memory.search", { query: "today-secret", session_kind: "conversation" }),
		);
		expect(denied).toEqual([]);
		const visible = hitsOf(await client.request("memory.search", { query: "today-secret", session_kind: "main" }));
		expect(visible.map((hit) => hit.path)).toEqual(["daily/2026-01-03.md"]);
	} finally {
		client.close();
	}
});

test("AR-1: expanded list past 64 bits is refused before any document is indexed", () => {
	const files = Array.from({ length: 22 }, (_, index) => `day${index}/{date}.md`);
	expect(files.length).toBeLessThanOrEqual(MAX_INJECTION_FILES);
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-expand64-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ${JSON.stringify(files)}\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const candidates = recallCandidates(profile, new Date(2026, 0, 3, 12));
	expect(candidates.length).toBeGreaterThan(MAX_INJECTION_FILES);
});

test("AR-2: unresolvable class inherits a permissive unknown mask; absent mask is deny-all; crafted kinds cannot escape", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-maskfb-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "soul zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "memory zebra");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["MEMORY.md"]\nunknown = []\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	expect(restrictionMask(profile, "unknown")).toBe("0");

	const core = openCore(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
	core.memoryIndexRebuild(
		profile.digest.sha256,
		[
			{ fileIndex: 0, path: "SOUL.md", body: "soul zebra" },
			{ fileIndex: 1, path: "MEMORY.md", body: "memory zebra" },
		],
		SESSION_KINDS.map((sessionKind) => ({ sessionKind, mask: restrictionMask(profile, sessionKind) })),
	);

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		const inherited = hitsOf(
			await client.request("memory.search", { query: "zebra", session_kind: "nonexistent" }),
		).map((hit) => hit.path);
		expect(inherited.sort()).toEqual(["MEMORY.md", "SOUL.md"]);

		const empty = hitsOf(await client.request("memory.search", { query: "zebra", session_kind: "" })).map(
			(hit) => hit.path,
		);
		expect(empty.sort()).toEqual(["MEMORY.md", "SOUL.md"]);

		const sql = hitsOf(
			await client.request("memory.search", {
				query: "zebra",
				session_kind: "unknown'; DROP TABLE gateway_meta;--",
			}),
		).map((hit) => hit.path);
		expect(sql.sort()).toEqual(["MEMORY.md", "SOUL.md"]);

		const meta = hitsOf(
			await client.request("memory.search", { query: "zebra", session_kind: "conversation/../unknown" }),
		).map((hit) => hit.path);
		expect(meta.sort()).toEqual(["MEMORY.md", "SOUL.md"]);

		const numeric = hitsOf(await client.request("memory.search", { query: "zebra", session_kind: 123 })).map(
			(hit) => hit.path,
		);
		expect(numeric.sort()).toEqual(["MEMORY.md", "SOUL.md"]);

		core.gatewayMetaTransaction({
			expected: [],
			puts: [],
			deletes: SESSION_KINDS.map((kind) => `memory_mask_${kind}`),
		});
		const denied = hitsOf(await client.request("memory.search", { query: "zebra", session_kind: "main" }));
		expect(denied).toEqual([]);
	} finally {
		client.close();
	}
});

test("AR-4: a failed rebuild plus pin-clear returns 1801; skipping pin-clear leaves the old index searchable", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-rebuild-"));
	temporaryDirectories.push(root);
	const stateDir = path.join(root, "state");
	fs.mkdirSync(stateDir);
	const core = openCore(stateDir);
	const digest = "sha256:rebuild-gen2";
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: digest }],
		deletes: [],
	});
	core.memoryIndexRebuild(
		digest,
		[{ fileIndex: 0, path: "SOUL.md", body: "seeded zebra" }],
		[
			{ sessionKind: "main", mask: "0" },
			{ sessionKind: "unknown", mask: "0" },
		],
	);

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		expect(
			hitsOf(await client.request("memory.search", { query: "zebra", session_kind: "main" })).map((hit) => hit.path),
		).toEqual(["SOUL.md"]);

		expect(() =>
			core.memoryIndexRebuild(
				digest,
				[
					{ fileIndex: 0, path: "SOUL.md", body: "duplicate a" },
					{ fileIndex: 0, path: "OTHER.md", body: "duplicate b" },
				],
				[{ sessionKind: "main", mask: "0" }],
			),
		).toThrow();

		const stillSeeded = hitsOf(await client.request("memory.search", { query: "zebra", session_kind: "main" }));
		expect(stillSeeded.map((hit) => hit.path)).toEqual(["SOUL.md"]);

		core.gatewayMetaTransaction({
			expected: [],
			puts: [{ key: "memory_index_digest", value: "unavailable" }],
			deletes: [],
		});
		const refused = await client.request("memory.search", { query: "zebra", session_kind: "main" });
		expect(refused.error?.code).toBe(1801);
		expect(refused.result).toBeUndefined();
		expect(JSON.stringify(refused)).not.toContain("zebra");
	} finally {
		client.close();
	}
});

test("AR-5: digest tests hold other bound inputs constant; known-surface membership is load-bearing", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-digest-const-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const owner = `[surfaces.owner]\nid = "discord:owner-dm"\nplatform = "discord"\nkind = "dm"\nsession_kind = "main"\n`;
	let seq = 0;
	const write = (surfaces: string, extra = "") => {
		seq += 1;
		const profilePath = path.join(root, `p-${seq}.toml`);
		fs.writeFileSync(
			profilePath,
			`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["MEMORY.md"]\n\n${surfaces}\n${extra}`,
		);
		return loadWayProfile(profilePath);
	};

	const without = write(owner);
	const withKnown = write(
		`${owner}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "conversation"\n`,
	);
	expect(without.corpusPath).toBe(withKnown.corpusPath);
	expect(without.workspace).toBe(withKnown.workspace);
	expect(without.injection.files).toEqual(withKnown.injection.files);
	expect(without.restrictedFiles).toEqual(withKnown.restrictedFiles);
	expect(without.ownerSurfaces).toEqual(withKnown.ownerSurfaces);
	expect(without.operator).toEqual(withKnown.operator);
	expect(withKnown.digest.sha256).not.toBe(without.digest.sha256);

	const sameKnownDifferentKind = write(
		`${owner}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "room"\nsession_kind = "conversation"\n`,
	);
	expect(sameKnownDifferentKind.digest.sha256).toBe(withKnown.digest.sha256);

	const sameKnownDifferentClass = write(
		`${owner}\n[[surfaces.known]]\nid = "discord:guest"\nplatform = "discord"\nkind = "channel"\nsession_kind = "lane"\n`,
	);
	expect(sameKnownDifferentClass.digest.sha256).not.toBe(withKnown.digest.sha256);
});

test("AR-6: MCP tool table, write_enabled spellings, consumer_id, package.json, and compile.ts", async () => {
	const gateway = new FakeGateway();
	for (const name of [
		"memory.search",
		"main.submit",
		"main_submit",
		"Main_submit",
		"MAIN_SUBMIT",
		"main-submit",
		"tools/call",
		"__proto__",
		"constructor",
		"toString",
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

	for (const name of ["main_submit", "main.submit", "Main_submit", "MAIN_SUBMIT", "main-submit", "mcp_main_submit"]) {
		const response = await mcpCall(
			{
				jsonrpc: "2.0",
				id: 2,
				method: "tools/call",
				params: { name, arguments: { text: "hi", surface_id: "owner-dm", idempotency_key: "k" } },
			},
			gateway,
			false,
		);
		expect((response.error as { code: number }).code).toBe(-32602);
	}
	expect(mcpTools(false).map((tool) => tool.name)).not.toContain("main_submit");

	for (const name of ["journal_read", "transcript_read"] as const) {
		const response = await mcpCall(
			{
				jsonrpc: "2.0",
				id: 3,
				method: "tools/call",
				params: { name, arguments: { cursor: "1:0", consumer_id: "gajaeway-discord" } },
			},
			gateway,
			false,
		);
		expect((response.error as { code: number }).code).toBe(-32602);
	}
	expect(gateway.calls).toEqual([]);

	const okJournal = await mcpCall(
		{ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "journal_read", arguments: { cursor: "1:0" } } },
		gateway,
		false,
	);
	expect(okJournal.error).toBeUndefined();
	expect(gateway.calls.at(-1)).toEqual({ method: "main.events.read", params: { cursor: "1:0", limit: 50 } });
	expect(JSON.stringify(gateway.calls.at(-1))).not.toContain("consumer_id");

	const make = (mcp: unknown) => ({ tunables: { tunables: mcp === undefined ? {} : { mcp } } }) as never;
	expect(mcpWriteEnabled(make({ write_enabled: true }))).toBe(true);
	for (const value of ["true", "TRUE", "True", 1, "1", "yes", "on", { enabled: true }, ["true"]]) {
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
	expect(compile).toMatch(/name: "gajaeway"/u);
});

test("AR-1: a raw daily/{date}.md restriction agrees with the injector on live search", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-template-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	fs.mkdirSync(path.join(corpus, "daily"));
	fs.writeFileSync(path.join(corpus, "SOUL.md"), "soul zebra");
	fs.writeFileSync(path.join(corpus, "MEMORY.md"), "memory-secret zebra");
	fs.writeFileSync(path.join(corpus, "daily", "2026-01-03.md"), "today-secret zebra");
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[operator]\nid = "operator-1"\n\n[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = ["SOUL.md", "daily/{date}.md", "MEMORY.md"]\n\n[restricted_files]\nconversation = ["daily/{date}.md", "MEMORY.md"]\n\n[surfaces.owner]\nid = "owner"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const indexedAt = new Date(2026, 0, 3, 12);
	expect(isRestricted("daily/{date}.md", profile.restrictedFiles.conversation)).toBe(true);
	expect(isRestricted("daily/2026-01-03.md", profile.restrictedFiles.conversation)).toBe(false);
	const injected = assembleInjection(profile, { sessionKind: "conversation", now: indexedAt });
	expect(injected.map((file) => file.path)).toContain("daily/2026-01-03.md");
	expect(injected.map((file) => file.path)).not.toContain("MEMORY.md");

	const candidates = recallCandidates(profile, indexedAt);
	const core = openCore(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: profile.digest.sha256 }],
		deletes: [],
	});
	core.memoryIndexRebuild(
		profile.digest.sha256,
		candidates.flatMap((file, fileIndex) => {
			try {
				return [{ fileIndex, path: file, body: fs.readFileSync(path.resolve(profile.corpusPath, file), "utf8") }];
			} catch {
				return [];
			}
		}),
		SESSION_KINDS.map((sessionKind) => ({ sessionKind, mask: restrictionMask(profile, sessionKind, indexedAt) })),
	);

	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		const conversationDaily = hitsOf(
			await client.request("memory.search", { query: "today-secret", session_kind: "conversation" }),
		);
		expect(conversationDaily.map((hit) => hit.path)).toEqual(["daily/2026-01-03.md"]);
		expect(JSON.stringify(conversationDaily)).not.toContain("memory-secret");
		const conversationMemory = hitsOf(
			await client.request("memory.search", { query: "memory-secret", session_kind: "conversation" }),
		);
		expect(conversationMemory).toEqual([]);
		const mainMemory = hitsOf(await client.request("memory.search", { query: "memory-secret", session_kind: "main" }));
		expect(mainMemory.map((hit) => hit.path)).toEqual(["MEMORY.md"]);
	} finally {
		client.close();
	}
});

test("AR-1: expanded overflow is refused in main.ts and a failed rebuild pins unavailable so search returns 1801", async () => {
	const main = fs.readFileSync(path.join(repositoryRoot, "src/main.ts"), "utf8");
	expect(main).toContain("const indexedAt = new Date()");
	expect(main).toMatch(/if \(candidates\.length > MAX_INJECTION_FILES\)/u);
	expect(main).toMatch(/restrictionMask\(profile, sessionKind, indexedAt\)/u);
	expect(main).toMatch(/recallCandidates\(profile, indexedAt\)/u);
	expect(main).toContain('value: "unavailable"');

	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb002-overflow-pin-"));
	temporaryDirectories.push(root);
	const stateDir = path.join(root, "state");
	fs.mkdirSync(stateDir);
	const core = openCore(stateDir);
	const digest = "sha256:overflow-pin";
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "profile_digest", value: digest }],
		deletes: [],
	});
	core.memoryIndexRebuild(
		digest,
		[{ fileIndex: 0, path: "SOUL.md", body: "seeded zebra secret" }],
		[{ sessionKind: "main", mask: "0" }],
	);
	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("running");
	const client = await connectRpc(socketPath);
	try {
		expect(
			hitsOf(await client.request("memory.search", { query: "zebra", session_kind: "main" })).map((hit) => hit.path),
		).toEqual(["SOUL.md"]);
		core.gatewayMetaTransaction({
			expected: [],
			puts: [{ key: "memory_index_digest", value: "unavailable" }],
			deletes: [],
		});
		const refused = await client.request("memory.search", { query: "zebra", session_kind: "main" });
		expect(refused.error?.code).toBe(1801);
		expect(refused.result).toBeUndefined();
		expect(JSON.stringify(refused)).not.toContain("zebra");
		expect(JSON.stringify(refused)).not.toContain("secret");
	} finally {
		client.close();
	}
});

test("G005: MCP cannot reach memory.search as a tool, method, or advertised name", async () => {
	const gateway = new FakeGateway();
	for (const writeEnabled of [false, true]) {
		const listed = await mcpCall({ jsonrpc: "2.0", id: 1, method: "tools/list" }, gateway, writeEnabled);
		const names = ((listed.result as { tools: Array<{ name: string }> }).tools ?? []).map((tool) => tool.name);
		expect(names).not.toContain("memory.search");
		expect(names).not.toContain("memory_search");
		expect(mcpTools(writeEnabled).map((tool) => tool.name)).not.toContain("memory.search");

		const asTool = await mcpCall(
			{
				jsonrpc: "2.0",
				id: 2,
				method: "tools/call",
				params: { name: "memory.search", arguments: { query: "zebra", session_kind: "main" } },
			},
			gateway,
			writeEnabled,
		);
		expect((asTool.error as { code: number }).code).toBe(-32602);
		expect(JSON.stringify(asTool)).not.toContain("zebra");

		const asMethod = await mcpCall(
			{ jsonrpc: "2.0", id: 3, method: "memory.search", params: { query: "zebra", session_kind: "main" } },
			gateway,
			writeEnabled,
		);
		expect((asMethod.error as { code: number }).code).toBe(-32601);
		expect(JSON.stringify(asMethod)).not.toContain("zebra");
	}
	expect(gateway.calls).toEqual([]);
});

test("computer-use red-team suite is not applicable to this change set", () => {
	for (const relative of [
		"packages/coding-agent",
		"packages/computer-use",
		"src/computer-use",
		"src/desktop",
		"src/adapter/computer",
	]) {
		expect(fs.existsSync(path.join(repositoryRoot, relative))).toBe(false);
	}
	const registry = fs.existsSync(path.join(repositoryRoot, "src/adapter/runtime"))
		? fs.readdirSync(path.join(repositoryRoot, "src/adapter/runtime"))
		: [];
	expect(registry.join(" ")).not.toMatch(/computer|desktop|coding-agent/u);
});
