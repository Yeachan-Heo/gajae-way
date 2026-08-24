import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { RpcClient } from "../../src/rpc-client";
import { FakeBrokerFixture } from "../helpers/main-session";

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "gajaeway");
	if (!fs.existsSync(executable)) throw new Error("dist/gajaeway is missing; run bun scripts/compile.ts");
	return executable;
}

function profileToml(corpus: string, workspace: string, sessionId: string, writeEnabled: boolean): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[main_session]
session_id = "${sessionId}"

[tunables.mcp]
write_enabled = ${writeEnabled}

[surfaces.owner]
id = "mcp-owner"
platform = "test"
kind = "dm"
session_kind = "main"
`;
}

async function run(command: readonly string[], environment: NodeJS.ProcessEnv): Promise<void> {
	const proc = Bun.spawn({ cmd: [...command], cwd: repositoryRoot, env: environment, stdout: "pipe", stderr: "pipe" });
	const exitCode = await proc.exited;
	if (exitCode !== 0) {
		throw new Error(`${command.join(" ")} failed (${exitCode}): ${await new Response(proc.stderr).text()}`);
	}
}

async function connectHealthy(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		try {
			const client = await RpcClient.connect(socketPath);
			const health = (await client.request("way.health", {})).result as { state?: string };
			if (health?.state === "running") return client;
			client.close();
		} catch {
			// The daemon is still coming up.
		}
		await Bun.sleep(50);
	}
	throw new Error("compiled daemon never became healthy");
}

/**
 * Drives `gajaeway mcp serve` as a real subprocess over stdio against a real
 * compiled daemon. The in-process tests prove the dispatch logic; this proves
 * the shipped subcommand actually speaks the protocol and reaches the UDS.
 */
async function mcpSession(
	executable: string,
	state: string,
	profilePath: string,
	environment: NodeJS.ProcessEnv,
	requests: readonly unknown[],
): Promise<Array<Record<string, unknown>>> {
	const proc = Bun.spawn({
		cmd: [executable, "mcp", "serve", "--state-dir", state, "--profile", profilePath],
		cwd: repositoryRoot,
		env: environment,
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	proc.stdin.write(`${requests.map((request) => JSON.stringify(request)).join("\n")}\n`);
	await proc.stdin.end();
	const stdout = await new Response(proc.stdout).text();
	await proc.exited;
	return stdout
		.split("\n")
		.filter((line) => line.trim())
		.map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function withDaemon(
	writeEnabled: boolean,
	body: (context: {
		executable: string;
		state: string;
		profilePath: string;
		environment: NodeJS.ProcessEnv;
	}) => Promise<void>,
): Promise<void> {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-mcp-e2e-"));
	const corpus = path.join(root, "corpus");
	const state = path.join(root, "state");
	fs.mkdirSync(corpus, { recursive: true });
	// The broker fixture owns the workspace; the profile must agree with it or
	// bootstrap refuses the locator.
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileToml(corpus, fixture.workspace, fixture.sessionId, writeEnabled));
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	} as NodeJS.ProcessEnv;

	let daemon: { kill: () => void } | undefined;
	let client: RpcClient | undefined;
	try {
		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], environment);
		const spawned = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		daemon = spawned;
		client = await connectHealthy(path.join(state, "rpc.sock"));
		await body({ executable, state, profilePath, environment });
	} finally {
		client?.close();
		daemon?.kill();
		fs.rmSync(root, { recursive: true, force: true });
	}
}

test("mcp serve lists exactly the five read tools over stdio when write is disabled", async () => {
	await withDaemon(false, async ({ executable, state, profilePath, environment }) => {
		const replies = await mcpSession(executable, state, profilePath, environment, [
			{ jsonrpc: "2.0", id: 1, method: "initialize" },
			{ jsonrpc: "2.0", id: 2, method: "tools/list" },
		]);

		expect(replies).toHaveLength(2);
		const names = (replies[1]?.result as { tools: Array<{ name: string }> }).tools.map((tool) => tool.name);
		expect(names).toEqual(["way_status", "journal_read", "transcript_read", "registry_list", "schedule_list"]);
	});
}, 60_000);

test("mcp serve advertises main_submit only when the tunable enables it", async () => {
	await withDaemon(true, async ({ executable, state, profilePath, environment }) => {
		const replies = await mcpSession(executable, state, profilePath, environment, [
			{ jsonrpc: "2.0", id: 1, method: "tools/list" },
		]);
		const names = (replies[0]?.result as { tools: Array<{ name: string }> }).tools.map((tool) => tool.name);
		expect(names).toContain("main_submit");
		expect(names).toHaveLength(6);
	});
}, 60_000);

test("mcp serve answers way_status from the live daemon over stdio", async () => {
	await withDaemon(false, async ({ executable, state, profilePath, environment }) => {
		const replies = await mcpSession(executable, state, profilePath, environment, [
			{ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "way_status", arguments: {} } },
		]);
		const result = replies[0]?.result as { isError: boolean; content: Array<{ text: string }> };
		expect(result.isError).toBe(false);
		// A real daemon answered, not a stub.
		expect(result.content[0]?.text).toContain("healthy");
	});
}, 60_000);
