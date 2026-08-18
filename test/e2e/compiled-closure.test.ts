import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { RpcClient } from "../../src/rpc-client";

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

async function run(command: readonly string[], cwd = repositoryRoot): Promise<string> {
	const child = Bun.spawn({ cmd: [...command], cwd, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	if (exitCode !== 0) throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
	return stdout;
}

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "way");
	if (!fs.existsSync(executable))
		throw new Error("The compiled closure drill requires dist/way. Run bun scripts/compile.ts before bun test.");
	return executable;
}

function profile(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "closure-owner"
platform = "test"
kind = "dm"
`;
}

async function connectHealthy(socketPath: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 200; attempt += 1) {
		let client: RpcClient | undefined;
		try {
			client = await RpcClient.connect(socketPath);
			const health = await client.request("way.health", {}, { timeoutMs: 1_000 });
			if ((health.result as { status?: string } | undefined)?.status === "healthy") return client;
			client.close();
		} catch (error) {
			client?.close();
			lastError = error;
		}
		await Bun.sleep(25);
	}
	throw new Error(
		`compiled daemon did not become healthy${lastError instanceof Error ? `: ${lastError.message}` : ""}`,
	);
}

test("compiled daemon routes main.corpus.close through the supervised closure executor", async () => {
	const executable = compiledWay();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-compiled-closure-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const workspace = path.join(root, "workspace");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, profile(corpus, workspace));
	const environment = {
		...process.env,
		NODE_ENV: "test",
		WAY_E2E_FILE_SDK: "1",
		WAY_BROKER_CLI: "/usr/bin/false",
		WAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: ReturnType<typeof Bun.spawn> | undefined;
	let client: RpcClient | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Compiled Closure Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "compiled-closure@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		const unavailableProbe = Bun.spawn({
			cmd: [executable, "--health", "--state-dir", state],
			cwd: repositoryRoot,
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [unavailableExitCode, unavailableOutput] = await Promise.all([
			unavailableProbe.exited,
			new Response(unavailableProbe.stdout).text(),
		]);
		expect(unavailableExitCode).toBe(1);
		expect(JSON.parse(unavailableOutput)).toMatchObject({
			status: "unhealthy",
			state: "unavailable",
			reason: "daemon_unreachable",
		});

		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath]);
		fs.writeFileSync(path.join(corpus, "compiled-close.txt"), "closed by compiled daemon\n");
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: environment,
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(path.join(state, "rpc.sock"));
		const cliHealth = JSON.parse(await run([executable, "--health", "--state-dir", state], repositoryRoot));
		expect(cliHealth).toMatchObject({ status: "healthy", state: "running", main: { resumed: true } });
		const response = await client.request("main.corpus.close", {
			paths: ["compiled-close.txt"],
			commit_message: "compiled daemon closure",
		});
		expect(response.result).toMatchObject({ committed: true });
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:compiled-close.txt"])).toBe(
			"closed by compiled daemon\n",
		);
		const subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "compiled daemon closure")).toHaveLength(1);
	} finally {
		client?.close();
		if (daemon?.exitCode === null) daemon.kill("SIGTERM");
		if (daemon) await daemon.exited;
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 30_000);
