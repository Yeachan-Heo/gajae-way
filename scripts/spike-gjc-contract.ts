import { mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

type CommandResult = {
	args: string[];
	exitCode: number;
	stdout: string;
	stderr: string;
	elapsedMs: number;
};

const gjcPath = Bun.which("gjc");
if (!gjcPath) {
	throw new Error("gjc was not found on PATH");
}
const gjc: string = gjcPath;

const root = await mkdtemp(join(tmpdir(), "gajaeway-gjc-spike-"));
const sessions = join(root, "sessions");
const sdkAgent = join(root, "sdk-agent");
const repo = join(root, "repo");
await Promise.all([mkdir(sessions), mkdir(sdkAgent), mkdir(repo)]);

const sanitizedEnvironment = {
	PATH: process.env.PATH ?? "",
	HOME: process.env.HOME ?? root,
	TERM: "dumb",
	LANG: "C",
	LC_ALL: "C",
	// This is read-only configuration lookup. Session persistence is always
	// overridden by --session-dir below.
	GJC_CODING_AGENT_DIR: process.env.GJC_CODING_AGENT_DIR ?? join(process.env.HOME ?? root, ".gjc", "agent"),
};

async function run(args: string[], cwd = repo, env = sanitizedEnvironment): Promise<CommandResult> {
	const started = performance.now();
	const proc = Bun.spawn([gjc, ...args], {
		cwd,
		env,
		stdout: "pipe",
		stderr: "pipe",
	});
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);
	return { args, exitCode, stdout, stderr, elapsedMs: Math.round(performance.now() - started) };
}

function sessionId(output: string): string {
	for (const line of output.split("\n")) {
		if (!line) continue;
		const frame = JSON.parse(line) as { type?: string; id?: string };
		if (frame.type === "session" && typeof frame.id === "string") return frame.id;
	}
	throw new Error("JSON output did not contain a session frame with an id");
}

function compact(result: CommandResult): Record<string, unknown> {
	return {
		command: ["gjc", ...result.args].join(" "),
		exitCode: result.exitCode,
		elapsedMs: result.elapsedMs,
		stdout: result.stdout.slice(0, 700),
		stderr: result.stderr.slice(0, 700),
	};
}

try {
	const turnArgs = [
		"-p",
		"--mode",
		"json",
		"--no-tools",
		"--no-mcp",
		"--no-rules",
		"--no-lsp",
		"--session-dir",
		sessions,
	];
	const version = await run(["--version"]);
	const sdkHelp = await run(["sdk", "--help"]);
	const sdkSessionHelp = await run(["sdk", "session", "--help"]);
	const first = await run([...turnArgs, "say exactly: spike-alpha"]);
	const id = sessionId(first.stdout);
	const filesAfterFirst = await readdir(sessions);
	const resumed = await run(["--resume", id, ...turnArgs, "say exactly: spike-beta"]);
	const resumedId = sessionId(resumed.stdout);

	// Give the deliberately invalid resume an isolated agent directory too: GJC's
	// crash recorder must not write to the caller's normal agent directory.
	const bogus = await run(
		["--resume", "00000000-0000-0000-0000-000000000000", ...turnArgs, "say exactly: bogus"],
		repo,
		{ ...sanitizedEnvironment, GJC_CODING_AGENT_DIR: join(root, "bogus-agent") },
	);

	const continued = await run(["--continue", ...turnArgs, "say exactly: spike-continue"]);
	const continuedId = sessionId(continued.stdout);
	const sdkInput = JSON.stringify({ cwd: repo });
	const createBaseArgs = ["sdk", "session", "raw", "global", "--agent-dir", sdkAgent, "--op", "session.create"];
	const createArgs = [...createBaseArgs, "--idempotency-key", "spike-external-key", "--json-input", sdkInput];
	const created = await run(createArgs);
	const createdAgain = await run(createArgs);
	const concurrent = await Promise.all(
		Array.from({ length: 5 }, () =>
			run([...createBaseArgs, "--idempotency-key", "spike-concurrent-key", "--json-input", sdkInput]),
		),
	);
	const sdkIds = concurrent.map((result) => JSON.parse(result.stdout).result.sessionId as string);
	const oneShots = await Promise.all(
		Array.from({ length: 5 }, (_, index) => run([...turnArgs, `say exactly: parallel-${index + 1}`])),
	);
	const oneShotIds = oneShots.map((result) => sessionId(result.stdout));

	const evidence = {
		root: "<temporary directory removed after probe>",
		version: compact(version),
		sdkHelp: compact(sdkHelp),
		sdkSessionHelp: compact(sdkSessionHelp),
		first: { ...compact(first), sessionId: id, sessionFiles: filesAfterFirst },
		resume: { ...compact(resumed), sessionId: resumedId, sameSession: id === resumedId },
		bogusResume: compact(bogus),
		continue: { ...compact(continued), sessionId: continuedId, sameSession: id === continuedId },
		sdkCreate: { first: JSON.parse(created.stdout), second: JSON.parse(createdAgain.stdout) },
		sdkConcurrentCreate: {
			exitCodes: concurrent.map((result) => result.exitCode),
			sessionIds: sdkIds,
			uniqueSessionIds: [...new Set(sdkIds)].length,
		},
		parallelOneShots: {
			exitCodes: oneShots.map((result) => result.exitCode),
			sessionIds: oneShotIds,
			uniqueSessionIds: [...new Set(oneShotIds)].length,
		},
		latencyMs: { oneShot: first.elapsedMs, resumed: resumed.elapsedMs },
	};

	console.log(JSON.stringify(evidence, null, 2));
	if (id !== resumedId || id !== continuedId) throw new Error("resume or --continue forked the session");
	if (bogus.exitCode === 0) throw new Error("bogus resume silently succeeded");
	if (new Set(sdkIds).size !== 1 || concurrent.some((result) => result.exitCode !== 0))
		throw new Error("SDK create was not concurrent-idempotent");
	if (new Set(oneShotIds).size !== 5 || oneShots.some((result) => result.exitCode !== 0))
		throw new Error("parallel one-shots did not complete cleanly");
} finally {
	await rm(root, { recursive: true, force: true });
}
