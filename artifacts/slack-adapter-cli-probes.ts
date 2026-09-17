import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const home = await mkdtemp(join(tmpdir(), "slack-cli-redteam-"));
const probes: unknown[] = [];
try {
	for (const [id, args, expectedExitCode] of [
		["RT-SLACK-26", ["--help"], 0],
		["RT-SLACK-27", ["--version"], 0],
		["RT-SLACK-28", ["--nope"], 2],
		["RT-SLACK-29", [], 2],
	] as const) {
		const pidfile = join(home, "adapter-slack.pid");
		if (args.length === 0) await writeFile(pidfile, `${process.pid}\n`);
		const processHandle = Bun.spawn(["dist/gajaeway-slack", ...args], {
			env: { ...process.env, GAJAEWAY_HOME: home, LC_ALL: "C" },
			stdout: "pipe",
			stderr: "pipe",
		});
		const [stdout, stderr, exitCode] = await Promise.all([
			new Response(processHandle.stdout).text(),
			new Response(processHandle.stderr).text(),
			processHandle.exited,
		]);
		const pidfileUnchanged = args.length === 0 ? (await readFile(pidfile, "utf8")) === `${process.pid}\n` : undefined;
		const correctOutput =
			args[0] === "--help"
				? stdout.includes("usage:")
				: args[0] === "--version"
					? /^\d+\.\d+\.\d+(?:[-+][\w.-]+)?\s*$/.test(stdout)
					: args[0] === "--nope"
						? stderr.includes("usage:")
						: stderr.includes("already running") && pidfileUnchanged;
		probes.push({
			id,
			command: ["dist/gajaeway-slack", ...args],
			cwd: ".",
			env: { GAJAEWAY_HOME: home, LC_ALL: "C" },
			expectedExitCode,
			exitCode,
			stdout,
			stderr,
			...(pidfileUnchanged === undefined ? {} : { pidfileUnchanged, liveHolderPid: process.pid }),
			verdict: exitCode === expectedExitCode && correctOutput ? "passed" : "failed",
		});
	}
	await Bun.write(
		"artifacts/slack-adapter-cli-proof.json",
		JSON.stringify(
			{
				schemaVersion: 1,
				kind: "black-box-cli-api-receipt",
				sourceHash: "sha256:afbb9a77cb5ff7da17c201a039f12fd78aa9b2660ed6ce4b1829566b98061917",
				probes,
			},
			null,
			2,
		) + "\n",
	);
	console.log(JSON.stringify(probes));
} finally {
	await rm(home, { recursive: true, force: true });
}
