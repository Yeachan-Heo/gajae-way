const sourceHash = "sha256:412c90d6ba22fb5309c14d4fa2b67d655dddd1b234d49916e2de11c36637fe0d";
const replayPath = "artifacts/slack-adapter-cli-replay.json";
const before = await Bun.file(replayPath).arrayBuffer();
async function run(command: string[], env = process.env) {
	const child = Bun.spawn(command, { env, stdout: "pipe", stderr: "pipe" });
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	return { command, stdout, stderr, exitCode };
}
const build = await run(["bun", "run", "build"]);
if (build.exitCode !== 0) throw new Error(JSON.stringify(build));
const probes = await run(["bun", "artifacts/slack-adapter-cli-probes.ts"]);
const spec = await Bun.file(replayPath).json();
const replay = await run(spec.command, { ...process.env, ...spec.env });
const after = await Bun.file(replayPath).arrayBuffer();
const replayByteIdentical = Buffer.from(before).equals(Buffer.from(after));
const receipt = { sourceHash, frozenCommit: "145fa8f", build, probes, replay, replayByteIdentical };
await Bun.write("artifacts/slack-adapter-frozen-verification.json", JSON.stringify(receipt, null, 2) + "\n");
if (
	!replayByteIdentical ||
	replay.exitCode !== spec.expectedExitCode ||
	replay.stdout !== spec.recordedStdout ||
	replay.stderr !== spec.recordedStderr ||
	probes.exitCode !== 0
)
	throw new Error("Frozen CLI verification failed");
console.log(JSON.stringify({ buildExitCode: build.exitCode, replayByteIdentical, probesExitCode: probes.exitCode }));
