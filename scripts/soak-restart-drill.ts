/**
 * I10 restart drill (checked in, non-product). Drives the throwaway soak
 * deployment through the S12 criterion-2/3 restarts and appends one row per
 * restart to `artifacts/soak/restarts.jsonl`:
 *   {executableDigest, transport, bootIncarnation, kind graceful|sigterm|poisoned,
 *    signalAt, exitAt, exitCode, forced, socketReadyAt, inflightOpRefsBefore[], recoveredOpRefsAfter[]}
 *
 * usage: bun scripts/soak-restart-drill.ts --home <GAJAEWAY_HOME> --bin <gateway-binary> --cli <gajaeway-binary>
 *        --kind graceful|sigterm|poisoned [--count N] [--transport channel|cli]
 * The gateway must be running under `--home` when the drill starts (it is restarted by the drill).
 * SIGTERM rows are taken mid-turn: a loopback prompt is sent first and the signal follows 1.5 s later.
 * `poisoned` allocates >= 33 unconsumed session.list continuation cursors on the private daemon before the restart.
 */
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

function arg(name: string, fallback?: string): string {
	const index = process.argv.indexOf(`--${name}`);
	const value = index >= 0 ? process.argv[index + 1] : undefined;
	if (!value && fallback === undefined) {
		console.error(`soak-restart-drill: missing --${name}`);
		process.exit(2);
	}
	return value ?? (fallback as string);
}

const home = arg("home");
const bin = arg("bin");
const cli = arg("cli");
const kind = arg("kind") as "graceful" | "sigterm" | "poisoned";
const count = Number(arg("count", "1"));
const transport = arg("transport", "channel");
const socket = join(home, "gateway.sock");
const out = join(home, "artifacts", "soak", "restarts.jsonl");
const digest = createHash("sha256").update(readFileSync(bin)).digest("hex");

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function pidOf(): number | undefined {
	// The running gateway for this home is the one whose argv carries this home's binary.
	const out = Bun.spawnSync(["pgrep", "-f", `${bin} daemon`])
		.stdout.toString()
		.trim();
	const pids = out
		.split("\n")
		.filter(Boolean)
		.map(Number)
		.filter((pid) => pid !== process.pid);
	if (pids.length > 1) throw new Error(`more than one gateway runs for ${home}: ${pids.join(",")}`);
	return pids[0];
}

function alive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

function nonterminalOpRefs(): string[] {
	const db = new Database(join(home, "gateway.db"), { readonly: true });
	try {
		return db
			.query<{ op_ref: string }, []>(
				"SELECT turn_op_ref AS op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound','accepted')",
			)
			.all()
			.map((row) => row.op_ref);
	} finally {
		db.close();
	}
}

async function status(): Promise<Record<string, unknown> | undefined> {
	const proc = Bun.spawn([cli, "--socket", socket, "status"], { stdout: "pipe", stderr: "ignore" });
	const text = await new Response(proc.stdout).text();
	await proc.exited;
	try {
		return JSON.parse(text) as Record<string, unknown>;
	} catch {
		return undefined;
	}
}

async function sendLoopback(text: string): Promise<void> {
	const proc = Bun.spawn([cli, "--socket", socket, "chat"], { stdin: "pipe", stdout: "ignore", stderr: "ignore" });
	proc.stdin.write(`${text}\n`);
	proc.stdin.flush();
	await sleep(1_500);
	proc.kill();
}

async function poisonDaemon(): Promise<void> {
	// >= 33 unconsumed session.list continuation cursors on the private daemon.
	const agentDirs = (await Array.fromAsync(new Bun.Glob("broker/*/agent").scan({ cwd: home, onlyFiles: false }))).map(
		(d) => join(home, d),
	);
	const agentDir = agentDirs[0];
	if (!agentDir) throw new Error("no private agent dir found");
	for (let i = 0; i < 34; i++) {
		const proc = Bun.spawn(
			[
				"gjc",
				"sdk",
				"session",
				"raw",
				"global",
				"--agent-dir",
				agentDir,
				"--op",
				"session.list",
				"--json-input",
				JSON.stringify({ scope: "all", limit: 1 }),
			],
			{
				stdout: "ignore",
				stderr: "ignore",
				env: { ...process.env, GJC_CODING_AGENT_DIR: agentDir },
			},
		);
		await proc.exited;
	}
}

async function restartOnce(index: number): Promise<void> {
	const pid = pidOf();
	if (!pid) throw new Error("gateway is not running under --home");
	const before = await status();
	const bootIncarnation = String((before?.startedAt as string | undefined) ?? "unknown");
	if (kind === "sigterm") await sendLoopback(`drill ${kind} ${index}: count to twenty slowly, one number per line`);
	if (kind === "poisoned") await poisonDaemon();
	const inflightOpRefsBefore = nonterminalOpRefs();
	const logOffsetBefore = existsSync(join(home, "daemon.log")) ? readFileSync(join(home, "daemon.log")).length : 0;
	const signalAt = new Date().toISOString();
	if (kind === "graceful") {
		const proc = Bun.spawn([cli, "--socket", socket, "shutdown"], { stdout: "ignore", stderr: "ignore" });
		await proc.exited;
	} else process.kill(pid, "SIGTERM");
	const t0 = Date.now();
	while (alive(pid) && Date.now() - t0 < 60_000) await sleep(50);
	const exitAt = new Date().toISOString();
	const exitedInTime = !alive(pid);
	if (!exitedInTime) process.kill(pid, "SIGKILL");
	// Only log written after the signal counts: shutdown_forced from an earlier drill must not leak in.
	const forced = readFileSync(join(home, "daemon.log"))
		.subarray(logOffsetBefore)
		.toString("utf8")
		.includes("shutdown_forced");
	// Relaunch the same binary and wait for the socket.
	const child = Bun.spawn([bin, "daemon", "--transport", transport], {
		cwd: home,
		env: { ...process.env, GAJAEWAY_HOME: home },
		stdout: Bun.file(join(home, "daemon.log")),
		stderr: Bun.file(join(home, "daemon.log")),
	});
	child.unref();
	await Bun.write(join(home, "gateway.pid"), String(child.pid));
	let socketReadyAt: string | undefined;
	const t1 = Date.now();
	while (Date.now() - t1 < 60_000) {
		if (existsSync(socket) && (await status())) {
			socketReadyAt = new Date().toISOString();
			break;
		}
		await sleep(100);
	}
	await sleep(3_000);
	const after = await status();
	const holds = ((after?.holds as Array<{ opRef: string }> | undefined) ?? []).map((h) => h.opRef);
	const recoveredOpRefsAfter = [
		...new Set([
			...holds,
			...nonterminalOpRefs(),
			...inflightOpRefsBefore.filter((ref) => !nonterminalOpRefs().includes(ref)),
		]),
	];
	const row = {
		executableDigest: digest,
		transport,
		bootIncarnation,
		kind,
		signalAt,
		exitAt,
		exitCode: exitedInTime ? 0 : 137,
		forced,
		socketReadyAt,
		inflightOpRefsBefore,
		recoveredOpRefsAfter,
		nextBootIncarnation: after?.startedAt,
	};
	appendFileSync(out, `${JSON.stringify(row)}\n`);
	console.log(JSON.stringify({ ...row, exitMs: Date.parse(exitAt) - Date.parse(signalAt) }));
}

for (let i = 0; i < count; i++) {
	await restartOnce(i + 1);
	await sleep(5_000);
}
