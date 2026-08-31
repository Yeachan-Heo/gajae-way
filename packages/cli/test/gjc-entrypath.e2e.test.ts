import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * End-to-end contract for `gajaeway gjc` (AC-1, AC-2, AC-5, AC-9, AC-10, AC-11,
 * AC-19 and the AC-6 child-safety half).
 *
 * The daemon runs as a REAL separate process so it can be SIGKILLed for the
 * abrupt-transport case, and the CLI runs as a real process so spawn-and-wait
 * liveness, exit codes, and signal forwarding are genuinely observed rather
 * than simulated.
 */

const REPO_ROOT = join(import.meta.dir, "..", "..", "..");
const GATEWAY_MAIN = join(REPO_ROOT, "packages", "gateway", "src", "main.ts");
const CLI_MAIN = join(REPO_ROOT, "packages", "cli", "src", "main.ts");

interface Fixture {
	readonly home: string;
	readonly socket: string;
	readonly sessionsRoot: string;
	daemon?: Bun.Subprocess;
}

let fixture: Fixture | undefined;

afterEach(async () => {
	if (fixture?.daemon) {
		fixture.daemon.kill("SIGKILL");
		await fixture.daemon.exited.catch(() => undefined);
	}
	if (fixture) await rm(fixture.home, { recursive: true, force: true });
	fixture = undefined;
});

async function makeFixture(): Promise<Fixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-gjc-e2e-"));
	const workspace = join(home, "workspace");
	await mkdir(workspace, { recursive: true });
	await writeFile(join(workspace, "SOUL.md"), "E2E-SOUL");
	await writeFile(join(workspace, "AGENTS.md"), "E2E-AGENTS");
	await writeFile(join(workspace, "USER.md"), "E2E-USER");
	await writeFile(
		join(home, "config.json"),
		JSON.stringify({ schemaVersion: 1, home, logVerbosity: "info", dmPolicy: "open" }),
	);
	return { home, socket: join(home, "gateway.sock"), sessionsRoot: join(home, "sessions", "terminal") };
}

/** Boot the daemon as its own process and wait until the socket answers. */
async function startDaemon(current: Fixture): Promise<void> {
	const daemon = Bun.spawn({
		cmd: ["bun", GATEWAY_MAIN, "daemon"],
		cwd: REPO_ROOT,
		env: { ...process.env, GAJAEWAY_HOME: current.home, GAJAEWAY_TEST_STUB_GJC: "1" },
		stdout: "pipe",
		stderr: "pipe",
	});
	current.daemon = daemon;
	for (let attempt = 0; attempt < 400; attempt++) {
		// A unix socket is not a regular file, so `Bun.file().exists()` is not a
		// usable readiness probe: actually try to connect.
		try {
			const probe = await Bun.connect({ unix: current.socket, socket: { data() {} } });
			probe.end();
			return;
		} catch {
			await Bun.sleep(25);
		}
	}
	throw new Error("daemon socket never became available");
}

interface CliRun {
	readonly proc: Bun.Subprocess;
	stdout(): Promise<string>;
	stderr(): Promise<string>;
}

function runCli(current: Fixture, args: string[], extraEnv: Record<string, string> = {}, epoch = 0): CliRun {
	const proc = Bun.spawn({
		cmd: ["bun", CLI_MAIN, "--socket", current.socket, "gjc", ...args],
		cwd: REPO_ROOT,
		env: {
			...process.env,
			GAJAEWAY_HOME: current.home,
			GAJAEWAY_TEST_STUB_GJC: "1",
			// Test-owned: the binder no longer injects `--session-dir`, so the stub
			// child is told out of band where to leave evidence. Production code
			// never sets this.
			GAJAEWAY_TEST_STUB_DIR: join(current.sessionsRoot, `e${epoch}`),
			GAJAEWAY_TEST_STUB_LIFETIME_MS: "30000",
			...extraEnv,
		},
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
	});
	return {
		proc,
		stdout: () => new Response(proc.stdout as ReadableStream).text(),
		stderr: () => new Response(proc.stderr as ReadableStream).text(),
	};
}

/** Wait for the stub child's marker file, which proves the child actually ran. */
async function waitForStubArgv(current: Fixture, epoch = 0): Promise<string[]> {
	const path = join(current.sessionsRoot, `e${epoch}`, "stub-argv.json");
	for (let attempt = 0; attempt < 300; attempt++) {
		if (await Bun.file(path).exists()) return JSON.parse(await readFile(path, "utf8")) as string[];
		await Bun.sleep(25);
	}
	throw new Error(`stub child never wrote ${path}`);
}

function isAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

describe("gajaeway gjc entrypath", () => {
	test("waits for the child, then exits with the child's code (AC-1, AC-2, AC-11)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const run = runCli(fixture, ["--new"], { GAJAEWAY_TEST_STUB_LIFETIME_MS: "1500" }, 1);
		const cliPid = run.proc.pid;
		const argv = await waitForStubArgv(fixture, 1);

		// AC-1: the wrapper is still alive while the child runs, and it is not the
		// child itself (no exec).
		expect(isAlive(cliPid)).toBe(true);

		// AC-2: wrapper-owned flags never reach the child.
		expect(argv).not.toContain("--new");
		expect(argv).not.toContain("--socket");
		// The binder's injections are present.
		expect(argv).toContain("--resume");
		// The binder must NOT inject `--session-dir`: the create path cannot accept
		// it, so pointing the TUI at another store breaks resume outright.
		expect(argv).not.toContain("--session-dir");
		expect(argv).toContain("--append-system-prompt");

		// AC-11: the child wrote its markers under the TEST-OWNED evidence directory
		// (the binder no longer injects a session dir), and the gateway independently
		// created that epoch directory.
		expect(await Bun.file(join(fixture.sessionsRoot, "e1", "stub-session.jsonl")).exists()).toBe(true);

		const code = await run.proc.exited;
		expect(code).toBe(0);
	}, 60_000);

	test("propagates the child's exit code (AC-10)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const run = runCli(fixture, [], { GAJAEWAY_TEST_STUB_EXIT: "7" });
		expect(await run.proc.exited).toBe(7);
	}, 60_000);

	test("forwards SIGTERM to the child and reports 128+N (AC-10)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const run = runCli(fixture, []);
		await waitForStubArgv(fixture, 0);
		run.proc.kill("SIGTERM");
		const code = await run.proc.exited;

		const log = await readFile(join(fixture.sessionsRoot, "e0", "stub-signals.log"), "utf8");
		// The child must have RECEIVED the signal (proving forwarding), and the
		// wrapper must be the one translating it to 128+N.
		expect(log).toContain("SIGTERM");
		expect(code).toBe(143);
	}, 60_000);

	test("refuses a second concurrent invocation naming the holder (AC-5)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const holder = runCli(fixture, []);
		await waitForStubArgv(fixture, 0);

		const rival = runCli(fixture, []);
		const rivalCode = await rival.proc.exited;
		const rivalErr = await rival.stderr();
		expect(rivalCode).not.toBe(0);
		expect(rivalErr).toContain("holds the terminal gjc lease");

		holder.proc.kill("SIGKILL");
		await holder.proc.exited;
	}, 60_000);

	test("refuses refused flags with no child at all (AC-7)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const run = runCli(fixture, ["--resume", "not-yours"]);
		const code = await run.proc.exited;
		expect(code).not.toBe(0);
		expect(await run.stderr()).toContain("--resume");
		// No child ever started.
		expect(await Bun.file(join(fixture.sessionsRoot, "e0", "stub-argv.json")).exists()).toBe(false);
	}, 60_000);

	test("names the resolved socket path when the daemon is down and never opens SQLite (AC-9)", async () => {
		fixture = await makeFixture();
		// Deliberately no daemon.
		const run = runCli(fixture, []);
		const code = await run.proc.exited;
		const stderr = await run.stderr();

		expect(code).not.toBe(0);
		expect(stderr).toContain(fixture.socket);
		// The CLI must not fall back to the database.
		expect(await Bun.file(join(fixture.home, "gateway.db")).exists()).toBe(false);
	}, 60_000);

	test("kills the child when the daemon dies abruptly, before a fresh attach wins (AC-6)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		const holder = runCli(fixture, []);
		await waitForStubArgv(fixture, 0);

		// The stub child records its own pid, so this does not depend on scanning
		// the process table.
		const childPid = await readStubChildPid(fixture, 0);
		expect(childPid).toBeGreaterThan(0);
		expect(isAlive(childPid)).toBe(true);

		// Abrupt daemon death: no protocol close, no gateway.stopping.
		fixture.daemon?.kill("SIGKILL");
		await fixture.daemon?.exited.catch(() => undefined);
		fixture.daemon = undefined;

		// (1) the child must die and (2) the holder CLI must exit nonzero...
		const holderCode = await holder.proc.exited;
		expect(holderCode).not.toBe(0);
		for (let attempt = 0; attempt < 200 && isAlive(childPid); attempt++) await Bun.sleep(25);
		expect(isAlive(childPid)).toBe(false);

		// ...and only THEN may a fresh attach succeed, with no overlap.
		await startDaemon(fixture);
		const next = runCli(fixture, [], { GAJAEWAY_TEST_STUB_LIFETIME_MS: "1500" });
		expect(await next.proc.exited).toBe(0);
	}, 90_000);

	test("refuses a non-TTY invocation without the stub exemption (AC-19)", async () => {
		fixture = await makeFixture();
		await startDaemon(fixture);

		// Stub env intentionally UNSET: this is the refuse path. stdin is a pipe.
		const refuseEnv: Record<string, string> = { ...(process.env as Record<string, string>) };
		delete refuseEnv.GAJAEWAY_TEST_STUB_GJC;
		refuseEnv.GAJAEWAY_HOME = fixture.home;
		const proc = Bun.spawn({
			cmd: ["bun", CLI_MAIN, "--socket", fixture.socket, "gjc"],
			cwd: REPO_ROOT,
			env: refuseEnv,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
		});
		const code = await proc.exited;
		const stderr = await new Response(proc.stderr as ReadableStream).text();

		expect(code).not.toBe(0);
		expect(stderr.toLowerCase()).toContain("tty");
		// The gate ran before connect: no child, and no session row was created.
		expect(await Bun.file(join(fixture.sessionsRoot, "e0", "stub-argv.json")).exists()).toBe(false);
	}, 60_000);
});

/** Read the pid the stub child recorded for itself. */
async function readStubChildPid(current: Fixture, epoch: number): Promise<number> {
	const path = join(current.sessionsRoot, `e${epoch}`, "stub-session.jsonl");
	for (let attempt = 0; attempt < 300; attempt++) {
		if (await Bun.file(path).exists()) {
			const line = (await readFile(path, "utf8")).split("\n").find((entry) => entry.trim());
			if (line) {
				const parsed = JSON.parse(line) as { pid?: number };
				if (typeof parsed.pid === "number") return parsed.pid;
			}
		}
		await Bun.sleep(25);
	}
	return -1;
}
