import { expect, test } from "bun:test";
import { join } from "node:path";

const watchdogModule = join(import.meta.dir, "../src/liveness-watchdog.ts");

/** Runs a child that installs the watchdog and then executes `body`. */
async function runChild(body: string, whileRunning?: (child: Bun.Subprocess) => Promise<void>) {
	const script = `
		const { installLivenessWatchdog } = await import(${JSON.stringify(watchdogModule)});
		installLivenessWatchdog({ stallMs: 400, beatMs: 50 });
		${body}
	`;
	const child = Bun.spawn([process.execPath, "--eval", script], { stdout: "pipe", stderr: "pipe" });
	const deadline = setTimeout(() => child.kill("SIGKILL"), 15_000);
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		whileRunning?.(child),
	]);
	clearTimeout(deadline);
	return { exitCode, signalCode: child.signalCode, stdout, stderr };
}

test("#615: a main thread wedged in a synchronous child wait is SIGKILLed with one exit line", async () => {
	const startedAt = Date.now();
	// Bun.spawnSync parks the event loop in epoll exactly like the live hang.
	const result = await runChild(`
		setTimeout(() => {
			Bun.spawnSync(["sleep", "6.15"]);
			console.log("survived");
		}, 200);
	`);
	// The wedged child's own child is orphaned by the SIGKILL; reap it here.
	Bun.spawnSync(["pkill", "-f", "sleep 6.15"]);
	expect(result.signalCode).toBe("SIGKILL");
	expect(result.stdout).not.toContain("survived");
	expect(result.stderr).toContain(
		"gateway_exit cause=event_loop_stalled detail=main thread serviced no timer for 400ms",
	);
	expect(Date.now() - startedAt).toBeLessThan(6_000);
});

test("a serviced event loop is never killed and the watchdog does not hold the process open", async () => {
	const result = await runChild(`
		let ticks = 0;
		const timer = setInterval(() => {
			if (++ticks === 30) {
				clearInterval(timer);
				console.log("done");
			}
		}, 50);
	`);
	expect(result.signalCode).toBeNull();
	expect(result.exitCode).toBe(0);
	expect(result.stdout).toContain("done");
	expect(result.stderr).not.toContain("event_loop_stalled");
});

test("a whole-process suspension longer than the deadline is not a stall", async () => {
	// SIGSTOP pauses the worker and the main thread alike, as host sleep does;
	// the wall-clock gap seen on resume must not read as a wedged main thread.
	const result = await runChild(
		`
		let ticks = 0;
		const timer = setInterval(() => {
			if (++ticks === 40) {
				clearInterval(timer);
				console.log("done");
			}
		}, 50);
	`,
		async (child) => {
			await Bun.sleep(400);
			child.kill("SIGSTOP");
			await Bun.sleep(1_500);
			child.kill("SIGCONT");
		},
	);
	expect(result.signalCode).toBeNull();
	expect(result.exitCode).toBe(0);
	expect(result.stdout).toContain("done");
	expect(result.stderr).not.toContain("event_loop_stalled");
});
