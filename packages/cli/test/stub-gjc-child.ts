#!/usr/bin/env bun
/**
 * Deterministic stand-in for the native gjc TUI.
 *
 * Exists so the CLI wrapper's contract — spawn-and-wait liveness, exit-code
 * passthrough, signal forwarding, injected argv, session-dir usage — can be
 * exercised without a real model or a real terminal.
 *
 * Deliberately does NOT compute `128 + N` for signals. That translation is the
 * wrapper's job; if the child did it, the wrapper could look correct while
 * being broken.
 */
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const argv = process.argv.slice(2);

/**
 * Where to write evidence.
 *
 * `GAJAEWAY_TEST_STUB_DIR` is set by the TEST, not by production code, so the
 * production stub seam still has exactly two effects (the GjcClient bind
 * short-circuit and the `argv[0]` rewrite). There is deliberately no argv
 * fallback: the binder never emits `--session-dir` and operators cannot pass it,
 * so reading it from argv would be a path nothing exercises.
 */
const sessionDir = process.env.GAJAEWAY_TEST_STUB_DIR;
if (sessionDir) {
	mkdirSync(sessionDir, { recursive: true });
	// The full argv the wrapper actually spawned, so tests can assert the binder's
	// injections and the absence of refused flags.
	writeFileSync(join(sessionDir, "stub-argv.json"), JSON.stringify(argv, null, 2));
	// Stands in for the native session transcript file, and records this child's
	// own pid so tests can assert its lifetime without scanning the process table.
	appendFileSync(
		join(sessionDir, "stub-session.jsonl"),
		`${JSON.stringify({ startedAt: Date.now(), pid: process.pid })}\n`,
	);
}

const signalLog = sessionDir ? join(sessionDir, "stub-signals.log") : undefined;
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as NodeJS.Signals[]) {
	process.on(signal, () => {
		if (signalLog) appendFileSync(signalLog, `${signal}\n`);
		// Re-raise with the DEFAULT disposition so the parent observes a genuine
		// signal death rather than a self-chosen exit code. The listener must be
		// removed first, or re-raising re-enters this handler forever.
		process.removeAllListeners(signal);
		process.kill(process.pid, signal);
	});
}

const requestedExit = process.env.GAJAEWAY_TEST_STUB_EXIT;
if (requestedExit !== undefined) {
	process.exit(Number(requestedExit));
}

// Otherwise idle until signalled or until an explicit lifetime elapses, so the
// wrapper's liveness can be observed.
const lifetimeMs = Number(process.env.GAJAEWAY_TEST_STUB_LIFETIME_MS ?? "30000");
setTimeout(() => process.exit(0), lifetimeMs);
