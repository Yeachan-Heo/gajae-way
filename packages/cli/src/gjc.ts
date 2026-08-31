import { join } from "node:path";
import type { SessionAttachResult } from "@gajaeway/protocol";
import { ProtocolError } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";

/**
 * `gajaeway gjc` — launch the native interactive gjc TUI as a gateway-managed
 * persona session.
 *
 * ARCH-007 (reworded): the gateway's `GjcClient` is the sole assembler of gjc
 * argv and the sole binder of managed sessions. This module spawns an
 * interactive TTY child because a child spawned inside the daemon would inherit
 * daemon stdio and could never be a TUI — but it is an OPAQUE CONSUMER of
 * `SessionAttachResult.argv`. It must not classify, extend, or reorder that
 * argv. Under `GAJAEWAY_TEST_STUB_GJC=1` the only permitted mutation is
 * rewriting `argv[0]` to the test stub child.
 *
 * The socket stays open for the child's whole life: the gateway's exclusive
 * lease is defined on that connection, so closing early would free the origin
 * while the TUI still holds the session.
 */

/** Attach can spawn `session.create`, which has a 300s ceiling in the gateway. */
const ATTACH_REQUEST_TIMEOUT_MS = 300_000;
/** Grace between SIGTERM and SIGKILL when the transport dies under us. */
const CHILD_KILL_GRACE_MS = 2_000;

const SIGNAL_NUMBERS: Record<string, number> = { SIGHUP: 1, SIGINT: 2, SIGTERM: 15 };

export interface GjcCliArgs {
	/** Wrapper-owned: rotate the epoch (`/new` semantics). Never forwarded. */
	readonly newSession: boolean;
	/** Everything else, passed verbatim to `session.attach` for classification. */
	readonly rest: string[];
}

/**
 * Parse ONLY wrapper-owned flags.
 *
 * Deliberately does not know the gjc allowlist: classification belongs to
 * `classifyGjcWrapperFlags` in the gateway. Duplicating it here would make the
 * CLI a second vendor adapter and would put the flag contract outside the
 * `GAJAEWAY_TEST_STUB_GJC` seam that tests it.
 */
export function parseGjcCliArgs(args: readonly string[]): GjcCliArgs {
	const rest: string[] = [];
	let newSession = false;
	let sawSeparator = false;
	for (const token of args) {
		// `--` ends wrapper-flag parsing: everything after it is positional payload
		// for gjc, so a literal `--new` there must NOT rotate the epoch.
		if (!sawSeparator && token === "--") {
			sawSeparator = true;
			rest.push(token);
			continue;
		}
		if (!sawSeparator && token === "--new") {
			newSession = true;
			continue;
		}
		rest.push(token);
	}
	return { newSession, rest };
}

/**
 * Refuse a non-interactive invocation before anything is claimed.
 *
 * A piped `gajaeway gjc` would leave a misbehaving TUI while still holding the
 * exclusive lease. The stub env is the only exemption, because the e2e suite
 * drives the wrapper with piped stdio on purpose.
 */
export function assertInteractiveStdin(
	stdin: { readonly isTTY?: boolean },
	env: Record<string, string | undefined> = process.env,
): void {
	if (stdin.isTTY === true) return;
	if (env.GAJAEWAY_TEST_STUB_GJC === "1") return;
	throw new Error("gajaeway gjc requires an interactive terminal: stdin is not a TTY");
}

export interface SpawnedChild {
	readonly pid?: number;
	readonly exited: Promise<number>;
	readonly exitCode: number | null;
	readonly signalCode: string | null;
	kill(signal?: number | NodeJS.Signals): void;
}

/**
 * Spawn seam. It receives the resolved env as well as argv and cwd, because the
 * env is part of what is being spawned: the gateway's native-state decision is
 * only honored if it actually reaches the child.
 */
export type SpawnFn = (cmd: string[], cwd: string, env: Record<string, string | undefined>) => SpawnedChild;

export interface SpawnGjcChildDeps {
	readonly spawn?: SpawnFn;
	readonly env?: Record<string, string | undefined>;
}

/**
 * Spawn the child with inherited stdio so the TUI owns the operator's terminal.
 *
 * The argv is opaque. The single documented exception is the stub rewrite of
 * `argv[0]`, which is what lets the e2e suite exercise the wait/exit/signal
 * contract without a real model.
 */
export function spawnGjcChild(argv: readonly string[], cwd: string, deps: SpawnGjcChildDeps = {}): SpawnedChild {
	const env = deps.env ?? process.env;
	const cmd = [...argv];
	if (env.GAJAEWAY_TEST_STUB_GJC === "1") {
		// The ONLY permitted argv mutation. The stub path is resolved relative to
		// this package, so the seam keeps exactly two effects (the GjcClient bind
		// short-circuit and this rewrite) and needs no extra environment variable.
		cmd.splice(0, 1, "bun", join(import.meta.dir, "..", "test", "stub-gjc-child.ts"));
	}
	if (deps.spawn) return deps.spawn(cmd, cwd, env);
	// Drop undefined entries instead of passing them through: they would be
	// rendered as empty strings, and for gjc's state resolver an EMPTY selector is
	// not the same as an ABSENT one.
	const childEnv: Record<string, string> = {};
	for (const [key, value] of Object.entries(env)) {
		if (typeof value === "string") childEnv[key] = value;
	}
	const child = Bun.spawn({
		cmd,
		cwd,
		stdin: "inherit",
		stdout: "inherit",
		stderr: "inherit",
		env: childEnv,
	});
	return child as unknown as SpawnedChild;
}

/** Translate a terminated-by-signal child into the shell's `128+N` convention. */
export function exitCodeForChild(child: { exitCode: number | null; signalCode: string | null }): number {
	if (child.signalCode) {
		const number = SIGNAL_NUMBERS[child.signalCode];
		if (number !== undefined) return 128 + number;
		return 1;
	}
	return child.exitCode ?? 1;
}

export interface RunGjcDeps {
	readonly connect?: (socket: string) => Promise<GajaewayClient>;
	readonly spawn?: SpawnFn;
	readonly env?: Record<string, string | undefined>;
	readonly stdin?: { readonly isTTY?: boolean };
	readonly errorLog?: (line: string) => void;
	/** Test seam so the kill grace does not slow the suite down. */
	readonly killGraceMs?: number;
}

export async function runGjc(socket: string, args: readonly string[], deps: RunGjcDeps = {}): Promise<number> {
	const env = deps.env ?? process.env;
	const errorLog = deps.errorLog ?? ((line: string) => console.error(line));

	// 1. TTY gate FIRST: before connecting, so a piped invocation can never even
	// reach session.attach, let alone claim the lease.
	try {
		assertInteractiveStdin(deps.stdin ?? process.stdin, env);
	} catch (error) {
		errorLog(error instanceof Error ? error.message : String(error));
		return 1;
	}

	const parsed = parseGjcCliArgs(args);

	// 2. Connect. No SQLite, no database fallback: the daemon is required.
	let client: GajaewayClient;
	try {
		client = deps.connect
			? await deps.connect(socket)
			: await GajaewayClient.connectSocket(socket, { requestTimeoutMs: ATTACH_REQUEST_TIMEOUT_MS });
	} catch {
		errorLog(`Unable to connect to gateway socket ${socket}. Start the daemon out-of-band first.`);
		return 1;
	}

	let offTerminal: (() => void) | undefined;
	let offStopping: (() => void) | undefined;
	const processSignals: Array<[NodeJS.Signals, () => void]> = [];

	try {
		// 3. Attach.
		let result: SessionAttachResult;
		try {
			result = await client.request<SessionAttachResult>("session.attach", {
				argv: parsed.rest,
				reset: parsed.newSession,
				holder: { pid: process.pid },
			});
		} catch (error) {
			errorLog(attachFailureMessage(error));
			return 1;
		}

		// 4. Register child-lifetime guards BEFORE spawn. A dead transport means the
		// gateway already released the lease, so an orphaned TUI would let the next
		// attach start a second gjc against the same session.
		let child: SpawnedChild | undefined;
		let killedByTransport = false;
		// Set the instant the transport dies, even if that happens BEFORE the child
		// exists. Without this, a transport death in the window between attach and
		// spawn would find `child` undefined, do nothing, and then the child would
		// be spawned anyway — an orphan holding a session whose lease is already
		// free.
		let transportDead = false;
		const killChild = () => {
			transportDead = true;
			killedByTransport = true;
			if (!child) return;
			try {
				child.kill("SIGTERM");
			} catch {
				// The child may already be gone; SIGKILL below is the backstop.
			}
			const grace = deps.killGraceMs ?? CHILD_KILL_GRACE_MS;
			setTimeout(() => {
				try {
					child?.kill("SIGKILL");
				} catch {
					// Already reaped.
				}
			}, grace).unref?.();
		};
		offTerminal = client.onTransportTerminal(() => {
			errorLog("gateway connection lost; terminating the gjc session");
			killChild();
		});
		offStopping = client.on("gateway.stopping", () => {
			errorLog("gateway is stopping; terminating the gjc session");
			killChild();
		});

		// If the transport already died while we were setting up, do NOT spawn: the
		// lease is gone, so the child would be an orphan on a session another
		// terminal can now claim.
		if (transportDead) return 1;

		// The gateway pins the native state the child must run with. Applying it is
		// a TOTAL override, not a merge: a selector the gateway reports as unset
		// must be DELETED from the child env, or an operator shell that exports it
		// wins and the child resolves a store where the bound session does not
		// exist ("Session not found"). This is not a second policy channel — the
		// CLI applies the gateway's decision without making one.
		const childEnv: Record<string, string | undefined> = { ...env, ...result.childEnv };
		for (const key of result.childEnvUnset ?? []) delete childEnv[key];
		child = spawnGjcChild(result.argv, result.cwd, { spawn: deps.spawn, env: childEnv });

		// Forward operator signals to the child without exiting first, so the TUI
		// gets a chance to shut down and the wrapper reports its real status.
		for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as NodeJS.Signals[]) {
			const handler = () => {
				try {
					child?.kill(signal);
				} catch {
					// Child already exited.
				}
			};
			process.on(signal, handler);
			processSignals.push([signal, handler]);
		}

		await child.exited;
		const code = exitCodeForChild(child);
		if (killedByTransport && code === 0) return 1;
		return code;
	} finally {
		for (const [signal, handler] of processSignals) process.off(signal, handler);
		// Unregister BEFORE the intentional close so ordinary teardown cannot
		// self-trigger the kill path. The SDK's intentional-close guard is the
		// second line of defense.
		offTerminal?.();
		offStopping?.();
		await client.close();
	}
}

function attachFailureMessage(error: unknown): string {
	if (error instanceof ProtocolError) {
		if (error.code === "unknown_verb") {
			return "This gateway is too old for `gajaeway gjc`; upgrade the daemon.";
		}
		return error.message;
	}
	return error instanceof Error ? error.message : String(error);
}
