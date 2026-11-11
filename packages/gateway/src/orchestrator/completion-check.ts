import { killGroup } from "./git-command";

/** Bytes of a completion-check command's stdout kept as its unmet-conditions output. */
const STDOUT_LIMIT_BYTES = 65_536;
/** Bytes of stderr kept for the gateway's own diagnostic line. */
const STDERR_LIMIT_BYTES = 4_096;

export interface CompletionCheckInput {
	/** Lane name, as given to `work.start`. */
	readonly name: string;
	/** The lane's session key, `work/task/<name>`. */
	readonly laneKey: string;
	readonly sessionId: string;
	/** The lane worktree; the command runs with this as its working directory. */
	readonly worktree: string;
	/** The attempt that just completed. */
	readonly opRef: string;
	/** Aborted when the gateway stops observing the attempt; the command is killed. */
	readonly signal?: AbortSignal;
}

/**
 * What a completion check concluded. Only `failed` asks for more work; every
 * `unavailable` cause is the check's own problem and never the lane's.
 */
export type CompletionCheckOutcome =
	| { readonly kind: "passed" }
	/** `output` is the command's raw stdout, cut at a fixed bound when `overflow` is set. */
	| { readonly kind: "failed"; readonly output: string; readonly overflow: boolean }
	| { readonly kind: "waiting" }
	| {
			readonly kind: "unavailable";
			readonly cause: "timeout" | "spawn_failed" | "abnormal_exit" | "aborted";
			readonly exitCode?: number;
			readonly detail?: string;
	  };

export interface CompletionCheckClient {
	/** Decides whether a completed attempt may settle. Must not throw. */
	check(input: CompletionCheckInput): Promise<CompletionCheckOutcome>;
}

/**
 * Production client: runs the operator's command without a shell, in the lane
 * worktree, with the gateway environment plus the lane's identity.
 *
 * Exit 0 passes, 1 fails with stdout as the unmet conditions, 3 waits on a
 * person. Any other exit, a timeout, or a command that cannot start is
 * `unavailable`, which callers treat as "settle as if no check ran".
 *
 * Like `runGit`, the command leads its own process group and the deadline
 * SIGKILLs that whole group, so a helper it spawned cannot hold the pipes open
 * past the deadline.
 */
export function commandCompletionCheck(argv: readonly string[], timeoutMs: number): CompletionCheckClient {
	return {
		async check(input) {
			if (input.signal?.aborted) return { kind: "unavailable", cause: "aborted" };
			// The deadline and the abort listener exist before the spawn, so an abort
			// that lands during the spawn still ends the call without waiting it out.
			let timer: ReturnType<typeof setTimeout> | undefined;
			let onAbort: (() => void) | undefined;
			const interrupted = new Promise<"timeout" | "aborted">((resolve) => {
				timer = setTimeout(() => resolve("timeout"), timeoutMs);
				onAbort = () => resolve("aborted");
				input.signal?.addEventListener("abort", onAbort, { once: true });
			});
			const clear = () => {
				clearTimeout(timer);
				if (onAbort) input.signal?.removeEventListener("abort", onAbort);
			};
			if (input.signal?.aborted) {
				clear();
				return { kind: "unavailable", cause: "aborted" };
			}
			let child: ReturnType<typeof spawnCheck>;
			try {
				child = spawnCheck(argv, input);
			} catch (error) {
				clear();
				return { kind: "unavailable", cause: "spawn_failed", detail: errorMessage(error) };
			}
			const completed = Promise.all([
				child.exited,
				readBounded(child.stdout, STDOUT_LIMIT_BYTES),
				readBounded(child.stderr, STDERR_LIMIT_BYTES),
			]).then(([exitCode, stdout, stderr]) => ({ exitCode, stdout, stderr }));
			let result: Awaited<typeof completed> | "timeout" | "aborted";
			try {
				result = await Promise.race([completed, interrupted]);
			} catch (error) {
				killGroup(child.pid);
				await child.exited;
				return { kind: "unavailable", cause: "abnormal_exit", detail: errorMessage(error) };
			} finally {
				clear();
			}
			if (result === "timeout" || result === "aborted") {
				killGroup(child.pid);
				// SIGKILL cannot be ignored: reap the command so nothing outlives the call.
				await child.exited;
				completed.catch(() => undefined);
				return { kind: "unavailable", cause: result };
			}
			const { exitCode, stdout, stderr } = result;
			if (exitCode === 0) return { kind: "passed" };
			if (exitCode === 1) return { kind: "failed", output: stdout.text, overflow: stdout.overflow };
			if (exitCode === 3) return { kind: "waiting" };
			return {
				kind: "unavailable",
				cause: "abnormal_exit",
				exitCode,
				...(stderr.text ? { detail: stderr.text } : {}),
			};
		},
	};
}

function spawnCheck(argv: readonly string[], input: CompletionCheckInput) {
	const env: Record<string, string> = {};
	for (const [key, value] of Object.entries(process.env)) if (value !== undefined) env[key] = value;
	env.GAJAEWAY_LANE_KEY = input.laneKey;
	env.GAJAEWAY_LANE_NAME = input.name;
	env.GAJAEWAY_SESSION_ID = input.sessionId;
	env.GAJAEWAY_WORKTREE = input.worktree;
	env.GAJAEWAY_OP_REF = input.opRef;
	return Bun.spawn([...argv], {
		cwd: input.worktree,
		env,
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
		detached: true,
	});
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/** Keeps the first `limit` bytes and drains the rest, so a chatty command can neither block nor grow memory. */
async function readBounded(
	stream: ReadableStream<Uint8Array>,
	limit: number,
): Promise<{ readonly text: string; readonly overflow: boolean }> {
	const chunks: Uint8Array[] = [];
	let size = 0;
	let overflow = false;
	const reader = stream.getReader();
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		const room = limit - size;
		if (value.byteLength > room) overflow = true;
		if (room <= 0) continue;
		const kept = value.byteLength > room ? value.subarray(0, room) : value;
		chunks.push(kept);
		size += kept.byteLength;
	}
	return { text: Buffer.concat(chunks).toString("utf8"), overflow };
}
