import type { GatewayDatabase } from "../store/db";

export interface TurnProgress {
	/** Tool executions the turn has started so far. */
	readonly toolCalls: number;
}

export interface GjcPort {
	ensureSession(originKey: string, epoch?: number): Promise<{ sessionId: string }>;
	sendTurn(
		sessionId: string,
		text: string,
		systemPreamble?: string,
		onProgress?: (progress: TurnProgress) => void,
	): Promise<string>;
}

/**
 * Incremental parser for the `gjc -p --mode json` ndjson event stream.
 * Tracks tool executions and captures the final assistant text.
 */
export class GjcTurnStream {
	#buffer = "";
	toolCalls = 0;
	finalText: string | undefined;

	feed(chunk: string): void {
		this.#buffer += chunk;
		let index = this.#buffer.indexOf("\n");
		while (index !== -1) {
			const line = this.#buffer.slice(0, index).trim();
			this.#buffer = this.#buffer.slice(index + 1);
			if (line) this.#line(line);
			index = this.#buffer.indexOf("\n");
		}
	}

	#line(line: string): void {
		let event: { type?: string; message?: { role?: string; content?: Array<{ type?: string; text?: string }> } };
		try {
			event = JSON.parse(line) as typeof event;
		} catch {
			return; // non-JSON noise line
		}
		if (event.type === "tool_execution_start") this.toolCalls++;
		if ((event.type === "message_end" || event.type === "turn_end") && event.message?.role === "assistant") {
			const text = (event.message.content ?? [])
				.filter((part) => part.type === "text" && typeof part.text === "string")
				.map((part) => part.text)
				.join("\n")
				.trim();
			if (text) this.finalText = text;
		}
	}
}

/**
 * The only module permitted to spawn or otherwise touch the gjc runtime
 * (plan ARCH-007: sole vendor adapter behind a gateway-owned port).
 *
 * P0 strategy per the spike verdict (artifacts/p0-gjc-spike-report.md):
 * - Session identity: atomic idempotent create-or-resume via
 *   `gjc sdk session raw global --op session.create --idempotency-key <key>`.
 *   The same origin key always resolves to the same gjc session (proven under
 *   5-way concurrency), so a crash between create and binding commit is
 *   harmless: re-running create returns the same session (§4 row 3, branch A).
 * - Turns: spawn-per-turn `gjc --resume <id> -p --append-system-prompt <text>`.
 *   The gateway intentionally does not pass `--no-tools`; gjc owns its tool
 *   policy, informed by the unoverridable ActionGuard floor notice.
 *
 * The child inherits the owner's environment: the gateway is the owner's own
 * process and gjc needs the owner's provider credentials. Secrets are never
 * logged or persisted by this module.
 */
export class GjcClient implements GjcPort {
	/** GAJAEWAY_TEST_STUB_GJC is a test-only deterministic process seam; never set it in production. */
	readonly #sessions = new Map<string, string>();
	readonly #database: GatewayDatabase;
	readonly #timeoutMs: number;
	readonly #cwd: string;

	// 300s ceiling: the persona is an action-capable agent that runs real tools per
	// turn; 120s killed live owner turns mid-investigation (P1 drill finding).
	constructor(database: GatewayDatabase, timeoutMs = 300_000, cwd = process.cwd()) {
		this.#database = database;
		this.#timeoutMs = timeoutMs;
		this.#cwd = cwd;
	}

	async ensureSession(originKey: string, epoch = 0): Promise<{ sessionId: string }> {
		const cacheKey = `${originKey}#${epoch}`;
		if (process.env.GAJAEWAY_TEST_STUB_GJC === "1") return { sessionId: `stub-${cacheKey}` };
		const record = this.#database.getSessionRecord(originKey);
		const cached =
			this.#sessions.get(cacheKey) ??
			(record && record.epoch === epoch && record.sessionId ? record.sessionId : undefined);
		if (cached) {
			this.#sessions.set(cacheKey, cached);
			return { sessionId: cached };
		}
		// Instance-scoped key: two gateway installs (or two homes on one machine)
		// must never collide on the same gjc session (cross-instance replay bug
		// found in P2 integration). Epoch is always included so /new provably
		// binds a fresh transcript.
		const idempotencyKey = `gajaeway-${this.#database.instanceId}-${originKey.replace(/[^A-Za-z0-9._-]/g, "-")}-e${epoch}`;
		const child = Bun.spawn({
			cmd: [
				"gjc",
				"sdk",
				"session",
				"raw",
				"global",
				"--op",
				"session.create",
				"--idempotency-key",
				idempotencyKey,
				"--json-input-stdin",
			],
			cwd: this.#cwd,
			stdin: new Response(JSON.stringify({ cwd: this.#cwd })).body ?? "ignore",
			stdout: "pipe",
			stderr: "pipe",
			env: process.env as Record<string, string>,
		});
		const [stdout, stderr, exitCode] = await this.#bounded(child, "session.create");
		if (exitCode !== 0) throw new Error(`gjc session.create exited ${exitCode}: ${stderr.trim()}`);
		const sessionId = parseCreateResult(stdout);
		this.#sessions.set(cacheKey, sessionId);
		this.#database.withTransaction(() => this.#database.putSession(originKey, sessionId));
		return { sessionId };
	}

	async sendTurn(
		sessionId: string,
		text: string,
		systemPreamble?: string,
		onProgress?: (progress: TurnProgress) => void,
	): Promise<string> {
		if (typeof text !== "string" || text.length === 0) {
			throw new Error("turn text must be non-empty");
		}
		if (process.env.GAJAEWAY_TEST_STUB_GJC === "1") {
			await Bun.sleep(50);
			if (process.env.GAJAEWAY_TEST_STUB_CAPTURE)
				await Bun.write(process.env.GAJAEWAY_TEST_STUB_CAPTURE, systemPreamble ?? "");
			onProgress?.({ toolCalls: 0 });
			return process.env.GAJAEWAY_TEST_STUB_REPLY ?? "stub reply";
		}
		const child = Bun.spawn({
			cmd: [
				"gjc",
				"--resume",
				sessionId,
				"-p",
				"--mode",
				"json",
				...(systemPreamble ? ["--append-system-prompt", systemPreamble] : []),
				text,
			],
			cwd: this.#cwd,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
			env: process.env as Record<string, string>,
		});
		// The ceiling is an INACTIVITY ceiling, not a wall-clock one: a turn that is
		// visibly working (streaming events, running tools) is never killed, while a
		// hung child that goes silent for turnTimeoutMs is reaped (owner directive:
		// long agentic work must not die mid-flight; liveness is reported instead).
		const stream = new GjcTurnStream();
		let lastActivity = Date.now();
		let killedForInactivity = false;
		const watchdog = setInterval(() => {
			if (Date.now() - lastActivity > this.#timeoutMs) {
				killedForInactivity = true;
				child.kill();
			}
		}, 1_000);
		try {
			const reader = (child.stdout as ReadableStream<Uint8Array>).getReader();
			const decoder = new TextDecoder();
			const stderrPromise = new Response(child.stderr as ReadableStream).text();
			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				lastActivity = Date.now();
				stream.feed(decoder.decode(value, { stream: true }));
				if (onProgress) onProgress({ toolCalls: stream.toolCalls });
			}
			const [stderr, exitCode] = await Promise.all([stderrPromise, child.exited]);
			if (killedForInactivity) throw new Error(`gjc turn made no progress for ${this.#timeoutMs}ms and was reaped`);
			if (exitCode !== 0) throw new Error(`gjc turn exited ${exitCode}: ${stderr.trim()}`);
			if (stream.finalText === undefined) throw new Error("gjc turn stream produced no assistant text");
			return stream.finalText;
		} finally {
			clearInterval(watchdog);
		}
	}

	async #bounded(child: ReturnType<typeof Bun.spawn>, label: string): Promise<[string, string, number]> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				Promise.all([
					new Response(child.stdout as ReadableStream).text(),
					new Response(child.stderr as ReadableStream).text(),
					child.exited,
				]),
				new Promise<never>((_, reject) => {
					timer = setTimeout(() => {
						child.kill();
						reject(new Error(`gjc ${label} timed out after ${this.#timeoutMs}ms`));
					}, this.#timeoutMs);
				}),
			]);
		} finally {
			if (timer) clearTimeout(timer);
		}
	}
}

function parseCreateResult(stdout: string): string {
	for (const line of stdout.trim().split("\n")) {
		try {
			const parsed = JSON.parse(line) as {
				ok?: boolean;
				result?: { sessionId?: string };
				error?: { message?: string };
			};
			if (parsed.ok && typeof parsed.result?.sessionId === "string") {
				return parsed.result.sessionId;
			}
			if (parsed.ok === false) {
				throw new Error(`gjc session.create failed: ${parsed.error?.message ?? line}`);
			}
		} catch (error) {
			if (error instanceof Error && error.message.startsWith("gjc session.create failed")) {
				throw error;
			}
			// non-JSON noise line; keep scanning
		}
	}
	throw new Error("gjc session.create produced no parseable sessionId");
}
