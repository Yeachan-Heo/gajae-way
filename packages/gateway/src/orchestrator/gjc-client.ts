import type { GatewayDatabase } from "../store/db";

export interface GjcPort {
	ensureSession(originKey: string): Promise<{ sessionId: string }>;
	sendTurn(sessionId: string, text: string): Promise<string>;
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
 * - Turns: spawn-per-turn `gjc --resume <id> -p --no-tools <text>` with plain
 *   text output. Persistent `gjc sdk serve` is deferred until its recovery
 *   contract is separately proven.
 *
 * The child inherits the owner's environment: the gateway is the owner's own
 * process and gjc needs the owner's provider credentials. Secrets are never
 * logged or persisted by this module.
 */
export class GjcClient implements GjcPort {
	readonly #sessions = new Map<string, string>();
	readonly #database: GatewayDatabase;
	readonly #timeoutMs: number;
	readonly #cwd: string;

	constructor(database: GatewayDatabase, timeoutMs = 120_000, cwd = process.cwd()) {
		this.#database = database;
		this.#timeoutMs = timeoutMs;
		this.#cwd = cwd;
	}

	async ensureSession(originKey: string): Promise<{ sessionId: string }> {
		const cached = this.#sessions.get(originKey) ?? this.#database.getSession(originKey);
		if (cached) {
			this.#sessions.set(originKey, cached);
			return { sessionId: cached };
		}
		const idempotencyKey = `gajaeway-${originKey.replace(/[^A-Za-z0-9._-]/g, "-")}`;
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
		this.#sessions.set(originKey, sessionId);
		this.#database.withTransaction(() => this.#database.putSession(originKey, sessionId));
		return { sessionId };
	}

	async sendTurn(sessionId: string, text: string): Promise<string> {
		if (typeof text !== "string" || text.length === 0) {
			throw new Error("turn text must be non-empty");
		}
		const child = Bun.spawn({
			cmd: ["gjc", "--resume", sessionId, "-p", "--no-tools", text],
			cwd: this.#cwd,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
			env: process.env as Record<string, string>,
		});
		const [stdout, stderr, exitCode] = await this.#bounded(child, "turn");
		if (exitCode !== 0) throw new Error(`gjc turn exited ${exitCode}: ${stderr.trim()}`);
		return stdout.trim();
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
