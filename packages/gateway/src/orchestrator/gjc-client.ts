import type { GatewayDatabase } from "../store/db";
import {
	DEFAULT_REBIND_CAP,
	extractRuntimeError,
	GjcRuntimeError,
	type RuntimeErrorDetail,
	rebindableCodeOf,
	runtimeErrorOfEnvelope,
	SessionRebinder,
	sanitizeDiagnostic,
} from "./rebind";

/** Process seam retained for bind-time session creation tests. */
export type SpawnFn = typeof Bun.spawn;

/** A bounded create request is distinct from turn liveness and is never config-driven. */
export const DEFAULT_SESSION_CREATE_TIMEOUT_MS = 30_000;

export interface SessionBindOptions {
	/** Workspace recorded by the SDK session locator. */
	readonly cwd?: string;
}

/**
 * Bind-only SDK session creator.
 *
 * Turns no longer flow through this class: BrokerSessionPort owns every send,
 * tail, status, and terminal-body operation. This class retains the durable,
 * idempotent session.create and bind-time rebind discipline for callers that
 * need to create a fresh epoch binding.
 */
export class GjcClient {
	readonly #sessions = new Map<string, string>();
	readonly #inflight = new Map<string, Promise<{ sessionId: string }>>();
	readonly #database: GatewayDatabase;
	readonly #sessionCreateTimeoutMs: number;
	readonly #cwd: string;
	readonly #rebinder: SessionRebinder;
	readonly #spawn: SpawnFn;

	constructor(
		database: GatewayDatabase,
		cwd = process.cwd(),
		deps: {
			/** Session creation request bound; it never controls a running turn. */
			readonly sessionCreateTimeoutMs?: number;
			readonly rebindCap?: number;
			readonly spawn?: SpawnFn;
			readonly log?: (line: string) => void;
		} = {},
	) {
		this.#database = database;
		this.#sessionCreateTimeoutMs = positiveMilliseconds(
			deps.sessionCreateTimeoutMs,
			DEFAULT_SESSION_CREATE_TIMEOUT_MS,
			"sessionCreateTimeoutMs",
		);
		this.#cwd = cwd;
		this.#spawn = deps.spawn ?? Bun.spawn.bind(Bun);
		this.#rebinder = new SessionRebinder(
			database,
			deps.rebindCap ?? DEFAULT_REBIND_CAP,
			deps.log ?? ((line) => console.warn(line)),
		);
	}

	/**
	 * Binds an origin to a SDK session, rebinding once when session.create reports
	 * a condemned idempotency key. Turn acceptance/recovery is deliberately not
	 * part of this class.
	 */
	async ensureSession(originKey: string, epoch = 0, options?: SessionBindOptions): Promise<{ sessionId: string }> {
		const cached = this.#cachedSession(originKey, epoch);
		if (cached) return { sessionId: cached };
		return await this.#bindAt(originKey, epoch, options);
	}

	/** An explicit `/new` clears the bind-time condemnation budget for this origin. */
	forgetRebinds(originKey: string): void {
		this.#rebinder.clear(originKey);
	}

	#bindAt(originKey: string, epoch: number, options?: SessionBindOptions): Promise<{ sessionId: string }> {
		return this.#coordinatedBind(originKey, epoch, options, true);
	}

	#coordinatedBind(
		originKey: string,
		epoch: number,
		options: SessionBindOptions | undefined,
		mayRebind: boolean,
	): Promise<{ sessionId: string }> {
		const cacheKey = `${originKey}#${epoch}`;
		const existing = this.#inflight.get(cacheKey);
		if (existing) return existing;
		const bind = this.#ensureSessionUncached(originKey, epoch, options, mayRebind).finally(() => {
			this.#inflight.delete(cacheKey);
		});
		this.#inflight.set(cacheKey, bind);
		return bind;
	}

	async #ensureSessionUncached(
		originKey: string,
		epoch: number,
		options: SessionBindOptions | undefined,
		mayRebind: boolean,
	): Promise<{ sessionId: string }> {
		try {
			return await this.#createSession(originKey, epoch, options);
		} catch (error) {
			const code = rebindableCodeOf(error);
			if (!code || !mayRebind) throw error;
			const nextEpoch = this.#rebinder.rebind(originKey, code, epoch);
			return await this.#coordinatedBind(originKey, nextEpoch, options, false);
		}
	}

	#cachedSession(originKey: string, epoch: number): string | undefined {
		const cacheKey = `${originKey}#${epoch}`;
		const record = this.#database.getSessionRecord(originKey);
		const cached =
			this.#sessions.get(cacheKey) ?? (record?.epoch === epoch && record.sessionId ? record.sessionId : undefined);
		if (!cached) return undefined;
		this.#sessions.set(cacheKey, cached);
		return cached;
	}

	/**
	 * Creates a session using the durable instance/origin/epoch idempotency key.
	 * This is the only direct SDK child retained here; turn work goes exclusively
	 * through the broker-bound SessionPort.
	 */
	async #createSession(originKey: string, epoch: number, options?: SessionBindOptions): Promise<{ sessionId: string }> {
		const cacheKey = `${originKey}#${epoch}`;
		const cwd = options?.cwd ?? this.#cwd;
		const idempotencyKey = `gajaeway-${this.#database.instanceId}-${originKey.replace(/[^A-Za-z0-9._-]/g, "-")}-e${epoch}`;
		const child = this.#spawn({
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
			cwd,
			stdin: new Response(JSON.stringify({ cwd })).body ?? "ignore",
			stdout: "pipe",
			stderr: "pipe",
			env: process.env as Record<string, string>,
		});
		const [stdout, stderr, exitCode] = await this.#bounded(child, "session.create");
		if (exitCode !== 0) {
			const detail = extractRuntimeError(stdout) ?? extractRuntimeError(stderr);
			throw runtimeFailure(`gjc session.create exited ${exitCode}`, detail, stderr.trim());
		}
		const sessionId = parseCreateResult(stdout);
		const persistedEpoch = this.#database.getSessionRecord(originKey)?.epoch ?? -1;
		if (persistedEpoch > epoch) return await this.#bindAt(originKey, persistedEpoch, options);
		this.#forgetOrigin(originKey);
		this.#sessions.set(cacheKey, sessionId);
		this.#database.withTransaction(() => this.#database.putSession(originKey, sessionId));
		return { sessionId };
	}

	#forgetOrigin(originKey: string): void {
		for (const cacheKey of this.#sessions.keys())
			if (cacheKey.startsWith(`${originKey}#`)) this.#sessions.delete(cacheKey);
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
						reject(new Error(`gjc ${label} did not complete within ${this.#sessionCreateTimeoutMs}ms`));
					}, this.#sessionCreateTimeoutMs);
				}),
			]);
		} finally {
			if (timer) clearTimeout(timer);
		}
	}
}

/**
 * One framing for every SDK create failure. Classification is by the structured
 * runtime code, never human error wording.
 */
function runtimeFailure(context: string, detail: RuntimeErrorDetail | undefined, fallback: string): GjcRuntimeError {
	const resolved: RuntimeErrorDetail = detail ?? { message: fallback };
	const code = resolved.code ? sanitizeDiagnostic(resolved.code) || undefined : undefined;
	const message = sanitizeDiagnostic(resolved.message ?? fallback) || context;
	return new GjcRuntimeError(`${context}: ${code ? `${code}: ` : ""}${message}`, {
		...(code ? { code } : {}),
		message,
	});
}

/** Reads the sessionId out of the SDK envelope without weakening code classification. */
function parseCreateResult(stdout: string): string {
	for (const line of stdout.trim().split("\n")) {
		let parsed: { ok?: boolean; result?: { sessionId?: string } } | undefined;
		try {
			parsed = JSON.parse(line) as typeof parsed;
		} catch {
			continue;
		}
		if (parsed?.ok && typeof parsed.result?.sessionId === "string") return parsed.result.sessionId;
		if (parsed?.ok === false) throw runtimeFailure("gjc session.create failed", runtimeErrorOfEnvelope(parsed), line);
	}
	throw new Error("gjc session.create produced no parseable sessionId");
}

function positiveMilliseconds(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${name} must be a positive integer`);
	return result;
}
