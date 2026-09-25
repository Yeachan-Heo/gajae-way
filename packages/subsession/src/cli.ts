/**
 * Broker-bound control surface for GJC subsessions.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, including the 1/5
 * correction): the canonical external controller is the **Broker plus the
 * `gjc sdk session` CLI**. Endpoint URLs and tokens stay inside SDK core, so a
 * runtime must never rediscover `<worktree>/.gjc/state/sdk/*.json` or open its
 * own authenticated socket. Everything here therefore goes through the CLI and
 * treats its JSON envelope as the only source of truth.
 *
 * The CLI is injected as a `CliRunner` so the supervisor is testable without a
 * live broker, and so the trusted binary path stays a deployment decision.
 */

export type CliResult = {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
};

export type CliRunner = (args: readonly string[], options?: { readonly timeoutMs?: number; readonly priority?: "interactive" | "background" }) => Promise<CliResult>;

export type BrokerSession = {
	readonly sessionId: string;
	/** `locator.repo`: the workspace the session was launched in. */
	readonly repo: string;
	readonly stateRoot?: string;
	readonly pid?: number;
	readonly live: boolean;
	readonly deleted: boolean;
	readonly activityState?: string;
	readonly lastHeartbeatAt?: number;
};

export class GjcCliError extends Error {
	readonly exitCode: number;
	readonly stderr: string;

	/** The `error` payload from an `ok: false` envelope, when there was one. */
	readonly details: unknown;

	constructor(message: string, exitCode: number, stderr: string, details: unknown = undefined) {
		super(message);
		this.name = "GjcCliError";
		this.exitCode = exitCode;
		this.stderr = stderr;
		this.details = details;
	}
}

/**
 * Unwraps the `{ ok, result }` envelope every `gjc sdk` verb prints.
 *
 * A non-zero exit, unparseable stdout, or `ok: false` are all failures: the
 * supervisor must never treat a partially printed envelope as success.
 */
export function parseEnvelope<T>(result: CliResult, command: string): T {
	if (result.exitCode !== 0) {
		throw new GjcCliError(`gjc sdk ${command} exited ${result.exitCode}`, result.exitCode, result.stderr.trim());
	}
	let body: unknown;
	try {
		body = JSON.parse(result.stdout);
	} catch {
		throw new GjcCliError(`gjc sdk ${command} did not print a JSON envelope`, 0, result.stdout.slice(0, 400));
	}
	if (typeof body !== "object" || body === null) {
		throw new GjcCliError(`gjc sdk ${command} envelope is not an object`, 0, result.stdout.slice(0, 400));
	}
	const envelope = body as { ok?: unknown; result?: unknown; error?: unknown };
	if (envelope.ok !== true) {
		throw new GjcCliError(
			`gjc sdk ${command} reported failure: ${JSON.stringify(envelope.error ?? null)}`,
			0,
			"",
			envelope.error,
		);
	}
	return envelope.result as T;
}

type RawSession = {
	sessionId?: unknown;
	locator?: { repo?: unknown; stateRoot?: unknown };
	pid?: unknown;
	live?: unknown;
	deleted?: unknown;
	activity?: { state?: unknown };
	lastHeartbeatAt?: unknown;
};

function normalizeSession(raw: RawSession): BrokerSession | undefined {
	if (typeof raw.sessionId !== "string" || raw.sessionId.length === 0) {
		return undefined;
	}
	if (typeof raw.locator?.repo !== "string") {
		return undefined;
	}
	return {
		sessionId: raw.sessionId,
		repo: raw.locator.repo,
		...(typeof raw.locator.stateRoot === "string" ? { stateRoot: raw.locator.stateRoot } : {}),
		...(typeof raw.pid === "number" ? { pid: raw.pid } : {}),
		live: raw.live === true,
		deleted: raw.deleted === true,
		...(typeof raw.activity?.state === "string" ? { activityState: raw.activity.state } : {}),
		...(typeof raw.lastHeartbeatAt === "number" ? { lastHeartbeatAt: raw.lastHeartbeatAt } : {}),
	};
}

export type ControllerOptions = {
	readonly run: CliRunner;
	/**
	 * Absolute worktree path. Passed as `--repo` only to scoped commands
	 * (`list`, `tail`); exact-session commands resolve the session by ID and
	 * gjc 0.17.4 rejects `--repo` on them.
	 */
	readonly repo: string;
	readonly agentDir?: string;
};

/**
 * `gjc sdk session <leaf...> [--agent-dir <dir>]`. The agent dir is a leaf
 * option: gjc 0.17.4 rejects it between `session` and the leaf (exit 2 usage);
 * the trailing spelling parses on 0.17.2 and 0.17.4.
 */
export function sessionArgs(options: ControllerOptions, leaf: readonly string[]): string[] {
	return ["sdk", "session", ...leaf, ...(options.agentDir ? ["--agent-dir", options.agentDir] : [])];
}

export async function listSessions(options: ControllerOptions): Promise<readonly BrokerSession[]> {
	const result = await options.run(sessionArgs(options, ["list", "--repo", options.repo]));
	const payload = parseEnvelope<{ sessions?: readonly RawSession[] }>(result, "session list");
	return (payload.sessions ?? [])
		.map(normalizeSession)
		.filter((session): session is BrokerSession => session !== undefined);
}

export async function inspectSession(
	options: ControllerOptions,
	sessionId: string,
): Promise<BrokerSession | undefined> {
	const result = await options.run(sessionArgs(options, ["inspect", sessionId]));
	const payload = parseEnvelope<{ session?: RawSession }>(result, "session inspect");
	return payload.session ? normalizeSession(payload.session) : undefined;
}

export type NotReadyReason = "not-found" | "deleted" | "not-live" | "identity-mismatch" | "cwd-mismatch";

export type ReadinessResult =
	| { readonly ready: true; readonly session: BrokerSession }
	| {
			readonly ready: false;
			readonly reason: NotReadyReason;
			readonly detail: string;
			readonly session?: BrokerSession;
	  };

/**
 * Applies the readiness contract: reachable via the broker AND identity matches
 * AND the session's own cwd is the requested worktree.
 *
 * A stale index entry that merely names the session is not readiness, which is
 * why a repo mismatch is rejected rather than tolerated.
 */
export async function verifyReady(
	options: ControllerOptions,
	expected: { readonly sessionId: string; readonly worktreePath: string },
): Promise<ReadinessResult> {
	let session: BrokerSession | undefined;
	try {
		session = await inspectSession(options, expected.sessionId);
	} catch (error) {
		return {
			ready: false,
			reason: "not-found",
			detail: `inspect failed: ${error instanceof Error ? error.message : String(error)}`,
		};
	}
	if (!session) {
		return { ready: false, reason: "not-found", detail: `broker has no session ${expected.sessionId}` };
	}
	if (session.deleted) {
		return { ready: false, reason: "deleted", detail: "session is marked deleted", session };
	}
	if (!session.live) {
		return { ready: false, reason: "not-live", detail: "session is not live", session };
	}
	if (session.sessionId !== expected.sessionId) {
		return {
			ready: false,
			reason: "identity-mismatch",
			detail: `broker returned ${session.sessionId} for ${expected.sessionId}`,
			session,
		};
	}
	if (session.repo !== expected.worktreePath) {
		return {
			ready: false,
			reason: "cwd-mismatch",
			detail: `session cwd ${session.repo} is not the requested worktree ${expected.worktreePath}`,
			session,
		};
	}
	return { ready: true, session };
}

/** Polls `verifyReady` and fails closed when the deadline passes. */
export async function awaitReady(
	options: ControllerOptions,
	expected: { readonly sessionId: string; readonly worktreePath: string },
	polling: {
		readonly timeoutMs?: number;
		readonly pollMs?: number;
		readonly now?: () => number;
		readonly sleep?: (ms: number) => Promise<void>;
	} = {},
): Promise<ReadinessResult> {
	const timeoutMs = polling.timeoutMs ?? 30_000;
	const pollMs = polling.pollMs ?? 500;
	const now = polling.now ?? (() => Date.now());
	const sleep = polling.sleep ?? ((ms: number) => Bun.sleep(ms));
	const deadline = now() + timeoutMs;

	let last = await verifyReady(options, expected);
	while (!last.ready && now() < deadline) {
		await sleep(pollMs);
		last = await verifyReady(options, expected);
	}
	return last;
}

/**
 * Picks the session that owns a worktree.
 *
 * With several candidates the newest live one wins, but the winner still has to
 * pass identity/cwd verification, so a leftover index row can never be promoted
 * to the lane's controller.
 */
export function selectSessionForWorktree(
	sessions: readonly BrokerSession[],
	worktreePath: string,
): BrokerSession | undefined {
	return sessions
		.filter((session) => session.repo === worktreePath && session.live && !session.deleted)
		.sort((left, right) => (right.lastHeartbeatAt ?? right.pid ?? 0) - (left.lastHeartbeatAt ?? left.pid ?? 0))
		.at(0);
}
