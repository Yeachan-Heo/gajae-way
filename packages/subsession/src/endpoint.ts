/**
 * GJC subsession endpoint discovery and readiness.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26):
 *   - tmux (or any supervisor) owns the process lifecycle only; the SDK endpoint
 *     is the canonical control and status surface.
 *   - discovery location is `<worktree>/.gjc/state/sdk/*.json`
 *   - the file body's `sessionId` wins; the filename stem is only a fallback
 *   - a file existing is NOT readiness. Ready requires all of:
 *       1. endpoint JSON parses
 *       2. `stale !== true`
 *       3. `pid` is actually alive
 *       4. a token-authenticated WebSocket connect succeeds
 */

import { readdir } from "node:fs/promises";
import { basename, join } from "node:path";

export type SubsessionEndpoint = {
	readonly version: number;
	readonly sessionId: string;
	readonly pid: number;
	readonly url: string;
	readonly token: string;
	readonly stale?: boolean;
	/** Absolute path of the endpoint file this record was read from. */
	readonly sourcePath: string;
};

export type EndpointParseFailure = {
	readonly sourcePath: string;
	readonly reason: string;
};

export type DiscoveryResult = {
	readonly endpoints: readonly SubsessionEndpoint[];
	readonly failures: readonly EndpointParseFailure[];
};

export type NotReadyReason = "no-endpoint" | "unparseable" | "marked-stale" | "pid-dead" | "connect-failed";

export type ReadinessResult =
	| { readonly ready: true; readonly endpoint: SubsessionEndpoint }
	| {
			readonly ready: false;
			readonly reason: NotReadyReason;
			readonly detail: string;
			readonly endpoint?: SubsessionEndpoint;
	  };

export function endpointDirectory(worktreePath: string): string {
	return join(worktreePath, ".gjc", "state", "sdk");
}

/** Builds the token-authenticated control URL for an endpoint. */
export function controlUrl(endpoint: Pick<SubsessionEndpoint, "url" | "token">): string {
	const base = endpoint.url.endsWith("/") ? endpoint.url : `${endpoint.url}/`;
	return `${base}?token=${encodeURIComponent(endpoint.token)}`;
}

function nonEmptyString(value: unknown): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

/**
 * Parses one endpoint record.
 *
 * `sourcePath` is used both for provenance and for the `sessionId` fallback, so
 * a body missing `sessionId` still yields a usable identifier instead of being
 * dropped.
 */
export function parseEndpoint(
	raw: unknown,
	sourcePath: string,
): { ok: true; endpoint: SubsessionEndpoint } | { ok: false; reason: string } {
	if (typeof raw !== "object" || raw === null) {
		return { ok: false, reason: "endpoint body is not an object" };
	}
	const body = raw as Record<string, unknown>;

	const url = nonEmptyString(body.url);
	if (!url) {
		return { ok: false, reason: "missing url" };
	}
	const token = nonEmptyString(body.token);
	if (!token) {
		return { ok: false, reason: "missing token" };
	}
	if (typeof body.pid !== "number" || !Number.isInteger(body.pid) || body.pid <= 0) {
		return { ok: false, reason: "missing or invalid pid" };
	}
	const version = typeof body.version === "number" ? body.version : 0;

	const sessionId = nonEmptyString(body.sessionId) ?? basename(sourcePath).replace(/\.json$/, "");
	if (!sessionId) {
		return { ok: false, reason: "missing sessionId and filename fallback is empty" };
	}

	return {
		ok: true,
		endpoint: {
			version,
			sessionId,
			pid: body.pid,
			url,
			token,
			...(typeof body.stale === "boolean" ? { stale: body.stale } : {}),
			sourcePath,
		},
	};
}

/** Reads and parses every endpoint file published under a worktree. */
export async function discoverEndpoints(worktreePath: string): Promise<DiscoveryResult> {
	const directory = endpointDirectory(worktreePath);
	let names: string[];
	try {
		names = await readdir(directory);
	} catch {
		return { endpoints: [], failures: [] };
	}

	const endpoints: SubsessionEndpoint[] = [];
	const failures: EndpointParseFailure[] = [];

	for (const name of names.filter((candidate) => candidate.endsWith(".json")).sort()) {
		const sourcePath = join(directory, name);
		let raw: unknown;
		try {
			raw = await Bun.file(sourcePath).json();
		} catch (error) {
			failures.push({ sourcePath, reason: `unreadable json: ${String(error)}` });
			continue;
		}
		const parsed = parseEndpoint(raw, sourcePath);
		if (parsed.ok) {
			endpoints.push(parsed.endpoint);
		} else {
			failures.push({ sourcePath, reason: parsed.reason });
		}
	}

	return { endpoints, failures };
}

export type ReadinessProbes = {
	/** Defaults to a signal-0 liveness check on the current host. */
	readonly isPidAlive?: (pid: number) => boolean;
	/** Defaults to a real token-authenticated WebSocket connect. */
	readonly connect?: (url: string) => Promise<boolean>;
};

export function defaultIsPidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		// EPERM means the process exists but is owned by someone else.
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

export function defaultConnect(url: string, timeoutMs = 5_000): Promise<boolean> {
	return new Promise((resolve) => {
		let socket: WebSocket;
		try {
			socket = new WebSocket(url);
		} catch {
			resolve(false);
			return;
		}
		const timer = setTimeout(() => {
			socket.close();
			resolve(false);
		}, timeoutMs);
		const settle = (value: boolean) => {
			clearTimeout(timer);
			resolve(value);
		};
		socket.addEventListener("open", () => {
			socket.close();
			settle(true);
		});
		socket.addEventListener("error", () => settle(false));
		socket.addEventListener("close", () => settle(false));
	});
}

/** Applies the four-condition readiness contract to a single endpoint. */
export async function checkReady(endpoint: SubsessionEndpoint, probes: ReadinessProbes = {}): Promise<ReadinessResult> {
	if (endpoint.stale === true) {
		return {
			ready: false,
			reason: "marked-stale",
			detail: `endpoint ${endpoint.sessionId} is marked stale`,
			endpoint,
		};
	}

	const isPidAlive = probes.isPidAlive ?? defaultIsPidAlive;
	if (!isPidAlive(endpoint.pid)) {
		return {
			ready: false,
			reason: "pid-dead",
			detail: `pid ${endpoint.pid} for ${endpoint.sessionId} is not alive`,
			endpoint,
		};
	}

	const connect = probes.connect ?? ((url: string) => defaultConnect(url));
	if (!(await connect(controlUrl(endpoint)))) {
		return {
			ready: false,
			reason: "connect-failed",
			detail: `token-authenticated connect to ${endpoint.url} failed`,
			endpoint,
		};
	}

	return { ready: true, endpoint };
}

/**
 * Waits for a worktree to publish a ready endpoint.
 *
 * Returns the first ready endpoint, or the most informative not-ready reason so
 * the caller can distinguish "never spawned" from "spawned but unhealthy".
 */
export async function awaitReadyEndpoint(
	worktreePath: string,
	options: {
		readonly timeoutMs?: number;
		readonly pollMs?: number;
		readonly probes?: ReadinessProbes;
		readonly now?: () => number;
		readonly sleep?: (ms: number) => Promise<void>;
	} = {},
): Promise<ReadinessResult> {
	const timeoutMs = options.timeoutMs ?? 30_000;
	const pollMs = options.pollMs ?? 250;
	const now = options.now ?? (() => Date.now());
	const sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
	const deadline = now() + timeoutMs;

	let last: ReadinessResult = {
		ready: false,
		reason: "no-endpoint",
		detail: `no endpoint json under ${endpointDirectory(worktreePath)}`,
	};

	for (;;) {
		const discovered = await discoverEndpoints(worktreePath);
		if (discovered.endpoints.length === 0 && discovered.failures.length > 0) {
			const first = discovered.failures[0];
			last = {
				ready: false,
				reason: "unparseable",
				detail: first ? `${first.sourcePath}: ${first.reason}` : "unparseable endpoint",
			};
		}
		for (const endpoint of discovered.endpoints) {
			const result = await checkReady(endpoint, options.probes);
			if (result.ready) {
				return result;
			}
			last = result;
		}
		if (now() >= deadline) {
			return last;
		}
		await sleep(pollMs);
	}
}
