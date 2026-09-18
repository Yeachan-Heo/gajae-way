import { readFile } from "node:fs/promises";

/** Process-table seam used when judging the daemon that published discovery. */
export type PidAliveProbe = (pid: number) => boolean | Promise<boolean>;

/** gjc's own discovery heartbeat TTL. */
export const BROKER_HEARTBEAT_TTL_MS = 15_000;

export type BrokerDiscovery = {
	readonly pid: number;
	readonly url: string;
	readonly token: string;
	readonly heartbeatAt: number;
};

export type BrokerLivenessVerdict =
	| { readonly state: "live"; readonly pid: number; readonly heartbeatAt: number }
	| { readonly state: "absent" }
	| {
			readonly state: "wedged";
			readonly reason: "pid_dead" | "heartbeat_stale";
			readonly pid: number;
			readonly heartbeatAt: number;
	  };
export type BrokerLivenessProbe = () => Promise<BrokerLivenessVerdict>;

function isLoopbackWebSocketUrl(value: string): boolean {
	try {
		const url = new URL(value);
		return (
			url.protocol === "ws:" &&
			url.hostname === "127.0.0.1" &&
			url.port !== "" &&
			url.username === "" &&
			url.password === "" &&
			(url.pathname === "" || url.pathname === "/") &&
			url.search === "" &&
			url.hash === ""
		);
	} catch {
		return false;
	}
}

function discoveryRecord(raw: unknown): BrokerDiscovery | undefined {
	if (typeof raw !== "object" || raw === null) return undefined;
	const value = raw as Record<string, unknown>;
	if (
		value.protocolVersion !== 3 ||
		value.host !== "127.0.0.1" ||
		typeof value.url !== "string" ||
		!isLoopbackWebSocketUrl(value.url) ||
		typeof value.token !== "string" ||
		value.token.length === 0 ||
		typeof value.pid !== "number" ||
		!Number.isSafeInteger(value.pid) ||
		value.pid <= 0 ||
		typeof value.heartbeatAt !== "number" ||
		!Number.isFinite(value.heartbeatAt)
	)
		return undefined;
	return { pid: value.pid, url: value.url, token: value.token, heartbeatAt: value.heartbeatAt };
}

/** Reads a currently live, fresh discovery record for the ordinary broker client path. */
export async function readBrokerDiscovery(
	discoveryPath: string,
	isPidAlive: PidAliveProbe,
	now = Date.now(),
	ttlMs = BROKER_HEARTBEAT_TTL_MS,
): Promise<BrokerDiscovery | undefined> {
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(discoveryPath, "utf8"));
	} catch {
		return undefined;
	}
	const discovery = discoveryRecord(raw);
	if (!discovery) return undefined;
	if (now - discovery.heartbeatAt > ttlMs || discovery.heartbeatAt > now + ttlMs) return undefined;
	try {
		if (!(await isPidAlive(discovery.pid))) return undefined;
	} catch {
		return undefined;
	}
	return discovery;
}

/**
 * Judges the daemon from its own discovery file without making an SDK request.
 * A malformed/missing file is absent; a valid file with a dead owner or frozen
 * heartbeat is a wedge verdict that callers can hold on rather than rotate.
 */
export async function judgeBrokerLiveness(
	discoveryPath: string,
	isPidAlive: PidAliveProbe,
	now = Date.now(),
	ttlMs = BROKER_HEARTBEAT_TTL_MS,
): Promise<BrokerLivenessVerdict> {
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(discoveryPath, "utf8"));
	} catch {
		return { state: "absent" };
	}
	const discovery = discoveryRecord(raw);
	if (!discovery || discovery.heartbeatAt > now + ttlMs) return { state: "absent" };
	try {
		if (!(await isPidAlive(discovery.pid)))
			return { state: "wedged", reason: "pid_dead", pid: discovery.pid, heartbeatAt: discovery.heartbeatAt };
	} catch {
		return { state: "absent" };
	}
	if (now - discovery.heartbeatAt > ttlMs)
		return { state: "wedged", reason: "heartbeat_stale", pid: discovery.pid, heartbeatAt: discovery.heartbeatAt };
	return { state: "live", pid: discovery.pid, heartbeatAt: discovery.heartbeatAt };
}

export interface BindHoldDescription {
	readonly reason: "broker_wedged" | "broker_discovery_absent" | "sdk_unavailable";
	readonly notice: string;
}

/** Builds the single-line operator/user-facing notice for a pending bind hold. */
export function describeBindHold(
	verdict: BrokerLivenessVerdict | undefined,
	detail: string,
	attempts: number,
): BindHoldDescription {
	const failures = `${attempts} consecutive bind failures: ${detail}`;
	if (verdict?.state === "wedged") {
		const since = new Date(verdict.heartbeatAt).toISOString();
		const why =
			verdict.reason === "pid_dead"
				? `daemon pid ${verdict.pid} is dead`
				: `daemon pid ${verdict.pid} stopped heartbeating`;
		return {
			reason: "broker_wedged",
			notice: `[turn held] sdk unavailable / broker wedged since ${since} (${why}; ${failures}). Retrying in the background.`,
		};
	}
	if (verdict?.state === "absent")
		return {
			reason: "broker_discovery_absent",
			notice: `[turn held] sdk unavailable / broker discovery absent (${failures}). Retrying in the background.`,
		};
	return {
		reason: "sdk_unavailable",
		notice: `[turn held] sdk unavailable (broker daemon ${verdict ? "is live" : "liveness unknown"}; ${failures}). Retrying in the background.`,
	};
}
