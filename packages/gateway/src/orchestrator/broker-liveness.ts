import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

/** Process-table seam used when judging the daemon that published discovery. */
export type PidAliveProbe = (pid: number) => boolean | Promise<boolean>;

interface BrokerExitRecord {
	readonly reason?: unknown;
	readonly path?: unknown;
	readonly detail?: unknown;
	[key: string]: unknown;
}

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
	readonly brokerExitReason?: string;
}

function brokerExitRecordPaths(agentDir: string): string[] {
	return [join(agentDir, "sdk", "broker.exit.json"), join(agentDir, "sdk", "broker.startup-exit.json")];
}

function parseBrokerExitRecord(raw: string): BrokerExitRecord | undefined {
	try {
		const parsed: unknown = JSON.parse(raw);
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
		return parsed as BrokerExitRecord;
	} catch {
		return undefined;
	}
}

/** Reads the broker exit record from the GJC agent directory. */
async function readBrokerExitRecord(agentDir: string): Promise<BrokerExitRecord | undefined> {
	for (const path of brokerExitRecordPaths(agentDir)) {
		try {
			const record = parseBrokerExitRecord(await readFile(path, "utf8"));
			if (record) return record;
		} catch {
			// Try next path
		}
	}
	return undefined;
}

/** Synchronous snapshot reader for the runtime-cycle projection. */
export function readBrokerExitRecordSync(agentDir: string): unknown {
	for (const path of brokerExitRecordPaths(agentDir)) {
		try {
			const record = parseBrokerExitRecord(readFileSync(path, "utf8"));
			if (record) return record;
		} catch {
			// Try next path
		}
	}
	return undefined;
}

/** Formats broker exit information for display in hold notices. */
function describeBrokerExit(record: BrokerExitRecord | undefined): string | undefined {
	if (!record) return undefined;
	const reason = typeof record.reason === "string" ? record.reason : "unknown";
	const details: string[] = [reason];
	if (record.path && typeof record.path === "string") details.push(`path=${record.path}`);
	if (record.detail && typeof record.detail === "string") details.push(`detail=${record.detail}`);
	return details.join(" ");
}

/** Detects broker_index_lock_blocked from exit record. */
export function isBrokerIndexLockBlocked(record: unknown): boolean {
	if (typeof record !== "object" || record === null || Array.isArray(record)) return false;
	const candidate = record as BrokerExitRecord;
	const reason = typeof candidate.reason === "string" ? candidate.reason.toLowerCase() : "";
	const detail = typeof candidate.detail === "string" ? candidate.detail.toLowerCase() : "";
	return (
		reason.includes("startup-lock-blocked") ||
		reason.includes("retained removal transition") ||
		detail.includes("retained removal transition")
	);
}

/** Builds the single-line operator/user-facing notice for a pending bind hold. */
export async function describeBindHold(
	verdict: BrokerLivenessVerdict | undefined,
	detail: string,
	attempts: number,
	agentDir?: string,
): Promise<BindHoldDescription> {
	const failures = `${attempts} consecutive bind failures: ${detail}`;
	const exitRecord = agentDir ? await readBrokerExitRecord(agentDir) : undefined;
	const brokerExitReason = describeBrokerExit(exitRecord);
	const exitCause = isBrokerIndexLockBlocked(exitRecord) ? "broker index lock blocked; " : "";
	const exitDetails = brokerExitReason ? ` (${exitCause}broker exit: ${brokerExitReason})` : "";

	if (verdict?.state === "wedged") {
		const since = new Date(verdict.heartbeatAt).toISOString();
		const why =
			verdict.reason === "pid_dead"
				? `daemon pid ${verdict.pid} is dead`
				: `daemon pid ${verdict.pid} stopped heartbeating`;
		return {
			reason: "broker_wedged",
			notice: `[turn held] sdk unavailable / broker wedged since ${since} (${why}; ${failures})${exitDetails}. Retrying in the background.`,
			brokerExitReason,
		};
	}
	if (verdict?.state === "absent")
		return {
			reason: "broker_discovery_absent",
			notice: `[turn held] sdk unavailable / broker discovery absent (${failures})${exitDetails}. Retrying in the background.`,
			brokerExitReason,
		};
	return {
		reason: "sdk_unavailable",
		notice: `[turn held] sdk unavailable (broker daemon ${verdict ? "is live" : "liveness unknown"}; ${failures})${exitDetails}. Retrying in the background.`,
		brokerExitReason,
	};
}
