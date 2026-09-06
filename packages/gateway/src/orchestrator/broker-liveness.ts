import { readFile } from "node:fs/promises";
import { isLoopbackWebSocketUrl } from "./broker";

/** Stale-lock / dead-owner proof seam shared with the broker supervisor. */
export type PidAliveProbe = (pid: number) => boolean | Promise<boolean>;
/** gjc's own discovery TTL: a heartbeat older than this means the daemon is gone even if the file remains. */
export const BROKER_HEARTBEAT_TTL_MS = 15_000;

/**
 * Broker liveness judged from the daemon's own discovery file
 * (`<agentDir>/sdk/broker.json`), independent of the SDK CLI. When every
 * `gjc sdk` request answers `unavailable`, the CLI cannot tell a slow daemon
 * from a dead one whose lock tree it can no longer quarantine; the file can
 * (issue #174: pid dead, heartbeat frozen for 3h, gateway retried 165 times).
 */
export type BrokerLivenessVerdict =
	| { readonly state: "live"; readonly pid: number; readonly heartbeatAt: number }
	/** No readable, well-formed discovery file: the daemon never published or was retired. */
	| { readonly state: "absent" }
	| {
			readonly state: "wedged";
			readonly reason: "pid_dead" | "heartbeat_stale";
			readonly pid: number;
			readonly heartbeatAt: number;
	  };
export type BrokerLivenessProbe = () => Promise<BrokerLivenessVerdict>;

/**
 * Reads the same file as `readBrokerDiscovery`, but it reports WHY the
 * daemon is judged gone instead of collapsing every reason into undefined:
 * the wedge classification and the user-facing notice need the dead pid and
 * the frozen heartbeat timestamp.
 */
export async function judgeBrokerLiveness(
	discoveryPath: string,
	isPidAlive: PidAliveProbe,
	now = Date.now(),
	ttlMs = BROKER_HEARTBEAT_TTL_MS,
): Promise<BrokerLivenessVerdict> {
	// Unlike readBrokerDiscovery, this preserves the reason for a missing daemon so bind holds can explain it.
	try {
		const raw: unknown = JSON.parse(await readFile(discoveryPath, "utf8"));
		if (typeof raw !== "object" || raw === null) return { state: "absent" };
		const d = raw as Record<string, unknown>;
		if (
			d.protocolVersion !== 3 ||
			d.host !== "127.0.0.1" ||
			typeof d.url !== "string" ||
			!isLoopbackWebSocketUrl(d.url) ||
			typeof d.token !== "string" ||
			d.token.length === 0 ||
			typeof d.pid !== "number" ||
			!Number.isSafeInteger(d.pid) ||
			d.pid <= 0 ||
			typeof d.heartbeatAt !== "number" ||
			!Number.isFinite(d.heartbeatAt)
		)
			return { state: "absent" };
		const { pid, heartbeatAt } = d;
		if (!(await isPidAlive(pid))) return { state: "wedged", reason: "pid_dead", pid, heartbeatAt };
		if (now - heartbeatAt > ttlMs) return { state: "wedged", reason: "heartbeat_stale", pid, heartbeatAt };
		return { state: "live", pid, heartbeatAt };
	} catch {
		return { state: "absent" };
	}
}

export interface BindHoldDescription {
	/** Stable log token: `broker_wedged`, `broker_discovery_absent`, or `sdk_unavailable`. */
	readonly reason: string;
	/** The single string delivered to the conversation AND written to the operator log. */
	readonly notice: string;
}

/**
 * Names the actual cause of a bind hold for the person waiting on the reply.
 * `[turn held]` (not `[turn failed]`): the message is still pending and is
 * retried; nothing was lost.
 */
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
			notice: `[turn held] sdk unavailable / broker wedged since ${since} (${why}; ${failures}). Retrying in the background; move the agent dir's sdk/broker.lock and sdk/broker.json aside and restart the gateway to recover.`,
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
