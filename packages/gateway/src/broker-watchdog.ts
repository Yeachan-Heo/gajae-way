import { probeBrokerEndpoint } from "./orchestrator/broker";
import { readBrokerDiscovery } from "./orchestrator/broker-liveness";

/** How often the broker host asks its own broker for a response. */
export const BROKER_WATCHDOG_INTERVAL_MS = 15_000;
/** Bound on one probe; a broker that cannot answer in this time counts as failed. */
export const BROKER_WATCHDOG_PROBE_TIMEOUT_MS = 10_000;
/**
 * How long every probe must fail before the host restarts the broker. Well under
 * the gateway's live-outage exit so recovery happens before clients give up.
 */
export const BROKER_WATCHDOG_UNRESPONSIVE_MS = 90_000;

export type BrokerWatchdogOptions = {
	/** The broker process this host spawned; a discovery record for any other pid does not count. */
	readonly childPid: number;
	readonly discoveryPath: string;
	readonly signal: AbortSignal;
	readonly intervalMs?: number;
	readonly unresponsiveMs?: number;
	/** Returns true when the broker answered a request. Tests replace the network probe. */
	readonly probe?: (childPid: number, discoveryPath: string) => Promise<boolean>;
	readonly now?: () => number;
	readonly sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
};

/**
 * Resolves with a diagnostic once the broker has failed every probe for
 * `unresponsiveMs`, or with undefined when aborted. A live process with a fresh
 * heartbeat that cannot answer requests is exactly the wedge the service manager
 * cannot see, so the host has to.
 */
export async function watchBrokerLiveness(options: BrokerWatchdogOptions): Promise<string | undefined> {
	const intervalMs = options.intervalMs ?? BROKER_WATCHDOG_INTERVAL_MS;
	const unresponsiveMs = options.unresponsiveMs ?? BROKER_WATCHDOG_UNRESPONSIVE_MS;
	const probe = options.probe ?? probeOwnBroker;
	const now = options.now ?? Date.now;
	const sleep = options.sleep ?? abortableSleep;
	// Startup counts toward the window: a broker that never starts answering is wedged too.
	let failingSince = now();
	let failures = 0;
	while (!options.signal.aborted) {
		await sleep(intervalMs, options.signal);
		if (options.signal.aborted) return undefined;
		let healthy: boolean;
		try {
			healthy = await probe(options.childPid, options.discoveryPath);
		} catch {
			healthy = false;
		}
		if (options.signal.aborted) return undefined;
		if (healthy) {
			if (failures > 0) console.log(`broker_watchdog_recovered pid=${options.childPid} failures=${failures}`);
			failingSince = now();
			failures = 0;
			continue;
		}
		failures++;
		const elapsed = now() - failingSince;
		console.error(`broker_watchdog_probe_failed pid=${options.childPid} failures=${failures} elapsedMs=${elapsed}`);
		if (elapsed >= unresponsiveMs)
			return `broker pid ${options.childPid} unresponsive for ${Math.floor(elapsed / 1000)}s (${failures} failed probes)`;
	}
	return undefined;
}

async function probeOwnBroker(childPid: number, discoveryPath: string): Promise<boolean> {
	const discovery = await readBrokerDiscovery(discoveryPath, (pid) => pid === childPid);
	if (!discovery) return false;
	return await probeBrokerEndpoint(discovery, BROKER_WATCHDOG_PROBE_TIMEOUT_MS);
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal.aborted) return resolve();
		const timer = setTimeout(done, ms);
		function done() {
			clearTimeout(timer);
			signal.removeEventListener("abort", done);
			resolve();
		}
		signal.addEventListener("abort", done, { once: true });
	});
}
