import { probeBrokerEndpoint } from "./orchestrator/broker";
import { readBrokerDiscovery } from "./orchestrator/broker-liveness";

/** How often the broker host asks its own broker for a response. */
export const BROKER_WATCHDOG_INTERVAL_MS = 15_000;
/** Bound on one probe; a broker that cannot answer in this time counts as failed. */
export const BROKER_WATCHDOG_PROBE_TIMEOUT_MS = 10_000;
/**
 * Gap between two observed ticks that means this process was suspended (lid
 * closed, VM pause) rather than that the broker is wedged. One awake iteration
 * costs at most `intervalMs` plus a probe, so no probe failed during a longer
 * gap — whether it opens between iterations or inside the probe itself.
 */
export const BROKER_WATCHDOG_SUSPEND_GAP_MS = 60_000;
/**
 * How long every probe must fail before the host restarts the broker. Well under
 * the gateway's live-outage exit so recovery happens before clients give up.
 */
export const BROKER_WATCHDOG_UNRESPONSIVE_MS = 90_000;
/**
 * Budget before the first answered probe. Broker recovery after a host reboot
 * can take minutes (#182); restarting it at 90s would loop instead of recover.
 */
export const BROKER_WATCHDOG_STARTUP_MS = 300_000;

export type BrokerWatchdogOptions = {
	/** The broker process this host spawned; a discovery record for any other pid does not count. */
	readonly childPid: number;
	readonly discoveryPath: string;
	readonly signal: AbortSignal;
	readonly intervalMs?: number;
	readonly unresponsiveMs?: number;
	readonly startupMs?: number;
	/** Gap between observed ticks treated as a suspend; defaults to `BROKER_WATCHDOG_SUSPEND_GAP_MS`. */
	readonly suspendGapMs?: number;
	/** Returns true when the broker answered a request. Tests replace the network probe. */
	readonly probe?: (childPid: number, discoveryPath: string) => Promise<boolean>;
	readonly now?: () => number;
	readonly sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
};

/**
 * Resolves with a diagnostic once the broker has failed every probe for
 * `unresponsiveMs`, or with undefined when aborted. A live process with a fresh
 * heartbeat that cannot answer requests is exactly the wedge the service manager
 * cannot see, so the host has to. The window measures awake time: a gap in which
 * this process was suspended — between iterations or across a probe — restarts
 * the window instead of counting downtime.
 */
export async function watchBrokerLiveness(options: BrokerWatchdogOptions): Promise<string | undefined> {
	const intervalMs = options.intervalMs ?? BROKER_WATCHDOG_INTERVAL_MS;
	const unresponsiveMs = options.unresponsiveMs ?? BROKER_WATCHDOG_UNRESPONSIVE_MS;
	const startupMs = options.startupMs ?? BROKER_WATCHDOG_STARTUP_MS;
	const suspendGapMs = options.suspendGapMs ?? Math.max(BROKER_WATCHDOG_SUSPEND_GAP_MS, intervalMs * 3);
	const probe = options.probe ?? probeOwnBroker;
	const now = options.now ?? Date.now;
	const sleep = options.sleep ?? abortableSleep;
	// A broker that never starts answering is wedged too, but it gets the longer
	// startup budget until its first answer.
	let failingSince = now();
	let answered = false;
	let failures = 0;
	let lastTick = failingSince;
	// No probe can fail while this process is suspended, so a gap that long is
	// evidence about the host, never about the broker.
	const noteSuspend = (gapMs: number, phase: "idle" | "probe"): void => {
		if (failures > 0)
			console.log(
				`broker_watchdog_suspended pid=${options.childPid} gapMs=${gapMs} failures=${failures} phase=${phase}`,
			);
	};
	while (!options.signal.aborted) {
		await sleep(intervalMs, options.signal);
		if (options.signal.aborted) return undefined;
		const started = now();
		if (started - lastTick > suspendGapMs) {
			// Suspended between iterations (lid closed, VM pause): restart the window
			// at the wake instead of killing a broker that was never asked.
			noteSuspend(started - lastTick, "idle");
			failingSince = started;
			failures = 0;
		}
		lastTick = started;
		let healthy: boolean;
		try {
			healthy = await probe(options.childPid, options.discoveryPath);
		} catch {
			healthy = false;
		}
		if (options.signal.aborted) return undefined;
		const settled = now();
		if (settled - started > suspendGapMs) {
			// The suspend overlapped the probe, so the host woke with a dead socket
			// and this result says nothing about the broker: discard it before any
			// failure or elapsed threshold sees it, and restart the window at the wake.
			noteSuspend(settled - started, "probe");
			failingSince = settled;
			failures = 0;
			lastTick = settled;
			continue;
		}
		lastTick = settled;
		if (healthy) {
			answered = true;
			if (failures > 0) console.log(`broker_watchdog_recovered pid=${options.childPid} failures=${failures}`);
			failingSince = settled;
			failures = 0;
			continue;
		}
		failures++;
		const elapsed = settled - failingSince;
		console.error(`broker_watchdog_probe_failed pid=${options.childPid} failures=${failures} elapsedMs=${elapsed}`);
		if (elapsed >= (answered ? unresponsiveMs : startupMs))
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
