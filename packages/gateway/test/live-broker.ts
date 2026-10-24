import { join } from "node:path";
import { defaultPidAlive } from "../src/orchestrator/broker";
import { readBrokerDiscovery } from "../src/orchestrator/broker-liveness";

/**
 * Live GJC suites run attach-only (`GJC_SDK_BROKER_AUTOSTART=0`), so a client
 * never starts a broker. This fixture plays the supervisor: it reuses a live
 * broker already serving `agentDir`, or runs `gjc sdk broker run` in the
 * foreground for the test and stops it afterwards.
 */
export interface LiveBroker {
	readonly pid: number;
	stop(): Promise<void>;
}

export async function superviseLiveBroker(
	executable: string,
	agentDir: string,
	options: { readonly env?: Record<string, string>; readonly cwd?: string } = {},
): Promise<LiveBroker> {
	const discoveryPath = join(agentDir, "sdk", "broker.json");
	const existing = await readBrokerDiscovery(discoveryPath, defaultPidAlive);
	if (existing) return { pid: existing.pid, stop: async () => {} };
	const child = Bun.spawn([executable, "sdk", "broker", "run", "--agent-dir", agentDir], {
		cwd: options.cwd,
		env: { ...(options.env ?? process.env), GJC_CODING_AGENT_DIR: agentDir, GJC_SDK_BROKER_AUTOSTART: "0" },
		stdin: "ignore",
		stdout: "ignore",
		stderr: "pipe",
	});
	const stop = async () => {
		if (child.exitCode !== null || child.signalCode !== null) return;
		for (const signal of ["SIGTERM", "SIGKILL"] as const) {
			try {
				child.kill(signal);
			} catch {
				// Only observing exit proves the child is gone.
			}
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				const exited = await Promise.race([
					child.exited.then(() => true),
					new Promise<false>((resolve) => {
						timer = setTimeout(() => resolve(false), 2_000);
					}),
				]);
				if (exited) return;
			} catch {
				// An unsuccessful exit observation must still attempt escalation.
			} finally {
				clearTimeout(timer);
			}
		}
		throw new Error(`broker ${child.pid} exit unconfirmed after bounded TERM/KILL waits`);
	};
	const deadline = Date.now() + 30_000;
	while (Date.now() < deadline) {
		const discovery = await readBrokerDiscovery(discoveryPath, defaultPidAlive);
		if (discovery?.pid === child.pid) return { pid: child.pid, stop };
		if (child.exitCode !== null) {
			// A broker that won the startup race is a live incumbent: attach to it.
			const incumbent = await readBrokerDiscovery(discoveryPath, defaultPidAlive);
			if (incumbent) return { pid: incumbent.pid, stop: async () => {} };
			const stderr = (await new Response(child.stderr).text()).slice(0, 1_000);
			throw new Error(`gjc sdk broker run exited ${child.exitCode} before serving ${agentDir}: ${stderr}`);
		}
		await Bun.sleep(100);
	}
	await stop();
	throw new Error(`gjc sdk broker run did not publish a live discovery record for ${agentDir} within 30s`);
}
