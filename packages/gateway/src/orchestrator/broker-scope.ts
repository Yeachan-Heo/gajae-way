import { readdirSync, readFileSync } from "node:fs";

/**
 * GJC autostarts its shared user broker as a detached child of whichever SDK
 * command first finds it absent. When that command is the gateway's, the broker
 * and every session host it later forks land in the gateway's systemd cgroup
 * (#183). That forced `KillMode=process`, which in turn let the gateway's own
 * relays and CLI children outlive every stop and be re-adopted by the next start.
 *
 * Releasing moves a GJC broker found inside this unit, with its descendants,
 * into a transient scope of its own. The broker's lifecycle stays GJC's; it only
 * stops sharing the gateway unit's fate, so the unit can kill its whole cgroup.
 */
export type BrokerRelease =
	| { readonly outcome: "released"; readonly scope: string; readonly pids: readonly number[] }
	| { readonly outcome: "skipped"; readonly reason: string };

export type BrokerReleaser = (pid: number) => Promise<BrokerRelease>;

export interface UnitScopePorts {
	/** Unified-hierarchy cgroup path of a process, or undefined if it is gone. */
	cgroup(pid: number | "self"): string | undefined;
	/** NUL-split argv of a process, or undefined if it is gone. */
	argv(pid: number): readonly string[] | undefined;
	/** Direct children of a process across all of its threads. */
	children(pid: number): readonly number[];
	/** Creates a transient scope unit holding exactly `pids`. */
	startScope(name: string, pids: readonly number[], userManager: boolean): Promise<void>;
}

const ATTEMPTS = 3;

export function createBrokerReleaser(ports: UnitScopePorts = procScopePorts()): BrokerReleaser {
	return async (pid) => {
		const own = ports.cgroup("self");
		if (!own?.endsWith(".service")) return { outcome: "skipped", reason: "not_a_systemd_service" };
		let failure = "";
		for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
			if (ports.cgroup(pid) !== own) return { outcome: "skipped", reason: "outside_unit" };
			if (!ports.argv(pid)?.includes("broker-internal")) return { outcome: "skipped", reason: "not_a_gjc_broker" };
			const pids = [pid, ...descendantsIn(ports, pid, own)];
			const scope = `gajaeway-gjc-broker-${pid}.scope`;
			try {
				await ports.startScope(scope, pids, own.includes("/user@"));
				return { outcome: "released", scope, pids };
			} catch (error) {
				// A descendant that exits between enumeration and the call fails the
				// whole request; enumerate again.
				failure = error instanceof Error ? error.message : String(error);
			}
		}
		return { outcome: "skipped", reason: `scope_failed: ${failure}` };
	};
}

function descendantsIn(ports: UnitScopePorts, root: number, cgroup: string): number[] {
	const found: number[] = [];
	const pending = [...ports.children(root)];
	const seen = new Set<number>([root]);
	for (let pid = pending.pop(); pid !== undefined; pid = pending.pop()) {
		if (seen.has(pid)) continue;
		seen.add(pid);
		if (ports.cgroup(pid) !== cgroup) continue;
		found.push(pid);
		pending.push(...ports.children(pid));
	}
	return found;
}

function readProc(path: string): string | undefined {
	try {
		return readFileSync(path, "utf8");
	} catch {
		return undefined;
	}
}

export function procScopePorts(): UnitScopePorts {
	return {
		cgroup(pid) {
			const text = readProc(`/proc/${pid}/cgroup`);
			return text
				?.split("\n")
				.find((line) => line.startsWith("0::"))
				?.slice(3);
		},
		argv(pid) {
			return readProc(`/proc/${pid}/cmdline`)?.split("\0");
		},
		children(pid) {
			let tasks: string[];
			try {
				tasks = readdirSync(`/proc/${pid}/task`);
			} catch {
				return [];
			}
			return tasks.flatMap((task) =>
				(readProc(`/proc/${pid}/task/${task}/children`) ?? "").split(" ").filter(Boolean).map(Number),
			);
		},
		async startScope(name, pids, userManager) {
			const child = Bun.spawn(
				[
					"busctl",
					...(userManager ? ["--user"] : []),
					"call",
					"org.freedesktop.systemd1",
					"/org/freedesktop/systemd1",
					"org.freedesktop.systemd1.Manager",
					"StartTransientUnit",
					"ssa(sv)a(sa(sv))",
					name,
					"fail",
					"2",
					"PIDs",
					"au",
					String(pids.length),
					...pids.map(String),
					"Description",
					"s",
					"Shared GJC broker released from the gajaeway gateway unit",
					"0",
				],
				{ stdin: "ignore", stdout: "ignore", stderr: "pipe", timeout: 5_000 },
			);
			const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);
			if (code !== 0) throw new Error(stderr.trim() || `busctl exited ${code}`);
		},
	};
}
