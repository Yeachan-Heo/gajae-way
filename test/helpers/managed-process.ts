import { type ChildProcess, type SpawnOptions, spawn } from "node:child_process";

export type ManagedBunProcess = Bun.Subprocess<"ignore", "ignore", "pipe">;
type TrackedBunProcess = ReturnType<typeof Bun.spawn>;

export interface ManagedDaemonSpawnOptions {
	readonly cmd: readonly string[];
	readonly cwd?: string;
	readonly env?: NodeJS.ProcessEnv;
	readonly stdin?: "ignore" | "pipe";
	readonly stdout?: "ignore" | "pipe";
	readonly stderr?: "ignore" | "pipe";
}

/**
 * Test-only ownership registry for daemon process trees. Every managed daemon
 * starts in its own process group. Cleanup first asks the group to stop, then
 * escalates to SIGKILL, waits for actual exit, and finally verifies each known
 * process group is gone. Detached closure workers are captured before a crash
 * so an intentional parent SIGKILL cannot leave them behind after the drill.
 */
export class ManagedProcessRegistry {
	readonly #bunChildren = new Set<TrackedBunProcess>();
	readonly #nodeChildren = new Set<ChildProcess>();
	readonly #groups = new Set<number>();

	spawnDaemon(options: ManagedDaemonSpawnOptions): ManagedBunProcess {
		const child = Bun.spawn({
			cmd: [...options.cmd],
			...(options.cwd ? { cwd: options.cwd } : {}),
			...(options.env ? { env: options.env } : {}),
			stdin: "ignore",
			stdout: "ignore",
			stderr: "pipe",
			detached: true,
		});
		return this.trackBun(child);
	}

	spawnNodeGroup(command: string, arguments_: readonly string[], options: SpawnOptions = {}): ChildProcess {
		const child = spawn(command, [...arguments_], { ...options, detached: true });
		return this.trackNode(child);
	}

	trackBun<T extends TrackedBunProcess>(child: T): T {
		this.#bunChildren.add(child);
		this.trackGroup(child.pid, "Bun child");
		return child;
	}

	trackNode(child: ChildProcess): ChildProcess {
		this.#nodeChildren.add(child);
		this.trackGroup(child.pid, "Node child");
		return child;
	}

	/** Capture detached descendants before deliberately crashing only the daemon group. */
	async crashDaemon(child: TrackedBunProcess, signal: "SIGTERM" | "SIGKILL" = "SIGKILL"): Promise<void> {
		await this.captureDescendantGroups(child.pid);
		signalGroup(child.pid, signal);
		if (!(await waitForBunExit(child, 3_000))) {
			throw new Error(`Managed daemon process ${child.pid} did not exit after ${signal}.`);
		}
	}

	async stopDaemon(child: TrackedBunProcess): Promise<void> {
		if (child.exitCode === null) {
			await this.captureDescendantGroups(child.pid);
			signalGroup(child.pid, "SIGTERM");
			if (!(await waitForBunExit(child, 3_000))) {
				await this.captureDescendantGroups(child.pid);
				signalGroup(child.pid, "SIGKILL");
				if (!(await waitForBunExit(child, 3_000))) {
					throw new Error(`Managed daemon process ${child.pid} did not exit after SIGKILL.`);
				}
			}
		} else {
			await child.exited;
		}
		await this.reapKnownGroups();
	}

	async crashNodeGroup(child: ChildProcess, signal: "SIGTERM" | "SIGKILL" = "SIGKILL"): Promise<void> {
		const pid = requiredPid(child.pid, "Node child");
		signalGroup(pid, signal);
		if (!(await waitForNodeExit(child, 3_000))) {
			throw new Error(`Managed Node child process ${pid} did not exit after ${signal}.`);
		}
	}

	async stopNodeGroup(child: ChildProcess): Promise<void> {
		const pid = requiredPid(child.pid, "Node child");
		if (nodeChildRunning(child)) {
			signalGroup(pid, "SIGTERM");
			if (!(await waitForNodeExit(child, 3_000))) {
				signalGroup(pid, "SIGKILL");
				if (!(await waitForNodeExit(child, 3_000))) {
					throw new Error(`Managed Node child process ${pid} did not exit after SIGKILL.`);
				}
			}
		}
		await this.reapKnownGroups();
	}

	/** Always call from afterEach before deleting test-owned directories. */
	async reapAll(): Promise<void> {
		const failures: unknown[] = [];
		for (const child of this.#bunChildren) {
			try {
				await this.stopDaemon(child);
			} catch (error) {
				failures.push(error);
			}
		}
		for (const child of this.#nodeChildren) {
			try {
				await this.stopNodeGroup(child);
			} catch (error) {
				failures.push(error);
			}
		}
		try {
			await this.reapKnownGroups();
			this.assertNoLiveGroups();
		} catch (error) {
			failures.push(error);
		}
		this.#bunChildren.clear();
		this.#nodeChildren.clear();
		if (failures.length > 0) throw new AggregateError(failures, "Managed test process cleanup failed.");
	}

	assertNoLiveGroups(): void {
		const live = [...this.#groups].filter(groupAlive);
		if (live.length > 0) {
			throw new Error(`Managed test process groups survived teardown: ${live.join(", ")}.`);
		}
	}

	private trackGroup(pid: number | undefined, label: string): void {
		this.#groups.add(requiredPid(pid, label));
	}

	private async captureDescendantGroups(rootPid: number): Promise<void> {
		const pending = [rootPid];
		const seen = new Set<number>(pending);
		while (pending.length > 0) {
			const parent = pending.pop() as number;
			for (const childPid of directChildren(parent)) {
				if (seen.has(childPid)) continue;
				seen.add(childPid);
				pending.push(childPid);
				const group = processGroup(childPid);
				if (group !== undefined) this.#groups.add(group);
			}
		}
	}

	private async reapKnownGroups(): Promise<void> {
		const failures: unknown[] = [];
		for (const group of [...this.#groups]) {
			if (!groupAlive(group)) {
				this.#groups.delete(group);
				continue;
			}
			try {
				signalGroup(group, "SIGTERM");
				if (!(await waitForGroupGone(group, 750))) {
					signalGroup(group, "SIGKILL");
					if (!(await waitForGroupGone(group, 3_000))) {
						throw new Error(`Managed process group ${group} did not exit after SIGKILL.`);
					}
				}
				this.#groups.delete(group);
			} catch (error) {
				failures.push(error);
			}
		}
		if (failures.length > 0) throw new AggregateError(failures, "Managed test process-group cleanup failed.");
	}
}

function requiredPid(pid: number | undefined, label: string): number {
	const candidate = pid;
	if (candidate === undefined || !Number.isSafeInteger(candidate) || candidate <= 0) {
		throw new Error(`${label} did not expose a valid PID.`);
	}
	return candidate;
}

function nodeChildRunning(child: ChildProcess): boolean {
	return child.exitCode === null && child.signalCode === null;
}

function signalGroup(group: number, signal: NodeJS.Signals): void {
	try {
		process.kill(-group, signal);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
	}
}

function groupAlive(group: number): boolean {
	try {
		process.kill(-group, 0);
		return true;
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ESRCH") return false;
		throw error;
	}
}

async function waitForBunExit(child: TrackedBunProcess, timeoutMs: number): Promise<boolean> {
	if (child.exitCode !== null) {
		await child.exited;
		return true;
	}
	return await Promise.race([child.exited.then(() => true), Bun.sleep(timeoutMs).then(() => false)]);
}

async function waitForNodeExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
	if (!nodeChildRunning(child)) return true;
	return await Promise.race([
		new Promise<boolean>((resolve) => child.once("close", () => resolve(true))),
		Bun.sleep(timeoutMs).then(() => false),
	]);
}

async function waitForGroupGone(group: number, timeoutMs: number): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (!groupAlive(group)) return true;
		await Bun.sleep(25);
	}
	return !groupAlive(group);
}

function directChildren(parentPid: number): number[] {
	const result = Bun.spawnSync({ cmd: ["pgrep", "-P", String(parentPid)], stdout: "pipe", stderr: "ignore" });
	if (result.exitCode === 1) return [];
	if (result.exitCode !== 0) {
		throw new Error(`Could not inspect descendants of managed process ${parentPid}.`);
	}
	return new TextDecoder()
		.decode(result.stdout)
		.split(/\s+/)
		.map((value) => Number(value))
		.filter((value): value is number => Number.isSafeInteger(value) && value > 0);
}

function processGroup(pid: number): number | undefined {
	const result = Bun.spawnSync({ cmd: ["ps", "-o", "pgid=", "-p", String(pid)], stdout: "pipe", stderr: "ignore" });
	if (result.exitCode === 1) return undefined;
	if (result.exitCode !== 0) throw new Error(`Could not inspect process group for managed process ${pid}.`);
	const group = Number(new TextDecoder().decode(result.stdout).trim());
	return Number.isSafeInteger(group) && group > 0 ? group : undefined;
}
