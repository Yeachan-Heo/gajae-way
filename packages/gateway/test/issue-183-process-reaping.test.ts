import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type ProcessTableEntry,
	parseUnixProcessTable,
	reapLeftoverWorkers,
	selectOrphanedWorkers,
	UNIX_PROCESS_TABLE_COMMAND,
} from "../src/ops/process-reaper";
import { GJC_ATTACH_ONLY_ENV } from "../src/orchestrator/broker";

const GJC = "/home/user/.gajaeway/gjc/0.18.12/bin/gjc";
const GJC_PREFIX = "/home/user/.gajaeway/gjc";
const AGENT_DIR = "/home/user/.gajaeway/broker/agent";
const SELF_PID = 1000;
const SESSION = "00000000-0000-4000-8000-000000000001";

function worker(
	pid: number,
	ppid: number,
	overrides: Partial<ProcessTableEntry> & { executable?: string } = {},
): ProcessTableEntry {
	const { executable = GJC, ...entryOverrides } = overrides;
	return {
		pid,
		ppid,
		argv: [executable, "sdk", "serve", "--stdio", "--session", SESSION],
		agentDir: AGENT_DIR,
		identity: `start-${pid}`,
		...entryOverrides,
	};
}

function select(
	table: ProcessTableEntry[],
	options: { gjcPath?: string; gjcPrefix?: string; brokerAgentDir?: string } = {},
) {
	return selectOrphanedWorkers(table, {
		gjcPath: options.gjcPath ?? GJC,
		gjcPrefix: options.gjcPrefix,
		brokerAgentDir: options.brokerAgentDir ?? AGENT_DIR,
		selfPid: SELF_PID,
	});
}

async function temporaryHome(prefix: string): Promise<string> {
	return await realpath(await mkdtemp(join(tmpdir(), prefix)));
}

function linuxStat(pid: number, ppid: number, startTime: number, comm = "gjc worker ) (fixture)"): string {
	const fields = Array.from({ length: 20 }, () => "0");
	fields[0] = "S";
	fields[1] = String(ppid);
	fields[19] = String(startTime);
	return `${pid} (${comm}) ${fields.join(" ")}`;
}

test("reaper returns an empty result in a clean home", async () => {
	const home = await temporaryHome("gajaeway-process-reaper-clean-");
	try {
		const executable = join(home, "gjc", "current", "bin", "gjc");
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(executable, "");
		expect(await reapLeftoverWorkers(home, { log: () => {} })).toEqual({ found: 0, killed: 0, failed: 0 });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper tolerates a missing default GJC executable", async () => {
	const home = await temporaryHome("gajaeway-process-reaper-missing-gjc-");
	try {
		await mkdir(join(home, "broker", "agent"), { recursive: true });
		expect(await reapLeftoverWorkers(home, { log: () => {} })).toEqual({ found: 0, killed: 0, failed: 0 });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper scopes executable and agent matching to the supplied home", async () => {
	const home1 = await temporaryHome("gajaeway-reaper-home1-");
	const home2 = await temporaryHome("gajaeway-reaper-home2-");
	const executable = join(home1, "gjc", "current", "bin", "gjc");
	const agentDir = join(home1, "broker", "agent");
	const calls: number[] = [];
	try {
		await mkdir(join(home1, "gjc", "current", "bin"), { recursive: true });
		await mkdir(agentDir, { recursive: true });
		await mkdir(join(home2, "gjc", "current", "bin"), { recursive: true });
		await mkdir(join(home2, "broker", "agent"), { recursive: true });
		await writeFile(executable, "");
		const table = [worker(2401, 2, { executable, agentDir })];
		const options = {
			processTable: table,
			kill: async (pid: number) => {
				calls.push(pid);
			},
			isProcessAlive: async () => false,
			log: () => {},
		};

		expect(await reapLeftoverWorkers(home1, options)).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(await reapLeftoverWorkers(home2, options)).toEqual({ found: 0, killed: 0, failed: 0 });
		expect(calls).toEqual([2401]);
	} finally {
		await Promise.all([rm(home1, { recursive: true, force: true }), rm(home2, { recursive: true, force: true })]);
	}
});

test("reaper tolerates an unavailable or malformed proc root", async () => {
	const home = await temporaryHome("gajaeway-reaper-proc-unavailable-");
	try {
		const missing = await reapLeftoverWorkers(home, { procRoot: join(home, "missing-proc"), log: () => {} });
		const malformedRoot = join(home, "proc");
		await mkdir(join(malformedRoot, "4001"), { recursive: true });
		await writeFile(join(malformedRoot, "4001", "stat"), "invalid");
		const malformed = await reapLeftoverWorkers(home, { procRoot: malformedRoot, log: () => {} });
		expect(missing).toEqual({ found: 0, killed: 0, failed: 0 });
		expect(malformed).toEqual({ found: 0, killed: 0, failed: 0 });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("classifier selects exact relays orphaned under systemd and skips live gateway children", () => {
	const systemdPid = 1100;
	const gatewayPid = 1200;
	const table: ProcessTableEntry[] = [
		{ pid: systemdPid, ppid: 1, argv: ["/usr/lib/systemd/systemd", "--user"], agentDir: undefined },
		worker(2001, systemdPid),
		worker(2002, SELF_PID),
		{ pid: gatewayPid, ppid: 1, argv: ["/usr/local/bin/gajaeway-gateway", "daemon"], agentDir: undefined },
		worker(2003, gatewayPid),
		worker(2004, systemdPid, { agentDir: "/home/user/.gajaeway/broker/agent2" }),
		worker(2005, systemdPid, { executable: "/opt/other-gjc/bin/gjc" }),
		{
			pid: 2006,
			ppid: systemdPid,
			argv: ["bun", "unrelated.ts", "sdk", "serve", "--stdio", "--session", SESSION],
			agentDir: AGENT_DIR,
		},
		worker(2007, systemdPid, {
			argv: [GJC, "sdk", "serve", "--stdio", "--session", SESSION, "--extra"],
		}),
		worker(2008, systemdPid, { argv: [GJC, "sdk", "serve-x", "--stdio", "--session", SESSION] }),
	];

	expect(select(table)).toEqual([2001]);
});

test("classifier accepts the source-run gateway as a live parent", () => {
	const table: ProcessTableEntry[] = [
		{
			pid: 1300,
			ppid: 1,
			argv: ["/usr/local/bin/bun", "/srv/gajaeway/packages/gateway/src/main.ts", "daemon"],
			agentDir: undefined,
		},
		worker(2100, 1300),
	];

	expect(select(table)).toEqual([]);
});

test("classifier accepts only exact current and previously pinned executable identities", () => {
	const currentExecutable = `${GJC_PREFIX}/0.18.12/node_modules/@gajae-code/coding-agent/bin/gjc.js`;
	const previousExecutable = `${GJC_PREFIX}/0.18.9/node_modules/@gajae-code/coding-agent/bin/gjc.js`;
	const table = [
		worker(2201, 2, { executable: previousExecutable }),
		worker(2202, 2, { executable: currentExecutable }),
		worker(2203, 2, { executable: `${GJC_PREFIX}/current/bin/gjc` }),
		worker(2204, 2, {
			executable: "/home/other/.gajaeway/gjc/0.18.9/node_modules/@gajae-code/coding-agent/bin/gjc.js",
		}),
	];

	expect(select(table, { gjcPath: currentExecutable, gjcPrefix: GJC_PREFIX })).toEqual([2201, 2202]);
	expect(select(table, { gjcPath: "/opt/custom/gjc" })).toEqual([]);
});

test("reaper recognizes a previous GJC entrypoint after its version directory was pruned", async () => {
	const home = await temporaryHome("gajaeway-reaper-old-pin-");
	const prefix = join(home, "gjc");
	const currentPackageBin = join(prefix, "0.18.12", "node_modules", "@gajae-code", "coding-agent", "bin");
	const currentExecutable = join(prefix, "0.18.12", "bin", "gjc");
	const oldProcessExecutable = join(prefix, "0.18.9", "node_modules", "@gajae-code", "coding-agent", "bin", "gjc.js");
	const agentDir = join(home, "agent");
	const calls: Array<{ pid: number; signal: string }> = [];
	try {
		await mkdir(currentPackageBin, { recursive: true });
		await mkdir(join(prefix, "0.18.12", "bin"), { recursive: true });
		await writeFile(join(currentPackageBin, "gjc.js"), "");
		await symlink("../node_modules/@gajae-code/coding-agent/bin/gjc.js", currentExecutable);
		const currentProcessExecutable = await realpath(currentExecutable);
		const result = await reapLeftoverWorkers(home, {
			gjcExecutable: currentProcessExecutable,
			gjcPrefix: prefix,
			brokerAgentDir: agentDir,
			processTable: [
				worker(2205, 2, { executable: oldProcessExecutable, agentDir }),
				worker(2206, 2, { executable: join(prefix, "0.18.9", "bin", "unrelated"), agentDir }),
			],
			kill: async (pid, signal) => {
				calls.push({ pid, signal });
			},
			isProcessAlive: async () => false,
		});

		expect(result).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(calls).toEqual([{ pid: 2205, signal: "SIGTERM" }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("Unix ps parser preserves exact agent directories, including paths with spaces", () => {
	const spacedAgentDir = "/home/user/gajae way/agent";
	const spacedPrefix = "/home/user/gajae way/.gajaeway/gjc";
	const spacedCurrentGjc = `${spacedPrefix}/0.18.12/bin/gjc`;
	const spacedPreviousGjc = `${spacedPrefix}/0.18.9/node_modules/@gajae-code/coding-agent/bin/gjc.js`;
	const attachOnlyEnv = `${GJC_ATTACH_ONLY_ENV}=0`;
	const output = [
		"  1  300 Sun Oct 13 03:20:00 2026 /usr/lib/systemd/systemd --user",
		`300 401 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
		`300 402 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}2`,
		`300 403 Mon Oct 13 03:20:00 2026 bun ${spacedPreviousGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
		`300 404 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} --extra HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
		`300 405 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir} other`,
		`300 406 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir} OTHER=value`,
		`300 407 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin GJC_CODING_AGENT_DIR=${spacedAgentDir} ${attachOnlyEnv}`,
		`300 408 Mon Oct 13 03:20:00 2026 ${spacedCurrentGjc} sdk serve --stdio --session ${SESSION} HOME=/home/user PATH=/bin ${attachOnlyEnv} GJC_CODING_AGENT_DIR=/other GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
	].join("\n");
	const table = parseUnixProcessTable(output, spacedAgentDir);

	expect(table).toHaveLength(9);
	expect(table[1]).toEqual({
		pid: 401,
		ppid: 300,
		argv: [spacedCurrentGjc, "sdk", "serve", "--stdio", "--session", SESSION],
		agentDir: spacedAgentDir,
		identity: "Mon Oct 13 03:20:00 2026",
	});
	expect(table[3]?.argv).toEqual(["bun", spacedPreviousGjc, "sdk", "serve", "--stdio", "--session", SESSION]);
	expect(table[4]?.argv).toEqual([spacedCurrentGjc, "sdk", "serve", "--stdio", "--session", SESSION, "--extra"]);
	expect(table[5]?.agentDir).toBeUndefined();
	expect(table[6]?.agentDir).toBeUndefined();
	expect(table[7]?.agentDir).toBeUndefined();
	expect(table[8]?.agentDir).toBeUndefined();
	expect(
		select(table, {
			gjcPath: spacedCurrentGjc,
			gjcPrefix: spacedPrefix,
			brokerAgentDir: spacedAgentDir,
		}),
	).toEqual([401, 403]);
});

test("Unix ps parser preserves spaced gateway and Bun launcher paths", () => {
	const spacedAgentDir = "/home/user/gajae way/agent";
	const spacedPrefix = "/home/user/gajae way/.gajaeway/gjc";
	const spacedGjc = `${spacedPrefix}/0.18.12/bin/gjc`;
	const spacedPreviousGjc = `${spacedPrefix}/0.18.9/node_modules/@gajae-code/coding-agent/bin/gjc.js`;
	const spacedBun = "/home/user/gajae way/.bun/bin/bun";
	const spacedSource = "/home/user/gajae way/packages/gateway/src/main.ts";
	const spacedGateway = "/home/user/gajae way/.gajaeway/bin/gajaeway-gateway";
	const attachOnlyEnv = `${GJC_ATTACH_ONLY_ENV}=0`;
	const output = [
		"  1  300 Sun Oct 13 03:20:00 2026 /usr/lib/systemd/systemd --user",
		`300 501 Mon Oct 13 03:20:00 2026 ${spacedBun} ${spacedSource} daemon`,
		`501 601 Mon Oct 13 03:20:00 2026 ${spacedGjc} sdk serve --stdio --session ${SESSION} ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
		`300 502 Mon Oct 13 03:20:00 2026 ${spacedGateway} daemon`,
		`502 602 Mon Oct 13 03:20:00 2026 ${spacedGjc} sdk serve --stdio --session ${SESSION} ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
		`300 603 Mon Oct 13 03:20:00 2026 ${spacedBun} ${spacedPreviousGjc} sdk serve --stdio --session ${SESSION} ${attachOnlyEnv} GJC_CODING_AGENT_DIR=${spacedAgentDir}`,
	].join("\n");
	const table = parseUnixProcessTable(output, spacedAgentDir, { gjcPath: spacedGjc, gjcPrefix: spacedPrefix });

	expect(table[1]?.argv).toEqual([spacedBun, spacedSource, "daemon"]);
	expect(table[3]?.argv).toEqual([spacedGateway, "daemon"]);
	expect(table[5]?.argv).toEqual([
		"/home/user/gajae way/.bun/bin/bun",
		spacedPreviousGjc,
		"sdk",
		"serve",
		"--stdio",
		"--session",
		SESSION,
	]);
	expect(
		select(table, {
			gjcPath: spacedGjc,
			gjcPrefix: spacedPrefix,
			brokerAgentDir: spacedAgentDir,
		}),
	).toEqual([603]);
});

test("injected process tables require isolated signal and liveness seams", async () => {
	await expect(reapLeftoverWorkers("/tmp/gajaeway-process-reaper", { processTable: [] })).rejects.toThrow(
		"processTable injection requires kill and isProcessAlive functions",
	);
});

test("reaper canonicalizes the configured agent directory and uses a custom executable", async () => {
	const home = await temporaryHome("gajaeway-reaper-custom-");
	const targetAgent = join(home, "custom-agent");
	const agentAlias = join(home, "agent-alias");
	const customExecutable = join(home, "custom-gjc");
	const calls: Array<{ pid: number; signal: string }> = [];
	try {
		await mkdir(targetAgent);
		await symlink(targetAgent, agentAlias);
		const canonicalAgent = await realpath(targetAgent);
		const table = [worker(2301, 2, { executable: customExecutable, agentDir: canonicalAgent })];
		const result = await reapLeftoverWorkers(home, {
			gjcExecutable: customExecutable,
			brokerAgentDir: agentAlias,
			processTable: table,
			kill: async (pid, signal) => {
				calls.push({ pid, signal });
			},
			isProcessAlive: async () => false,
		});

		expect(result).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(calls).toEqual([{ pid: 2301, signal: "SIGTERM" }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("Linux proc reader parses comm with spaces and parentheses and selects a systemd-adopted worker", async () => {
	const home = await temporaryHome("gajaeway-procroot-");
	const procRoot = join(home, "proc");
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcExecutable = join(home, "gjc", "0.18.12", "bin", "gjc");
	const calls: Array<{ pid: number; signal: string }> = [];
	const writeProc = async (
		pid: number,
		ppid: number,
		startTime: number,
		argv: string[],
		environment: string[],
		comm?: string,
	) => {
		const directory = join(procRoot, String(pid));
		await mkdir(directory, { recursive: true });
		await writeFile(join(directory, "stat"), linuxStat(pid, ppid, startTime, comm));
		await writeFile(join(directory, "cmdline"), `${argv.join("\0")}\0`);
		await writeFile(join(directory, "environ"), `${environment.join("\0")}\0`);
	};
	try {
		await mkdir(brokerAgentDir, { recursive: true });
		const workerArgv = [gjcExecutable, "sdk", "serve", "--stdio", "--session", SESSION];
		await writeProc(4500, 1, 10, ["/usr/lib/systemd/systemd", "--user"], [], "systemd (user) manager");
		await writeProc(4501, 4500, 11, workerArgv, [`GJC_CODING_AGENT_DIR=${brokerAgentDir}`], "gjc relay ) (worker)");
		await writeProc(4502, process.pid, 12, workerArgv, [`GJC_CODING_AGENT_DIR=${brokerAgentDir}`]);
		await writeProc(4503, 4500, 13, workerArgv, [`GJC_CODING_AGENT_DIR=${brokerAgentDir}2`]);
		await writeProc(
			4504,
			4500,
			14,
			["/opt/other-gjc/bin/gjc", ...workerArgv.slice(1)],
			[`GJC_CODING_AGENT_DIR=${brokerAgentDir}`],
		);

		const result = await reapLeftoverWorkers(home, {
			gjcExecutable,
			brokerAgentDir,
			procRoot,
			timeoutMs: 1,
			kill: async (pid, signal) => {
				calls.push({ pid, signal });
			},
			isProcessAlive: async () => false,
		});

		expect(result).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(calls).toEqual([{ pid: 4501, signal: "SIGTERM" }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper confirms Linux worker exit through procfs", async () => {
	const home = await temporaryHome("gajaeway-reaper-proc-exit-");
	const procRoot = join(home, "proc");
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcExecutable = join(home, "gjc", "0.18.12", "bin", "gjc");
	const workerPid = 4551;
	const signals: string[] = [];
	try {
		await mkdir(brokerAgentDir, { recursive: true });
		const workerDir = join(procRoot, String(workerPid));
		await mkdir(workerDir, { recursive: true });
		await writeFile(join(workerDir, "stat"), linuxStat(workerPid, 1, 20));
		await writeFile(
			join(workerDir, "cmdline"),
			`${[gjcExecutable, "sdk", "serve", "--stdio", "--session", SESSION].join("\0")}\0`,
		);
		await writeFile(join(workerDir, "environ"), `GJC_CODING_AGENT_DIR=${brokerAgentDir}\0`);

		const result = await reapLeftoverWorkers(home, {
			gjcExecutable,
			brokerAgentDir,
			procRoot,
			timeoutMs: 0,
			kill: async (_pid, signal) => {
				signals.push(signal);
				await rm(join(workerDir, "stat"), { force: true });
			},
		});

		expect(result).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(signals).toEqual(["SIGTERM"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper sends SIGKILL when SIGTERM leaves the worker alive and counts only confirmed exits", async () => {
	const home = await temporaryHome("gajaeway-reaper-kill-escalation-");
	const agentDir = join(home, "agent");
	let alive = true;
	const signals: string[] = [];
	try {
		const result = await reapLeftoverWorkers(home, {
			gjcExecutable: GJC,
			brokerAgentDir: agentDir,
			processTable: [worker(4601, 2, { agentDir })],
			timeoutMs: 1,
			kill: async (_pid, signal) => {
				signals.push(signal);
				if (signal === "SIGKILL") alive = false;
			},
			isProcessAlive: async () => alive,
		});

		expect(result).toEqual({ found: 1, killed: 1, failed: 0 });
		expect(signals).toEqual(["SIGTERM", "SIGKILL"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper does not report a live worker as killed when both signals fail", async () => {
	const home = await temporaryHome("gajaeway-reaper-kill-failed-");
	const agentDir = join(home, "agent");
	const signals: string[] = [];
	try {
		const result = await reapLeftoverWorkers(home, {
			gjcExecutable: GJC,
			brokerAgentDir: agentDir,
			processTable: [worker(4602, 2, { agentDir })],
			timeoutMs: 0,
			kill: async (_pid, signal) => {
				signals.push(signal);
				throw new Error("EPERM");
			},
			isProcessAlive: async () => true,
			log: () => {},
		});

		expect(result).toEqual({ found: 1, killed: 0, failed: 1 });
		expect(signals).toEqual(["SIGTERM", "SIGKILL"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reaper revalidates process identity before escalating a stale PID", async () => {
	const home = await temporaryHome("gajaeway-reaper-pid-reuse-");
	const agentDir = join(home, "agent");
	const signals: string[] = [];
	let identityChecks = 0;
	try {
		const result = await reapLeftoverWorkers(home, {
			gjcExecutable: GJC,
			brokerAgentDir: agentDir,
			processTable: [worker(4603, 2, { agentDir })],
			timeoutMs: 0,
			kill: async (_pid, signal) => {
				signals.push(signal);
			},
			isProcessAlive: async () => true,
			isCurrentWorker: async () => ++identityChecks === 1,
			log: () => {},
		});

		expect(result).toEqual({ found: 1, killed: 0, failed: 1 });
		expect(signals).toEqual(["SIGTERM"]);
		expect(identityChecks).toBe(2);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test.skipIf(process.platform !== "darwin")(
	"the BSD ps scan runs on macOS and appends the environment after argv",
	async () => {
		const marker = `GJC_REAPER_PROBE_${process.pid}`;
		const child = Bun.spawn([process.execPath, "-e", "await Bun.sleep(10_000)"], {
			env: { ...process.env, [marker]: "1" },
			stdin: "ignore",
			stdout: "ignore",
			stderr: "ignore",
		});
		try {
			await Bun.sleep(200);
			const proc = Bun.spawn([...UNIX_PROCESS_TABLE_COMMAND], {
				env: { ...process.env, LC_ALL: "C" },
				stdout: "pipe",
				stderr: "ignore",
			});
			const output = await new Response(proc.stdout as ReadableStream).text();
			expect(await proc.exited).toBe(0);
			const line = output.split("\n").find((candidate) => new RegExp(`^\\s*\\d+\\s+${child.pid}\\s`).test(candidate));
			expect(line).toBeDefined();
			expect(line).toContain("await Bun.sleep(10_000)");
			expect(line).toContain(`${marker}=1`);
		} finally {
			child.kill();
			await child.exited;
		}
	},
);
