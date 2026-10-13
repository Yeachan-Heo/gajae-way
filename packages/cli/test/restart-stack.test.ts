import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";

import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, type Lease, type LeaseName, leaseHeld } from "@gajae-gateway/home";

import { main } from "../src/main";
import {
	defaultServiceProbe,
	effectiveRestartState,
	launchRestartStack,
	readRestartReceipt,
	renderRestartReceipt,
	runRestartStack,
	supervisorArgv,
} from "../src/restart-stack";

import { serviceSpecs, systemdUnitName, systemdUnitPath } from "../src/services";

const DEPLOYED_AT = Date.parse("2026-08-29T06:40:00.000Z");
const BIN_DIR = "/opt/gajaeway/bin";
const LEASES: Partial<Record<string, LeaseName>> = {
	broker: "broker",
	gateway: "gateway",
	"adapter-discord": "adapter-discord",
	"adapter-slack": "adapter-slack",
	"adapter-telegram": "adapter-telegram",
};

async function tempHome(): Promise<{ home: string; cleanup: () => Promise<void> }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-"));
	const provisionDir = join(home, "broker", "provision");
	await mkdir(provisionDir, { recursive: true });
	await writeFile(join(provisionDir, "manifest.json"), "{}\\n", "utf8");
	return { home, cleanup: () => rm(home, { recursive: true, force: true }) };
}

async function fakeHost(options: {
	home: string;
	platform?: "darwin" | "linux";
	stuck?: ReadonlySet<string>;
	fail?: string;
	installed?: (label: string) => Promise<boolean>;
}) {
	let clock = Date.now();
	const ran: string[] = [];
	const binaryPaths: string[] = [];
	const pids = new Map<string, number>();
	const leases = new Map<LeaseName, Lease>();
	for (const spec of serviceSpecs()) pids.set(spec.label, spec.id === "admin" ? 7000 : process.pid);

	const restart = async (id: string): Promise<void> => {
		const leaseName = LEASES[id];
		if (leaseName !== undefined) {
			await leases.get(leaseName)?.release();
			leases.set(leaseName, await acquireLease(options.home, leaseName));
		}
		const spec = serviceSpecs().find((candidate) => candidate.id === id);
		if (spec !== undefined && id === "admin") pids.set(spec.label, (pids.get(spec.label) ?? 0) + 1);
		clock = Date.now();
	};

	return {
		ran,
		binaryPaths,
		close: async () => {
			for (const lease of leases.values()) await lease.release();
		},
		options: {
			home: options.home,
			id: "test-run",
			platform: options.platform ?? "darwin",
			uid: 501,
			binDir: BIN_DIR,
			now: () => clock,
			sleep: async (ms: number) => {
				clock += ms;
			},
			verifyTimeoutMs: 5_000,
			pollMs: 1_000,
			runner: async (command: readonly string[]) => {
				const commandTarget = command.at(-1) ?? "";
				const spec = serviceSpecs().find(
					(candidate) =>
						candidate.label === commandTarget.split("/").at(-1) || systemdUnitName(candidate) === commandTarget,
				);
				const label = spec?.label ?? commandTarget;
				ran.push(label);
				if (label === options.fail) return 3;
				if (options.platform === "linux" && spec?.id === "gateway") {
					for (const candidate of serviceSpecs()) await restart(candidate.id);
				} else if (spec !== undefined && !options.stuck?.has(label)) await restart(spec.id);
				return 0;
			},
			probe: async (label: string) => {
				const pid = pids.get(label);
				return pid === undefined ? undefined : { pid };
			},
			binaryModifiedAt: async (path: string) => {
				binaryPaths.push(path);
				return DEPLOYED_AT;
			},
			isServiceInstalled: options.installed ?? (async () => true),
		},
	};
}
test("defaultServiceProbe reads MainPID from launchd and systemd", async () => {
	const { home, cleanup } = await tempHome();
	const binDir = join(home, "bin");
	await mkdir(binDir);
	const argsPath = join(home, "probe-args");
	const previousPath = process.env.PATH;
	const previousArgsPath = process.env.PROBE_ARGS;
	try {
		const launchctl = join(binDir, "launchctl");
		await writeFile(
			launchctl,
			"#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$PROBE_ARGS\"\nprintf 'state = running\\n pid = 4242;\\n'\n",
		);
		await chmod(launchctl, 0o755);
		const systemctl = join(binDir, "systemctl");
		await writeFile(systemctl, "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$PROBE_ARGS\"\nprintf '5252\\n'\n");
		await chmod(systemctl, 0o755);
		process.env.PATH = `${binDir}:${previousPath ?? ""}`;
		process.env.PROBE_ARGS = argsPath;

		const gateway = serviceSpecs().find((spec) => spec.id === "gateway");
		if (!gateway) throw new Error("gateway service spec not found");
		expect(await defaultServiceProbe("darwin", 501)(gateway.label)).toEqual({ pid: 4242 });
		expect((await Bun.file(argsPath).text()).trim()).toBe(`print gui/501/${gateway.label}`);
		expect(await defaultServiceProbe("linux", 1000)(gateway.label)).toEqual({ pid: 5252 });
		expect((await Bun.file(argsPath).text()).trim()).toBe(`--user show -p MainPID --value ${systemdUnitName(gateway)}`);
	} finally {
		if (previousPath === undefined) delete process.env.PATH;
		else process.env.PATH = previousPath;
		if (previousArgsPath === undefined) delete process.env.PROBE_ARGS;
		else process.env.PROBE_ARGS = previousArgsPath;
		await cleanup();
	}
});

test("restarting the gateway as part of the sequence still completes the remaining labels", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const caller = Bun.spawn([process.execPath, join(import.meta.dir, "fixtures/restart-stack-caller.ts")], {
			detached: true,
			stdout: "pipe",
			stderr: "inherit",
			env: { ...process.env, GAJAEWAY_HOME: home },
		});
		expect(await caller.exited).not.toBe(0);
		expect(caller.signalCode).toBe("SIGKILL");
		expect(await new Response(caller.stdout).text()).toContain("launched");

		const deadline = Date.now() + 5_000;
		let receipt = await readRestartReceipt(home);
		while (receipt?.finishedAt === undefined && Date.now() < deadline) {
			await Bun.sleep(50);
			receipt = await readRestartReceipt(home);
		}
		expect(receipt?.state).toBe("ok");
		expect(receipt?.steps.map((step) => [step.label, step.result])).toEqual(
			serviceSpecs().map((spec) => [spec.label, "ok"]),
		);
		expect((await Bun.file(join(home, "ran.log")).text()).trim().split("\n")).toEqual(
			serviceSpecs().map((spec) => spec.label),
		);
	} finally {
		await cleanup();
	}
});

test("lease owner PID and start time verify a restarted service", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	try {
		const receipt = await runRestartStack({ ...host.options, id: "lease-success" });
		expect(await leaseHeld(home, "update")).toBe(false);
		expect(receipt.state).toBe("ok");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "ok", "ok", "ok", "ok"]);
		expect(receipt.steps[0]?.pid).toBe(process.pid);
		expect(Date.parse(receipt.steps[0]?.processStartedAt ?? "")).toBeGreaterThanOrEqual(DEPLOYED_AT);
		expect(host.binaryPaths).toContain(join(BIN_DIR, "gajaeway-gateway"));
	} finally {
		await host.close();
		await cleanup();
	}
});

test("a matching MainPID with a lease owner predating the restart is stale", async () => {
	const { home, cleanup } = await tempHome();
	const holder = await acquireLease(home, "adapter-discord");
	const host = await fakeHost({ home, stuck: new Set(["dev.gajaeway.adapter-discord"]) });
	try {
		await Bun.sleep(2);
		const receipt = await runRestartStack({ ...host.options, id: "lease-stale" });
		expect(receipt.state).toBe("stale");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "stale", "skipped", "skipped", "skipped"]);
		expect(receipt.steps[2]?.detail).toBe("lease owner predates this restart sequence");
		expect(host.ran).toEqual(["dev.gajaeway.broker", "dev.gajaeway.gateway", "dev.gajaeway.adapter-discord"]);
	} finally {
		await host.close();
		await holder.release();
		await cleanup();
	}
});

test("lease verification stats only SERVICE_SPECS binaries, never zombie ps output", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	try {
		const receipt = await runRestartStack({ ...host.options, id: "zombie-path" });
		expect(receipt.state).toBe("ok");
		expect(host.binaryPaths).toContain(join(BIN_DIR, "gajaeway-gateway"));
		expect(host.binaryPaths.some((path) => path.includes("[gajaeway-gatewa]"))).toBe(false);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("a failed service-manager command fails the receipt and skips later units", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, fail: "dev.gajaeway.adapter-slack" });
	try {
		const receipt = await runRestartStack({ ...host.options, id: "failed-command" });
		expect(receipt.state).toBe("failed");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "ok", "failed", "skipped", "skipped"]);
		expect(receipt.steps[3]?.detail).toBe(
			"launchctl kickstart -k gui/501/dev.gajaeway.adapter-slack exited with status 3",
		);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("systemd gateway restart verifies dependent services through their leases", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, platform: "linux" });
	try {
		const receipt = await runRestartStack({ ...host.options, id: "systemd-restart" });
		expect(host.ran).toEqual(["dev.gajaeway.broker", "dev.gajaeway.gateway"]);

		expect(receipt.state).toBe("ok");
		expect(receipt.steps.every((step) => step.result === "ok")).toBe(true);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("runRestartStack and launchRestartStack refuse while the update lease is held", async () => {
	const { home, cleanup } = await tempHome();
	const holder = await acquireLease(home, "update");
	try {
		await expect(runRestartStack({ home, id: "blocked" })).rejects.toThrow("lease_held name=update");
		await expect(launchRestartStack({ home, worker: ["/bin/true"] })).rejects.toThrow("lease_held name=update");
		expect(await Bun.file(join(home, "restart-stack.json")).exists()).toBe(false);
	} finally {
		await holder.release();
		await cleanup();
	}
});
test("restart-stack CLI exits 1 when an update owns the lease", async () => {
	const { home, cleanup } = await tempHome();
	const previousHome = process.env.GAJAEWAY_HOME;
	const previousExitCode = process.exitCode;
	const originalError = console.error;
	const errors: string[] = [];
	const holder = await acquireLease(home, "update");
	try {
		process.env.GAJAEWAY_HOME = home;
		console.error = (message: unknown) => errors.push(String(message));
		await main(["ops", "restart-stack", "--run", "held"]);
		expect(process.exitCode).toBe(1);
		expect(errors.join("\n")).toContain("lease_held name=update");
		expect(await Bun.file(join(home, "restart-stack.json")).exists()).toBe(false);
	} finally {
		console.error = originalError;
		process.exitCode = previousExitCode ?? 0;

		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await holder.release();
		await cleanup();
	}
});

test("launchRestartStack queues a detached worker and preserves the home", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const queued = await launchRestartStack({ home, worker: [process.execPath, "-e", "process.exit(0)"] });
		expect(queued.receipt.state).toBe("queued");
		expect(queued.receipt.trigger).toBe("manual");
		expect(queued.supervisorPid).toBeGreaterThan(0);
		const update = await launchRestartStack({
			home,
			trigger: "update",
			worker: [process.execPath, "-e", "process.exit(0)"],
		});
		expect(update.receipt.trigger).toBe("update");
		expect((await readRestartReceipt(home))?.trigger).toBe("update");
	} finally {
		await cleanup();
	}
});

test("restart status reads the persisted lease diagnostics without a gateway connection", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, stuck: new Set(["dev.gajaeway.admin"]) });
	const previousHome = process.env.GAJAEWAY_HOME;
	const previousExitCode = process.exitCode;
	const originalLog = console.log;
	const lines: string[] = [];
	try {
		process.env.GAJAEWAY_HOME = home;
		await runRestartStack({ ...host.options, id: "status-receipt" });
		console.log = (line: unknown) => lines.push(String(line));
		await main(["ops", "restart-stack", "--status"]);
		expect(lines[0]).toBe("restart-stack status-receipt: stale");
		expect(lines.some((line) => line.startsWith(`dev.gajaeway.gateway: ok pid=${process.pid} started=`))).toBe(true);
		expect(process.exitCode).toBe(1);
	} finally {
		console.log = originalLog;
		process.exitCode = previousExitCode ?? 0;
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await host.close();
		await cleanup();
	}
});

test("update-triggered broker restart provisions and verifies while manual restarts never provision", async () => {
	for (const provisionStatus of [0, 1, 3, 4]) {
		const { home, cleanup } = await tempHome();
		let brokerLease: Lease | undefined = await acquireLease(home, "broker");
		let running = true;
		const commands: string[][] = [];
		try {
			const receipt = await runRestartStack({
				home,
				id: `update-${provisionStatus}`,
				trigger: "update",
				platform: "darwin",
				uid: 501,
				binDir: BIN_DIR,
				runner: async (command) => {
					commands.push([...command]);
					if (command.includes("provision")) return provisionStatus;
					if (command[1] === "bootout") {
						await brokerLease?.release();
						brokerLease = undefined;
						running = false;
						return 0;
					}
					if (command[1] === "bootstrap") {
						brokerLease = await acquireLease(home, "broker");
						running = true;
						return 0;
					}
					return 0;
				},
				probe: async (label) => (label === "dev.gajaeway.broker" && running ? { pid: process.pid } : undefined),
				isServiceInstalled: async (label) => label === "dev.gajaeway.broker",
				binaryModifiedAt: async () => Date.now() - 1000,
				verifyTimeoutMs: 100,
				sleep: async () => {},
			});
			const provision = commands.find((command) => command.includes("provision"));
			expect(provision).toEqual([
				join(BIN_DIR, "gajaeway-gateway"),
				"broker-agent",
				"provision",
				"--mode",
				"update",
				"--lease-wait-ms",
				"35000",
			]);
			const brokerStep = receipt.steps[0];
			expect(brokerStep?.agentSync?.exitStatus).toBe(provisionStatus);
			if (provisionStatus === 4) {
				expect(receipt.state).toBe("failed");
				expect(commands.some((command) => command[1] === "bootstrap")).toBe(false);
				expect(brokerStep?.agentSync?.error).toBe("broker_agent_unrecoverable");
			} else {
				expect(receipt.state).toBe("ok");
				expect(commands.at(-1)?.[1]).toBe("bootstrap");
				expect(brokerStep?.result).toBe("ok");
				expect(brokerStep?.agentSync?.error).toBe(
					provisionStatus === 0 ? undefined : `broker-agent provision exited with status ${provisionStatus}`,
				);
			}
		} finally {
			await brokerLease?.release();
			await cleanup();
		}
	}
});

test("manual broker restarts do not run provisioning", async () => {
	const { home, cleanup } = await tempHome();
	let brokerLease: Lease | undefined = await acquireLease(home, "broker");
	const commands: string[][] = [];
	try {
		const receipt = await runRestartStack({
			home,
			id: "manual-broker",
			trigger: "manual",
			platform: "darwin",
			uid: 501,
			runner: async (command) => {
				commands.push([...command]);
				if (command.at(-1) === "gui/501/dev.gajaeway.broker") {
					await brokerLease?.release();
					brokerLease = await acquireLease(home, "broker");
				}
				return 0;
			},
			probe: async (label) => (label === "dev.gajaeway.broker" ? { pid: process.pid } : undefined),
			isServiceInstalled: async (label) => label === "dev.gajaeway.broker",
			binaryModifiedAt: async () => Date.now() - 1000,
			verifyTimeoutMs: 100,
			sleep: async () => {},
		});
		expect(receipt.state).toBe("ok");
		expect(commands.some((command) => command.includes("provision"))).toBe(false);
		expect(receipt.steps[0]?.agentSync).toBeUndefined();
	} finally {
		await brokerLease?.release();
		await cleanup();
	}
});
test("an unfinished receipt is interrupted when its update lease is gone", async () => {
	const { home, cleanup } = await tempHome();
	const receipt = {
		id: "interrupted",
		state: "running" as const,
		trigger: "manual" as const,
		platform: "darwin" as const,
		requestedAt: new Date(DEPLOYED_AT).toISOString(),
		steps: [],
	};
	try {
		expect(await effectiveRestartState(receipt, home)).toBe("interrupted");
		expect(renderRestartReceipt(receipt, "interrupted")[0]).toBe("restart-stack interrupted: interrupted");
	} finally {
		await cleanup();
	}
});

test("a supervisor that cannot start fails the receipt instead of leaving it queued", async () => {
	const { home, cleanup } = await tempHome();
	try {
		// A container has no systemd-run: the spawn itself fails.
		await expect(
			launchRestartStack({ home, platform: "linux", env: { PATH: join(home, "no-such-bin") } }),
		).rejects.toThrow(/restart-stack .*: failed: supervisor systemd-run could not be started/);
		const receipt = await readRestartReceipt(home);
		expect(receipt?.state).toBe("failed");
		expect(receipt?.finishedAt).toBeDefined();
		expect(receipt?.steps.every((step) => step.result === "skipped")).toBe(true);
		const rendered = renderRestartReceipt(receipt!);
		expect(rendered[0]).toMatch(/: failed$/);
		expect(rendered).toContainEqual(expect.stringContaining("detail: supervisor systemd-run could not be started"));
	} finally {
		await cleanup();
	}
});

test("an uninstalled service is skipped and does not abort the remaining stack", async () => {
	const { home, cleanup } = await tempHome();
	const unitDir = await mkdtemp(join(tmpdir(), "gajaeway-restart-units-"));
	try {
		const gatewaySpec = serviceSpecs().find((spec) => spec.id === "gateway");
		if (!gatewaySpec) throw new Error("gateway service spec not found");
		await writeFile(systemdUnitPath(gatewaySpec, unitDir), "[Unit]\nDescription=test\n");
		const processes = new Map<string, number>([[gatewaySpec.label, process.pid]]);
		let gatewayLease: Lease | undefined;

		try {
			const receipt = await runRestartStack({
				home,
				id: "uninstalled",
				platform: "linux",
				binDir: BIN_DIR,
				env: { HOME: "/home/testuser" },

				unitDir,
				runner: async () => {
					await gatewayLease?.release();
					gatewayLease = await acquireLease(home, "gateway");
					return 0;
				},

				probe: async (label) => {
					const pid = processes.get(label);
					return pid === undefined ? undefined : { pid };
				},
				binaryModifiedAt: async () => DEPLOYED_AT,
				sleep: async () => {},
				now: () => Date.now(),
			});
			expect(receipt.state).toBe("ok");
			expect(receipt.steps.map((step) => step.result)).toEqual([
				"skipped",
				"ok",
				"skipped",
				"skipped",
				"skipped",
				"skipped",
			]);
		} finally {
			await gatewayLease?.release();
		}
	} finally {
		await cleanup();
		await rm(unitDir, { recursive: true, force: true });
	}
});

test("supervisorArgv escapes the gateway cgroup on systemd without bootout", () => {
	const worker = ["/opt/bin/gajaeway", "ops", "restart-stack", "--run", "r1"];
	expect(supervisorArgv("darwin", worker, "/h", "r1")).toEqual(worker);
	const linux = supervisorArgv("linux", worker, "/h", "r1", BIN_DIR);

	expect(linux.slice(0, 2)).toEqual(["systemd-run", "--user"]);
	expect(linux).toContain("--unit=gajaeway-restart-stack-r1");
	expect(linux).toContain(`--setenv=GAJAEWAY_BIN_DIR=${BIN_DIR}`);
	expect(linux.slice(-worker.length)).toEqual(worker);
	expect([...linux, ...worker]).not.toContain("bootout");
});
