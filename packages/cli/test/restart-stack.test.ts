import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "../src/main";
import {
	effectiveRestartState,
	readRestartReceipt,
	renderRestartReceipt,
	runRestartStack,
	type ServiceProcess,
	supervisorArgv,
} from "../src/restart-stack";
import { GATEWAY_UNIT, serviceSpecs, systemdUnitPath } from "../src/services";

const DEPLOYED_AT = Date.parse("2026-08-29T06:40:00.000Z");

async function tempHome(): Promise<{ home: string; cleanup: () => Promise<void> }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-"));
	return { home, cleanup: () => rm(home, { recursive: true, force: true }) };
}

/** A launchd host whose kickstart replaces the process unless the label is in `stuck`. */
function fakeHost(stuck: ReadonlySet<string> = new Set()) {
	let clock = DEPLOYED_AT + 60_000;
	let pid = 8000;
	const ran: string[] = [];
	const processes = new Map<string, ServiceProcess>();
	for (const label of ["dev.gajaeway.gateway", "dev.gajaeway.adapter-discord", "dev.gajaeway.adapter-slack"])
		processes.set(label, { pid: pid++, startedAt: DEPLOYED_AT - 3_600_000, binary: `/opt/bin/${label}` });
	processes.set("dev.gajaeway.admin", { pid: pid++, startedAt: DEPLOYED_AT - 3_600_000, binary: "/opt/bin/admin" });
	return {
		ran,
		options: {
			platform: "darwin" as const,
			uid: 501,
			now: () => clock,
			sleep: async (ms: number) => {
				clock += ms;
			},
			verifyTimeoutMs: 5_000,
			pollMs: 1_000,
			runner: (command: readonly string[]) => {
				const label = command.at(-1)?.split("/").at(-1) ?? "";
				ran.push(label);
				if (!stuck.has(label)) {
					const previous = processes.get(label);
					if (previous) processes.set(label, { ...previous, pid: pid++, startedAt: clock + 1_000 });
				}
				return 0;
			},
			probe: async (label: string) => processes.get(label),
			binaryModifiedAt: async () => DEPLOYED_AT,
			isServiceInstalled: async () => true,
		},
	};
}

test("restarting the gateway as part of the sequence still completes the remaining labels", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const caller = Bun.spawn([process.execPath, join(import.meta.dir, "fixtures/restart-stack-caller.ts")], {
			detached: true,
			stdout: "pipe",
			stderr: "inherit",
			env: { ...process.env, GAJAEWAY_HOME: home },
		});
		// The caller is killed by the gateway restart it requested, before it can exit on its own.
		expect(await caller.exited).not.toBe(0);
		expect(caller.signalCode).toBe("SIGKILL");
		expect(await new Response(caller.stdout).text()).toContain("launched");

		const deadline = Date.now() + 4_000;
		let receipt = await readRestartReceipt(home);
		while (receipt?.finishedAt === undefined && Date.now() < deadline) {
			await Bun.sleep(50);
			receipt = await readRestartReceipt(home);
		}
		expect(receipt?.state).toBe("ok");
		expect(receipt?.steps.map((step) => [step.label, step.result])).toEqual([
			["dev.gajaeway.gateway", "ok"],
			["dev.gajaeway.adapter-discord", "ok"],
			["dev.gajaeway.adapter-slack", "ok"],
			["dev.gajaeway.admin", "ok"],
		]);
		expect((await readFile(join(home, "ran.log"), "utf8")).trim().split("\n")).toEqual([
			"dev.gajaeway.gateway",
			"dev.gajaeway.adapter-discord",
			"dev.gajaeway.adapter-slack",
			"dev.gajaeway.admin",
		]);
	} finally {
		await cleanup();
	}
});

test("a label whose process predates the deployed binary is stale, not ok, and aborts the rest", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const host = fakeHost(new Set(["dev.gajaeway.adapter-discord"]));
		const receipt = await runRestartStack({ ...host.options, home, id: "r1" });
		expect(receipt.state).toBe("stale");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "stale", "skipped", "skipped"]);
		const discord = receipt.steps[1];
		expect(discord?.detail).toBe("process started before the deployed binary was modified");
		expect(discord?.processStartedAt).toBe(new Date(DEPLOYED_AT - 3_600_000).toISOString());
		expect(discord?.binaryModifiedAt).toBe(new Date(DEPLOYED_AT).toISOString());
		expect(host.ran).toEqual(["dev.gajaeway.gateway", "dev.gajaeway.adapter-discord"]);
	} finally {
		await cleanup();
	}
});

test("a failing service-manager command fails the receipt and names the command", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const host = fakeHost();
		const receipt = await runRestartStack({
			...host.options,
			runner: (command) => (command.at(-1)?.endsWith("adapter-slack") ? 3 : host.options.runner(command)),
			home,
			id: "r2",
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "failed", "skipped"]);
		expect(receipt.steps[2]?.detail).toBe(
			"launchctl kickstart -k gui/501/dev.gajaeway.adapter-slack exited with status 3",
		);
	} finally {
		await cleanup();
	}
});

test("on systemd one gateway restart carries the dependents, each still verified", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const ran: (readonly string[])[] = [];
		let clock = DEPLOYED_AT + 60_000;
		const receipt = await runRestartStack({
			home,
			id: "r3",
			platform: "linux",
			now: () => clock,
			sleep: async (ms) => {
				clock += ms;
			},
			runner: (command) => {
				ran.push(command);
				return 0;
			},
			probe: async (label) => ({ pid: 9, startedAt: clock + 1_000, binary: `/opt/bin/${label}` }),
			binaryModifiedAt: async () => DEPLOYED_AT,
			isServiceInstalled: async () => true,
		});
		expect(ran).toEqual([["systemctl", "--user", "restart", GATEWAY_UNIT]]);
		expect(receipt.state).toBe("ok");
		expect(receipt.steps.every((step) => step.result === "ok")).toBe(true);
	} finally {
		await cleanup();
	}
});

test("the receipt survives the restart and is readable by the following turn", async () => {
	const { home, cleanup } = await tempHome();
	const lines: string[] = [];
	const originalLog = console.log;
	const previousHome = process.env.GAJAEWAY_HOME;
	const previousExit = process.exitCode;
	try {
		process.env.GAJAEWAY_HOME = home;
		const host = fakeHost(new Set(["dev.gajaeway.admin"]));
		await runRestartStack({ ...host.options, home, id: "r4" });

		console.log = (line: unknown) => lines.push(String(line));
		// A fresh invocation with no state other than $GAJAEWAY_HOME.
		await main(["ops", "restart-stack", "--status"]);
		expect(lines[0]).toBe("restart-stack r4: stale");
		expect(lines).toContain(
			"dev.gajaeway.gateway: ok pid=8004 started=2026-08-29T06:41:01.000Z binary=2026-08-29T06:40:00.000Z",
		);
		expect(lines.some((line) => line.startsWith("dev.gajaeway.admin: stale"))).toBe(true);
		expect(process.exitCode).toBe(1);
	} finally {
		console.log = originalLog;
		process.exitCode = previousExit ?? 0;
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await cleanup();
	}
});

test("a sequence whose supervisor died reads as interrupted, not running", async () => {
	const receipt = {
		id: "r5",
		state: "running" as const,
		platform: "darwin" as const,
		requestedAt: new Date(DEPLOYED_AT).toISOString(),
		supervisorPid: 2 ** 22 + 17,
		steps: [],
	};
	expect(effectiveRestartState(receipt)).toBe("interrupted");
	expect(renderRestartReceipt(receipt)[0]).toBe("restart-stack r5: interrupted");
});

test("the supervisor never uses bootout and escapes the gateway cgroup on systemd", () => {
	const worker = ["/opt/bin/gajaeway", "ops", "restart-stack", "--run", "r6"];
	expect(supervisorArgv("darwin", worker, "/h", "r6")).toEqual(worker);
	const linux = supervisorArgv("linux", worker, "/h", "r6");
	expect(linux.slice(0, 2)).toEqual(["systemd-run", "--user"]);
	expect(linux).toContain("--unit=gajaeway-restart-stack-r6");
	expect(linux.slice(-worker.length)).toEqual(worker);
	expect([...linux, ...worker]).not.toContain("bootout");
});

test("an uninstalled service is skipped and does not abort remaining services", async () => {
	const { home, cleanup } = await tempHome();
	try {
		let clock = DEPLOYED_AT + 60_000;
		let pid = 8000;
		const ran: string[] = [];
		const processes = new Map<string, ServiceProcess>();
		// Only gateway and admin are installed/running; adapter-discord is not installed
		processes.set("dev.gajaeway.gateway", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/gateway",
		});
		// adapter-discord is intentionally not in the map (not installed)
		processes.set("dev.gajaeway.adapter-slack", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/adapter-slack",
		});
		processes.set("dev.gajaeway.admin", { pid: pid++, startedAt: DEPLOYED_AT - 3_600_000, binary: "/opt/bin/admin" });

		const receipt = await runRestartStack({
			home,
			id: "r7",
			platform: "darwin",
			uid: 501,
			now: () => clock,
			sleep: async (ms: number) => {
				clock += ms;
			},
			verifyTimeoutMs: 5_000,
			pollMs: 1_000,
			runner: (command: readonly string[]) => {
				const label = command.at(-1)?.split("/").at(-1) ?? "";
				ran.push(label);
				const previous = processes.get(label);
				if (previous) processes.set(label, { ...previous, pid: pid++, startedAt: clock + 1_000 });
				return 0;
			},
			probe: async (label: string) => {
				// Return undefined for uninstalled service
				return processes.get(label);
			},
			isServiceInstalled: async (label) => {
				// Only adapter-discord is not installed
				return label !== "dev.gajaeway.adapter-discord";
			},
			binaryModifiedAt: async (_path: string) => {
				return DEPLOYED_AT;
			},
		});

		// The receipt should be ok because the uninstalled service is skipped
		expect(receipt.state).toBe("ok");
		// gateway: ok, adapter-discord: skipped (not installed), adapter-slack: ok, admin: ok
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "skipped", "ok", "ok"]);
		// The restart commands should have been run for installed services
		expect(ran).toContain("dev.gajaeway.gateway");
		expect(ran).not.toContain("dev.gajaeway.adapter-discord");
		expect(ran).toContain("dev.gajaeway.adapter-slack");
		expect(ran).toContain("dev.gajaeway.admin");
		// adapter-discord should have a skip detail
		const discord = receipt.steps[1];
		expect(discord?.detail).toContain("not installed");
	} finally {
		await cleanup();
	}
});

test("a restart ps reports one second early is ok; a process older than that is stale", async () => {
	const { home, cleanup } = await tempHome();
	try {
		const run = async (reportedOffsetMs: number) => {
			// Restart issued at 10:55:50.039; systemd starts the gateway at 10:55:50.170
			// and Linux `ps -o lstart` reports it as 10:55:49 (gaebal-gajae, 2026-09-30).
			const issuedAt = Date.parse("2026-09-30T10:55:50.039Z");
			let clock = issuedAt;
			let current: ServiceProcess = { pid: 1, startedAt: issuedAt - 3_600_000, binary: "/opt/bin/gateway" };
			return await runRestartStack({
				home,
				id: `skew-${reportedOffsetMs}`,
				platform: "linux",
				uid: 1000,
				now: () => clock,
				sleep: async (ms: number) => {
					clock += ms;
				},
				verifyTimeoutMs: 3_000,
				pollMs: 1_000,
				runner: () => {
					current = { ...current, pid: 2, startedAt: Date.parse("2026-09-30T10:55:50.000Z") + reportedOffsetMs };
					return 0;
				},
				probe: async (label) => (label === "dev.gajaeway.gateway" ? current : undefined),
				isServiceInstalled: async (label) => label === "dev.gajaeway.gateway",
				binaryModifiedAt: async () => Date.parse("2026-09-30T10:55:49.601Z"),
			});
		};
		expect((await run(-1_000)).steps[0]?.result).toBe("ok");
		expect((await run(-2_000)).steps[0]?.result).toBe("stale");
	} finally {
		await cleanup();
	}
});

test("default check skips services not in the unit dir", async () => {
	const { home: gajaewayHome, cleanup: cleanupGajaeway } = await tempHome();
	const { home: unitDir, cleanup: cleanupUnitDir } = await tempHome();
	try {
		let clock = DEPLOYED_AT + 60_000;
		let pid = 8000;
		const ran: string[] = [];
		const processes = new Map<string, ServiceProcess>();
		// All services are running
		processes.set("dev.gajaeway.gateway", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/gateway",
		});
		processes.set("dev.gajaeway.adapter-discord", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/adapter-discord",
		});
		processes.set("dev.gajaeway.adapter-slack", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/adapter-slack",
		});
		processes.set("dev.gajaeway.admin", {
			pid: pid++,
			startedAt: DEPLOYED_AT - 3_600_000,
			binary: "/opt/bin/admin",
		});

		// Only create unit file for gateway
		const gatewaySpec = serviceSpecs().find((s) => s.id === "gateway");
		if (!gatewaySpec) throw new Error("gateway spec not found");
		const gatewayUnitPath = systemdUnitPath(gatewaySpec, unitDir);
		await writeFile(gatewayUnitPath, "[Unit]\nDescription=test\n", { encoding: "utf8" });

		const receipt = await runRestartStack({
			home: gajaewayHome,
			id: "r8",
			platform: "linux",
			now: () => clock,
			sleep: async (ms: number) => {
				clock += ms;
			},
			verifyTimeoutMs: 5_000,
			pollMs: 1_000,
			runner: (command: readonly string[]) => {
				ran.push(command.join(" "));
				// Restart the gateway by updating its process start time
				const previous = processes.get("dev.gajaeway.gateway");
				if (previous) processes.set("dev.gajaeway.gateway", { ...previous, pid: pid++, startedAt: clock + 1_000 });
				return 0;
			},
			probe: async (label: string) => {
				return processes.get(label);
			},
			binaryModifiedAt: async () => DEPLOYED_AT,
			env: { HOME: "/home/testuser" },
			unitDir,
		});

		// The receipt should be ok because uninstalled services are skipped
		expect(receipt.state).toBe("ok");
		// gateway: ok, others: skipped (not in unitDir)
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "skipped", "skipped", "skipped"]);
		// Only the systemctl restart command should have run (for gateway)
		expect(ran).toHaveLength(1);
		expect(ran[0]).toContain("systemctl");
		expect(ran[0]).toContain(GATEWAY_UNIT);
		// Adapters and admin should have skip details
		const discord = receipt.steps[1];
		expect(discord?.detail).toContain("not installed");
		const slack = receipt.steps[2];
		expect(slack?.detail).toContain("not installed");
		const admin = receipt.steps[3];
		expect(admin?.detail).toContain("not installed");
	} finally {
		await cleanupGajaeway();
		await cleanupUnitDir();
	}
});
