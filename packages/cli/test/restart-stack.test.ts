import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
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
import { GATEWAY_UNIT } from "../src/services";

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
