import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";

import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, type Lease, type LeaseName, leaseHeld } from "@gajae-gateway/home";
import { ProtocolError } from "@gajae-gateway/protocol";

import { main } from "../src/main";
import {
	defaultServiceProbe,
	effectiveRestartState,
	launchRestartStack,
	parseSystemdJobs,
	readRestartReceipt,
	recoverRestartStack,
	renderRestartReceipt,
	runRestartStack,
	supervisorArgv,
} from "../src/restart-stack";

import { launchAgentPlistPath, restartOrder, serviceSpecs, systemdUnitName, systemdUnitPath } from "../src/services";

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
	await writeFile(join(provisionDir, "manifest.json"), "{}\n", "utf8");
	// Installed adapters are units that can actually start; the unconfigured case has its
	// own test and removes one of these files.
	for (const adapter of ["discord", "slack", "telegram"])
		await writeFile(join(home, `adapter-${adapter}.json`), "{}\n", "utf8");
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
			env: { ...process.env, HOME: options.home },
			launchAgentsDir: join(options.home, "agents"),
			unitDir: join(options.home, "units"),
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

test("systemd job output is parsed without mistaking the empty summary for a job", () => {
	expect(parseSystemdJobs("No jobs running.\n")).toEqual([]);
	expect(parseSystemdJobs(" 42 gajaeway-gateway.service restart running\n")).toEqual([
		{ id: 42, unit: "gajaeway-gateway.service" },
	]);
});

test("a timed-out systemd command continues only after the manager reports no matching jobs", async () => {
	const { home, cleanup } = await tempHome();
	const binDir = join(home, "fake-bin");
	await mkdir(binDir);
	const systemctl = join(binDir, "systemctl");
	await writeFile(
		systemctl,
		'#!/bin/sh\ncase "$2" in\n  restart) exec /bin/sleep 30 ;;\n  list-jobs) printf "No jobs running.\\n" ;;\n  *) exit 0 ;;\nesac\n',
	);
	await chmod(systemctl, 0o755);
	const host = await fakeHost({ home, platform: "linux" });
	const previousPath = process.env.PATH;
	try {
		process.env.PATH = `${binDir}:${previousPath ?? ""}`;
		const receipt = await runRestartStack({
			...host.options,
			platform: "linux",
			runner: undefined,
			commandTimeoutMs: 20,
		});
		expect(receipt.state).toBe("stale");
		expect(receipt.steps[0]?.result).toBe("stale");
		expect(receipt.steps[0]?.detail).toContain("no lease owner after restart");
	} finally {
		if (previousPath === undefined) delete process.env.PATH;
		else process.env.PATH = previousPath;
		await host.close();
		await cleanup();
	}
});

test("a timed-out launchd command retains the drain until explicit operator recovery", async () => {
	const { home, cleanup } = await tempHome();
	const binDir = join(home, "fake-bin");
	await mkdir(binDir);
	const pidPath = join(home, "child.pid");
	const launchctl = join(binDir, "launchctl");
	await writeFile(launchctl, `#!/bin/sh\nprintf '%s\\n' "$$" > "$RESTART_CHILD_PID"\nexec /bin/sleep 30\n`);
	await chmod(launchctl, 0o755);
	const host = await fakeHost({ home });
	const previousPath = process.env.PATH;
	const previousPidPath = process.env.RESTART_CHILD_PID;
	let cancelled = false;
	let closed = false;
	try {
		process.env.PATH = `${binDir}:${previousPath ?? ""}`;
		process.env.RESTART_CHILD_PID = pidPath;
		const receipt = await runRestartStack({
			...host.options,
			id: "launchd-command-timeout",
			runner: undefined,
			commandTimeoutMs: 20,
			prepareRestartDrain: async () => ({
				superseded: new AbortController().signal,
				assertActive: async () => {},
				cancel: async () => {
					cancelled = true;
				},
				close: async () => {
					closed = true;
				},
			}),
		});
		expect(receipt.state).toBe("indeterminate");
		expect(receipt.detail).toContain("launchd operation could not be confirmed");
		expect(cancelled).toBe(false);
		expect(closed).toBe(true);
		const childPid = Number(await Bun.file(pidPath).text());
		expect(() => process.kill(childPid, 0)).toThrow();
		expect(await leaseHeld(home, "update")).toBe(false);
		expect((await readRestartReceipt(home))?.state).toBe("indeterminate");
		const recovered = await recoverRestartStack({
			home,
			confirmServiceManagerSettled: true,
		});
		expect(recovered.state).toBe("failed");
		expect(recovered.detail).toContain("operator recovered");
	} finally {
		if (previousPath === undefined) delete process.env.PATH;
		else process.env.PATH = previousPath;
		if (previousPidPath === undefined) delete process.env.RESTART_CHILD_PID;
		else process.env.RESTART_CHILD_PID = previousPidPath;
		await host.close();
		await cleanup();
	}
});

test("an unverified gateway restart keeps the drain recoverable", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, fail: "dev.gajaeway.gateway" });
	let cancelled = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "unverified-gateway-restart",
			prepareRestartDrain: async () => ({
				superseded: new AbortController().signal,
				assertActive: async () => {},
				cancel: async () => {
					cancelled = true;
				},
				close: async () => {},
			}),
		});
		expect(receipt.state).toBe("indeterminate");
		expect(receipt.detail).toContain("gateway restart did not verify");
		expect(cancelled).toBe(false);
		expect(await leaseHeld(home, "update")).toBe(false);
		const gatewayLease = await acquireLease(home, "gateway");
		try {
			await expect(recoverRestartStack({ home, confirmServiceManagerSettled: true })).rejects.toThrow(
				"gateway lease is still held but its socket is unavailable",
			);
			expect((await readRestartReceipt(home))?.state).toBe("indeterminate");
		} finally {
			await gatewayLease.release();
		}
		const recovered = await recoverRestartStack({ home, confirmServiceManagerSettled: true });
		expect(recovered.state).toBe("failed");
	} finally {
		await host.close();
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
			restartOrder("darwin").map((spec) => [spec.label, "ok"]),
		);
		expect((await Bun.file(join(home, "ran.log")).text()).trim().split("\n")).toEqual(
			restartOrder("darwin").map((spec) => spec.label),
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

test("restart-stack waits for persona drain before issuing service restarts", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	const started = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let closed = false;
	let reattached = false;
	try {
		const running = runRestartStack({
			...host.options,
			id: "drain-before-restart",
			gatewaySocketPath: join(home, "custom-gateway.sock"),
			prepareRestartDrain: async (socketPath, restartId, _trigger, prepareOptions) => {
				if (prepareOptions?.mode === "reattach") {
					reattached = true;
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {},
						close: async () => {},
					};
				}
				expect(socketPath).toBe(join(home, "custom-gateway.sock"));
				expect(restartId).toBe("drain-before-restart");
				started.resolve();
				await release.promise;
				return {
					superseded: new AbortController().signal,
					assertActive: async () => {},
					cancel: async () => {
						throw new Error("successful gateway restart must not cancel its drain");
					},
					close: async () => {
						closed = true;
					},
				};
			},
		});
		await started.promise;
		expect(host.ran).toEqual([]);
		release.resolve();
		const receipt = await running;
		expect(receipt.state).toBe("ok");
		expect(host.ran).toContain("dev.gajaeway.broker");
		expect(closed).toBe(true);
		expect(reattached).toBe(true);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack retries drain reattachment while the replacement gateway socket starts", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	let reattachAttempts = 0;
	let replacementCancelled = false;
	let originalCancelled = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "retry-gateway-reattach",
			prepareRestartDrain: async (_socketPath, _restartId, _trigger, prepareOptions) => {
				if (prepareOptions?.mode === "reattach") {
					reattachAttempts++;
					if (reattachAttempts < 3) return undefined;
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {
							replacementCancelled = true;
						},
						close: async () => {},
					};
				}
				return {
					superseded: new AbortController().signal,
					assertActive: async () => {},
					cancel: async () => {
						originalCancelled = true;
					},
					close: async () => {},
				};
			},
		});
		expect(receipt.state).toBe("ok");
		expect(reattachAttempts).toBe(3);
		expect(replacementCancelled).toBe(true);
		expect(originalCancelled).toBe(false);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack stays in progress until the replacement gateway confirms drain release", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	const reattachStarted = Promise.withResolvers<void>();
	const finishReattach = Promise.withResolvers<void>();
	try {
		const running = runRestartStack({
			...host.options,
			id: "restart-drain-status",
			prepareRestartDrain: async (_socketPath, _restartId, _trigger, prepareOptions) => {
				if (prepareOptions?.mode === "reattach") {
					reattachStarted.resolve();
					await finishReattach.promise;
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {},
						close: async () => {},
					};
				}
				return {
					superseded: new AbortController().signal,
					assertActive: async () => {},
					cancel: async () => {
						throw new Error("successful gateway restart must not cancel its drain");
					},
					close: async () => {},
				};
			},
		});
		await reattachStarted.promise;
		const inProgress = await readRestartReceipt(home);
		if (inProgress === undefined) throw new Error("restart receipt was not persisted before reattachment");
		expect(inProgress.state).toBe("running");
		expect(inProgress.finishedAt).toBeUndefined();
		expect(await effectiveRestartState(inProgress, home)).toBe("running");

		finishReattach.resolve();
		const receipt = await running;
		expect(receipt.state).toBe("ok");
		expect(receipt.finishedAt).toBeDefined();
	} finally {
		finishReattach.resolve();
		await host.close();
		await cleanup();
	}
});

test("restart-stack keeps the receipt indeterminate when the replacement socket never becomes ready", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	let reattachAttempts = 0;
	let originalCancelled = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "gateway-reattach-timeout",
			pollMs: 10,
			verifyTimeoutMs: 20,
			prepareRestartDrain: async (_socketPath, _restartId, _trigger, prepareOptions) => {
				if (prepareOptions?.mode === "reattach") {
					reattachAttempts++;
					return undefined;
				}
				return {
					superseded: new AbortController().signal,
					assertActive: async () => {},
					cancel: async () => {
						originalCancelled = true;
					},
					close: async () => {},
				};
			},
		});
		expect(receipt.state).toBe("indeterminate");
		expect(receipt.detail).toContain("replacement gateway socket did not become ready");
		expect(reattachAttempts).toBe(3);
		expect(originalCancelled).toBe(false);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack waits for an in-flight service command before releasing a superseded drain", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	const commandStarted = Promise.withResolvers<void>();
	const finishCommand = Promise.withResolvers<void>();
	const superseded = new AbortController();
	const commands: string[][] = [];
	let commandFinished = false;
	let drainCancelled = false;
	let drainClosed = false;
	try {
		const running = runRestartStack({
			...host.options,
			id: "settle-command-before-takeover",
			binDir: join(home, "bin"),
			prepareRestartDrain: async () => ({
				superseded: superseded.signal,
				assertActive: async () => {},
				cancel: async () => {
					expect(commandFinished).toBe(true);
					drainCancelled = true;
				},
				close: async () => {
					expect(commandFinished).toBe(true);
					drainClosed = true;
				},
			}),
			runner: async (command) => {
				commands.push([...command]);
				commandStarted.resolve();
				await finishCommand.promise;
				commandFinished = true;
				return 0;
			},
		});
		await commandStarted.promise;
		superseded.abort(new Error("owner restart requested"));
		await Bun.sleep(10);
		expect(drainCancelled).toBe(false);
		expect(drainClosed).toBe(false);
		expect(commands).toHaveLength(1);
		finishCommand.resolve();
		const receipt = await running;
		expect(receipt.state).toBe("failed");
		expect(receipt.steps[0]?.detail).toContain("owner restart requested");
		expect(receipt.steps.slice(1).every((step) => step.result === "skipped")).toBe(true);
		expect(drainCancelled).toBe(true);
		expect(drainClosed).toBe(true);
		expect(commands).toHaveLength(1);
	} finally {
		finishCommand.resolve();
		await host.close();
		await cleanup();
	}
});

test("macOS restarts gateway last so the old gateway holds the drain through adapter restarts", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	const order: string[] = [];
	let drainClosed = false;
	let reattached = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "gateway-last-for-drain",
			prepareRestartDrain: async (_socketPath, _restartId, _trigger, prepareOptions) => {
				if (prepareOptions?.mode === "reattach") {
					reattached = true;
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {},
						close: async () => {},
					};
				}
				return {
					superseded: new AbortController().signal,
					assertActive: async () => {},
					cancel: async () => {
						throw new Error("successful gateway restart must not cancel its drain");
					},
					close: async () => {
						drainClosed = true;
					},
				};
			},
			runner: async (command) => {
				const label = command.at(-1)?.split("/").at(-1);
				if (!label) throw new Error("restart command has no service label");
				order.push(label);
				if (label === "dev.gajaeway.gateway") {
					expect(order).toEqual([
						"dev.gajaeway.broker",
						"dev.gajaeway.adapter-discord",
						"dev.gajaeway.adapter-slack",
						"dev.gajaeway.adapter-telegram",
						"dev.gajaeway.admin",
						"dev.gajaeway.gateway",
					]);
					expect(drainClosed).toBe(false);
				}
				return await host.options.runner(command);
			},
		});
		expect(receipt.state).toBe("ok");
		expect(order.at(-1)).toBe("dev.gajaeway.gateway");
		expect(drainClosed).toBe(true);
		expect(reattached).toBe(true);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack fences a stale supervisor before restarting the gateway", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	let assertions = 0;
	let cancelled = false;
	let closed = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "fenced-supervisor",
			prepareRestartDrain: async () => ({
				superseded: new AbortController().signal,
				assertActive: async () => {
					assertions++;
					if (assertions > 1) throw new Error("restart drain ownership was transferred");
				},
				cancel: async () => {
					cancelled = true;
				},
				close: async () => {
					closed = true;
				},
			}),
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.steps[1]?.detail).toContain("restart drain ownership was transferred");
		expect(assertions).toBe(2);
		expect(cancelled).toBe(true);
		expect(closed).toBe(true);
		expect(host.ran).toEqual(["dev.gajaeway.broker"]);
		expect(receipt.steps.slice(2).every((step) => step.result === "skipped")).toBe(true);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack cancels the persona drain when a service restart fails before the gateway", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, fail: "dev.gajaeway.broker" });
	let cancelled = false;
	let closed = false;
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "cancel-drain-on-failure",
			prepareRestartDrain: async () => ({
				superseded: new AbortController().signal,
				assertActive: async () => {},
				cancel: async () => {
					cancelled = true;
				},
				close: async () => {
					closed = true;
				},
			}),
		});
		expect(receipt.state).toBe("failed");
		expect(cancelled).toBe(true);
		expect(closed).toBe(true);
		expect(host.ran).toEqual(["dev.gajaeway.broker"]);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack cancels and closes the persona drain when receipt persistence fails", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, installed: async () => false });
	let cancelled = 0;
	let closed = 0;
	try {
		await expect(
			runRestartStack({
				...host.options,
				id: "drain-receipt-write-failure",
				prepareRestartDrain: async () => {
					await mkdir(join(home, `restart-stack.json.${process.pid}.tmp`));
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {
							cancelled++;
						},
						close: async () => {
							closed++;
						},
					};
				},
			}),
		).rejects.toThrow();
		expect(cancelled).toBe(1);
		expect(closed).toBe(1);
		expect(host.ran).toEqual([]);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("the first update from a pre-drain gateway records its durable-recovery bootstrap", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, installed: async (label) => label === "dev.gajaeway.gateway" });
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "legacy-gateway-update",
			trigger: "update",
			prepareRestartDrain: async (_socketPath, _restartId, trigger) => {
				expect(trigger).toBe("update");
				throw new ProtocolError("unknown_verb", "gateway restart drain is not supported");
			},
		});
		expect(receipt.state).toBe("ok");
		expect(receipt.warning).toBe("running gateway lacks restart draining; this update relies on durable turn recovery");
		expect(renderRestartReceipt(receipt)).toContain(
			"warning: running gateway lacks restart draining; this update relies on durable turn recovery",
		);
		expect(receipt.steps.find((step) => step.label === "dev.gajaeway.gateway")?.result).toBe("ok");
	} finally {
		await host.close();
		await cleanup();
	}
});

test("restart-stack fails closed when the Gateway cannot confirm terminal delivery", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home, installed: async (label) => label === "dev.gajaeway.gateway" });
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "drain-delivery-unconfirmed",
			prepareRestartDrain: async () => {
				throw new ProtocolError("verb_failed", "persona turn delivery was not confirmed before the drain deadline");
			},
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.detail).toContain("persona turn delivery was not confirmed");
		expect(receipt.steps.every((step) => step.result === "skipped")).toBe(true);
		expect(host.ran).toEqual([]);
	} finally {
		await host.close();
		await cleanup();
	}
});

test("manual restart refuses to bypass the drain on a pre-drain gateway", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	try {
		const receipt = await runRestartStack({
			...host.options,
			id: "legacy-manual-restart",
			prepareRestartDrain: async () => {
				throw new ProtocolError("unknown_verb", "gateway restart drain is not supported");
			},
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.detail).toContain("gateway restart drain is not supported");
		expect(receipt.steps.every((step) => step.result === "skipped")).toBe(true);
		expect(host.ran).toEqual([]);
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
		expect(receipt.steps.map((step) => step.result)).toEqual([
			"ok",
			"stale",
			"skipped",
			"skipped",
			"skipped",
			"skipped",
		]);
		expect(receipt.steps[1]?.detail).toBe("lease owner predates this restart sequence");
		expect(host.ran).toEqual(["dev.gajaeway.broker", "dev.gajaeway.adapter-discord"]);
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

test("an installed adapter with no adapter config is skipped, not a failed restart", async () => {
	const { home, cleanup } = await tempHome();
	const host = await fakeHost({ home });
	try {
		// A legacy install left the slack unit behind after its adapter-slack.json was
		// removed: the process exits on the credential read, so the restart must skip the
		// unit and carry on instead of failing the receipt and skipping the later services.
		await rm(join(home, "adapter-slack.json"));
		const receipt = await runRestartStack({ ...host.options, id: "unconfigured-adapter" });
		expect(receipt.state).toBe("ok");
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "skipped", "ok", "ok", "ok"]);
		expect(receipt.steps[2]?.detail).toBe("adapter is not configured");
		expect(host.ran).not.toContain("dev.gajaeway.adapter-slack");
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
		expect(receipt.steps.map((step) => step.result)).toEqual(["ok", "ok", "failed", "skipped", "skipped", "skipped"]);
		expect(receipt.steps[2]?.detail).toBe(
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

for (const platform of ["darwin", "linux"] as const) {
	test(`${platform} restart-stack cleans stale service definitions deferred by --no-restart`, async () => {
		const { home, cleanup } = await tempHome();
		const binDir = join(home, "bin");
		const definitionDir = join(home, platform === "darwin" ? "agents" : "units");
		const staleAdmin = serviceSpecs().find((spec) => spec.id === "admin");
		if (!staleAdmin) throw new Error("admin service spec not found");
		const stalePath =
			platform === "darwin"
				? launchAgentPlistPath(staleAdmin, definitionDir)
				: systemdUnitPath(staleAdmin, definitionDir);
		const commands: string[][] = [];
		let drainPrepared = false;
		try {
			await mkdir(binDir);
			await mkdir(definitionDir);
			await writeFile(stalePath, "stale service definition");
			const receipt = await runRestartStack({
				home,
				id: `${platform}-stale-cleanup`,
				platform,
				uid: 501,
				binDir,
				env: { ...process.env, HOME: home },
				...(platform === "darwin" ? { launchAgentsDir: definitionDir } : { unitDir: definitionDir }),
				probe: async () => undefined,
				prepareRestartDrain: async () => {
					drainPrepared = true;
					return {
						superseded: new AbortController().signal,
						assertActive: async () => {},
						cancel: async () => {},
						close: async () => {},
					};
				},
				runner: async (command) => {
					expect(drainPrepared).toBe(true);
					commands.push([...command]);
					return 0;
				},
			});
			expect(receipt.state).toBe("ok");
			expect(receipt.steps.every((step) => step.result === "skipped")).toBe(true);
			expect(commands).toEqual(
				platform === "darwin"
					? [
							["launchctl", "print", "gui/501/dev.gajaeway.admin"],
							["launchctl", "bootout", "gui/501/dev.gajaeway.admin"],
						]
					: [
							["systemctl", "--user", "disable", "--now", "gajaeway-admin.service"],
							["systemctl", "--user", "daemon-reload"],
						],
			);
			expect(await readdir(definitionDir)).not.toContain(stalePath.split("/").at(-1));
		} finally {
			await cleanup();
		}
	});
}

test("restart-stack does not stop stale services when persona drain fails", async () => {
	const { home, cleanup } = await tempHome();
	const binDir = join(home, "bin");
	const unitDir = join(home, "units");
	const admin = serviceSpecs().find((spec) => spec.id === "admin");
	if (!admin) throw new Error("admin service spec not found");
	const stalePath = systemdUnitPath(admin, unitDir);
	const commands: string[][] = [];
	try {
		await mkdir(binDir);
		await mkdir(unitDir);
		await writeFile(stalePath, "stale systemd unit");
		const receipt = await runRestartStack({
			home,
			id: "drain-fails-before-stale-cleanup",
			platform: "linux",
			binDir,
			unitDir,
			env: { ...process.env, HOME: home },
			probe: async () => undefined,
			prepareRestartDrain: async () => {
				throw new ProtocolError("verb_failed", "persona reply delivery was not confirmed");
			},
			runner: async (command) => {
				commands.push([...command]);
				return 0;
			},
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.detail).toContain("persona turn drain failed");
		expect(commands).toEqual([]);
		expect(await Bun.file(stalePath).exists()).toBe(true);
	} finally {
		await cleanup();
	}
});

test("Linux restart-stack preserves a stale unit when systemd cannot stop it", async () => {
	const { home, cleanup } = await tempHome();
	const binDir = join(home, "bin");
	const unitDir = join(home, "units");
	const admin = serviceSpecs().find((spec) => spec.id === "admin");
	if (!admin) throw new Error("admin service spec not found");
	const stalePath = systemdUnitPath(admin, unitDir);
	const commands: string[][] = [];
	let drainCancelled = false;
	let drainClosed = false;
	try {
		await mkdir(binDir);
		await mkdir(unitDir);
		await writeFile(stalePath, "stale systemd unit");
		const receipt = await runRestartStack({
			home,
			id: "failed-stale-unit-cleanup",
			platform: "linux",
			binDir,
			unitDir,
			env: { ...process.env, HOME: home },
			probe: async () => undefined,
			prepareRestartDrain: async () => ({
				superseded: new AbortController().signal,
				assertActive: async () => {},
				cancel: async () => {
					drainCancelled = true;
				},
				close: async () => {
					drainClosed = true;
				},
			}),
			runner: async (command) => {
				commands.push([...command]);
				return 1;
			},
		});
		expect(receipt.state).toBe("failed");
		expect(receipt.detail).toContain("systemctl --user disable --now gajaeway-admin.service exited with status 1");
		expect(commands).toEqual([["systemctl", "--user", "disable", "--now", "gajaeway-admin.service"]]);
		expect(drainCancelled).toBe(true);
		expect(drainClosed).toBe(true);
		expect(await Bun.file(stalePath).exists()).toBe(true);
	} finally {
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
		const queued = await launchRestartStack({
			home,
			platform: "darwin",
			worker: [process.execPath, "-e", "process.exit(0)"],
		});
		expect(queued.receipt.state).toBe("queued");
		expect(queued.receipt.trigger).toBe("manual");
		expect(queued.supervisorPid).toBeGreaterThan(0);
		const update = await launchRestartStack({
			home,
			platform: "darwin",
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
		expect(lines.some((line) => line.startsWith(`dev.gajaeway.adapter-discord: ok pid=${process.pid} started=`))).toBe(
			true,
		);
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
				env: { ...process.env, HOME: home },
				launchAgentsDir: join(home, "agents"),
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

test("manual restart bootstraps a provisioned LaunchAgent that was never loaded", async () => {
	const { home, cleanup } = await tempHome();
	let brokerLease: Lease | undefined;
	let running = false;
	const commands: string[][] = [];
	try {
		const receipt = await runRestartStack({
			home,
			id: "manual-unloaded-broker",
			trigger: "manual",
			platform: "darwin",
			uid: 501,
			env: { HOME: "/Users/operator" },
			launchAgentsDir: "/Users/operator/Library/LaunchAgents",
			runner: async (command) => {
				commands.push([...command]);
				if (command[1] === "kickstart") return 113;
				if (command[1] === "bootstrap") {
					brokerLease = await acquireLease(home, "broker");
					running = true;
				}
				return 0;
			},
			probe: async (label) => (label === "dev.gajaeway.broker" && running ? { pid: process.pid } : undefined),
			isServiceInstalled: async (label) => label === "dev.gajaeway.broker",
			binaryModifiedAt: async () => Date.now() - 1000,
			verifyTimeoutMs: 100,
			sleep: async () => {},
		});

		expect(receipt.state).toBe("ok");
		expect(receipt.steps[0]?.command).toEqual([
			"launchctl",
			"bootstrap",
			"gui/501",
			"/Users/operator/Library/LaunchAgents/dev.gajaeway.broker.plist",
		]);
		expect(commands).toEqual([
			["launchctl", "kickstart", "-k", "gui/501/dev.gajaeway.broker"],
			["launchctl", "bootstrap", "gui/501", "/Users/operator/Library/LaunchAgents/dev.gajaeway.broker.plist"],
		]);
		expect(commands.some((command) => command.includes("provision"))).toBe(false);
	} finally {
		await brokerLease?.release();
		await cleanup();
	}
});

test("manual restart does not bootstrap the broker before it is provisioned", async () => {
	const { home, cleanup } = await tempHome();
	const commands: string[][] = [];
	try {
		await rm(join(home, "broker", "provision", "manifest.json"));
		const receipt = await runRestartStack({
			home,
			id: "manual-unprovisioned-broker",
			trigger: "manual",
			platform: "darwin",
			uid: 501,
			runner: async (command) => {
				commands.push([...command]);
				return 113;
			},
			probe: async () => undefined,
			isServiceInstalled: async (label) => label === "dev.gajaeway.broker",
		});

		expect(receipt.state).toBe("ok");
		expect(receipt.steps[0]?.result).toBe("skipped");
		expect(receipt.steps[0]?.detail).toBe("broker agent is not provisioned");
		expect(commands).toEqual([]);
	} finally {
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
		if (receipt === undefined) throw new Error("restart receipt was not written");
		const rendered = renderRestartReceipt(receipt);
		expect(rendered[0]).toMatch(/: failed$/);
		expect(rendered).toContainEqual(expect.stringContaining("detail: supervisor systemd-run could not be started"));
	} finally {
		await cleanup();
	}
});

test("a nonzero systemd-run result fails the receipt and cannot be replayed as a worker", async () => {
	const { home, cleanup } = await tempHome();
	let command: readonly string[] | undefined;
	try {
		await expect(
			launchRestartStack({
				home,
				platform: "linux",
				systemdRun: (argv) => {
					command = argv;
					return Bun.spawn([process.execPath, "-e", "process.exit(23)"], {
						stdin: "ignore",
						stdout: "ignore",
						stderr: "ignore",
					});
				},
			}),
		).rejects.toThrow(/supervisor systemd-run exited with status 23/);
		const receipt = await readRestartReceipt(home);
		expect(command?.at(-2)).toBe("--run");
		expect(receipt?.state).toBe("failed");
		expect(receipt?.detail).toContain("systemd-run exited with status 23");
		expect(receipt?.steps.every((step) => step.result === "skipped")).toBe(true);
		await expect(runRestartStack({ home, id: receipt!.id })).rejects.toThrow(
			"receipt is failed; refusing to start worker",
		);
	} finally {
		await cleanup();
	}
});

test("a timed-out systemd-run result records an indeterminate failed receipt", async () => {
	const { home, cleanup } = await tempHome();
	try {
		await expect(
			launchRestartStack({
				home,
				platform: "linux",
				systemdRunTimeoutMs: 20,
				systemdRun: () =>
					Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], {
						stdin: "ignore",
						stdout: "ignore",
						stderr: "ignore",
					}),
			}),
		).rejects.toThrow(/systemd-run timed out after 20ms; handoff outcome is indeterminate/);
		const receipt = await readRestartReceipt(home);
		expect(receipt?.state).toBe("failed");
		expect(receipt?.finishedAt).toBeDefined();
		expect(receipt?.detail).toContain("handoff outcome is indeterminate");
		expect(receipt?.steps.every((step) => step.result === "skipped")).toBe(true);
	} finally {
		await cleanup();
	}
});

test("an uninstalled service is skipped and does not abort the remaining stack", async () => {
	const { home, cleanup } = await tempHome();
	const unitDir = await mkdtemp(join(tmpdir(), "gajaeway-restart-units-"));
	const binDir = join(home, "bin");
	try {
		await mkdir(binDir);
		await writeFile(join(binDir, "gajaeway-gateway"), "gateway binary");
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
				binDir,
				env: { ...process.env, HOME: home },

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

test("#621 regression: restart-stack-worker fixture fails if GAJAEWAY_LAUNCH_AGENTS_DIR is not set", async () => {
	// This prevents the fixture from accidentally touching the real ~/Library/LaunchAgents
	// on a macOS host. The fixture must be run with GAJAEWAY_LAUNCH_AGENTS_DIR set.
	const { home, cleanup } = await tempHome();
	try {
		const worker = Bun.spawn(
			[process.execPath, join(import.meta.dir, "fixtures/restart-stack-worker.ts"), "ops", "restart-stack"],
			{
				stdout: "pipe",
				stderr: "pipe",
				env: {
					...process.env,
					GAJAEWAY_HOME: home,
					FIXTURE_CALLER_PGID: String(process.pid),
					// Deliberately omit GAJAEWAY_LAUNCH_AGENTS_DIR to test the guard
				},
			},
		);
		const exitCode = await worker.exited;
		const stderr = await new Response(worker.stderr).text();
		expect(exitCode).not.toBe(0);
		expect(stderr).toContain("GAJAEWAY_LAUNCH_AGENTS_DIR");
		expect(stderr).toContain("prevent touching real system files");
	} finally {
		await cleanup();
	}
});

test("supervisorArgv escapes the gateway cgroup on systemd without bootout", () => {
	const worker = ["/opt/bin/gajaeway", "ops", "restart-stack", "--run", "r1"];
	expect(supervisorArgv("darwin", worker, "/h", "r1")).toEqual(worker);
	const socketPath = "/run/user/1000/gajaeway/custom.sock";
	const linux = supervisorArgv("linux", worker, "/h", "r1", BIN_DIR, socketPath);

	expect(linux.slice(0, 2)).toEqual(["systemd-run", "--user"]);
	expect(linux).toContain("--service-type=exec");
	expect(linux).toContain("--unit=gajaeway-restart-stack-r1");
	expect(linux).toContain(`--setenv=GAJAEWAY_BIN_DIR=${BIN_DIR}`);
	expect(linux).toContain(`--setenv=GAJAEWAY_SOCKET_PATH=${socketPath}`);
	expect(linux.slice(-worker.length)).toEqual(worker);
	expect([...linux, ...worker]).not.toContain("bootout");
});
