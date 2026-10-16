import { expect, spyOn, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runForegroundBroker } from "../src/broker-host";
import { watchBrokerLiveness } from "../src/broker-watchdog";

/** A fake clock whose sleep advances time instead of waiting. */
function clock() {
	let at = 0;
	return {
		now: () => at,
		sleep: async (ms: number) => {
			at += ms;
		},
	};
}

function quiet<T>(run: () => Promise<T>): Promise<T> {
	const log = spyOn(console, "log").mockImplementation(() => {});
	const error = spyOn(console, "error").mockImplementation(() => {});
	return run().finally(() => {
		log.mockRestore();
		error.mockRestore();
	});
}

async function waitForFile(path: string, timeoutMs = 1_000): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await Bun.file(path).exists()) return true;
		await Bun.sleep(5);
	}
	return await Bun.file(path).exists();
}

test("trips once every probe has failed for the unresponsive window", async () => {
	const time = clock();
	const probed: number[] = [];
	const reason = await quiet(() =>
		watchBrokerLiveness({
			childPid: 42,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 15,
			unresponsiveMs: 90,
			startupMs: 90,
			probe: async (pid) => {
				probed.push(pid);
				return false;
			},
			...time,
		}),
	);
	expect(reason).toMatch(/^broker pid 42 unresponsive for 0s \(6 failed probes\)$/);
	expect(probed).toEqual([42, 42, 42, 42, 42, 42]);
	expect(time.now()).toBe(90);
});

test("one answered probe restarts the window, so a slow broker is not killed", async () => {
	const time = clock();
	// Healthy only at the 5th probe: failing 15..60, healthy 75, then failing from 90.
	let calls = 0;
	const reason = await quiet(() =>
		watchBrokerLiveness({
			childPid: 7,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 15,
			unresponsiveMs: 90,
			startupMs: 90,
			probe: async () => ++calls === 5,
			...time,
		}),
	);
	expect(reason).toContain("6 failed probes");
	expect(time.now()).toBe(75 + 90);
});

test("a system-suspend gap restarts the window instead of counting as broker downtime", async () => {
	const lines: string[] = [];
	const log = spyOn(console, "log").mockImplementation((line) => lines.push(String(line)));
	const error = spyOn(console, "error").mockImplementation(() => {});
	let at = 0;
	let sleeps = 0;
	try {
		const reason = await watchBrokerLiveness({
			childPid: 5,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 15,
			unresponsiveMs: 90,
			startupMs: 90,
			probe: async () => false,
			now: () => at,
			sleep: async (ms) => {
				at += ms;
				// The lid closes after two failed probes and the process freezes for ten minutes.
				if (++sleeps === 3) at += 600_000;
			},
		});
		// The failures before the gap are discarded, so the trip needs a whole fresh
		// window of awake time (7 probes including the one at the wake).
		expect(reason).toMatch(/^broker pid 5 unresponsive for 0s \(7 failed probes\)$/);
		expect(at).toBe(600_135);
		expect(
			lines.some(
				(line) =>
					line.startsWith("broker_watchdog_suspended") && line.includes("failures=2") && line.includes("phase=idle"),
			),
		).toBe(true);
	} finally {
		log.mockRestore();
		error.mockRestore();
	}
});

test("a suspend that overlaps a failed probe restarts the window instead of killing the broker", async () => {
	const lines: string[] = [];
	const log = spyOn(console, "log").mockImplementation((line) => lines.push(String(line)));
	const error = spyOn(console, "error").mockImplementation(() => {});
	let at = 0;
	let probes = 0;
	try {
		const reason = await watchBrokerLiveness({
			childPid: 7,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 15,
			unresponsiveMs: 90,
			startupMs: 90,
			now: () => at,
			sleep: async (ms) => {
				at += ms;
			},
			probe: async () => {
				// The lid closes inside the sixth probe: the host freezes for ten minutes
				// and wakes with the socket dead.
				if (++probes === 6) at += 600_000;
				return false;
			},
		});
		// The five failures before the suspend are discarded, and a broker that stays
		// hung still trips on a fresh window of continuous awake failures.
		expect(reason).toMatch(/^broker pid 7 unresponsive for 0s \(6 failed probes\)$/);
		expect(at).toBe(600_180);
		expect(
			lines.some(
				(line) =>
					line.startsWith("broker_watchdog_suspended") && line.includes("failures=5") && line.includes("phase=probe"),
			),
		).toBe(true);
	} finally {
		log.mockRestore();
		error.mockRestore();
	}
});

test("before its first answer a broker gets the startup budget, then the shorter window", async () => {
	const time = clock();
	let calls = 0;
	const reason = await quiet(() =>
		watchBrokerLiveness({
			childPid: 9,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 10,
			unresponsiveMs: 30,
			startupMs: 100,
			// First answer at t=80 (8th probe), past the steady-state window but inside startup.
			probe: async () => ++calls === 8,
			...time,
		}),
	);
	expect(time.now()).toBe(80 + 30);
	expect(reason).toContain("3 failed probes");
});

test("a throwing probe counts as a failure and abort stops the watch quietly", async () => {
	const time = clock();
	const throwing = await quiet(() =>
		watchBrokerLiveness({
			childPid: 1,
			discoveryPath: "/unused",
			signal: new AbortController().signal,
			intervalMs: 50,
			unresponsiveMs: 100,
			startupMs: 100,
			probe: async () => {
				throw new Error("socket exploded");
			},
			...time,
		}),
	);
	expect(throwing).toContain("2 failed probes");

	const abort = new AbortController();
	let calls = 0;
	const aborted = await quiet(() =>
		watchBrokerLiveness({
			childPid: 1,
			discoveryPath: "/unused",
			signal: abort.signal,
			intervalMs: 10,
			unresponsiveMs: 1_000,
			probe: async () => {
				if (++calls === 3) abort.abort();
				return true;
			},
			...clock(),
		}),
	);
	expect(aborted).toBeUndefined();
	expect(calls).toBe(3);
});

test("a watchdog restart ends session-host descendants with the broker process group", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-broker-process-group-"));
	const brokerPath = join(directory, "broker.sh");
	const hostPath = join(directory, "session-host.sh");
	const readyPath = join(directory, "ready");
	const stoppedPath = join(directory, "stopped");
	const hostPidPath = join(directory, "host.pid");
	await writeFile(
		hostPath,
		[
			"#!/bin/sh",
			"exec 2>/dev/null",
			`trap 'touch "$BROKER_TEST_DIR/stopped"; exit 0' TERM`,
			`printf '%s\\n' "$$" > "$BROKER_TEST_DIR/host.pid"`,
			`touch "$BROKER_TEST_DIR/ready"`,
			"while :; do sleep 0.05; done",
			"",
		].join("\n"),
		{ mode: 0o700 },
	);
	await writeFile(
		brokerPath,
		["#!/bin/sh", "exec 2>/dev/null", `"$BROKER_TEST_DIR/session-host.sh" &`, "wait", ""].join("\n"),
		{ mode: 0o700 },
	);
	let hostPid: number | undefined;
	try {
		const code = await quiet(() =>
			runForegroundBroker(
				"/bin/sh",
				"/unused",
				{ PATH: process.env.PATH ?? "", BROKER_TEST_DIR: directory },
				process.cwd(),
				{
					watchdog: async (_pid, signal) => {
						const deadline = Date.now() + 2_000;
						while (!signal.aborted && Date.now() < deadline) {
							if (await Bun.file(readyPath).exists()) return "simulated broker wedge";
							await Bun.sleep(5);
						}
						return undefined;
					},
					stopGraceMs: 500,
				},
				["/bin/sh", brokerPath],
			),
		);
		hostPid = Number((await readFile(hostPidPath, "utf8")).trim());
		expect(Number.isSafeInteger(hostPid)).toBe(true);
		expect(code).toBe(1);
		expect(await waitForFile(stoppedPath)).toBe(true);
	} finally {
		if (hostPid && Number.isSafeInteger(hostPid) && !(await Bun.file(stoppedPath).exists())) {
			try {
				process.kill(hostPid, "SIGTERM");
			} catch {
				// The group cleanup already reaped it.
			}
			await Bun.sleep(100);
			if (!(await Bun.file(stoppedPath).exists())) {
				try {
					process.kill(hostPid, "SIGKILL");
				} catch {
					// The host already exited.
				}
			}
		}
		await rm(directory, { recursive: true, force: true });
	}
}, 10_000);

test("an unresponsive broker is stopped and the host exits non-zero for a service restart", async () => {
	const lines: string[] = [];
	const error = spyOn(console, "error").mockImplementation((line) => lines.push(String(line)));
	const log = spyOn(console, "log").mockImplementation(() => {});
	try {
		const started = Date.now();
		const code = await runForegroundBroker(
			"/bin/sh",
			"/unused",
			{ PATH: process.env.PATH ?? "" },
			process.cwd(),
			{ watchdog: async () => "broker pid 1 unresponsive for 90s (6 failed probes)", stopGraceMs: 5_000 },
			["/bin/sh", "-c", "exec sleep 60"],
		);
		expect(code).toBe(1);
		expect(Date.now() - started).toBeLessThan(4_000);
		expect(lines.some((line) => line.startsWith("broker_host_unresponsive broker pid 1 unresponsive"))).toBe(true);
		expect(lines.some((line) => line.startsWith("broker_host_stop_timeout"))).toBe(false);
	} finally {
		error.mockRestore();
		log.mockRestore();
	}
});

test("a broker that ignores SIGTERM is killed after the stop grace", async () => {
	const lines: string[] = [];
	const error = spyOn(console, "error").mockImplementation((line) => lines.push(String(line)));
	const log = spyOn(console, "log").mockImplementation(() => {});
	try {
		const code = await runForegroundBroker(
			"/bin/sh",
			"/unused",
			{ PATH: process.env.PATH ?? "" },
			process.cwd(),
			{
				// Give the shell time to install its trap before the watchdog fires.
				watchdog: async () => {
					await Bun.sleep(300);
					return "wedged";
				},
				stopGraceMs: 300,
			},
			["/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done"],
		);
		expect(code).toBe(1);
		expect(lines.some((line) => line.startsWith("broker_host_stop_timeout") && line.includes("action=SIGKILL"))).toBe(
			true,
		);
	} finally {
		error.mockRestore();
		log.mockRestore();
	}
});

test("a broker that exits on its own keeps its exit code and stops the watchdog", async () => {
	let aborted = false;
	const code = await quiet(() =>
		runForegroundBroker(
			"/bin/sh",
			"/unused",
			{ PATH: process.env.PATH ?? "" },
			process.cwd(),
			{
				watchdog: (_pid, signal) =>
					new Promise((resolve) => {
						signal.addEventListener("abort", () => {
							aborted = true;
							resolve(undefined);
						});
					}),
			},
			["/bin/sh", "-c", "exit 3"],
		),
	);
	expect(code).toBe(3);
	expect(aborted).toBe(true);
});
