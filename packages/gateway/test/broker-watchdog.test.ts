import { expect, spyOn, test } from "bun:test";
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
			probe: async () => ++calls === 5,
			...time,
		}),
	);
	expect(reason).toContain("6 failed probes");
	expect(time.now()).toBe(75 + 90);
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
