import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { bootGateway, waitForBroker } from "../src/boot";
import { GjcCliUnavailableError, MIN_GJC_VERSION } from "../src/orchestrator/broker";

const directories: string[] = [];
afterEach(async () => {
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

const SESSIONS = JSON.stringify({ ok: true, result: { sessions: [] } });

function brokerRecoveringAfter(failures: number, home: string) {
	let listCalls = 0;
	const command: CliRunner = async (args) => {
		if (args[0] === "--version") return { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" };
		if (args[1] === "session" && args[2] === "list" && ++listCalls <= failures) {
			// What `gjc sdk session list` does while the shared broker clears a stale lock.
			return { exitCode: 1, stdout: "", stderr: "broker lock held by dead pid\n" };
		}
		return { exitCode: 0, stdout: SESSIONS, stderr: "" };
	};
	return {
		executable: "/test-only/gjc",
		agentDir: home,
		command,
		healthProbe: async () => true,
		discovery: async () => ({ pid: 1, url: "ws://127.0.0.1:1", token: "test-only", heartbeatAt: Date.now() }),
		healthIntervalMs: 60_000,
		log: () => {},
	};
}

test("boot waits out a shared broker that is still recovering instead of exiting (#182)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-wait-"));
	directories.push(home);
	const lines: string[] = [];
	const sleeps: number[] = [];
	const server = await bootGateway({
		home,
		broker: brokerRecoveringAfter(3, home),
		brokerWait: {
			initialMs: 1_000,
			maxMs: 4_000,
			sleep: async (ms) => {
				sleeps.push(ms);
			},
			log: (line) => lines.push(line),
		},
	});
	try {
		expect(sleeps).toEqual([1_000, 2_000, 4_000]);
		expect(lines).toHaveLength(3);
		expect(lines[0]).toStartWith("gateway_boot_waiting_for_broker step=preflight attempt=1 retry_in_ms=1000 ");
		expect(lines[0]).toContain("broker_unavailable");
	} finally {
		await server.stop("test shutdown");
	}
});

test("a broker that never returns still fails boot once the wait deadline passes", async () => {
	let clock = 0;
	let calls = 0;
	const unavailable = new GjcCliUnavailableError("global broker is not ready; no repair attempted");
	await expect(
		waitForBroker(
			"start",
			async () => {
				calls++;
				throw unavailable;
			},
			{
				initialMs: 1_000,
				maxMs: 8_000,
				deadlineMs: 20_000,
				now: () => clock,
				sleep: async (ms) => {
					clock += ms;
				},
				log: () => {},
			},
		),
	).rejects.toBe(unavailable);
	// 1s + 2s + 4s + 8s = 15s slept; the next 8s wait would overrun the 20s deadline.
	expect(calls).toBe(5);
	expect(clock).toBe(15_000);
});

test("failures that will not heal are not waited on", async () => {
	const sleeps: number[] = [];
	const wrongVersion = new Error("gjc runtime preflight failed: requires gjc >= 0.15.6");
	await expect(
		waitForBroker(
			"preflight",
			async () => {
				throw wrongVersion;
			},
			{ sleep: async (ms) => void sleeps.push(ms), log: () => {} },
		),
	).rejects.toBe(wrongVersion);
	expect(sleeps).toEqual([]);
});
