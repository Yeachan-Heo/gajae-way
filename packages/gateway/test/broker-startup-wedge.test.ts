import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { BrokerStartupFailureTracker, validateStartupExitRecord } from "../src/orchestrator/broker-liveness";

describe("BrokerStartupFailureTracker", () => {
	test("K distinct records with the same reason trigger exactly one alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		const now = Date.now();

		// Create 3 distinct records with the same reason
		const record1 = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record2 = { pid: 1002, writtenAt: now + 100, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record3 = { pid: 1003, writtenAt: now + 200, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		// First two: no alert
		expect(tracker.recordFailure(record1, now)).toBe(false);
		expect(tracker.recordFailure(record2, now)).toBe(false);

		// Third distinct record: alert
		expect(tracker.recordFailure(record3, now)).toBe(true);

		// Fourth distinct record: no alert (rate-limited to same reason)
		const record4 = { pid: 1004, writtenAt: now + 300, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		expect(tracker.recordFailure(record4, now)).toBe(false);

		const state = tracker.state();
		expect(state.failureCount).toBe(4);
		expect(state.lastAlertedReason).toBe("startup-lock-blocked");
	});

	test("the same record re-read many times gives no alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		const now = Date.now();

		// Create one record
		const record = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		// Try to record the same record many times
		expect(tracker.recordFailure(record, now)).toBe(false);
		expect(tracker.recordFailure(record, now)).toBe(false);
		expect(tracker.recordFailure(record, now)).toBe(false);
		expect(tracker.recordFailure(record, now)).toBe(false);
		expect(tracker.recordFailure(record, now)).toBe(false);

		// Should still be 1 distinct record, no alert
		const state = tracker.state();
		expect(state.failureCount).toBe(1);
		expect(state.lastAlertedReason).toBeUndefined();
	});

	test("a confirmed live broker re-arms the alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		const now = Date.now();

		// Record 3 distinct records to trigger alert
		const record1 = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record2 = { pid: 1002, writtenAt: now + 100, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record3 = { pid: 1003, writtenAt: now + 200, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		expect(tracker.recordFailure(record1, now)).toBe(false);
		expect(tracker.recordFailure(record2, now)).toBe(false);
		expect(tracker.recordFailure(record3, now)).toBe(true); // Alert

		let state = tracker.state();
		expect(state.failureCount).toBe(3);
		expect(state.lastAlertedReason).toBe("startup-lock-blocked");

		// Reset on broker liveness confirmation
		tracker.reset();

		state = tracker.state();
		expect(state.failureCount).toBe(0);
		expect(state.lastAlertedReason).toBeUndefined();

		// New distinct records should alert again after threshold
		const record4 = { pid: 2001, writtenAt: now + 1000, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record5 = { pid: 2002, writtenAt: now + 1100, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record6 = { pid: 2003, writtenAt: now + 1200, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		expect(tracker.recordFailure(record4, now + 1000)).toBe(false);
		expect(tracker.recordFailure(record5, now + 1000)).toBe(false);
		expect(tracker.recordFailure(record6, now + 1000)).toBe(true); // Alert again
	});

	test("distinct records identified by pid/writtenAt pair", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 2 });
		const now = Date.now();

		// Create records with same pid but different writtenAt
		const record1 = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const record2 = { pid: 1001, writtenAt: now + 100, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		// Both are distinct (different writtenAt), so second one should trigger alert
		expect(tracker.recordFailure(record1, now)).toBe(false);
		expect(tracker.recordFailure(record2, now)).toBe(true);

		const state = tracker.state();
		expect(state.failureCount).toBe(2);
	});

	test("different reasons don't interfere with alert rate-limiting", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 2 });
		const now = Date.now();

		// Record 2 of reason A to trigger alert
		const recordA1 = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		const recordA2 = { pid: 1002, writtenAt: now + 100, reason: "startup-lock-blocked", version: 1, mode: "startup" };

		expect(tracker.recordFailure(recordA1, now)).toBe(false);
		expect(tracker.recordFailure(recordA2, now)).toBe(true); // Alert for reason A

		// Record 2 of reason B
		const recordB1 = { pid: 2001, writtenAt: now + 200, reason: "startup-deadline", version: 1, mode: "startup" };
		const recordB2 = { pid: 2002, writtenAt: now + 300, reason: "startup-deadline", version: 1, mode: "startup" };

		expect(tracker.recordFailure(recordB1, now)).toBe(false);
		// Should trigger alert for reason B (different from A)
		expect(tracker.recordFailure(recordB2, now)).toBe(true);

		const state = tracker.state();
		expect(state.failureCount).toBe(4);
		expect(state.lastAlertedReason).toBe("startup-deadline");
	});

	test("bounded window removes old records", () => {
		const tracker = new BrokerStartupFailureTracker({ windowMs: 1000, threshold: 3 });
		const now = Date.now();

		// Add first record
		const record1 = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		expect(tracker.recordFailure(record1, now)).toBe(false);

		// Simulate time passing beyond window
		const laterNow = now + 2000;

		// Add second record well after the window
		const record2 = { pid: 1002, writtenAt: laterNow, reason: "startup-lock-blocked", version: 1, mode: "startup" };
		expect(tracker.recordFailure(record2, laterNow)).toBe(false);

		// State should show only 1 record (old one was cleaned up)
		const state = tracker.state();
		expect(state.failureCount).toBe(1);
		expect(state.latestReason).toBe("startup-lock-blocked");
	});

	test("invalid records are ignored", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		const now = Date.now();

		// Try to record various invalid records
		expect(tracker.recordFailure(null, now)).toBe(false);
		expect(tracker.recordFailure(undefined, now)).toBe(false);
		expect(tracker.recordFailure({}, now)).toBe(false);
		expect(tracker.recordFailure({ pid: 1001 }, now)).toBe(false); // Missing writtenAt and reason
		expect(tracker.recordFailure({ writtenAt: now }, now)).toBe(false); // Missing pid and reason
		expect(tracker.recordFailure({ pid: 1001, writtenAt: now }, now)).toBe(false); // Missing reason

		const state = tracker.state();
		expect(state.failureCount).toBe(0);
	});
});

describe("validateStartupExitRecord", () => {
	test("validates records with valid schema", () => {
		const now = Date.now();
		const record = { pid: 1001, writtenAt: now, reason: "startup-lock-blocked" };

		const validated = validateStartupExitRecord(record);
		expect(validated).toBeDefined();
		expect(validated?.pid).toBe(1001);
		expect(validated?.writtenAt).toBe(now);
		expect(validated?.reason).toBe("startup-lock-blocked");
	});

	test("includes optional blockingLockPath", () => {
		const now = Date.now();
		const record = {
			pid: 1001,
			writtenAt: now,
			reason: "startup-lock-unavailable",
			blockingLockPath: "/tmp/lock",
		};

		const validated = validateStartupExitRecord(record);
		expect(validated?.blockingLockPath).toBe("/tmp/lock");
	});

	test("rejects invalid reasons", () => {
		const now = Date.now();
		const record = { pid: 1001, writtenAt: now, reason: "unknown-reason" };

		const validated = validateStartupExitRecord(record);
		expect(validated).toBeUndefined();
	});

	test("rejects missing required fields", () => {
		const now = Date.now();

		expect(validateStartupExitRecord({ pid: 1001, reason: "startup-lock-blocked" })).toBeUndefined();
		expect(validateStartupExitRecord({ writtenAt: now, reason: "startup-lock-blocked" })).toBeUndefined();
		expect(validateStartupExitRecord({ pid: 1001, writtenAt: now })).toBeUndefined();
	});
});

describe("GlobalGjcClient startup-exit tracking (#800)", () => {
	const cleanups: Array<() => Promise<void>> = [];
	afterEach(async () => {
		for (const cleanup of cleanups.splice(0)) await cleanup();
	});

	async function wedgedClient() {
		const agentDir = await realpath(await mkdtemp(join(tmpdir(), "gw-800-")));
		await mkdir(join(agentDir, "sdk"), { recursive: true });
		let available = true;
		const alerts: Array<{ reason: string; blockingPath?: string }> = [];
		const logs: string[] = [];
		const client = new GlobalGjcClient({
			executable: "/fake/nondefault/bin/gjc",
			agentDir,
			command: async () => ({
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessions: [] } }),
				stderr: "",
			}),
			discovery: async () => ({ pid: 12345, url: "ws://127.0.0.1:12345", token: "t", heartbeatAt: Date.now() }),
			healthProbe: async () => available,
			healthIntervalMs: 5,
			reconnectBackoff: { initialMs: 2, maxMs: 5 },
			log: (line) => logs.push(String(line)),
		});
		client.setOnStartupWedgeAlert((wedge) => alerts.push(wedge));
		cleanups.push(async () => {
			await client.stop();
			await rm(agentDir, { recursive: true, force: true });
		});
		let writtenAt = Date.now();
		const writeStartupExit = (pid: number) =>
			writeFile(
				join(agentDir, "sdk", "broker.startup-exit.json"),
				JSON.stringify({
					reason: "startup-lock-unavailable",
					pid,
					writtenAt: ++writtenAt,
					blockingLockPath: join(agentDir, "sdk", "sessions", "index.jsonl.lock"),
				}),
			);
		return {
			client,
			alerts,
			logs,
			writeStartupExit,
			setAvailable: (value: boolean) => {
				available = value;
			},
		};
	}

	async function ticks(logs: string[], count: number): Promise<void> {
		const observed = () => logs.filter((line) => line.includes("observing without repair")).length;
		const target = observed() + count;
		for (let i = 0; i < 400 && observed() < target; i++) await Bun.sleep(5);
		expect(observed()).toBeGreaterThanOrEqual(target);
	}

	test("K distinct startup-exit records while the broker is down raise exactly one owner alert", async () => {
		const wedge = await wedgedClient();
		await wedge.client.start();
		wedge.setAvailable(false);
		for (let pid = 1; pid <= 5; pid++) {
			await wedge.writeStartupExit(pid);
			await ticks(wedge.logs, 2);
		}
		expect(wedge.alerts).toHaveLength(1);
		expect(wedge.alerts[0]?.reason).toBe("startup-lock-unavailable");
		expect(wedge.alerts[0]?.blockingPath).toEndWith("index.jsonl.lock");
		expect(wedge.logs.filter((line) => line.startsWith("broker_startup_wedge "))).toHaveLength(1);
	});

	test("one startup-exit record observed on many ticks raises no alert", async () => {
		const wedge = await wedgedClient();
		await wedge.client.start();
		wedge.setAvailable(false);
		await wedge.writeStartupExit(1);
		await ticks(wedge.logs, 10);
		expect(wedge.alerts).toEqual([]);
	});

	test("a live broker re-arms the alert for the next wedge", async () => {
		const wedge = await wedgedClient();
		await wedge.client.start();
		wedge.setAvailable(false);
		for (let pid = 1; pid <= 3; pid++) {
			await wedge.writeStartupExit(pid);
			await ticks(wedge.logs, 2);
		}
		expect(wedge.alerts).toHaveLength(1);
		wedge.setAvailable(true);
		for (let i = 0; i < 400 && !wedge.client.isAvailable; i++) await Bun.sleep(5);
		expect(wedge.client.isAvailable).toBe(true);
		wedge.setAvailable(false);
		for (let pid = 11; pid <= 13; pid++) {
			await wedge.writeStartupExit(pid);
			await ticks(wedge.logs, 2);
		}
		expect(wedge.alerts).toHaveLength(2);
	});
});
