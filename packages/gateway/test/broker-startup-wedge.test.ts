import { describe, expect, test } from "bun:test";
import { BrokerStartupFailureTracker } from "../src/orchestrator/broker-liveness";

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
