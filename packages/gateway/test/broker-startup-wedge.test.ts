import { describe, expect, test } from "bun:test";
import { BrokerStartupFailureTracker, brokerStartupFailureReason } from "../src/orchestrator/broker-liveness";

describe("BrokerStartupFailureTracker", () => {
	test("startup-lock-unavailable reason is detected", () => {
		const record = { reason: "startup-lock-unavailable", path: "/some/path" };
		expect(brokerStartupFailureReason(record)).toBe("startup-lock-unavailable");
	});

	test("other reasons are not detected as startup failures", () => {
		expect(brokerStartupFailureReason({ reason: "ordinary-shutdown" })).toBeUndefined();
		expect(brokerStartupFailureReason({ reason: "heartbeat-renewal-blocked" })).toBeUndefined();
		expect(brokerStartupFailureReason({})).toBeUndefined();
		expect(brokerStartupFailureReason(null)).toBeUndefined();
	});

	test("transient single failure does not trigger alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		const state = tracker.state();
		expect(state.failureCount).toBe(2);
		expect(state.latestReason).toBe("startup-lock-unavailable");
	});

	test("K consecutive startup-lock-unavailable records trigger exactly one alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		// First two failures: no alert
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		// Third failure: alert
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(true);
		// Fourth failure: no alert (rate-limited)
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);

		const state = tracker.state();
		expect(state.failureCount).toBe(4);
		expect(state.lastAlertedReason).toBe("startup-lock-unavailable");
	});

	test("different reason resets alert rate-limiting", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		// Three startup-lock-unavailable failures -> alert
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(true);
		// Switch to different reason and record three of them -> alert
		expect(tracker.recordFailure("startup-lock-blocked")).toBe(false);
		expect(tracker.recordFailure("startup-lock-blocked")).toBe(false);
		expect(tracker.recordFailure("startup-lock-blocked")).toBe(true);

		const state = tracker.state();
		expect(state.lastAlertedReason).toBe("startup-lock-blocked");
	});

	test("reset clears failure tracking and re-arms alert", () => {
		const tracker = new BrokerStartupFailureTracker({ threshold: 3 });
		// Three failures -> alert
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(true);

		// Reset on successful broker start
		tracker.reset();

		const state = tracker.state();
		expect(state.failureCount).toBe(0);
		expect(state.lastAlertedReason).toBeUndefined();

		// New failures should alert again after threshold
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(false);
		expect(tracker.recordFailure("startup-lock-unavailable")).toBe(true);
	});

	test("bounded window removes old failures", () => {
		const tracker = new BrokerStartupFailureTracker({ windowMs: 1000, threshold: 3 });
		const now = Date.now();

		// Add first failure
		expect(tracker.recordFailure("startup-lock-unavailable", now)).toBe(false);

		// Simulate time passing beyond window
		const laterNow = now + 2000;
		expect(tracker.recordFailure("startup-lock-unavailable", laterNow)).toBe(false);

		const state = tracker.state();
		// Old failure should be outside the window, so only 1 failure counted
		expect(state.failureCount).toBe(1);
	});
});
