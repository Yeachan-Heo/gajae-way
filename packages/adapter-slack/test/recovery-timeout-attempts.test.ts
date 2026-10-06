import { describe, expect, test } from "bun:test";
import { ProtocolError } from "@gajae-gateway/protocol";
import {
	classifyRecoveryFailure,
	EMPTY_RECOVERY_STATE,
	RECOVERY_MAX_ATTEMPTS,
	type RecoveryCursorState,
	recordAttempt,
} from "../src/recovery";

describe("Recovery dead-lettering after N timeout attempts (issue #420 acceptance)", () => {
	test("timeout error is classified as terminal-message, not retryable", () => {
		// Create a timeout error (what the client emits after 30s)
		const timeoutError = new ProtocolError("verb_failed", "request timed out after 30000ms");

		const classification = classifyRecoveryFailure(timeoutError);

		// Should be terminal-message so it counts toward RECOVERY_MAX_ATTEMPTS
		expect(classification).toBe("terminal-message");
	});

	test("connection errors remain retryable (not timeout)", () => {
		const connectionError = new Error("gateway is not connected");
		const classification = classifyRecoveryFailure(connectionError);

		// Should be retryable, not terminal
		expect(classification).toBe("retryable");
	});

	test("client closed error remains retryable (not timeout)", () => {
		const closedError = new Error("client closed");
		const classification = classifyRecoveryFailure(closedError);

		expect(classification).toBe("retryable");
	});

	test("message times out once: recorded as terminal-message attempt", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const messageId = "slack-msg-1";
		const conversationId = "C123";
		const timeoutError = new ProtocolError("verb_failed", "request timed out after 30000ms");
		const classification = classifyRecoveryFailure(timeoutError);

		const { state: newState, exhausted } = recordAttempt(
			state,
			messageId,
			conversationId,
			classification,
			"timeout",
			Date.now(),
		);

		state = newState;

		// After one timeout attempt (terminal-message classification)
		expect(state.attempts[messageId]?.attempts).toBe(1);
		expect(exhausted).toBe(false); // Should not be exhausted yet

		// Recovery cursor should keep retrying until RECOVERY_MAX_ATTEMPTS
		expect(RECOVERY_MAX_ATTEMPTS).toBeGreaterThan(1);
	});

	test("message times out N times: dead-lettered after RECOVERY_MAX_ATTEMPTS", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const messageId = "slack-msg-2";
		const conversationId = "C123";
		const timeoutError = new ProtocolError("verb_failed", "request timed out after 30000ms");
		const classification = classifyRecoveryFailure(timeoutError);

		let exhausted = false;

		// Record RECOVERY_MAX_ATTEMPTS failures
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS; i++) {
			const result = recordAttempt(state, messageId, conversationId, classification, "timeout", Date.now());
			state = result.state;
			exhausted = result.exhausted;
		}

		// After RECOVERY_MAX_ATTEMPTS terminal-message failures, should be exhausted
		expect(state.attempts[messageId]?.attempts).toBe(RECOVERY_MAX_ATTEMPTS);
		expect(exhausted).toBe(true);
	});

	test("message with connection error does NOT count toward attempts", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const messageId = "slack-msg-3";
		const conversationId = "C123";

		// Record many connection errors (retryable classification)
		for (let i = 0; i < 10; i++) {
			const connectionError = new Error("gateway is not connected");
			const classification = classifyRecoveryFailure(connectionError);

			const result = recordAttempt(state, messageId, conversationId, classification, "offline", Date.now());
			state = result.state;
		}

		// Should not increase attempts counter (retryable doesn't count)
		const attempts = state.attempts[messageId]?.attempts ?? 0;
		expect(attempts).toBe(0);
	});

	test("message times out once, then connection error: only 1 attempt recorded", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const messageId = "slack-msg-4";
		const conversationId = "C123";

		// First: timeout (terminal-message)
		let result = recordAttempt(state, messageId, conversationId, "terminal-message", "timeout", Date.now());
		state = result.state;

		expect(state.attempts[messageId]?.attempts).toBe(1);

		// Then: connection error (retryable)
		result = recordAttempt(state, messageId, conversationId, "retryable", "offline", Date.now());
		state = result.state;

		// Still only 1 attempt (retryable doesn't increment)
		expect(state.attempts[messageId]?.attempts).toBe(1);
	});

	test("multiple messages: each tracks attempts independently", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const msg1 = "slack-msg-5";
		const msg2 = "slack-msg-6";
		const conversationId = "C123";

		// Message 1: 1 timeout
		let result = recordAttempt(state, msg1, conversationId, "terminal-message", "timeout", Date.now());
		state = result.state;

		// Message 2: 2 timeouts
		result = recordAttempt(state, msg2, conversationId, "terminal-message", "timeout", Date.now());
		state = result.state;
		result = recordAttempt(state, msg2, conversationId, "terminal-message", "timeout", Date.now());
		state = result.state;

		// Message 1 should have 1 attempt
		expect(state.attempts[msg1]?.attempts).toBe(1);
		// Message 2 should have 2 attempts
		expect(state.attempts[msg2]?.attempts).toBe(2);
	});

	test("exhausted message does not retry again (filtered out before recovery)", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;

		const messageId = "slack-msg-7";
		const conversationId = "C123";

		// Exhaust the message with timeouts
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS; i++) {
			const result = recordAttempt(state, messageId, conversationId, "terminal-message", "timeout", Date.now());
			state = result.state;
		}

		// Message should be marked as exhausted and will not be retried
		// (in the real adapter, exhausted messages are moved to dead-letter)
		const maxAttempts = state.attempts[messageId];
		expect(maxAttempts?.attempts).toBe(RECOVERY_MAX_ATTEMPTS);

		// If a future pass tries to retry this message, the adapter code should skip it
		// because attempts >= RECOVERY_MAX_ATTEMPTS
		const shouldRetry = !maxAttempts || maxAttempts.attempts < RECOVERY_MAX_ATTEMPTS;
		expect(shouldRetry).toBe(false);
	});
});
