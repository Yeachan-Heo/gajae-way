import { describe, expect, test } from "bun:test";
import { ProtocolError } from "@gajae-gateway/protocol";
import {
	classifyRecoveryFailure,
	EMPTY_RECOVERY_STATE,
	RECOVERY_MAX_ATTEMPTS,
	type RecoveryCursorState,
	recordAttempt,
	recoverConversation,
} from "../src/recovery";

describe("Recovery timeout attempts", () => {
	test("local request timeout and connection failures are retryable; payload failures remain terminal", () => {
		for (const error of [
			new ProtocolError("verb_failed", "request timed out after 30000ms"),
			new Error("gateway is not connected"),
			new Error("client closed"),
		]) {
			expect(classifyRecoveryFailure(error)).toBe("retryable");
		}
		for (const code of ["invalid_params", "payload_too_large"] as const) {
			expect(classifyRecoveryFailure(new ProtocolError(code, "request timed out"))).toBe("terminal-message");
		}
		expect(classifyRecoveryFailure(new Error("unknown failure"))).toBe("write-path-unknown");
	});

	test("repeated timeouts hold the cursor and never exhaust the poison budget", async () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;
		const port = {
			history: async () => ({
				messages: [
					{ ts: "1.0", user: "U1" },
					{ ts: "2.0", user: "U1" },
				],
				has_more: false,
			}),
			replies: async () => ({ messages: [], has_more: false }),
		};
		for (let i = 0; i <= RECOVERY_MAX_ATTEMPTS; i++) {
			const seen: string[] = [];
			const outcome = await recoverConversation(port, "C1", {
				cursor: "0.0",
				nowMs: 2_000_000_000_000,
				botUserId: "BOT",
				deliver: async (message) => {
					seen.push(message.ts);
					const classification = classifyRecoveryFailure(
						new ProtocolError("verb_failed", "request timed out after 30000ms"),
					);
					const result = recordAttempt(state, `C1:${message.ts}`, "C1", classification, "timeout", i);
					state = result.state;
					expect(result.exhausted).toBe(false);
					return "unavailable";
				},
			});
			expect(seen).toEqual(["1.0"]);
			expect(outcome).toMatchObject({ failed: true, delivered: 0, discarded: 0 });
			expect(outcome.advancedTo).toBeUndefined();
			expect(state.attempts["C1:1.0"]?.attempts).toBe(0);
			expect(state.deadLetters).toHaveLength(0);
		}
	});

	test("timeouts preserve existing payload attempts; only definitive failures exhaust them", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;
		const terminal = classifyRecoveryFailure(new ProtocolError("invalid_params", "invalid"));
		let result = recordAttempt(state, "C1:1.0", "C1", terminal, "invalid", 0);
		state = result.state;
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS + 1; i++) {
			const timeout = classifyRecoveryFailure(new ProtocolError("verb_failed", "request timed out after 30000ms"));
			result = recordAttempt(state, "C1:1.0", "C1", timeout, "timeout", i + 1);
			state = result.state;
			expect(result.exhausted).toBe(false);
			expect(state.attempts["C1:1.0"]?.attempts).toBe(1);
		}
		for (let i = 1; i < RECOVERY_MAX_ATTEMPTS; i++) {
			result = recordAttempt(state, "C1:1.0", "C1", terminal, "invalid", i + 10);
			state = result.state;
		}
		expect(result.exhausted).toBe(true);
		expect(state.attempts["C1:1.0"]?.attempts).toBe(RECOVERY_MAX_ATTEMPTS);
	});
});
