import { describe, expect, test } from "bun:test";
import type { OriginRef } from "@gajae-gateway/protocol";
import { type TrackedTurn, TURN_RETENTION_MS, TURN_STALL_MS, TurnTracker } from "../src/turns";

const ORIGIN: OriginRef = { platform: "discord", kind: "channel", conversationId: "1493635653441945762" };

function clock(start = 1_000_000) {
	let at = start;
	return {
		now: () => at,
		advance: (ms: number) => {
			at += ms;
		},
	};
}

/** The single tracked turn, so an assertion cannot silently pass on an empty list. */
function only(tracker: TurnTracker): TrackedTurn {
	const [turn, ...rest] = tracker.list();
	if (!turn) throw new Error("expected one tracked turn, found none");
	if (rest.length > 0) throw new Error(`expected one tracked turn, found ${rest.length + 1}`);
	return turn;
}

describe("TurnTracker", () => {
	test("a progress heartbeat creates a running turn with the reported evidence", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 15_000, toolCalls: 3, outputTokens: 120 });
		const [turn] = tracker.list();
		expect(turn?.toolCalls).toBe(3);
		expect(turn?.outputTokens).toBe(120);
		expect(tracker.stateOf(only(tracker))).toBe("running");
		expect(tracker.activeCount).toBe(1);
	});

	test("the start time is derived from the reported elapsed, not from first sight", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 60_000, toolCalls: 1, outputTokens: 1 });
		expect(tracker.list()[0]?.startedAt).toBe(time.now() - 60_000);
	});

	test("liveness expires after three missed heartbeats and the counters are kept", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 15_000, toolCalls: 14, outputTokens: 3240 });
		time.advance(TURN_STALL_MS);
		expect(tracker.stateOf(only(tracker))).toBe("running");
		time.advance(1);
		const turn = only(tracker);
		expect(tracker.stateOf(turn)).toBe("stalled");
		expect(turn.toolCalls).toBe(14);
		expect(tracker.activeCount).toBe(1);
	});

	test("only a final message terminates a turn", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 1000, toolCalls: 1, outputTokens: 1 });
		expect(tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "hi", final: false })).toBe(false);
		expect(tracker.stateOf(only(tracker))).toBe("running");
		expect(tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "hi", final: true })).toBe(true);
		expect(tracker.stateOf(only(tracker))).toBe("finished");
		expect(tracker.activeCount).toBe(0);
	});

	test("a silence token is a distinct outcome from a reply", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "[SILENT]", final: true });
		expect(tracker.list()[0]?.outcome).toBe("silent");
	});

	test("a bracket-insensitive silence token is still silence", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "[NO_REPLY]", final: true });
		expect(tracker.list()[0]?.outcome).toBe("silent");
	});

	test("a ledgered turn failure is a distinct outcome", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "[turn failed] boom", final: true });
		expect(tracker.list()[0]?.outcome).toBe("failed");
	});

	test("a final message for a turn never seen in flight is marked unobserved", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.final({ turnId: "unseen", origin: ORIGIN, role: "assistant", text: "hi", final: true, deliveryId: "d1" });
		// Its zero counters are an absence of evidence, not evidence of no work.
		expect(only(tracker)).toMatchObject({ turnId: "unseen", outcome: "replied", deliveryId: "d1", observed: false });
	});

	test("a turn seen in flight keeps its observed counters through the final message", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 15_000, toolCalls: 6, outputTokens: 900 });
		tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "hi", final: true });
		expect(only(tracker)).toMatchObject({ observed: true, toolCalls: 6, outputTokens: 900 });
	});

	test("finished turns persist for ten minutes and then leave", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.final({ turnId: "t1", origin: ORIGIN, role: "assistant", text: "hi", final: true });
		time.advance(TURN_RETENTION_MS);
		expect(tracker.prune()).toBe(false);
		expect(tracker.list()).toHaveLength(1);
		time.advance(1);
		expect(tracker.prune()).toBe(true);
		expect(tracker.list()).toHaveLength(0);
	});

	test("live turns sort oldest first and finished turns newest first, after them", () => {
		const time = clock();
		const tracker = new TurnTracker(time.now);
		tracker.final({ turnId: "done-old", origin: ORIGIN, role: "assistant", text: "a", final: true });
		time.advance(1000);
		tracker.final({ turnId: "done-new", origin: ORIGIN, role: "assistant", text: "b", final: true });
		tracker.progress({ turnId: "live-old", origin: ORIGIN, elapsedMs: 30_000, toolCalls: 0, outputTokens: 0 });
		tracker.progress({ turnId: "live-new", origin: ORIGIN, elapsedMs: 1000, toolCalls: 0, outputTokens: 0 });
		expect(tracker.list().map((turn) => turn.turnId)).toEqual(["live-old", "live-new", "done-new", "done-old"]);
	});

	test("clear() forgets everything: after a stream drop nothing known is live", () => {
		const tracker = new TurnTracker(clock().now);
		tracker.progress({ turnId: "t1", origin: ORIGIN, elapsedMs: 1000, toolCalls: 1, outputTokens: 1 });
		tracker.clear();
		expect(tracker.list()).toHaveLength(0);
		expect(tracker.activeCount).toBe(0);
	});
});
