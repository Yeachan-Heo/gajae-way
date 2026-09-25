import { describe, expect, test } from "bun:test";
import type { GatewayStatusResult } from "@gajae-gateway/protocol";
import { ATTENTION_GAPS, buildAttention, PENDING_DELIVERY_MS, STUCK_EVENT_MS } from "../src/attention";
import { FIXED_NOW, MONITOR, monitorEvent, STATUS } from "./fixture";

const status = (
	pending: number,
	oldestPendingAgeMs: number | null,
	expired = 0,
	recentExpired: NonNullable<GatewayStatusResult["delivery"]>["recentExpired"] = [],
	recentPending: NonNullable<GatewayStatusResult["delivery"]>["recentPending"] = [],
) => ({
	...STATUS,
	delivery: { pending, oldestPendingAgeMs, expired, recentExpired, recentPending },
});

describe("buildAttention", () => {
	test("a healthy runtime produces an empty queue", () => {
		expect(buildAttention(STATUS, [{ monitor: MONITOR, recentEvents: [monitorEvent()] }], FIXED_NOW)).toEqual([]);
	});

	test("an unreachable gateway does not fabricate attention items", () => {
		expect(buildAttention(null, [], FIXED_NOW)).toEqual([]);
	});

	test("a young pending delivery is in flight, not an incident", () => {
		expect(buildAttention(status(1, PENDING_DELIVERY_MS - 1), [], FIXED_NOW)).toEqual([]);
	});

	test("a pending delivery past the threshold is a symptom stated in the owner's language", () => {
		const [item] = buildAttention(status(2, 11 * 60_000), [], FIXED_NOW);
		expect(item?.title).toBe("2 replies never reached their platform");
		expect(item?.detail).toContain("waiting 11m");
		expect(item?.tone).toBe("danger");
	});

	test("a single stuck delivery is still an incident, one rank lower", () => {
		const [item] = buildAttention(status(1, PENDING_DELIVERY_MS), [], FIXED_NOW);
		expect(item?.title).toBe("1 reply never reached their platform");
		expect(item?.severity).toBe(2);
	});

	test("a stuck delivery names its last failure and next retry", () => {
		const [item] = buildAttention(
			status(
				1,
				PENDING_DELIVERY_MS,
				0,
				[],
				[
					{
						deliveryId: "delivery-1",
						originKey: "discord/channel/room",
						state: "pending",
						attempts: 2,
						lastError: "network",
						nextRetryAt: "2026-08-27T14:00:04.000Z",
						createdAt: "2026-08-27T13:50:00.000Z",
					},
				],
			),
			[],
			FIXED_NOW,
		);
		expect(item?.detail).toContain("Last failure: network after 2 attempts; next retry 2026-08-27T14:00:04.000Z.");
	});

	test("expired deliveries raise attention with identifiers and origins only", () => {
		const [item] = buildAttention(
			status(0, null, 2, [
				{
					deliveryId: "delivery-1",
					originKey: "discord/channel/room",
					attempts: 5,
					expiredAt: "2026-08-27T14:00:00.000Z",
					lastError: "not_found",
				},
			]),
			[],
			FIXED_NOW,
		);
		expect(item?.key).toBe("delivery:expired");
		expect(item?.title).toBe("2 deliveries expired");
		expect(item?.detail).toBe("delivery-1 · discord/channel/room");
		expect(item?.at).toBe("2026-08-27T14:00:00.000Z");
	});

	test("a failed monitor event is an incident immediately, with no age grace", () => {
		const [item] = buildAttention(
			STATUS,
			[
				{
					monitor: MONITOR,
					recentEvents: [
						monitorEvent({ stage: "failed", firedAt: new Date(FIXED_NOW.getTime() - 1000).toISOString() }),
					],
				},
			],
			FIXED_NOW,
		);
		expect(item?.title).toBe("weekday-review · review.due produced nothing");
		expect(item?.detail).toBe("The event was failed and has not been replayed.");
		expect(item?.tone).toBe("danger");
	});

	test("an admitted event only escalates once the gateway's own replay has had time to run", () => {
		const young = monitorEvent({
			stage: "admitted",
			firedAt: new Date(FIXED_NOW.getTime() - STUCK_EVENT_MS + 1).toISOString(),
		});
		expect(buildAttention(STATUS, [{ monitor: MONITOR, recentEvents: [young] }], FIXED_NOW)).toEqual([]);

		const old = monitorEvent({
			stage: "admitted",
			firedAt: new Date(FIXED_NOW.getTime() - STUCK_EVENT_MS).toISOString(),
		});
		const [item] = buildAttention(STATUS, [{ monitor: MONITOR, recentEvents: [old] }], FIXED_NOW);
		expect(item?.tone).toBe("warn");
		expect(item?.detail).toBe("The event was admitted but never dispatched to a session.");
	});

	test("terminal stages are never attention items", () => {
		for (const stage of ["delivered", "authored", "authored_no_delivery", "failed_no_retry"]) {
			const event = monitorEvent({ stage, firedAt: new Date(FIXED_NOW.getTime() - 30 * 86_400_000).toISOString() });
			expect(buildAttention(STATUS, [{ monitor: MONITOR, recentEvents: [event] }], FIXED_NOW)).toEqual([]);
		}
	});

	test("items are ordered by severity, and the ordering is stable", () => {
		const items = buildAttention(
			status(3, 20 * 60_000),
			[
				{
					monitor: MONITOR,
					recentEvents: [
						monitorEvent({
							eventId: "b",
							stage: "admitted",
							firedAt: new Date(FIXED_NOW.getTime() - 3_600_000).toISOString(),
						}),
						monitorEvent({ eventId: "a", stage: "failed" }),
					],
				},
			],
			FIXED_NOW,
		);
		expect(items.map((item) => item.severity)).toEqual([1, 2, 3]);
		expect(items[0]?.key).toBe("delivery:pending");
	});

	test("the queue declares what it cannot see, so an empty queue is not read as all-clear", () => {
		expect(ATTENTION_GAPS.map((gap) => gap.gap)).toEqual(["G8", "G3", "G5"]);
		for (const gap of ATTENTION_GAPS) expect(gap.missing.length).toBeGreaterThan(10);
	});
});
