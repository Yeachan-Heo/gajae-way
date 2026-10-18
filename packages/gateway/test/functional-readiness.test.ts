import { expect, test } from "bun:test";
import {
	FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
	FUNCTIONAL_READINESS_WINDOW_MS,
	FunctionalReadiness,
} from "../src/functional-readiness";

const START = Date.UTC(2026, 0, 1);

function fail(
	readiness: FunctionalReadiness,
	category: "session_create" | "turn" | "delivery" | "monitor_delivery",
	at: number,
	code = "persona_bind_failed",
): void {
	readiness.recordFailure({ category, code, detail: "safe failure detail", origin: "discord/dm/peer=42", at });
}

test("readiness degrades only after a category reaches the consecutive failure threshold", () => {
	const readiness = new FunctionalReadiness();
	fail(readiness, "session_create", START);
	fail(readiness, "session_create", START + 1_000);
	fail(readiness, "monitor_delivery", START + 2_000, "authoring_turn_failed");
	fail(readiness, "session_create", START + 3_000);

	const snapshot = readiness.snapshot(START + 3_000);
	expect(snapshot).toMatchObject({ ready: false, state: "degraded", failureThreshold: 3 });
	expect(snapshot.reason).toContain("session_create: persona_bind_failed");
	expect(snapshot.failure).toMatchObject({
		category: "session_create",
		code: "persona_bind_failed",
		detail: "safe failure detail",
		origin: "discord/dm/peer=42",
		consecutiveFailures: FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
	});
	expect(Date.parse(snapshot.failure?.at ?? "")).toBe(START + 3_000);
});

test("a success resets only its own failure streak", () => {
	const readiness = new FunctionalReadiness();
	for (let i = 0; i < 3; i++) fail(readiness, "monitor_delivery", START + i);
	readiness.recordSuccess("delivery");
	expect(readiness.snapshot(START + 3).ready).toBe(false);

	readiness.recordSuccess("monitor_delivery");
	expect(readiness.snapshot(START + 3)).toMatchObject({ ready: true, state: "ready", reason: null, failure: null });
});

test("failure evidence expires outside the bounded readiness window", () => {
	const readiness = new FunctionalReadiness();
	fail(readiness, "delivery", START);
	fail(readiness, "delivery", START + FUNCTIONAL_READINESS_WINDOW_MS - 1_000);
	fail(readiness, "delivery", START + FUNCTIONAL_READINESS_WINDOW_MS + 1_000);
	expect(readiness.snapshot(START + FUNCTIONAL_READINESS_WINDOW_MS + 1_000).ready).toBe(true);

	fail(readiness, "delivery", START + FUNCTIONAL_READINESS_WINDOW_MS + 2_000);
	fail(readiness, "delivery", START + FUNCTIONAL_READINESS_WINDOW_MS + 3_000);
	expect(readiness.snapshot(START + FUNCTIONAL_READINESS_WINDOW_MS + 3_000).ready).toBe(false);
});

test("readiness retains only threshold-sized evidence and bounds diagnostic fields", () => {
	const readiness = new FunctionalReadiness();
	for (let i = 0; i < 100; i++) {
		readiness.recordFailure({
			category: "turn",
			code: `turn/${i}`,
			detail: `${"x".repeat(250)}\nunsafe`,
			at: START + i,
			origin: "origin".repeat(40),
		});
	}

	const snapshot = readiness.snapshot(START + 100);
	expect(snapshot.ready).toBe(false);
	expect(snapshot.failure?.consecutiveFailures).toBe(FUNCTIONAL_READINESS_FAILURE_THRESHOLD);
	expect(snapshot.failure?.code).toBe("turn_99");
	expect(snapshot.failure?.detail).toHaveLength(200);
	expect(snapshot.failure?.detail).not.toContain("\n");
	expect(snapshot.failure?.origin?.length).toBe(160);
});
