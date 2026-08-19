import { expect, test } from "bun:test";
import { consoleStartupDecision, renderConsoleStatusSummary } from "../../src/console/console";

const healthyHealth = {
	status: "healthy",
	state: "running",
	main: { resumed: true, session_id: "main-session" },
};

const healthyStatus = {
	...healthyHealth,
	turn_state: "busy",
	follow_up_queue_depth: 2,
	journal: { head_cursor: "7:42", degraded: false },
	lock: {
		held: true,
		holder: { session_id: "main-session" },
		queue_len: 1,
		stuck: false,
		quarantined: true,
	},
	write_mode: false,
	reconcile: { last_ok_at: 10_000, cycle_ms: 5_000, drift_count: 3 },
};

test("console maps health and status into the owner-visible summary", () => {
	expect(consoleStartupDecision(healthyHealth, healthyStatus)).toEqual({ interactive: true });
	const summary = renderConsoleStatusSummary(healthyHealth, healthyStatus, 15_000);
	expect(summary).toContain("daemon: status=healthy state=running");
	expect(summary).toContain("main: resumed=true session_id=main-session turn_state=busy follow_up_queue_depth=2");
	expect(summary).toContain("journal: head_cursor=7:42 degraded=false");
	expect(summary).toContain("lock: held=true holder=session=main-session queue_len=1 stuck=false quarantined=true write_mode=false");
	expect(summary).toContain("reconcile: freshness=fresh last_ok_at=10000 age_ms=5000 cycle_ms=5000 drift_count=3");
});

test("console refuses a failed-closed or unhealthy daemon before interactive input", () => {
	const failedClosed = {
		status: "unhealthy",
		state: "failed_closed",
		reason: "profile_drift",
		main: { resumed: false, session_id: null },
	};
	const failedClosedDecision = consoleStartupDecision(failedClosed, failedClosed);
	expect(failedClosedDecision).toMatchObject({ interactive: false });
	expect(failedClosedDecision.refusal).toContain("failed closed");
	expect(failedClosedDecision.refusal).toContain("profile_drift");
	expect(failedClosedDecision.refusal).toContain("fenced");

	const unavailableDecision = consoleStartupDecision(
		{ status: "unhealthy", state: "degraded", main: { resumed: true } },
		{ status: "unhealthy", state: "degraded", main: { resumed: true } },
	);
	expect(unavailableDecision).toMatchObject({ interactive: false });
	expect(unavailableDecision.refusal).toContain("not healthy");
});
