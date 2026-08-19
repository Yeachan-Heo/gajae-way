import { expect, test } from "bun:test";
import {
	ConsoleOutput,
	consoleStartupDecision,
	renderConsoleStatusSummary,
	sanitizeConsoleText,
} from "../../src/console/console";

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
	expect(summary).toContain(
		"lock: held=true holder=session=main-session queue_len=1 stuck=false quarantined=true write_mode=false",
	);
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

test("untrusted gateway text escapes CSI, OSC 52, C0, and C1 controls before terminal publication", async () => {
	const hostile = "readable \x1b[2J CSI \x1b]52;c;SGVsbG8=\u0007 OSC52 \u0000\b\t\n\r\u009b1A C1";
	const sanitized = sanitizeConsoleText(hostile);
	expect(sanitized).toContain("readable \\x1B[2J CSI");
	expect(sanitized).toContain("\\x1B]52;c;SGVsbG8=\\u0007 OSC52");
	expect(sanitized).toContain("\\u0000\\u0008\\t\\n\\r\\u009B1A C1");
	expect(sanitized).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);

	const writes: string[] = [];
	const output = new ConsoleOutput((text) => {
		writes.push(text);
	});
	await output.writeTrusted("\x1b[2K");
	await output.writeUntrusted(hostile);
	expect(writes).toEqual(["\x1b[2K", sanitized]);
	expect(writes[1]).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);

	const hostileRefusal = consoleStartupDecision(
		{ status: "unhealthy", state: "failed_closed", reason: "\x1b]52;c;SGVsbG8=\u0007" },
		{ status: "unhealthy", state: "failed_closed", reason: "\x1b]52;c;SGVsbG8=\u0007" },
	).refusal;
	expect(hostileRefusal).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
	expect(hostileRefusal).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
});

test("console output serializes complete frames from concurrent publishers", async () => {
	const writes: string[] = [];
	let releaseFirst: (() => void) | undefined;
	let signalFirst: (() => void) | undefined;
	const firstBlocked = new Promise<void>((resolve) => {
		releaseFirst = resolve;
	});
	const firstStarted = new Promise<void>((resolve) => {
		signalFirst = resolve;
	});
	const output = new ConsoleOutput(async (text) => {
		writes.push(text);
		if (text === "first frame\n") {
			signalFirst?.();
			await firstBlocked;
		}
	});
	const first = output.writeFrame("first frame\n");
	await firstStarted;
	const second = output.writeFrame("second frame\n");
	await Bun.sleep(10);
	expect(writes).toEqual(["first frame\n"]);
	releaseFirst?.();
	await Promise.all([first, second]);
	expect(writes).toEqual(["first frame\n", "second frame\n"]);
});
