import { describe, expect, test } from "bun:test";
import type { OriginRef } from "@gajae-gateway/protocol";
import {
	cronSummary,
	formatCount,
	formatDuration,
	originId,
	originLabel,
	pluralise,
	shortId,
	triggerSummary,
} from "../src/format";

describe("formatDuration", () => {
	test("shows at most two units", () => {
		expect(formatDuration(4 * 86_400_000 + 6 * 3_600_000 + 30 * 60_000)).toBe("4d 6h");
		expect(formatDuration(2 * 3_600_000 + 5 * 60_000 + 9000)).toBe("2h 5m");
		expect(formatDuration(192_000)).toBe("3m 12s");
		expect(formatDuration(41_000)).toBe("41s");
	});

	test("drops a zero trailing unit, because 5m is what a person says", () => {
		expect(formatDuration(300_000)).toBe("5m");
		expect(formatDuration(3_600_000)).toBe("1h");
		expect(formatDuration(86_400_000)).toBe("1d");
		expect(formatDuration(0)).toBe("0s");
	});

	test("never renders a negative age", () => {
		expect(formatDuration(-5000)).toBe("0s");
	});
});

describe("shortId", () => {
	test("keeps both ends of a long id and leaves a short one alone", () => {
		expect(shortId("1493635653441945762")).toBe("1493…5762");
		expect(shortId("smoke-1")).toBe("smoke-1");
	});
});

describe("originLabel", () => {
	const cases: [OriginRef, string][] = [
		[{ platform: "loopback", kind: "loopback", conversationId: "loopback" }, "loopback console"],
		[
			{ platform: "discord", kind: "dm", conversationId: "1468535438498336923", peerId: "660473980301344768" },
			"discord DM · 6604…4768",
		],
		[{ platform: "discord", kind: "channel", conversationId: "1493635653441945762" }, "discord channel · 1493…5762"],
		[{ platform: "monitor", kind: "eventtype", conversationId: "review.due" }, "monitor event · review.due"],
	];

	test.each(cases)("names %o plainly", (origin, expected) => {
		expect(originLabel(origin)).toBe(expected);
	});

	test("an origin kind this protocol copy does not know is still named from its own fields", () => {
		// The running deployment serves `work/task`, which ORIGIN_KINDS lacks.
		expect(originLabel({ platform: "work", kind: "task", conversationId: "smoke-1" } as unknown as OriginRef)).toBe(
			"work task · smoke-1",
		);
	});
});

describe("originId", () => {
	test("uses the canonical key when the origin is valid", () => {
		expect(originId({ platform: "discord", kind: "channel", conversationId: "123" })).toBe("discord/channel/123");
		expect(originId({ platform: "discord", kind: "dm", conversationId: "1", peerId: "2" })).toBe("discord/dm/1/peer=2");
	});

	test("falls back to the same deterministic shape instead of throwing", () => {
		expect(originId({ platform: "work", kind: "task", conversationId: "smoke-1" } as unknown as OriginRef)).toBe(
			"work/task/smoke-1",
		);
	});
});

describe("cronSummary", () => {
	test("says a simple schedule in words", () => {
		expect(cronSummary("30 8 * * 1-5")).toBe("weekdays 08:30");
		expect(cronSummary("0 9 * * 0,6")).toBe("weekends 09:00");
		expect(cronSummary("0 7 * * *")).toBe("daily 07:00");
		expect(cronSummary("15 * * * *")).toBe("daily hourly at :15");
	});

	test("shows the expression verbatim rather than guessing at a complex one", () => {
		expect(cronSummary("*/7 3,15 * * *")).toBe("cron */7 3,15 * * *");
	});

	test("marks an unparseable schedule instead of pretending to understand it", () => {
		expect(cronSummary("every tuesday")).toBe("cron every tuesday (unparseable)");
	});
});

describe("triggerSummary", () => {
	test("names what makes a monitor fire, whatever its kind", () => {
		expect(triggerSummary({ kind: "cron", schedule: "30 8 * * 1-5" })).toBe("weekdays 08:30");
		expect(triggerSummary({ kind: "webhook", route: "/hooks/x" })).toBe("webhook /hooks/x");
		expect(triggerSummary({ kind: "watcher", root: "/repo" })).toBe("watches /repo");
		expect(triggerSummary({ kind: "script", command: ["ls", "-l"], intervalMs: 300_000 })).toBe("runs ls -l every 5m");
	});
});

describe("counts", () => {
	test("thousands are separated, because 3240 and 32400 look alike", () => {
		expect(formatCount(3240)).toBe("3,240");
	});

	test("plurals are explicit, so no panel ever says 2 replys", () => {
		expect(pluralise(1, "session")).toBe("1 session");
		expect(pluralise(2, "session")).toBe("2 sessions");
		expect(pluralise(2, "reply", "replies")).toBe("2 replies");
		expect(pluralise(1, "reply", "replies")).toBe("1 reply");
	});
});
