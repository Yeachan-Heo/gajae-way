import { describe, expect, test } from "bun:test";
import { formatClock, formatClockSeconds, formatDuration } from "../src/format";
import { DEFAULT_ALLOWLIST } from "../src/gate";
import { TurnTracker } from "../src/turns";
import { renderIndex } from "../src/ui";
import { buildSnapshot } from "../src/view";
import { FIXED_NOW, MONITOR, monitorEvent, SESSIONS, STATUS } from "./fixture";

/**
 * The client recomputes ticking ages, so it needs the duration algorithm in the
 * browser. A hand-written mirror of it drifted once already. These tests pin the
 * property that replaced the mirror: the page ships the real implementations, so
 * there is exactly one algorithm and it cannot drift.
 */
async function document(): Promise<string> {
	const snapshot = await buildSnapshot({
		request: async (method, params) => {
			switch (method) {
				case "gateway.status":
					return STATUS;
				case "session.list":
					return SESSIONS;
				case "monitor.list":
					return { monitors: [MONITOR] };
				case "monitor.inspect":
					return { monitor: MONITOR, recentEvents: [monitorEvent(params as { monitorId: string })] };
				default:
					throw new Error(`unexpected verb ${method}`);
			}
		},
		turns: new TurnTracker(() => FIXED_NOW.getTime()),
		now: () => FIXED_NOW,
	});
	return renderIndex(snapshot, DEFAULT_ALLOWLIST);
}

/** Evaluate the shipped script's helpers in a bare context, with no DOM. */
function shipped(html: string): {
	formatDuration: (ms: number) => string;
	formatClock: (at: Date) => string;
	formatClockSeconds: (at: Date) => string;
	relative: (iso: string, atMs: number) => string;
} {
	const script = html.slice(html.lastIndexOf("<script>") + "<script>".length, html.lastIndexOf("</script>"));
	const start = script.indexOf("var formatDuration =");
	// The injected block sits at the top of the IIFE; the hand-written body starts
	// at the bootstrap lookup, so that is where the block ends.
	const end = script.indexOf("var boot =");
	expect(start).toBeGreaterThan(-1);
	expect(end).toBeGreaterThan(start);
	const source = script.slice(start, end);
	// A bare Function context: no document, no window, nothing but the helpers.
	return new Function(
		`${source}\nreturn { formatDuration, formatClock, formatClockSeconds, relative };`,
	)() as ReturnType<typeof shipped>;
}

describe("the shipped helpers are the server's own", () => {
	test("the page carries no second, hand-written duration algorithm", async () => {
		const html = await document();
		// One definition only. A restated mirror would add a second.
		expect(html.match(/function formatDuration/g)).toHaveLength(1);
		expect(html.match(/function relative/g)).toHaveLength(1);
		expect(html).not.toContain('days + "d " + hours + "h"');
	});

	test("formatDuration agrees with the server across the whole shape of the scale", async () => {
		const client = shipped(await document());
		const cases = [
			0,
			1,
			999,
			1000,
			9999,
			10_000,
			41_000,
			59_999,
			60_000,
			60_500,
			61_000,
			192_000,
			300_000,
			3_599_000,
			3_600_000,
			3_660_000,
			7_200_000,
			86_399_000,
			86_400_000,
			86_400_000 + 6 * 3_600_000,
			400 * 86_400_000,
			-1,
			-5000,
		];
		for (const ms of cases) {
			expect(client.formatDuration(ms)).toBe(formatDuration(ms));
		}
	});

	test("relative agrees with the server, including the just-now floor and a bad date", async () => {
		const client = shipped(await document());
		const at = FIXED_NOW.getTime();
		for (const offset of [0, 9_999, 10_000, 60_000, 11 * 60_000, 4 * 86_400_000]) {
			const iso = new Date(at - offset).toISOString();
			expect(client.relative(iso, at)).toBe(relativeOnServer(iso, at));
		}
		expect(client.relative("not a date", at)).toBe("—");
	});

	test("the clock helpers agree with the server", async () => {
		const client = shipped(await document());
		for (const iso of ["2026-08-27T14:02:31.000Z", "2026-01-01T00:00:09.000Z", "2026-12-31T23:59:59.000Z"]) {
			const at = new Date(iso);
			expect(client.formatClock(at)).toBe(formatClock(at));
			expect(client.formatClockSeconds(at)).toBe(formatClockSeconds(at));
		}
	});
});

/** `relative` is module-private in ui.ts; this is its contract, restated once for comparison. */
function relativeOnServer(iso: string, atMs: number): string {
	const then = new Date(iso).getTime();
	if (Number.isNaN(then)) return "—";
	const diff = atMs - then;
	if (diff < 10_000) return "just now";
	return `${formatDuration(diff)} ago`;
}
