import { expect, test } from "bun:test";
import { sweepIdleSessions } from "../src/server/server";

const HOUR = 60 * 60 * 1000;
const t0 = Date.parse("2026-09-26T00:00:00.000Z");
const row = (origin: object, lastActivityAt: string | null, epoch = 0) => ({
	origin_ref_json: JSON.stringify(origin),
	created_at: "2026-09-25T00:00:00.000Z",
	last_activity_at: lastActivityAt,
	epoch,
	last_bootstrapped_epoch: epoch,
	bootstrap_applied_at: null,
	bootstrap_sections_json: "[]",
	bootstrap_byte_count: 0,
	bootstrap_truncated: 0,
	bootstrap_diagnostics_json: "[]",
});
const slack = { platform: "slack", kind: "channel", conversationId: "C1" };
const dm = { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" };
const loopback = { platform: "loopback", kind: "loopback", conversationId: "loopback" };
const monitor = { platform: "monitor", kind: "eventtype", conversationId: "worklog.daily" };

function harness(idleMs: number | undefined, rows: ReturnType<typeof row>[], running: string[] = []) {
	const resets: Array<{ key: string; originRefJson: string }> = [];
	const runtime = {
		config: { sessionIdleResetMs: idleMs } as { sessionIdleResetMs?: number },
		personaSessions: {
			state: (key: string) => (running.includes(key) ? "turn-running" : "idle"),
			reset: async (key: string, originRefJson: string) => {
				resets.push({ key, originRefJson });
			},
		},
	};
	return { runtime: runtime as never, database: { sessionRows: () => rows }, resets };
}

test("idle rotation is off when sessionIdleResetMs is unset", async () => {
	const h = harness(undefined, [row(slack, new Date(t0 - 48 * HOUR).toISOString())]);
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual([]);
	expect(h.resets).toEqual([]);
});

test("a session quiet past the window is reset exactly like /new; a fresh one is left alone", async () => {
	const h = harness(6 * HOUR, [
		row(slack, new Date(t0 - 7 * HOUR).toISOString(), 2),
		row(dm, new Date(t0 - 5 * HOUR).toISOString()),
	]);
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual(["slack/channel/C1"]);
	expect(h.resets).toEqual([{ key: "slack/channel/C1", originRefJson: JSON.stringify(slack) }]);
});

test("loopback and monitor origins are never rotated", async () => {
	const h = harness(HOUR, [
		row(loopback, new Date(t0 - 10 * HOUR).toISOString()),
		row(monitor, new Date(t0 - 10 * HOUR).toISOString()),
	]);
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual([]);
});

test("a running turn is skipped, not interrupted", async () => {
	const h = harness(HOUR, [row(slack, new Date(t0 - 10 * HOUR).toISOString())], ["slack/channel/C1"]);
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual([]);
	expect(h.resets).toEqual([]);
});

test("created_at stands in when last_activity_at is null; a failed reset does not stop the sweep", async () => {
	const h = harness(HOUR, [row(slack, null), row(dm, new Date(t0 - 2 * HOUR).toISOString())]);
	let calls = 0;
	(h.runtime as { personaSessions: { reset: (k: string) => Promise<void> } }).personaSessions.reset = async (k) => {
		calls++;
		if (k === "slack/channel/C1") throw new Error("boom");
	};
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual(["slack/dm/D1/peer=U1"]);
	expect(calls).toBe(2);
});
