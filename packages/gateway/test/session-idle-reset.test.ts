import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import { sweepIdleSessions } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

const HOUR = 60 * 60 * 1000;
const t0 = Date.parse("2026-09-26T00:00:00.000Z");
const row = (
	origin: object,
	lastActivityAt: string | null,
	epoch = 0,
	key = originKey(origin as OriginRef),
	gjcSessionId = "gjc-bound",
) => ({
	origin_key: key,
	gjc_session_id: gjcSessionId,
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
	return { runtime: runtime as never, database: { sessionIdentityRows: () => rows }, resets };
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

test("a row whose stored origin ref names another conversation is skipped and logged once; a matching row still resets", async () => {
	const thread = { platform: "discord", kind: "thread", conversationId: "T9", parentId: "C9" };
	const channel = { platform: "discord", kind: "channel", conversationId: "C9" };
	const quiet = new Date(t0 - 10 * HOUR).toISOString();
	const h = harness(HOUR, [row(thread, quiet, 0, "discord/channel/C9"), row(channel, quiet)]);
	const errorSpy = spyOn(console, "error").mockImplementation(() => {});
	try {
		expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual(["discord/channel/C9"]);
		expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual(["discord/channel/C9"]);
		expect(h.resets.map((reset) => reset.key)).toEqual(["discord/channel/C9", "discord/channel/C9"]);
		const skipped = errorSpy.mock.calls
			.map((call) => String(call[0]))
			.filter((line) => line.startsWith("session_idle_reset_skipped"));
		expect(skipped).toEqual([
			`session_idle_reset_skipped origin=discord/channel/C9 ref=${originKey(thread as OriginRef)} reason=origin_ref_mismatch`,
		]);
	} finally {
		errorSpy.mockRestore();
	}
});

test("a conversation with no gjc session since its last rotation is not rotated again", async () => {
	const quiet = new Date(t0 - 10 * HOUR).toISOString();
	const h = harness(3 * HOUR, [row(slack, quiet, 4, undefined, ""), row(dm, quiet, 4, undefined, "gjc-bound")]);
	expect(await sweepIdleSessions(h.runtime, h.database, t0)).toEqual(["slack/dm/D1/peer=U1"]);
	expect(h.resets.map((reset) => reset.key)).toEqual(["slack/dm/D1/peer=U1"]);
});

test("on the real store a rotated conversation waits for its next gjc session before it can rotate again", async () => {
	const directory = await mkdtemp(join(tmpdir(), "idle-reset-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const key = originKey(slack as OriginRef);
		const rotations: number[] = [];
		const runtime = {
			config: { sessionIdleResetMs: 3 * HOUR },
			personaSessions: {
				state: () => "idle",
				reset: async (resetKey: string, originRefJson: string) => {
					rotations.push(database.bumpEpoch(resetKey, originRefJson));
				},
			},
		} as never;
		const later = (hours: number) => Date.now() + hours * HOUR;
		database.bumpEpoch(key, JSON.stringify(slack));
		expect(database.putSessionAtEpoch(key, "gjc-1", 1)).toBe(true);
		expect(await sweepIdleSessions(runtime, database, later(4))).toEqual([key]);
		expect(await sweepIdleSessions(runtime, database, later(8))).toEqual([]);
		expect(await sweepIdleSessions(runtime, database, later(30))).toEqual([]);
		expect(database.putSessionAtEpoch(key, "gjc-2", 2)).toBe(true);
		expect(await sweepIdleSessions(runtime, database, later(34))).toEqual([key]);
		expect(rotations).toEqual([2, 3]);
	} finally {
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});
