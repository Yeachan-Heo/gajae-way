import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { UNREAD_CONTEXT_MAX_AGE_MS, UNREAD_CONTEXT_MAX_LINES } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

/**
 * Live incident 2026-08-28: a bot whose session for a channel was created at
 * 11:02 received 287 unread rows going back 27 hours, and answered a day-old
 * instruction as if it had just been given.
 */
let home = "";
let db: GatewayDatabase;
const KEY = "discord/channel/c1";

beforeEach(async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-unread-"));
	db = await GatewayDatabase.open(join(home, "gateway.db"));
});
afterEach(async () => {
	db.close();
	await rm(home, { recursive: true, force: true });
});

describe("unread bounds", () => {
	test("the age window is six hours and the line cap is 60", () => {
		expect(UNREAD_CONTEXT_MAX_AGE_MS).toBe(6 * 60 * 60 * 1000);
		expect(UNREAD_CONTEXT_MAX_LINES).toBe(60);
	});

	test("contextUnread with a cutoff returns only rows at or after it", () => {
		db.contextRecord({ messageId: "old", originKey: KEY, body: "old" });
		db.contextRecord({ messageId: "new", originKey: KEY, body: "new" });
		const all = db.contextUnread(KEY, 100);
		expect(all).toHaveLength(2);
		const future = new Date(Date.now() + 60_000).toISOString();
		expect(db.contextUnread(KEY, 100, future)).toHaveLength(0);
		const past = new Date(Date.now() - 60_000).toISOString();
		expect(db.contextUnread(KEY, 100, past)).toHaveLength(2);
	});

	test("older rows are counted so truncation can be stated", () => {
		db.contextRecord({ messageId: "a", originKey: KEY, body: "a" });
		const future = new Date(Date.now() + 60_000).toISOString();
		expect(db.contextUnreadOlderCount(KEY, future)).toBe(1);
		expect(db.contextUnreadOlderCount(KEY, new Date(Date.now() - 60_000).toISOString())).toBe(0);
	});

	test("dropping a backlog consumes it once instead of re-evaluating forever", () => {
		db.contextRecord({ messageId: "a", originKey: KEY, body: "a" });
		db.contextRecord({ messageId: "b", originKey: KEY, body: "b" });
		const future = new Date(Date.now() + 60_000).toISOString();
		expect(db.contextConsumeOlderThan(KEY, future)).toBe(2);
		expect(db.contextUnread(KEY, 100)).toHaveLength(0);
		expect(db.contextUnreadOlderCount(KEY, future)).toBe(0);
	});

	test("a dropped row stays in the table as history", () => {
		db.contextRecord({ messageId: "a", originKey: KEY, body: "a" });
		db.contextConsumeOlderThan(KEY, new Date(Date.now() + 60_000).toISOString());
		const rows = db.contextUnread(KEY, 100);
		expect(rows).toHaveLength(0);
	});

	test("sessionCreatedAt is exposed so the cutoff can respect a fresh session", () => {
		expect(db.sessionCreatedAt(KEY)).toBeUndefined();
		db.putSession(KEY, "session-1");
		const created = db.sessionCreatedAt(KEY);
		expect(created).toBeTruthy();
		expect(Date.parse(created as string)).toBeGreaterThan(0);
	});
});
