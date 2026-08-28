import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	CONVERSATION_CONTEXT_RETENTION_MS,
	CONVERSATION_DIFF_MAX_AGE_MS,
	CONVERSATION_DIFF_MAX_ROWS,
	GatewayDatabase,
} from "../src/store/db";

const ORIGIN_KEY = "discord/channel/context-diff";
const NOW = new Date("2026-08-28T11:00:00.000Z");
let directory = "";
let database: GatewayDatabase | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function open(): Promise<GatewayDatabase> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-context-diff-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	return database;
}

function record(db: GatewayDatabase, messageId: string, body: string, receivedAt: string, authorName = "alice"): void {
	db.contextRecord({
		messageId,
		originKey: ORIGIN_KEY,
		authorId: `author-${authorName}`,
		authorName,
		body,
		receivedAt,
	});
}

test("287-row stale backlog selects only newest recent rows chronologically and never replays omissions", async () => {
	const db = await open();
	const staleAt = new Date(NOW.getTime() - CONVERSATION_DIFF_MAX_AGE_MS - 60_000).toISOString();
	for (let index = 0; index < 226; index++)
		record(db, `stale-${String(index).padStart(3, "0")}`, `/old-command-${index}`, staleAt);
	for (let index = 0; index < CONVERSATION_DIFF_MAX_ROWS; index++) {
		const receivedAt = new Date(NOW.getTime() - (CONVERSATION_DIFF_MAX_ROWS - index) * 1_000).toISOString();
		record(db, `recent-${String(index).padStart(3, "0")}`, `recent-${index}`, receivedAt, index % 2 ? "bot" : "bob");
	}
	record(db, "trigger-current", "@persona current owner request", NOW.toISOString(), "owner");

	const first = db.contextWindow(ORIGIN_KEY, "trigger-current", NOW);
	expect(first.rows).toHaveLength(CONVERSATION_DIFF_MAX_ROWS);
	expect(first.rows.map((row) => row.body)).toEqual(
		Array.from({ length: CONVERSATION_DIFF_MAX_ROWS }, (_, index) => `recent-${index}`),
	);
	expect(first.rows.some((row) => row.body.startsWith("/old-command"))).toBe(false);
	expect(first.diagnostics.expired).toBe(226);
	expect(first.diagnostics.truncated).toBe(0);

	db.contextConsume([...first.selectedMessageIds, "trigger-current"]);
	record(db, "trigger-next", "next owner turn", new Date(NOW.getTime() + 1_000).toISOString(), "owner");
	const next = db.contextWindow(ORIGIN_KEY, "trigger-next", new Date(NOW.getTime() + 1_000));
	expect(next.rows).toEqual([]);
	expect(next.diagnostics.expired).toBe(226);
});

test("more than the count bound keeps newest N once and expires older recent rows", async () => {
	const db = await open();
	for (let index = 0; index < CONVERSATION_DIFF_MAX_ROWS + 7; index++)
		record(
			db,
			`recent-${String(index).padStart(3, "0")}`,
			`body-${index}`,
			new Date(NOW.getTime() - (CONVERSATION_DIFF_MAX_ROWS + 7 - index) * 1_000).toISOString(),
		);
	record(db, "trigger", "current", NOW.toISOString());

	const window = db.contextWindow(ORIGIN_KEY, "trigger", NOW);
	expect(window.rows).toHaveLength(CONVERSATION_DIFF_MAX_ROWS);
	expect(window.rows[0]?.body).toBe("body-7");
	expect(window.rows.at(-1)?.body).toBe(`body-${CONVERSATION_DIFF_MAX_ROWS + 6}`);
	expect(window.diagnostics.truncated).toBe(7);
	db.contextConsume([...window.selectedMessageIds, "trigger"]);

	record(db, "trigger-2", "again", new Date(NOW.getTime() + 1_000).toISOString());
	expect(db.contextWindow(ORIGIN_KEY, "trigger-2", new Date(NOW.getTime() + 1_000)).rows).toEqual([]);
	expect(db.contextDiagnostics(ORIGIN_KEY).truncated).toBe(7);
});

test("exact timestamp ties use deterministic message-id ordering", async () => {
	const db = await open();
	const tiedAt = new Date(NOW.getTime() - 1_000).toISOString();
	for (const id of ["message-c", "message-a", "message-b"]) record(db, id, id, tiedAt);
	record(db, "trigger", "current", NOW.toISOString());

	const window = db.contextWindow(ORIGIN_KEY, "trigger", NOW);
	expect(window.rows.map((row) => row.message_id)).toEqual(["message-a", "message-b", "message-c"]);
});

test("reset floor survives restart and excludes every pre-reset row", async () => {
	const db = await open();
	const path = join(directory, "gateway.db");
	const before = new Date(NOW.getTime() - 60_000).toISOString();
	const floor = NOW.toISOString();
	record(db, "before-reset", "old session context", before);
	db.withTransaction(() => db.contextSetFloor(ORIGIN_KEY, floor));
	db.close();
	database = undefined;

	const restarted = await GatewayDatabase.open(path);
	database = restarted;
	record(restarted, "after-reset", "new session context", new Date(NOW.getTime() + 1_000).toISOString());
	record(restarted, "trigger", "current", new Date(NOW.getTime() + 2_000).toISOString());
	const window = restarted.contextWindow(ORIGIN_KEY, "trigger", new Date(NOW.getTime() + 2_000));
	expect(window.rows.map((row) => row.message_id)).toEqual(["after-reset"]);
	expect(window.diagnostics.floorAt).toBe(floor);
	expect(window.diagnostics.expired).toBe(1);
});

test("session creation time participates in the effective context floor", async () => {
	const db = await open();
	record(db, "before-session", "pre-session context", new Date(Date.now() - 60_000).toISOString());
	db.putSession(ORIGIN_KEY, "session-1");
	const sessionFloor = db.contextSessionCreatedAt(ORIGIN_KEY);
	expect(sessionFloor).toBeString();
	const after = new Date(Date.parse(sessionFloor as string) + 1).toISOString();
	record(db, "after-session", "current session context", after);
	const triggerAt = new Date(Date.parse(after) + 1).toISOString();
	record(db, "trigger", "current", triggerAt);

	const window = db.contextWindow(ORIGIN_KEY, "trigger", new Date(triggerAt));
	expect(window.rows.map((row) => row.message_id)).toEqual(["after-session"]);
});

test("active maintenance drops old bodies but preserves recent retry-relevant unread rows", async () => {
	const db = await open();
	const now = new Date();
	const old = new Date(now.getTime() - CONVERSATION_CONTEXT_RETENTION_MS - 60_000).toISOString();
	const recent = new Date(now.getTime() - 60_000).toISOString();
	record(db, "old-body", "private stale body", old);
	record(db, "retry-relevant", "recent retry body", recent);

	db.contextMaintain(now);
	expect(db.contextUnread(ORIGIN_KEY).map((row) => row.message_id)).toEqual(["retry-relevant"]);
	expect(db.contextDiagnostics(ORIGIN_KEY)).toMatchObject({ unread: 1, expired: 1 });
	const raw = new Database(join(directory, "gateway.db"), { readonly: true });
	expect(raw.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM conversation_context").get()?.n).toBe(1);
	raw.close();
});
