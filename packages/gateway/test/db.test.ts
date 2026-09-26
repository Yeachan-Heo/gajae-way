import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { type DatabaseStartupError, GatewayDatabase } from "../src/store/db";

test("migrates the sessions foundation", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		expect(database.schemaVersion).toBe(24);
		database.memoryIntentCreate({ id: "memory-schema", kind: "daily_capture", payloadJson: "{}" });
		expect(database.memoryIntentRows()[0]).toMatchObject({
			state: "queued",
			attempts: 0,
			quarantine_reason: null,
		});
		database.memoryIntentBeginAttempt("memory-schema");
		database.memoryIntentQuarantine("memory-schema", "Error: test failure");
		expect(database.memoryIntentRows()[0]).toMatchObject({
			state: "quarantined",
			attempts: 1,
			quarantine_reason: "Error: test failure",
		});
		database.withTransaction(() => database.putSession("loopback/loopback/loopback", "session-1"));
		expect(database.getSession("loopback/loopback/loopback")).toBe("session-1");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("adds quarantine diagnostics to existing memory intents", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-migration-"));
	const path = join(directory, "gateway.db");
	try {
		const current = await GatewayDatabase.open(path);
		current.memoryIntentCreate({ id: "legacy-intent", kind: "daily_capture", payloadJson: "{}" });
		current.close();

		const legacy = new Database(path);
		legacy.exec(
			"DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; ALTER TABLE memory_intents DROP COLUMN quarantine_reason; ALTER TABLE memory_intents DROP COLUMN attempts; DELETE FROM schema_migrations WHERE version = 24; DELETE FROM schema_migrations WHERE version = 23",
		);
		legacy.close();

		const migrated = await GatewayDatabase.open(path);
		expect(migrated.schemaVersion).toBe(24);
		expect(migrated.memoryIntentRows()[0]).toMatchObject({
			id: "legacy-intent",
			state: "queued",
			attempts: 0,
			quarantine_reason: null,
		});
		migrated.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
test("/new discards platform input but preserves internal lane reports", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const key = originKey(LOOPBACK_ORIGIN);
		database.inboundEnqueue({
			messageId: "platform-before-new",
			originKey: key,
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: "platform message",
			receivedAt: "2026-09-01T00:00:00.000Z",
		});
		database.inboundEnqueue({
			messageId: "lane-report-internal",
			originKey: key,
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: "internal report",
			receivedAt: "2026-09-01T00:00:01.000Z",
			source: "lane_report",
		});
		expect(database.inboundDiscardBefore(key, "2026-09-02T00:00:00.000Z")).toEqual(["platform-before-new"]);
		expect(database.inboundPendingOldest(key)).toMatchObject({
			message_id: "lane-report-internal",
			source: "lane_report",
			engagement_json: null,
			body: "internal report",
		});
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("refuses a database from a newer schema", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-"));
	const path = join(directory, "gateway.db");
	try {
		const raw = new Database(path);
		raw.exec(
			"CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES (99, 'now')",
		);
		raw.close();
		await expect(GatewayDatabase.open(path)).rejects.toMatchObject({
			code: "newer_schema",
		} satisfies Partial<DatabaseStartupError>);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
