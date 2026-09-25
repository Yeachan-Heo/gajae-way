import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
			"ALTER TABLE memory_intents DROP COLUMN quarantine_reason; ALTER TABLE memory_intents DROP COLUMN attempts; ALTER TABLE deliveries DROP COLUMN last_error; DELETE FROM schema_migrations WHERE version >= 23",
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

test("adds last_error to existing deliveries without touching their state (#171)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-migration-"));
	const path = join(directory, "gateway.db");
	try {
		const current = await GatewayDatabase.open(path);
		current.deliveryCreate({
			id: "legacy-delivery",
			turnId: "turn",
			originKey: "discord/channel/c",
			payloadJson: "{}",
		});
		current.deliveryUpdate("legacy-delivery", "pending", 2);
		current.close();

		const legacy = new Database(path);
		legacy.exec("ALTER TABLE deliveries DROP COLUMN last_error; DELETE FROM schema_migrations WHERE version = 24");
		legacy.close();

		const migrated = await GatewayDatabase.open(path);
		expect(migrated.schemaVersion).toBe(24);
		expect(migrated.deliveryRows()[0]).toMatchObject({
			delivery_id: "legacy-delivery",
			state: "pending",
			attempts: 2,
			last_error: null,
		});
		migrated.close();
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
