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
		expect(database.schemaVersion).toBe(16);
		database.withTransaction(() => database.putSession("loopback/loopback/loopback", "session-1"));
		expect(database.getSession("loopback/loopback/loopback")).toBe("session-1");
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
