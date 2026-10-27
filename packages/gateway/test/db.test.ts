import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { MonitorRegistry } from "../src/monitors/registry";
import { type DatabaseStartupError, Store } from "../src/store";

test("migrates the sessions foundation", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		expect(database.schemaVersion).toBe(39);

		database.transaction((tx) =>
			database.memory.createIntent(tx, { id: "memory-schema", kind: "daily_capture", payloadJson: "{}" }),
		);
		expect(database.memory.rows()[0]).toMatchObject({
			state: "queued",
			attempts: 0,
			quarantine_reason: null,
		});
		database.transaction((tx) => database.memory.beginAttempt(tx, "memory-schema"));
		database.transaction((tx) => database.memory.quarantine(tx, "memory-schema", "Error: test failure"));
		expect(database.memory.rows()[0]).toMatchObject({
			state: "quarantined",
			attempts: 1,
			quarantine_reason: "Error: test failure",
		});
		database.transaction((tx) => database.bindings.bind(tx, "loopback/loopback/loopback", "session-1"));
		expect(database.bindings.getSession("loopback/loopback/loopback")).toBe("session-1");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("durable monitor cron cursor combines slot claims and skips, then clears on removal", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-cron-cursor-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "cron-cursor",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["cron.cursor"],
			enabled: true,
		});
		const claimedAt = new Date(Date.now() + 60_000).toISOString();
		expect(registry.cronCursor(monitor.monitorId)).toBeUndefined();
		expect(
			database.transaction((tx) =>
				database.monitors.slotClaimWithEvent(tx, {
					monitorId: monitor.monitorId,
					slotAt: claimedAt,
					eventId: crypto.randomUUID(),
					eventType: "cron.cursor",
					payloadJson: "{}",
				}),
			).admitted,
		).toBe(true);
		expect(registry.cronCursor(monitor.monitorId)).toBe(claimedAt);

		const skipped = {
			count: 2,
			oldest: new Date(Date.parse(claimedAt) + 60_000).toISOString(),
			newest: new Date(Date.parse(claimedAt) + 120_000).toISOString(),
		};
		const recordedAt = new Date(Date.parse(claimedAt) + 180_000).toISOString();
		expect(registry.recordCronSkip(monitor.monitorId, skipped, recordedAt)).toEqual({
			cursor: skipped.newest,
			skippedTotal: 2,
			lastSkip: { ...skipped, recordedAt },
		});
		expect(registry.cronCursor(monitor.monitorId)).toBe(skipped.newest);
		expect(registry.remove(monitor.monitorId)).toBe(true);
		expect(registry.cronState(monitor.monitorId)).toBeUndefined();
		expect(database.core.metaGet(`monitor_cron:${monitor.monitorId}`)).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("removing a monitor settles its waiting events and slots; an in-flight event keeps its turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-monitor-remove-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "work-nudge",
			trigger: { kind: "cron", schedule: "*/30 * * * *" },
			eventTypes: ["work.nudge"],
			reportTo: { platform: "discord", kind: "channel", conversationId: "cleanup-room" },
			enabled: true,
		});
		const other = registry.add({
			name: "untouched",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["other.nudge"],
			enabled: true,
		});
		const slotAt = (minutes: number) => new Date(Date.UTC(2026, 9, 14, 9, minutes)).toISOString();
		const admit = (monitorId: string, eventType: string, at: string) => {
			const eventId = crypto.randomUUID();
			const claim = database.transaction((tx) =>
				database.monitors.slotClaimWithEvent(tx, { monitorId, slotAt: at, eventId, eventType, payloadJson: "{}" }),
			);
			expect(claim.admitted).toBe(true);
			return eventId;
		};
		const waiting = admit(monitor.monitorId, "work.nudge", slotAt(0));
		const retrying = admit(monitor.monitorId, "work.nudge", slotAt(30));
		const inFlight = admit(monitor.monitorId, "work.nudge", slotAt(60));
		const unrelated = admit(other.monitorId, "other.nudge", slotAt(0));
		database.transaction((tx) => {
			database.monitors.eventUpdate(tx, retrying, "failed");
			database.monitors.eventUpdate(tx, inFlight, "dispatched", "batch-1");
		});

		expect(database.core.metaGet(`monitor_report_to:${monitor.monitorId}`)).toBe(
			JSON.stringify({ platform: "discord", kind: "channel", conversationId: "cleanup-room" }),
		);
		expect(registry.remove(monitor.monitorId)).toBe(true);
		expect(database.core.metaGet(`monitor_report_to:${monitor.monitorId}`)).toBeUndefined();

		for (const eventId of [waiting, retrying]) {
			expect(database.monitors.eventGet(eventId)).toMatchObject({ stage: "failed_no_retry" });
			expect(database.monitors.failure(eventId)).toMatchObject({ code: "monitor_removed" });
		}
		expect(database.monitors.eventGet(inFlight)).toMatchObject({ stage: "dispatched", batch_id: "batch-1" });
		expect(database.monitors.failure(inFlight)).toBeUndefined();
		expect(database.monitors.eventGet(unrelated)).toMatchObject({ stage: "admitted" });
		expect(database.monitors.slotExists(monitor.monitorId, slotAt(0))).toBe(false);
		expect(database.monitors.slotExists(monitor.monitorId, slotAt(60))).toBe(false);
		expect(database.monitors.slotExists(other.monitorId, slotAt(0))).toBe(true);
		// Nothing is left for a dispatcher to reject in the removed monitor's name.
		expect(
			database.monitors
				.eventsOpen()
				.map((row) => row.event_id)
				.sort(),
		).toEqual([inFlight, unrelated].sort());
		expect(registry.remove(monitor.monitorId)).toBe(false);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("adds quarantine diagnostics to existing memory intents", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-migration-"));
	const path = join(directory, "gateway.db");
	try {
		const current = await Store.open(path);
		current.transaction((tx) =>
			current.memory.createIntent(tx, { id: "legacy-intent", kind: "daily_capture", payloadJson: "{}" }),
		);
		current.close();

		const legacy = new Database(path);
		legacy.exec(
			"DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; ALTER TABLE memory_intents DROP COLUMN quarantine_reason; ALTER TABLE memory_intents DROP COLUMN attempts; ALTER TABLE deliveries DROP COLUMN last_error; DELETE FROM schema_migrations WHERE version >= 23",
		);
		legacy.close();

		const migrated = await Store.open(path);
		expect(migrated.schemaVersion).toBe(39);
		expect(migrated.memory.rows()[0]).toMatchObject({
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
		const database = await Store.open(join(directory, "gateway.db"));
		const key = originKey(LOOPBACK_ORIGIN);
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "platform-before-new",
				originKey: key,
				originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
				body: "platform message",
				receivedAt: "2026-09-01T00:00:00.000Z",
			}),
		);
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "lane-report-internal",
				originKey: key,
				originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
				body: "internal report",
				receivedAt: "2026-09-01T00:00:01.000Z",
				source: "lane_report",
			}),
		);
		expect(database.transaction((tx) => database.inbox.discardBefore(tx, key, "2026-09-02T00:00:00.000Z"))).toEqual([
			"platform-before-new",
		]);
		expect(database.inbox.pendingOldest(key)).toMatchObject({
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

test("adds last_error to existing deliveries without touching their state (#171)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-migration-"));
	const path = join(directory, "gateway.db");
	try {
		const current = await Store.open(path);
		current.transaction((tx) =>
			current.outbox.create(tx, {
				deliveryId: "legacy-delivery",
				turnId: "turn",
				originKey: "discord/channel/c",
				payloadJson: "{}",
			}),
		);
		const seeded = new Database(path);
		seeded.query("UPDATE deliveries SET attempts = 2 WHERE delivery_id = ?").run("legacy-delivery");
		seeded.close();
		const latest = current.schemaVersion;
		current.close();

		const legacy = new Database(path);
		legacy.exec("ALTER TABLE deliveries DROP COLUMN last_error; DELETE FROM schema_migrations WHERE version >= 26");
		legacy.close();

		const migrated = await Store.open(path);
		expect(migrated.schemaVersion).toBe(latest);
		expect(migrated.outbox.rows()[0]).toMatchObject({
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
		await expect(Store.open(path)).rejects.toMatchObject({
			code: "newer_schema",
		} satisfies Partial<DatabaseStartupError>);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("configures SQLite journal mode and changes an existing WAL database", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-db-journal-mode-"));
	try {
		const walPath = join(directory, "wal.db");
		const wal = await Store.open(walPath);
		const schemaVersion = wal.schemaVersion;
		expect(wal.core.getJournalMode()).toBe("wal");
		wal.close();

		const deletePath = join(directory, "delete.db");
		const deleteStore = await Store.open(deletePath, { journalMode: "DELETE" });
		expect(deleteStore.core.getJournalMode()).toBe("delete");
		deleteStore.close();

		for (const mode of ["TRUNCATE", "PERSIST"] as const) {
			const path = join(directory, `${mode.toLowerCase()}.db`);
			const store = await Store.open(path, { journalMode: mode });
			expect(store.core.getJournalMode()).toBe(mode.toLowerCase());
			store.close();
		}

		const reopened = await Store.open(walPath, { journalMode: "DELETE" });
		expect(reopened.schemaVersion).toBe(schemaVersion);
		expect(reopened.core.getJournalMode()).toBe("delete");
		reopened.close();

		await expect(Store.open(join(directory, "invalid.db"), { journalMode: "OFF" as never })).rejects.toThrow(
			/sqlite\.journalMode must be one of/,
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("allows SQLite's intrinsic memory journal mode for an in-memory store", async () => {
	const store = await Store.open(":memory:");
	try {
		expect(store.core.getJournalMode()).toBe("memory");
	} finally {
		store.close();
	}
});
