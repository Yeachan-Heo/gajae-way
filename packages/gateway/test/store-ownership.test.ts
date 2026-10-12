import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { type OwnerGroup, Store, StoreOwnershipError, TABLE_OWNER } from "../src/store";

const groups: readonly OwnerGroup[] = ["core", "bindings", "outbox", "inbox", "work", "monitors", "memory"];

async function withStore(operation: (store: Store) => void | Promise<void>): Promise<void> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-store-ownership-"));
	const store = await Store.open(join(directory, "gateway.db"));
	try {
		await operation(store);
	} finally {
		store.close();
		await rm(directory, { recursive: true, force: true });
	}
}

function expectOwnershipError(operation: () => unknown, group: OwnerGroup, table: string): void {
	let error: unknown;
	try {
		operation();
	} catch (caught) {
		error = caught;
	}
	expect(error).toBeInstanceOf(StoreOwnershipError);
	expect(error).toMatchObject({ reason: "not_owner", group, table });
}

test("TABLE_OWNER covers every fully migrated table exactly once", async () => {
	await withStore((store) => {
		const tables = store.core
			.groupDb("core")
			.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
			.all() as Array<{ name: string }>;
		expect(Object.keys(TABLE_OWNER).sort()).toEqual(tables.map((table) => table.name).sort());
	});
});

test("every owner accepts its writes and every non-owner rejects all classified write forms", async () => {
	await withStore((store) => {
		for (const [table, owner] of Object.entries(TABLE_OWNER) as Array<[keyof typeof TABLE_OWNER, OwnerGroup]>) {
			const quoted = `"${table}"`;
			const writes = [
				`INSERT INTO ${table} DEFAULT VALUES`,
				`INSERT OR IGNORE INTO main.${quoted} DEFAULT VALUES`,
				`WITH ids AS (SELECT 1) INSERT INTO ${quoted} DEFAULT VALUES`,
				`UPDATE ${table} SET rowid = rowid`,
				`UPDATE OR FAIL main.${quoted} SET rowid = rowid`,
				`WITH ids AS (SELECT 1) UPDATE ${quoted} SET rowid = rowid`,
				`DELETE FROM ${table}`,
				`DELETE FROM main.${quoted}`,
				`WITH ids AS (SELECT 1) DELETE FROM ${quoted}`,
				`REPLACE INTO ${table} DEFAULT VALUES`,
				`REPLACE INTO main.${quoted} DEFAULT VALUES`,
				`WITH ids AS (SELECT 1) REPLACE INTO ${quoted} DEFAULT VALUES`,
			];
			for (const group of groups) {
				const db = store.core.groupDb(group);
				if (group === owner) {
					store.transaction((tx) => db.prepare(`DELETE FROM main.${quoted} WHERE 0`).run(tx));
					continue;
				}
				for (const sql of writes) expectOwnershipError(() => db.prepare(sql), group, table);
			}
		}
	});
});

test("every group fails closed for unclassified SQL", async () => {
	await withStore((store) => {
		for (const group of groups) {
			const db = store.core.groupDb(group);
			for (const sql of [
				"PRAGMA integrity_check",
				"ATTACH DATABASE ':memory:' AS auxiliary",
				"CREATE TABLE unsafe (id INTEGER)",
				"INSERT INTO sessions DEFAULT VALUES; DELETE FROM sessions",
			]) {
				expect(() => db.prepare(sql)).toThrow(StoreOwnershipError);
				expect(() => db.prepare(sql)).toThrow(/unclassified SQL/);
			}
		}
	});
});

test("one transaction rolls back outbox enqueue and binding touch together", async () => {
	await withStore((store) => {
		const originKey = "loopback/loopback/store-ownership-atomicity";
		const originRefJson = JSON.stringify({
			platform: "loopback",
			kind: "loopback",
			conversationId: "store-ownership-atomicity",
		});
		store.transaction((tx) => {
			store.bindings.bind(tx, originKey, "session-atomicity");
			store.bindings.touch(tx, originKey, originRefJson, "2026-10-01T00:00:00.000Z");
		});
		const bindingDb = store.core.groupDb("bindings");
		const before = bindingDb
			.query("SELECT last_activity_at, origin_ref_json FROM sessions WHERE origin_key = ?")
			.get(originKey);
		const deliveryId = "delivery-ownership-rollback";

		expect(() =>
			store.transaction((tx) => {
				store.outbox.enqueue(tx, {
					deliveryId,
					turnId: "turn-ownership-rollback",
					originKey,
					payloadJson: "{}",
				});
				store.bindings.touch(
					tx,
					originKey,
					JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "changed" }),
				);
				throw new Error("injected transaction failure");
			}),
		).toThrow("injected transaction failure");

		expect(store.outbox.get(deliveryId)).toBeUndefined();
		expect(
			bindingDb.query("SELECT last_activity_at, origin_ref_json FROM sessions WHERE origin_key = ?").get(originKey),
		).toEqual(before);
	});
});

test("fenced monitor authoring and memory intent admission roll back together", async () => {
	await withStore((store) => {
		const eventId = "ownership-composite-monitor-event";
		store.transaction((tx) =>
			store.monitors.eventCreate(tx, {
				eventId,
				monitorId: "ownership-composite-monitor",
				eventType: "ownership.composite",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			}),
		);
		const createIntent = store.memory.createIntent.bind(store.memory);
		store.memory.createIntent = () => {
			throw new Error("injected crash after authoring fence");
		};
		try {
			expect(() =>
				store.transaction((tx) => {
					const authored = store.monitors.authorFenced(tx, eventId, "", "authored note");
					expect(authored).toBe(true);
					store.memory.createIntent(tx, {
						id: `monitor-event-intent:${eventId}`,
						kind: "monitor-event",
						payloadJson: JSON.stringify({ identity: `monitor-event:${eventId}` }),
					});
				}),
			).toThrow("injected crash after authoring fence");
		} finally {
			store.memory.createIntent = createIntent;
		}

		expect(store.monitors.eventGet(eventId)?.stage).toBe("admitted");
		expect(store.monitors.authoredOutput(eventId)).toBeUndefined();
		expect(store.memory.get(`monitor-event-intent:${eventId}`)).toBeUndefined();
	});
});

test("the legacy store/db.ts monolith is absent", async () => {
	expect(await Bun.file(fileURLToPath(new URL("../src/store/db.ts", import.meta.url))).exists()).toBe(false);
});
