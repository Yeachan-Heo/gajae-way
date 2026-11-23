import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LATEST_SCHEMA_VERSION, Store } from "../src/store";

const ORIGIN_KEY = "discord:dm:images";

let home = "";
let database: Store | undefined;
afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function open(): Promise<Store> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-inbox-images-"));
	database = await Store.open(join(home, "gateway.db"));
	return database;
}

const message = (id: string, body: string, originKey = ORIGIN_KEY) => ({
	messageId: id,
	originKey,
	originRefJson: '{"platform":"discord"}',
	body,
});

const preparedImagesJson = JSON.stringify([{ data: "prepared-image", mimeType: "image/jpeg" }]);

test("inbound images survive requeue and reopening the store", async () => {
	const db = await open();
	db.transaction((tx) => db.inbox.enqueue(tx, { ...message("image-1", "caption"), imagesJson: preparedImagesJson }));
	const opRef = "gw-p-image-requeue";
	db.transaction((tx) =>
		db.inbox.bindTurn(tx, {
			messageId: "image-1",
			originKey: ORIGIN_KEY,
			epoch: 0,
			opRef,
			sessionId: "session-1",
		}),
	);
	expect(db.transaction((tx) => db.inbox.turnRequeue(tx, opRef))).toBe(1);
	expect(db.inbox.pendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "image-1",
		state: "pending",
		images_json: preparedImagesJson,
	});

	db.close();
	database = await Store.open(join(home, "gateway.db"));
	expect(database.inbox.pendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "image-1",
		state: "pending",
		images_json: preparedImagesJson,
	});
});

test("omitted and empty image arrays remain ordinary closeable text rows", async () => {
	const db = await open();
	db.transaction((tx) => db.inbox.enqueue(tx, { ...message("empty-images", "text"), imagesJson: "[]" }));
	db.transaction((tx) => db.inbox.enqueue(tx, message("omitted-images", "more text")));
	expect(db.inbox.pendingOldest(ORIGIN_KEY)).toMatchObject({ message_id: "empty-images", images_json: null });
	expect(
		db
			.transaction((tx) =>
				db.inbox.closeClaimedPending(tx, {
					originKey: ORIGIN_KEY,
					epoch: 1,
					opRef: "gw-p-text-context",
					messageIds: ["empty-images", "omitted-images"],
				}),
			)
			.map((row) => [row.message_id, row.images_json]),
	).toEqual([
		["empty-images", null],
		["omitted-images", null],
	]);
	expect(db.inbox.pendingCount(ORIGIN_KEY)).toBe(0);
});

test("closeClaimedPending leaves image rows pending and remains origin-scoped", async () => {
	const db = await open();
	const otherOrigin = "discord:dm:other";
	db.transaction((tx) =>
		db.inbox.enqueue(tx, { ...message("same-origin-image", "caption"), imagesJson: preparedImagesJson }),
	);
	db.transaction((tx) => db.inbox.enqueue(tx, message("same-origin-text", "text")));
	db.transaction((tx) =>
		db.inbox.enqueue(tx, {
			...message("other-origin-image", "other caption", otherOrigin),
			imagesJson: preparedImagesJson,
		}),
	);

	const closed = db.transaction((tx) =>
		db.inbox.closeClaimedPending(tx, {
			originKey: ORIGIN_KEY,
			epoch: 1,
			opRef: "gw-p-context-claim",
			messageIds: ["same-origin-image", "same-origin-text", "other-origin-image"],
		}),
	);
	expect(closed.map((row) => row.message_id)).toEqual(["same-origin-text"]);
	expect(db.inbox.pendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "same-origin-image",
		images_json: preparedImagesJson,
	});
	expect(db.inbox.pendingOldest(otherOrigin)).toMatchObject({
		message_id: "other-origin-image",
		images_json: preparedImagesJson,
	});
});

test("schema 41 upgrade adds nullable images_json without changing old rows", async () => {
	const db = await open();
	db.transaction((tx) => db.inbox.enqueue(tx, message("pre-images-schema", "old row")));
	db.close();
	database = undefined;

	const legacy = new Database(join(home, "gateway.db"));
	legacy.exec("ALTER TABLE inbound_messages DROP COLUMN images_json; DELETE FROM schema_migrations WHERE version >= 42");
	legacy.close();

	database = await Store.open(join(home, "gateway.db"));
	expect(database.schemaVersion).toBe(LATEST_SCHEMA_VERSION);
	expect(database.inbox.pendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "pre-images-schema",
		images_json: null,
	});
});

test("images are JSON arrays and lane reports cannot carry them", async () => {
	const db = await open();
	for (const imagesJson of ['{"data":"not-an-array"}', "not-json"])
		expect(() =>
			db.transaction((tx) => db.inbox.enqueue(tx, { ...message(`invalid-${imagesJson.length}`, "text"), imagesJson })),
		).toThrow();
	expect(() =>
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				...message("lane-image", "report"),
				source: "lane_report",
				imagesJson: preparedImagesJson,
			}),
		),
	).toThrow("lane report inbound rows cannot carry images");
});
