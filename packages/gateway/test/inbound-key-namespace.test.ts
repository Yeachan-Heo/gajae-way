import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";

/**
 * `inbound_messages.message_id` is one global primary key. Telegram's ingestion
 * key is namespaced (`telegram:<botUserId>:update:<update_id>`) precisely so a
 * Telegram update can never collide with a Discord snowflake or with the same
 * update id under a previous bot identity in the same home.
 */
async function open(): Promise<{ db: GatewayDatabase; dir: string }> {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-inbound-key-"));
	return { db: await GatewayDatabase.open(join(dir, "gateway.db")), dir };
}

function row(messageId: string, origin: "discord" | "telegram") {
	return {
		messageId,
		originKey: `${origin}/channel/c1`,
		originRefJson: JSON.stringify({ platform: origin, kind: "channel", conversationId: "c1" }),
		body: `body ${messageId}`,
	};
}

test("a Telegram update id equal to a Discord numeric message id does not collide", async () => {
	const { db, dir } = await open();
	try {
		expect(db.inboundEnqueue(row("123456", "discord"))).toBe(true);
		expect(db.inboundEnqueue(row("telegram:900:update:123456", "telegram"))).toBe(true);
		// The raw numeric form WOULD have collided, which is why adapters never send it.
		expect(db.inboundEnqueue(row("123456", "telegram"))).toBe(false);
	} finally {
		db.close();
		await rm(dir, { recursive: true, force: true });
	}
});

test("the same update id under a rotated bot identity is a distinct message; a retry under the same identity dedupes", async () => {
	const { db, dir } = await open();
	try {
		expect(db.inboundEnqueue(row("telegram:900:update:81", "telegram"))).toBe(true);
		expect(db.inboundEnqueue(row("telegram:900:update:81", "telegram"))).toBe(false);
		expect(db.inboundEnqueue(row("telegram:901:update:81", "telegram"))).toBe(true);
	} finally {
		db.close();
		await rm(dir, { recursive: true, force: true });
	}
});
