/**
 * The delivery ledger is the last boundary before text becomes a platform
 * message, so it sanitizes instead of trusting its caller. These are the two
 * rules that boundary has to get right at once: no raw control syntax may be
 * persisted, and a runtime failure notice must never be swallowed just because
 * its diagnostic text quotes a token.
 */
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

const ORIGIN = { platform: "discord", kind: "channel", conversationId: "c1" } as const;

async function service(): Promise<{ delivery: DeliveryService; close: () => Promise<void> }> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-delivery-sanitation-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	return {
		delivery: new DeliveryService(new DeliveryLedger(database)),
		close: async () => {
			database.close();
			await rm(directory, { recursive: true, force: true });
		},
	};
}

test("the ledger write strips control syntax even when its caller forgot to", async () => {
	const { delivery, close } = await service();
	const payload = delivery.prepare("t1", ORIGIN, "[REPLY:123] 확인 [REACT:👍] 했습니다");
	expect(payload?.text).toBe("확인 했습니다");
	// Persisted, not just returned: a reconnect replays the stored payload verbatim.
	expect(delivery.redeliveries()[0]?.text).toBe("확인 했습니다");
	await close();
});

test("a payload that is nothing but control syntax is never ledgered", async () => {
	const { delivery, close } = await service();
	for (const text of ["[SILENT]", "  [no_reply]  ", "[REACT:👍]", "[BREAK]"])
		expect(delivery.prepare("t1", ORIGIN, text)).toBeUndefined();
	expect(delivery.redeliveries()).toHaveLength(0);
	await close();
});

test("a failure notice that merely quotes a silence token still reaches the owner", async () => {
	const { delivery, close } = await service();
	// Containment belongs where parts are judged. Applying it this far down made a
	// runtime diagnostic disappear, and a failed turn must always leave something
	// visible in the room.
	const payload = delivery.prepare("t1", ORIGIN, "[turn failed] runtime rejected the reply [SILENT] token");
	expect(payload?.text).toBe("[turn failed] runtime rejected the reply [SILENT] token");
	await close();
});
