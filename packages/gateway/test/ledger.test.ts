import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

test("delivery ledger transitions pending, inflight, retry, expiry, and confirmation", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const ledger = new DeliveryLedger(database);
		ledger.createPending({ deliveryId: "one", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		expect(ledger.counts().pending).toBe(1);
		ledger.markInflight("one");
		expect(ledger.get("one")?.state).toBe("inflight");
		ledger.fail("one");
		expect(ledger.get("one")?.state).toBe("pending");
		ledger.fail("one");
		ledger.fail("one");
		expect(ledger.get("one")?.state).toBe("expired");
		ledger.createPending({ deliveryId: "two", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.confirm("two");
		expect(ledger.get("two")?.state).toBe("confirmed");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
