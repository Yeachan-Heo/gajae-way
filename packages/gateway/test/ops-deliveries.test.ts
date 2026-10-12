import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GajaewayClient } from "../../sdk/src/client";

import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

let home = "";
let server: GatewayServer | undefined;
let client: GajaewayClient | undefined;
afterEach(async () => {
	await client?.close();
	client = undefined;
	await server?.stop();
	server = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function startDeliveryServer() {
	home = await mkdtemp(join(tmpdir(), "gajaeway-ops-deliveries-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const port = sessionPortFromResponder({
		bind: (key, epoch) => `session-${key}-${epoch}`,
		respond: async () => "unused",
	});
	const initial = await Store.open(config.dbPath);
	attachTestBrokerOwnership(initial, port, join(home, "agent"));

	initial.close();

	const base = Date.now() - 10_000;
	const seeded = new Database(config.dbPath);
	try {
		const insert = seeded.query(
			"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
		);
		for (const row of [
			{ id: "delivery-1", createdAt: new Date(base).toISOString(), state: "confirmed", text: "1".repeat(100) },
			{ id: "delivery-2", createdAt: new Date(base + 1_000).toISOString(), state: "pending", text: "pending" },
			{ id: "delivery-3", createdAt: new Date(base + 2_000).toISOString(), state: "confirmed", text: "recent" },
			{ id: "delivery-4", createdAt: new Date(base + 3_000).toISOString(), state: "confirmed", text: "4".repeat(100) },
		]) {
			const payloadJson = JSON.stringify({
				origin: { platform: "discord", kind: "channel", conversationId: "channel-42" },
				text: row.text,
			});
			insert.run(
				row.id,
				`turn-${row.id}`,
				"discord:channel:channel-42",
				payloadJson,
				row.state,
				row.createdAt,
				row.createdAt,
			);
		}
	} finally {
		seeded.close();
	}

	const database = await Store.open(config.dbPath);
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	client = await GajaewayClient.connectSocket(config.socketPath);
	return { client, base };
}

test("ops.deliveries returns newest first and truncates text prefixes at 80 characters", async () => {
	const { client } = await startDeliveryServer();
	const result = await client.opsDeliveries();
	expect(result.rows.map((row) => row.deliveryId)).toEqual(["delivery-4", "delivery-3", "delivery-2", "delivery-1"]);
	expect(result.rows[0]).toMatchObject({
		deliveryId: "delivery-4",
		turnId: "turn-delivery-4",
		originKey: "discord:channel:channel-42",
		conversationId: "channel-42",
		state: "confirmed",
		textPrefix: "4".repeat(80),
	});
	expect(result.rows[0]?.textPrefix).toHaveLength(80);
});

test("ops.deliveries filters by since and state before applying its limit", async () => {
	const { client, base } = await startDeliveryServer();
	const since = new Date(base + 1_500).toISOString();
	const result = await client.opsDeliveries({ since, state: "confirmed", limit: 1 });
	expect(result.rows.map((row) => row.deliveryId)).toEqual(["delivery-4"]);

	const pending = await client.opsDeliveries({ since: new Date(base).toISOString(), state: "pending", limit: 10 });
	expect(pending.rows.map((row) => row.deliveryId)).toEqual(["delivery-2"]);
});

test("ops.deliveries rejects limits above the bounded maximum", async () => {
	const { client } = await startDeliveryServer();
	await expect(client.opsDeliveries({ limit: 501 })).rejects.toMatchObject({ code: "invalid_params" });
});
