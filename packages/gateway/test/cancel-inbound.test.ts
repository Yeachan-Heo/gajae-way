import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseOpsCancelInboundParams } from "@gajae-gateway/protocol";
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

const RESTART_ID = "e3702be7-c592-40f9-b15c-dfda94398615";

async function startServer(withDrain: boolean) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-cancel-inbound-"));
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

	const database = await Store.open(config.dbPath);
	const enqueue = (id: string, engagement: Record<string, unknown>) => {
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: id,
				originKey: "discord:dm:owner",
				originRefJson: '{"platform":"discord","kind":"dm","conversationId":"owner"}',
				body: `ping ${id}`,
				receivedAt: new Date().toISOString(),
				engagementJson: JSON.stringify(engagement),
			}),
		);
	};
	const PING = { mentioned: true, explicitMention: true, authorId: "1423547893301776387" } as const;
	for (const id of ["a1", "a2", "a3"]) enqueue(id, { ...PING });
	enqueue("b1", { ...PING, authorId: "someone-else" });
	enqueue("c1", { mentioned: false, authorId: "other-person" });

	if (withDrain) {
		// A persisted indeterminate restart receipt makes the gateway restore the
		// drain under this id on boot, exactly like the stuck Mac runtime.
		await writeFile(
			join(home, "restart-stack.json"),
			JSON.stringify({ id: RESTART_ID, state: "indeterminate", drainPrepared: true }),
			{ mode: 0o600 },
		);
	}

	attachTestBrokerOwnership(database, port, join(home, "agent"));
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	client = await GajaewayClient.connectSocket(config.socketPath);
	return { client, database };
}

test("parse rejects empty, duplicate, malformed and unknown params", () => {
	expect(() => parseOpsCancelInboundParams(undefined)).toThrow();
	expect(() => parseOpsCancelInboundParams({ restartId: "ok", messageIds: [] })).toThrow();
	expect(() => parseOpsCancelInboundParams({ restartId: "ok", messageIds: ["x", "x"] })).toThrow();
	expect(() => parseOpsCancelInboundParams({ restartId: "ok", messageIds: ["a b"] })).toThrow();
	expect(() => parseOpsCancelInboundParams({ restartId: "bad id!", messageIds: ["x"] })).toThrow();
	expect(() => parseOpsCancelInboundParams({ restartId: "ok", messageIds: ["x"], extra: 1 })).toThrow();
});

test("cancels only the selected unbound pending rows while the drain is held", async () => {
	const { client } = await startServer(true);
	const ids = ["a1", "a2", "a3"];
	const raw = new Database(join(home, "gateway.db"), { readonly: true });
	try {
		const rows = () =>
			raw
				.query("SELECT message_id, state, terminal_delivery_id FROM inbound_messages ORDER BY message_id")
				.all() as Array<{
				message_id: string;
				state: string;
				terminal_delivery_id: string | null;
			}>;
		const result = await client.opsCancelInbound({ restartId: RESTART_ID, messageIds: ids });
		expect(result.restartId).toBe(RESTART_ID);
		expect(result.cancelledMessageIds).toEqual(ids);
		const cancelled = rows();
		for (const row of cancelled) {
			if (ids.includes(row.message_id)) {
				expect(row.state).toBe("done");
				if (row.terminal_delivery_id === null) throw new Error("cancelled row has no receipt");
				expect(JSON.parse(row.terminal_delivery_id)).toMatchObject({
					none: "operator_cancelled",
					restartId: RESTART_ID,
				});
			} else {
				expect(row).toMatchObject({ state: "pending", terminal_delivery_id: null });
			}
		}
		const replay = await client.opsCancelInbound({ restartId: RESTART_ID, messageIds: ids });
		expect(replay.cancelledMessageIds).toEqual(ids);
		expect(rows()).toEqual(cancelled);
	} finally {
		raw.close();
	}
});

test("an invalid selection rolls back every requested cancellation", async () => {
	const { client } = await startServer(true);
	await expect(client.opsCancelInbound({ restartId: RESTART_ID, messageIds: ["a1", "missing"] })).rejects.toMatchObject(
		{
			code: "verb_failed",
		},
	);
	const raw = new Database(join(home, "gateway.db"), { readonly: true });
	try {
		expect(
			raw.query("SELECT state, terminal_delivery_id FROM inbound_messages WHERE message_id = ?").get("a1"),
		).toEqual({
			state: "pending",
			terminal_delivery_id: null,
		});
	} finally {
		raw.close();
	}
});

test("aged cancellations remain replayable until the receipt retention period expires", async () => {
	const { client, database } = await startServer(true);
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "old-pending",
			originKey: "discord:dm:owner",
			originRefJson: '{"platform":"discord","kind":"dm","conversationId":"owner"}',
			body: "old pending request",
			receivedAt: "2000-01-01T00:00:00.000Z",
		}),
	);
	const params = { restartId: RESTART_ID, messageIds: ["old-pending"] };
	await client.opsCancelInbound(params);
	database.sweep();
	expect(database.inbox.messageExists("old-pending")).toBe(true);
	expect((await client.opsCancelInbound(params)).cancelledMessageIds).toEqual(params.messageIds);
	database.sweep(new Date(Date.now() + 31 * 24 * 60 * 60 * 1000));
	expect(database.inbox.messageExists("old-pending")).toBe(false);
});

test("rejects when the matching restart drain is not held", async () => {
	const { client } = await startServer(false);
	await expect(client.opsCancelInbound({ restartId: RESTART_ID, messageIds: ["a1"] })).rejects.toMatchObject({
		code: "verb_failed",
	});
});
