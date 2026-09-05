import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type GatewayStatusResult, PROFILE_VERSION } from "@gajaeway/protocol";
import { GajaewayClient } from "../../sdk/src/client";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function configuration(): Promise<GatewayConfig> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-status-holds-"));
	return {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
}

async function start(config: GatewayConfig, database: GatewayDatabase): Promise<void> {
	// A temporarily unreachable broker must leave durable held turns available
	// to read-only operators, including after restart.
	const sessionPort = new ScriptedSessionPort();
	sessionPort.inspect = async () => {
		throw new Error("broker temporarily unreachable");
	};
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		startedAt: "2026-01-01T00:00:00.000Z",
		onStop: () => database.close(),
	});
}

async function status(config: GatewayConfig): Promise<GatewayStatusResult> {
	const client = await GajaewayClient.connectSocket(config.socketPath);
	try {
		return await client.status();
	} finally {
		await client.close();
	}
}

test("gateway.status exposes empty holds and zero rotations without changing existing fields", async () => {
	const config = await configuration();
	const database = await GatewayDatabase.open(config.dbPath);
	await start(config, database);
	const result = await status(config);
	expect(result).toMatchObject({
		profileVersion: PROFILE_VERSION,
		pid: process.pid,
		startedAt: "2026-01-01T00:00:00.000Z",
		schemaVersion: 21,
		sessions: { active: 0 },
		delivery: { pending: 0, oldestPendingAgeMs: null },
		holds: [],
		rotations: { last24h: 0, byReason: {}, byScope: {} },
	});
	expect(result.capabilities.length).toBeGreaterThan(0);
	expect(result.contextDiff).toEqual({
		unread: 0,
		expired: 0,
		truncated: 0,
		omittedOldestAt: null,
		omittedNewestAt: null,
		floorAt: null,
	});
});

test("gateway.status exposes seeded holds, fences and 24h rotations across database and gateway restart", async () => {
	const config = await configuration();
	const database = await GatewayDatabase.open(config.dbPath);
	await start(config, database);
	const originKey = "discord/channel/111";
	database.mutateEpoch(originKey, { scope: "persona", reason: "operator_new", cause: { kind: "operator" } });
	database.mutateEpoch("monitor/test", {
		scope: "monitor",
		reason: "monitor_context_roll",
		cause: { kind: "policy" },
	});
	database.mutateEpoch("work/old", { scope: "work", reason: "operator_new", cause: { kind: "operator" } });
	database.inboundEnqueue({
		messageId: "held-message",
		originKey,
		originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "111" }),
		body: "not exposed in status",
	});
	database.inboundBindTurn({
		messageId: "held-message",
		originKey,
		epoch: 1,
		opRef: "op-held",
		sessionId: "session-held",
	});
	database.inboundTurnAccept("op-held");
	// I1a deliberately provides no production hold/fence writers yet.
	const sql = new Database(config.dbPath);
	try {
		sql
			.query(
				"UPDATE inbound_messages SET hold_reason = ?, hold_since = ?, hold_deadline_at = ?, hold_sweeps = ? WHERE message_id = ?",
			)
			.run("execution_uncertain", "2026-09-01T00:00:00.000Z", "2026-09-02T00:00:00.000Z", 4, "held-message");
		sql
			.query("UPDATE sessions SET fence_op_ref = ?, fence_since = ?, fence_deadline_at = ? WHERE origin_key = ?")
			.run("op-held", "2026-09-01T00:00:00.000Z", "2026-09-02T00:00:00.000Z", originKey);
		sql
			.query("UPDATE epoch_mutations SET at = ? WHERE origin_key = ?")
			.run(new Date(Date.now() - 48 * 60 * 60 * 1_000).toISOString(), "work/old");
	} finally {
		sql.close();
	}
	const expectedHold = {
		opRef: "op-held",
		originKey,
		epoch: 1,
		state: "accepted" as const,
		reason: "execution_uncertain",
		since: "2026-09-01T00:00:00.000Z",
		deadline: "2026-09-02T00:00:00.000Z",
		sweeps: 4,
		fence: { opRef: "op-held", since: "2026-09-01T00:00:00.000Z", deadline: "2026-09-02T00:00:00.000Z" },
	};
	const before = await status(config);
	expect(before.holds).toEqual([expectedHold]);
	expect(before.rotations).toEqual({
		last24h: 2,
		byReason: { operator_new: 1, monitor_context_roll: 1 },
		byScope: { persona: 1, monitor: 1 },
	});
	expect(JSON.stringify(before)).not.toContain("not exposed in status");
	await server?.stop();
	server = undefined;
	const reopened = await GatewayDatabase.open(config.dbPath);
	expect(reopened.listHolds()).toEqual([expectedHold]);
	await start(config, reopened);
	const after = await status(config);
	expect(after.holds).toEqual(before.holds);
	expect(after.rotations).toEqual(before.rotations);
});
