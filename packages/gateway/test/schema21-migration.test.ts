import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";
import { openSchema20 } from "./fixtures/schema20-db";

test("schema20 copy migrates every nonterminal trigger, open lane attempt, and bound dispatched monitor exactly once", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-schema21-"));
	const fixture = join(directory, "fixture20.db");
	const copy = join(directory, "copy.db");
	try {
		const previous = await openSchema20(fixture);
		expect(previous.schemaVersion).toBe(20);
		previous.putSession("monitor/eventtype/tick", "monitor-session");
		previous.close();
		const raw = new Database(fixture);
		expect(raw.query("SELECT name FROM sqlite_master WHERE name IN ('turn_attempts','epoch_mutations')").all()).toEqual(
			[],
		);
		for (const [id, state, role] of [
			["bound", "bound", "trigger"],
			["accepted", "accepted", "trigger"],
			["done", "done", "trigger"],
			["steer", "bound", "steer"],
		]) {
			raw
				.query(
					"INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, state, received_at, turn_role, turn_epoch, turn_state, turn_op_ref, bound_session_id, dispatched_at) VALUES (?, ?, '{}', 'body', 'pending', '2026-01-01', ?, 0, ?, ?, 'session', '2026-01-01')",
				)
				.run(id!, `origin-${id}`, role!, state!, `op-${id}`);
		}
		raw
			.query(
				"INSERT INTO lane_jobs VALUES ('job', 'work/task/job', 'branch', '/tmp/work', 'running', ?, '2026-01-01', '2026-01-01')",
			)
			.run(
				JSON.stringify({
					attempts: [
						{ opRef: "closed", sessionId: "s", startedAt: "2026-01-01", endedAt: "2026-01-02" },
						{ opRef: "open", sessionId: "s", startedAt: "2026-01-03" },
					],
				}),
			);
		raw.exec(
			"INSERT INTO monitors (monitor_id,name,trigger_json,event_types_json,burst_policy,enabled,created_at) VALUES ('m','monitor','{}','[\"tick\"]','queue',1,'2026-01-01')",
		);
		for (const [id, stage, type] of [
			["dispatched", "dispatched", "tick"],
			["delivered", "delivered", "tick"],
			["unbound", "dispatched", "other"],
		])
			raw
				.query(
					"INSERT INTO monitor_events (event_id,monitor_id,event_type,payload_json,fired_at,stage,batch_id,updated_at) VALUES (?,'m',?,'{}','2026-01-01',?,'batch-id','2026-01-01')",
				)
				.run(id!, type!, stage!);
		raw.close();
		await copyFile(fixture, copy);
		const upgraded = await GatewayDatabase.open(copy);
		expect(upgraded.schemaVersion).toBe(21);
		expect(upgraded.metaGet("credential_generation")).toBe("0");
		expect(upgraded.metaGet("audit_backfill_done")).toBe("0");
		expect(upgraded.reclassifyPendingWrites()).toEqual({ preWriteFailure: 0, writtenUnconfirmed: 0 });
		upgraded.close();
		const after = new Database(copy, { readonly: true });
		const rows = after
			.query("SELECT scope, send_state, admission, legacy FROM turn_attempts ORDER BY scope, trigger_message_id")
			.all();
		expect(rows).toEqual(
			["monitor", "persona", "persona", "work"].map((scope) => ({
				scope,
				send_state: "written_unconfirmed",
				admission: "unknown",
				legacy: 1,
			})),
		);
		expect(
			after.query("SELECT binding_reconstructed, op_ref FROM turn_attempts WHERE scope = 'monitor'").get(),
		).toEqual({ binding_reconstructed: 1, op_ref: "gw-m-batchid" });
		expect(after.query("SELECT COUNT(*) AS n FROM inbound_messages").get()).toEqual({ n: 4 });
		after.close();
		const reopened = await GatewayDatabase.open(copy);
		reopened.close();
		const check = new Database(copy, { readonly: true });
		expect(check.query("SELECT COUNT(*) AS n FROM turn_attempts").get()).toEqual({ n: 4 });
		check.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
