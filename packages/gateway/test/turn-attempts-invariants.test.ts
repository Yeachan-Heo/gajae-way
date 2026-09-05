import { Database } from "bun:sqlite";
import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";

let directory: string;
let database: GatewayDatabase;
let raw: Database;
beforeEach(async () => {
	directory = await mkdtemp(join(tmpdir(), "gw-attempts-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	raw = new Database(join(directory, "gateway.db"));
});
afterEach(async () => {
	raw.close();
	database.close();
	await rm(directory, { recursive: true, force: true });
});
function seed(sendState = "pending_write", admission = "unknown", id = "trigger") {
	raw
		.query(
			"INSERT INTO turn_attempts (trigger_message_id, attempt, origin_key, bound_at, send_state, admission) VALUES (?, 0, 'origin', ?, ?, ?)",
		)
		.run(id, new Date().toISOString(), sendState, admission);
}
function oracle(id = "trigger") {
	return raw
		.query<{ n: number }, [string]>(
			"SELECT COUNT(*) AS n FROM turn_attempts WHERE trigger_message_id = ? AND send_state IN ('pending_write','written_unconfirmed','accepted') AND admission <> 'refused'",
		)
		.get(id)!.n;
}
const states = ["pending_write", "pre_write_failure", "written_unconfirmed", "accepted"];
const forward = new Set([
	"pending_write:pre_write_failure",
	"pending_write:written_unconfirmed",
	"written_unconfirmed:accepted",
]);
for (const from of states)
	for (const to of states) {
		if (from === to) continue;
		test(`send arrow ${from} -> ${to} ${forward.has(`${from}:${to}`) ? "allowed" : "refused"}`, () => {
			seed(from);
			const update = () => raw.query("UPDATE turn_attempts SET send_state = ?").run(to);
			if (forward.has(`${from}:${to}`)) expect(update).not.toThrow();
			else {
				expect(update).toThrow("turn_attempts_non_monotonic");
				expect(raw.query("SELECT send_state FROM turn_attempts").get()).toEqual({ send_state: from });
			}
		});
	}
for (const from of ["unknown", "refused", "accepted"])
	for (const to of ["unknown", "refused", "accepted"]) {
		if (from === to) continue;
		test(`admission arrow ${from} -> ${to}`, () => {
			seed("written_unconfirmed", from);
			const update = () => raw.query("UPDATE turn_attempts SET admission = ?").run(to);
			if (from === "unknown") expect(update).not.toThrow();
			else expect(update).toThrow("turn_attempts_non_monotonic");
		});
	}
test("13/13b boot distinguishes committed spawn gate from proven pre-spawn crash", async () => {
	seed();
	seed("pending_write", "unknown", "unspawned");
	expect(oracle()).toBe(1);
	expect(oracle("unspawned")).toBe(1);
	raw
		.query("UPDATE turn_attempts SET spawn_gate_at = ? WHERE trigger_message_id = 'trigger'")
		.run(new Date().toISOString());
	database.close();
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	expect(database.reclassifyPendingWrites()).toEqual({ preWriteFailure: 1, writtenUnconfirmed: 1 });
	expect(
		raw
			.query(
				"SELECT trigger_message_id, send_state, admission, send_decided_by FROM turn_attempts ORDER BY trigger_message_id",
			)
			.all(),
	).toEqual([
		{ trigger_message_id: "trigger", send_state: "written_unconfirmed", admission: "unknown", send_decided_by: "boot" },
		{ trigger_message_id: "unspawned", send_state: "pre_write_failure", admission: "unknown", send_decided_by: "boot" },
	]);
	expect(oracle()).toBe(1);
	expect(oracle("unspawned")).toBe(0);
	expect(database.reclassifyPendingWrites()).toEqual({ preWriteFailure: 0, writtenUnconfirmed: 0 });
});
test("13c DB gate marker is write-once and only while pending_write", () => {
	seed();
	raw.exec("UPDATE turn_attempts SET spawn_gate_at = 'first'");
	expect(() => raw.exec("UPDATE turn_attempts SET spawn_gate_at = 'second'")).toThrow("spawn_gate_immutable");
	expect(() => raw.exec("UPDATE turn_attempts SET spawn_gate_at = NULL")).toThrow("spawn_gate_immutable");
	seed("written_unconfirmed", "unknown", "observed");
	expect(() =>
		raw.exec("UPDATE turn_attempts SET spawn_gate_at = 'late' WHERE trigger_message_id = 'observed'"),
	).toThrow("spawn_gate_immutable");
});
test("pruning retains nonterminal uncertainty and deletes only expired terminal evidence", () => {
	seed("written_unconfirmed");
	seed("accepted", "accepted", "old");
	seed("accepted", "accepted", "recent");
	raw.exec("UPDATE turn_attempts SET terminal_at = '2000-01-01T00:00:00.000Z' WHERE trigger_message_id = 'old'");
	raw
		.query("UPDATE turn_attempts SET terminal_at = ? WHERE trigger_message_id = 'recent'")
		.run(new Date().toISOString());
	expect(database.pruneTurnAttempts(30 * 24 * 60 * 60 * 1000)).toBe(1);
	expect(oracle()).toBe(1);
	expect(raw.query("SELECT trigger_message_id FROM turn_attempts ORDER BY trigger_message_id").all()).toEqual([
		{ trigger_message_id: "recent" },
		{ trigger_message_id: "trigger" },
	]);
});

test("real boot reclassifies and prunes before even broker preflight", async () => {
	const { bootGateway } = await import("../src/boot");
	const { loadConfig } = await import("../src/config");
	const home = join(directory, "home");
	const config = await loadConfig({ home });
	const prepared = await GatewayDatabase.open(config.dbPath);
	prepared.close();
	const disk = new Database(config.dbPath);
	try {
		disk.exec(
			"INSERT INTO turn_attempts (trigger_message_id, attempt, origin_key, bound_at, send_state, admission, spawn_gate_at) VALUES ('boot', 0, 'origin', '2026-01-01', 'pending_write', 'unknown', '2026-01-01'); INSERT INTO turn_attempts (trigger_message_id, attempt, origin_key, bound_at, send_state, admission, terminal_at) VALUES ('expired', 0, 'origin', '2000-01-01', 'accepted', 'accepted', '2000-01-01')",
		);
		let observed = false;
		await expect(
			bootGateway({
				home,
				broker: {
					ssotAgentDir: null,
					command: async () => {
						observed = true;
						expect(disk.query("SELECT send_state, send_decided_by, admission FROM turn_attempts").all()).toEqual([
							{ send_state: "written_unconfirmed", send_decided_by: "boot", admission: "unknown" },
						]);
						return { exitCode: 0, stdout: "gjc/0.15.5\n", stderr: "" };
					},
				},
			}),
		).rejects.toThrow("requires gjc >=");
		expect(observed).toBe(true);
	} finally {
		disk.close();
	}
});
