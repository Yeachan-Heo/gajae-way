import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { copyFile, lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { inspectSnapshot, main } from "../../../scripts/gjc-authority-cutover";
import { GatewayDatabase } from "../src/store/db";
import { acquireGatewayHome } from "../src/takeover";

const roots: string[] = [];
afterEach(async () => {
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture() {
	const root = await realpath(await mkdtemp(join(tmpdir(), "authority-cli-")));
	roots.push(root);
	const home = join(root, "home");
	const agent = join(root, "agent");
	await mkdir(home);
	await mkdir(agent);
	const config = JSON.stringify({ schemaVersion: 1, dbPath: "history.db" });
	await writeFile(join(home, "config.json"), config);
	const path = join(home, "history.db");
	const database = await GatewayDatabase.open(path);
	database.putSession("discord/channel/old", "historical-session");
	database.close();
	const args = ["--home", home, "--agent-dir", agent];
	const backup = join(root, "backup.db");
	const apply = [
		"apply",
		...args,
		"--expected-authority",
		"null",
		"--quarantine",
		"--evidence",
		"offline deployment",
		"--backup",
		backup,
	];
	return { root, home, agent, path, config, args, backup, apply };
}

describe("offline authority command with kernel-exclusive gateway ownership", () => {
	for (const phase of ["backup-published", "before-commit", "after-commit"] as const) {
		test(`SIGKILL at ${phase} preserves atomic authority, history, backup and recoverable lease`, async () => {
			const f = await fixture();
			const seed = new Database(f.path);
			seed.exec(
				"INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, state, received_at) VALUES ('crash-message', 'discord/channel/old', '{}', 'preserved', 'pending', '2026-01-01')",
			);
			seed.exec(
				"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES ('crash-delivery', 'old-turn', 'discord/channel/old', '{}', 'failed_ambiguous', 1, '2026-01-01', '2026-01-01')",
			);
			const inbound = seed.query("SELECT * FROM inbound_messages").all();
			const deliveries = seed.query("SELECT * FROM deliveries").all();
			seed.close();
			const marker = join(f.root, "barrier");
			const script = `
				import { Database } from "bun:sqlite";
				import { writeFileSync } from "node:fs";
				import { GatewayDatabase } from ${JSON.stringify(new URL("../src/store/db.ts", import.meta.url).href)};
				import { main } from ${JSON.stringify(new URL("../../../scripts/gjc-authority-cutover.ts", import.meta.url).href)};
				const phase = ${JSON.stringify(phase)};
				function barrier() {
					writeFileSync(${JSON.stringify(marker)}, phase);
					Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
				}
				const originalOpen = GatewayDatabase.open;
				GatewayDatabase.open = async function(...args) {
					if (phase === "backup-published") barrier();
					return originalOpen.apply(this, args);
				};
				let cutting = false;
				const originalCutover = GatewayDatabase.prototype.cutoverBrokerAuthority;
				GatewayDatabase.prototype.cutoverBrokerAuthority = function(...args) {
					cutting = true;
					const result = originalCutover.apply(this, args);
					if (phase === "after-commit") barrier();
					cutting = false;
					return result;
				};
				const originalExec = Database.prototype.exec;
				Database.prototype.exec = function(sql, ...args) {
					if (cutting && phase === "before-commit" && sql === "COMMIT") barrier();
					return originalExec.call(this, sql, ...args);
				};
				await main(${JSON.stringify(f.apply)});
				throw new Error("fault barrier was not reached");
			`;
			const child = Bun.spawn([process.execPath, "--eval", script], { stdout: "pipe", stderr: "pipe" });
			try {
				const deadline = Date.now() + 10_000;
				while (!(await Bun.file(marker).exists()) && child.exitCode === null && Date.now() < deadline)
					await Bun.sleep(20);
				expect(await Bun.file(marker).exists()).toBe(true);
				expect(await readFile(marker, "utf8")).toBe(phase);
				await expect(acquireGatewayHome(f.home)).rejects.toThrow("gateway_home_owned");
			} finally {
				if (child.exitCode === null) child.kill("SIGKILL");
				await child.exited;
			}
			expect(child.signalCode).toBe("SIGKILL");
			const lease = await acquireGatewayHome(f.home);
			try {
				await expect(acquireGatewayHome(f.home)).rejects.toThrow("gateway_home_owned");
				const backup = new Database(f.backup, { readonly: true });
				try {
					expect(backup.query("PRAGMA integrity_check").all()).toEqual([{ integrity_check: "ok" }]);
					expect(backup.query("SELECT * FROM broker_authority").all()).toEqual([]);
					expect(backup.query("SELECT * FROM inbound_messages").all()).toEqual(inbound);
					expect(backup.query("SELECT * FROM deliveries").all()).toEqual(deliveries);
				} finally {
					backup.close();
				}
				const reopened = await GatewayDatabase.open(f.path);
				const target = { canonicalAgentDir: f.agent, identity: `gjc:${f.agent}` };
				try {
					if (phase === "after-commit") {
						expect(reopened.inspectBrokerAuthority().authority).toEqual(target);
						expect(reopened.inspectBrokerAuthority().openInbound).toBe(0);
						expect(reopened.isBrokerQuarantined("inbound", "crash-message")).toBe(true);
					} else {
						expect(reopened.inspectBrokerAuthority().authority).toBeNull();
						expect(() => reopened.assertBrokerAuthority(target, { initializeEmpty: true })).toThrow("cutover_required");
					}
				} finally {
					reopened.close();
				}
				const rows = new Database(f.path);
				try {
					expect(rows.query("PRAGMA integrity_check").all()).toEqual([{ integrity_check: "ok" }]);
					expect(rows.query("SELECT * FROM inbound_messages").all()).toEqual(inbound);
					expect(rows.query("SELECT * FROM deliveries").all()).toEqual(deliveries);
					expect(rows.query("SELECT COUNT(*) AS n FROM broker_cutovers").get()).toEqual({
						n: phase === "after-commit" ? 1 : 0,
					});
					if (phase === "after-commit") {
						expect(() => rows.exec("DELETE FROM inbound_messages")).toThrow("quarantined");
						expect(() => rows.exec("DELETE FROM broker_cutovers")).toThrow("immutable");
						const cutover = rows
							.query<{ snapshot_json: string }, []>("SELECT snapshot_json FROM broker_cutovers")
							.get()!;
						const snapshot = JSON.parse(cutover.snapshot_json);
						expect(snapshot.inbound_messages).toEqual(inbound);
						expect(snapshot.deliveries).toEqual(deliveries);
						expect(snapshot.sessions[0].gjc_session_id).toBe("historical-session");
					}
				} finally {
					rows.close();
				}
			} finally {
				await lease.release();
			}
		}, 20_000);
	}

	test("invalid migration ledger is refused read-only", async () => {
		const f = await fixture();
		const database = new Database(f.path);
		database.exec("DELETE FROM schema_migrations WHERE version = 10");
		database.close();
		const before = await readFile(f.path);
		await expect(main(["inspect", ...f.args])).rejects.toThrow("invalid_migration_ledger");
		expect(await readFile(f.path)).toEqual(before);
	});
	test("inspect is read-only and reports an unqualified populated authority without adoption", async () => {
		const f = await fixture();
		const bytes = await readFile(f.path);
		const entries = await readdir(f.home);
		const report = await main(["inspect", ...f.args]);
		expect(report.oldAuthority).toBeNull();
		expect(report.targetAuthority.identity).toBe(`gjc:${f.agent}`);
		expect(report.census.rowCounts.sessions).toBe(1);
		expect(report.applyAvailable).toBe(true);
		expect(await readFile(f.path)).toEqual(bytes);
		expect(await readdir(f.home)).toEqual(entries);
		const database = await GatewayDatabase.open(f.path);
		try {
			expect(() => database.assertBrokerAuthority(report.targetAuthority, { initializeEmpty: true })).toThrow(
				"cutover_required",
			);
		} finally {
			database.close();
		}
	});

	test("inspection creates no source sidecars when a WAL-mode database has none", async () => {
		const f = await fixture();
		// A standalone copy of the orderly closed database has its WAL-mode header
		// but no adjacent sidecars. Inspection must leave this exact layout intact.
		const standalone = join(f.home, "standalone.db");
		await copyFile(f.path, standalone);
		await writeFile(join(f.home, "config.json"), JSON.stringify({ schemaVersion: 1, dbPath: standalone }));
		const bytes = await readFile(standalone);
		const before = await readdir(f.home);
		expect((await main(["inspect", ...f.args])).census.rowCounts.sessions).toBe(1);
		expect(await readdir(f.home)).toEqual(before);
		expect(await readFile(standalone)).toEqual(bytes);
		for (const suffix of ["-wal", "-shm"]) expect(await Bun.file(`${standalone}${suffix}`).exists()).toBe(false);
	});

	test("inspection includes committed live WAL data without changing source sidecars", async () => {
		const f = await fixture();
		const writer = new Database(f.path);
		try {
			writer.exec(
				"PRAGMA wal_autocheckpoint = 0; INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('wal-only', 'wal-session', '2026-01-01')",
			);
			expect((await lstat(`${f.path}-wal`)).size).toBeGreaterThan(0);
			const before = await Promise.all(["", "-wal", "-shm"].map((suffix) => readFile(`${f.path}${suffix}`)));
			const report = await main(["inspect", ...f.args]);
			expect(report.census.rowCounts.sessions).toBe(2);
			const after = await Promise.all(["", "-wal", "-shm"].map((suffix) => readFile(`${f.path}${suffix}`)));
			expect(after).toEqual(before);
		} finally {
			writer.close();
		}
	});

	test("inspection rejects a source changed during snapshot copying and cleans its private copy", async () => {
		const f = await fixture();
		const writer = new Database(f.path);
		let snapshotDirectory: string | undefined;
		try {
			writer.exec(
				"PRAGMA wal_autocheckpoint = 0; INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('wal-first', 'first', '2026-01-01')",
			);
			await expect(
				inspectSnapshot(f.path, async (source, destination, flags) => {
					await copyFile(source, destination, flags);
					snapshotDirectory = String(destination).slice(0, String(destination).lastIndexOf("/"));
					if (String(source) === f.path)
						writer.exec(
							"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('during-copy', 'changed', '2026-01-01')",
						);
				}),
			).rejects.toThrow("inspection_source_changed");
			expect(snapshotDirectory).toBeDefined();
			await expect(lstat(snapshotDirectory!)).rejects.toMatchObject({ code: "ENOENT" });
		} finally {
			writer.close();
		}
	});

	test("apply backs up pre-cutover history, quarantines old work and preserves delivery holds", async () => {
		const f = await fixture();
		const seed = new Database(f.path);
		seed.exec(
			"INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, state, received_at) VALUES ('old-message', 'discord/channel/old', '{}', 'history', 'pending', '2026-01-01')",
		);
		seed.exec(
			"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES ('old-delivery', 'old-turn', 'discord/channel/old', '{}', 'failed_ambiguous', 1, '2026-01-01', '2026-01-01')",
		);
		const delivery = seed.query("SELECT * FROM deliveries").all();
		seed.close();
		const report = await main(f.apply);
		expect(report.mode).toBe("apply");
		if (report.mode !== "apply") throw new Error("wrong mode");
		expect(report.backup.integrity).toBe("ok");
		expect(report.semanticCensus.openInbound).toBe(1);
		const published = await lstat(f.backup);
		expect(published.isFile()).toBe(true);
		expect(published.nlink).toBe(1);
		expect(published.mode & 0o777).toBe(0o600);
		expect((await readdir(f.root)).filter((name) => name.startsWith(".gjc-cutover-"))).toEqual([]);
		const backup = new Database(f.backup, { readonly: true });
		try {
			expect(backup.query("SELECT * FROM broker_authority").all()).toEqual([]);
			expect(backup.query("SELECT gjc_session_id FROM sessions").get()).toEqual({
				gjc_session_id: "historical-session",
			});
			expect(backup.query("SELECT * FROM deliveries").all()).toEqual(delivery);
		} finally {
			backup.close();
		}
		const database = await GatewayDatabase.open(f.path);
		try {
			expect(database.inspectBrokerAuthority().authority).toEqual(report.targetAuthority);
			expect(database.inspectBrokerAuthority().openInbound).toBe(0);
			expect(database.isBrokerQuarantined("inbound", "old-message")).toBe(true);
			expect(JSON.parse(database.brokerCutoverSnapshot(report.snapshotId)!).sessions[0].gjc_session_id).toBe(
				"historical-session",
			);
		} finally {
			database.close();
		}
		const result = new Database(f.path, { readonly: true });
		try {
			expect(result.query("SELECT * FROM deliveries").all()).toEqual(delivery);
		} finally {
			result.close();
		}
		expect(await readFile(join(f.home, "config.json"), "utf8")).toBe(f.config);
		expect(await Bun.file(join(f.home, "daemon.pid")).exists()).toBe(false);
	});

	test("backup destination failure leaves the source unchanged and releases ownership", async () => {
		const f = await fixture();
		const before = await readFile(f.path);
		await writeFile(f.backup, "do not overwrite");
		await expect(main(f.apply)).rejects.toThrow("backup_exists");
		expect(await readFile(f.path)).toEqual(before);
		expect(await readFile(f.backup, "utf8")).toBe("do not overwrite");
		expect((await readdir(f.root)).filter((name) => name.startsWith(".gjc-cutover-"))).toEqual([]);
		const lease = await acquireGatewayHome(f.home);
		await lease.release();
	});

	test("boot/admin and admin/admin share the same exclusive boundary", async () => {
		const f = await fixture();
		const lease = await acquireGatewayHome(f.home);
		try {
			await expect(main(f.apply)).rejects.toThrow("gateway_home_owned");
			await expect(acquireGatewayHome(f.home)).rejects.toThrow("gateway_home_owned");
			expect(await Bun.file(f.backup).exists()).toBe(false);
		} finally {
			await lease.release();
		}
	});

	test("simultaneous admin applications produce exactly one cutover and one backup", async () => {
		const f = await fixture();
		const other = [...f.apply];
		const secondBackup = join(f.root, "second-backup.db");
		other[other.indexOf("--backup") + 1] = secondBackup;
		const outcomes = await Promise.allSettled([main(f.apply), main(other)]);
		// Counting outcomes alone cannot tell "correctly excluded" from "both lost",
		// so the losing call must name the ownership refusal it lost to.
		const rejections = outcomes
			.filter((outcome) => outcome.status === "rejected")
			.map((outcome) => String((outcome as PromiseRejectedResult).reason?.message ?? outcome.reason));
		expect({ winners: outcomes.filter((outcome) => outcome.status === "fulfilled").length, rejections }).toEqual({
			winners: 1,
			rejections: ["gateway_home_owned"],
		});
		expect(Number(await Bun.file(f.backup).exists()) + Number(await Bun.file(secondBackup).exists())).toBe(1);
		const database = new Database(f.path, { readonly: true });
		try {
			expect(database.query("SELECT COUNT(*) AS n FROM broker_cutovers").get()).toEqual({ n: 1 });
		} finally {
			database.close();
		}
	});

	test("cutover transaction rollback retains authority and history with a valid backup", async () => {
		const f = await fixture();
		const seed = new Database(f.path);
		seed.exec(
			"CREATE TRIGGER refuse_cutover BEFORE UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END",
		);
		seed.close();
		await expect(main(f.apply)).rejects.toThrow("fixture rollback");
		const result = new Database(f.path, { readonly: true });
		try {
			expect(result.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
			expect(result.query("SELECT * FROM broker_authority").all()).toEqual([]);
			expect(result.query("SELECT gjc_session_id FROM sessions").get()).toEqual({
				gjc_session_id: "historical-session",
			});
		} finally {
			result.close();
		}
		expect(await Bun.file(f.backup).exists()).toBe(true);
		const lease = await acquireGatewayHome(f.home);
		await lease.release();
	});

	test("legacy schema is backed up before migrations 22–23 and authority adoption", async () => {
		const f = await fixture();
		const legacy = new Database(f.path);
		for (const table of [
			"inbound_messages",
			"lane_jobs",
			"work_attempt_runtime",
			"monitor_events",
			"authored_outputs",
		]) {
			for (const action of ["update", "delete"]) legacy.exec(`DROP TRIGGER ${table}_quarantine_${action}`);
		}
		for (const table of [
			"broker_authority",
			"broker_owned_bindings",
			"broker_tail_cursors",
			"broker_cutovers",
			"broker_quarantine",
			"broker_retired_sessions",
		]) {
			legacy.exec(`DROP TABLE ${table}`);
		}
		legacy.exec("DELETE FROM schema_migrations WHERE version >= 22");
		legacy.close();
		const report = await main(f.apply);
		expect(report.mode).toBe("apply");
		if (report.mode !== "apply") throw new Error("wrong mode");
		expect(report.census.schema).toBe(21);
		expect(report.backup.schema).toBe(21);
		expect(report.targetSchema).toBe(23);
		const backup = new Database(f.backup, { readonly: true });
		try {
			expect(backup.query("SELECT MAX(version) AS version FROM schema_migrations").get()).toEqual({ version: 21 });
			expect(backup.query("SELECT name FROM sqlite_master WHERE name = 'broker_authority'").get()).toBeNull();
		} finally {
			backup.close();
		}
		const database = await GatewayDatabase.open(f.path);
		try {
			expect(database.schemaVersion).toBe(23);
			expect(database.inspectBrokerAuthority().authority).toEqual(report.targetAuthority);
		} finally {
			database.close();
		}
	});

	test("expected authority mismatch refuses without a backup", async () => {
		const f = await fixture();
		const args = [...f.apply];
		args[args.indexOf("--expected-authority") + 1] = JSON.stringify({ canonicalAgentDir: f.agent, identity: "other" });
		await expect(main(args)).rejects.toThrow("authority_mismatch");
		expect(await Bun.file(f.backup).exists()).toBe(false);
	});

	test("a live PID is refused regardless of command or recorded home", async () => {
		const f = await fixture();
		const pid = JSON.stringify({ pid: process.pid, home: "/another-home", startedAt: "fixture" });
		await writeFile(join(f.home, "daemon.pid"), pid);
		await expect(main(f.apply)).rejects.toThrow("gateway_live_pid");
		expect(await readFile(join(f.home, "daemon.pid"), "utf8")).toBe(pid);
	});

	test("malformed PID is indeterminate, not stale", async () => {
		const f = await fixture();
		await writeFile(join(f.home, "daemon.pid"), "not json");
		await expect(main(f.apply)).rejects.toThrow("pid_indeterminate");
	});

	test("live socket without a PID refuses and leaves the socket intact", async () => {
		const f = await fixture();
		const path = join(f.home, "gateway.sock");
		const server = createServer((socket) => socket.end());
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(path, resolve);
		});
		try {
			await expect(main(f.apply)).rejects.toThrow("gateway_live_socket");
			expect(server.listening).toBe(true);
		} finally {
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	});

	test("existing backup is never overwritten", async () => {
		const f = await fixture();
		await writeFile(f.backup, "existing backup");
		await expect(main(f.apply)).rejects.toThrow("backup_exists");
		expect(await readFile(f.backup, "utf8")).toBe("existing backup");
	});

	test("legacy SQLite inspection does not create authority tables or migrate", async () => {
		const f = await fixture();
		const legacyPath = join(f.home, "legacy.db");
		const database = new Database(legacyPath);
		database.exec(
			"CREATE TABLE sessions (session_id TEXT); INSERT INTO sessions VALUES ('old'); PRAGMA user_version = 1",
		);
		database.close();
		await writeFile(join(f.home, "config.json"), JSON.stringify({ schemaVersion: 1, dbPath: legacyPath }));
		const before = await readFile(legacyPath);
		const result = await main(["inspect", ...f.args]);
		expect(result.census.schema).toBe(0);
		expect(result.census.rowCounts.broker_authority).toBeUndefined();
		expect(await readFile(legacyPath)).toEqual(before);
	});

	test("apply requires explicit mode, quarantine, evidence and absolute backup", async () => {
		const f = await fixture();
		await expect(main(f.args)).rejects.toThrow("usage");
		for (const flag of ["--quarantine", "--evidence", "--expected-authority", "--backup"]) {
			const args = [...f.apply];
			args.splice(args.indexOf(flag), flag === "--quarantine" ? 1 : 2);
			await expect(main(args)).rejects.toThrow("explicit_apply_arguments_required");
		}
	});
});
