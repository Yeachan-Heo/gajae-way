import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveGatewayAgentDir } from "../src/boot";
import { parseConfigFile } from "../src/config";
import { GatewayDatabase } from "../src/store/db";

const directories: string[] = [];

afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function tempDir(): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), "boot-agent-dir-"));
	directories.push(directory);
	return directory;
}

describe("resolveGatewayAgentDir precedence", () => {
	test("explicit override beats all other sources", () => {
		const result = resolveGatewayAgentDir({
			explicit: "/explicit/agent",
			configAgentDir: "/config/agent",
			env: { GJC_CODING_AGENT_DIR: "/env/agent" },
			recordedAgentDir: "/recorded/agent",
			home: "/home",
		});
		expect(result).toBe("/explicit/agent");
	});

	test("config beats env, recorded, and default", () => {
		const result = resolveGatewayAgentDir({
			configAgentDir: "/config/agent",
			env: { GJC_CODING_AGENT_DIR: "/env/agent" },
			recordedAgentDir: "/recorded/agent",
			home: "/home",
		});
		expect(result).toBe("/config/agent");
	});

	test("GJC_CODING_AGENT_DIR env beats recorded and default", () => {
		const result = resolveGatewayAgentDir({
			env: { GJC_CODING_AGENT_DIR: "/env/agent" },
			recordedAgentDir: "/recorded/agent",
			home: "/home",
		});
		expect(result).toBe("/env/agent");
	});

	test("PI_CODING_AGENT_DIR env works when GJC_CODING_AGENT_DIR is not set", () => {
		const result = resolveGatewayAgentDir({
			env: { PI_CODING_AGENT_DIR: "/pi/agent" },
			recordedAgentDir: "/recorded/agent",
			home: "/home",
		});
		expect(result).toBe("/pi/agent");
	});

	test("recorded agent directory beats default", () => {
		const result = resolveGatewayAgentDir({
			env: {},
			recordedAgentDir: "/recorded/agent",
			home: "/home",
		});
		expect(result).toBe("/recorded/agent");
	});

	test("default is used when nothing else is specified", () => {
		const result = resolveGatewayAgentDir({
			env: {},
			recordedAgentDir: null,
			home: "/home",
		});
		expect(result).toBe("/home/gjc-agent");
	});

	test("config beats env (env is ignored when config is set)", () => {
		const result = resolveGatewayAgentDir({
			configAgentDir: "/config/agent",
			env: { GJC_CODING_AGENT_DIR: "/env/agent" },
			recordedAgentDir: null,
			home: "/home",
		});
		expect(result).toBe("/config/agent");
	});
});

describe("agent directory in practice", () => {
	test("fresh home gets default agent directory", async () => {
		const home = await tempDir();
		const dbPath = join(home, "gateway.db");

		const database = await GatewayDatabase.open(dbPath, { canonicalAgentDir: join(home, "gjc-agent") });
		const authority = database.inspectBrokerAuthority();
		expect(authority.authority).not.toBeNull();
		expect(authority.authority?.canonicalAgentDir).toBe(join(home, "gjc-agent"));
		database.close();
	});

	test("established home with broker_authority keeps recorded agent directory on subsequent boots", async () => {
		const home = await tempDir();
		const dbPath = join(home, "gateway.db");
		const recordedDir = join(home, "custom-agent");

		// First boot: establish with custom agent dir
		const db1 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: recordedDir });
		expect(db1.inspectBrokerAuthority().authority?.canonicalAgentDir).toBe(recordedDir);
		db1.close();

		// Second boot: peek recorded authority without full init
		const peeked = GatewayDatabase.peekRecordedAuthority(dbPath);
		expect(peeked).toBe(recordedDir);

		// Third boot: using resolved directory (from peeked authority)
		const resolved = resolveGatewayAgentDir({
			env: {},
			recordedAgentDir: peeked,
			home,
		});
		expect(resolved).toBe(recordedDir);

		// Fourth boot: full DB open succeeds with resolved directory
		const db2 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: resolved });
		expect(db2.inspectBrokerAuthority().authority?.canonicalAgentDir).toBe(recordedDir);
		db2.close();
	});

	test("peekRecordedAuthority returns null for fresh database", async () => {
		const home = await tempDir();
		const dbPath = join(home, "gateway.db");

		// Fresh DB path (doesn't exist yet)
		const peeked = GatewayDatabase.peekRecordedAuthority(dbPath);
		expect(peeked).toBeNull();
	});

	test("authority_mismatch on boot leaves schema_migrations unchanged", async () => {
		const home = await tempDir();
		const dbPath = join(home, "gateway.db");
		const dir1 = join(home, "agent1");
		const dir2 = join(home, "agent2");

		// First boot with dir1 creates a fully-initialized DB
		const db1 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: dir1 });
		const schemaAfterFirstBoot = db1.schemaVersion;
		db1.close();

		// Record the migration count and table checksum
		const readMigrations = (): { count: number; rows: string } => {
			const db = new Database(dbPath, { readonly: true });
			try {
				const count =
					db.query<{ count: number }, []>("SELECT COUNT(*) as count FROM schema_migrations").get()?.count ?? 0;
				const rows = db
					.query<{ version: number; applied_at: string }, []>(
						"SELECT version, applied_at FROM schema_migrations ORDER BY version",
					)
					.all()
					.map((r) => `${r.version}:${r.applied_at}`)
					.join(",");
				return { count, rows };
			} finally {
				db.close();
			}
		};

		// Simulate an upgrade: the newest migration is pending, as on a host booting a newer build.
		const raw = new Database(dbPath);
		raw.run("DELETE FROM schema_migrations WHERE version = (SELECT MAX(version) FROM schema_migrations)");
		raw.close();

		const migrationsBefore = readMigrations();
		expect(migrationsBefore.count).toBeGreaterThan(0);

		// Boot with dir2 should fail at authority check (before any new migrations)
		try {
			await GatewayDatabase.open(dbPath, { canonicalAgentDir: dir2 });
			throw new Error("Expected authority_mismatch");
		} catch (e) {
			if (!(e instanceof Error) || !e.message.includes("authority_mismatch")) throw e;
		}

		// Verify schema_migrations table is byte-identical
		const migrationsAfter = readMigrations();
		expect(migrationsAfter.count).toBe(migrationsBefore.count);
		expect(migrationsAfter.rows).toBe(migrationsBefore.rows);

		// Boot with original dir1 still works and applies the pending migration
		const db3 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: dir1 });
		expect(db3.schemaVersion).toBe(schemaAfterFirstBoot);
		db3.close();
	});
});

describe("config parsing", () => {
	test("config.json gjc.agentDir with absolute path is accepted", () => {
		const config = parseConfigFile({
			schemaVersion: 1,
			gjc: { agentDir: "/home/user/.gjc/agent" },
		});
		expect(config.gjc?.agentDir).toBe("/home/user/.gjc/agent");
	});

	test("config.json gjc.agentDir with relative path is rejected", () => {
		expect(() =>
			parseConfigFile({
				schemaVersion: 1,
				gjc: { agentDir: "relative/path" },
			}),
		).toThrow("gjc.agentDir must be an absolute path");
	});

	test("config.json without gjc field is valid", () => {
		const config = parseConfigFile({ schemaVersion: 1 });
		expect(config.gjc).toBeUndefined();
	});
});
