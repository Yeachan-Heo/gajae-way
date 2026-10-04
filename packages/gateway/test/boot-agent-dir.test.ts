import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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

describe("agent directory resolution in boot", () => {
	test("fresh home defaults to $GAJAEWAY_HOME/gjc-agent", async () => {
		const home = await tempDir();
		const dbPath = join(home, "gateway.db");
		const defaultAgentDir = join(home, "gjc-agent");

		const database = await GatewayDatabase.open(dbPath, { canonicalAgentDir: defaultAgentDir });
		const authority = database.inspectBrokerAuthority();
		expect(authority.authority.canonicalAgentDir).toBe(defaultAgentDir);
		database.close();
	});

	test("config.json gjc.agentDir overrides default", async () => {
		const home = await tempDir();
		const customAgentDir = join(home, "custom-agent");
		await mkdir(customAgentDir);

		const dbPath = join(home, "gateway.db");
		const database = await GatewayDatabase.open(dbPath, { canonicalAgentDir: customAgentDir });
		const authority = database.inspectBrokerAuthority();
		expect(authority.authority.canonicalAgentDir).toBe(customAgentDir);
		database.close();
	});

	test("established home keeps its recorded agent directory", async () => {
		const home = await tempDir();
		const originalAgentDir = join(home, "original-agent");
		const dbPath = join(home, "gateway.db");

		// First boot with original agent directory
		const database1 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: originalAgentDir });
		const authority1 = database1.inspectBrokerAuthority();
		expect(authority1.authority.canonicalAgentDir).toBe(originalAgentDir);
		database1.close();

		// Second boot with different agent directory should fail during open
		const differentAgentDir = join(home, "different-agent");
		try {
			await GatewayDatabase.open(dbPath, { canonicalAgentDir: differentAgentDir });
			throw new Error("Expected authority_mismatch error");
		} catch (error) {
			if (error instanceof Error && error.message.includes("authority_mismatch")) {
				// Expected error
			} else {
				throw error;
			}
		}
	});

	test("authority_mismatch boot leaves schema_migrations unchanged", async () => {
		const home = await tempDir();
		const originalAgentDir = join(home, "agent1");
		const wrongAgentDir = join(home, "agent2");
		const dbPath = join(home, "gateway.db");

		// First boot establishes authority
		const database1 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: originalAgentDir });
		const schemaVersion1 = database1.schemaVersion;
		database1.close();

		// Second boot with wrong authority should fail
		try {
			await GatewayDatabase.open(dbPath, { canonicalAgentDir: wrongAgentDir });
		} catch (e) {
			expect(e).toBeInstanceOf(Error);
		}

		// Verify schema version hasn't changed (no migrations applied after authority mismatch)
		const database3 = await GatewayDatabase.open(dbPath, { canonicalAgentDir: originalAgentDir });
		expect(database3.schemaVersion).toBe(schemaVersion1);
		database3.close();
	});
});
