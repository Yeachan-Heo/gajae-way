import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { brokerAgentDrift } from "../src/broker-agent/drift";
import { provisionBrokerAgent } from "../src/broker-agent/provision";

const roots: string[] = [];
afterEach(async () => {
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture() {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-agent-drift-")));
	roots.push(root);
	const home = join(root, "home");
	const source = join(root, "operator-agent");
	const agentDir = homeLayout(home).brokerAgentDir;
	await mkdir(home, { recursive: true, mode: 0o700 });
	await mkdir(source);
	const database = new Database(join(source, "agent.db"));
	database.exec(
		"CREATE TABLE auth_credentials (provider TEXT, token TEXT); INSERT INTO auth_credentials VALUES ('p', 'token');",
	);
	database.close();
	return { root, home, source, agentDir };
}

describe("broker agent drift", () => {
	test("distinguishes unprovisioned, interrupted, missing required copies, and absent optional entries", async () => {
		const f = await fixture();
		const unprovisioned = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(unprovisioned.status).toBe("unprovisioned");
		expect(unprovisioned.entries.find((entry) => entry.name === "agent.db")?.state).toBe("missing_copy");
		expect(unprovisioned.entries.find((entry) => entry.name === "models.yml")?.state).toBe("absent");

		const lease = await acquireLease(f.home, "broker");
		try {
			await provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source: f.source, mode: "force", lease });
		} finally {
			await lease.release();
		}
		const provisioned = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(provisioned.status).toBe("provisioned");
		expect(provisioned.entries.find((entry) => entry.name === "agent.db")?.state).toBe("in_sync");
		expect(provisioned.entries.find((entry) => entry.name === "models.yml")?.state).toBe("absent");

		await rm(join(f.agentDir, "agent.db"));
		const missingCopy = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(missingCopy.entries.find((entry) => entry.name === "agent.db")?.state).toBe("missing_copy");
		await writeFile(join(f.home, "broker", "provision", "journal.json"), "{}\n", { mode: 0o600 });
		const interrupted = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(interrupted.status).toBe("interrupted");
	});

	test("reports source and copy movement independently", async () => {
		const f = await fixture();
		await writeFile(join(f.source, "models.yml"), "model: a\n");
		const lease = await acquireLease(f.home, "broker");
		try {
			await provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source: f.source, mode: "force", lease });
		} finally {
			await lease.release();
		}
		await writeFile(join(f.source, "models.yml"), "model: source\n");
		let drift = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(drift.entries.find((entry) => entry.name === "models.yml")?.state).toBe("source_changed");
		await writeFile(join(f.agentDir, "models.yml"), "model: copy\n");
		drift = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(drift.entries.find((entry) => entry.name === "models.yml")?.state).toBe("diverged");
		await rm(join(f.source, "models.yml"));
		drift = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(drift.entries.find((entry) => entry.name === "models.yml")?.state).toBe("missing_source");
	});
});
