import { Database } from "bun:sqlite";
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { credentialFingerprint } from "../src/broker-agent/agent-db";
import { runBrokerAgentCommand } from "../src/broker-agent/command";
import { loadConfig } from "../src/config";

const roots: string[] = [];
afterEach(async () => {
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture() {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-agent-command-")));
	roots.push(root);
	const home = join(root, "home");
	const source = join(root, "source");
	await mkdir(home, { recursive: true, mode: 0o700 });
	await mkdir(source);
	const database = new Database(join(source, "agent.db"));
	database.exec(
		"CREATE TABLE auth_credentials (provider TEXT, token TEXT); INSERT INTO auth_credentials VALUES ('p', 'token');",
	);
	database.close();
	return { root, home, source, agentDir: homeLayout(home).brokerAgentDir, config: await loadConfig({ home }) };
}

function args(f: Awaited<ReturnType<typeof fixture>>, mode = "force") {
	return ["provision", "--mode", mode, "--from", f.source] as const;
}

describe("broker-agent command", () => {
	test("provision and status verbs return usable structured output", async () => {
		const f = await fixture();
		const log = spyOn(console, "log").mockImplementation(() => {});
		try {
			expect(await runBrokerAgentCommand(args(f), f.config)).toBe(0);
			expect(await runBrokerAgentCommand(args(f, "update"), f.config)).toBe(0);
			const provisionLines = log.mock.calls
				.map((call) => String(call[0]))
				.filter((line) => line.startsWith("broker_agent_provision status="));
			expect(provisionLines.some((line) => line.startsWith("broker_agent_provision status=ok"))).toBe(true);
			expect(provisionLines).toContain("broker_agent_provision status=nothing_to_do");
			log.mockClear();
			expect(await runBrokerAgentCommand(["status", "--json"], f.config)).toBe(0);
			const status = JSON.parse(String(log.mock.calls[0]?.[0])) as {
				status: string;
				entries: Array<{ name: string; state: string }>;
			};
			expect(status.status).toBe("provisioned");
			expect(status.entries.find((entry) => entry.name === "agent.db")?.state).toBe("in_sync");
		} finally {
			log.mockRestore();
		}
	});

	test("usage and refusal errors map to exit 2 or 1, and lease wait can be disabled", async () => {
		const f = await fixture();
		const error = spyOn(console, "error").mockImplementation(() => {});
		try {
			expect(await runBrokerAgentCommand(["provision", "--mode", "invalid"], f.config)).toBe(2);
			expect(
				await runBrokerAgentCommand(["provision", "--mode", "force", "--from", join(f.root, "missing")], f.config),
			).toBe(1);
			const lease = await acquireLease(f.home, "broker");
			try {
				expect(await runBrokerAgentCommand([...args(f), "--lease-wait-ms", "0"], f.config)).toBe(1);
			} finally {
				await lease.release();
			}
		} finally {
			error.mockRestore();
		}
	});

	test("a failed generation restored to the previous copy exits 3; an unrecoverable journal exits 4", async () => {
		const f = await fixture();
		const error = spyOn(console, "error").mockImplementation(() => {});
		try {
			expect(await runBrokerAgentCommand(args(f), f.config)).toBe(0);
			const oldFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
			expect(
				await runBrokerAgentCommand(args(f), f.config, {
					fault: (point) => {
						if (point === "before_manifest") throw new Error("injected publish failure");
					},
				}),
			).toBe(3);
			expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(oldFingerprint);

			await mkdir(homeLayout(f.home).brokerProvisionDir, { recursive: true, mode: 0o700 });
			await writeFile(join(homeLayout(f.home).brokerProvisionDir, "journal.json"), "{}\n", { mode: 0o600 });
			expect(await runBrokerAgentCommand(args(f), f.config)).toBe(4);
			expect(await Bun.file(join(homeLayout(f.home).brokerProvisionDir, "journal.json")).exists()).toBe(true);
		} finally {
			error.mockRestore();
		}
	});
});
