import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import type { CliRunner } from "@gajae-gateway/subsession";
import { bootGateway } from "../src/boot";
import { loadConfig, parseConfigFile } from "../src/config";
import { Store } from "../src/store";

const directories: string[] = [];

afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function tempDir(): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), "boot-agent-dir-"));
	directories.push(directory);
	return directory;
}

function brokerFor() {
	const command: CliRunner = async () => ({ exitCode: 0, stdout: "{}", stderr: "" });
	return { executable: "/test-only/gjc", command };
}

test("resolved config always selects the home broker agent directory unless explicitly configured", async () => {
	const home = await tempDir();
	const defaults = await loadConfig({
		home,
		env: { GJC_CODING_AGENT_DIR: "/environment/agent", PI_CODING_AGENT_DIR: "/legacy/agent" },
	});
	expect(defaults.gjc.agentDir).toBe(homeLayout(home).brokerAgentDir);
	expect(isAbsolute(defaults.gjc.agentDir)).toBe(true);

	await Bun.write(
		join(home, "config.json"),
		JSON.stringify({ schemaVersion: 1, gjc: { agentDir: "/configured/agent" } }),
	);
	const configured = await loadConfig({ home, env: { GJC_CODING_AGENT_DIR: "/environment/agent" } });
	expect(configured.gjc.agentDir).toBe("/configured/agent");
});

test("boot uses config agentDir only and the database authority verifier still fails closed", async () => {
	const home = await tempDir();
	const dbPath = join(home, "gateway.db");
	const recordedDir = join(home, "recorded-agent");
	const database = await Store.open(dbPath, { canonicalAgentDir: recordedDir });
	database.close();

	await expect(bootGateway({ home, broker: brokerFor() })).rejects.toMatchObject({
		code: "authority_mismatch",
	});
});

test("boot refuses a restore journal before opening the database", async () => {
	const home = await tempDir();
	await Bun.write(join(home, "restore.journal.json"), "{}");

	await expect(bootGateway({ home, broker: brokerFor() })).rejects.toMatchObject({
		code: "restore_incomplete",
	});
	expect(await Bun.file(join(home, "gateway.db")).exists()).toBe(false);
});

test("boot reports a missing pinned gjc executable instead of searching PATH", async () => {
	const home = await tempDir();
	const broker = { ...brokerFor(), executable: undefined as unknown as string };

	await expect(bootGateway({ home, broker })).rejects.toThrow(`Pinned gjc executable cannot be resolved at ${homeLayout(home).gjcExecutable()}`);
	expect(await Bun.file(join(home, "gateway.db")).exists()).toBe(false);
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

	test("config.json without gjc field remains valid as an input file", () => {
		const config = parseConfigFile({ schemaVersion: 1 });
		expect(config.gjc).toBeUndefined();
	});
});
