import { expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { parseConfigFile } from "../src/config";
import { GlobalGjcClient } from "../src/orchestrator/broker";

const SOURCE_ROOT = join(import.meta.dir, "../src");

async function sourceFiles(directory: string): Promise<string[]> {
	const entries = await readdir(directory, { withFileTypes: true });
	const files: string[] = [];
	for (const entry of entries) {
		const path = join(directory, entry.name);
		if (entry.isDirectory()) files.push(...(await sourceFiles(path)));
		else if (entry.isFile() && path.endsWith(".ts")) files.push(path);
	}
	return files;
}

async function sourceSnapshot(): Promise<{ readonly files: readonly string[]; readonly text: string }> {
	const files = await sourceFiles(SOURCE_ROOT);
	return { files, text: (await Promise.all(files.map((path) => readFile(path, "utf8")))).join("\n") };
}

test("hard cutover leaves no resume argv, KeyedQueue subject, or turn-abort path", async () => {
	const { files, text } = await sourceSnapshot();
	const relativeFiles = files.map((path) => relative(SOURCE_ROOT, path));

	expect(relativeFiles).not.toContain("server/keyed-queue.ts");
	expect(text).not.toContain("keyed-queue");
	expect(text).not.toContain("--resume");
	expect(text).not.toContain("turn.abort");
	expect(text).not.toContain("turn.replace");
});

test("configuration has no turn-path coexistence switch and rejects the removed timeout", async () => {
	const config = await readFile(join(SOURCE_ROOT, "config.ts"), "utf8");

	expect(config).not.toMatch(
		/\b(?:legacyTurn|persistentTurn|turnPath|turnMode|turnTransport|useLegacyTurn|usePersistentTurn|turnPathFeatureFlag)\b/i,
	);
	for (const field of ["legacyTurn", "persistentTurn", "turnPath", "turnMode", "turnTransport"]) {
		const parsed = parseConfigFile({ schemaVersion: 1, [field]: true });
		expect(parsed).not.toHaveProperty(field);
	}
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 60_000 })).toThrow(
		"turnTimeoutMs was removed with persistent SDK sessions",
	);
});

test("persistent-session lifecycle logs retain their grep-stable formats", async () => {
	const { text } = await sourceSnapshot();
	for (const format of [
		"steer_delivered originKey=",
		"stall_alert originKey=",
		"compaction_event sessionId=",
		"retired_hold originKey=",
	])
		expect(text).toContain(format);
});

test("global client observes availability and identity changes without private broker lifecycle commands", async () => {
	const brokerSource = await readFile(join(SOURCE_ROOT, "orchestrator/broker.ts"), "utf8");
	expect(brokerSource).not.toMatch(/\b(?:BrokerManager|startBroker|stopBroker|restartBroker|ensureBroker)\b/);
	expect(brokerSource).not.toContain("broker_restart generation=");
	const commands: string[][] = [];
	const logs: string[] = [];
	const generations: number[] = [];
	let healthy = true;
	let pid = 100;
	let spawned = 0;
	const client = new GlobalGjcClient({
		executable: process.execPath,
		agentDir: SOURCE_ROOT,
		spawn: () => {
			spawned++;
			throw new Error("global observation must not spawn a broker or relay");
		},
		command: async (args) => {
			commands.push([...args]);
			throw new Error(`unexpected SDK command: ${args.join(" ")}`);
		},
		discovery: async () => ({ pid, url: "ws://127.0.0.1:4567", token: "fixture-token", heartbeatAt: Date.now() }),
		healthProbe: async () => healthy,
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		log: (line) => logs.push(line),
	});
	const unsubscribe = client.onGeneration((generation) => generations.push(generation));
	const waitUntil = async (predicate: () => boolean) => {
		for (let attempt = 0; attempt < 200 && !predicate(); attempt++) await Bun.sleep(5);
		expect(predicate()).toBe(true);
	};
	try {
		await client.start();
		expect(generations).toEqual([1]);
		healthy = false;
		await waitUntil(() => logs.some((line) => line.includes("global broker unavailable; observing without repair")));
		expect(client.generation).toBe(1);
		expect(generations).toEqual([1]);
		healthy = true;
		pid = 101;
		await waitUntil(() => client.generation === 2);
		expect(generations).toEqual([1, 2]);
		expect(logs.every((line) => !line.includes("broker_restart"))).toBe(true);
	} finally {
		unsubscribe();
		await client.stop();
	}
	// Discovery-backed start, outage observation, reconnection and stop never launch or repair the daemon.
	expect(commands).toEqual([]);
	expect(spawned).toBe(0);
});
