import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigError, loadConfig, parseConfigFile, reloadConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { respondingSessionPort } from "./session-port.fake";

const homes: string[] = [];
let server: GatewayServer | undefined;
let client: ReloadClient | undefined;
afterEach(async () => {
	client?.close();
	client = undefined;
	await server?.stop();
	server = undefined;
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

interface ReloadFrame {
	readonly type?: string;
	readonly id?: string;
	readonly result?: { readonly ok?: boolean; readonly changed?: readonly string[] };
}

interface ReloadClient {
	send(value: unknown): void;
	readonly frames: ReloadFrame[];
	close(): void;
}

async function home(): Promise<string> {
	const path = await mkdtemp(join(tmpdir(), "gajaeway-config-"));
	homes.push(path);
	return path;
}

async function connectReloadClient(socketPath: string): Promise<ReloadClient> {
	const frames: ReloadFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as ReloadFrame);
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function waitUntil(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 600; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("file config is canonical while per-process CLI overrides win", async () => {
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({ schemaVersion: 1, socketPath: "/file.sock", dbPath: "/file.db", logVerbosity: "warn" }),
	);
	const config = await loadConfig({
		home: path,
		env: { GAJAEWAY_HOME: "/ignored" },
		overrides: { socketPath: "/cli.sock" },
	});
	expect(config.socketPath).toBe("/cli.sock");
	expect(config.dbPath).toBe("/file.db");
	expect(config.logVerbosity).toBe("warn");
});

test("rejects a credential file reachable through more than one source", async () => {
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({
			schemaVersion: 1,
			credentials: { one: { credentialFile: "/credential" }, two: { credentialFile: "/credential" } },
		}),
	);
	await expect(loadConfig({ home: path })).rejects.toMatchObject({
		code: "secret_source_conflict",
	} satisfies Partial<ConfigError>);
});

test("atomic reload keeps the last valid config on validation failure", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, logVerbosity: "info" }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), "{ invalid");
	const result = await reloadConfig(current);
	expect(result.ok).toBe(false);
	if (!result.ok) {
		expect(result.config).toBe(current);
		expect(result.diagnostics[0]?.code).toBe("config_invalid");
	}
});

test("turnTimeoutMs is rejected with persistent-session migration guidance", () => {
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 900_000 })).toThrow(
		"turnTimeoutMs was removed with persistent SDK sessions",
	);
});

test("monitorContextFailureRollThreshold parses when bounded and rejects out-of-range values", () => {
	expect(
		parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 3 }).monitorContextFailureRollThreshold,
	).toBe(3);
	// Unset means the code default (MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD), not a config value.
	expect(parseConfigFile({ schemaVersion: 1 }).monitorContextFailureRollThreshold).toBeUndefined();
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 0 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 21 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 2.5 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
});

test("model accepts an explicit selector or a preset", () => {
	expect(parseConfigFile({ schemaVersion: 1, model: "openai/gpt-5.2" }).model).toBe("openai/gpt-5.2");
	expect(parseConfigFile({ schemaVersion: 1, model: { preset: "reliable" } }).model).toEqual({ preset: "reliable" });
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "" } })).toThrow("model.preset");
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "reliable", extra: true } })).toThrow(
		"contain only preset",
	);
	expect(parseConfigFile({ schemaVersion: 1, serviceTier: "priority" }).serviceTier).toBe("priority");
	expect(() => parseConfigFile({ schemaVersion: 1, serviceTier: "fast" })).toThrow("serviceTier must be one of");
	expect(() => parseConfigFile({ schemaVersion: 1, model: ["one", "two"] })).toThrow("model must be an object");
});

test("memory seed policy accepts only booleans and rejects unknown memory settings", () => {
	for (const seedDefaultMonitors of [false, true]) {
		const memory = { seedDefaultMonitors, debloatByteThreshold: 2048 };
		expect(parseConfigFile({ schemaVersion: 1, memory }).memory).toEqual(memory);
	}
	for (const invalid of ["false", 0, 1, null, {}, []])
		expect(() => parseConfigFile({ schemaVersion: 1, memory: { seedDefaultMonitors: invalid } })).toThrow(
			"memory.seedDefaultMonitors must be a boolean",
		);
	expect(() => parseConfigFile({ schemaVersion: 1, memory: { seedDefaultMonitor: false } })).toThrow(
		"memory contains an unknown field",
	);
});

test("memory seed policy changes require restart without applying or seeding live", async () => {
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({ schemaVersion: 1, memory: { seedDefaultMonitors: false } }),
	);
	const current = await loadConfig({ home: path });
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({ schemaVersion: 1, memory: { seedDefaultMonitors: true } }),
	);
	const result = await reloadConfig(current);
	expect(result).toMatchObject({ ok: true, changed: [], restartRequired: ["memory"] });
	if (result.ok) expect(result.config.memory?.seedDefaultMonitors).toBe(false);
});

test("stallTimeoutMs has an operational default and the removed batching fields give a migration error", async () => {
	const path = await home();
	const defaults = await loadConfig({ home: path });
	expect(defaults.stallTimeoutMs).toBe(120_000);
	expect(parseConfigFile({ schemaVersion: 1, stallTimeoutMs: 120_000 }).stallTimeoutMs).toBe(120_000);
	for (const removed of ["debounceMs", "settleWindowMs", "maxInboundAgeMs"])
		expect(() => parseConfigFile({ schemaVersion: 1, [removed]: 1_000 })).toThrow(`${removed} was removed`);
	for (const removed of ["debounceMs", "settleWindowMs"])
		expect(() => parseConfigFile({ schemaVersion: 1, channels: { c1: { [removed]: 1_000 } } })).toThrow(
			`channels.c1.${removed} was removed`,
		);
});

test("stallTimeoutMs reloads as live actor policy", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, stallTimeoutMs: 120_000 }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, stallTimeoutMs: 240_000 }));
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.changed).toEqual(["stallTimeoutMs"]);
	expect(result.config.stallTimeoutMs).toBe(240_000);
});

test("work lane limits parse without materializing omitted configuration", () => {
	const work = { maxLanes: 4, idleRetireMs: 3_600_000 };
	expect(parseConfigFile({ schemaVersion: 1, work }).work).toEqual(work);
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("work");
});

test("work.allowNested is a boolean restart-required policy and defaults disabled", async () => {
	const path = await home();
	const defaults = await loadConfig({ home: path });
	expect(defaults.work?.allowNested).toBeUndefined();
	expect(parseConfigFile({ schemaVersion: 1, work: { allowNested: true } }).work).toEqual({ allowNested: true });
	expect(parseConfigFile({ schemaVersion: 1, work: { allowNested: false } }).work).toEqual({ allowNested: false });
	for (const invalid of ["true", 1, null])
		expect(() => parseConfigFile({ schemaVersion: 1, work: { allowNested: invalid } })).toThrow("work.allowNested");
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { allowNested: false } }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { allowNested: true } }));
	const result = await reloadConfig(current);
	expect(result).toMatchObject({ ok: true, changed: [], restartRequired: ["work"] });
	if (result.ok) expect(result.config.work).toEqual({ allowNested: false });
});

for (const [work, field] of [
	[{ maxLanes: 0 }, "work.maxLanes"],
	[{ maxLanes: 257 }, "work.maxLanes"],
	[{ maxLanes: 1.5 }, "work.maxLanes"],
	[{ idleRetireMs: 1_000 }, "work.idleRetireMs"],
	[{ foo: 1 }, "work"],
] as const) {
	test(`rejects invalid work configuration ${JSON.stringify(work)}`, () => {
		const parse = () => parseConfigFile({ schemaVersion: 1, work });
		expect(parse).toThrow(ConfigError);
		expect(parse).toThrow(field);
		try {
			parse();
		} catch (error) {
			expect(error).toMatchObject({ code: "config_invalid" });
		}
	});
}

test("work lane limit changes require restart and retain the live limits", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { maxLanes: 4 } }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { maxLanes: 8 } }));
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.restartRequired).toContain("work");
	expect(result.changed).not.toContain("work");
	expect(result.config.work).toEqual({ maxLanes: 4 });
});

test("bot audience budgets parse, default to unset, and reject non-positive counts", () => {
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("botAudience");
	expect(parseConfigFile({ schemaVersion: 1, botAudience: { maxConsecutiveTurns: 3 } }).botAudience).toEqual({
		maxConsecutiveTurns: 3,
	});
	expect(
		parseConfigFile({
			schemaVersion: 1,
			channels: { c1: { engagement: "open", audience: "all", botAudienceMaxTurnsPerWindow: 5 } },
		}).channels?.c1,
	).toEqual({ engagement: "open", audience: "all", botAudienceMaxTurnsPerWindow: 5 });
	for (const invalid of [0, -1, 2.5, "3"])
		expect(() => parseConfigFile({ schemaVersion: 1, botAudience: { maxTurnsPerWindow: invalid } })).toThrow(
			"botAudience.maxTurnsPerWindow must be an integer of at least 1",
		);
	expect(() => parseConfigFile({ schemaVersion: 1, botAudience: { maxTurns: 4 } })).toThrow(
		"botAudience contains an unknown field",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, channels: { c1: { botAudienceCap: 4 } } })).toThrow(
		"channels.c1 contains an unknown field",
	);
});

test("interim speech config parses non-negative integers and requires restart", () => {
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("interimSpeech");
	expect(parseConfigFile({ schemaVersion: 1, interimSpeech: { maxPerTurn: 0 } }).interimSpeech).toEqual({
		maxPerTurn: 0,
	});
	expect(
		parseConfigFile({ schemaVersion: 1, interimSpeech: { maxPerTurn: 2, minGapMs: 45000 } }).interimSpeech,
	).toEqual({
		maxPerTurn: 2,
		minGapMs: 45000,
	});
	// Reject negative integers and non-integers
	for (const invalid of [-1, 2.5, "0", null])
		expect(() => parseConfigFile({ schemaVersion: 1, interimSpeech: { maxPerTurn: invalid } })).toThrow(
			"interimSpeech.maxPerTurn must be a non-negative integer",
		);
	for (const invalid of [-1, 2.5, "45000"])
		expect(() => parseConfigFile({ schemaVersion: 1, interimSpeech: { minGapMs: invalid } })).toThrow(
			"interimSpeech.minGapMs must be a non-negative integer",
		);
	// Reject unknown fields
	expect(() => parseConfigFile({ schemaVersion: 1, interimSpeech: { maxPerTurn: 2, unknownField: true } })).toThrow(
		"interimSpeech contains an unknown field",
	);
});

test("sessions host limits parse, omit absent fields, and reload live", async () => {
	const sessions = { maxLiveHosts: 4, idleRetireMs: 3_600_000, idleResetMs: 86_400_000 };
	expect(parseConfigFile({ schemaVersion: 1, sessions }).sessions).toEqual(sessions);
	expect(parseConfigFile({ schemaVersion: 1, sessions: { maxLiveHosts: 1 } }).sessions).toEqual({ maxLiveHosts: 1 });
	expect(parseConfigFile({ schemaVersion: 1, sessions: { idleResetMs: 60_000 } }).sessions).toEqual({
		idleResetMs: 60_000,
	});
	expect(parseConfigFile({ schemaVersion: 1, sessions: { idleResetMs: 2_592_000_000 } }).sessions).toEqual({
		idleResetMs: 2_592_000_000,
	});
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("sessions");
	// #686/#687 recovery bounds, at both ends of their range.
	expect(
		parseConfigFile({ schemaVersion: 1, sessions: { busyStallMs: 60_000, unresolvedHoldMs: 86_400_000 } }).sessions,
	).toEqual({ busyStallMs: 60_000, unresolvedHoldMs: 86_400_000 });
	expect(
		parseConfigFile({ schemaVersion: 1, sessions: { busyStallMs: 86_400_000, unresolvedHoldMs: 60_000 } }).sessions,
	).toEqual({ busyStallMs: 86_400_000, unresolvedHoldMs: 60_000 });
	// Dead retired hold abandonment window, at both ends of its range.
	expect(parseConfigFile({ schemaVersion: 1, sessions: { retiredHoldAbandonMs: 3_600_000 } }).sessions).toEqual({
		retiredHoldAbandonMs: 3_600_000,
	});
	expect(parseConfigFile({ schemaVersion: 1, sessions: { retiredHoldAbandonMs: 2_592_000_000 } }).sessions).toEqual({
		retiredHoldAbandonMs: 2_592_000_000,
	});
	for (const retiredHoldAbandonMs of [3_599_999, 2_592_000_001, 1.5])
		expect(() => parseConfigFile({ schemaVersion: 1, sessions: { retiredHoldAbandonMs } })).toThrow(
			"sessions.retiredHoldAbandonMs must be an integer",
		);
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({ schemaVersion: 1, sessions: { maxLiveHosts: 8, idleResetMs: 120_000 } }),
	);
	const current = await loadConfig({ home: path });
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({
			schemaVersion: 1,
			sessions: { maxLiveHosts: 2, idleResetMs: 240_000, busyStallMs: 900_000, unresolvedHoldMs: 300_000 },
		}),
	);
	const result = await reloadConfig(current);
	expect(result).toMatchObject({ ok: true, changed: ["sessions"], restartRequired: [] });
	if (result.ok)
		expect(result.config.sessions).toEqual({
			maxLiveHosts: 2,
			idleResetMs: 240_000,
			busyStallMs: 900_000,
			unresolvedHoldMs: 300_000,
		});
});

for (const [sessions, field] of [
	[{ maxLiveHosts: 0 }, "sessions.maxLiveHosts"],
	[{ maxLiveHosts: 257 }, "sessions.maxLiveHosts"],
	[{ maxLiveHosts: 2.5 }, "sessions.maxLiveHosts"],
	[{ maxLiveHosts: "8" }, "sessions.maxLiveHosts"],
	[{ idleRetireMs: 1_000 }, "sessions.idleRetireMs"],
	[{ idleRetireMs: 8 * 24 * 60 * 60_000 }, "sessions.idleRetireMs"],
	[{ idleResetMs: 59_999 }, "sessions.idleResetMs"],
	[{ idleResetMs: 2_592_000_001 }, "sessions.idleResetMs"],
	[{ idleResetMs: 60_000.5 }, "sessions.idleResetMs"],
	[{ idleResetMs: "60000" }, "sessions.idleResetMs"],
	[{ busyStallMs: 59_999 }, "sessions.busyStallMs"],
	[{ busyStallMs: 86_400_001 }, "sessions.busyStallMs"],
	[{ busyStallMs: "300000" }, "sessions.busyStallMs"],
	[{ unresolvedHoldMs: 59_999 }, "sessions.unresolvedHoldMs"],
	[{ unresolvedHoldMs: 600_000.5 }, "sessions.unresolvedHoldMs"],
	[{ unresolvedHoldMs: 86_400_001 }, "sessions.unresolvedHoldMs"],
	[{ unresolvedHoldMs: null }, "sessions.unresolvedHoldMs"],
	[{ maxHosts: 1 }, "sessions may only contain maxLiveHosts, idleRetireMs, idleResetMs, busyStallMs, unresolvedHoldMs"],
] as const) {
	test(`rejects invalid sessions configuration ${JSON.stringify(sessions)}`, () => {
		const parse = () => parseConfigFile({ schemaVersion: 1, sessions });
		expect(parse).toThrow(ConfigError);
		expect(parse).toThrow(field);
	});
}

test("sqlite journalMode accepts only durable modes and requires restart when changed", async () => {
	for (const [mode, expected] of [
		["DELETE", "DELETE"],
		["delete", "DELETE"],
		["TRUNCATE", "TRUNCATE"],
		["PERSIST", "PERSIST"],
		["wal", "WAL"],
	] as const) {
		expect(parseConfigFile({ schemaVersion: 1, sqlite: { journalMode: mode } }).sqlite?.journalMode).toBe(expected);
	}
	for (const mode of ["INVALID", "wal2", "MEMORY", "memory", "OFF", "off"])
		expect(() => parseConfigFile({ schemaVersion: 1, sqlite: { journalMode: mode } })).toThrow(
			/sqlite\.journalMode must be one of/,
		);
	expect(() => parseConfigFile({ schemaVersion: 1, sqlite: { journalMode: "" } })).toThrow(
		"sqlite.journalMode must be a non-empty string",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, sqlite: { journalMode: "WAL", extra: true } })).toThrow(
		"sqlite may only contain journalMode",
	);

	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, sqlite: { journalMode: "WAL" } }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, sqlite: { journalMode: "DELETE" } }));
	const result = await reloadConfig(current);
	expect(result).toMatchObject({ ok: true, changed: [], restartRequired: ["sqlite"] });
	if (result.ok) expect(result.config.sqlite).toEqual({ journalMode: "WAL" });
});

test("concurrent reloads serialize stale reads and keep the latest default model", async () => {
	const path = await home();
	const configPath = join(path, "config.json");
	const initialConfig = {
		schemaVersion: 1,
		mentionAllowlist: ["owner"],
		model: "provider/boot",
		stallTimeoutMs: 60_000,
	};
	await Bun.write(configPath, JSON.stringify(initialConfig));
	const config = await loadConfig({ home: path });
	const database = await Store.open(config.dbPath);
	const port = respondingSessionPort(async () => "reply");
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	client = await connectReloadClient(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitUntil(() => client!.frames.length >= 1, "server did not complete the handshake");

	const version1 = {
		schemaVersion: 1,
		mentionAllowlist: ["owner"],
		model: "provider/model-v1",
		stallTimeoutMs: 90_000,
	};
	const version2 = {
		schemaVersion: 1,
		mentionAllowlist: ["owner"],
		model: "provider/model-v2",
		stallTimeoutMs: 90_000,
	};
	await Bun.write(configPath, JSON.stringify(version1));

	const originalFile = Bun.file;
	let readCount = 0;
	let firstReadStarted = false;
	let signalSecondRead!: () => void;
	let releaseFirstRead!: () => void;
	const secondRead = new Promise<void>((resolve) => {
		signalSecondRead = resolve;
	});
	const holdFirstRead = new Promise<void>((resolve) => {
		releaseFirstRead = resolve;
	});
	Bun.file = ((...args: Parameters<typeof Bun.file>) => {
		const file = originalFile(...args);
		if (String(args[0]) !== configPath) return file;
		return new Proxy(file, {
			get(target, property, receiver) {
				if (property !== "text") return Reflect.get(target, property, receiver);
				return async () => {
					const contents = await target.text();
					readCount++;
					if (readCount === 1) {
						firstReadStarted = true;
						await holdFirstRead;
					} else if (readCount === 2) signalSecondRead();
					return contents;
				};
			},
		});
	}) as typeof Bun.file;

	try {
		client.send({ v: "0.1", type: "request", id: "reload-a", verb: "gateway.reloadConfig" });
		await waitUntil(() => firstReadStarted, "reload A did not read config v1");
		await Bun.write(configPath, JSON.stringify(version2));
		client.send({ v: "0.1", type: "request", id: "reload-b", verb: "gateway.reloadConfig" });

		// An unlocked reload reaches the file while A is held, publishes v2, and
		// replies before A is released. A reload protected by the mutex waits.
		const bReadBeforeAReleased = await Promise.race([secondRead.then(() => true), Bun.sleep(100).then(() => false)]);
		if (bReadBeforeAReleased)
			await waitUntil(() => client!.frames.some((frame) => frame.id === "reload-b"), "reload B did not finish");
	} finally {
		releaseFirstRead();
		Bun.file = originalFile;
	}

	await waitUntil(
		() =>
			client!.frames.some((frame) => frame.id === "reload-a") &&
			client!.frames.some((frame) => frame.id === "reload-b"),
		"both reloads did not finish",
	);
	const resultA = client.frames.find((frame) => frame.id === "reload-a")?.result;
	const resultB = client.frames.find((frame) => frame.id === "reload-b")?.result;
	expect(resultA).toMatchObject({ ok: true, changed: ["stallTimeoutMs", "model"] });
	expect(resultB).toMatchObject({ ok: true, changed: ["model"] });

	client.send({
		v: "0.1",
		type: "request",
		id: "use-default-model",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "reload-race" },
			text: "use the configured default",
			messageId: "reload-race-message",
			engagement: { mentioned: true, group: true, authorId: "owner" },
		},
	});
	await waitUntil(() => port.sends.length === 1, "the persona did not send its first turn");
	await waitUntil(
		() => port.models.some((model) => model.selection === "provider/model-v2"),
		"the inherited session did not receive the latest configured default model",
	);
	expect(port.models.at(-1)?.selection).toBe("provider/model-v2");
});
