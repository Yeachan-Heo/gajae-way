import { afterEach, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, RELOADABLE_FIELDS, RESTART_REQUIRED_FIELDS, reloadConfig, UNCONSUMED_FIELDS } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

/**
 * Live config reload.
 *
 * Regression anchor: changing one line of config.json — a mention allowlist
 * entry, a channel policy — used to require a full gateway restart, and
 * restarting is exactly what poisoned session keys and produced the outage the
 * rest of this branch fixes. Reload is therefore wired into the running daemon
 * through BOTH a SIGHUP handler and the gateway.reloadConfig verb, sharing one
 * implementation.
 */

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

/** A decoded server frame; only the fields these tests assert on are named. */
interface ServerFrame {
	readonly type?: string;
	readonly id?: string;
	readonly result?: {
		readonly ok?: boolean;
		readonly changed?: readonly string[];
		readonly restartRequired?: readonly string[];
		readonly ignored?: readonly string[];
		readonly diagnostics?: readonly { readonly code: string; readonly message: string }[];
		readonly engaged?: boolean;
		readonly pid?: number;
	};
}

interface Client {
	send(value: unknown): void;
	readonly frames: ServerFrame[];
	close(): void;
}

async function connect(socketPath: string): Promise<Client> {
	const frames: ServerFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as ServerFrame);
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function waitFor(frames: readonly ServerFrame[], count: number): Promise<void> {
	for (let attempt = 0; attempt < 600 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

/** The result of the nth frame, asserting it arrived at all. */
function resultOf(frames: readonly ServerFrame[], index: number): NonNullable<ServerFrame["result"]> {
	const frame = frames.at(index);
	if (!frame?.result) throw new Error(`frame ${index} carries no result: ${JSON.stringify(frame)}`);
	return frame.result;
}

async function writeConfig(home: string, value: unknown): Promise<void> {
	await Bun.write(join(home, "config.json"), JSON.stringify(value));
}

/** Starts a daemon whose config.json is `initial`, with a negotiated client. */
async function daemon(initial: Record<string, unknown>): Promise<{ client: Client; home: string; socketPath: string }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, { schemaVersion: 1, ...initial });
	const config = await loadConfig({ home: directory });
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async () => "mock reply",
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	return { client, home: directory, socketPath: config.socketPath };
}

test("the partition is honest and exhaustive: live, restart-only, or unconsumed", () => {
	// mentionAllowlist and channels are what the operator actually needs live;
	// they are read per request by the dispatch path.
	expect(RELOADABLE_FIELDS).toContain("mentionAllowlist");
	expect(RELOADABLE_FIELDS).toContain("channels");
	// Anything bound to a listener, an open database, or the constructed gjc
	// client is restart-only, and the sets never overlap.
	for (const field of RESTART_REQUIRED_FIELDS) expect(RELOADABLE_FIELDS).not.toContain(field);
	for (const field of UNCONSUMED_FIELDS) {
		expect(RELOADABLE_FIELDS).not.toContain(field);
		expect(RESTART_REQUIRED_FIELDS).not.toContain(field);
	}
	expect(RESTART_REQUIRED_FIELDS).toContain("socketPath");
	expect(RESTART_REQUIRED_FIELDS).toContain("turnTimeoutMs");
	// logVerbosity is parsed but read by nothing, so claiming it was applied
	// would be a false report; it is declared unconsumed instead.
	expect(UNCONSUMED_FIELDS).toContain("logVerbosity");
	expect(RELOADABLE_FIELDS).not.toContain("logVerbosity");
});

test("an edit to an unconsumed field is reported as ignored, never as applied", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, { schemaVersion: 1, logVerbosity: "info", mentionAllowlist: ["owner"] });
	const current = await loadConfig({ home: directory });
	await writeConfig(directory, { schemaVersion: 1, logVerbosity: "debug", mentionAllowlist: ["owner"] });
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.changed).toEqual([]);
	expect(result.restartRequired).toEqual([]);
	expect(result.ignored).toEqual(["logVerbosity"]);
});

test("a missing config file is fail-safe: the previous policy is retained", async () => {
	// Regression: reloading a deleted config used to publish defaults, dropping
	// mentionAllowlist and channels and opening a mention-gated room to anyone.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, {
		schemaVersion: 1,
		mentionAllowlist: ["owner"],
		channels: { "discord:c1": { engagement: "open" } },
	});
	const current = await loadConfig({ home: directory });
	await rm(join(directory, "config.json"));
	const vanished = await reloadConfig(current);
	expect(vanished.ok).toBe(false);
	if (vanished.ok) return;
	expect(vanished.config).toBe(current);
	expect(vanished.config.mentionAllowlist).toEqual(["owner"]);
	// The diagnostic distinguishes absent from unreadable, which is the invariant.
	expect(vanished.diagnostics[0]?.message).toContain("is missing");
	// A directory in the config's place is the same fail-closed path.
	await Bun.write(join(directory, "config.json", "placeholder"), "x");
	const directoryInstead = await reloadConfig(current);
	expect(directoryInstead.ok).toBe(false);
	if (directoryInstead.ok) return;
	expect(directoryInstead.config.mentionAllowlist).toEqual(["owner"]);
});

test("an unreadable but PRESENT config never boots on defaults", async () => {
	// Regression: distinguishing "absent" from "unreadable" is the whole point.
	// Absent means a first boot and defaults are right; a config that exists and
	// cannot be read must never silently become an open mention policy.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	const configPath = join(directory, "config.json");
	await writeConfig(directory, { schemaVersion: 1, mentionAllowlist: ["owner"] });
	await chmod(configPath, 0o000);
	await expect(loadConfig({ home: directory })).rejects.toMatchObject({ code: "config_invalid" });
	await chmod(configPath, 0o644);
	expect((await loadConfig({ home: directory })).mentionAllowlist).toEqual(["owner"]);
	// A directory in its place is refused at boot too.
	const directoryHome = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await mkdir(join(directoryHome, "config.json"));
	await expect(loadConfig({ home: directoryHome })).rejects.toMatchObject({ code: "config_invalid" });
	await rm(directoryHome, { recursive: true, force: true });
	// A genuinely absent config still means defaults.
	const freshHome = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	const defaults = await loadConfig({ home: freshHome });
	expect(defaults.mentionAllowlist).toBeUndefined();
	expect(defaults.logVerbosity).toBe("info");
	await rm(freshHome, { recursive: true, force: true });
});

test("a config.json symlinked to a deleted target is unreadable, not absent", async () => {
	// An operator layout: config.json symlinked into a dotfiles repo whose target
	// moved. The read reports ENOENT while the directory entry plainly exists, so
	// treating it as "absent" booted the daemon on defaults and opened the room.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await symlink(join(directory, "moved-away.json"), join(directory, "config.json"));
	await expect(loadConfig({ home: directory })).rejects.toMatchObject({ code: "config_invalid" });
	await rm(join(directory, "config.json"));
	// With the entry genuinely gone, a first boot still gets defaults.
	expect((await loadConfig({ home: directory })).logVerbosity).toBe("info");
});

test("over the socket, a vanished config cannot widen a mention-gated room", async () => {
	const { client, home } = await daemon({ mentionAllowlist: ["owner"], channels: {} });
	const groupSend = (id: string, authorId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "c1" },
				text: "hello",
				messageId: `m-${id}`,
				engagement: { mentioned: true, group: true, authorId },
			},
		});
	groupSend("before", "stranger");
	await waitFor(client.frames, 2);
	expect(resultOf(client.frames, 1).engaged).toBe(false);
	await rm(join(home, "config.json"));
	client.send({ v: "0.1", type: "request", id: "reload", verb: "gateway.reloadConfig" });
	await waitFor(client.frames, 3);
	expect(resultOf(client.frames, 2).ok).toBe(false);
	// Still gated: the stranger must not have been let in by an unreadable file.
	groupSend("after", "stranger");
	await waitFor(client.frames, 4);
	expect(resultOf(client.frames, 3).engaged).toBe(false);
	client.close();
});

test("a reloadable field is applied while a restart-only edit is reported and NOT applied", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, { schemaVersion: 1, mentionAllowlist: ["owner"], turnTimeoutMs: 300_000 });
	const current = await loadConfig({ home: directory });
	await writeConfig(directory, {
		schemaVersion: 1,
		mentionAllowlist: ["owner", "second"],
		turnTimeoutMs: 900_000,
	});
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.changed).toEqual(["mentionAllowlist"]);
	expect(result.restartRequired).toEqual(["turnTimeoutMs"]);
	expect(result.config.mentionAllowlist).toEqual(["owner", "second"]);
	// Refused, not half-applied: the live value stays at the old one.
	expect(result.config.turnTimeoutMs).toBe(300_000);
});

test("an invalid config leaves the old one intact and reports the error", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, { schemaVersion: 1, mentionAllowlist: ["owner"] });
	const current = await loadConfig({ home: directory });
	await Bun.write(join(directory, "config.json"), "{ invalid json");
	const broken = await reloadConfig(current);
	expect(broken.ok).toBe(false);
	if (broken.ok) return;
	expect(broken.config).toBe(current);
	expect(broken.diagnostics[0]?.code).toBe("config_invalid");
	// A structurally valid file that fails validation is equally fail-safe.
	await writeConfig(directory, { schemaVersion: 1, mentionAllowlist: ["owner"], debounceMs: 999_999 });
	const rejected = await reloadConfig(current);
	expect(rejected.ok).toBe(false);
	if (rejected.ok) return;
	expect(rejected.config.mentionAllowlist).toEqual(["owner"]);
	expect(rejected.diagnostics[0]?.message).toContain("debounceMs");
});

test("CLI overrides survive a reload instead of being dropped back to the file value", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-reload-"));
	await writeConfig(directory, { schemaVersion: 1, logVerbosity: "warn" });
	const current = await loadConfig({ home: directory, overrides: { logVerbosity: "debug" } });
	expect(current.logVerbosity).toBe("debug");
	const result = await reloadConfig(current, { logVerbosity: "debug" });
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.config.logVerbosity).toBe("debug");
	expect(result.changed).toEqual([]);
});

test("the reload verb applies a mention allowlist change without a restart", async () => {
	// Only "owner" may trigger a mention-gated group turn at boot.
	const { client, home } = await daemon({ mentionAllowlist: ["owner"], channels: {} });
	const groupSend = (id: string, authorId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "c1" },
				text: "hello",
				messageId: `m-${id}`,
				engagement: { mentioned: true, group: true, authorId },
			},
		});
	groupSend("before", "newcomer");
	await waitFor(client.frames, 2);
	expect(resultOf(client.frames, 1).engaged).toBe(false);
	await writeConfig(home, { schemaVersion: 1, mentionAllowlist: ["owner", "newcomer"], channels: {} });
	client.send({ v: "0.1", type: "request", id: "reload", verb: "gateway.reloadConfig" });
	await waitFor(client.frames, 3);
	expect(resultOf(client.frames, 2)).toMatchObject({ ok: true, changed: ["mentionAllowlist"], restartRequired: [] });
	// The very next turn honours the new allowlist: no restart in between.
	groupSend("after", "newcomer");
	await waitFor(client.frames, 4);
	expect(resultOf(client.frames, 3).engaged).toBe(true);
	client.close();
});

test("the reload verb reports a restart-only field instead of pretending to apply it", async () => {
	const { client, home } = await daemon({ mentionAllowlist: ["owner"] });
	await writeConfig(home, {
		schemaVersion: 1,
		mentionAllowlist: ["owner"],
		dbPath: join(home, "moved.db"),
		model: "opus",
	});
	client.send({ v: "0.1", type: "request", id: "reload", verb: "gateway.reloadConfig" });
	await waitFor(client.frames, 2);
	expect(resultOf(client.frames, 1).ok).toBe(true);
	expect(resultOf(client.frames, 1).changed).toEqual([]);
	expect(resultOf(client.frames, 1).restartRequired).toEqual(["dbPath", "model"]);
	client.close();
});

test("the reload verb reports failure and keeps serving on an invalid config", async () => {
	const { client, home } = await daemon({ mentionAllowlist: ["owner"] });
	await Bun.write(join(home, "config.json"), "{ not json");
	client.send({ v: "0.1", type: "request", id: "reload", verb: "gateway.reloadConfig" });
	await waitFor(client.frames, 2);
	expect(resultOf(client.frames, 1).ok).toBe(false);
	expect(resultOf(client.frames, 1).diagnostics?.[0]?.code).toBe("config_invalid");
	// The daemon is still alive and still holding the previous config.
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	await waitFor(client.frames, 3);
	expect(resultOf(client.frames, 2).pid).toBe(process.pid);
	client.close();
});

test("SIGHUP triggers the same reload as the verb", async () => {
	const { client, home } = await daemon({ mentionAllowlist: ["owner"], channels: {} });
	const groupSend = (id: string, authorId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "c1" },
				text: "hello",
				messageId: `m-${id}`,
				engagement: { mentioned: true, group: true, authorId },
			},
		});
	groupSend("before", "newcomer");
	await waitFor(client.frames, 2);
	expect(resultOf(client.frames, 1).engaged).toBe(false);
	await writeConfig(home, { schemaVersion: 1, mentionAllowlist: ["owner", "newcomer"], channels: {} });
	// What an operator actually reaches for.
	process.kill(process.pid, "SIGHUP");
	for (let attempt = 0; attempt < 200; attempt++) {
		await Bun.sleep(5);
		groupSend(`probe-${attempt}`, "newcomer");
		await waitFor(client.frames, client.frames.length + 1);
		if (resultOf(client.frames, -1).engaged === true) break;
	}
	expect(resultOf(client.frames, -1).engaged).toBe(true);
	client.close();
});

test("the SIGHUP handler does not outlive the daemon it belongs to", async () => {
	const before = process.listenerCount("SIGHUP");
	const { client } = await daemon({ mentionAllowlist: ["owner"] });
	expect(process.listenerCount("SIGHUP")).toBe(before + 1);
	client.close();
	await server?.stop();
	server = undefined;
	expect(process.listenerCount("SIGHUP")).toBe(before);
});
