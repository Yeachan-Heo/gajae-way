import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { originKey } from "@gajae-gateway/protocol";
import { ConfigError, type GatewayConfig, parseConfigFile } from "../src/config";
import { DeliveryService } from "../src/delivery/delivery";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import {
	cancelledText,
	onlineText,
	personaNameFrom,
	RestartNotices,
	restartingText,
} from "../src/server/restart-notice";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort, sessionPortFromResponder } from "./session-port.fake";

interface WireFrame {
	readonly type?: string;
	readonly event?: string;
	readonly id?: string;
	readonly payload?: { readonly text?: string };
}

const DM: OriginRef = { platform: "discord", kind: "dm", conversationId: "dm-1", peerId: "u1" };
const THREAD: OriginRef = { platform: "slack", kind: "thread", conversationId: "171.1", parentId: "C1" };
const STALE: OriginRef = { platform: "telegram", kind: "dm", conversationId: "old", peerId: "u3" };
const NOW = Date.parse("2026-10-24T12:00:00.000Z");

let home = "";
let database: Store | undefined;
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function openStore(): Promise<Store> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-restart-notice-"));
	database = await Store.open(join(home, "gateway.db"));
	return database;
}

function inbound(store: Store, origin: OriginRef, at: number, engagement: object = { authorId: "u" }): void {
	store.transaction((tx) =>
		store.inbox.enqueue(tx, {
			messageId: crypto.randomUUID(),
			originKey: originKey(origin),
			originRefJson: JSON.stringify(origin),
			body: "hi",
			engagementJson: JSON.stringify(engagement),
			receivedAt: new Date(at).toISOString(),
		}),
	);
}

function notices(
	store: Store,
	options: { enabled?: boolean; bootId?: string; inheritedRestartId?: string } = {},
): { readonly sent: ChatMessagePayload[]; readonly notices: RestartNotices } {
	const sent: ChatMessagePayload[] = [];
	return {
		sent,
		notices: new RestartNotices({
			database: store,
			delivery: new DeliveryService(store),
			enabled: () => options.enabled ?? true,
			personaName: () => "Kami",
			broadcast: (payloads) => sent.push(...payloads),
			now: () => NOW,
			log: () => {},
			...(options.bootId ? { bootId: options.bootId } : {}),
			...(options.inheritedRestartId ? { inheritedRestartId: options.inheritedRestartId } : {}),
		}),
	};
}

test("a restart is announced once to recently active human conversations", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	inbound(store, THREAD, NOW - 5_000);
	inbound(store, STALE, NOW - 3 * 60 * 60_000);
	inbound(store, { platform: "discord", kind: "channel", conversationId: "bots" }, NOW - 1_000, {
		authorId: "b",
		authorIsBot: true,
	});
	const { sent, notices: restart } = notices(store, { bootId: "boot-a" });

	restart.restarting("stack-1");
	restart.restarting("stack-1");

	expect(sent.map((payload) => originKey(payload.origin)).sort()).toEqual([originKey(DM), originKey(THREAD)].sort());
	expect(sent.every((payload) => payload.text === restartingText("Kami") && payload.final === false)).toBe(true);
	// Each notice is an ordinary outbox obligation the adapters settle like any reply.
	expect(store.outbox.listUndelivered(Number.POSITIVE_INFINITY, NOW, true)).toHaveLength(2);
});

test("a later process generation closes the announcement with exactly one online notice", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	notices(store, { bootId: "boot-a" }).notices.restarting("stack-1");

	// The replacement gateway inherited the held drain from the receipt; boot leaves it open.
	const next = notices(store, { bootId: "boot-b", inheritedRestartId: "stack-1" });
	next.notices.resumeAfterBoot();
	next.notices.restarting("stack-1");
	expect(next.sent).toEqual([]);

	// The supervisor's reattach + cancel releases the drain on the new process.
	next.notices.released("stack-1");
	next.notices.released("stack-1");
	expect(next.sent.map((payload) => [originKey(payload.origin), payload.text])).toEqual([
		[originKey(DM), onlineText("Kami")],
	]);

	const afterAnotherBoot = notices(store, { bootId: "boot-c" });
	afterAnotherBoot.notices.resumeAfterBoot();
	expect(afterAnotherBoot.sent).toEqual([]);
});

test("a boot after an owner restart sends the online notice itself", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	notices(store, { bootId: "boot-a" }).notices.restarting("owner-1");

	const next = notices(store, { bootId: "boot-b" });
	next.notices.resumeAfterBoot();
	expect(next.sent.map((payload) => payload.text)).toEqual([onlineText("Kami")]);
});

test("a restart released by the announcing process sends cancelled and never online", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	const { sent, notices: restart } = notices(store, { bootId: "boot-a" });
	restart.restarting("stack-1");
	restart.released("stack-1");

	expect(sent.map((payload) => payload.text)).toEqual([restartingText("Kami"), cancelledText("Kami")]);
	const next = notices(store, { bootId: "boot-b" });
	next.notices.resumeAfterBoot();
	expect(next.sent).toEqual([]);
});

test("a supervisor hold transferred to an owner restart keeps one open announcement", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	const { sent, notices: restart } = notices(store, { bootId: "boot-a" });
	restart.restarting("stack-1");
	restart.transferred("stack-1", "owner-1");
	restart.restarting("owner-1");
	expect(sent).toHaveLength(1);

	const next = notices(store, { bootId: "boot-b" });
	next.notices.resumeAfterBoot();
	expect(next.sent.map((payload) => payload.text)).toEqual([onlineText("Kami")]);
});

test("disabled restart notices send nothing and leave no record behind", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	const { sent, notices: restart } = notices(store, { enabled: false, bootId: "boot-a" });
	restart.restarting("stack-1");
	restart.released("stack-1");
	const next = notices(store, { enabled: false, bootId: "boot-b" });
	next.notices.resumeAfterBoot();

	expect(sent).toEqual([]);
	expect(next.sent).toEqual([]);
	expect(store.core.metaKeysInRange("restart_notice:", "restart_notice:\uffff")).toEqual([]);
	expect(store.outbox.listUndelivered(Number.POSITIVE_INFINITY, NOW, true)).toEqual([]);
});

test("a muted thread is not interrupted by a restart notice", async () => {
	const store = await openStore();
	inbound(store, THREAD, NOW - 5_000);
	store.transaction((tx) => store.inbox.muteThread(tx, originKey(THREAD)));
	const { sent, notices: restart } = notices(store, { bootId: "boot-a" });
	restart.restarting("stack-1");
	expect(sent).toEqual([]);
});

test("the drain observer fires once per restart id, not per repeated drain call", async () => {
	const store = await openStore();
	const started: string[] = [];
	const cancelled: string[] = [];
	const manager = new PersonaSessionManager({
		database: store,
		port: new ScriptedSessionPort(),
		instanceId: store.instanceId,
		repo: home,
		log: () => {},
		onRestartDrainStarted: (restartId) => started.push(restartId),
		onRestartDrainCancelled: (restartId) => cancelled.push(restartId),
	});
	try {
		expect(await manager.drainForRestart("stack-1", 0)).toBe("drained");
		expect(await manager.drainForRestart("stack-1", 0)).toBe("drained");
		expect(started).toEqual(["stack-1"]);
		expect(await manager.cancelRestartDrain("stack-1")).toBe(true);
		expect(cancelled).toEqual(["stack-1"]);
		expect(await manager.drainForRestart("stack-2", 0)).toBe("drained");
		expect(started).toEqual(["stack-1", "stack-2"]);
	} finally {
		await manager.stop();
	}
});

test("restart notice config follows the schema conventions", () => {
	expect(parseConfigFile({ schemaVersion: 1, restartNotice: { enabled: false } }).restartNotice).toEqual({
		enabled: false,
	});
	expect(parseConfigFile({ schemaVersion: 1, restartNotice: { personaName: " Kami " } }).restartNotice).toEqual({
		personaName: "Kami",
	});
	expect(() => parseConfigFile({ schemaVersion: 1, restartNotice: { enabled: "no" } })).toThrow(ConfigError);
	expect(() => parseConfigFile({ schemaVersion: 1, restartNotice: { quiet: true } })).toThrow(ConfigError);
	expect(() => parseConfigFile({ schemaVersion: 1, restartNotice: { personaName: "" } })).toThrow(ConfigError);
});

test("the persona name comes from config, then the SOUL.md heading", () => {
	expect(personaNameFrom("Jip", "# 카미 (Kami)\n")).toBe("Jip");
	expect(personaNameFrom(undefined, "intro\n# 카미 (Kami)\n## 말투\n")).toBe("카미 (Kami)");
	expect(personaNameFrom(undefined, undefined)).toBe("Gajaeway");
});

function serverConfig(directory: string, restartNotice?: GatewayConfig["restartNotice"]): GatewayConfig {
	return {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		...(restartNotice ? { restartNotice } : {}),
	};
}

async function connect(
	socketPath: string,
): Promise<{ send(value: unknown): void; frames: WireFrame[]; close(): void }> {
	const frames: WireFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString("utf8");
				let newline = buffered.indexOf("\n");
				while (newline >= 0) {
					const line = buffered.slice(0, newline);
					buffered = buffered.slice(newline + 1);
					if (line) frames.push(JSON.parse(line));
					newline = buffered.indexOf("\n");
				}
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function until(check: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 400 && !check(); attempt++) await Bun.sleep(5);
	expect(check()).toBe(true);
}

const noticeTexts = (frames: readonly WireFrame[]): string[] =>
	frames.flatMap((frame) =>
		frame.type === "event" && frame.event === "chat.message" && typeof frame.payload?.text === "string"
			? [frame.payload.text]
			: [],
	);

async function restartAcrossServers(restartNotice?: GatewayConfig["restartNotice"]): Promise<{
	readonly before: string[];
	readonly after: string[];
}> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-restart-notice-server-"));
	const config = serverConfig(home, restartNotice);
	const sessionPort = sessionPortFromResponder({ bind: (key) => `session-${key}`, respond: async () => "hello back" });
	database = await Store.open(config.dbPath);
	attachTestBrokerOwnership(database, sessionPort, join(home, "agent"));
	const first = database;
	server = await startUnixServer({ config, database: first, sessionPort, onStop: () => first.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await until(() => client.frames.length >= 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "talk",
		verb: "chat.send",
		params: { origin: DM, text: "hi", engagement: { mentioned: false, group: false, authorId: "u1" } },
	});
	await until(() => noticeTexts(client.frames).includes("hello back"));
	for (const id of ["prepare-1", "prepare-2"]) {
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "gateway.restart.prepare",
			params: { restartId: "stack-9", waitMs: 0 },
		});
		await until(() => client.frames.some((frame) => frame.id === id));
	}
	const before = noticeTexts(client.frames).filter((text) => text !== "hello back");
	// The service manager restarts the gateway with the drain still recorded as held.
	await writeFile(
		join(home, "restart-stack.json"),
		JSON.stringify({ id: "stack-9", state: "running", drainPrepared: true }),
	);
	client.close();
	await server.stop();
	server = undefined;

	database = await Store.open(config.dbPath);
	attachTestBrokerOwnership(database, sessionPort, join(home, "agent"));
	const second = database;
	server = await startUnixServer({ config, database: second, sessionPort, onStop: () => second.close() });
	database = undefined;
	const supervisor = await connect(config.socketPath);
	supervisor.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await until(() => supervisor.frames.length >= 1);
	for (const [id, verb] of [
		["reattach", "gateway.restart.prepare"],
		["release", "gateway.restart.cancel"],
	] as const) {
		supervisor.send({ v: "0.1", type: "request", id, verb, params: { restartId: "stack-9", waitMs: 0 } });
		await until(() => supervisor.frames.some((frame) => frame.id === id));
	}
	await Bun.sleep(20);
	const after = noticeTexts(supervisor.frames);
	supervisor.close();
	return { before, after };
}

test("a stack restart sends one restarting notice and one online notice through the delivery path", async () => {
	const { before, after } = await restartAcrossServers({ personaName: "Kami" });
	expect(before).toEqual([restartingText("Kami")]);
	// The connect sweep may replay the unconfirmed restarting notice; online arrives once.
	expect(after.filter((text) => text === onlineText("Kami"))).toHaveLength(1);
	expect(after.filter((text) => text === cancelledText("Kami"))).toEqual([]);
});

test("a stack restart sends nothing when restart notices are disabled", async () => {
	const { before, after } = await restartAcrossServers({ enabled: false });
	expect(before).toEqual([]);
	expect(after.filter((text) => text !== "hello back")).toEqual([]);
});
