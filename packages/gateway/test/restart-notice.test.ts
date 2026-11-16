import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { originKey } from "@gajae-gateway/protocol";
import { ConfigError, type GatewayConfig, parseConfigFile, type RestartNoticePhase } from "../src/config";
import { DeliveryService } from "../src/delivery/delivery";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { RestartNotices, restartNoticeText } from "../src/server/restart-notice";
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
const KAMI = { personaName: "Kami" } as const;
const restartingText = restartNoticeText(KAMI, "restarting");
const onlineText = restartNoticeText(KAMI, "online");
const cancelledText = restartNoticeText(KAMI, "cancelled");

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
	options: {
		enabled?: boolean;
		bootId?: string;
		inheritedRestartId?: string;
		beforeText?: (phase: RestartNoticePhase) => void;
	} = {},
): { readonly sent: ChatMessagePayload[]; readonly notices: RestartNotices } {
	const sent: ChatMessagePayload[] = [];
	return {
		sent,
		notices: new RestartNotices({
			database: store,
			delivery: new DeliveryService(store),
			enabled: () => options.enabled ?? true,
			text: (phase) => {
				options.beforeText?.(phase);
				return restartNoticeText(KAMI, phase);
			},
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
	expect(sent.every((payload) => payload.text === restartingText && payload.final === false)).toBe(true);
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
	expect(next.sent.map((payload) => [originKey(payload.origin), payload.text])).toEqual([[originKey(DM), onlineText]]);

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
	expect(next.sent.map((payload) => payload.text)).toEqual([onlineText]);
});

test("a restart released by the announcing process sends cancelled and never online", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	const { sent, notices: restart } = notices(store, { bootId: "boot-a" });
	restart.restarting("stack-1");
	restart.released("stack-1");

	expect(sent.map((payload) => payload.text)).toEqual([restartingText, cancelledText]);
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
	expect(next.sent.map((payload) => payload.text)).toEqual([onlineText]);
});

test("a record closed by another gateway connection after it was read is not closed twice", async () => {
	const store = await openStore();
	inbound(store, DM, NOW - 60_000);
	const announcer = notices(store, { bootId: "boot-a" });
	announcer.notices.restarting("stack-1");

	// The replacement generation on its own SQLite connection reads the record, and
	// before it takes the write lock the announcing process releases the drain.
	const other = await Store.open(join(home, "gateway.db"));
	try {
		let raced = false;
		const replacement = notices(other, {
			bootId: "boot-b",
			beforeText: (phase) => {
				if (phase !== "online" || raced) return;
				raced = true;
				announcer.notices.released("stack-1");
			},
		});
		replacement.notices.released("stack-1");

		expect(raced).toBe(true);
		expect(announcer.sent.map((payload) => payload.text)).toEqual([restartingText, cancelledText]);
		expect(replacement.sent).toEqual([]);
		expect(
			store.outbox.listUndelivered(Number.POSITIVE_INFINITY, NOW, true).map((row) => JSON.parse(row.payloadJson).text),
		).toEqual([restartingText, cancelledText]);
	} finally {
		other.close();
	}
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

test("restart notice templates are validated per phase", () => {
	const messages = {
		restarting: "⚠️ {name} 재시작 중입니다. 그동안 보낸 메시지는 재시작 후 답합니다.",
		online: "♻️ {name} 돌아왔습니다.",
	};
	expect(parseConfigFile({ schemaVersion: 1, restartNotice: { messages } }).restartNotice).toEqual({ messages });
	const invalid = (messagesValue: unknown) => () =>
		parseConfigFile({ schemaVersion: 1, restartNotice: { messages: messagesValue } });
	expect(invalid({ back: "x" })).toThrow("restartNotice.messages may only contain restarting, online, cancelled");
	expect(invalid({ online: "" })).toThrow("restartNotice.messages.online must be a non-empty string");
	expect(invalid({ online: "   " })).toThrow("restartNotice.messages.online must be 1 to 1000 characters");
	expect(invalid({ online: "x".repeat(1001) })).toThrow("restartNotice.messages.online must be 1 to 1000 characters");
	expect(invalid({ cancelled: 3 })).toThrow("restartNotice.messages.cancelled must be a non-empty string");
	expect(invalid({ restarting: "{persona} 재시작" })).toThrow("unknown placeholder {persona}");
	expect(invalid([])).toThrow("restartNotice.messages must be an object");
});

test("notice text uses the configured name and templates, else neutral defaults named Gajaeway", () => {
	expect(restartNoticeText(undefined, "restarting")).toBe(
		"⚠️ Gajaeway is restarting. Messages sent meanwhile are kept and answered after restart.",
	);
	expect(restartNoticeText(undefined, "online")).toBe("♻️ Gajaeway is back online.");
	expect(restartNoticeText({ personaName: "카미" }, "online")).toBe("♻️ 카미 is back online.");
	const messages = { online: "♻️ {name} 복귀했습니다. {name} 대기 중." };
	expect(restartNoticeText({ personaName: "카미", messages }, "online")).toBe("♻️ 카미 복귀했습니다. 카미 대기 중.");
	// A phase without an override keeps its default.
	expect(restartNoticeText({ personaName: "카미", messages }, "cancelled")).toBe(
		"↩️ 카미's restart was cancelled. Queued messages are being answered now.",
	);
	expect(restartNoticeText({ messages }, "online")).toBe("♻️ Gajaeway 복귀했습니다. Gajaeway 대기 중.");
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

async function restartAcrossServers(
	restartNotice?: GatewayConfig["restartNotice"],
	soul?: string,
): Promise<{
	readonly before: string[];
	readonly after: string[];
}> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-restart-notice-server-"));
	if (soul !== undefined) {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(join(home, "workspace", "SOUL.md"), soul);
	}
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
	expect(before).toEqual([restartingText]);
	// The connect sweep may replay the unconfirmed restarting notice; online arrives once.
	expect(after.filter((text) => text === onlineText)).toHaveLength(1);
	expect(after.filter((text) => text === cancelledText)).toEqual([]);
});

test("a stack restart sends nothing when restart notices are disabled", async () => {
	const { before, after } = await restartAcrossServers({ enabled: false });
	expect(before).toEqual([]);
	expect(after.filter((text) => text !== "hello back")).toEqual([]);
});

test("a SOUL.md document title is never posted as the persona name", async () => {
	const { before, after } = await restartAcrossServers(undefined, "# SOUL.md — Who You Are\n\nYou are Kami.\n");
	expect(before).toEqual(["⚠️ Gajaeway is restarting. Messages sent meanwhile are kept and answered after restart."]);
	expect(after.filter((text) => text === "♻️ Gajaeway is back online.")).toHaveLength(1);
	expect([...before, ...after].some((text) => text.includes("SOUL.md"))).toBe(false);
});

test("configured phase templates reach chat with the configured name", async () => {
	const { before, after } = await restartAcrossServers({
		personaName: "카미",
		messages: { restarting: "⚠️ {name} 재시작 중입니다.", online: "♻️ {name} 돌아왔습니다." },
	});
	expect(before).toEqual(["⚠️ 카미 재시작 중입니다."]);
	expect(after.filter((text) => text === "♻️ 카미 돌아왔습니다.")).toHaveLength(1);
});
