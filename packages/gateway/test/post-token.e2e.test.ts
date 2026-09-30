import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OriginRef } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { deterministicPostDeliveryId } from "../src/orchestrator/tail-runner";
import { type GatewayServer, resolvePostTarget, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

/**
 * [POST:<origin key>] (owner request 2026-10-01): a part of a persona answer
 * goes to another configured channel of the same platform - the "work done"
 * line in a report room - through the ordinary delivery ledger, once.
 */

const HERE = "discord/channel/chan-1";
const REPORT = "discord/channel/report-room";
const channels: GatewayConfig["channels"] = {
	"chan-1": { engagement: "open" },
	"report-room": { engagement: "open" },
	"slack:C9": { engagement: "open" },
};

let directory = "";
let server: GatewayServer | undefined;
let database: GatewayDatabase | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(path: string) {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: path,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value: unknown) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

function gatewayConfig(): GatewayConfig {
	return {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels,
	};
}

async function startGateway(port: ScriptedSessionPort) {
	if (!directory) directory = await mkdtemp(join(tmpdir(), "gajaeway-post-"));
	const config = gatewayConfig();
	database = await GatewayDatabase.open(config.dbPath);
	attachTestBrokerOwnership(database, port, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database?.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	return client;
}

function sendChannelMessage(client: { send(value: unknown): void }, id: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "chan-1" },
			text,
			messageId: `m-${id}`,
			engagement: { mentioned: true, group: true, authorId: "human-1" },
		},
	});
}

function messages(client: { frames: any[] }): any[] {
	return client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
}

function rows(originKey: string) {
	return database!.deliveryRows().filter((row) => row.origin_key === originKey);
}

async function eventually(predicate: () => boolean, message: string, attempts = 400): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function startTurn(port: ScriptedSessionPort, client: Awaited<ReturnType<typeof startGateway>>, id: string) {
	sendChannelMessage(client, id, "끝나면 업무보고에 올려");
	await eventually(() => port.sends.length >= 1, "turn was not sent");
	return port.sends.at(-1)!;
}

test("resolvePostTarget accepts only configured channels of the current platform", () => {
	const here: OriginRef = { platform: "discord", kind: "channel", conversationId: "chan-1" };
	const config = { channels } as GatewayConfig;
	expect(resolvePostTarget(REPORT, here, config)).toMatchObject({ ok: true, key: REPORT });
	expect(resolvePostTarget(` ${REPORT} `, here, config)).toMatchObject({ ok: true, key: REPORT });
	for (const [raw, reason] of [
		["discord/channel/unlisted", "unconfigured_channel"],
		["discord/dm/report-room/peer=u1", "not_a_channel"],
		["discord/thread/t1/parent=report-room", "not_a_channel"],
		["slack/channel/C9", "other_platform"],
		[HERE, "current_conversation"],
		["#업무보고", "invalid_origin_key"],
		["discord/channel", "invalid_origin_key"],
		["", "invalid_origin_key"],
	] as const)
		expect(resolvePostTarget(raw, here, config)).toEqual({ ok: false, reason });
});

test("a [POST:] part goes to the configured target once, recorded there as the persona's own words", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const client = await startGateway(port);
	const send = await startTurn(port, client, "a1");
	port.complete(send.opRef, `여기 답\n[BREAK]\n[POST:${REPORT}] 완료: 배포 끝 <#chan-1>`);
	await eventually(() => messages(client).length === 2, "both parts were not delivered");
	const [here, report] = messages(client);
	expect(here.payload).toMatchObject({ text: "여기 답", origin: { conversationId: "chan-1" } });
	expect(report.payload).toMatchObject({
		text: "완료: 배포 끝 <#chan-1>",
		origin: { platform: "discord", kind: "channel", conversationId: "report-room" },
		final: false,
	});
	const postId = deterministicPostDeliveryId(HERE, REPORT, "m-a1", 1);
	expect(report.payload.deliveryId).toBe(postId);
	expect(rows(REPORT).map((row) => row.delivery_id)).toEqual([postId]);
	// The target's next turn reads the post as unread context from "you".
	const window = database!.contextWindow(REPORT, "next-trigger");
	expect(window.rows.map((row) => [row.message_id, row.body])).toEqual([[`post/${postId}`, "완료: 배포 끝 <#chan-1>"]]);
	expect(window.rows[0]?.author_name).toStartWith("you (posted here from");
	// Once confirmed, a fresh target session sees it once in its recent history.
	expect(new DeliveryLedger(database!).confirm(postId)).toBe("transitioned");
	const recent = database!.recentConversation(REPORT, "report-room", 20, new Date(Date.now() - 60_000).toISOString());
	expect(recent.map((entry) => entry.body)).toEqual(["완료: 배포 끝 <#chan-1>"]);
	// The source room's history keeps the local part only.
	expect(rows(HERE).map((row) => JSON.parse(row.payload_json).text)).toEqual(["여기 답"]);
});

test("a refused target keeps the part here with the token stripped", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const client = await startGateway(port);
	const send = await startTurn(port, client, "f1");
	const logged: string[] = [];
	const original = console.error;
	console.error = (...args: unknown[]) => {
		logged.push(args.map(String).join(" "));
	};
	try {
		port.complete(
			send.opRef,
			[
				"[POST:discord/channel/unlisted] 미등록",
				"[POST:discord/dm/report-room/peer=u1] 디엠",
				"[POST:discord/thread/t1/parent=report-room] 스레드",
				"[POST:slack/channel/C9] 슬랙",
				"[POST:업무보고] 형식오류",
			].join("\n[BREAK]\n"),
		);
		await eventually(() => messages(client).length === 5, "refused parts were not delivered here");
	} finally {
		console.error = original;
	}
	expect(messages(client).map((frame) => [frame.payload.origin.conversationId, frame.payload.text])).toEqual([
		["chan-1", "미등록"],
		["chan-1", "디엠"],
		["chan-1", "스레드"],
		["chan-1", "슬랙"],
		["chan-1", "형식오류"],
	]);
	expect(database!.deliveryRows().every((row) => row.origin_key === HERE)).toBe(true);
	const refusals = logged.filter((line) => line.includes("gateway post target refused"));
	expect(refusals.map((line) => line.match(/reason=(\w+)/)?.[1])).toEqual([
		"unconfigured_channel",
		"not_a_channel",
		"not_a_channel",
		"other_platform",
		"invalid_origin_key",
	]);
});

test("[REPLY:] in a [POST:] part is ignored: the post carries no reply target", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const client = await startGateway(port);
	const send = await startTurn(port, client, "r1");
	port.complete(send.opRef, `[POST:${REPORT}] [REPLY:msg-9] 완료`);
	await eventually(() => messages(client).length === 1, "post was not delivered");
	const [post] = messages(client);
	expect(post.payload).toMatchObject({ text: "완료", origin: { conversationId: "report-room" } });
	expect(post.payload.replyToMessageId).toBeUndefined();
});

test("mid-work speech holds a [POST:] part; the terminal answer posts it exactly once", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const client = await startGateway(port);
	const send = await startTurn(port, client, "h1");
	port.emitTool(send.sessionId);
	// Speech that only appeared mid-work, and the final answer streamed ahead of its terminal.
	port.emitAssistant(send.sessionId, `작업 중\n[BREAK]\n[POST:${REPORT}] 중간에만 쓴 보고`, null, send.opRef);
	port.emitAssistant(send.sessionId, `[POST:${REPORT}] 최종 보고`, "evt-final", send.opRef);
	await eventually(() => messages(client).length === 1, "the local interim part was not delivered");
	await Bun.sleep(100);
	expect(rows(REPORT)).toHaveLength(0);
	expect(messages(client).map((frame) => frame.payload.text)).toEqual(["작업 중"]);
	port.complete(send.opRef, `[POST:${REPORT}] 최종 보고`);
	await eventually(() => rows(REPORT).length === 2, "held and final posts were not sent");
	await eventually(() => database!.inboundNonterminalTurns(HERE).length === 0, "turn never settled");
	await Bun.sleep(50);
	expect(rows(REPORT).map((row) => JSON.parse(row.payload_json).text)).toEqual(["최종 보고", "중간에만 쓴 보고"]);
	expect(messages(client).filter((frame) => frame.payload.origin.conversationId === "report-room")).toHaveLength(2);
});

test("a second terminal pass or a regenerated answer for the same trigger posts nothing again", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const client = await startGateway(port);
	const send = await startTurn(port, client, "d1");
	port.complete(send.opRef, `[POST:${REPORT}] 한 번만`);
	await eventually(() => rows(REPORT).length === 1, "post was not sent");
	const postId = deterministicPostDeliveryId(HERE, REPORT, "m-d1", 0);
	expect(rows(REPORT)[0]?.delivery_id).toBe(postId);
	// The slot is owned: a regenerated answer's claim returns the existing post.
	expect(
		database!.inboundTurnClaimTerminal(
			send.opRef,
			0,
			deterministicPostDeliveryId(HERE, "discord/channel/x", "m-d1", 0),
		),
	).toBe(postId);
	expect(database!.contextWindow(REPORT, "next").rows).toHaveLength(1);
	expect(rows(REPORT)).toHaveLength(1);
});

test("the post survives a gateway restart exactly once: held before the crash, posted by the recovered terminal", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
	const bind = port.bind.bind(port);
	const resume = port.resume.bind(port);
	const client = await startGateway(port);
	const send = await startTurn(port, client, "s1");
	port.emitTool(send.sessionId);
	port.emitAssistant(send.sessionId, `로컬 답\n[BREAK]\n[POST:${REPORT}] 재시작 보고`, "evt-final-s1", send.opRef);
	await eventually(() => messages(client).length === 1, "local part was not delivered");
	expect(rows(REPORT)).toHaveLength(0);
	await server!.stop();
	server = undefined;
	port.seedOperation(send.opRef, send.sessionId, "terminal_ok", `로컬 답\n[BREAK]\n[POST:${REPORT}] 재시작 보고`);
	port.bind = bind;
	port.resume = resume;
	await startGateway(port);
	await eventually(() => database!.inboundNonterminalTurns(HERE).length === 0, "recovered turn never completed", 2_000);
	expect(rows(REPORT).map((row) => [row.delivery_id, JSON.parse(row.payload_json).text])).toEqual([
		[deterministicPostDeliveryId(HERE, REPORT, "m-s1", 1), "재시작 보고"],
	]);
	// The local part kept its pre-restart row.
	expect(rows(HERE)).toHaveLength(1);
}, 20_000);
