import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { SessionPort } from "../src/orchestrator/session-port";
import { InterimSpeechGate, isNearDuplicate } from "../src/server/interim-speech";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

// ---------------------------------------------------------------------------
// Unit: the one remaining rule is near-duplicate suppression.
// ---------------------------------------------------------------------------

test("near-duplicate: identical after normalization, or an 80% prefix", () => {
	expect(isNearDuplicate("확인 중...", "확인 중…!")).toBe(true);
	expect(isNearDuplicate("메모리 파일 4개 다 읽었습니다", "메모리 파일 4개 다 읽었습니다요")).toBe(true);
	expect(isNearDuplicate("Reading server.ts", "reading server.ts!")).toBe(true);
	expect(
		isNearDuplicate("빌드가 stale main으로 붙었다", "빌드가 stale main으로 붙었다, 픽스 없는 바이너리가 나왔다"),
	).toBe(false);
	expect(isNearDuplicate("", "x")).toBe(false);
});

test("every mid-work message is delivered, except a near-repeat of the previous one", () => {
	// Measured 2026-09-18: the former gate (pre-tool / narration / cap 2 / 45 s)
	// suppressed 232 of 232 mid-turn messages in a day; in one thread four
	// [REPLY:…] answers to four user messages were delivered one-late-three-dropped.
	const gate = new InterimSpeechGate();
	const texts = [
		"형님 멘션이다. 기동부터 찍고 한 방만 친다.",
		"[REPLY:C1:1.1] 아닙니다 — 이 랩탑의 gh 활성 계정은 보스 것입니다",
		"[REPLY:C1:1.2] 확인해보니 안 됩니다. 레포 단위 브랜치 보호입니다",
		"[REPLY:C1:1.3] 네, 맞습니다 — 2FA 모바일 인증이 필요합니다",
		"[REPLY:C1:1.4] 네, 그렇습니다. org owner 권한입니다",
		"로그부터 확인한다",
	];
	for (const text of texts) expect(gate.admit(text).deliver).toBe(true);
	expect(gate.deliveredCount).toBe(texts.length);
	const dup = gate.admit("로그부터 확인한다!");
	expect(dup.deliver === false && dup.reason).toBe("duplicate");
	const empty = gate.admit("   ");
	expect(empty.deliver === false && empty.reason).toBe("empty");
});

// ---------------------------------------------------------------------------
// Wired through the server
// ---------------------------------------------------------------------------

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(socketPath: string): Promise<{ send(value: unknown): void; frames: any[]; close(): void }> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function startGateway(sessionPort: SessionPort, progress?: { firstAfterMs?: number; intervalMs?: number }) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-interim-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		...(progress ? { progress } : {}),
	});
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

async function waitForMessages(client: { frames: any[] }, count: number): Promise<any[]> {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (messages(client).length >= count) break;
		await Bun.sleep(5);
	}
	return messages(client);
}

async function waitForSend(port: ScriptedSessionPort) {
	for (let attempt = 0; attempt < 300 && port.sends.length === 0; attempt++) await Bun.sleep(5);
	expect(port.sends).toHaveLength(1);
	return port.sends[0]!;
}

test("a turn that answers steers as it goes delivers each reply immediately, then the final answer", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port);
	sendChannelMessage(client, "p1", "생각 새는거 원인이 뭐야?");
	const send = await waitForSend(port);
	port.emitAssistant(send.sessionId, "[REPLY:p1] 네, 확인했습니다. 원인은 게이트입니다.");
	port.emitAssistant(send.sessionId, "로그부터 확인한다");
	port.emitAssistant(send.sessionId, "[REPLY:p1] 추가로, rate 제한도 걸려 있었습니다.");
	port.emitTool(send.sessionId);
	port.complete(send.opRef, "원인은 게이트가 답변을 밀어내는 거다");
	const all = await waitForMessages(client, 4);
	expect(all.map((m) => m.payload.text)).toEqual([
		"네, 확인했습니다. 원인은 게이트입니다.",
		"로그부터 확인한다",
		"추가로, rate 제한도 걸려 있었습니다.",
		"원인은 게이트가 답변을 밀어내는 거다",
	]);
});

test("a repeated mid-work line is delivered once", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port);
	sendChannelMessage(client, "d1", "상태?");
	const send = await waitForSend(port);
	port.emitAssistant(send.sessionId, "파일 4개 읽었습니다");
	port.emitAssistant(send.sessionId, "파일 4개 읽었습니다.");
	port.emitTool(send.sessionId);
	port.complete(send.opRef, "다 봤습니다");
	const all = await waitForMessages(client, 2);
	expect(all.map((m) => m.payload.text)).toEqual(["파일 4개 읽었습니다", "다 봤습니다"]);
});
