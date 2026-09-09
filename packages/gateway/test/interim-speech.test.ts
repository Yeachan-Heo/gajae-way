import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { SessionPort } from "../src/orchestrator/session-port";
import { InterimSpeechGate, isNearDuplicate, isProceduralNarration } from "../src/server/interim-speech";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

// ---------------------------------------------------------------------------
// Pure content gate
// ---------------------------------------------------------------------------

test("process narration is recognized in Korean and English", () => {
	for (const narration of [
		// The exact live failure that produced issue #71.
		"채널이랑 직전 지시를 더 볼게요",
		"먼저 서버 로그부터 보겠습니다",
		"관련 파일을 읽어보겠습니다",
		"DB에서 해당 행을 조회해볼게요",
		"확인해볼게요",
		"지금 채널 히스토리 확인 중이에요",
		"let me check the channel and the last instruction first",
		"reading server.ts now",
		"I'll look at the logs first",
		"now querying the database for that row",
	])
		expect(isProceduralNarration(narration)).toBe(true);
});

test("findings, reactions, heads-ups and questions are not narration", () => {
	for (const worthSaying of [
		"로그에 500이 3분마다 찍히고 있어요. 원인은 auth 토큰 갱신 실패네요",
		"어 이거 생각보다 큰데",
		"이거 10분쯤 걸릴 것 같아요",
		"prod DB랑 staging 둘 중 어디를 고쳐야 해요?",
		"the 500s come from the auth service, every 3 minutes",
		"huh, that's uglier than I thought",
		"this will take a while — the migration has 2M rows",
		"which branch should I push this to?",
		// Verdict, not narration, despite the progressive inspection verb.
		"looking good so far",
	])
		expect(isProceduralNarration(worthSaying)).toBe(false);
});

test("a narration line riding along with a real finding is still delivered", () => {
	// Suppressing the whole message would lose the finding; the narration clause
	// costs one sentence. Documented tradeoff, not an accident.
	expect(isProceduralNarration("auth 토큰 갱신이 실패하고 있어요. 관련 파일을 더 볼게요")).toBe(false);
});

test("near-duplicate detection tolerates punctuation and trailing growth", () => {
	expect(isNearDuplicate("도구 6개 돌렸어요", "도구 6개 돌렸어요.")).toBe(true);
	expect(isNearDuplicate("found the culprit in auth", "Found the culprit in auth!")).toBe(true);
	// Only ~66% shared prefix: a genuinely longer message is not a duplicate.
	expect(isNearDuplicate("found the culprit", "found the culprit and fixed it too")).toBe(false);
	expect(isNearDuplicate("found the culprit", "the retry loop is the problem")).toBe(false);
});

// ---------------------------------------------------------------------------
// Pure pacing gate
// ---------------------------------------------------------------------------

test("the first mid-work message is immediate and the second waits for the gap", () => {
	const gate = new InterimSpeechGate({ minGapMs: 45_000, maxPerTurn: 2 });
	expect(gate.admit("auth 갱신이 실패하고 있어요", 0)).toEqual({ deliver: true });
	expect(gate.admit("retry 루프가 3번째에서 죽어요", 44_999)).toEqual({ deliver: false, reason: "rate" });
	expect(gate.admit("retry 루프가 3번째에서 죽어요", 45_000)).toEqual({ deliver: true });
});

test("a turn spends at most maxPerTurn mid-work messages even when well spaced", () => {
	const gate = new InterimSpeechGate({ minGapMs: 1_000, maxPerTurn: 2 });
	expect(gate.admit("첫 발견", 0).deliver).toBe(true);
	expect(gate.admit("두번째 발견", 10_000).deliver).toBe(true);
	expect(gate.admit("세번째 발견", 20_000)).toEqual({ deliver: false, reason: "turn-cap" });
	expect(gate.deliveredCount).toBe(2);
});

test("consecutive near-identical mid-work messages are suppressed", () => {
	const gate = new InterimSpeechGate({ minGapMs: 0, maxPerTurn: 5 });
	expect(gate.admit("auth 토큰 갱신 실패 확인", 0).deliver).toBe(true);
	expect(gate.admit("auth 토큰 갱신 실패 확인!", 10_000)).toEqual({ deliver: false, reason: "duplicate" });
	// KNOWN LIMITATION: only the PREVIOUS delivered message is compared, and
	// overlap with the not-yet-existing final answer cannot be detected at all.
	expect(gate.admit("retry 루프가 원인이에요", 20_000).deliver).toBe(true);
	expect(gate.admit("auth 토큰 갱신 실패 확인", 30_000).deliver).toBe(true);
});

test("suppressed narration does not spend the turn budget", () => {
	const gate = new InterimSpeechGate({ minGapMs: 0, maxPerTurn: 2 });
	for (const narration of ["파일을 읽어보겠습니다", "채널을 확인해볼게요", "DB를 조회해볼게요", "로그부터 보겠습니다"])
		expect(gate.admit(narration, 0)).toEqual({ deliver: false, reason: "procedural" });
	expect(gate.deliveredCount).toBe(0);
	expect(gate.admit("원인은 auth 토큰 갱신 실패예요", 0).deliver).toBe(true);
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

async function startGateway(
	sessionPort: SessionPort,
	interimSpeech?: { maxPerTurn?: number; minGapMs?: number },
	progress?: { firstAfterMs?: number; intervalMs?: number },
) {
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
		...(interimSpeech ? { interimSpeech } : {}),
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

test("a turn that narrates four steps delivers only its final answer", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port);
	sendChannelMessage(client, "n1", "무슨 일이야?");
	const send = await waitForSend(port);
	port.emitAssistant(send.sessionId, "채널이랑 직전 지시를 더 볼게요");
	port.emitAssistant(send.sessionId, "관련 파일부터 보겠습니다");
	port.emitAssistant(send.sessionId, "DB에서 해당 행을 조회해볼게요");
	port.emitAssistant(send.sessionId, "로그를 확인해볼게요");
	port.emitTool(send.sessionId);
	port.complete(send.opRef, "원인은 auth 토큰 갱신 실패였어요");
	const delivered = await waitForMessages(client, 1);
	expect(messages(client)).toHaveLength(1);
	expect(delivered[0].payload.text).toContain("auth 토큰 갱신 실패");
});

test("a real mid-work finding is delivered while the turn is still running", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port, { minGapMs: 0 });
	sendChannelMessage(client, "f1", "무슨 일이야?");
	const send = await waitForSend(port);
	port.emitTool(send.sessionId);
	port.emitAssistant(send.sessionId, "어 이거 500이 3분마다 찍히고 있는데");
	const midTurn = await waitForMessages(client, 1);
	expect(midTurn).toHaveLength(1);
	expect(midTurn[0].payload.text).toContain("3분마다");
	port.complete(send.opRef, "원인은 auth 토큰 갱신 실패");
	const all = await waitForMessages(client, 2);
	expect(all).toHaveLength(2);
	expect(all[1].payload.text).toContain("auth 토큰 갱신 실패");
});

test("a mid-work message inside the minimum gap is dropped, and the final answer still arrives", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port, { minGapMs: 45_000 });
	sendChannelMessage(client, "g1", "무슨 일이야?");
	const send = await waitForSend(port);
	port.emitTool(send.sessionId);
	port.emitAssistant(send.sessionId, "500이 3분마다 찍히고 있어요");
	port.emitAssistant(send.sessionId, "retry 루프가 3번째에서 죽네요");
	port.complete(send.opRef, "원인은 auth 토큰 갱신 실패");
	const all = await waitForMessages(client, 2);
	expect(messages(client)).toHaveLength(2);
	expect(all[0].payload.text).toContain("3분마다");
	expect(all[1].payload.text).toContain("auth 토큰 갱신 실패");
});

test("the working indicator still announces and clears around a gated turn", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port, undefined, { firstAfterMs: 0, intervalMs: 0 });
	sendChannelMessage(client, "p1", "상태 어때?");
	const send = await waitForSend(port);
	port.emitAssistant(send.sessionId, "파일부터 확인해볼게요");
	port.emitTool(send.sessionId);
	port.emitAssistant(send.sessionId, "다 됐어요");
	port.complete(send.opRef, "다 됐어요");
	const delivered = await waitForMessages(client, 1);
	expect(delivered[0].payload.text).toContain("다 됐어요");
	const progressFrames = client.frames.filter((frame: any) => frame.event === "chat.progress");
	expect(progressFrames.length).toBeGreaterThan(0);
	expect(progressFrames.at(-1).payload.final).toBe(true);
});

// ---------------------------------------------------------------------------
// Regression: plain declarative (반말) narration and the pre-tool structural gate.
// The first round of this feature only recognized polite Korean intent endings,
// so every line jip-gajae actually leaked in #playground-ko (2026-08-31) passed
// the gate. These cases are the measured strings, verbatim.
// ---------------------------------------------------------------------------

test("plain declarative narration is procedural too", () => {
	expect(isProceduralNarration("이미지부터 보고 끼어들 자리인지 판단한다")).toBe(true);
	expect(isProceduralNarration("로그부터 확인한다")).toBe(true);
	expect(isProceduralNarration("채널 히스토리를 먼저 본다")).toBe(true);
});

test("a past-tense report survives the plain declarative rule", () => {
	// "확인했다" is a finding about work already done, not an announcement of work.
	expect(isProceduralNarration("어댑터 기동 시각을 확인했다, 08-29 그대로다")).toBe(false);
	expect(isProceduralNarration("로그를 봤는데 500이 3분마다 찍힌다")).toBe(false);
});

test("a message streamed before the turn ran any tool is suppressed as pre-tool", () => {
	const gate = new InterimSpeechGate({ minGapMs: 0 });
	// Verbatim leak: mixed narration that no content regex classifies as procedural.
	const decision = gate.admit("형님 멘션이다. 기동부터 찍고 한 방만 친다.", 1_000, { toolCallsSoFar: 0 });
	expect(decision.deliver).toBe(false);
	expect(decision.deliver === false && decision.reason).toBe("pre-tool");
	expect(gate.deliveredCount).toBe(0);
});

test("the same message after a tool ran is delivered", () => {
	const gate = new InterimSpeechGate({ minGapMs: 0 });
	const decision = gate.admit("kickstart가 안 걸렸다. 지금 바로 넣는다.", 1_000, { toolCallsSoFar: 3 });
	expect(decision.deliver).toBe(true);
	expect(gate.deliveredCount).toBe(1);
});

test("pre-tool suppression does not spend the turn budget", () => {
	const gate = new InterimSpeechGate({ minGapMs: 0, maxPerTurn: 1 });
	gate.admit("먼저 상황을 본다", 1_000, { toolCallsSoFar: 0 });
	gate.admit("채널부터 읽는다", 2_000, { toolCallsSoFar: 0 });
	const real = gate.admit("빌드가 stale main으로 붙어서 픽스 없는 바이너리가 나왔다", 3_000, { toolCallsSoFar: 5 });
	expect(real.deliver).toBe(true);
});

test("a pre-tool narration turn delivers only its final answer", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	const client = await startGateway(port, { minGapMs: 0 });
	sendChannelMessage(client, "p1", "생각 새는거 원인이 뭐야?");
	const send = await waitForSend(port);
	port.emitAssistant(send.sessionId, "형님 질문이다. 생각 누수부터 원인 잡고 바로 답한다.");
	port.emitAssistant(send.sessionId, "맞다. 도구 돌기 전에 혼잣말이 채팅으로 나갔다.");
	port.emitTool(send.sessionId);
	port.complete(send.opRef, "원인은 게이트가 도구 전 혼잣말을 밀어내는 거다");
	const all = await waitForMessages(client, 1);
	expect(all).toHaveLength(1);
	expect(all[0].payload.text).toContain("도구 전 혼잣말");
});
