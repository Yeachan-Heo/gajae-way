import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort, TurnOptions } from "../src/orchestrator/gjc-client";
import { GENERIC_AGENT_SYSTEM_PROMPT } from "../src/orchestrator/gjc-client";
import { InterimSpeechGate, isNearDuplicate, isProceduralNarration } from "../src/server/interim-speech";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

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

test("the base system prompt carries the mid-work speech rule", () => {
	// The gate is the backstop; this instruction is the primary mechanism.
	expect(GENERIC_AGENT_SYSTEM_PROMPT).toContain("Never narrate your process");
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

async function startGateway(gjc: GjcPort, interimSpeech?: { maxPerTurn?: number; minGapMs?: number }) {
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
	server = await startUnixServer({
		config,
		database,
		gjc,
		onStop: () => database.close(),
		...(interimSpeech ? { interimSpeech } : {}),
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

test("a turn that narrates four steps delivers only its final answer", async () => {
	// The live #71 shape: 4 procedural fragments inside one turn.
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, _progress, options?: TurnOptions) => {
			options?.onAssistantText?.("채널이랑 직전 지시를 더 볼게요");
			options?.onAssistantText?.("관련 파일부터 보겠습니다");
			options?.onAssistantText?.("DB에서 해당 행을 조회해볼게요");
			options?.onAssistantText?.("로그를 확인해볼게요");
			options?.onAssistantText?.("원인은 auth 토큰 갱신 실패였어요");
			return "원인은 auth 토큰 갱신 실패였어요";
		},
		forgetRebinds: () => {},
	};
	const client = await startGateway(gjc);
	sendChannelMessage(client, "n1", "무슨 일이야?");
	const delivered = await waitForMessages(client, 1);
	await Bun.sleep(80);
	expect(messages(client).length).toBe(1);
	expect(delivered[0].payload.text).toContain("auth 토큰 갱신 실패");
});

test("a real mid-work finding is delivered while the turn is still running", async () => {
	let releaseTurn: (() => void) | undefined;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, _progress, options?: TurnOptions) => {
			options?.onAssistantText?.("어 이거 500이 3분마다 찍히고 있는데");
			await new Promise<void>((resolve) => {
				releaseTurn = resolve;
			});
			options?.onAssistantText?.("원인은 auth 토큰 갱신 실패");
			return "원인은 auth 토큰 갱신 실패";
		},
		forgetRebinds: () => {},
	};
	// minGapMs 0 so the final answer is not held back by pacing in this test.
	const client = await startGateway(gjc, { minGapMs: 0 });
	sendChannelMessage(client, "f1", "무슨 일이야?");
	const midTurn = await waitForMessages(client, 1);
	expect(midTurn.length).toBe(1);
	expect(midTurn[0].payload.text).toContain("3분마다");
	releaseTurn?.();
	const all = await waitForMessages(client, 2);
	expect(all.length).toBe(2);
	expect(all[1].payload.text).toContain("auth 토큰 갱신 실패");
});

test("a mid-work message inside the minimum gap is dropped, and the final answer still arrives", async () => {
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, _progress, options?: TurnOptions) => {
			options?.onAssistantText?.("500이 3분마다 찍히고 있어요");
			options?.onAssistantText?.("retry 루프가 3번째에서 죽네요");
			return "원인은 auth 토큰 갱신 실패";
		},
		forgetRebinds: () => {},
	};
	const client = await startGateway(gjc, { minGapMs: 45_000 });
	sendChannelMessage(client, "g1", "무슨 일이야?");
	const all = await waitForMessages(client, 2);
	await Bun.sleep(80);
	expect(messages(client).length).toBe(2);
	expect(all[0].payload.text).toContain("3분마다");
	// The second mid-work message was rate-dropped; the final answer is never gated.
	expect(all[1].payload.text).toContain("auth 토큰 갱신 실패");
});

test("the working indicator still announces and clears around a gated turn", async () => {
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, progress, options?: TurnOptions) => {
			options?.onAssistantText?.("파일부터 확인해볼게요");
			await Bun.sleep(40);
			progress?.({ toolCalls: 6, outputTokens: 120 });
			await Bun.sleep(40);
			return "다 됐어요";
		},
		forgetRebinds: () => {},
	};
	directory = await mkdtemp(join(tmpdir(), "gajaeway-interim-progress-"));
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
	server = await startUnixServer({
		config,
		database,
		gjc,
		onStop: () => database.close(),
		progress: { firstAfterMs: 0, intervalMs: 0 },
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	sendChannelMessage(client, "p1", "상태 어때?");
	const delivered = await waitForMessages(client, 1);
	expect(delivered[0].payload.text).toContain("다 됐어요");
	const progressFrames = client.frames.filter((frame: any) => frame.event === "chat.progress");
	expect(progressFrames.length).toBeGreaterThan(0);
	expect(progressFrames.at(-1).payload.final).toBe(true);
});
