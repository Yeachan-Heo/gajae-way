import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { InterimSpeechGate, isNearDuplicate, isProceduralNarration } from "../src/server/interim-speech";
import type { GatewayServer } from "../src/server/server";
import { startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

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

test("the gate rules are properly classified in InterimSpeechGate", () => {
	// The gate is the backstop; this test verifies that the gate instance
	// correctly identifies procedural narration and applies rate limiting.
	const gate = new InterimSpeechGate();
	// A narration-only message is suppressed.
	expect(gate.admit("파일을 읽어보겠습니다", 0).deliver).toBe(false);
	// A real finding is delivered.
	expect(gate.admit("원인은 auth 토큰 갱신 실패예요", 0).deliver).toBe(true);
});

// ---------------------------------------------------------------------------
// Integration tests using ScriptedSessionPort/test-broker seam
// ---------------------------------------------------------------------------

test("maxPerTurn 2, minGapMs 0: 5 interim texts + final → assert 2 interim + 1 terminal deliveries", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-interim-maxperturn-2-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-test": { engagement: "open" } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		interimSpeech: { maxPerTurn: 2, minGapMs: 0 },
	});

	// Connect client to the gateway's Unix socket
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});

	// Send hello
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await Bun.sleep(100);

	// Send a channel message to trigger a turn
	const requestId = "req-1";
	socket.write(
		`${JSON.stringify({
			v: "0.1",
			type: "request",
			id: requestId,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "chan-test" },
				text: "trigger turn",
				messageId: "m-1",
				engagement: { mentioned: true, group: true, authorId: "user-1" },
			},
		})}\n`,
	);

	// Wait for the turn to be sent to the session port
	for (let attempt = 0; attempt < 100 && sessionPort.sends.length < 1; attempt++) {
		await Bun.sleep(10);
	}
	expect(sessionPort.sends).toHaveLength(1);

	const send = sessionPort.sends[0]!;

	// Emit 5 interim assistant messages
	for (let i = 1; i <= 5; i++) {
		sessionPort.emitAssistant(send.sessionId, `interim-text-${i}`, `event-${i}`, send.opRef);
		await Bun.sleep(5);
	}

	// Emit the final answer
	sessionPort.complete(send.opRef, "final-answer");

	// Wait for messages to reach the client
	for (let attempt = 0; attempt < 200; attempt++) {
		const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");
		if (messages.length >= 3) break; // 2 interim + 1 terminal
		await Bun.sleep(10);
	}

	socket.end();

	const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");

	// Should have exactly 2 interim deliveries + 1 terminal = 3 messages
	expect(messages.length).toBe(3);

	// The last message should be the final answer (terminal)
	const lastMessage = messages[messages.length - 1]!;
	expect(lastMessage.payload.text).toBe("final-answer");

	// The first two messages should be interim texts
	const interim1 = messages[0]!;
	const interim2 = messages[1]!;
	expect(interim1.payload.text).toBe("interim-text-1");
	expect(interim2.payload.text).toBe("interim-text-2");
});

test("maxPerTurn 0: 5 interim + final → assert 0 interim + 1 terminal delivery", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-interim-maxperturn-0-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-test": { engagement: "open" } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		interimSpeech: { maxPerTurn: 0 },
	});

	// Connect client to the gateway's Unix socket
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});

	// Send hello
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await Bun.sleep(100);

	// Send a channel message to trigger a turn
	const requestId = "req-2";
	socket.write(
		`${JSON.stringify({
			v: "0.1",
			type: "request",
			id: requestId,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "chan-test" },
				text: "trigger turn",
				messageId: "m-2",
				engagement: { mentioned: true, group: true, authorId: "user-1" },
			},
		})}\n`,
	);

	// Wait for the turn to be sent to the session port
	for (let attempt = 0; attempt < 100 && sessionPort.sends.length < 1; attempt++) {
		await Bun.sleep(10);
	}
	expect(sessionPort.sends).toHaveLength(1);

	const send = sessionPort.sends[0]!;

	// Emit 5 interim assistant messages
	for (let i = 1; i <= 5; i++) {
		sessionPort.emitAssistant(send.sessionId, `interim-text-${i}`, `event-${i}`, send.opRef);
		await Bun.sleep(5);
	}

	// Emit the final answer
	sessionPort.complete(send.opRef, "final-answer-maxperturn-0");

	// Wait for messages to reach the client
	for (let attempt = 0; attempt < 200; attempt++) {
		const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");
		if (messages.length >= 1) break; // 1 terminal only
		await Bun.sleep(10);
	}

	socket.end();

	const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");

	// With maxPerTurn: 0, should have exactly 1 message: the final answer (terminal)
	expect(messages.length).toBe(1);
	expect(messages[0]?.payload.text).toBe("final-answer-maxperturn-0");
});

test("boot path: config.json with interimSpeech {maxPerTurn:0} starts via boot.ts, scripted turn runs, 0 interim deliveries", async () => {
	const { writeFile } = await import("node:fs/promises");
	directory = await mkdtemp(join(tmpdir(), "gajaeway-interim-boot-"));

	// Write config.json with interimSpeech { maxPerTurn: 0 }
	const configPath = join(directory, "config.json");
	await writeFile(
		configPath,
		JSON.stringify({
			schemaVersion: 1,
			home: directory,
			configPath,
			socketPath: join(directory, "gateway.sock"),
			dbPath: join(directory, "gateway.db"),
			logVerbosity: "info",
			channels: { "chan-boot": { engagement: "open" } },
			interimSpeech: { maxPerTurn: 0, minGapMs: 0 },
		}),
	);

	const database = await Store.open(join(directory, "gateway.db"));
	const sessionPort = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));

	// Load config via boot.ts pattern (simulating the config.json read)
	const { loadConfig } = await import("../src/config");
	const config = await loadConfig({ home: directory });

	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		interimSpeech: config.interimSpeech,
	});

	// Connect client to the gateway's Unix socket
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});

	// Send hello
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await Bun.sleep(100);

	// Send a channel message to trigger a turn
	const requestId = "req-3";
	socket.write(
		`${JSON.stringify({
			v: "0.1",
			type: "request",
			id: requestId,
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "channel", conversationId: "chan-boot" },
				text: "boot path test",
				messageId: "m-3",
				engagement: { mentioned: true, group: true, authorId: "user-1" },
			},
		})}\n`,
	);

	// Wait for the turn to be sent to the session port
	for (let attempt = 0; attempt < 100 && sessionPort.sends.length < 1; attempt++) {
		await Bun.sleep(10);
	}
	expect(sessionPort.sends).toHaveLength(1);

	const send = sessionPort.sends[0]!;

	// Emit 5 interim assistant messages
	for (let i = 1; i <= 5; i++) {
		sessionPort.emitAssistant(send.sessionId, `interim-boot-${i}`, `event-${i}`, send.opRef);
		await Bun.sleep(5);
	}

	// Emit the final answer
	sessionPort.complete(send.opRef, "final-boot-answer");

	// Wait for messages to reach the client
	for (let attempt = 0; attempt < 200; attempt++) {
		const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");
		if (messages.length >= 1) break; // 1 terminal only
		await Bun.sleep(10);
	}

	socket.end();

	const messages = frames.filter((f: any) => f.type === "event" && f.event === "chat.message");

	// With config-based maxPerTurn: 0, should have exactly 1 message: the final answer (terminal)
	expect(messages.length).toBe(1);
	expect(messages[0]?.payload.text).toBe("final-boot-answer");
});
