import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { draftScore, speechGateApplies } from "../src/engagement/speech-gate";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const OWNER = "owner-1";
const NARRATION = "기술 질문이라 채널에는 안 올리고, 오늘 기록만 남깁니다.";
const ANSWER = "그 에러는 dmPolicy가 owner-only라서 그렇습니다.";

/**
 * A stand-in for the gate model. The value questions (5 of them) answer with
 * `valueHelp` for `help`; the draft questions (2) read "does it respond" as
 * no for the narration line and yes for anything else.
 */
let valueHelp = 0.9;
let gate: ReturnType<typeof Bun.serve> | undefined;
const drafts: string[] = [];
const saved: Record<string, string | undefined> = {};
const ENV = ["KEV_SHADOW_URL", "KEV_SHADOW_TOKEN", "KEV_GATE_MODE"] as const;

let directory = "";
let server: GatewayServer | undefined;
const logs: string[] = [];
const consoleError = console.error;

beforeEach(() => {
	for (const name of ENV) saved[name] = process.env[name];
	delete process.env.KEV_SHADOW_TOKEN;
	valueHelp = 0.9;
	drafts.length = 0;
	logs.length = 0;
	gate = Bun.serve({
		port: 0,
		async fetch(request) {
			const body = (await request.json()) as { state: string; questions: unknown[] };
			if (body.questions.length === 2) {
				drafts.push(body.state);
				const yes = body.state.includes(NARRATION) ? 0.1 : 0.9;
				return Response.json({ probs: [0, 1].map(() => [1 - yes, yes]) });
			}
			return Response.json({
				probs: [
					[1 - valueHelp, valueHelp],
					[0.9, 0.1],
					[0.9, 0.1],
					[0.9, 0.1],
					[0.9, 0.1],
				],
			});
		},
	});
	process.env.KEV_SHADOW_URL = `http://127.0.0.1:${gate.port}`;
	process.env.KEV_GATE_MODE = "enforce";
	console.error = (...args: unknown[]) => {
		logs.push(args.map(String).join(" "));
	};
});

afterEach(async () => {
	console.error = consoleError;
	await server?.stop();
	server = undefined;
	gate?.stop(true);
	gate = undefined;
	for (const name of ENV) {
		if (saved[name] === undefined) delete process.env[name];
		else process.env[name] = saved[name];
	}
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

test("only unaddressed human traffic from someone other than the owner is gated", () => {
	const ambient = { originKind: "channel", authorId: "human-1", ownerId: OWNER };
	expect(speechGateApplies(ambient)).toBe(true);
	expect(speechGateApplies({ ...ambient, mentioned: true })).toBe(false);
	expect(speechGateApplies({ ...ambient, replyToSelf: true })).toBe(false);
	expect(speechGateApplies({ ...ambient, threadFollowUp: true })).toBe(false);
	expect(speechGateApplies({ ...ambient, authorId: OWNER })).toBe(false);
	expect(speechGateApplies({ ...ambient, authorIsBot: true })).toBe(false);
	expect(speechGateApplies({ ...ambient, originKind: "dm" })).toBe(false);
});

test("the gate is opt-in and needs the gate model", () => {
	const ambient = { originKind: "channel", authorId: "human-1", ownerId: OWNER };
	process.env.KEV_GATE_MODE = "shadow";
	expect(speechGateApplies(ambient)).toBe(false);
	delete process.env.KEV_GATE_MODE;
	expect(speechGateApplies(ambient)).toBe(false);
	process.env.KEV_GATE_MODE = "enforce";
	delete process.env.KEV_SHADOW_URL;
	expect(speechGateApplies(ambient)).toBe(false);
});

test("a missing draft verdict reads as a response, never as narration", () => {
	expect(draftScore([])).toBe(1);
	expect(draftScore([0.1, 0.2])).toBeCloseTo(0.15);
});

async function openChannel(reply: string): Promise<{ send(text: string, engagement: object): Promise<any[]> }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-speech-gate-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open" } },
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: OWNER } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => reply,
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
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
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	for (let attempt = 0; attempt < 60 && frames.length < 1; attempt++) await Bun.sleep(5);
	let sequence = 0;
	return {
		async send(text, engagement) {
			const id = `c${++sequence}`;
			socket.write(
				`${JSON.stringify({
					v: "0.1",
					type: "request",
					id,
					verb: "chat.send",
					params: {
						origin: { platform: "discord", kind: "channel", conversationId: "chan-1" },
						text,
						messageId: `m${sequence}`,
						engagement: { group: true, mentioned: false, ...engagement },
					},
				})}\n`,
			);
			for (let attempt = 0; attempt < 100; attempt++) await Bun.sleep(5);
			return frames;
		},
	};
}

const messages = (frames: any[]) =>
	frames.filter((frame) => frame.type === "event" && frame.event === "chat.message").map((frame) => frame.payload.text);

test("abstention narration on ambient traffic never reaches the channel, and the drop is logged", async () => {
	const room = await openChannel(NARRATION);
	const frames = await room.send("GJC 모델 설정 어디서 바꿈?", { authorId: "human-1" });
	expect(messages(frames)).toEqual([]);
	expect(logs.some((line) => line.includes("speech-gate drop") && line.includes(NARRATION))).toBe(true);
});

test("a real reply on ambient traffic is judged and delivered", async () => {
	const room = await openChannel(ANSWER);
	const frames = await room.send("DM이 안 먹혀요 왜죠", { authorId: "human-1" });
	expect(messages(frames)).toEqual([ANSWER]);
	expect(drafts).toHaveLength(1);
});

test("a message the value score would skip opens no turn", async () => {
	valueHelp = 0.1;
	const room = await openChannel(ANSWER);
	const frames = await room.send("ㅋㅋㅋ", { authorId: "human-1" });
	const response = frames.find((frame) => frame.type === "response" && frame.id === "c1");
	expect(response.result).toEqual({ turnId: null, engaged: false });
	expect(messages(frames)).toEqual([]);
	expect(logs.some((line) => line.includes("speech-gate skip"))).toBe(true);
});

test("a mention is never gated, whatever the persona says", async () => {
	valueHelp = 0.1;
	const room = await openChannel(NARRATION);
	const frames = await room.send("GJC 모델 설정 어디서 바꿈?", { authorId: "human-1", mentioned: true });
	expect(messages(frames)).toEqual([NARRATION]);
	expect(drafts).toHaveLength(0);
});

test("the owner is never gated, whatever the persona says", async () => {
	valueHelp = 0.1;
	const room = await openChannel(NARRATION);
	const frames = await room.send("GJC 모델 설정 어디서 바꿈?", { authorId: OWNER });
	expect(messages(frames)).toEqual([NARRATION]);
	expect(drafts).toHaveLength(0);
});

test("shadow mode measures but changes nothing", async () => {
	process.env.KEV_GATE_MODE = "shadow";
	valueHelp = 0.1;
	const room = await openChannel(NARRATION);
	const frames = await room.send("GJC 모델 설정 어디서 바꿈?", { authorId: "human-1" });
	expect(messages(frames)).toEqual([NARRATION]);
	expect(drafts).toHaveLength(0);
});

test("an unreachable gate model fails open", async () => {
	gate?.stop(true);
	valueHelp = 0.1;
	const room = await openChannel(NARRATION);
	const frames = await room.send("GJC 모델 설정 어디서 바꿈?", { authorId: "human-1" });
	expect(messages(frames)).toEqual([NARRATION]);
});
