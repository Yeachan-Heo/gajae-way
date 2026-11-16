import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { SlackWebApi } from "../src/api";
import { describeInboundBody, type SlackFileLike } from "../src/attachments";
import { type LoadedSlackAdapterConfig, loadSlackAdapterConfig, SlackAdapterStartupError } from "../src/config";
import { type GatewayClientLike, settleSlackDelivery, startSlackAdapter } from "../src/main";
import type { WebSocketLike } from "../src/socket";
import {
	firstSlackVoiceClip,
	isSlackVoiceClip,
	type LoadedSlackVoiceConfig,
	type SlackSpeechConfig,
	SlackVoiceReplies,
	selectVoice,
	transcribeSlackVoiceClip,
} from "../src/voice";

process.env.GAJAEWAY_HOME = join(tmpdir(), `slack-voice-test-home-${crypto.randomUUID()}`);

const CLIP: SlackFileLike = {
	id: "F0CLIP0001",
	name: "audio_message.webm",
	mimetype: "audio/webm",
	subtype: "slack_audio",
	duration_ms: 4300,
	size: 71_782,
	url_private: "https://files.slack.com/files-pri/T1-F0CLIP0001/audio_message.webm",
	url_private_download: "https://files.slack.com/files-pri/T1-F0CLIP0001/download/audio_message.webm",
};
const STT = "https://stt.test/v1/audio/transcriptions";
const TTS = "https://stt.test/v1/audio/speech";
const TRANSCRIPTION = { endpoint: STT, model: "sionic-ai/qwen3-asr-1.7b", language: "ko", apiKey: "og-test-key" };
const SPEECH: SlackSpeechConfig = {
	endpoint: TTS,
	model: "sionic-ai/qwen3-tts-1.7b",
	voice: "sohee",
	voices: {},
	apiKey: "og-test-key",
};
const WAV = Uint8Array.from([...Buffer.from("RIFF"), ...new Uint8Array(60)]);
const AUDIO = Uint8Array.from([0x1a, 0x45, 0xdf, 0xa3, 1, 2, 3, 4]);

type Call = { url: string; init?: RequestInit };
function recorder(route: (url: string, init?: RequestInit) => Response | Promise<Response>) {
	const calls: Call[] = [];
	const fetcher = (async (input: string | URL | Request, init?: RequestInit) => {
		const url = String(input instanceof Request ? input.url : input);
		calls.push({ url, ...(init ? { init } : {}) });
		return route(url, init);
	}) as typeof fetch;
	return { calls, fetcher };
}
const slackOk = (url: string) =>
	url.startsWith("https://files.slack.com/")
		? new Response(AUDIO, { status: 200, headers: { "content-type": "audio/webm" } })
		: undefined;

// ---- detection and rendering ----------------------------------------------------------

test("a Slack clip is recognized by subtype or audio mime type; other files are not", () => {
	expect(isSlackVoiceClip(CLIP)).toBe(true);
	expect(isSlackVoiceClip({ subtype: "slack_audio" })).toBe(true);
	expect(isSlackVoiceClip({ mimetype: "audio/mp4" })).toBe(true);
	expect(isSlackVoiceClip({ mimetype: "video/mp4", subtype: "slack_video" })).toBe(false);
	expect(isSlackVoiceClip({ mimetype: "image/png" })).toBe(false);
});

test("only the first clip of a message is picked for transcription", () => {
	const second = { ...CLIP, id: "F0CLIP0002" };
	expect(firstSlackVoiceClip([{ mimetype: "text/plain" }, CLIP, second])).toBe(CLIP);
	expect(firstSlackVoiceClip([{ mimetype: "text/plain" }])).toBeUndefined();
	expect(firstSlackVoiceClip(undefined)).toBeUndefined();
});

test("with voice on, a clip renders as a voice message with its length, id, size and url", () => {
	expect(describeInboundBody({ text: "", files: [CLIP] }, { voiceClips: true })).toBe(
		`[voice message · 4.3s · id F0CLIP0001 · 70.1 KB · ${CLIP.url_private}]`,
	);
	expect(describeInboundBody({ text: "", files: [{ ...CLIP, duration_ms: 75_000 }] }, { voiceClips: true })).toContain(
		"[voice message · 1:15 ·",
	);
});

test("with voice off, a clip keeps the pre-existing audio line", () => {
	expect(describeInboundBody({ text: "", files: [CLIP] })).toBe(
		`[audio · audio_message.webm · id F0CLIP0001 · 70.1 KB · ${CLIP.url_private}]`,
	);
});

// ---- transcription --------------------------------------------------------------------

test("transcription downloads with the bot token and posts an OpenAI-compatible form", async () => {
	const { calls, fetcher } = recorder(
		(url) => slackOk(url) ?? Response.json({ text: " 회의 일정 확인해 줘 ", usage: { seconds: 2 } }),
	);
	const result = await transcribeSlackVoiceClip(CLIP, TRANSCRIPTION, "xoxb-bot", { fetch: fetcher });
	expect(result).toEqual({ kind: "speech", text: "회의 일정 확인해 줘" });
	expect(calls.map((call) => call.url)).toEqual([CLIP.url_private_download as string, STT]);
	expect(new Headers(calls[0]?.init?.headers).get("authorization")).toBe("Bearer xoxb-bot");
	expect(calls[0]?.init?.redirect).toBe("manual");
	const post = calls[1]?.init;
	expect(new Headers(post?.headers).get("authorization")).toBe("Bearer og-test-key");
	const form = post?.body as FormData;
	expect(form.get("model")).toBe("sionic-ai/qwen3-asr-1.7b");
	expect(form.get("language")).toBe("ko");
	expect(form.get("response_format")).toBe("json");
	const file = form.get("file") as File;
	expect(file.name).toBe("audio_message.webm");
	expect(file.type).toBe("audio/webm");
	expect(new Uint8Array(await file.arrayBuffer())).toEqual(AUDIO);
});

test("a provider rejection becomes an explicit unavailable outcome without leaking the body", async () => {
	const logs: string[] = [];
	const { fetcher } = recorder(
		(url) =>
			slackOk(url) ??
			Response.json(
				{ error: { message: "bad key sk-live-abcdef123456", type: "auth", code: "invalid_api_key" } },
				{
					status: 401,
				},
			),
	);
	const result = await transcribeSlackVoiceClip(CLIP, TRANSCRIPTION, "xoxb-bot", {
		fetch: fetcher,
		log: (line) => logs.push(line),
	});
	expect(result).toEqual({ kind: "unavailable", reason: "authentication" });
	expect(logs.join("\n")).not.toContain("sk-live-abcdef123456");
});

test("download failures, html sign-in pages, oversize clips, foreign urls and timeouts all fail open", async () => {
	const cases: [string, (url: string) => Response | Promise<Response>, SlackFileLike?][] = [
		["404", () => new Response("no", { status: 404 })],
		["html", () => new Response("<html>", { headers: { "content-type": "text/html" } })],
		["oversize", () => new Response(AUDIO, { headers: { "content-length": String(26 * 1024 * 1024) } })],
		["redirect off slack", () => new Response(null, { status: 302, headers: { location: "https://evil.test/steal" } })],
		["foreign url", () => new Response(AUDIO), { ...CLIP, url_private_download: "https://evil.test/a.webm" }],
		["network", () => Promise.reject(new Error("socket hang up"))],
	];
	for (const [name, route, file] of cases) {
		const { calls, fetcher } = recorder(route);
		const result = await transcribeSlackVoiceClip(file ?? CLIP, TRANSCRIPTION, "xoxb-bot", { fetch: fetcher });
		expect({ name, result }).toEqual({ name, result: undefined });
		expect(calls.some((call) => call.url === STT)).toBe(false);
		expect(calls.some((call) => call.url.startsWith("https://evil.test"))).toBe(false);
	}
	const hang = recorder(
		(url, init) =>
			slackOk(url) ??
			new Promise<Response>((_, reject) => init?.signal?.addEventListener("abort", () => reject(new Error("aborted")))),
	);
	expect(
		await transcribeSlackVoiceClip(CLIP, { ...TRANSCRIPTION, timeoutMs: 20 }, "xoxb-bot", { fetch: hang.fetcher }),
	).toBeUndefined();
});

test("a redirect to another Slack file host is followed without the bot token", async () => {
	const { calls, fetcher } = recorder((url) => {
		if (url === CLIP.url_private_download)
			return new Response(null, { status: 302, headers: { location: "https://files-edge.slack.com/x.webm" } });
		if (url.startsWith("https://files-edge.slack.com/")) return new Response(AUDIO);
		return Response.json({ text: "ok" });
	});
	expect(await transcribeSlackVoiceClip(CLIP, TRANSCRIPTION, "xoxb-bot", { fetch: fetcher })).toEqual({
		kind: "speech",
		text: "ok",
	});
	expect(new Headers(calls[1]?.init?.headers).get("authorization")).toBeNull();
});

// ---- voice selection ------------------------------------------------------------------

test("voice selection prefers the speaker, then the conversation, then the default", () => {
	const speech = { ...SPEECH, voices: { U1: "ryan", C1: "vivian", D9: "aiden" } };
	expect(selectVoice(speech, "U1", "C1")).toBe("ryan");
	expect(selectVoice(speech, "U2", "C1")).toBe("vivian");
	expect(selectVoice(speech, undefined, "D9")).toBe("aiden");
	expect(selectVoice(speech, "U2", "C2")).toBe("sohee");
});

// ---- outbound delivery ----------------------------------------------------------------

const origin: OriginRef = { platform: "slack", kind: "thread", conversationId: "C1:1700000000.000100", parentId: "C1" };
const delivery = (extra: Partial<ChatMessagePayload> = {}): ChatMessagePayload => ({
	turnId: "turn-1",
	origin,
	role: "assistant",
	text: "세 시 회의입니다.",
	final: true,
	deliveryId: "d1",
	...extra,
});

class Gateway implements GatewayClientLike {
	readonly requests: { verb: string; params: unknown }[] = [];
	readonly handlers = new Set<(message: ChatMessagePayload) => void>();
	turnId = "accepted-turn";
	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		this.requests.push({ verb, params });
		return { engaged: true, turnId: this.turnId } as T;
	}
	onChatMessage(handler: (message: ChatMessagePayload) => void) {
		this.handlers.add(handler);
		return () => {
			this.handlers.delete(handler);
		};
	}
}

class Api extends SlackWebApi {
	readonly events: string[] = [];
	readonly uploads: { channel: string; name: string; bytes: Uint8Array; threadTs?: string }[] = [];
	uploadFailure?: Error;
	constructor() {
		super("unused", async () => {
			throw new Error("Slack test must not fetch");
		});
	}
	override async postMessage(channel: string, text: string, threadTs?: string) {
		this.events.push(`post ${channel} ${threadTs} ${text}`);
		return { channel, ts: "2.000" };
	}
	override async addReaction() {}
	override async removeReaction() {}
	override async uploadFile(channel: string, file: { name: string; bytes: Uint8Array }, threadTs?: string) {
		this.events.push(`upload ${channel} ${threadTs} ${file.name}`);
		if (this.uploadFailure) throw this.uploadFailure;
		this.uploads.push({ channel, name: file.name, bytes: file.bytes, ...(threadTs ? { threadTs } : {}) });
	}
	override async authTest() {
		return { user_id: "UBOT", bot_id: "B1", user: "bot", team_id: "T1", team: "Workspace" };
	}
	override async usersInfo(id: string) {
		return { id, name: id };
	}
	override async conversationsInfo(id: string) {
		return { id, name: "general" };
	}
	override async connectionsOpen() {
		return { url: "wss://slack.test" };
	}
	override async conversationsHistory() {
		return { messages: [], has_more: false };
	}
	override async conversationsReplies() {
		return { messages: [], has_more: false };
	}
}

function speechRecorder() {
	return recorder(() => new Response(WAV, { headers: { "content-type": "audio/wav" } }));
}

test("a voiceText delivery posts the text, confirms, then uploads the WAV into the same thread", async () => {
	const api = new Api();
	const gateway = new Gateway();
	const tts = speechRecorder();
	const replies = new SlackVoiceReplies(SPEECH, { fetch: tts.fetcher });
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ voiceText: "세 시 회의입니다." }),
		console,
		undefined,
		undefined,
		undefined,
		replies,
	);
	expect(api.events).toEqual([
		"post C1 1700000000.000100 세 시 회의입니다.",
		"upload C1 1700000000.000100 voice-reply.wav",
	]);
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "d1" } }]);
	expect(api.uploads[0]?.bytes).toEqual(WAV);
	expect(JSON.parse(String(tts.calls[0]?.init?.body))).toEqual({
		model: "sionic-ai/qwen3-tts-1.7b",
		input: "세 시 회의입니다.",
		voice: "sohee",
		response_format: "wav",
	});
});

test("an upload or synthesis failure never fails the delivery", async () => {
	for (const setup of ["upload", "synthesis", "not wav"] as const) {
		const api = new Api();
		const gateway = new Gateway();
		const logs: string[] = [];
		if (setup === "upload") api.uploadFailure = new Error("missing_scope");
		const tts = recorder(() =>
			setup === "synthesis"
				? Response.json({ error: { message: "Invalid voice" } }, { status: 400 })
				: setup === "not wav"
					? Response.json({ ok: true })
					: new Response(WAV),
		);
		const replies = new SlackVoiceReplies(SPEECH, { fetch: tts.fetcher, log: (line) => logs.push(line) });
		await settleSlackDelivery(
			gateway,
			api,
			delivery({ voiceText: "hi" }),
			console,
			undefined,
			undefined,
			undefined,
			replies,
		);
		expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "d1" } }]);
		expect(api.uploads).toEqual([]);
		expect(logs.length).toBe(1);
	}
});

test("without speech configured a voiceText delivery is text only", async () => {
	const api = new Api();
	const gateway = new Gateway();
	await settleSlackDelivery(gateway, api, delivery({ voiceText: "hi" }));
	expect(api.events).toEqual(["post C1 1700000000.000100 세 시 회의입니다."]);
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "d1" } }]);
});

test("a delivery without voiceText never synthesizes", async () => {
	const api = new Api();
	const tts = speechRecorder();
	await settleSlackDelivery(
		new Gateway(),
		api,
		delivery(),
		console,
		undefined,
		undefined,
		undefined,
		new SlackVoiceReplies(SPEECH, { fetch: tts.fetcher }),
	);
	expect(tts.calls).toEqual([]);
	expect(api.uploads).toEqual([]);
});

test("the remembered speaker's voice wins over the conversation's; an unknown turn uses the conversation's", async () => {
	const tts = speechRecorder();
	const replies = new SlackVoiceReplies({ ...SPEECH, voices: { U1: "ryan", C1: "vivian" } }, { fetch: tts.fetcher });
	replies.rememberSpeaker("turn-1", "U1");
	const api = new Api();
	await replies.reply(api, { turnId: "turn-1", voiceText: "a" }, "C1", undefined);
	await replies.reply(api, { turnId: "turn-2", voiceText: "b" }, "C1", undefined);
	await replies.reply(api, { turnId: "turn-3", voiceText: "c" }, "C2", undefined);
	expect(tts.calls.map((call) => JSON.parse(String(call.init?.body)).voice)).toEqual(["ryan", "vivian", "sohee"]);
});

// ---- end to end through the adapter ---------------------------------------------------

class Socket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	send() {}
	close() {}
}

async function adapter(voice: LoadedSlackVoiceConfig | undefined, voiceFetch: typeof fetch) {
	const api = new Api();
	const gateway = new Gateway();
	const config: LoadedSlackAdapterConfig = {
		botToken: "xoxb-test",
		appToken: "xapp-test",
		botTokenFile: "bot",
		appTokenFile: "app",
		configPath: "test",
		gatewaySocket: `/tmp/slack-missing-${crypto.randomUUID()}.sock`,
		...(voice ? { voice } : {}),
	};
	const started = await startSlackAdapter(config, {
		api,
		connectGateway: async () => {},
		recoveryCursorPath: join(tmpdir(), `slack-voice-recovery-${crypto.randomUUID()}.json`),
		log: { log() {}, error() {} },
		voiceFetch,
		socketFactory: () => {
			const socket = new Socket();
			queueMicrotask(() => socket.onopen?.({}));
			return socket;
		},
	});
	started.gateway.onConnected = undefined;
	started.gateway.adoptClient(gateway);
	return {
		api,
		gateway,
		started,
		sends: () =>
			gateway.requests
				.filter((request) => request.verb === "chat.send")
				.map((request) => request.params as { text: string; voice?: boolean }),
		stop() {
			started.socket.stop();
			started.recovery.stop();
			started.gateway.stop();
		},
	};
}

const clipMessage = { type: "message", channel: "C1", user: "U1", ts: "1700000000.000001", text: "", files: [CLIP] };

test("a clip is transcribed, flagged as voice, and a text sent right after it still arrives second", async () => {
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const { fetcher } = recorder(async (url) => {
		const slack = slackOk(url);
		if (slack) return slack;
		await gate;
		return Response.json({ text: "세 시 회의 확인해 줘" });
	});
	const f = await adapter({ transcription: TRANSCRIPTION, speech: SPEECH }, fetcher);
	try {
		await f.started.handleEvent(clipMessage);
		await f.started.handleEvent({
			type: "message",
			channel: "C1",
			user: "U1",
			ts: "1700000001.000001",
			text: "그리고",
		});
		await Bun.sleep(5);
		expect(f.sends()).toEqual([]);
		release();
		await f.started.ingress.drain();
		const sends = f.sends();
		expect(sends.map((send) => send.text)).toEqual([
			`[voice message · 4.3s · id F0CLIP0001 · 70.1 KB · ${CLIP.url_private}]\n세 시 회의 확인해 줘`,
			"그리고",
		]);
		expect(sends.map((send) => send.voice)).toEqual([true, undefined]);
	} finally {
		f.stop();
	}
});

test("a failed transcription still forwards the clip as a voice turn", async () => {
	const { fetcher } = recorder((url) => slackOk(url) ?? new Response("down", { status: 503 }));
	const f = await adapter({ transcription: TRANSCRIPTION }, fetcher);
	try {
		await f.started.handleEvent(clipMessage);
		await f.started.ingress.drain();
		expect(f.sends()).toEqual([
			expect.objectContaining({
				text: `[voice message · 4.3s · id F0CLIP0001 · 70.1 KB · ${CLIP.url_private}]\n[transcription unavailable · provider_error]`,
				voice: true,
			}),
		]);
	} finally {
		f.stop();
	}
});

test("the voice turn's speaker picks the reply voice end to end", async () => {
	const { calls, fetcher } = recorder((url) => {
		const slack = slackOk(url);
		if (slack) return slack;
		if (url === STT) return Response.json({ text: "안녕" });
		return new Response(WAV);
	});
	const f = await adapter({ transcription: TRANSCRIPTION, speech: { ...SPEECH, voices: { U1: "eric" } } }, fetcher);
	try {
		await f.started.handleEvent(clipMessage);
		await f.started.ingress.drain();
		for (const handler of f.gateway.handlers)
			handler(
				delivery({
					turnId: "accepted-turn",
					origin: { platform: "slack", kind: "channel", conversationId: "C1" },
					voiceText: "안녕하세요",
				}),
			);
		for (let i = 0; i < 20 && f.api.uploads.length === 0; i++) await Bun.sleep(5);
		expect(f.api.uploads.map((upload) => upload.name)).toEqual(["voice-reply.wav"]);
		const tts = calls.find((call) => call.url === TTS);
		expect(JSON.parse(String(tts?.init?.body)).voice).toBe("eric");
	} finally {
		f.stop();
	}
});

test("without a voice section a clip is forwarded exactly as before: audio line, no transcription, no voice flag", async () => {
	const { calls, fetcher } = recorder(() => Response.json({ text: "never" }));
	const f = await adapter(undefined, fetcher);
	try {
		await f.started.handleEvent(clipMessage);
		await f.started.ingress.drain();
		expect(f.sends()).toEqual([
			expect.objectContaining({
				text: `[audio · audio_message.webm · id F0CLIP0001 · 70.1 KB · ${CLIP.url_private}]`,
			}),
		]);
		expect(f.sends()[0]?.voice).toBeUndefined();
		expect(calls).toEqual([]);
	} finally {
		f.stop();
	}
});

// ---- config ---------------------------------------------------------------------------

async function configFixture(body: (home: string, save: (voice: unknown) => Promise<void>) => Promise<void>) {
	const home = await mkdtemp(join(tmpdir(), "slack-voice-config-"));
	try {
		await writeFile(join(home, "bot"), "xoxb-secret");
		await writeFile(join(home, "app"), "xapp-secret");
		await writeFile(join(home, "og-key"), " og-secret-value \n");
		await body(home, (voice) =>
			writeFile(
				join(home, "adapter-slack.json"),
				JSON.stringify({ botTokenFile: "bot", appTokenFile: "app", ...(voice === undefined ? {} : { voice }) }),
			),
		);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}

const VOICE_SECTION = {
	endpoint: STT,
	model: "sionic-ai/qwen3-asr-1.7b",
	language: "ko",
	apiKeyFile: "og-key",
	speechEndpoint: TTS,
	speechModel: "sionic-ai/qwen3-tts-1.7b",
	voice: "sohee",
};

test("an absent voice section loads no voice config", async () => {
	await configFixture(async (home, save) => {
		await save(undefined);
		expect((await loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).voice).toBeUndefined();
	});
});

test("a voice section loads transcription and speech with the key from a file or the environment", async () => {
	await configFixture(async (home, save) => {
		await save(VOICE_SECTION);
		const fromFile = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home });
		expect(fromFile.voice).toEqual({
			transcription: { endpoint: STT, model: "sionic-ai/qwen3-asr-1.7b", language: "ko", apiKey: "og-secret-value" },
			speech: {
				endpoint: TTS,
				model: "sionic-ai/qwen3-tts-1.7b",
				voice: "sohee",
				voices: {},
				apiKey: "og-secret-value",
			},
		});
		expect(fromFile).not.toHaveProperty("voice.apiKeyFile");
		const { apiKeyFile: _file, speechEndpoint: _a, speechModel: _b, voice: _c, ...sttOnly } = VOICE_SECTION;
		await save({ ...sttOnly, apiKeyEnv: "OG_TEST_KEY" });
		const fromEnv = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home, OG_TEST_KEY: "env-secret" });
		expect(fromEnv.voice?.transcription.apiKey).toBe("env-secret");
		expect(fromEnv.voice?.speech).toBeUndefined();
	});
});

test("voices overrides keep valid user and conversation entries and drop bad ones with a warning", async () => {
	await configFixture(async (home, save) => {
		await save({
			...VOICE_SECTION,
			voices: { U1: "ryan", C1: "vivian", D1: "ono_anna", U2: "Ryan", C2: "alloy", U3: 7, alice: "eric" },
		});
		const warnings: string[] = [];
		const config = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home }, (line) => warnings.push(line));
		expect(config.voice?.speech?.voices).toEqual({ U1: "ryan", C1: "vivian", D1: "ono_anna" });
		expect(warnings).toHaveLength(4);
		expect(warnings.join("\n")).toContain("voice.voices.C2");
		expect(warnings.join("\n")).not.toContain("og-secret-value");
		// A dropped override falls back to the default voice.
		expect(selectVoice(config.voice?.speech as SlackSpeechConfig, "U2", "C2")).toBe("sohee");
	});
});

test("a present but broken voice section is a startup error that never discloses the key", async () => {
	await configFixture(async (home, save) => {
		const { apiKeyFile: _file, ...noKey } = VOICE_SECTION;
		const broken: [unknown, string][] = [
			[{ ...VOICE_SECTION, endpoint: undefined }, "non-empty endpoint"],
			[{ ...VOICE_SECTION, endpoint: "http://plain.test" }, "https url"],
			[{ ...VOICE_SECTION, voice: undefined }, "set together"],
			[noKey, "exactly one of apiKeyFile or apiKeyEnv"],
			[{ ...VOICE_SECTION, apiKeyEnv: "X" }, "exactly one of apiKeyFile or apiKeyEnv"],
			[{ ...noKey, apiKeyEnv: "MISSING_KEY" }, "MISSING_KEY is unset"],
			[{ ...VOICE_SECTION, apiKeyFile: "missing" }, "Check voice.apiKeyFile"],
			[{ ...VOICE_SECTION, timeoutMs: 0 }, "positive integer"],
			[{ ...VOICE_SECTION, voices: ["ryan"] }, "voices must be an object"],
		];
		for (const [voice, message] of broken) {
			await save(voice);
			const error = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home }).catch((caught: unknown) => caught);
			expect(error).toBeInstanceOf(SlackAdapterStartupError);
			expect((error as Error).message).toContain(message);
			expect((error as Error).message).not.toContain("og-secret-value");
		}
	});
});
