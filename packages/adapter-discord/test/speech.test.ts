import { expect, test } from "bun:test";
import { discordSpeechPorts } from "../src/main";
import { clampSpeed, MAX_SPEED, MIN_SPEED, oggOpusDuration, speakable, synthesizeVoice } from "../src/speech";

const KEY = { apiKey: "test-key" };

function stubFetch(responses: Array<Response | Error>) {
	const calls: Array<{ url: string; init?: RequestInit }> = [];
	const stub = (async (input: unknown, init?: RequestInit) => {
		calls.push({ url: String(input), ...(init ? { init } : {}) });
		const next = responses[calls.length - 1];
		if (next instanceof Error) throw next;
		if (!next) throw new Error(`unexpected fetch call ${calls.length}`);
		return next;
	}) as unknown as typeof globalThis.fetch;
	return { fetch: stub, calls };
}

/** A minimal Ogg page whose granule position encodes `samples` at 48 kHz. */
function oggWithGranule(samples: number): Uint8Array {
	const page = new Uint8Array(27);
	page.set([0x4f, 0x67, 0x67, 0x53], 0); // "OggS"
	let remaining = samples;
	for (let byte = 0; byte < 8; byte++) {
		page[6 + byte] = remaining % 256;
		remaining = Math.floor(remaining / 256);
	}
	return page;
}

test("spoken text drops what is noise when read aloud", () => {
	expect(speakable("**형님** 확인했습니다")).toBe("형님 확인했습니다");
	expect(speakable("보세요 https://cdn.discordapp.com/a/b/c.ogg?ex=1 끝")).toBe("보세요 끝");
	expect(speakable("결과는 `rc=0` 입니다")).toBe("결과는 rc=0 입니다");
	expect(speakable("[PR 링크](https://github.com/x/y/pull/1) 올렸습니다")).toBe("PR 링크 올렸습니다");
});

test("a fenced code block is removed whole, not left as loose lines", () => {
	expect(speakable("결과입니다\n```\nbun test\n 1157 pass\n```\n끝")).toBe("결과입니다\n끝");
});

test("table and heading markers do not get read as punctuation", () => {
	expect(speakable("## 제목\n| 항목 | 값 |\n- 첫째")).toBe("제목\n항목 | 값 |\n첫째");
});

test("spoken text is capped, and the cut prefers a sentence boundary", () => {
	const body = `${"가".repeat(40)}. ${"나".repeat(400)}`;
	const spoken = speakable(body, 60);
	expect(spoken.length).toBeLessThanOrEqual(60);
	// The boundary at char 40 is past 60% of the cap, so it is used.
	expect(spoken).toBe(`${"가".repeat(40)}.`);
});

test("a cap with no nearby boundary still cuts rather than overrunning", () => {
	const spoken = speakable("나".repeat(300), 50);
	expect(spoken.length).toBe(50);
});

test("duration comes from the Ogg granule position, needing no decoder", () => {
	expect(oggOpusDuration(oggWithGranule(48_000))).toBe(1);
	expect(oggOpusDuration(oggWithGranule(24_000))).toBe(0.5);
	expect(oggOpusDuration(new Uint8Array([1, 2, 3]))).toBeUndefined();
	expect(oggOpusDuration(oggWithGranule(0))).toBeUndefined();
});

test("synthesis posts the spoken text and returns audio with a duration", async () => {
	const { fetch, calls } = stubFetch([new Response(oggWithGranule(96_000), { status: 200 })]);
	const voice = await synthesizeVoice("**안녕하세요** https://x.test/a", KEY, { fetch });
	expect(voice?.seconds).toBe(2);
	expect(voice?.ogg.byteLength).toBeGreaterThan(0);
	expect(calls[0]?.url).toBe(
		"https://api.elevenlabs.io/v1/text-to-speech/pNInz6obpgDQGcFmaJgB?output_format=opus_48000_64",
	);
	// The url and the bold markers must not be spoken or billed.
	expect(JSON.parse(String(calls[0]?.init?.body)).text).toBe("안녕하세요");
});

test("a waveform is always produced, flat when no decoder is wired", async () => {
	const { fetch } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	const voice = await synthesizeVoice("안녕", KEY, { fetch });
	// 64 buckets -> 64 bytes -> 88 base64 chars, which is what Discord echoes back.
	expect(voice?.waveform).toHaveLength(88);
	expect(atob(voice?.waveform ?? "")).toBe("\u0080".repeat(64));
});

test("a wired decoder produces real peaks instead of the flat bar", async () => {
	const { fetch } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	const pcm = new Int16Array(6_400).fill(32_767);
	const voice = await synthesizeVoice("안녕", KEY, { fetch, decodePcm: async () => pcm });
	expect(atob(voice?.waveform ?? "")).toBe("\u00ff".repeat(64));
});

test("a decoder that throws costs the bar, never the voice message", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	const voice = await synthesizeVoice("안녕", KEY, {
		fetch,
		decodePcm: async () => {
			throw new Error("ffmpeg not found");
		},
		log: (line) => logged.push(line),
	});
	expect(voice?.waveform).toHaveLength(88);
	expect(logged[0]).toContain("ffmpeg not found");
});

test("a bare 401 is an authentication failure and synthesis still fails open", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([new Response("not-json", { status: 401 })]);
	expect(await synthesizeVoice("안녕", KEY, { fetch, log: (line) => logged.push(line) })).toBeUndefined();
	expect(logged).toEqual([
		"voice synthesis failed: text-to-speech status=401 category=authentication code=authentication",
	]);
});

test("a bare 429 is classified separately from quota exhaustion", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([new Response("", { status: 429 })]);
	expect(await synthesizeVoice("안녕", KEY, { fetch, log: (line) => logged.push(line) })).toBeUndefined();
	expect(logged[0]).toContain("status=429 category=rate_limited code=rate_limited");
});

test("a malformed provider error body is not copied into logs", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([new Response("Bearer sk-live-do-not-log\nsecond line", { status: 503 })]);
	expect(await synthesizeVoice("안녕", KEY, { fetch, log: (line) => logged.push(line) })).toBeUndefined();
	expect(logged).toEqual([
		"voice synthesis failed: text-to-speech status=503 category=provider_error code=provider_error",
	]);
	expect(logged[0]).not.toContain("sk-live-do-not-log");
	expect(logged[0]).not.toContain("second line");
});

test("secret-like provider codes are replaced rather than logged", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([
		Response.json({ detail: { code: "sk-live-code-secret", message: "provider failed" } }, { status: 503 }),
	]);
	expect(await synthesizeVoice("안녕", KEY, { fetch, log: (line) => logged.push(line) })).toBeUndefined();
	expect(logged[0]).toContain("category=provider_error code=provider_error");
	expect(logged[0]).not.toContain("sk-live-code-secret");
});

test("synthesis fails open on a thrown network error and on empty audio", async () => {
	const a = stubFetch([new Error("econnreset")]);
	expect(await synthesizeVoice("안녕", KEY, { fetch: a.fetch })).toBeUndefined();
	const b = stubFetch([new Response(new Uint8Array(), { status: 200 })]);
	expect(await synthesizeVoice("안녕", KEY, { fetch: b.fetch })).toBeUndefined();
});

test("audio with no readable duration is declined rather than posted as a broken voice message", async () => {
	const { fetch } = stubFetch([new Response(new Uint8Array([9, 9, 9]), { status: 200 })]);
	expect(await synthesizeVoice("안녕", KEY, { fetch })).toBeUndefined();
});

test("a reply that is entirely urls and markup has nothing to say and is not billed", async () => {
	const { fetch, calls } = stubFetch([]);
	expect(await synthesizeVoice("https://a.test/x", KEY, { fetch })).toBeUndefined();
	expect(calls).toHaveLength(0);
});

// The two services share a key, not a URL. Passing the loaded voice config
// straight through type-checks and silently sends TTS to the STT endpoint.
test("speech ports never inherit the speech-to-text endpoint, model or timeout", () => {
	const ports = discordSpeechPorts({
		apiKey: "k",
		apiKeyFile: "/tmp/k",
		endpoint: "https://stt.test/v1/speech-to-text",
		model: "scribe_v1",
		timeoutMs: 20_000,
		languageCode: "ko",
	});
	expect(ports?.config.endpoint).toBeUndefined();
	expect(ports?.config.model).toBeUndefined();
	expect(ports?.config.timeoutMs).toBeUndefined();
	expect(ports?.config.apiKey).toBe("k");
});

test("speech-side settings are carried through under their own names", () => {
	const ports = discordSpeechPorts({
		apiKey: "k",
		apiKeyFile: "/tmp/k",
		voiceId: "voice-9",
		speechModel: "eleven_turbo_v2",
		speechEndpoint: "https://tts.test/v1/text-to-speech",
		outputFormat: "opus_48000_32",
		maxSpokenChars: 200,
		speechTimeoutMs: 5_000,
	});
	expect(ports?.config).toMatchObject({
		voiceId: "voice-9",
		model: "eleven_turbo_v2",
		endpoint: "https://tts.test/v1/text-to-speech",
		outputFormat: "opus_48000_32",
		maxSpokenChars: 200,
		timeoutMs: 5_000,
	});
});

test("no voice config means no speech ports, so voiceText is simply ignored", () => {
	expect(discordSpeechPorts(undefined)).toBeUndefined();
});

// An unterminated fence used to fall through to the inline-code rule, which ate
// two of the three backticks and left one to be spoken — while the code itself
// was read aloud and billed.
test("an unterminated code fence is removed to the end, leaving no stray backtick", () => {
	expect(speakable("앞\n```\ncode here\n뒤")).toBe("앞");
	expect(speakable("```\nbun test\n 1157 pass")).toBe("");
});

test("closed fences are still removed and surrounding prose survives", () => {
	expect(speakable("A\n```\nx\n```\nB\n```\ny\n```\nC")).toBe("A\nB\nC");
});

// The cap lands at an arbitrary offset, so it will eventually fall between the
// halves of an astral character. A lone surrogate is not valid text to send.
test("the cap never leaves a lone surrogate", () => {
	const body = `${"가".repeat(19)}𝄞${"나".repeat(50)}`;
	for (let limit = 15; limit <= 25; limit++) {
		const spoken = speakable(body, limit);
		const last = spoken.charCodeAt(spoken.length - 1);
		expect(last >= 0xd800 && last <= 0xdbff).toBe(false);
	}
});

test("markup-heavy input does not backtrack pathologically", () => {
	const start = Date.now();
	speakable("*".repeat(5_000) + "끝");
	speakable("```" + "a".repeat(20_000));
	expect(Date.now() - start).toBeLessThan(1_000);
});

// Owner decision, 2026-08-29: no cap by default. A listener cannot read the
// remainder out of the text message, so truncating the audio truncates the
// answer for exactly the person the audio exists for.
test("a long reply is spoken in full when no cap is configured", () => {
	const body = "가".repeat(5_000);
	expect(speakable(body)).toHaveLength(5_000);
});

test("an explicitly configured cap still applies", () => {
	expect(speakable("가".repeat(5_000), 100)).toHaveLength(100);
});

test("synthesis sends the whole reply when uncapped", async () => {
	const { fetch, calls } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	const long = `${"긴 보고입니다. ".repeat(200)}끝`;
	await synthesizeVoice(long, KEY, { fetch });
	expect(JSON.parse(String(calls[0]?.init?.body)).text).toBe(speakable(long));
	expect(JSON.parse(String(calls[0]?.init?.body)).text.length).toBeGreaterThan(600);
});

// Owner asked for faster delivery. 1.2 is the provider's ceiling: measured, the
// same line runs 3.77s at 1.0 and 2.56s at 1.2, and 1.3 is rejected with
// invalid_voice_settings. Billing is per character, so speed is free.
test("speech is sent at the provider's maximum speed by default", async () => {
	const { fetch, calls } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	await synthesizeVoice("안녕", KEY, { fetch });
	expect(JSON.parse(String(calls[0]?.init?.body)).voice_settings).toEqual({ speed: 1.2 });
});

test("a configured speed is forwarded", async () => {
	const { fetch, calls } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	await synthesizeVoice("안녕", { ...KEY, speed: 0.9 }, { fetch });
	expect(JSON.parse(String(calls[0]?.init?.body)).voice_settings.speed).toBe(0.9);
});

// An out-of-range speed is a 400 from the provider, which would cost the entire
// voice reply over a config typo. Clamping keeps the audio.
test("an out-of-range speed is clamped rather than losing the voice reply", async () => {
	expect(clampSpeed(1.3)).toBe(MAX_SPEED);
	expect(clampSpeed(0.1)).toBe(MIN_SPEED);
	expect(clampSpeed(Number.NaN)).toBe(1.2);
	const { fetch, calls } = stubFetch([new Response(oggWithGranule(48_000), { status: 200 })]);
	await synthesizeVoice("안녕", { ...KEY, speed: 99 }, { fetch });
	expect(JSON.parse(String(calls[0]?.init?.body)).voice_settings.speed).toBe(1.2);
});
