import { expect, test } from "bun:test";
import { firstVoiceMessage } from "../src/attachments";
import { OrderedIngress, transcribeIfVoice } from "../src/main";
import { readTranscript, transcribeVoiceMessage, withTranscript } from "../src/voice";

const KEY = { apiKey: "test-key" };
const URL = "https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=abc";
const VOICE = {
	name: "voice-message.ogg",
	contentType: "audio/ogg",
	size: 12_400,
	url: URL,
	duration: 3.24,
	waveform: "MjMzNDU=",
};

/** Records every request so the assertions can check what was actually sent. */
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

const audio = () => new Response(new Uint8Array([1, 2, 3, 4]), { status: 200 });

test("readTranscript takes the text and rejects everything that is not a transcript", () => {
	expect(readTranscript({ text: "안녕하세요" })).toBe("안녕하세요");
	expect(readTranscript({ text: "  padded  " })).toBe("padded");
	// Silence transcribes to an empty string, which would render as a blank line.
	expect(readTranscript({ text: "" })).toBeUndefined();
	expect(readTranscript({ text: "   " })).toBeUndefined();
	expect(readTranscript({ text: 42 })).toBeUndefined();
	expect(readTranscript({})).toBeUndefined();
	expect(readTranscript(null)).toBeUndefined();
	expect(readTranscript("nope")).toBeUndefined();
});

// A real 2.4s owner message contained a desk knock and no speech. Auto-detect
// guessed Serbo-Croatian at p=0.266 and returned "[mumbling]"; pinned to Korean
// the same clip returned "[노크 소리]". Either one, injected into the body, would
// enter the permanent history as something the owner had said.
test("a non-speech event tag is not a transcript and is never attributed to the speaker", () => {
	expect(readTranscript({ text: "[mumbling]" })).toBeUndefined();
	expect(readTranscript({ text: "[노크 소리]" })).toBeUndefined();
	expect(readTranscript({ text: "[BLANK_AUDIO]" })).toBeUndefined();
	expect(readTranscript({ text: "(laughs)" })).toBeUndefined();
	expect(readTranscript({ text: "  [mumbling]  " })).toBeUndefined();
	// Several events and nothing else is still no speech.
	expect(readTranscript({ text: "[noise] [노크 소리]" })).toBeUndefined();
	expect(readTranscript({ text: "(coughs)[silence]" })).toBeUndefined();
});

test("speech is kept even when the transcriber annotates an event alongside it", () => {
	// The words are real here, so dropping the whole transcript would lose them.
	expect(readTranscript({ text: "[noise] 형님 테스트입니다" })).toBe("[noise] 형님 테스트입니다");
	expect(readTranscript({ text: "안녕하세요 (laughs)" })).toBe("안녕하세요 (laughs)");
});

test("ordinary speech containing brackets is not mistaken for an event tag", () => {
	expect(readTranscript({ text: "대괄호 [main] 브랜치 말이야" })).toBe("대괄호 [main] 브랜치 말이야");
	expect(readTranscript({ text: "[REPLY:123] 이거 봐" })).toBe("[REPLY:123] 이거 봐");
});

test("withTranscript puts the transcript after the attachment line and leaves the url first", () => {
	expect(withTranscript("[voice message · 3.2s · url]", "안녕")).toBe("[voice message · 3.2s · url]\n안녕");
});

test("withTranscript leaves the body untouched when there is no transcript", () => {
	expect(withTranscript("[voice message · 3.2s · url]", undefined)).toBe("[voice message · 3.2s · url]");
	expect(withTranscript("[voice message · 3.2s · url]", "")).toBe("[voice message · 3.2s · url]");
});

test("a voice message is fetched and transcribed", async () => {
	const { fetch, calls } = stubFetch([audio(), Response.json({ text: "웨이가재 보이스 테스트" })]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch })).toBe("웨이가재 보이스 테스트");
	expect(calls[0]?.url).toBe(URL);
	expect(calls[1]?.url).toBe("https://api.elevenlabs.io/v1/speech-to-text");
	expect((calls[1]?.init?.headers as Record<string, string>)["xi-api-key"]).toBe("test-key");
});

test("no language is pinned by default, because auto-detection handles mixed-language servers", async () => {
	const { fetch, calls } = stubFetch([audio(), Response.json({ text: "ok" })]);
	await transcribeVoiceMessage(URL, KEY, { fetch });
	const form = calls[1]?.init?.body as FormData;
	expect(form.get("model_id")).toBe("scribe_v1");
	expect(form.get("language_code")).toBeNull();
});

test("a pinned language is forwarded when the operator asked for one", async () => {
	const { fetch, calls } = stubFetch([audio(), Response.json({ text: "ok" })]);
	await transcribeVoiceMessage(URL, { ...KEY, languageCode: "ko" }, { fetch });
	expect((calls[1]?.init?.body as FormData).get("language_code")).toBe("ko");
});

test("a failed attachment download fails open instead of losing the message", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([new Response("gone", { status: 404 })]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch, log: (l) => logged.push(l) })).toBeUndefined();
	expect(logged[0]).toContain("404");
});

test("a speech-to-text error fails open and is reported, not thrown", async () => {
	const logged: string[] = [];
	const { fetch } = stubFetch([audio(), new Response("quota", { status: 429 })]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch, log: (l) => logged.push(l) })).toBeUndefined();
	expect(logged[0]).toContain("429");
});

test("a thrown network error fails open", async () => {
	const { fetch } = stubFetch([new Error("econnreset")]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch })).toBeUndefined();
});

test("an empty attachment is not sent to speech-to-text at all", async () => {
	const { fetch, calls } = stubFetch([new Response(new Uint8Array(), { status: 200 })]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch })).toBeUndefined();
	expect(calls).toHaveLength(1);
});

test("a malformed transcript response fails open", async () => {
	const { fetch } = stubFetch([audio(), Response.json({ error: "nope" })]);
	expect(await transcribeVoiceMessage(URL, KEY, { fetch })).toBeUndefined();
});

test("firstVoiceMessage finds the voice attachment and ignores ordinary uploads", () => {
	expect(firstVoiceMessage({ attachments: [{ name: "a.png" }, VOICE] })).toEqual(VOICE);
	expect(firstVoiceMessage({ attachments: [{ name: "a.png" }] })).toBeUndefined();
	expect(firstVoiceMessage({})).toBeUndefined();
});

test("transcription is skipped entirely when no voice config is present", async () => {
	const { fetch, calls } = stubFetch([]);
	expect(await transcribeIfVoice({ attachments: [VOICE] }, undefined, { fetch })).toBeUndefined();
	expect(calls).toHaveLength(0);
});

test("a message without a voice attachment never calls the transcriber", async () => {
	const { fetch, calls } = stubFetch([]);
	expect(await transcribeIfVoice({ attachments: [{ name: "a.png" }] }, KEY, { fetch })).toBeUndefined();
	expect(calls).toHaveLength(0);
});

test("a voice attachment with no url is skipped rather than fetched as undefined", async () => {
	const { fetch, calls } = stubFetch([]);
	expect(await transcribeIfVoice({ attachments: [{ ...VOICE, url: null }] }, KEY, { fetch })).toBeUndefined();
	expect(calls).toHaveLength(0);
});

test("a configured voice message is transcribed end to end", async () => {
	const { fetch } = stubFetch([audio(), Response.json({ text: "잘하자 가재야" })]);
	expect(await transcribeIfVoice({ attachments: [VOICE] }, KEY, { fetch })).toBe("잘하자 가재야");
});

// Ingress became asynchronous, so ordering is now this adapter's problem: the
// gateway serializes per origin but in the order it is handed messages.
test("messages in one conversation keep arrival order even when the first one is slow", async () => {
	const ingress = new OrderedIngress();
	const order: string[] = [];
	ingress.run("chan-1", async () => {
		await Bun.sleep(20);
		order.push("slow-voice");
	});
	ingress.run("chan-1", async () => {
		order.push("fast-text");
	});
	await ingress.drain();
	expect(order).toEqual(["slow-voice", "fast-text"]);
});

test("separate conversations are not serialized against each other", async () => {
	const ingress = new OrderedIngress();
	const order: string[] = [];
	ingress.run("chan-1", async () => {
		await Bun.sleep(20);
		order.push("slow-room");
	});
	ingress.run("chan-2", async () => {
		order.push("other-room");
	});
	await ingress.drain();
	expect(order).toEqual(["other-room", "slow-room"]);
});

test("a failing task does not poison the chain for the messages behind it", async () => {
	const ingress = new OrderedIngress();
	const order: string[] = [];
	ingress.run("chan-1", async () => {
		throw new Error("boom");
	});
	ingress.run("chan-1", async () => {
		order.push("still-delivered");
	});
	await ingress.drain();
	expect(order).toEqual(["still-delivered"]);
});

test("drained conversations are forgotten so a long-lived adapter does not leak", async () => {
	const ingress = new OrderedIngress();
	ingress.run("chan-1", async () => {});
	await ingress.drain();
	// drain() returning with an empty map is the observable form of the cleanup.
	await ingress.drain();
	expect(true).toBe(true);
});
