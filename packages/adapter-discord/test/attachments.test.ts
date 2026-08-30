import { expect, test } from "bun:test";
import {
	collectAttachments,
	describeAttachment,
	describeInboundBody,
	isVoiceMessageAttachment,
} from "../src/attachments";

const VOICE = {
	name: "voice-message.ogg",
	contentType: "audio/ogg",
	size: 12_400,
	url: "https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=abc",
	duration: 3.24,
	waveform: "MjMzNDU=",
};

test("a voice message is recognized by its duration and waveform", () => {
	expect(isVoiceMessageAttachment(VOICE)).toBe(true);
});

test("an ordinary audio upload is not a voice message even with the same mime type", () => {
	expect(isVoiceMessageAttachment({ ...VOICE, duration: null, waveform: null })).toBe(false);
});

test("a waveform without a duration is not enough to claim a voice message", () => {
	expect(isVoiceMessageAttachment({ waveform: "MjM=", contentType: "audio/ogg" })).toBe(false);
});

test("a voice message renders its length, size and url", () => {
	expect(describeAttachment(VOICE)).toBe(
		"[voice message · 3.2s · 12.1 KB · https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=abc]",
	);
});

test("a voice message over a minute renders as m:ss", () => {
	expect(describeAttachment({ ...VOICE, duration: 95.4, size: null, url: null })).toBe("[voice message · 1:35]");
});

test("attachment kind comes from the mime type so the persona need not parse it", () => {
	expect(describeAttachment({ name: "shot.png", contentType: "image/png", size: 500 })).toBe(
		"[image · shot.png · 500 B]",
	);
	expect(describeAttachment({ name: "clip.mp4", contentType: "video/mp4", size: 3_000_000 })).toBe(
		"[video · clip.mp4 · 2.9 MB]",
	);
	expect(describeAttachment({ name: "report.pdf", contentType: "application/pdf" })).toBe("[file · report.pdf]");
	expect(describeAttachment({ name: "notes.txt" })).toBe("[file · notes.txt]");
});

test("an attachment-only message produces a body instead of the empty string that was dropped", () => {
	expect(describeInboundBody({ content: "", attachments: [VOICE] })).toBe(describeAttachment(VOICE));
});

test("a caption keeps its own line and the attachment follows it", () => {
	expect(
		describeInboundBody({ content: "이거 봐라", attachments: [{ name: "a.png", contentType: "image/png" }] }),
	).toBe("이거 봐라\n[image · a.png]");
});

test("a whitespace-only caption is treated as no caption rather than kept as a blank line", () => {
	expect(describeInboundBody({ content: "   ", attachments: [{ name: "a.png", contentType: "image/png" }] })).toBe(
		"[image · a.png]",
	);
});

test("a plain text message is passed through untouched", () => {
	expect(describeInboundBody({ content: "안녕", attachments: [] })).toBe("안녕");
	expect(describeInboundBody({ content: "안녕" })).toBe("안녕");
});

test("a message with neither text nor attachments stays empty so it is still dropped", () => {
	expect(describeInboundBody({ content: "" })).toBe("");
	expect(describeInboundBody({})).toBe("");
});

test("every attachment gets a line", () => {
	const body = describeInboundBody({
		content: "",
		attachments: [
			{ name: "a.png", contentType: "image/png" },
			{ name: "b.png", contentType: "image/png" },
		],
	});
	expect(body).toBe("[image · a.png]\n[image · b.png]");
});

test("a flood of attachments is capped with a remainder line so one message cannot swamp the turn", () => {
	const attachments = Array.from({ length: 14 }, (_, index) => ({ name: `f${index}.png`, contentType: "image/png" }));
	const lines = describeInboundBody({ content: "", attachments }).split("\n");
	expect(lines).toHaveLength(11);
	expect(lines[10]).toBe("[+4 more attachments]");
});

test("the remainder line stays singular for exactly one extra attachment", () => {
	const attachments = Array.from({ length: 11 }, (_, index) => ({ name: `f${index}.png`, contentType: "image/png" }));
	expect(describeInboundBody({ content: "", attachments }).split("\n")[10]).toBe("[+1 more attachment]");
});

test("collectAttachments tolerates the field being absent, null or already an array", () => {
	expect(collectAttachments({})).toEqual([]);
	expect(collectAttachments({ attachments: null })).toEqual([]);
	expect(collectAttachments({ attachments: [] })).toEqual([]);
	expect(collectAttachments({ attachments: [VOICE] })).toEqual([VOICE]);
});

// discord.js `Collection` extends `Map`, so bare iteration yields `[id, attachment]`
// entry pairs. Reading those as attachments produced tuples with no fields, rendering
// every line as a bare `[file]`, which is why `values()` is preferred.
test("a Map-like collection is read through values() rather than as entry pairs", () => {
	const collection = new Map([["1", VOICE]]);
	expect(collectAttachments({ attachments: collection })).toEqual([VOICE]);
	expect(describeInboundBody({ content: "", attachments: collection })).toBe(describeAttachment(VOICE));
});
