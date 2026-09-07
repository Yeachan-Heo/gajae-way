import { expect, test } from "bun:test";
import { redactHistoricalAttachments } from "../src/server/attachment-scope";

test("historical attachment lines lose their url but keep kind, name and size", () => {
	const body =
		"look at this\n[image · IMG_1790.png · 217.3 KB · https://cdn.discordapp.com/attachments/1/2/IMG_1790.png?ex=1&is=2]";
	expect(redactHistoricalAttachments(body)).toBe(
		"look at this\n[image · IMG_1790.png · 217.3 KB · past attachment; not part of this message, do not fetch]",
	);
});

test("every attachment kind and a voice message are redacted, several per body", () => {
	const body = [
		"[voice message · 4.2s · 12 KB · https://cdn.discordapp.com/a.ogg]",
		"[video · clip.mp4 · 3.1 MB · https://cdn.discordapp.com/b.mp4]",
		"[file · https://cdn.discordapp.com/c.bin]",
		"[+2 more attachments]",
	].join("\n");
	expect(redactHistoricalAttachments(body)).toBe(
		[
			"[voice message · 4.2s · 12 KB · past attachment; not part of this message, do not fetch]",
			"[video · clip.mp4 · 3.1 MB · past attachment; not part of this message, do not fetch]",
			"[file · past attachment; not part of this message, do not fetch]",
			"[+2 more attachments]",
		].join("\n"),
	);
});

test("plain urls and prose stay untouched", () => {
	const body = "see https://example.com/page and [not an attachment] and image: foo.png";
	expect(redactHistoricalAttachments(body)).toBe(body);
});
