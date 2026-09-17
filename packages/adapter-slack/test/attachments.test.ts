import { expect, test } from "bun:test";
import { describeInboundBody, describeSlackFile } from "../src/attachments";

test("Slack files use MIME kind, preferred name/url, and readable sizes", () => {
	expect(
		describeSlackFile({
			mimetype: "image/png",
			name: "photo.png",
			title: "title",
			size: 1.2 * 1024 * 1024,
			url_private: "https://files.slack.com/photo",
			permalink: "ignored",
		}),
	).toBe("[image · photo.png · 1.2 MB · https://files.slack.com/photo]");
	expect(describeSlackFile({ name: " ", title: "clip", mimetype: "VIDEO/mp4", size: 1024, permalink: "url" })).toBe(
		"[video · clip · 1 KB · url]",
	);
	expect(describeSlackFile({ mimetype: "audio/ogg", size: 12 })).toBe("[audio · 12 B]");
	for (const size of [-1, Number.NaN, Number.POSITIVE_INFINITY]) expect(describeSlackFile({ size })).toBe("[file]");
});

test("Slack inbound bodies retain captions, cap file rendering, and distinguish empty messages", () => {
	expect(describeInboundBody({})).toBe("");
	expect(describeInboundBody({ text: " untouched " })).toBe(" untouched ");
	expect(describeInboundBody({ text: "caption", files: [{ name: "one" }] })).toBe("caption\n[file · one]");
	expect(describeInboundBody({ text: "  ", files: [{}] })).toBe("[file]");
	for (const count of [11, 12]) {
		const lines = describeInboundBody({ files: Array.from({ length: count }, () => ({})) }).split("\n");
		expect(lines).toHaveLength(11);
		expect(lines[10]).toBe(`[+${count - 10} more attachment${count === 11 ? "" : "s"}]`);
	}
});
