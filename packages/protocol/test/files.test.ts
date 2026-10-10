import { expect, test } from "bun:test";
import { fileFallbackText, parseFileReply, platformSupportsFiles } from "../src/index";

test("a reply without MEDIA: lines is left alone", () => {
	expect(parseFileReply("그냥 답장")).toBeUndefined();
	expect(parseFileReply("MEDIA:")).toBeUndefined();
	expect(parseFileReply("MEDIA:   ")).toBeUndefined();
});

test("standalone MEDIA: lines are collected in order, de-duplicated, and removed from the text", () => {
	expect(parseFileReply("보고서입니다\nMEDIA:/tmp/a.csv\n그래프도요\nMEDIA: /tmp/b.png\nMEDIA:/tmp/a.csv")).toEqual({
		paths: ["/tmp/a.csv", "/tmp/b.png"],
		body: "보고서입니다\n그래프도요",
	});
	expect(parseFileReply("MEDIA:/tmp/spaced name.txt ")).toEqual({ paths: ["/tmp/spaced name.txt"], body: "" });
	expect(parseFileReply("a\r\nMEDIA:/tmp/a.csv\r\nb")).toEqual({ paths: ["/tmp/a.csv"], body: "a\nb" });
});

test("only a line that starts with MEDIA: is a directive; prose mentioning it is kept", () => {
	expect(parseFileReply("see MEDIA:/tmp/a.csv")).toBeUndefined();
	expect(parseFileReply("  MEDIA:/tmp/a.csv")).toBeUndefined();
	expect(parseFileReply("`MEDIA:/tmp/a.csv`")).toBeUndefined();
});

test("only Slack uploads files through the ledger; the fallback text names the file", () => {
	expect(platformSupportsFiles("slack")).toBe(true);
	expect(platformSupportsFiles("discord")).toBe(false);
	expect(platformSupportsFiles("telegram")).toBe(false);
	expect(fileFallbackText({ name: "report.csv" })).toBe("📎 report.csv");
});
