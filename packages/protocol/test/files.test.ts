import { expect, test } from "bun:test";
import { fileFallbackText, parseFileReply, platformSupportsFiles } from "../src/index";

test("a reply without [FILE:] tokens is left alone", () => {
	expect(parseFileReply("그냥 답장")).toBeUndefined();
	expect(parseFileReply("[FILE:]")).toBeUndefined();
});

test("tokens anywhere in the reply are collected in order, de-duplicated, and stripped from the text", () => {
	expect(parseFileReply("보고서입니다 [FILE:/tmp/a.csv]\n그래프도요\n[FILE:/tmp/b.png]\n[FILE:/tmp/a.csv]")).toEqual({
		paths: ["/tmp/a.csv", "/tmp/b.png"],
		body: "보고서입니다\n그래프도요",
	});
	expect(parseFileReply("[FILE: /tmp/spaced name.txt ]")).toEqual({ paths: ["/tmp/spaced name.txt"], body: "" });
});

test("a token never spans lines", () => {
	expect(parseFileReply("[FILE:/tmp/a\n.csv]")).toBeUndefined();
});

test("only Slack uploads files; the fallback text names the file", () => {
	expect(platformSupportsFiles("slack")).toBe(true);
	expect(platformSupportsFiles("discord")).toBe(false);
	expect(platformSupportsFiles("telegram")).toBe(false);
	expect(fileFallbackText({ name: "report.csv" })).toBe("📎 report.csv");
});
