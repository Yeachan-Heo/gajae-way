import { expect, test } from "bun:test";
import { chunkSlackMessage, markdownToMrkdwn } from "../src/mrkdwn";

test("Slack markdown conversion handles emphasis, links, headings, lists and escaping", () => {
	for (const [input, output] of [
		["**bold** __bold__ *italic* _italic_ ~~gone~~", "*bold* *bold* _italic_ _italic_ ~gone~"],
		["[label](https://x?a=1&b=2)", "<https://x?a=1&amp;b=2|label>"],
		["# Heading\n### Third", "*Heading*\n*Third*"],
		["- **bold**\n* item\n  - nested\n1. numbered", "• *bold*\n• item\n  • nested\n1. numbered"],
		["a & b < c > d", "a &amp; b &lt; c &gt; d"],
	])
		expect(markdownToMrkdwn(input as string)).toBe(output);
});

test("Slack code retains literal markup and fence language tags", () => {
	const code = "```ts\nconst x = a < b && c > d; // **bold**\n```";
	expect(markdownToMrkdwn(`**before**\n${code}\n*after*`)).toBe(`*before*\n${code}\n_after_`);
	expect(markdownToMrkdwn("`<x> & **literal**` **bold**")).toBe("`<x> & **literal**` *bold*");
	expect(markdownToMrkdwn("```ts\na < b")).toBe("```ts\na < b");
});

test("Slack chunks respect exact boundaries, preserve text and prefer newlines", () => {
	expect(chunkSlackMessage("")).toEqual([""]);
	expect(chunkSlackMessage("x".repeat(4000))).toEqual(["x".repeat(4000)]);
	const text = `${"x".repeat(3900)}\n${"y".repeat(200)}`;
	const chunks = chunkSlackMessage(text);
	expect(chunks[0]).toBe(`${"x".repeat(3900)}\n`);
	expect(chunks.join("")).toBe(text);
	const emoji = chunkSlackMessage(`${"x".repeat(3999)}😀z`);
	expect(emoji.join("")).toBe(`${"x".repeat(3999)}😀z`);
	expect(emoji[0]?.length).toBe(3999);
});

test("Slack chunks avoid short fences and close/reopen oversized code blocks", () => {
	const short = `${"a".repeat(3900)}\n\`\`\`ts\n${"b".repeat(200)}\n\`\`\``;
	const safe = chunkSlackMessage(short);
	expect(safe[0]).toBe(`${"a".repeat(3900)}\n`);
	expect(safe.join("")).toBe(short);
	const long = `\`\`\`ts\n${"b".repeat(8500)}\n\`\`\``;
	const chunks = chunkSlackMessage(long);
	expect(chunks.length).toBeGreaterThan(2);
	for (const chunk of chunks) {
		expect(chunk.length).toBeLessThanOrEqual(4000);
		expect(/^```(?:ts)?\n/.test(chunk)).toBe(true);
		expect(chunk.endsWith("```")).toBe(true);
	}
	expect(chunks.map((chunk) => chunk.replace(/^```(?:ts)?\n/, "").slice(0, -4)).join("")).toBe("b".repeat(8500));
});

test("Slack chunking remains bounded with long language tags and nearby fence boundaries", () => {
	for (const tag of ["ts", "x".repeat(3990), "x".repeat(5000)]) {
		const text = `prefix\n\`\`\`${tag}\n${"a".repeat(8010)}\n\`\`\`\nsuffix`;
		const chunks = chunkSlackMessage(text);
		for (const chunk of chunks) expect(chunk.length).toBeLessThanOrEqual(4000);
	}
});
