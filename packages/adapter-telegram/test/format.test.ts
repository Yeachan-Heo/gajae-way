import { expect, test } from "bun:test";
import { markdownToTelegramHtml, splitTelegramHtml, telegramHtmlToPlainText } from "../src/format";

test("fenced code keeps its content literal and escaped", () => {
	expect(markdownToTelegramHtml("before\n```ts\nconst a = **b** < c;\n```\nafter")).toBe(
		"before\n<pre>const a = **b** &lt; c;\n</pre>\nafter",
	);
});

test("headers, blockquotes, italics and strikethrough map to Telegram tags", () => {
	expect(markdownToTelegramHtml("# Title\n> one\n> two\n*soft* ~~gone~~")).toBe(
		"<b>Title</b>\n<blockquote>one\ntwo</blockquote>\n<i>soft</i> <s>gone</s>",
	);
});

test("unsafe link schemes and unbalanced markers stay literal", () => {
	expect(markdownToTelegramHtml("[x](javascript:alert(1)) **open and snake_case_name")).toBe(
		"[x](javascript:alert(1)) **open and snake_case_name",
	);
});

test("compact tables render as an aligned monospace block", () => {
	expect(markdownToTelegramHtml("| a | bb |\n|---|---:|\n| 1 | 2 |")).toBe("<pre>a | bb\n--|---\n1 |  2</pre>");
});

test("wide tables render as labelled records", () => {
	const wide = "x".repeat(50);
	expect(markdownToTelegramHtml(`| name | note |\n|---|---|\n| a | ${wide} |`)).toBe(
		`<b>name</b>: a\n<b>note</b>: ${wide}`,
	);
});

test("splitting never breaks an entity and reopens attributes on the next chunk", () => {
	const html = `<a href="https://example.com">${"&amp;".repeat(30)}</a>`;
	const chunks = splitTelegramHtml(html, 60);
	expect(chunks.length).toBeGreaterThan(1);
	for (const chunk of chunks) {
		expect(chunk.length).toBeLessThanOrEqual(60);
		expect(chunk.startsWith('<a href="https://example.com">')).toBe(true);
		expect(chunk.endsWith("</a>")).toBe(true);
		expect(chunk.replace(/<[^>]*>/g, "").replace(/&amp;/g, "")).toBe("");
	}
});

test("plain-text recovery drops tags and decodes entities", () => {
	expect(telegramHtmlToPlainText('<b>a</b> &lt;b&gt; &quot;q&quot; &amp;amp; <a href="https://x">y</a>')).toBe(
		'a <b> "q" &amp; y',
	);
});
