import { expect, test } from "bun:test";
import { markdownToTelegramHtml, splitTelegramHtml, telegramHtmlToPlainText } from "../src/format";
import { renderTelegramDelivery } from "../src/main";

function expectSafeChunks(chunks: string[], visible: string, max = 4096): void {
	for (const chunk of chunks) {
		expect(chunk.isWellFormed()).toBe(true);
		expect(chunk.length).toBeLessThanOrEqual(max);
		expect(telegramHtmlToPlainText(chunk).length).toBeGreaterThan(0);
		expect(chunk).not.toContain("\u0000");
	}
	expect(chunks.map(telegramHtmlToPlainText).join("")).toBe(visible);
}

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
	expectSafeChunks(chunks, "&".repeat(30), 60);
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

test("plain and formatted chunk boundaries preserve surrogate pairs", () => {
	for (const [markdown, visible] of [
		[`${"a".repeat(4095)}😀`, `${"a".repeat(4095)}😀`],
		[`**${"😀".repeat(2500)}**`, "😀".repeat(2500)],
	] as const) {
		const chunks = renderTelegramDelivery(markdown);
		expect(chunks).toHaveLength(2);
		expectSafeChunks(chunks, visible);
	}
});

test("link labels resolve protected inline code without leaking placeholders", () => {
	for (const [markdown, html] of [
		["[`code`](https://example.com)", '<a href="https://example.com">code</a>'],
		["[a `x<&` b](https://example.com)", '<a href="https://example.com">a x&lt;&amp; b</a>'],
		["[code](https://example.com/`path`)", '<a href="https://example.com/path">code</a>'],
		["\u0000ph0\u0000 [`code`](https://example.com)", 'ph0 <a href="https://example.com">code</a>'],
	] as const) {
		const rendered = markdownToTelegramHtml(markdown);
		expect(rendered).toBe(html);
		expect(rendered).not.toContain("\u0000");
	}
});

test("tables containing protected code resolve nested placeholders literally", () => {
	const html = markdownToTelegramHtml("| h |\n|---|\n| ```x<&``` |");
	expect(html).not.toContain("\u0000");
	expect(telegramHtmlToPlainText(html)).toContain("x<&");
});

test("same-line and unclosed fences preserve their literal code content", () => {
	for (const [markdown, html] of [
		["```hello```", "<code>hello</code>"],
		["```hello```\nafter", "<code>hello</code>\nafter"],
		["```<hello & goodbye>```", "<code>&lt;hello &amp; goodbye&gt;</code>"],
		["```ts\nconst x = **raw** & <tag>;", "<pre>const x = **raw** &amp; &lt;tag&gt;;</pre>"],
		["```hello", "```hello"],
		["  ```ts\n  const x = **raw** < y;\n  ```", "<pre>const x = **raw** &lt; y;\n</pre>"],
		["   ```\n   a & b\n    c\n   ```", "<pre>a &amp; b\n c\n</pre>"],
	] as const) {
		const rendered = markdownToTelegramHtml(markdown);
		expect(rendered).toBe(html);
		expect(rendered).not.toContain("\u0000");
	}
});

test("oversized anchors degrade to label and URL without empty or oversized chunks", () => {
	const url = `https://x/${"a".repeat(4068)}`;
	const chunks = renderTelegramDelivery(`[&&&&](${url})`);
	expect(chunks.length).toBeGreaterThan(1);
	expect(chunks.every((chunk) => !chunk.includes("<a "))).toBe(true);
	expectSafeChunks(chunks, `&&&& (${url})`);

	const escapedUrl = `https://x/?q=${"a".repeat(4096)}&other=😀`;
	expectSafeChunks(renderTelegramDelivery(`[\`x<&\`](${escapedUrl})`), `x<& (${escapedUrl})`);
});

test("chunk reopening budgets entities and nested formatting without tag-only chunks", () => {
	const html = `<b>${"z".repeat(30)}<a href="https://x/${"a".repeat(25)}">&amp;😀&amp;</a></b>`;
	const chunks = splitTelegramHtml(html, 60);
	expectSafeChunks(chunks, `${"z".repeat(30)}&😀& (https://x/${"a".repeat(25)})`, 60);
	expectSafeChunks(splitTelegramHtml(`<b></b>${"x".repeat(100)}`, 10), "x".repeat(100), 10);
	expect(splitTelegramHtml("<b></b>".repeat(1000))).toEqual([]);
	expect(splitTelegramHtml("<b></b>")).toEqual([]);
});

test("crossing emphasis stays literal inside properly nested tags", () => {
	for (const [markdown, html] of [
		["**bold ~~both** strike~~", "<b>bold ~~both</b> strike~~"],
		["~~strike **both~~ bold**", "<s>strike **both</s> bold**"],
		["**bold ~~both~~**", "<b>bold <s>both</s></b>"],
		["~~strike **both**~~", "<s>strike <b>both</b></s>"],
		["# ~~open\nclose~~", "<b>~~open</b>\nclose~~"],
	] as const) {
		const rendered = markdownToTelegramHtml(markdown);
		expect(rendered).toBe(html);
		expect(rendered).not.toContain("\u0000");
		const stack: string[] = [];
		for (const match of rendered.matchAll(/<(\/?)([a-z-]+)(?:\s[^>]*)?>/g)) {
			if (match[1]) expect(stack.pop()).toBe(match[2]);
			else stack.push(match[2] ?? "");
		}
		expect(stack).toEqual([]);
	}
});
