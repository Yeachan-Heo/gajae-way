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

test("whitespace-flanked or adjacent delimiters stay literal; real emphasis still renders", () => {
	for (const literal of [
		"2 * 3 * 4",
		"O(n * m * k)",
		"src/**/*.ts",
		"5 * 4 = 20 and 3 * 2 = 6",
		"* use *.ts files",
		"a ** b ** c",
		"x ~~ y ~~ z",
		"**bold *",
	]) {
		expect(markdownToTelegramHtml(literal)).toBe(literal);
	}
	for (const [markdown, html] of [
		["*bold*", "<i>bold</i>"],
		["**bold**", "<b>bold</b>"],
		["~~del~~", "<s>del</s>"],
		["*a*", "<i>a</i>"],
		["**two words**", "<b>two words</b>"],
		["* item with *emphasis*", "* item with <i>emphasis</i>"],
		["**bold ~~inner~~ text**", "<b>bold <s>inner</s> text</b>"],
		["a*b*c", "a<i>b</i>c"],
	] as const) {
		expect(markdownToTelegramHtml(markdown)).toBe(html);
	}
});

test("wide-table records keep body cells beyond the header width", () => {
	const wide = "x".repeat(50);
	expect(markdownToTelegramHtml(`| h1 | h2 |\n|---|---|\n| 1 | ${wide} | 3 |`)).toBe(
		`<b>h1</b>: 1\n<b>h2</b>: ${wide}\n<b>Column 3</b>: 3`,
	);
});

test("a reply made only of markup is posted as its literal text", () => {
	expect(renderTelegramDelivery(">")).toEqual(["&gt;"]);
	expect(renderTelegramDelivery("# ")).toEqual(["# "]);
	expect(renderTelegramDelivery("> ", true)).toEqual(["[recovered - may be a duplicate] &gt; "]);
	expect(renderTelegramDelivery("> <b>")).toEqual(["<blockquote>&lt;b&gt;</blockquote>"]);
	expect(renderTelegramDelivery(">\n>")).toEqual(["&gt;\n&gt;"]);
	expect(renderTelegramDelivery("# \n# ")).toEqual(["# \n# "]);
	expect(renderTelegramDelivery(">\n# ")).toEqual(["&gt;\n# "]);
});

test("whitespace inside a long code block survives chunking", () => {
	const body = `${"x".repeat(4060)}\n${" ".repeat(100)}\n${"y".repeat(10)}`;
	const chunks = renderTelegramDelivery(`\`\`\`\n${body}\n\`\`\``);
	expect(chunks.length).toBeGreaterThan(1);
	expect(chunks.map(telegramHtmlToPlainText).join("")).toBe(`${body}\n`);
});

test("a very large table renders as bounded records without materializing a padded grid", () => {
	const chunks = renderTelegramDelivery(`|h|\n|-|\n${"|x|\n".repeat(1_000_000)}`);
	expect(chunks.every((chunk) => chunk.length <= 4096)).toBe(true);
	expect(markdownToTelegramHtml(`| ${"h".repeat(200)} |\n|---|\n| a |\n| b |`)).toBe(
		`<b>${"h".repeat(200)}</b>: a\n\n<b>${"h".repeat(200)}</b>: b`,
	);
});

test("wide characters align by display width in compact tables", () => {
	expect(markdownToTelegramHtml("| 한국 | b |\n|---|---|\n| x | y |")).toBe("<pre>한국 | b\n-----|--\nx    | y</pre>");
});

test("a table whose records would multiply its size is posted as its literal source", () => {
	const source = `| ${"h".repeat(50_000)} |\n|---|\n${"| x |\n".repeat(10_000)}`.trimEnd();
	const chunks = renderTelegramDelivery(source);
	const visible = chunks.map(telegramHtmlToPlainText).join("");
	expect(visible).toBe(source);
	expect(chunks.every((chunk) => chunk.length <= 4096)).toBe(true);
	expect(chunks.length).toBeLessThan(40);
});

test("a near-limit anchor that would be reopened per character degrades to label and URL", () => {
	const url = `https://e.com/${"a".repeat(4033)}`;
	for (const size of [100, 100_000, 1_000_000]) {
		const label = "x".repeat(size);
		const chunks = renderTelegramDelivery(`[${label}](${url})`);
		expectSafeChunks(chunks, `${label} (${url})`);
		expect(chunks.length).toBeLessThanOrEqual(Math.ceil((size + url.length + 3) / 4063) + 1);
	}
	// An anchor that fits whole on one chunk keeps its link even with a long href.
	const fits = `https://e.com/${"a".repeat(2500)}`;
	expect(renderTelegramDelivery(`[go](${fits})`)).toEqual([`<a href="${fits}">go</a>`]);
});
