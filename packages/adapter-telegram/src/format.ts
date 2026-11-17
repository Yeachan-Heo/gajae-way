/**
 * Telegram HTML formatting for outbound gateway deliveries.
 *
 * Adapted from Gajae-Code's Telegram notifier (`sdk/bus/html-format.ts`), with
 * local fixes for malformed Markdown and safe chunk boundaries. The persona
 * writes Markdown, Telegram receives `parse_mode: "HTML"`.
 *
 * The adapted Gajae-Code source is distributed under the MIT License:
 *
 *   Copyright (c) 2025-2026 Yeachan-Heo and Gajae Code Contributors
 *
 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:
 *
 *   The above copyright notice and this permission notice shall be included in all
 *   copies or substantial portions of the Software.
 *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 *
 * Discipline: escape first, tag second. Telegram only parses a small tag set
 * (b, i, u, s, code, pre, a, blockquote, tg-spoiler); a stray `<` or unbalanced
 * tag makes Telegram reject the whole message, so dynamic text is always
 * escaped before any tag is emitted.
 */

export const TELEGRAM_PARSE_MODE = "HTML" as const;
export const TELEGRAM_MESSAGE_LIMIT = 4096;

/** Tags Telegram parses in HTML mode (used by the chunking guard). */
const ALLOWED_TAGS = new Set(["b", "i", "u", "s", "code", "pre", "a", "blockquote", "tg-spoiler"]);

/** Escape text for Telegram HTML body content (`& < >`). */
export function escapeHtml(value: string): string {
	return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/** Escape a value for use inside a double-quoted HTML attribute. */
function escapeAttr(value: string): string {
	return escapeHtml(value).replace(/"/g, "&quot;");
}

/** Wrap already-escaped text in a Telegram tag. */
function tag(name: string, escaped: string): string {
	return `<${name}>${escaped}</${name}>`;
}

/** Render the given raw text as inline code (escaped internally). */
function code(raw: string): string {
	return tag("code", escapeHtml(raw));
}

/** Render the given raw text as a preformatted block (escaped internally). */
function pre(raw: string): string {
	return tag("pre", escapeHtml(raw));
}

const PLACEHOLDER_PREFIX = "\u0000ph";
const PLACEHOLDER_SUFFIX = "\u0000";

/** Only http(s) and mailto links are emitted; anything else stays literal. */
function isSafeUrl(url: string): boolean {
	return /^(https?:\/\/|mailto:)/i.test(url);
}

/** Column alignment parsed from a GFM table separator cell. */
type ColumnAlign = "left" | "right" | "center";

/** Split a markdown table row into trimmed cells, honoring escaped `\|`. */
function splitTableRow(line: string): string[] {
	let s = line.trim();
	if (s.startsWith("|")) s = s.slice(1);
	if (s.endsWith("|") && !s.endsWith("\\|")) s = s.slice(0, -1);
	const cells: string[] = [];
	let cur = "";
	for (let i = 0; i < s.length; i++) {
		const ch = s.charAt(i);
		if (ch === "\\" && s[i + 1] === "|") {
			cur += "|";
			i++;
			continue;
		}
		if (ch === "|") {
			cells.push(cur);
			cur = "";
			continue;
		}
		cur += ch;
	}
	cells.push(cur);
	return cells.map((c) => c.trim());
}

/** A line is a candidate table row when it contains an unescaped `|`. */
function looksLikeTableRow(line: string): boolean {
	return /(?:^|[^\\])\|/.test(line);
}

/** A separator row has only dashes/colons/spaces per cell, with at least one dash. */
function isTableSeparator(line: string): boolean {
	if (!looksLikeTableRow(line)) return false;
	const cells = splitTableRow(line);
	return cells.length > 0 && cells.every((c) => /^:?-+:?$/.test(c));
}

/** Derive a column alignment from a separator cell (`:---`, `---:`, `:---:`). */
function parseAlign(cell: string): ColumnAlign {
	const c = cell.trim();
	const left = c.startsWith(":");
	const right = c.endsWith(":");
	if (left && right) return "center";
	if (right) return "right";
	return "left";
}

/** Column count and per-column display widths. Loops, not spreads: a table may have any number of rows. */
function tableLayout(header: string[], body: string[][]): { cols: number; widths: number[] } {
	let cols = header.length;
	for (const row of body) cols = Math.max(cols, row.length);
	const widths = Array.from({ length: cols }, () => 1);
	for (const row of [header, ...body])
		for (let c = 0; c < row.length; c++) widths[c] = Math.max(widths[c] ?? 1, Bun.stringWidth(row[c] ?? ""));
	return { cols, widths };
}

/** Render parsed table parts as an aligned, monospace-friendly plain-text grid. */
function renderTableText(header: string[], aligns: ColumnAlign[], body: string[][], widths: number[]): string {
	const padCell = (value: string, c: number): string => {
		const pad = (widths[c] ?? 1) - Bun.stringWidth(value);
		if (pad <= 0) return value;
		const align = aligns[c] ?? "left";
		if (align === "right") return " ".repeat(pad) + value;
		if (align === "center") {
			const leftPad = Math.floor(pad / 2);
			return " ".repeat(leftPad) + value + " ".repeat(pad - leftPad);
		}
		return value + " ".repeat(pad);
	};
	const renderRow = (row: string[]): string => widths.map((_w, c) => padCell(row[c] ?? "", c)).join(" | ");
	const divider = widths.map((w) => "-".repeat(w)).join("-|-");
	return [renderRow(header), divider, ...body.map(renderRow)].join("\n");
}

/**
 * Records repeat every header label on every row. Past this multiple of the
 * table's own source size (plus one message of slack) the table is posted as
 * its literal source in a monospace block instead, so a long header over many
 * rows cannot multiply into hundreds of messages.
 */
const TABLE_RECORDS_AMPLIFICATION_LIMIT = 8;

/**
 * Render compact tables as a grid and wide tables as readable Telegram-native
 * records. The choice is made from the column widths before any padding, so a
 * wide table never materializes its padded grid.
 */
function renderTelegramTable(header: string[], aligns: ColumnAlign[], body: string[][], source: string): string {
	const { cols, widths } = tableLayout(header, body);
	const gridWidth = widths.reduce((sum, w) => sum + w, 0) + 3 * (cols - 1);
	if (body.length === 0 || gridWidth <= 42) return pre(renderTableText(header, aligns, body, widths));
	const labels = Array.from({ length: cols }, (_v, column) =>
		tag("b", escapeHtml(header[column] || `Column ${column + 1}`)),
	);
	const labelsSize = labels.reduce((sum, label) => sum + label.length + 3, 0);
	if (body.length * labelsSize > TABLE_RECORDS_AMPLIFICATION_LIMIT * source.length + TELEGRAM_MESSAGE_LIMIT)
		return pre(source);
	return body
		.map((row) => labels.map((label, column) => `${label}: ${escapeHtml(row[column] ?? "")}`).join("\n"))
		.join("\n\n");
}

/**
 * Replace GFM tables with stashed monospace `<pre>` blocks. Telegram HTML has no
 * table primitive, so a header row followed by a `|---|` separator is rendered as
 * an aligned plain-text grid (cell content escaped by `pre`).
 */
function convertMarkdownTables(text: string, stash: (html: string) => string): string {
	const lines = text.split("\n");
	const out: string[] = [];
	for (let i = 0; i < lines.length; i++) {
		const headerLine = lines[i] ?? "";
		const separatorLine = lines[i + 1];
		const header = looksLikeTableRow(headerLine) ? splitTableRow(headerLine) : [];
		if (
			separatorLine !== undefined &&
			isTableSeparator(separatorLine) &&
			splitTableRow(separatorLine).length === header.length
		) {
			const aligns = splitTableRow(separatorLine).map(parseAlign);
			const body: string[][] = [];
			let j = i + 2;
			for (; j < lines.length; j++) {
				const row = lines[j] ?? "";
				if (!looksLikeTableRow(row) || isTableSeparator(row)) break;
				body.push(splitTableRow(row));
			}
			out.push(stash(renderTelegramTable(header, aligns, body, lines.slice(i, j).join("\n"))));
			i = j - 1;
			continue;
		}
		out.push(headerLine);
	}
	return out.join("\n");
}

/** Characters a backslash keeps from being syntax (CommonMark: any ASCII punctuation). */
const ESCAPABLE = new Set("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");

/** One line of a fenced code block's opening or closing fence. */
const FENCE_OPEN = /^([ \t]*)(`{3,}|~{3,})(.*)$/;
const FENCE_CLOSE = /^[ \t]*(`{3,}|~{3,})[ \t]*$/;

/**
 * Replace fenced code blocks with stashed `<pre>` blocks. A fence opens on its
 * own line (any indentation, so fences inside list items count) with three or
 * more backticks or tildes, and closes only on a line holding nothing but a run
 * of the same character at least as long. An unclosed fence runs to the end of
 * the message; a fence with no body stays literal.
 */
function convertFences(text: string, stash: (html: string) => string): string {
	const lines = text.split("\n");
	const out: string[] = [];
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i] ?? "";
		const open = FENCE_OPEN.exec(line);
		const indent = open?.[1] ?? "";
		const fence = open?.[2] ?? "";
		if (!open || i + 1 >= lines.length || (fence.startsWith("`") && (open[3] ?? "").includes("`"))) {
			out.push(line);
			continue;
		}
		let end = i + 1;
		for (; end < lines.length; end++) {
			const close = FENCE_CLOSE.exec(lines[end] ?? "")?.[1];
			if (close && close[0] === fence[0] && close.length >= fence.length) break;
		}
		const closed = end < lines.length;
		const body = lines.slice(i + 1, end).map((bodyLine) => {
			let strip = 0;
			while (strip < indent.length && (bodyLine[strip] === " " || bodyLine[strip] === "\t")) strip++;
			return bodyLine.slice(strip);
		});
		const code = body.join("\n") + (closed ? "\n" : "");
		if (code.trim() === "") {
			out.push(line);
			continue;
		}
		out.push(stash(pre(code)));
		i = closed ? end : lines.length;
	}
	return out.join("\n");
}

/** Start offsets of every maximal backtick run, grouped by run length. */
function backtickRuns(text: string): Map<number, number[]> {
	const runs = new Map<number, number[]>();
	for (let i = 0; i < text.length; ) {
		if (text[i] !== "`") {
			i++;
			continue;
		}
		let end = i;
		while (text[end] === "`") end++;
		const list = runs.get(end - i);
		if (list) list.push(i);
		else runs.set(end - i, [i]);
		i = end;
	}
	return runs;
}

/**
 * Stash backslash escapes and code spans in one left-to-right pass, the way
 * CommonMark reads them: an escaped character is not syntax, a backtick run opens
 * a code span closed by the next run of exactly the same length on the same
 * line, and a run with no closer is literal. Linear: each run length keeps a
 * cursor that only moves forward.
 */
function convertInlineCode(text: string, stash: (html: string, raw: string) => string): string {
	const runs = backtickRuns(text);
	const cursors = new Map<number, number>();
	let out = "";
	let lineEnd = -1;
	for (let i = 0; i < text.length; ) {
		const ch = text.charAt(i);
		if (ch === "\\" && ESCAPABLE.has(text.charAt(i + 1))) {
			// The backslash stays visible: in prose it is as often a path or regex
			// (`C:\*name*`, `\.ssh`) as a Markdown escape, so dropping it could lose
			// a character. A link destination still reads the escaped character.
			out += stash(escapeHtml(text.slice(i, i + 2)), text.charAt(i + 1));
			i += 2;
			continue;
		}
		if (ch !== "`") {
			out += ch;
			i++;
			continue;
		}
		let runEnd = i;
		while (text[runEnd] === "`") runEnd++;
		const length = runEnd - i;
		if (lineEnd < i) {
			lineEnd = text.indexOf("\n", i);
			if (lineEnd === -1) lineEnd = text.length;
		}
		const starts = runs.get(length) ?? [];
		let cursor = cursors.get(length) ?? 0;
		while (cursor < starts.length && (starts[cursor] ?? 0) < runEnd) cursor++;
		cursors.set(length, cursor);
		const close = starts[cursor];
		if (close === undefined || close >= lineEnd) {
			out += text.slice(i, runEnd);
			i = runEnd;
			continue;
		}
		let body = text.slice(runEnd, close);
		if (body.length > 2 && body.startsWith(" ") && body.endsWith(" ") && body.trim() !== "") body = body.slice(1, -1);
		out += stash(code(body), text.slice(i, close + length));
		i = close + length;
	}
	return out;
}

/** CommonMark link-destination terminators: whitespace and ASCII control characters (NUL marks placeholders). */
function endsDestination(code: number): boolean {
	return code === 0x20 || code === 0x7f || (code < 0x20 && code !== 0);
}

/**
 * Replace `[label](destination)` with whatever `render` returns. The label is
 * the text after the last `[` before a `]` on the same line; the destination
 * runs from `(` to its balanced `)` and may not contain whitespace. Matching
 * parentheses are paired once up front, so the scan is linear. `render` also
 * receives the construct's whole source (including an image's `!`) so it can
 * keep a link it refuses literal as one unit; unbalanced destinations are not
 * links at all.
 */
function convertLinks(
	text: string,
	render: (label: string, destination: string, source: string, image: boolean) => string,
): string {
	const closing = new Int32Array(text.length).fill(-1);
	const stack: number[] = [];
	for (let i = 0; i < text.length; i++) {
		const ch = text.charAt(i);
		if (endsDestination(text.charCodeAt(i))) stack.length = 0;
		else if (ch === "(") stack.push(i);
		else if (ch === ")" && stack.length > 0) closing[stack.pop() ?? 0] = i;
	}
	let out = "";
	let copied = 0;
	let open = -1;
	for (let i = 0; i < text.length; i++) {
		const ch = text.charAt(i);
		if (ch === "\n") open = -1;
		else if (ch === "[") open = i;
		else if (ch === "]" && open !== -1) {
			const start = open;
			open = -1;
			const end = text[i + 1] === "(" ? (closing[i + 1] ?? -1) : -1;
			if (end === -1 || i === start + 1) continue;
			const first = text[start - 1] === "!" ? start - 1 : start;
			out +=
				text.slice(copied, first) +
				render(text.slice(start + 1, i), text.slice(i + 2, end), text.slice(first, end + 1), first < start);
			copied = end + 1;
			i = end;
		}
	}
	return out + text.slice(copied);
}

/**
 * An emphasis opener may not sit between an ASCII letter or digit and
 * punctuation (`a*(`), nor a closer between punctuation and an ASCII letter or
 * digit (`)*d`): CommonMark's flanking rule. Non-ASCII letters are exempt so
 * Korean or Japanese text that ends a bold span with punctuation and continues
 * without a space (`**설명(예)**은`) still renders.
 */
function emphasisSpan(char: string, count: number, body: string): string {
	const delimiter = char.repeat(count);
	const open = `(?:(?<![A-Za-z0-9])${delimiter}|${delimiter}(?![\\p{P}\\p{S}]))`;
	const close = `(?:(?<![\\p{P}\\p{S}])${delimiter}|${delimiter}(?![A-Za-z0-9]))`;
	return `(?<!${char})${open}(${body})${close}(?!${char})`;
}

const EMPHASIS_PATTERN = [
	emphasisSpan("\\*", 3, "[^*\\s<>](?:[^*\\n<>]*[^*\\s<>])?"),
	emphasisSpan("\\*", 2, "[^*\\s<>](?:(?:[^*\\n<>]|(?<!\\*)\\*(?!\\*))*[^*\\s<>])?"),
	emphasisSpan("\\*", 1, "[^*\\s<>](?:[^*\\n<>]*[^*\\s<>])?"),
	emphasisSpan("~", 2, "[^~\\s<>](?:[^~\\n<>]*[^~\\s<>])?"),
].join("|");

/** `# Title ##` -> `Title`: an ATX header line's content, or undefined when the line is not a header or is empty. */
function headerContent(line: string): string | undefined {
	const match = /^ {0,3}#{1,6}[ \t]+(.*)$/.exec(line);
	if (!match) return undefined;
	let content = (match[1] ?? "").trimEnd();
	let hashes = content.length;
	while (hashes > 0 && content[hashes - 1] === "#") hashes--;
	if (hashes < content.length && (hashes === 0 || content[hashes - 1] === " " || content[hashes - 1] === "\t"))
		content = content.slice(0, hashes).trimEnd();
	return content === "" ? undefined : content;
}

/**
 * Convert a bounded markdown subset into Telegram HTML. Supported: fenced code
 * (backtick or tilde), inline code spans of any backtick length, backslash
 * escapes (the backslash stays visible), `***bold italic***`, `**bold**` (which may hold `*italic*`),
 * `*italic*`, `~~strikethrough~~`, `[text](url)` (safe schemes, balanced
 * parentheses), `#` headers, `>` blockquotes, and GFM tables (rendered as a
 * monospace block or records).
 * Unsupported or malformed markdown (images, link titles, `_underscore_`
 * emphasis, indented code without a fence) is left as escaped literal text,
 * never emitted as unbalanced tags.
 */
export function markdownToTelegramHtml(markdown: string): string {
	const placeholders: string[] = [];
	/** What each placeholder stood for in the source, for link destinations. */
	const sources: string[] = [];
	const placeholderPattern = new RegExp(`${PLACEHOLDER_PREFIX}(\\d+)${PLACEHOLDER_SUFFIX}`, "g");
	const stash = (html: string, raw = ""): string => {
		const token = `${PLACEHOLDER_PREFIX}${placeholders.length}${PLACEHOLDER_SUFFIX}`;
		placeholders.push(html);
		sources.push(raw);
		return token;
	};
	const restorePlain = (value: string): string =>
		value.replace(placeholderPattern, (_m, i: string) => telegramHtmlToPlainText(placeholders[Number(i)] ?? ""));
	const restoreSource = (value: string): string =>
		value.replace(placeholderPattern, (_m, i: string) => sources[Number(i)] ?? "");

	// NUL is reserved for internal placeholders and must never reach Telegram.
	let text = markdown.replaceAll("\u0000", "").replaceAll("\r\n", "\n");

	// 1. Fenced code blocks (protect literal content before any other transform).
	text = convertFences(text, stash);

	// 1b. GFM tables -> aligned monospace <pre> block (no native table primitive).
	text = convertMarkdownTables(text, stash);

	// 2. Backslash escapes and inline code spans.
	text = convertInlineCode(text, stash);

	// 3. Links (destination read from the source before escaping). Images,
	// unsafe schemes and destinations that are not one URL stay literal as a
	// unit, so no later pass reformats part of them.
	text = convertLinks(text, (label, destination, source, image) => {
		const url = restoreSource(destination);
		// Raw control characters already ended the destination; a code span's
		// source can still bring whitespace back.
		if (image || !isSafeUrl(url) || /\s/.test(url))
			return stash(escapeHtml(source).replace(placeholderPattern, (_m, i: string) => placeholders[Number(i)] ?? ""));
		return stash(`<a href="${escapeAttr(url)}">${escapeHtml(restorePlain(label))}</a>`);
	});

	// 4. Escape everything that remains (placeholders contain no escapable chars).
	text = escapeHtml(text);

	// 5. Line-level transforms on escaped text: headers and merged blockquotes.
	// A header or blockquote with no content stays literal rather than emitting
	// an empty tag.
	const lines = text.split("\n");
	const out: string[] = [];
	let quoteLines: string[] = [];
	let quoteBody: string[] = [];
	const flushQuote = () => {
		if (quoteLines.length === 0) return;
		if (quoteBody.some((line) => line.trim() !== "")) out.push(tag("blockquote", quoteBody.join("\n")));
		else out.push(...quoteLines);
		quoteLines = [];
		quoteBody = [];
	};
	for (const line of lines) {
		const quote = /^ {0,3}&gt;[ \t]?(.*)$/.exec(line);
		if (quote) {
			quoteLines.push(line);
			quoteBody.push(quote[1] ?? "");
			continue;
		}
		flushQuote();
		const header = headerContent(line);
		out.push(header === undefined ? line : tag("b", header));
	}
	flushQuote();
	text = out.join("\n");

	// 6. Match outer emphasis first, then its body, never across an emitted tag.
	// This leaves crossing/unbalanced delimiters literal instead of crossing tags.
	// A body starts and ends with a non-space character and its delimiters are
	// not part of a longer run, so `2 * 3 * 4`, a `* item` bullet and the `**/*`
	// in `src/**/*.ts` stay literal instead of losing their characters. A bold
	// body may hold single `*` (so `**a *b* c**` nests); no body holds a `**`.
	// Delimiters also follow CommonMark flanking against ASCII letters and
	// digits, so `a*(b+c)*d` stays literal.
	const emphasis = (value: string): string =>
		value.replace(
			new RegExp(EMPHASIS_PATTERN, "gu"),
			(
				_m,
				both: string | undefined,
				bold: string | undefined,
				italic: string | undefined,
				strike: string | undefined,
			) => {
				if (both !== undefined) return tag("b", tag("i", emphasis(both)));
				return tag(
					bold !== undefined ? "b" : italic !== undefined ? "i" : "s",
					emphasis(bold ?? italic ?? strike ?? ""),
				);
			},
		);
	text = emphasis(text);

	// 7. Restore protected placeholders.
	text = text.replace(placeholderPattern, (_m, i: string) => placeholders[Number(i)] ?? "");

	return text;
}
interface Token {
	value: string;
	/** Tag name if this token opens a tag, else undefined. */
	open?: string;
	/** Exact opening tag text, including attributes, for safe chunk reopening. */
	openTag?: string;
	/** Tag name if this token closes a tag, else undefined. */
	close?: string;
}

/** Tokenize HTML into tags, entities, and Unicode code points (never splits them). */
function tokenize(html: string): Token[] {
	const tokens: Token[] = [];
	let i = 0;
	while (i < html.length) {
		const ch = String.fromCodePoint(html.codePointAt(i) ?? 0);
		if (ch === "<") {
			// A tag ends at the next `>`; a `<` met first means this one is text.
			let end = i + 1;
			while (end < html.length && html[end] !== ">" && html[end] !== "<") end++;
			if (html[end] === ">") {
				const raw = html.slice(i, end + 1);
				const close = /^<\/([a-z-]+)>$/i.exec(raw)?.[1]?.toLowerCase();
				const open = /^<([a-z-]+)(?:\s[^>]*)?>$/i.exec(raw)?.[1]?.toLowerCase();
				const token: Token = { value: raw };
				if (close && ALLOWED_TAGS.has(close)) token.close = close;
				else if (open && ALLOWED_TAGS.has(open)) {
					token.open = open;
					token.openTag = raw;
				}
				tokens.push(token);
				i = end + 1;
				continue;
			}
		}
		if (ch === "&") {
			const end = html.slice(i, i + 11).indexOf(";") + i;
			if (end > i) {
				tokens.push({ value: html.slice(i, end + 1) });
				i = end + 1;
				continue;
			}
		}
		tokens.push({ value: ch });
		i += ch.length;
	}
	return tokens;
}

interface OpenTag {
	name: string;
	tag: string;
}

/** A formatting span whose reopened tags would grow it past this multiple of its own size loses its tags. */
const REOPEN_AMPLIFICATION_LIMIT = 8;

/** Split a finished Telegram HTML message into ordered chunks without splitting tags or entities. */
export function splitTelegramHtml(message: string, max = TELEGRAM_MESSAGE_LIMIT): string[] {
	if (message.length <= max) return telegramHtmlToPlainText(message).length > 0 ? [message] : [];
	const chunks: string[] = [];
	const tokens = tokenize(message);
	const stack: OpenTag[] = [];
	let out = "";
	let chunkHasBody = false;

	const closersFor = (s: OpenTag[]): string =>
		s
			.map((t) => `</${t.name}>`)
			.reverse()
			.join("");
	const openersFor = (s: OpenTag[]): string => s.map((t) => t.tag).join("");
	const flush = (): void => {
		if (chunkHasBody) chunks.push(out + closersFor(stack));
		out = openersFor(stack);
		chunkHasBody = false;
	};

	const updateStackForClose = (name: string): boolean => {
		const idx = stack.findLastIndex((t) => t.name === name);
		if (idx === -1) return false;
		stack.splice(idx, 1);
		return true;
	};

	const pushText = (value: string): void => {
		if (out.length + value.length + closersFor(stack).length > max) flush();
		if (out.length + value.length + closersFor(stack).length > max)
			throw new RangeError("Telegram chunk limit cannot fit a visible text unit");
		out += value;
		chunkHasBody = true;
	};

	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i];
		if (!token?.value) continue;
		if (token.open) {
			const nextStack = [...stack, { name: token.open, tag: token.openTag ?? token.value }];
			let depth = 1;
			let end = i + 1;
			let bodyWidth = 0;
			let bodyLength = 0;
			for (; end < tokens.length; end++) {
				const inner = tokens[end];
				if (inner?.open === token.open) depth++;
				if (inner?.close === token.open && --depth === 0) break;
				if (inner) bodyLength += inner.value.length;
				if (inner && !inner.open && !inner.close) bodyWidth = Math.max(bodyWidth, inner.value.length);
			}
			// Reserve the widest visible unit, not just one UTF-16 code unit. If
			// formatting cannot fit even on a fresh chunk, preserve its text instead.
			// Likewise when reopening it across chunks would multiply its size:
			// a long label under a near-limit href would otherwise fan out into
			// one message per character.
			const overhead = openersFor(nextStack).length + closersFor(nextStack).length;
			const projected =
				overhead + bodyWidth > max
					? Number.POSITIVE_INFINITY
					: Math.ceil(bodyLength / (max - overhead)) * overhead + bodyLength;
			const spanLength = token.value.length + bodyLength + `</${token.open}>`.length;
			if (projected > REOPEN_AMPLIFICATION_LIMIT * spanLength + max) {
				if (token.open === "a") {
					const label = tokens
						.slice(i + 1, end)
						.map((inner) => inner.value)
						.join("");
					const href = /\bhref="([^"]*)"/.exec(token.value)?.[1] ?? "";
					const plain = `${telegramHtmlToPlainText(label)} (${telegramHtmlToPlainText(href)})`;
					// Emit the replacement text in place; rebuilding the token array per
					// degraded anchor would make many long anchors quadratic.
					for (const unit of tokenize(escapeHtml(plain))) pushText(unit.value);
					i = end;
				} else if (end < tokens.length) {
					tokens[end] = { value: "" };
				}
				continue;
			}
			if (out.length + token.value.length + closersFor(nextStack).length + bodyWidth > max) flush();
			out += token.value;
			stack.push({ name: token.open, tag: token.openTag ?? token.value });
			continue;
		}

		if (token.close) {
			const idx = stack.findLastIndex((t) => t.name === token.close);
			if (idx === -1) continue;
			const nextStack = stack.toSpliced(idx, 1);
			if (chunkHasBody && out.length + token.value.length + closersFor(nextStack).length > max) flush();
			if (out.length + token.value.length + closersFor(nextStack).length > max) {
				updateStackForClose(token.close);
				continue;
			}
			out += token.value;
			updateStackForClose(token.close);
			continue;
		}

		pushText(token.value);
	}
	if (chunkHasBody) chunks.push(out + closersFor(stack));
	return chunks;
}

/**
 * The visible text of a Telegram HTML chunk this module produced: tags dropped,
 * entities decoded. Used to resend a chunk as plain text when Telegram refuses
 * its entities, so a formatting defect never costs the message itself.
 */
export function telegramHtmlToPlainText(html: string): string {
	return html
		.replace(/<[^<>]*>/g, "")
		.replace(/&lt;/g, "<")
		.replace(/&gt;/g, ">")
		.replace(/&quot;/g, '"')
		.replace(/&amp;/g, "&");
}
