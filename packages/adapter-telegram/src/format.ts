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
		if (looksLikeTableRow(headerLine) && separatorLine !== undefined && isTableSeparator(separatorLine)) {
			const header = splitTableRow(headerLine);
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

/**
 * Convert a bounded markdown subset into Telegram HTML. Supported: fenced code,
 * inline code, `**bold**`, `*italic*`, `~~strikethrough~~`, `[text](url)`
 * (safe schemes only), `#` headers, `>` blockquotes, and GFM tables (rendered
 * as a monospace block).
 * Unsupported or malformed markdown is left as escaped literal text, never
 * emitted as unbalanced tags.
 */
export function markdownToTelegramHtml(markdown: string): string {
	const placeholders: string[] = [];
	const placeholderPattern = new RegExp(`${PLACEHOLDER_PREFIX}(\\d+)${PLACEHOLDER_SUFFIX}`, "g");
	const stash = (html: string): string => {
		const token = `${PLACEHOLDER_PREFIX}${placeholders.length}${PLACEHOLDER_SUFFIX}`;
		// A table can contain previously protected code. Resolve it before this
		// fragment is protected in turn, keeping restoration single-pass and literal.
		placeholders.push(
			html.replace(placeholderPattern, (_m, i: string) =>
				escapeHtml(telegramHtmlToPlainText(placeholders[Number(i)] ?? "")),
			),
		);
		return token;
	};
	const restorePlain = (value: string): string =>
		value.replace(placeholderPattern, (_m, i: string) => telegramHtmlToPlainText(placeholders[Number(i)] ?? ""));

	// NUL is reserved for internal placeholders and must never reach Telegram.
	let text = markdown.replaceAll("\u0000", "");

	// 1. Fenced code blocks (protect literal content before any other transform).
	text = text.replace(
		/(^|\n)( {0,3})```[^\n`]*\n([\s\S]*?)(?:```|$)/g,
		(_m, prefix: string, indent: string, body: string) =>
			prefix + stash(pre(indent ? body.replace(new RegExp(`^ {0,${indent.length}}`, "gm"), "") : body)),
	);
	text = text.replace(/```([^`\n]*)```/g, (_m, body: string) => stash(code(body)));

	// 1b. GFM tables -> aligned monospace <pre> block (no native table primitive).
	text = convertMarkdownTables(text, stash);

	// 2. Inline code.
	text = text.replace(/`([^`\n]+)`/g, (_m, body: string) => stash(code(body)));

	// 3. Links (capture raw URL before escaping). Unsafe/malformed links stay literal.
	text = text.replace(/\[([^\]\n]+)\]\(([^)\s]+)\)/g, (whole, label: string, url: string) => {
		url = restorePlain(url);
		if (!isSafeUrl(url)) return whole;
		return stash(`<a href="${escapeAttr(url)}">${escapeHtml(restorePlain(label))}</a>`);
	});

	// 4. Escape everything that remains (placeholders contain no escapable chars).
	text = escapeHtml(text);

	// 5. Line-level transforms on escaped text: headers and merged blockquotes.
	const lines = text.split("\n");
	const out: string[] = [];
	let quoteBuffer: string[] | null = null;
	const flushQuote = () => {
		if (quoteBuffer) {
			out.push(tag("blockquote", quoteBuffer.join("\n")));
			quoteBuffer = null;
		}
	};
	for (const line of lines) {
		const quote = /^&gt;\s?(.*)$/.exec(line);
		if (quote) {
			if (!quoteBuffer) {
				quoteBuffer = [];
			}
			quoteBuffer.push(quote[1] ?? "");
			continue;
		}
		flushQuote();
		const header = /^(#{1,6})\s+(.*)$/.exec(line);
		out.push(header ? tag("b", header[2] ?? "") : line);
	}
	flushQuote();
	text = out.join("\n");

	// 6. Match outer emphasis first, then its body, never across an emitted tag.
	// This leaves crossing/unbalanced delimiters literal instead of crossing tags.
	// A body starts and ends with a non-space character and its delimiters are
	// not part of a longer run, so `2 * 3 * 4`, a `* item` bullet and the `**/*`
	// in `src/**/*.ts` stay literal instead of losing their characters.
	const emphasis = (value: string): string =>
		value.replace(
			/(?<!\*)\*\*([^*\s<>](?:[^*\n<>]*[^*\s<>])?)\*\*(?!\*)|(?<!\*)\*([^*\s<>](?:[^*\n<>]*[^*\s<>])?)\*(?!\*)|(?<!~)~~([^~\s<>](?:[^~\n<>]*[^~\s<>])?)~~(?!~)/g,
			(_m, bold: string | undefined, italic: string | undefined, strike: string | undefined) =>
				tag(bold !== undefined ? "b" : italic !== undefined ? "i" : "s", emphasis(bold ?? italic ?? strike ?? "")),
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
			const end = html.indexOf(">", i);
			if (end !== -1) {
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
			const end = html.indexOf(";", i);
			if (end !== -1 && end - i <= 10) {
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
	let tokens = tokenize(message);
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

	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i];
		if (!token || !token.value) continue;
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
					// concat, not a spread splice: a long label has more tokens than a call takes arguments.
					tokens = tokens.slice(0, i).concat(tokenize(escapeHtml(plain)), tokens.slice(end + 1));
					i--;
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

		if (out.length + token.value.length + closersFor(stack).length > max) flush();
		if (out.length + token.value.length + closersFor(stack).length > max)
			throw new RangeError("Telegram chunk limit cannot fit a visible text unit");
		out += token.value;
		chunkHasBody = true;
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
		.replace(/<[^>]*>/g, "")
		.replace(/&lt;/g, "<")
		.replace(/&gt;/g, ">")
		.replace(/&quot;/g, '"')
		.replace(/&amp;/g, "&");
}
