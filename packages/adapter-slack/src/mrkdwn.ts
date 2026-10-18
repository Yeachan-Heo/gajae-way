export const SLACK_MESSAGE_LIMIT = 4000;

const CODE_SEGMENTS =
	/(^[ \t]*```[^\n]*\n[\s\S]*?^[ \t]*```[^\n]*(?:\n|$)|^[ \t]*```[^\n]*(?:\n[\s\S]*)?$|`+[^`\n]*`+)/gm;
const UNICODE_WORD_CHARACTER = /[\p{L}\p{N}_$]/u;
const UNICODE_LETTER = /\p{L}/u;
type AngleOpener = { readonly markup: boolean; quote?: '"' | "'"; linkInside?: boolean; closePosition?: number };

/** Protect Slack links before parsing code spans; code then retains literal markup and language tags. */
export function markdownToMrkdwn(text: string): string {
	const links: string[] = [];
	const markerPrefix = `\u0000SLACKLINK${crypto.randomUUID()}\u0000`;
	const protectedText = text.replace(SLACK_LINK, (link) => {
		const index = links.push(link) - 1;
		return `${markerPrefix}${index}\u0000`;
	});
	if (links.length === 0) {
		const segments = text.split(CODE_SEGMENTS);
		return segments.map((segment, index) => (index % 2 === 1 ? segment : convertText(segment))).join("");
	}
	const linkMarkers = new RegExp(`${markerPrefix}(\\d+)\\u0000`, "g");
	const keepLink = links.map(() => false);
	const scanSegments = protectedText.split(CODE_SEGMENTS);
	const angleOpeners: AngleOpener[] = [];
	let markupDepth = 0;
	const candidates: {
		readonly index: number;
		readonly inCode: boolean;
		readonly markupDepth: number;
		readonly offset: number;
		readonly opener: AngleOpener | undefined;
	}[] = [];
	let baseOffset = 0;
	const scanAngleSegment = (segment: string, offset: number, ignoreBrackets = false, linkFollows = false): void => {
		for (let index = 0; index < segment.length; index++) {
			const character = segment[index];
			if (character === "\n" || character === "\r") {
				angleOpeners.length = 0;
				markupDepth = 0;
				continue;
			}
			if (ignoreBrackets) continue;
			const currentOpener = angleOpeners[angleOpeners.length - 1];
			if (currentOpener?.quote !== undefined) {
				if (character === currentOpener.quote) currentOpener.quote = undefined;
				continue;
			}
			if (currentOpener && (character === '"' || character === "'")) {
				currentOpener.quote = character;
				continue;
			}
			if (character === "<") {
				const next = segment[index + 1];
				const previous = segment[index - 1];
				const previousIsWord = previous !== undefined && UNICODE_WORD_CHARACTER.test(previous);
				const nextIsLetter = next !== undefined && UNICODE_LETTER.test(next);
				const looksLikeMarkup =
					(linkFollows && index === segment.length - 1) ||
					next === "<" ||
					(nextIsLetter && !previousIsWord) ||
					(next !== undefined && "!/?@#:".includes(next));
				angleOpeners.push({ markup: looksLikeMarkup });
				if (looksLikeMarkup) markupDepth++;
			} else if (character === ">") {
				const opener = angleOpeners.pop();
				if (opener) opener.closePosition = offset + index;
				if (opener?.markup) markupDepth--;
			} else if (markupDepth === 0 && ";,.!?".includes(character)) {
				for (let openerIndex = angleOpeners.length - 1; openerIndex >= 0; openerIndex--) {
					const opener = angleOpeners[openerIndex];
					if (!opener?.markup && !opener?.linkInside) angleOpeners.splice(openerIndex, 1);
				}
			}
		}
	};
	// Scan code/newline boundaries too; code brackets stay literal but cannot bridge an angle construct across lines.
	for (let segmentIndex = 0; segmentIndex < scanSegments.length; segmentIndex++) {
		const segment = scanSegments[segmentIndex] ?? "";
		const inCode = segmentIndex % 2 === 1;
		linkMarkers.lastIndex = 0;
		let cursor = 0;
		for (const match of segment.matchAll(linkMarkers)) {
			scanAngleSegment(segment.slice(cursor, match.index), baseOffset + cursor, inCode, !inCode);
			const index = Number(match[1]);
			const opener = angleOpeners[angleOpeners.length - 1];
			if (!inCode && opener && !opener.markup) opener.linkInside = true;
			candidates.push({ index, inCode, markupDepth, offset: baseOffset + match.index, opener });
			cursor = match.index + match[0].length;
		}
		scanAngleSegment(segment.slice(cursor), baseOffset + cursor, inCode);
		baseOffset += segment.length;
	}
	for (const candidate of candidates) {
		keepLink[candidate.index] =
			candidate.inCode ||
			(candidate.markupDepth === 0 &&
				!(candidate.opener?.closePosition !== undefined && candidate.opener.closePosition > candidate.offset));
	}
	linkMarkers.lastIndex = 0;
	const textForConversion = protectedText.replace(linkMarkers, (marker, index: string) =>
		keepLink[Number(index)] ? marker : (links[Number(index)] ?? marker),
	);
	const escapeProtectedLinks = (value: string): string => {
		linkMarkers.lastIndex = 0;
		return value.replace(linkMarkers, (marker, index: string) => escapeHtml(links[Number(index)] ?? marker));
	};
	const segments = textForConversion.split(CODE_SEGMENTS);
	const converted = segments
		.map((segment, index) => (index % 2 === 1 ? segment : convertText(segment, escapeProtectedLinks)))
		.join("");
	linkMarkers.lastIndex = 0;
	return converted.replace(linkMarkers, (_marker, index: string) => links[Number(index)] ?? _marker);
}

/**
 * Slack's own angle-bracket syntax, which must survive HTML escaping: user
 * mentions, channel links, and broadcast keywords. Everything else in angle
 * brackets is text and is escaped. Until this existed every `<@U\u2026>` the
 * persona wrote went out as `&lt;@U\u2026&gt;` and pinged nobody (2026-09-17).
 */
const SLACK_ENTITY =
	/<(?:@[UW][A-Z0-9]+(?:\|[^>\n]*)?|#C[A-Z0-9]+(?:\|[^>\n]*)?|!(?:here|channel|everyone)(?:\|[^>\n]*)?|!subteam\^[A-Z0-9]+(?:\|[^>\n]*)?)>/g;

/** Slack URLs and labels are protected before code-span and Markdown parsing. */
const SLACK_LINK = /<(?:https?:\/\/|mailto:)[^\s<>|]+(?:\|[^<>\n]*)?>/gi;

function escapeKeepingEntities(text: string): string {
	const parts: string[] = [];
	let last = 0;
	for (const match of text.matchAll(SLACK_ENTITY)) {
		parts.push(escapeHtml(text.slice(last, match.index)), match[0]);
		last = match.index + match[0].length;
	}
	parts.push(escapeHtml(text.slice(last)));
	return parts.join("");
}

function escapeHtml(text: string): string {
	return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function convertText(text: string, escapeProtectedLinks?: (value: string) => string): string {
	const escaped = escapeKeepingEntities(text);
	// One pass prevents the bold tokens we emit from being converted again as italics.
	return escaped.replace(
		/\[([^\]\n]+)\]\(([^)\n]+)\)|^(#{1,6})[ \t]+(.+)$|^([ \t]*)[-*][ \t]+|\*\*([^\n]+?)\*\*|__([^\n]+?)__|~~([^\n]+?)~~|\*([^*\n]+)\*|_([^_\n]+)_/gm,
		(
			_match,
			label: string | undefined,
			url: string,
			heading: string | undefined,
			title: string,
			indent: string | undefined,
			bold: string | undefined,
			underBold: string | undefined,
			strike: string | undefined,
			italic: string | undefined,
			underItalic: string | undefined,
		) => {
			if (label !== undefined)
				return `<${escapeProtectedLinks?.(url) ?? url}|${escapeProtectedLinks?.(label) ?? label}>`;
			if (heading !== undefined) return `*${title}*`;
			if (indent !== undefined) return `${indent}• `;
			if (bold !== undefined || underBold !== undefined) return `*${bold ?? underBold}*`;
			if (strike !== undefined) return `~${strike}~`;
			return `_${italic ?? underItalic}_`;
		},
	);
}

type Fence = { readonly start: number; readonly end: number; readonly opener: string; readonly open: boolean };

/** Synthetic fence closers/openers count toward Slack's limit, not just the source text. */
export function chunkSlackMessage(text: string): string[] {
	if (text.length <= SLACK_MESSAGE_LIMIT) return [text];
	const fences: Fence[] = [];
	let open = false;
	for (const match of text.matchAll(/^[ \t]*```[^\n]*(?:\n|$)/gm)) {
		open = !open;
		fences.push({ start: match.index, end: match.index + match[0].length, opener: match[0].trimEnd(), open });
	}
	const stateAt = (position: number): string | undefined => {
		let opener: string | undefined;
		for (const fence of fences) {
			if (fence.end > position) break;
			opener = fence.open ? fence.opener : undefined;
		}
		return opener;
	};
	const chunks: string[] = [];
	let offset = 0;
	let prefix = "";
	while (offset < text.length) {
		const budget = SLACK_MESSAGE_LIMIT - prefix.length;
		if (text.length - offset <= budget) {
			chunks.push(prefix + text.slice(offset));
			break;
		}
		let end = offset + budget;
		// Reserve a closer before selecting a boundary, then avoid cutting a delimiter.
		if (fences.length > 0) end = offset + budget - 4;
		const crossing = fences.find((fence) => fence.start < end && fence.end > end);
		if (crossing && crossing.start > offset) end = crossing.start;
		const newline = text.lastIndexOf("\n", end - 1) + 1;
		let preferred = 0;
		for (let candidate = newline; candidate > offset; candidate = text.lastIndexOf("\n", candidate - 2) + 1) {
			if (!stateAt(candidate)) {
				preferred = candidate;
				break;
			}
		}
		if (preferred > offset) end = preferred;
		else if (newline > offset) end = newline;
		// Avoid cutting a surrogate pair at a forced hard break.
		if (end > offset && /[\uD800-\uDBFF]/.test(text[end - 1] as string) && /[\uDC00-\uDFFF]/.test(text[end] as string))
			end--;
		const opener = stateAt(end);
		chunks.push(prefix + text.slice(offset, end) + (opener ? "\n```" : ""));
		// Reopened fences need no language tag; omitting it also bounds hostile tags.
		prefix = opener ? "```\n" : "";
		offset = end;
	}
	return chunks;
}
