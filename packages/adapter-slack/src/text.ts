export interface SlackNameResolver {
	userName(userId: string): string | undefined;
}

export function mentionedUserIds(text: string): readonly string[] {
	return [...new Set([...text.matchAll(/<@([UW][A-Za-z0-9]+)(?:\|[^<>]*)?>/g)].map((match) => match[1] as string))];
}

/** Decode entities last so escaped user text cannot become an active Slack token. */
export function normalizeSlackText(text: string, names?: SlackNameResolver): string {
	return text
		.replace(/<([^<>]+)>/g, (raw: string, token: string) => {
			const separator = token.indexOf("|");
			const target = separator < 0 ? token : token.slice(0, separator);
			const label = separator < 0 ? undefined : token.slice(separator + 1);
			if (/^@[UW][A-Za-z0-9]+$/.test(target)) {
				const id = target.slice(1);
				return `@${names?.userName(id) ?? label ?? id}`;
			}
			if (target.startsWith("#")) return `#${label ?? target.slice(1)}`;
			if (/^https?:\/\//.test(target)) return label === undefined ? target : `${label} (${target})`;
			if (target.startsWith("mailto:")) return label ?? target.slice(7);
			if (/^!(here|channel|everyone)$/.test(target)) return `@${target.slice(1)}`;
			if (target.startsWith("!")) return label ?? raw;
			return raw;
		})
		.replace(/&(amp|lt|gt);/g, (_raw, entity: string) => ({ amp: "&", lt: "<", gt: ">" })[entity] as string);
}
