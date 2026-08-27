/**
 * Author naming for inbound Discord metadata.
 *
 * Discord has three names per account and they are not interchangeable: the
 * per-guild nickname, the account-wide display name, and the handle. Only the
 * first two are what a reader sees next to a message, and the nickname differs
 * per server for the same account. Reporting the handle upstream makes the
 * persona address people by a string nobody in the room recognises.
 *
 * Kept free of any `discord.js` import so the precedence rules stay unit
 * testable without the gateway client's dependency tree.
 */

export type AuthorLike = {
	readonly id: string;
	readonly username?: string;
	/** Account-wide display name; `global_name` on the raw API. */
	readonly globalName?: string | null;
};

export type MemberLike = {
	/**
	 * Raw-API spelling of the guild nickname. Present on
	 * `APIInteractionGuildMember`, which is what an uncached interaction carries.
	 */
	readonly nick?: string | null;
	/** discord.js spelling of the guild nickname. */
	readonly nickname?: string | null;
	/** discord.js resolves this to nickname ?? globalName ?? username. */
	readonly displayName?: string | null;
} | null;

export type AuthorNames = {
	/** The name to address this author by in this surface. */
	readonly displayName?: string;
	/** The raw handle, for identification and logs. */
	readonly handle?: string;
};

function firstNonBlank(candidates: readonly (string | null | undefined)[]): string | undefined {
	for (const candidate of candidates) {
		if (typeof candidate === "string" && candidate.trim() !== "") {
			return candidate;
		}
	}
	return undefined;
}

/**
 * Precedence: guild nickname (`nick` or `nickname`) -> member display name ->
 * global name -> handle.
 *
 * Blank and whitespace-only values are skipped rather than propagated, because
 * Discord returns an empty string for an unset global name.
 */
export function resolveDisplayName(author: AuthorLike | undefined, member?: MemberLike): string | undefined {
	return firstNonBlank([member?.nick, member?.nickname, member?.displayName, author?.globalName, author?.username]);
}

/** Both names at once, so callers never have to recompute the precedence. */
export function resolveAuthorNames(author: AuthorLike | undefined, member?: MemberLike): AuthorNames {
	const displayName = resolveDisplayName(author, member);
	const handle = firstNonBlank([author?.username]);
	return {
		...(displayName ? { displayName } : {}),
		...(handle ? { handle } : {}),
	};
}
