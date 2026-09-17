export interface SlackUserLike {
	readonly id: string;
	readonly name?: string | null;
	readonly real_name?: string | null;
	readonly is_bot?: boolean;
	readonly profile?: { readonly display_name?: string | null; readonly real_name?: string | null } | null;
}

export interface SlackConversationLike {
	readonly id: string;
	readonly name?: string | null;
	readonly is_im?: boolean;
	readonly is_mpim?: boolean;
}

/** Blank profile fields mean unset, not a name that should hide a useful fallback. */
export function resolveSlackDisplayName(user: SlackUserLike | undefined): string | undefined {
	return [user?.profile?.display_name, user?.profile?.real_name, user?.real_name, user?.name].find(
		(name): name is string => typeof name === "string" && name.trim() !== "",
	);
}

type Entry = { readonly value: SlackUserLike | SlackConversationLike | undefined; readonly expires: number };

/** A shared LRU bounds both caches; negative entries expire so transient API failures recover. */
export class SlackDirectory {
	private readonly entries = new Map<string, Entry>();
	private readonly pending = new Map<string, Promise<SlackUserLike | SlackConversationLike | undefined>>();

	constructor(
		private readonly api: {
			usersInfo(user: string): Promise<SlackUserLike>;
			conversationsInfo(channel: string): Promise<SlackConversationLike>;
		},
		private readonly capacity = 512,
		private readonly now: () => number = Date.now,
	) {
		if (!Number.isInteger(capacity) || capacity < 1)
			throw new Error("Slack directory capacity must be a positive integer");
	}

	user(id: string): Promise<SlackUserLike | undefined> {
		return this.load(`user:${id}`, () => this.api.usersInfo(id)) as Promise<SlackUserLike | undefined>;
	}

	conversation(id: string): Promise<SlackConversationLike | undefined> {
		return this.load(`channel:${id}`, () => this.api.conversationsInfo(id)) as Promise<
			SlackConversationLike | undefined
		>;
	}

	userName(id: string): string | undefined {
		return resolveSlackDisplayName(this.get(`user:${id}`)?.value as SlackUserLike | undefined);
	}

	userHandle(id: string): string | undefined {
		return (this.get(`user:${id}`)?.value as SlackUserLike | undefined)?.name ?? undefined;
	}

	channelName(id: string): string | undefined {
		const channel = this.get(`channel:${id}`)?.value as SlackConversationLike | undefined;
		if (id.startsWith("D") || channel?.is_im) return undefined;
		return channel?.name?.trim() ? channel.name : undefined;
	}

	prime(user: SlackUserLike): void {
		this.set(`user:${user.id}`, { value: user, expires: Number.POSITIVE_INFINITY });
	}

	private get(key: string): Entry | undefined {
		const entry = this.entries.get(key);
		if (!entry) return undefined;
		this.entries.delete(key);
		if (entry.expires <= this.now()) return undefined;
		this.entries.set(key, entry);
		return entry;
	}

	private set(key: string, entry: Entry): void {
		this.entries.delete(key);
		this.entries.set(key, entry);
		while (this.entries.size > this.capacity) this.entries.delete(this.entries.keys().next().value as string);
	}

	private async load(
		key: string,
		fetcher: () => Promise<SlackUserLike | SlackConversationLike>,
	): Promise<SlackUserLike | SlackConversationLike | undefined> {
		const cached = this.get(key);
		if (cached) return cached.value;
		const pending = this.pending.get(key);
		if (pending) return pending;
		const request = Promise.resolve()
			.then(fetcher)
			.catch(() => undefined)
			.then((value) => {
				// A payload primed during an in-flight request is fresher than its result.
				const primed = this.get(key);
				if (primed) return primed.value;
				this.set(key, { value, expires: value === undefined ? this.now() + 60_000 : Number.POSITIVE_INFINITY });
				return value;
			})
			.finally(() => this.pending.delete(key));
		this.pending.set(key, request);
		return request;
	}
}
