import type { SlackUserLike } from "./author";
import { resolveSlackDisplayName } from "./author";

/**
 * Outbound mention repair.
 *
 * The persona is told to address people as `<@U…>`, and mostly does - but a
 * model that just read `<@U…>` inside a code span, or that knows someone as
 * "형님"/"bellman", drifts. Measured on the live workspace (2026-09-17, last
 * ~300 bot messages):
 *
 *   `<@U0C2GSKTA6M>`   backticked   → renders literally, nobody is pinged
 *   @U0BT1S5UGS1        bare id      → renders literally
 *   @sionic-gajae       plain handle → renders literally
 *
 * Each of these is unambiguous in intent. Repairing them here, on the way out,
 * is the only place that has both the text and the directory. Anything not
 * clearly a person (`@RequestMapping`, `@OG`, an unknown handle, a mention
 * inside a fenced code block) is left exactly as written: a wrong ping is
 * worse than a missed one.
 */

export interface MentionDirectory {
	/** Every user this adapter has resolved so far; the reverse index is built from it. */
	knownUsers(): Iterable<SlackUserLike>;
}

const USER_ID = /U[A-Z0-9]{8,}/;
/** `<@U…>` or `<@U…|label>` wrapped in one pair of backticks. */
const BACKTICKED_MENTION = /`(<@(U[A-Z0-9]{8,})(?:\|[^>`]*)?>)`/g;
/** `@U…` with no angle brackets: the id leaked as a handle. */
const BARE_ID = /(?<![<\w`@])@(U[A-Z0-9]{8,})\b/g;
/**
 * `@handle` / `@Display Name` (single token; multi-word names are not
 * attempted - "@Yeachan Heo said" is ambiguous where the name ends).
 */
const PLAIN_HANDLE = /(?<![<\w`@/:.])@([A-Za-z\u00C0-\uFFFF][\w.\-\u00C0-\uFFFF]{0,39})(?![\w.\-\u00C0-\uFFFF])/g;
/** Fenced code: never touched; a mention there is code, not speech. */
const FENCE = /```[\s\S]*?```/g;

function normalizeKey(value: string): string {
	return value
		.trim()
		.toLowerCase()
		.replace(/[\s._-]+/g, "");
}

/**
 * Reverse index name/handle → id over users the adapter has actually seen.
 * A name shared by two users is dropped from the index: repairing it would
 * be a guess.
 */
export function buildReverseIndex(users: Iterable<SlackUserLike>): ReadonlyMap<string, string> {
	const index = new Map<string, string>();
	const ambiguous = new Set<string>();
	const add = (name: string | null | undefined, id: string) => {
		if (typeof name !== "string" || name.trim() === "") return;
		const key = normalizeKey(name);
		if (key.length < 2) return;
		const prior = index.get(key);
		if (prior !== undefined && prior !== id) ambiguous.add(key);
		else index.set(key, id);
	};
	for (const user of users) {
		if (!USER_ID.test(user.id)) continue;
		add(user.name, user.id);
		add(user.real_name, user.id);
		add(user.profile?.display_name, user.id);
		add(user.profile?.real_name, user.id);
		add(resolveSlackDisplayName(user), user.id);
	}
	for (const key of ambiguous) index.delete(key);
	return index;
}

/**
 * Repairs mentions in outbound text. Runs before Markdown → mrkdwn so the
 * result is ordinary `<@U…>` syntax the rest of the pipeline already handles.
 */
export function repairMentions(text: string, directory: MentionDirectory): string {
	if (!text.includes("@")) return text;
	// Split out fenced blocks; only prose segments are repaired.
	const segments: string[] = [];
	let last = 0;
	for (const match of text.matchAll(FENCE)) {
		segments.push(repairProse(text.slice(last, match.index), directory));
		segments.push(match[0]);
		last = match.index + match[0].length;
	}
	segments.push(repairProse(text.slice(last), directory));
	return segments.join("");
}

function repairProse(text: string, directory: MentionDirectory): string {
	if (!text.includes("@")) return text;
	let out = text.replace(BACKTICKED_MENTION, (_whole, mention: string) => mention);
	out = out.replace(BARE_ID, (_whole, id: string) => `<@${id}>`);
	if (!/(?<![<\w`@/:.])@[A-Za-z\u00C0-\uFFFF]/.test(out)) return out;
	let index: ReadonlyMap<string, string> | undefined;
	out = out.replace(PLAIN_HANDLE, (whole, handle: string) => {
		index ??= buildReverseIndex(directory.knownUsers());
		const id = index.get(normalizeKey(handle));
		return id ? `<@${id}>` : whole;
	});
	return out;
}
