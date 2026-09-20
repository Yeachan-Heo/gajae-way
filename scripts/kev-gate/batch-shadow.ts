/**
 * Batch replay of REAL traffic through the shadow gate: shipped renderer
 * (inbound-only) vs candidate (the assistant's own last reply inserted in true
 * chronological order, bounded to 300 chars).
 *
 * Why: the live gate scored three unmentioned owner follow-ups would-skip
 * (directed 0.19..0.26) on 2026-09-20 because the state never shows that the
 * assistant just spoke, while `Q_DIRECTED` explicitly asks about follow-ups to
 * the assistant's own last message. Four hand-built fixtures are not evidence;
 * this replays the gateway's own recorded messages with their real context.
 *
 * Read-only against the live database (immutable open); nothing is written.
 *
 *   bun scripts/kev-gate/batch-shadow.ts [limit]
 */
import { Database } from "bun:sqlite";
import {
	ASSISTANT_LABEL,
	type KevShadowInput,
	renderShadowState,
	shadowClass,
	shadowScore,
} from "../../packages/gateway/src/engagement/kev-shadow";

const BASE = process.env.KEV_SHADOW_URL;
const TOKEN = process.env.KEV_SHADOW_TOKEN;
if (!BASE) throw new Error("KEV_SHADOW_URL is required");
const DB_PATH = `${process.env.GAJAEWAY_HOME ?? `${process.env.HOME}/.gajaeway`}/gateway.db`;
const LIMIT = Number(process.argv[2] ?? 60);
const CONTEXT_TURNS = 16;
const CONTEXT_WINDOW_MS = 6 * 60 * 60_000;
/** The assistant turn only counts as context when it is recent enough to be the thing being followed up. */
const ASSISTANT_WINDOW_MS = 30 * 60_000;

const QUESTIONS = [
	"Looking only at NEW MESSAGE: is it a concrete request for help, a bug report, a setup problem, or a specific question that someone still needs to answer?",
	`Looking only at NEW MESSAGE: is it aimed at ${ASSISTANT_LABEL}, expecting ${ASSISTANT_LABEL} to reply or act now? Count short calls, nudges, liveness checks, single-word summons, and follow-ups to ${ASSISTANT_LABEL}'s own last message as yes.`,
	"Looking only at NEW MESSAGE: is it a closing acknowledgement, thanks, or agreement that needs no reply at all, adding no new question and asking for nothing?",
	`Looking only at NEW MESSAGE: is its author answering, explaining, or giving instructions to somebody other than ${ASSISTANT_LABEL}, rather than asking for something?`,
	`Looking only at NEW MESSAGE: is it small talk, a joke, a reaction, or an automated status post addressed to nobody in particular, with nothing for ${ASSISTANT_LABEL} to act on?`,
];

async function judge(state: string): Promise<number[]> {
	for (let attempt = 0; attempt < 3; attempt++) {
		try {
			const res = await fetch(`${BASE.replace(/\/$/, "")}/judge`, {
				method: "POST",
				headers: { "Content-Type": "application/json", ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}) },
				body: JSON.stringify({ state, questions: QUESTIONS.map((instr) => ({ instr, options: ["no", "yes"] })) }),
			});
			if (!res.ok) throw new Error(`judge ${res.status}`);
			const body = (await res.json()) as { probs: number[][] };
			return body.probs.map((p) => p[1] ?? 0);
		} catch (error) {
			if (attempt === 2) throw error;
			await new Promise((resolve) => setTimeout(resolve, 500 * (attempt + 1)));
		}
	}
	throw new Error("unreachable");
}

const db = new Database(DB_PATH, { readonly: true });

interface Row {
	message_id: string;
	origin_key: string;
	origin_ref_json: string;
	body: string;
	engagement_json: string | null;
	received_at: string;
}

const rows = db
	.query<Row, [number]>(
		`SELECT message_id, origin_key, origin_ref_json, body, engagement_json, received_at
		 FROM inbound_messages
		 WHERE origin_key NOT LIKE 'work/%' AND body <> '' AND body NOT LIKE '[reaction]%'
		 ORDER BY received_at DESC LIMIT ?`,
	)
	.all(LIMIT);

function earlierTurns(originKey: string, at: string, messageId: string) {
	const since = new Date(Date.parse(at) - CONTEXT_WINDOW_MS).toISOString();
	return db
		.query<{ message_id: string; author_name: string | null; author_id: string | null; body: string; received_at: string }, [string, string, string, number]>(
			`SELECT message_id, author_name, author_id, body, received_at FROM conversation_context
			 WHERE origin_key = ? AND body NOT LIKE '[reaction]%' AND received_at >= ? AND received_at < ?
			 ORDER BY received_at DESC LIMIT ?`,
		)
		.all(originKey, since, at, CONTEXT_TURNS)
		.filter((row) => row.message_id !== messageId)
		.map((row) => ({ author: row.author_name ?? row.author_id ?? "unknown", body: row.body, at: row.received_at }))
		.reverse();
}

function assistantLast(conversationId: string, at: string) {
	const since = new Date(Date.parse(at) - ASSISTANT_WINDOW_MS).toISOString();
	const row = db
		.query<{ created_at: string; payload_json: string }, [string, string, string]>(
			`SELECT created_at, payload_json FROM deliveries
			 WHERE state = 'confirmed' AND json_extract(payload_json, '$.origin.conversationId') = ?
			 AND json_extract(payload_json, '$.reaction') IS NULL AND created_at >= ? AND created_at < ?
			 ORDER BY created_at DESC LIMIT 1`,
		)
		.get(conversationId, since, at);
	if (!row) return undefined;
	const text = (JSON.parse(row.payload_json) as { text?: unknown }).text;
	if (typeof text !== "string" || !text.trim()) return undefined;
	return { author: ASSISTANT_LABEL, body: text.slice(0, 300), at: row.created_at };
}

type Verdicts = ReturnType<typeof shadowScore>;
interface Sample {
	text: string;
	klass: ReturnType<typeof shadowClass>;
	assistantSpokeLast: boolean;
	shipped: Verdicts;
	candidate: Verdicts;
}

const samples: Sample[] = [];
for (const row of rows) {
	const origin = JSON.parse(row.origin_ref_json) as { kind: string; conversationId: string };
	const engagement = row.engagement_json
		? (JSON.parse(row.engagement_json) as {
				mentioned?: boolean;
				authorIsBot?: boolean;
				authorName?: string;
				channelLabel?: string;
				serverLabel?: string;
				replyTo?: { fromSelf?: boolean };
			})
		: undefined;
	const addressedBy =
		origin.kind === "dm" ? "dm" : engagement?.mentioned ? "mention" : engagement?.replyTo?.fromSelf ? "reply" : undefined;
	const earlier = earlierTurns(row.origin_key, row.received_at, row.message_id);
	const input: KevShadowInput = {
		originKey: row.origin_key,
		text: row.body,
		...(engagement?.authorName ? { authorLabel: engagement.authorName } : {}),
		place:
			[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") || `discord ${origin.kind}`,
		earlier,
		addressed: addressedBy !== undefined,
		...(addressedBy ? { addressedBy } : {}),
		...(engagement?.authorIsBot ? { authorIsBot: true } : {}),
	};
	const assistant = assistantLast(origin.conversationId, row.received_at);
	const merged = assistant ? [...earlier, assistant].sort((a, b) => a.at.localeCompare(b.at)) : earlier;
	const now = Date.parse(row.received_at);
	const shipped = shadowScore(await judge(renderShadowState(input, 6000, now)));
	const candidate = shadowScore(await judge(renderShadowState({ ...input, earlier: merged }, 6000, now)));
	samples.push({
		text: row.body.replace(/\s+/g, " ").slice(0, 60),
		klass: shadowClass(input),
		assistantSpokeLast: merged.at(-1)?.author === ASSISTANT_LABEL,
		shipped,
		candidate,
	});
}

const pct = (n: number, d: number) => (d === 0 ? "  -  " : `${((100 * n) / d).toFixed(0)}%`.padStart(5));
function report(title: string, pick: (s: Sample) => Verdicts, group: Sample[]) {
	const engage = group.filter((s) => pick(s).verdict === "would-engage").length;
	const defer = group.filter((s) => pick(s).verdict === "would-defer").length;
	const skip = group.filter((s) => pick(s).verdict === "would-skip").length;
	const scores = group.map((s) => pick(s).score).sort((a, b) => a - b);
	const median = scores.length ? (scores[Math.floor(scores.length / 2)] ?? 0) : 0;
	console.log(
		`${title.padEnd(34)} n=${String(group.length).padStart(3)}  engage ${pct(engage, group.length)}  defer ${pct(defer, group.length)}  skip ${pct(skip, group.length)}  median ${median.toFixed(3)}`,
	);
}

const groups: Array<[string, Sample[]]> = [
	["machine (bot self-prompts)", samples.filter((s) => s.klass === "machine")],
	["addressed (human)", samples.filter((s) => s.klass === "addressed")],
	["ambient, assistant spoke last", samples.filter((s) => s.klass === "ambient" && s.assistantSpokeLast)],
	["ambient, someone else last", samples.filter((s) => s.klass === "ambient" && !s.assistantSpokeLast)],
];
console.log(`replayed ${samples.length} real messages\n`);
for (const [name, group] of groups) {
	if (group.length === 0) continue;
	report(`SHIPPED   ${name}`, (s) => s.shipped, group);
	report(`CANDIDATE ${name}`, (s) => s.candidate, group);
	console.log("");
}
console.log("flips (ambient, assistant spoke last):");
for (const s of samples.filter((x) => x.klass === "ambient" && x.assistantSpokeLast))
	console.log(
		`  ${s.shipped.verdict.padEnd(13)} -> ${s.candidate.verdict.padEnd(13)} ${s.shipped.score.toFixed(3)}->${s.candidate.score.toFixed(3)}  ${s.text}`,
	);
