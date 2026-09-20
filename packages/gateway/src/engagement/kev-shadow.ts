/**
 * Shadow measurement for a would-be value gate on engaged turns.
 *
 * `decideEngagement` answers authority: may this author open a turn here. It
 * does not answer value: is this message worth a turn at all. Archived sweep
 * records ended in NO_REPLY 495 times out of 505, so the value question is
 * worth measuring — but measuring is all this file does.
 *
 * Three properties make it safe to ship on by default being OFF:
 *
 *   1. It never returns a decision. Nothing here feeds `engaged`.
 *   2. It never blocks the turn. The probe is fire-and-forget; awaiting a
 *      ~500 ms model call on every inbound message would itself be the
 *      behaviour change this shadow exists to avoid.
 *   3. It is disabled unless `KEV_SHADOW_URL` is set, so a deploy with no
 *      configuration is a no-op.
 *
 * Output is one structured line per probe, greppable out of the gateway log:
 *   kev-shadow origin=<key> help=.. directed=.. ack=.. isAnswer=.. chatter=..
 *   score=.. verdict=.. addressed=.. ctx=.. ms=..
 */

const URL_ENV = "KEV_SHADOW_URL";
const TOKEN_ENV = "KEV_SHADOW_TOKEN";
const TIMEOUT_MS = Number(process.env.KEV_SHADOW_TIMEOUT_MS ?? 5000);

// Fitted on this deployment's traffic (24 real messages, 12 addressed to the bot
// in #dev-main and 12 ambient community messages, each judged with 16 turns of
// its own real history): help on answer-me messages ran 0.662..0.820, on ambient
// 0.024..0.391. Any cut in between separates them completely; the band keeps a
// defer zone rather than pretending the edge is sharp.
const ENGAGE_AT = Number(process.env.KEV_SHADOW_ENGAGE_AT ?? 0.6);
const SKIP_UNDER = Number(process.env.KEV_SHADOW_SKIP_UNDER ?? 0.45);
// A closing acknowledgement is the one veto worth keeping, and it bites earlier
// than a neutral 0.5: the live gate reads "ㅇㅇ 수고" at ack≈0.53 while still
// calling it directed at 0.98, so the bite starts at 0.35 and saturates at 0.7.
const ACK_AT = Number(process.env.KEV_SHADOW_ACK_AT ?? 0.35);
const ACK_FULL = Number(process.env.KEV_SHADOW_ACK_FULL ?? 0.7);

/**
 * The name the questions use for the assistant. The transcript itself is inbound
 * only (#243), so the assistant appears in the framing lines — `ADDRESSED`,
 * `LAST SPEAKER` — rather than as walls of its own text. Measured against the
 * live gate, a bare "야" summons scores directed 0.989 with inbound-only context
 * and 0.992 with the last reply appended: the referent costs nothing to withhold.
 */
export const ASSISTANT_LABEL = "ASSISTANT";

const Q_HELP =
	"Looking only at NEW MESSAGE: is it a concrete request for help, a bug report, a setup problem, or a specific question that someone still needs to answer?";
const Q_DIRECTED = `Looking only at NEW MESSAGE: is it aimed at ${ASSISTANT_LABEL}, expecting ${ASSISTANT_LABEL} to reply or act now? Count short calls, nudges, liveness checks, single-word summons, and follow-ups to ${ASSISTANT_LABEL}'s own last message as yes.`;
const Q_ACK =
	"Looking only at NEW MESSAGE: is it a closing acknowledgement, thanks, or agreement that needs no reply at all, adding no new question and asking for nothing?";
const Q_IS_ANSWER = `Looking only at NEW MESSAGE: is its author answering, explaining, or giving instructions to somebody other than ${ASSISTANT_LABEL}, rather than asking for something?`;
const Q_CHATTER = `Looking only at NEW MESSAGE: is it small talk, a joke, a reaction, or an automated status post addressed to nobody in particular, with nothing for ${ASSISTANT_LABEL} to act on?`;

const QUESTIONS = [Q_HELP, Q_DIRECTED, Q_ACK, Q_IS_ANSWER, Q_CHATTER];

export interface ShadowTurn {
	/** Display name of the speaker. */
	readonly author: string;
	readonly body: string;
	/** ISO timestamp, used only to render an age relative to now. */
	readonly at?: string;
}

export interface KevShadowInput {
	readonly originKey: string;
	readonly text: string;
	/** Conversation so far, oldest first. Inbound messages only; see `ASSISTANT_LABEL`. */
	readonly earlier?: readonly ShadowTurn[];
	readonly authorLabel?: string;
	/** Human-readable place, e.g. `direct message` or `group channel #dev-main`. */
	readonly place?: string;
	/**
	 * The message named this bot: an @mention, a reply to one of its messages, or
	 * a DM. Recorded, never acted on. The first live hour scored a direct owner
	 * question at help=0.2387 (would-skip), so the shadow has to separate
	 * addressed traffic from ambient traffic before any promotion argument holds.
	 */
	readonly addressed?: boolean;
	/** Why it counts as addressed, for the log line only. */
	readonly addressedBy?: "dm" | "mention" | "reply";
	/**
	 * The author is another bot. Cron follow-up posts name this bot and would
	 * therefore count as addressed, but they are machine self-prompts: measured
	 * over 20 of them the judge scored 0.182..0.252, so a promoted gate would
	 * skip EVERY scheduled sweep. They must be a separate population, never a
	 * silent part of the addressed one.
	 */
	readonly authorIsBot?: boolean;
}

export function kevShadowEnabled(): boolean {
	return Boolean(process.env[URL_ENV]);
}

function age(at: string | undefined, now: number): string {
	if (!at) return "";
	const ms = now - Date.parse(at);
	if (!Number.isFinite(ms) || ms < 0) return "";
	const minutes = Math.round(ms / 60000);
	if (minutes < 1) return "[just now] ";
	if (minutes < 60) return `[${minutes}m ago] `;
	const hours = Math.round(minutes / 60);
	return hours < 48 ? `[${hours}h ago] ` : `[${Math.round(hours / 24)}d ago] `;
}

/**
 * The model scores a span it can point at; asking about "the last message" of a
 * chat blob read a plain product question as chatter at 0.79 in the offline
 * harness. Isolating it under its own header fixed that.
 *
 * Everything above `NEW MESSAGE` exists so `directed` has something to resolve
 * against: who is speaking, where, whether the assistant just spoke, and whether
 * this message named it. A bare message string cannot distinguish "야" shouted at
 * a channel from "야" typed into the assistant's DM.
 */
export function renderShadowState(input: KevShadowInput, maxChars = 6000, now = Date.now()): string {
	const newMessage = `${input.authorLabel ?? "user"}: ${input.text.replace(/\s+/g, " ").slice(0, 1000)}`;
	const turns = (input.earlier ?? [])
		.filter((turn) => turn.body.trim().length > 0)
		.map((turn) => `${age(turn.at, now)}${turn.author}: ${turn.body.replace(/\s+/g, " ").slice(0, 500)}`);
	const facts = [
		`PLACE: ${input.place ?? "unknown conversation"}`,
		`ADDRESSED: ${
			input.addressed
				? `yes — ${
						input.addressedBy === "dm"
							? `a direct message to ${ASSISTANT_LABEL}`
							: input.addressedBy === "reply"
								? `a reply to ${ASSISTANT_LABEL}'s own message`
								: `it names ${ASSISTANT_LABEL}`
					}`
				: `no — ${ASSISTANT_LABEL} was not named`
		}`,
		`LAST SPEAKER BEFORE THIS: ${input.earlier?.at(-1)?.author ?? "(nobody — first message)"}`,
	].join("\n");
	const header = `${facts}\n\nCONVERSATION SO FAR (oldest first; ${ASSISTANT_LABEL} is the assistant being judged):\n`;
	const budget = Math.max(maxChars - newMessage.length - header.length - 64, 0);
	let body = turns.join("\n") || "(no earlier messages)";
	if (body.length > budget) body = `(older turns omitted)\n${body.slice(body.length - budget)}`;
	return `${header}${body}\n\nNEW MESSAGE (judge only this one):\n${newMessage}`;
}

export interface ShadowVerdict {
	help: number;
	directed: number;
	ack: number;
	isAnswer: number;
	chatter: number;
	score: number;
	verdict: string;
}

/**
 * Demand is the stronger of `help` and `directed`; the one veto is a closing
 * `ack`. `isAnswer` and `chatter` are recorded as diagnostics only.
 *
 * Both halves of that are measurements, not taste.
 *
 * Dropping `isAnswer`/`chatter` as vetoes: they existed to rescue a context-free
 * probe where `help` was too weak to separate anything. Judged with real history,
 * `help` separates the populations on its own (0.662..0.820 answer-me vs
 * 0.024..0.391 ambient across 24 real messages) and the vetoes only do damage —
 * they fire on the prior turns they were given and crushed true positives to
 * 0.083, losing 4 of 24.
 *
 * Adding `directed`: `help` is still the wrong question for traffic aimed at the
 * assistant. A live owner ping ("야", "살아있는거맞냐") carries no help signal at
 * all — the running gate scores it help≈0.32..0.36 — and the first live hour
 * judged every one of them would-skip. `directed` scores those at 0.98, which is
 * what authority already implied by admitting the turn.
 *
 * Keeping `ack`: a closing thanks is the one message that is genuinely directed
 * at the assistant and still needs nothing. It bites from `ACK_AT`, because the
 * gate's read of a real one is weak (0.53) next to its directed score (0.98).
 */
export function shadowScore(probs: readonly number[]): ShadowVerdict {
	const [help = 0, directed = 0, ack = 0, isAnswer = 0, chatter = 0] = probs;
	const bite = Math.max(0, 1 - Math.max(0, ack - ACK_AT) / Math.max(ACK_FULL - ACK_AT, 1e-9));
	const score = Math.max(help, directed) * bite;
	const verdict = score < SKIP_UNDER ? "would-skip" : score >= ENGAGE_AT ? "would-engage" : "would-defer";
	return { help, directed, ack, isAnswer, chatter, score, verdict };
}

async function probe(state: string): Promise<number[] | null> {
	const base = process.env[URL_ENV];
	if (!base) return null;
	const token = process.env[TOKEN_ENV];
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
	try {
		const res = await fetch(`${base.replace(/\/$/, "")}/judge`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				...(token ? { Authorization: `Bearer ${token}` } : {}),
			},
			body: JSON.stringify({
				state,
				questions: QUESTIONS.map((instr) => ({ instr, options: ["no", "yes"] })),
			}),
			signal: controller.signal,
		});
		if (!res.ok) return null;
		const body = (await res.json()) as { probs?: number[][] };
		if (!Array.isArray(body.probs) || body.probs.length !== QUESTIONS.length) return null;
		return body.probs.map((p) => (Array.isArray(p) && typeof p[1] === "number" ? p[1] : 0));
	} catch {
		return null;
	} finally {
		clearTimeout(timer);
	}
}

/**
 * The three populations behave differently enough that pooling them hides the
 * only question that matters at promotion time. Measured with 16 turns of real
 * history: `machine` 0.182..0.252, `addressed` (human) 0.362..0.915 median
 * 0.692, `ambient` 0.011..0.781 median 0.137.
 */
export function shadowClass(input: KevShadowInput): "machine" | "addressed" | "ambient" {
	if (input.authorIsBot) return "machine";
	return input.addressed ? "addressed" : "ambient";
}

/**
 * Fire-and-forget. Callers use `void recordKevShadow(...)` and never await:
 * the turn must not wait on this, and a failed probe must not surface.
 */
export async function recordKevShadow(input: KevShadowInput): Promise<void> {
	if (!kevShadowEnabled() || !input.text.trim()) return;
	const started = Date.now();
	const probs = await probe(renderShadowState(input));
	if (!probs) return;
	const s = shadowScore(probs);
	const f = (n: number) => n.toFixed(4);
	console.error(
		`kev-shadow origin=${input.originKey} help=${f(s.help)} directed=${f(s.directed)} ack=${f(s.ack)} ` +
			`isAnswer=${f(s.isAnswer)} chatter=${f(s.chatter)} score=${f(s.score)} verdict=${s.verdict} ` +
			`addressed=${input.addressed ? (input.addressedBy ?? "1") : "0"} ctx=${input.earlier?.length ?? 0} ` +
			`class=${shadowClass(input)} ms=${Date.now() - started}`,
	);
}
