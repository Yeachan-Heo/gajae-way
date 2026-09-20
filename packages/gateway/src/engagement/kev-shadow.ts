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
 * Output is one structured line per probe, greppable out of the journal:
 *   kev-shadow origin=<key> help=.. ack=.. isAnswer=.. chatter=.. score=.. verdict=..
 */

const URL_ENV = "KEV_SHADOW_URL";
const TOKEN_ENV = "KEV_SHADOW_TOKEN";
const TIMEOUT_MS = Number(process.env.KEV_SHADOW_TIMEOUT_MS ?? 5000);

// Thresholds mirror the offline harness so shadow verdicts are comparable to it.
const ENGAGE_AT = Number(process.env.KEV_SHADOW_ENGAGE_AT ?? 0.6);
const SKIP_UNDER = Number(process.env.KEV_SHADOW_SKIP_UNDER ?? 0.25);

const Q_HELP =
	"Looking only at NEW MESSAGE: is it a concrete request for help, a bug report, a setup problem, or a specific question that someone still needs to answer?";
const Q_ACK =
	"Looking only at NEW MESSAGE: is it just a short acknowledgement, thanks, or agreement, adding no new question?";
const Q_IS_ANSWER =
	"Looking only at NEW MESSAGE: is its author answering, explaining, or giving instructions to someone else, rather than asking for something?";
const Q_CHATTER =
	"Looking only at NEW MESSAGE: is it a greeting, a joke, a reaction, small talk, or an automated status post, with no question and nothing to act on?";

export interface KevShadowInput {
	readonly originKey: string;
	readonly text: string;
	/** Earlier turns, oldest first, already rendered as "who: text". */
	readonly earlier?: readonly string[];
	readonly authorLabel?: string;
}

export function kevShadowEnabled(): boolean {
	return Boolean(process.env[URL_ENV]);
}

/**
 * The model scores a span it can point at; asking about "the last message" of a
 * chat blob read a plain product question as chatter at 0.79 in the offline
 * harness. Isolating it under its own header fixed that.
 */
export function renderShadowState(input: KevShadowInput, maxChars = 3000): string {
	const newMessage = `${input.authorLabel ?? "user"}: ${input.text.replace(/\s+/g, " ").slice(0, 500)}`;
	const earlier = (input.earlier ?? []).join("\n") || "(no earlier messages)";
	const budget = Math.max(maxChars - newMessage.length - 64, 0);
	const context = earlier.length > budget ? earlier.slice(earlier.length - budget) : earlier;
	return `EARLIER CONTEXT:\n${context}\n\nNEW MESSAGE:\n${newMessage}`;
}

/**
 * `ack` and `isAnswer` both presuppose a prior turn. With no earlier context the
 * model still scored isAnswer 0.73 on a standalone product question, so those
 * vetoes are gated on the fact rather than trusted unconditionally.
 */
export function shadowScore(
	probs: readonly number[],
	hasContext: boolean,
): { help: number; ack: number; isAnswer: number; chatter: number; score: number; verdict: string } {
	const [help = 0, ack = 0, isAnswer = 0, chatter = 0] = probs;
	const veto = hasContext ? Math.max(ack, isAnswer, chatter) : chatter;
	const score = help * (1 - Math.max(0, veto - 0.5) * 2);
	const verdict = score < SKIP_UNDER ? "would-skip" : score >= ENGAGE_AT ? "would-engage" : "would-defer";
	return { help, ack, isAnswer, chatter, score, verdict };
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
				questions: [Q_HELP, Q_ACK, Q_IS_ANSWER, Q_CHATTER].map((instr) => ({
					instr,
					options: ["no", "yes"],
				})),
			}),
			signal: controller.signal,
		});
		if (!res.ok) return null;
		const body = (await res.json()) as { probs?: number[][] };
		if (!Array.isArray(body.probs) || body.probs.length !== 4) return null;
		return body.probs.map((p) => (Array.isArray(p) && typeof p[1] === "number" ? p[1] : 0));
	} catch {
		return null;
	} finally {
		clearTimeout(timer);
	}
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
	const s = shadowScore(probs, (input.earlier?.length ?? 0) > 0);
	const f = (n: number) => n.toFixed(4);
	console.error(
		`kev-shadow origin=${input.originKey} help=${f(s.help)} ack=${f(s.ack)} isAnswer=${f(s.isAnswer)} ` +
			`chatter=${f(s.chatter)} score=${f(s.score)} verdict=${s.verdict} ms=${Date.now() - started}`,
	);
}
