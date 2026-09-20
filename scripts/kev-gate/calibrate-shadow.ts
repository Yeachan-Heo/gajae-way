/**
 * Calibration harness for the kev shadow value gate.
 *
 * Replays labelled cases through the live gate and prints old-design vs
 * new-design verdicts side by side. Read `KEV_SHADOW_URL`/`KEV_SHADOW_TOKEN`
 * from the environment; it never writes anything.
 *
 *   bun scripts/kev-gate/calibrate-shadow.ts
 */
import {
	ASSISTANT_LABEL,
	type KevShadowInput,
	renderShadowState,
	shadowScore,
} from "../../packages/gateway/src/engagement/kev-shadow";

const BASE = process.env.KEV_SHADOW_URL;
const TOKEN = process.env.KEV_SHADOW_TOKEN;
if (!BASE) throw new Error("KEV_SHADOW_URL is required");

const OLD_QUESTIONS = [
	"Looking only at NEW MESSAGE: is it a concrete request for help, a bug report, a setup problem, or a specific question that someone still needs to answer?",
	"Looking only at NEW MESSAGE: is it just a short acknowledgement, thanks, or agreement, adding no new question?",
	"Looking only at NEW MESSAGE: is its author answering, explaining, or giving instructions to someone else, rather than asking for something?",
	"Looking only at NEW MESSAGE: is it a greeting, a joke, a reaction, small talk, or an automated status post, with no question and nothing to act on?",
];
const NEW_QUESTIONS = [
	"Looking only at NEW MESSAGE: is it a concrete request for help, a bug report, a setup problem, or a specific question that someone still needs to answer?",
	`Looking only at NEW MESSAGE: is it aimed at ${ASSISTANT_LABEL}, expecting ${ASSISTANT_LABEL} to reply or act now? Count short calls, nudges, liveness checks, single-word summons, and follow-ups to ${ASSISTANT_LABEL}'s own last message as yes.`,
	"Looking only at NEW MESSAGE: is it a closing acknowledgement, thanks, or agreement that needs no reply at all, adding no new question and asking for nothing?",
	`Looking only at NEW MESSAGE: is its author answering, explaining, or giving instructions to somebody other than ${ASSISTANT_LABEL}, rather than asking for something?`,
	`Looking only at NEW MESSAGE: is it small talk, a joke, a reaction, or an automated status post addressed to nobody in particular, with nothing for ${ASSISTANT_LABEL} to act on?`,
];

async function judge(state: string, questions: readonly string[]): Promise<number[]> {
	const res = await fetch(`${BASE?.replace(/\/$/, "")}/judge`, {
		method: "POST",
		headers: { "Content-Type": "application/json", ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}) },
		body: JSON.stringify({ state, questions: questions.map((instr) => ({ instr, options: ["no", "yes"] })) }),
	});
	if (!res.ok) throw new Error(`gate ${res.status}`);
	const body = (await res.json()) as { probs: number[][] };
	return body.probs.map((p) => p[1] ?? 0);
}

/** The pre-change renderer, so the comparison isolates the design and not the prompt. */
function renderOld(input: KevShadowInput): string {
	const newMessage = `${input.authorLabel ?? "user"}: ${input.text.replace(/\s+/g, " ").slice(0, 500)}`;
	const earlier = (input.earlier ?? []).map((t) => `${t.author}: ${t.body}`).join("\n") || "(no earlier messages)";
	return `EARLIER CONTEXT:\n${earlier}\n\nNEW MESSAGE:\n${newMessage}`;
}

/** The pre-change design: `help` alone, banded 0.45 / 0.6. */
function oldScore(probs: readonly number[]): { score: number; verdict: string } {
	const score = probs[0] ?? 0;
	return { score, verdict: score < 0.45 ? "would-skip" : score >= 0.6 ? "would-engage" : "would-defer" };
}

// Inbound-only, per #243: the persona's own replies are walls of text and cost 5 of
// 14 real owner messages a false skip. `WITH_REPLY` exists only to measure whether
// `directed` still resolves without them.
const DM_THREAD: KevShadowInput["earlier"] = [
	{ author: "owner", body: "kev 는 잘 되냐 최근에 적용한거", at: new Date(Date.now() - 9 * 60_000).toISOString() },
];
const WITH_REPLY: KevShadowInput["earlier"] = [
	...DM_THREAD,
	{
		author: ASSISTANT_LABEL,
		body: "돌아갑니다. 첫 7건 판정: would-skip 5 / would-defer 2.",
		at: new Date(Date.now() - 8 * 60_000).toISOString(),
	},
];

const CASES: Array<{ name: string; want: "engage" | "skip"; input: KevShadowInput }> = [
	{
		name: "owner DM bare summons",
		want: "engage",
		input: {
			originKey: "dm",
			text: "야",
			authorLabel: "owner",
			place: "direct message",
			addressed: true,
			addressedBy: "dm",
			earlier: DM_THREAD,
		},
	},
	{
		name: "owner DM liveness check",
		want: "engage",
		input: {
			originKey: "dm",
			text: "살아있는거맞냐",
			authorLabel: "owner",
			place: "direct message",
			addressed: true,
			addressedBy: "dm",
			earlier: DM_THREAD,
		},
	},
	{
		name: "owner DM nudge",
		want: "engage",
		input: {
			originKey: "dm",
			text: "아니 스코어 설계를 좀 바꿔라그럼",
			authorLabel: "owner",
			place: "direct message",
			addressed: true,
			addressedBy: "dm",
			earlier: DM_THREAD,
		},
	},
	{
		name: "owner DM closing thanks",
		want: "skip",
		input: {
			originKey: "dm",
			text: "ㅇㅇ 수고",
			authorLabel: "owner",
			place: "direct message",
			addressed: true,
			addressedBy: "dm",
			earlier: DM_THREAD,
		},
	},
	{
		name: "channel setup failure (mentioned)",
		want: "engage",
		input: {
			originKey: "ch",
			text: "gjc 설치했는데 bun install 에서 EACCES 나면서 죽습니다",
			authorLabel: "newcomer",
			place: "group channel #help",
			addressed: true,
			addressedBy: "mention",
			earlier: [{ author: "someone", body: "다들 설치 잘 됐나요?" }],
		},
	},
	{
		name: "channel ambient laughter",
		want: "skip",
		input: {
			originKey: "ch",
			text: "ㅋㅋㅋㅋㅋ 이게 맞나",
			authorLabel: "member",
			place: "group channel #playground",
			earlier: [{ author: "member2", body: "짤 하나 올림" }],
		},
	},
	{
		name: "channel ambient status post",
		want: "skip",
		input: {
			originKey: "ch",
			text: "CI 6/6 green, main 에 머지됨",
			authorLabel: "ci-bot",
			place: "group channel #ci",
			earlier: [{ author: "ci-bot", body: "build started" }],
		},
	},
	{
		name: "channel question aimed at another human",
		want: "skip",
		input: {
			originKey: "ch",
			text: "형 그거 어제 올린 PR 리뷰 좀",
			authorLabel: "member",
			place: "group channel #dev",
			earlier: [{ author: "member2", body: "ㅇㅋ 볼게" }],
		},
	},
];

let oldRight = 0;
let newRight = 0;
for (const testCase of CASES) {
	const [oldProbs, newProbs] = await Promise.all([
		judge(renderOld(testCase.input), OLD_QUESTIONS),
		judge(renderShadowState(testCase.input), NEW_QUESTIONS),
	]);
	const before = oldScore(oldProbs);
	const after = shadowScore(newProbs);
	const ok = (verdict: string) => (testCase.want === "engage" ? verdict !== "would-skip" : verdict !== "would-engage");
	if (ok(before.verdict)) oldRight += 1;
	if (ok(after.verdict)) newRight += 1;
	console.log(
		`${ok(after.verdict) ? "OK " : "BAD"} ${testCase.name.padEnd(38)} want=${testCase.want.padEnd(6)} ` +
			`old=${before.verdict.padEnd(13)}(${before.score.toFixed(3)}) new=${after.verdict.padEnd(13)}(${after.score.toFixed(3)}) ` +
			`help=${after.help.toFixed(2)} directed=${after.directed.toFixed(2)} ack=${after.ack.toFixed(2)} chatter=${after.chatter.toFixed(2)}`,
	);
}
console.log(`\nold design ${oldRight}/${CASES.length}   new design ${newRight}/${CASES.length}`);

// Does `directed` still resolve with the assistant's replies withheld (#243)?
for (const [label, earlier] of [
	["inbound only", DM_THREAD],
	["+ last assistant reply", WITH_REPLY],
] as const) {
	const probs = await judge(
		renderShadowState({
			originKey: "dm",
			text: "야",
			authorLabel: "owner",
			place: "direct message",
			addressed: true,
			addressedBy: "dm",
			earlier,
		}),
		NEW_QUESTIONS,
	);
	const s = shadowScore(probs);
	console.log(
		`summons context=${label.padEnd(24)} help=${s.help.toFixed(3)} directed=${s.directed.toFixed(3)} verdict=${s.verdict}`,
	);
}
