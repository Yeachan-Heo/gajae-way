/**
 * The speak/stay-silent decision, made in code (#260).
 *
 * A persona told in its prompt to output only `[SILENT]` when it will not speak
 * still posts its reasoning instead ("기술 질문이라 채널에는 안 올리고, 오늘
 * 기록만 남깁니다."), and the delivery filter only drops parts that carry the
 * silence token. Two gates close that:
 *
 *   1. Before the turn: an unaddressed message the kev value score would skip
 *      opens no turn at all. The message is still recorded as context.
 *   2. Before delivery: a reply part the gate model reads as not responding to
 *      the message at all (abstention narration) is dropped and logged.
 *
 * Both apply only to traffic nobody aimed at the persona. A DM, an @mention, a
 * reply to the persona, a thread it is already talking in, the owner, and bot
 * authors (their own admission guard owns them, and the value score was never
 * calibrated on them) always bypass.
 *
 * Off unless `KEV_GATE_MODE=enforce` and the gate model is configured
 * (`KEV_SHADOW_URL`). With the model unreachable both gates fail open: a
 * missing verdict never silences anyone.
 */
import { judgeKevShadow, type KevShadowInput, kevShadowEnabled, probeGate } from "./kev-shadow";

const MODE_ENV = "KEV_GATE_MODE";

/**
 * Below this, a reply part is abstention narration. The score is the mean of
 * the two questions below, measured against the live gate model:
 *
 *   calibration, 28 drafts: abstention 0.13..0.40, real replies 0.44..0.98
 *   held out,    18 drafts: abstention 0.03..0.58, real replies 0.61..0.93
 *
 * 0.35 caught 17 of 20 abstention drafts and dropped none of 28 real replies,
 * including the ones most like narration ("넵, 멘션 없으면 조용히 있겠습니다."
 * answering a request to be quiet, "Checking the CI log now.", "🤣"). A missed
 * narration costs one stray line; a dropped answer costs the answer, so the cut
 * sits low.
 */
const ABSTAIN_UNDER = Number(process.env.KEV_GATE_ABSTAIN_UNDER ?? 0.35);

const Q_RESPONDS = "Does REPLY DRAFT respond to MESSAGE?";
const Q_ENGAGES = "Does REPLY DRAFT engage with MESSAGE?";

export type SpeechGateMode = "off" | "enforce";

export function speechGateMode(): SpeechGateMode {
	return process.env[MODE_ENV] === "enforce" && kevShadowEnabled() ? "enforce" : "off";
}

export interface SpeechGateSubject {
	readonly originKind: string;
	readonly mentioned?: boolean;
	readonly replyToSelf?: boolean;
	/** The persona is already talking in this thread; see `threadFollowUpEngaged`. */
	readonly threadFollowUp?: boolean;
	readonly authorId?: string;
	readonly authorIsBot?: boolean;
	readonly ownerId?: string;
}

/** True when this message may be silenced by either gate. */
export function speechGateApplies(subject: SpeechGateSubject): boolean {
	if (speechGateMode() !== "enforce") return false;
	if (subject.originKind === "dm") return false;
	if (subject.mentioned === true || subject.replyToSelf === true || subject.threadFollowUp === true) return false;
	if (subject.authorIsBot === true) return false;
	if (subject.ownerId !== undefined && subject.authorId === subject.ownerId) return false;
	return true;
}

/**
 * Gate 1. True when the turn should not open. Awaited on the inbound path, so
 * it costs one gate call (~110 ms measured) on exactly the traffic it applies to.
 */
export async function preTurnSkip(input: KevShadowInput): Promise<boolean> {
	const verdict = await judgeKevShadow(input);
	if (verdict?.verdict !== "would-skip") return false;
	console.info(`speech-gate skip origin=${input.originKey} score=${verdict.score.toFixed(4)}`);
	return true;
}

export function renderDraftState(message: string, draft: string): string {
	return `PLACE: group channel\n\nMESSAGE (from a person in the channel):\n${message.replace(/\s+/g, " ").slice(0, 1000)}\n\nREPLY DRAFT (what the assistant is about to post in reply):\n${draft.replace(/\s+/g, " ").slice(0, 1000)}`;
}

/** Mean of the two questions; the model's `yes` for "this responds". */
export function draftScore(probs: readonly number[]): number {
	const [responds = 1, engages = 1] = probs;
	return (responds + engages) / 2;
}

/**
 * Gate 2. True when `draft` is abstention narration and must not be delivered.
 * Every drop is logged with its text so a false positive can be found and the
 * cut moved.
 */
export async function isAbstentionNarration(originKey: string, message: string, draft: string): Promise<boolean> {
	const probs = await probeGate(renderDraftState(message, draft), [Q_RESPONDS, Q_ENGAGES]);
	if (!probs) return false;
	const score = draftScore(probs);
	if (score >= ABSTAIN_UNDER) return false;
	console.info(`speech-gate drop origin=${originKey} score=${score.toFixed(4)} text=${JSON.stringify(draft)}`);
	return true;
}
