/**
 * Provider-agnostic two-stage barge-in decision logic.
 *
 * Audio energy alone is not enough to interrupt playback: the arriving transcript must be
 * real speech, must not be an echo of already-played text, and must be outside cooldown.
 */

export interface BargeInOptions {
	readonly minTranscriptChars: number;
	readonly cooldownMs: number;
	readonly echoSimilarity: number;
}

export interface BargeInInput {
	readonly audioPassed: boolean;
	readonly transcript: string;
	readonly playedText?: string;
	readonly textAlreadyPlayed?: string;
	readonly nowMs: number;
	readonly atMs?: number;
	readonly previousInterruptionAtMs?: number;
	readonly lastInterruptedAtMs?: number;
}

export type BargeInDecision = "interrupt" | "continue";
export type BargeInReason = "audio_gate" | "transcript_gate" | "min_chars" | "cooldown" | "echo" | "passed";

export interface BargeInResult {
	readonly decision: BargeInDecision;
	readonly shouldInterrupt: boolean;
	readonly reason: BargeInReason;
	readonly audioPassed: boolean;
	readonly transcriptChars: number;
	readonly echoSimilarity: number;
	readonly cooldownActive: boolean;
}

const EFFECT_WORDS: readonly string[] = [
	"applause",
	"breath",
	"breathing",
	"cough",
	"coughing",
	"gasp",
	"giggling",
	"laugh",
	"laughter",
	"laughing",
	"lmao",
	"lol",
	"music",
	"noise",
	"sigh",
	"sniff",
	"whistle",
	"ㅋㅋ",
	"ㅎㅎ",
	"ㅋ",
	"ㅎ",
];

function compactText(value: string): string {
	return value
		.normalize("NFKC")
		.toLowerCase()
		.replace(/[^\p{L}\p{N}]+/gu, "");
}

/** Normalization used by both the real-text and echo gates. */
export function normalizeBargeInText(value: string): string {
	return compactText(value);
}

function isEffectOnly(compact: string): boolean {
	if (compact.length === 0) return true;
	if (/^(?:ㅋ|ㅎ|ᄏ|ᄒ)+$/u.test(compact)) return true;
	return EFFECT_WORDS.includes(compact);
}

/** Counts meaningful Unicode letters/numbers after punctuation and spacing are removed. */
export function transcriptCharacterCount(transcript: string): number {
	return Array.from(compactText(transcript)).length;
}

/** Returns true only when the transcript contains speech-like text rather than effects/noise. */
export function isRealTranscript(transcript: string): boolean {
	const compact = compactText(transcript);
	return compact.length > 0 && !isEffectOnly(compact);
}

function editDistance(left: readonly string[], right: readonly string[]): number {
	let previous = Array.from({ length: right.length + 1 }, (_, index) => index);
	for (let leftIndex = 1; leftIndex <= left.length; leftIndex += 1) {
		const current = [leftIndex];
		for (let rightIndex = 1; rightIndex <= right.length; rightIndex += 1) {
			const substitution = previous[rightIndex - 1] + (left[leftIndex - 1] === right[rightIndex - 1] ? 0 : 1);
			const insertion = current[rightIndex - 1] + 1;
			const deletion = previous[rightIndex] + 1;
			current.push(Math.min(substitution, insertion, deletion));
		}
		previous = current;
	}
	return previous[right.length] ?? 0;
}

/** Normalized Levenshtein similarity in the inclusive range [0, 1]. */
export function normalizedEchoSimilarity(left: string, right: string): number {
	const normalizedLeft = Array.from(compactText(left));
	const normalizedRight = Array.from(compactText(right));
	if (normalizedLeft.length === 0 && normalizedRight.length === 0) return 1;
	if (normalizedLeft.length === 0 || normalizedRight.length === 0) return 0;
	const distance = editDistance(normalizedLeft, normalizedRight);
	return 1 - distance / Math.max(normalizedLeft.length, normalizedRight.length);
}

function validateOptions(options: BargeInOptions): void {
	if (!Number.isSafeInteger(options.minTranscriptChars) || options.minTranscriptChars < 1) {
		throw new RangeError("minTranscriptChars must be a positive safe integer");
	}
	if (!Number.isFinite(options.cooldownMs) || options.cooldownMs < 0) {
		throw new RangeError("cooldownMs must be finite and non-negative");
	}
	if (!Number.isFinite(options.echoSimilarity) || options.echoSimilarity < 0 || options.echoSimilarity > 1) {
		throw new RangeError("echoSimilarity must be between zero and one");
	}
}

function currentTime(input: BargeInInput): number {
	return input.nowMs;
}

function previousInterruption(input: BargeInInput): number | undefined {
	return input.previousInterruptionAtMs ?? input.lastInterruptedAtMs;
}

/** Evaluates the two-stage gate and all three false-positive defenses. */
export function decideBargeIn(input: BargeInInput, options: BargeInOptions): BargeInResult {
	if (!Number.isFinite(input.nowMs)) throw new RangeError("nowMs must be finite");
	validateOptions(options);
	const playedText = input.playedText ?? input.textAlreadyPlayed ?? "";
	const transcriptChars = transcriptCharacterCount(input.transcript);
	const realTranscript = isRealTranscript(input.transcript);
	const echoSimilarity = normalizedEchoSimilarity(input.transcript, playedText);
	const previousAtMs = previousInterruption(input);
	const nowMs = currentTime(input);
	const cooldownActive = previousAtMs !== undefined && nowMs - previousAtMs < options.cooldownMs;
	const echoActive = normalizeBargeInText(playedText).length > 0 && echoSimilarity >= options.echoSimilarity;

	let reason: BargeInReason = "passed";
	if (!input.audioPassed) reason = "audio_gate";
	else if (!realTranscript) reason = "transcript_gate";
	else if (transcriptChars < options.minTranscriptChars) reason = "min_chars";
	else if (cooldownActive) reason = "cooldown";
	else if (echoActive) reason = "echo";
	const shouldInterrupt = reason === "passed";
	return {
		decision: shouldInterrupt ? "interrupt" : "continue",
		shouldInterrupt,
		reason,
		audioPassed: input.audioPassed,
		transcriptChars,
		echoSimilarity,
		cooldownActive,
	};
}

/** Stateful convenience wrapper; time is still supplied by every decision input. */
export class BargeInGate {
	private readonly options: BargeInOptions;
	private lastInterruptedAtMs: number | undefined;

	constructor(options: BargeInOptions) {
		validateOptions(options);
		this.options = options;
	}

	decide(
		input: Omit<BargeInInput, "previousInterruptionAtMs" | "lastInterruptedAtMs"> & { readonly nowMs: number },
	): BargeInResult {
		const result = decideBargeIn({ ...input, previousInterruptionAtMs: this.lastInterruptedAtMs }, this.options);
		if (result.shouldInterrupt) this.lastInterruptedAtMs = input.nowMs;
		return result;
	}

	reset(): void {
		this.lastInterruptedAtMs = undefined;
	}
}
