/**
 * Machine-readable, append-only evidence for one voice utterance.
 *
 * Timestamps are injected epoch milliseconds. The core deliberately does not call
 * a clock, format dates, or write files; the adapter owns those concerns.
 */

export const VOICE_DRILL_LOG_SCHEMA = "voice-drill-log.v1" as const;

export type VoiceDrillTextSource = "committed" | "partial_fallback";
export type VoiceDrillBoundary = "silence_end" | "merge_window" | "held_capped" | "dropped_noise";
export type VoiceDrillIngress = "recorded" | "dropped" | "truncated";
export type VoiceDrillPlayback =
	| "spoken"
	| "aborted_barge_in"
	| "suppressed_redelivered"
	| "suppressed_text_modality"
	| "alignment_missing_text_fallback"
	| "none";

export interface VoiceDrillEvent {
	readonly kind: string;
	readonly ts: number;
}

export interface VoiceDrillBargeIn {
	readonly decision: string;
	readonly audioPassed: boolean;
	readonly transcriptChars: number;
	readonly echoSimilarity: number;
	readonly cooldownActive: boolean;
	readonly consumedMs: number;
	readonly truncationMs: number;
}

export interface VoiceDrillRecord {
	readonly schema: typeof VOICE_DRILL_LOG_SCHEMA;
	readonly utteranceId: string;
	readonly ts: number;
	readonly speakerUserId: string;
	readonly speakerDisplayName: string;
	readonly rawTranscript: string;
	readonly referenceTranscript: string | null;
	readonly textSource: VoiceDrillTextSource;
	readonly events: readonly VoiceDrillEvent[];
	readonly energyRms: number;
	readonly energyThreshold: number;
	readonly energyHoldMs: number;
	readonly energyGatePassed: boolean;
	readonly boundary: VoiceDrillBoundary;
	readonly boundaryAtMs: number;
	readonly mergedInto: string | null;
	readonly turnId: string | null;
	readonly ingress: VoiceDrillIngress;
	readonly playback: VoiceDrillPlayback;
	readonly bargeIn: VoiceDrillBargeIn;
	readonly detectedLanguage: string | null;
}

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

function isNonNegativeNumber(value: unknown): value is number {
	return isFiniteNumber(value) && value >= 0;
}

function isStringOrNull(value: unknown): value is string | null {
	return value === null || typeof value === "string";
}

function isOneOf<T extends string>(value: unknown, choices: readonly T[]): value is T {
	return typeof value === "string" && choices.includes(value as T);
}

function isVoiceDrillEvent(value: unknown): value is VoiceDrillEvent {
	if (!isRecord(value)) return false;
	return typeof value.kind === "string" && isNonNegativeNumber(value.ts);
}

function isVoiceDrillBargeIn(value: unknown): value is VoiceDrillBargeIn {
	if (!isRecord(value)) return false;
	return (
		typeof value.decision === "string" &&
		typeof value.audioPassed === "boolean" &&
		Number.isInteger(value.transcriptChars) &&
		Number(value.transcriptChars) >= 0 &&
		isFiniteNumber(value.echoSimilarity) &&
		typeof value.cooldownActive === "boolean" &&
		isNonNegativeNumber(value.consumedMs) &&
		isNonNegativeNumber(value.truncationMs)
	);
}

/**
 * Runtime validator shared by log writers and the S6 verification script. It is
 * intentionally strict about required fields while allowing future extra fields.
 */
export function isVoiceDrillRecord(value: unknown): value is VoiceDrillRecord {
	if (!isRecord(value)) return false;
	if (value.schema !== VOICE_DRILL_LOG_SCHEMA) return false;
	if (typeof value.utteranceId !== "string") return false;
	if (!isNonNegativeNumber(value.ts)) return false;
	if (typeof value.speakerUserId !== "string") return false;
	if (typeof value.speakerDisplayName !== "string") return false;
	if (typeof value.rawTranscript !== "string") return false;
	if (!isStringOrNull(value.referenceTranscript)) return false;
	if (!isOneOf(value.textSource, ["committed", "partial_fallback"] as const)) return false;
	if (!Array.isArray(value.events)) return false;
	let previousEventTs = -Infinity;
	for (const event of value.events) {
		if (!isVoiceDrillEvent(event) || event.ts < previousEventTs) return false;
		previousEventTs = event.ts;
	}
	if (!isNonNegativeNumber(value.energyRms)) return false;
	if (!isNonNegativeNumber(value.energyThreshold)) return false;
	if (!isNonNegativeNumber(value.energyHoldMs)) return false;
	if (typeof value.energyGatePassed !== "boolean") return false;
	if (!isOneOf(value.boundary, ["silence_end", "merge_window", "held_capped", "dropped_noise"] as const)) return false;
	if (!isNonNegativeNumber(value.boundaryAtMs)) return false;
	if (!isStringOrNull(value.mergedInto)) return false;
	if (!isStringOrNull(value.turnId)) return false;
	if (!isOneOf(value.ingress, ["recorded", "dropped", "truncated"] as const)) return false;
	if (
		!isOneOf(value.playback, [
			"spoken",
			"aborted_barge_in",
			"suppressed_redelivered",
			"suppressed_text_modality",
			"alignment_missing_text_fallback",
			"none",
		] as const)
	)
		return false;
	if (!isVoiceDrillBargeIn(value.bargeIn)) return false;
	return isStringOrNull(value.detectedLanguage);
}

/** Serializes exactly one validated record without a trailing newline. */
export function serializeDrillRecord(record: VoiceDrillRecord): string {
	if (!isVoiceDrillRecord(record)) throw new TypeError("Invalid voice drill record");
	const serialized = JSON.stringify(record);
	if (serialized === undefined || serialized.includes("\n") || serialized.includes("\r")) {
		throw new TypeError("Voice drill record did not serialize to one line");
	}
	return serialized;
}
