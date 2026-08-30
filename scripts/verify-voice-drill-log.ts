import { readFile } from "node:fs/promises";
import {
	isVoiceDrillRecord,
	VOICE_DRILL_LOG_SCHEMA,
	type VoiceDrillBoundary,
	type VoiceDrillIngress,
	type VoiceDrillPlayback,
	type VoiceDrillRecord,
} from "@gajaeway/voice-core";

const BOUNDARIES = [
	"silence_end",
	"merge_window",
	"held_capped",
	"dropped_noise",
] as const satisfies readonly VoiceDrillBoundary[];
const INGRESSES = ["recorded", "dropped", "truncated"] as const satisfies readonly VoiceDrillIngress[];
const PLAYBACKS = [
	"spoken",
	"aborted_barge_in",
	"suppressed_redelivered",
	"suppressed_text_modality",
	"alignment_missing_text_fallback",
	"none",
] as const satisfies readonly VoiceDrillPlayback[];

const REQUIRED_FIELDS = [
	"schema",
	"utteranceId",
	"ts",
	"speakerUserId",
	"speakerDisplayName",
	"rawTranscript",
	"referenceTranscript",
	"textSource",
	"events",
	"energyRms",
	"energyThreshold",
	"energyHoldMs",
	"energyGatePassed",
	"boundary",
	"boundaryAtMs",
	"mergedInto",
	"turnId",
	"ingress",
	"playback",
	"bargeIn",
	"detectedLanguage",
] as const;

type CountMap<T extends string> = Readonly<Record<T, number>>;

export interface VoiceDrillLogInvalidLine {
	readonly line: number;
	readonly reason: string;
}

export interface VoiceDrillLogSummary {
	readonly filePath: string | null;
	readonly totalLines: number;
	readonly validRecords: number;
	readonly distinctSpeakers: number;
	readonly distinctTurnIds: number;
	readonly byBoundary: CountMap<VoiceDrillBoundary>;
	readonly byIngress: CountMap<VoiceDrillIngress>;
	readonly byPlayback: CountMap<VoiceDrillPlayback>;
	readonly energyGatePassed: number;
	readonly referenceTranscript: number;
}

export interface VoiceDrillLogVerification {
	readonly ok: boolean;
	readonly records: readonly VoiceDrillRecord[];
	readonly invalidLines: readonly VoiceDrillLogInvalidLine[];
	readonly summary: VoiceDrillLogSummary;
}

interface ParsedRecord {
	readonly line: number;
	readonly record: VoiceDrillRecord;
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function zeroCounts<T extends string>(keys: readonly T[]): Record<T, number> {
	const counts = {} as Record<T, number>;
	for (const key of keys) counts[key] = 0;
	return counts;
}

function splitLines(text: string): string[] {
	const lines = text.split(/\r?\n/);
	if (lines.at(-1) === "") lines.pop();
	return lines;
}

function guardFailureReason(value: unknown, schemaMatches: boolean): string {
	if (!isObject(value)) return "record must be a JSON object";
	if (!schemaMatches) return `schema must equal "${VOICE_DRILL_LOG_SCHEMA}"`;
	for (const field of REQUIRED_FIELDS) {
		if (!Object.hasOwn(value, field)) return `missing required field "${field}"`;
	}
	return "isVoiceDrillRecord rejected the record (a field type or value is invalid)";
}

function makeSummary(
	records: readonly VoiceDrillRecord[],
	totalLines: number,
	filePath: string | null,
): VoiceDrillLogSummary {
	const byBoundary = zeroCounts(BOUNDARIES);
	const byIngress = zeroCounts(INGRESSES);
	const byPlayback = zeroCounts(PLAYBACKS);
	const speakers = new Set<string>();
	const turnIds = new Set<string>();
	let energyGatePassed = 0;
	let referenceTranscript = 0;

	for (const record of records) {
		byBoundary[record.boundary] += 1;
		byIngress[record.ingress] += 1;
		byPlayback[record.playback] += 1;
		speakers.add(record.speakerUserId);
		if (record.turnId !== null) turnIds.add(record.turnId);
		if (record.energyGatePassed) energyGatePassed += 1;
		if (record.referenceTranscript !== null) referenceTranscript += 1;
	}

	return {
		filePath,
		totalLines,
		validRecords: records.length,
		distinctSpeakers: speakers.size,
		distinctTurnIds: turnIds.size,
		byBoundary,
		byIngress,
		byPlayback,
		energyGatePassed,
		referenceTranscript,
	};
}

/**
 * Verifies JSONL text without writing or repairing the source file. The guard
 * remains the sole authority for record validity; local checks only explain
 * schema, line, and cross-record failures for the operator-facing report.
 */
export function verifyVoiceDrillLog(text: string, filePath: string | null = null): VoiceDrillLogVerification {
	const lines = splitLines(text);
	const invalidLines: VoiceDrillLogInvalidLine[] = [];
	const parsedRecords: ParsedRecord[] = [];
	const addInvalid = (line: number, reason: string): void => {
		invalidLines.push({ line, reason });
	};

	if (lines.length === 0) {
		addInvalid(0, "file is empty");
		return {
			ok: false,
			records: [],
			invalidLines,
			summary: makeSummary([], 0, filePath),
		};
	}

	for (const [index, line] of lines.entries()) {
		const lineNumber = index + 1;
		let value: unknown;
		try {
			value = JSON.parse(line) as unknown;
		} catch (error: unknown) {
			const message = error instanceof Error ? error.message : String(error);
			addInvalid(lineNumber, `malformed JSON: ${message}`);
			continue;
		}

		const schemaMatches = isObject(value) && value.schema === VOICE_DRILL_LOG_SCHEMA;
		if (!schemaMatches || !isVoiceDrillRecord(value)) {
			addInvalid(lineNumber, guardFailureReason(value, schemaMatches));
			continue;
		}
		parsedRecords.push({ line: lineNumber, record: value });
	}

	const utteranceLines = new Map<string, number>();
	for (const entry of parsedRecords) {
		const firstLine = utteranceLines.get(entry.record.utteranceId);
		if (firstLine !== undefined) {
			addInvalid(
				entry.line,
				`duplicate utteranceId "${entry.record.utteranceId}"; first occurrence is on line ${firstLine}`,
			);
		} else {
			utteranceLines.set(entry.record.utteranceId, entry.line);
		}
	}

	for (const entry of parsedRecords) {
		const { mergedInto } = entry.record;
		if (mergedInto !== null && !utteranceLines.has(mergedInto)) {
			addInvalid(entry.line, `mergedInto "${mergedInto}" does not name an utteranceId present in the file`);
		}
	}

	const invalidLineNumbers = new Set(invalidLines.map((invalid) => invalid.line));
	const records = parsedRecords.filter((entry) => !invalidLineNumbers.has(entry.line)).map((entry) => entry.record);

	return {
		ok: invalidLines.length === 0,
		records,
		invalidLines,
		summary: makeSummary(records, lines.length, filePath),
	};
}

function formatSummary(summary: VoiceDrillLogSummary, ok: boolean): string {
	const lines = [
		`voice drill log: ${ok ? "VALID" : "INVALID"}`,
		`file: ${summary.filePath ?? "<text>"}`,
		`total lines: ${summary.totalLines}`,
		`valid records: ${summary.validRecords}`,
		`distinct speakers: ${summary.distinctSpeakers}`,
		`distinct turn ids: ${summary.distinctTurnIds}`,
		"boundary counts:",
	];
	for (const boundary of BOUNDARIES) lines.push(`  ${boundary}: ${summary.byBoundary[boundary]}`);
	lines.push("ingress counts:");
	for (const ingress of INGRESSES) lines.push(`  ${ingress}: ${summary.byIngress[ingress]}`);
	lines.push("playback counts:");
	for (const playback of PLAYBACKS) lines.push(`  ${playback}: ${summary.byPlayback[playback]}`);
	lines.push(`energyGatePassed: ${summary.energyGatePassed}`);
	lines.push(`referenceTranscript: ${summary.referenceTranscript}`);
	return lines.join("\n");
}

async function runCli(args: readonly string[]): Promise<number> {
	if (args.length !== 1) {
		console.error("Usage: bun run scripts/verify-voice-drill-log.ts <file.jsonl>");
		return 1;
	}

	const filePath = args[0];
	let text: string;
	try {
		text = await readFile(filePath, "utf8");
	} catch (error: unknown) {
		const message = error instanceof Error ? error.message : String(error);
		console.error(`error: unable to read ${filePath}: ${message}`);
		return 1;
	}

	const result = verifyVoiceDrillLog(text, filePath);
	console.log(formatSummary(result.summary, result.ok));
	for (const invalid of result.invalidLines) {
		if (invalid.line === 0) console.error(`invalid file: ${invalid.reason}`);
		else console.error(`invalid line ${invalid.line}: ${invalid.reason}`);
	}
	return result.ok ? 0 : 1;
}

if (import.meta.main) {
	const exitCode = await runCli(process.argv.slice(2));
	process.exitCode = exitCode;
}
