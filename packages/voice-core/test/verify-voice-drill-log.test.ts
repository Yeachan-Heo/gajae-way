import { describe, expect, test } from "bun:test";
import { serializeDrillRecord, VOICE_DRILL_LOG_SCHEMA, type VoiceDrillRecord } from "@gajaeway/voice-core";
import { verifyVoiceDrillLog } from "../../../scripts/verify-voice-drill-log";

const firstRecord: VoiceDrillRecord = {
	schema: VOICE_DRILL_LOG_SCHEMA,
	utteranceId: "utterance-1",
	ts: 1_700_000_000_000,
	speakerUserId: "speaker-1",
	speakerDisplayName: "Listener",
	rawTranscript: "안녕하세요",
	referenceTranscript: "안녕하세요",
	textSource: "committed",
	events: [
		{ kind: "partial", ts: 1_699_999_999_700 },
		{ kind: "silence_end", ts: 1_700_000_000_000 },
	],
	energyRms: 0.2,
	energyThreshold: 0.02,
	energyHoldMs: 300,
	energyGatePassed: true,
	boundary: "silence_end",
	boundaryAtMs: 1_700_000_000_000,
	mergedInto: null,
	turnId: "turn-1",
	ingress: "recorded",
	playback: "spoken",
	bargeIn: {
		decision: "continue",
		audioPassed: false,
		transcriptChars: 5,
		echoSimilarity: 0.1,
		cooldownActive: false,
		consumedMs: 0,
		truncationMs: 0,
	},
	detectedLanguage: "ko",
};

const secondRecord: VoiceDrillRecord = {
	...firstRecord,
	utteranceId: "utterance-2",
	ts: 1_700_000_001_000,
	speakerUserId: "speaker-2",
	speakerDisplayName: "Speaker",
	rawTranscript: "hello",
	referenceTranscript: null,
	textSource: "partial_fallback",
	events: [{ kind: "merge_window", ts: 1_700_000_001_000 }],
	energyRms: 0.01,
	energyGatePassed: false,
	boundary: "merge_window",
	boundaryAtMs: 1_700_000_001_000,
	mergedInto: "utterance-1",
	playback: "none",
	ingress: "truncated",
};

function line(record: VoiceDrillRecord): string {
	return serializeDrillRecord(record);
}

function mutableRecord(record: VoiceDrillRecord): Record<string, unknown> {
	return { ...record };
}

function invalidLine(
	result: ReturnType<typeof verifyVoiceDrillLog>,
	lineNumber: number,
): { line: number; reason: string } {
	const invalid = result.invalidLines.find((entry) => entry.line === lineNumber);
	expect(invalid).toBeDefined();
	return invalid as { line: number; reason: string };
}

describe("verifyVoiceDrillLog", () => {
	test("accepts a schema-valid two-record sample and reports counts", () => {
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${line(secondRecord)}`, "fixture.jsonl");

		expect(result.ok).toBe(true);
		expect(result.invalidLines).toEqual([]);
		expect(result.summary).toEqual({
			filePath: "fixture.jsonl",
			totalLines: 2,
			validRecords: 2,
			distinctSpeakers: 2,
			distinctTurnIds: 1,
			byBoundary: { silence_end: 1, merge_window: 1, held_capped: 0, dropped_noise: 0 },
			byIngress: { recorded: 1, dropped: 0, truncated: 1 },
			byPlayback: {
				spoken: 1,
				aborted_barge_in: 0,
				suppressed_redelivered: 0,
				suppressed_text_modality: 0,
				alignment_missing_text_fallback: 0,
				none: 1,
			},
			energyGatePassed: 1,
			referenceTranscript: 1,
		});
	});

	test("rejects a malformed JSON line and reports its line number", () => {
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n{not-json}\n${line(secondRecord)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("malformed JSON");
	});

	test("rejects a wrong schema string and reports its line number", () => {
		const wrongSchema = mutableRecord(secondRecord);
		wrongSchema.schema = "voice-drill-log.v0";
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${JSON.stringify(wrongSchema)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("schema");
	});

	test("rejects a missing required field and reports its line number", () => {
		const missingField = mutableRecord(secondRecord);
		delete missingField.rawTranscript;
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${JSON.stringify(missingField)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("rawTranscript");
	});

	test("rejects a wrong field type and reports its line number", () => {
		const wrongType = mutableRecord(secondRecord);
		wrongType.ts = "1700000001000";
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${JSON.stringify(wrongType)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("isVoiceDrillRecord");
	});

	test("rejects a duplicate utteranceId and reports its line number", () => {
		const duplicate = { ...secondRecord, utteranceId: firstRecord.utteranceId };
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${line(duplicate)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("duplicate utteranceId");
	});

	test("rejects mergedInto references to absent utterances and reports its line number", () => {
		const missingMergeTarget = { ...secondRecord, mergedInto: "utterance-absent" };
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${line(missingMergeTarget)}`);
		const invalid = invalidLine(result, 2);

		expect(result.ok).toBe(false);
		expect(invalid.reason).toContain("does not name an utteranceId present in the file");
	});

	test("treats a trailing newline as valid rather than an empty record", () => {
		const result = verifyVoiceDrillLog(`${line(firstRecord)}\n${line(secondRecord)}\n`);

		expect(result.ok).toBe(true);
		expect(result.summary.totalLines).toBe(2);
		expect(result.summary.validRecords).toBe(2);
	});
});
