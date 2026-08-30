import { describe, expect, test } from "bun:test";
import {
	isVoiceDrillRecord,
	serializeDrillRecord,
	VOICE_DRILL_LOG_SCHEMA,
	type VoiceDrillRecord,
} from "../src/drill-log";

const validRecord: VoiceDrillRecord = {
	schema: VOICE_DRILL_LOG_SCHEMA,
	utteranceId: "utterance-1",
	ts: 1_000,
	speakerUserId: "user-1",
	speakerDisplayName: "Listener",
	rawTranscript: "hello",
	referenceTranscript: null,
	textSource: "committed",
	events: [
		{ kind: "energy_gate_passed", ts: 900 },
		{ kind: "silence_end", ts: 1_000 },
	],
	energyRms: 0.2,
	energyThreshold: 0.02,
	energyHoldMs: 300,
	energyGatePassed: true,
	boundary: "silence_end",
	boundaryAtMs: 1_000,
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
	detectedLanguage: "en",
};

function copyRecord(): Record<string, unknown> {
	return {
		...validRecord,
		events: validRecord.events.map((event) => ({ ...event })),
		bargeIn: { ...validRecord.bargeIn },
	};
}

describe("voice drill records", () => {
	test("serializes as one parseable newline-free JSON line", () => {
		const serialized = serializeDrillRecord({ ...validRecord, rawTranscript: "line 1\nline 2" });
		expect(serialized.includes("\n")).toBe(false);
		expect(serialized.includes("\r")).toBe(false);
		expect(JSON.parse(serialized)).toEqual({ ...validRecord, rawTranscript: "line 1\nline 2" });
	});

	test("accepts every required field with the documented types", () => {
		expect(isVoiceDrillRecord(validRecord)).toBe(true);
	});

	test("rejects every missing top-level required field", () => {
		const fields = [
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
		for (const field of fields) {
			const candidate = copyRecord();
			delete candidate[field];
			expect(isVoiceDrillRecord(candidate), field).toBe(false);
		}
	});

	test("rejects wrong top-level field types", () => {
		const wrongValues: Record<string, unknown> = {
			schema: 1,
			utteranceId: 1,
			ts: "1000",
			speakerUserId: 1,
			speakerDisplayName: 1,
			rawTranscript: 1,
			referenceTranscript: 1,
			textSource: 1,
			events: {},
			energyRms: "0.2",
			energyThreshold: "0.02",
			energyHoldMs: "300",
			energyGatePassed: "true",
			boundary: 1,
			boundaryAtMs: "1000",
			mergedInto: 1,
			turnId: 1,
			ingress: 1,
			playback: 1,
			bargeIn: [],
			detectedLanguage: 1,
		};
		for (const [field, value] of Object.entries(wrongValues)) {
			const candidate = copyRecord();
			candidate[field] = value;
			expect(isVoiceDrillRecord(candidate), field).toBe(false);
		}
	});

	test("rejects malformed event and barge-in fields", () => {
		const eventFields: Record<string, unknown>[] = [{ kind: 1, ts: 1 }, { kind: "ok", ts: "1" }, { kind: "ok" }];
		for (const event of eventFields) {
			const candidate = copyRecord();
			candidate.events = [event];
			expect(isVoiceDrillRecord(candidate)).toBe(false);
		}
		const bargeInFields: Record<string, unknown>[] = [
			{ ...validRecord.bargeIn, decision: 1 },
			{ ...validRecord.bargeIn, audioPassed: "false" },
			{ ...validRecord.bargeIn, transcriptChars: "5" },
			{ ...validRecord.bargeIn, echoSimilarity: "0.1" },
			{ ...validRecord.bargeIn, cooldownActive: 0 },
			{ ...validRecord.bargeIn, consumedMs: "0" },
			{ ...validRecord.bargeIn, truncationMs: "0" },
		];
		for (const bargeIn of bargeInFields) {
			const candidate = copyRecord();
			candidate.bargeIn = bargeIn;
			expect(isVoiceDrillRecord(candidate)).toBe(false);
		}
	});

	test("rejects missing nested event and barge-in fields", () => {
		for (const field of ["kind", "ts"] as const) {
			const event: Record<string, unknown> = { kind: "ok", ts: 1 };
			delete event[field];
			const candidate = copyRecord();
			candidate.events = [event];
			expect(isVoiceDrillRecord(candidate)).toBe(false);
		}
		for (const field of [
			"decision",
			"audioPassed",
			"transcriptChars",
			"echoSimilarity",
			"cooldownActive",
			"consumedMs",
			"truncationMs",
		] as const) {
			const bargeIn = { ...validRecord.bargeIn } as Record<string, unknown>;
			delete bargeIn[field];
			const candidate = copyRecord();
			candidate.bargeIn = bargeIn;
			expect(isVoiceDrillRecord(candidate)).toBe(false);
		}
	});
});
