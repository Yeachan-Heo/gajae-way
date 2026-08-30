import { describe, expect, test } from "bun:test";
import { BargeInGate, decideBargeIn, isRealTranscript } from "../src/barge-in";

const options = { minTranscriptChars: 3, cooldownMs: 100, echoSimilarity: 0.8 };
const input = (audioPassed: boolean, transcript: string) => ({ audioPassed, transcript, nowMs: 1_000 });

describe("decideBargeIn", () => {
	test("interrupts only when both audio and real transcript stages pass", () => {
		expect(decideBargeIn(input(true, "hello"), options).decision).toBe("interrupt");
		expect(decideBargeIn(input(true, "..."), options).decision).toBe("continue");
		expect(decideBargeIn(input(false, "hello"), options).decision).toBe("continue");
		expect(decideBargeIn(input(false, "..."), options).decision).toBe("continue");
	});

	test("reports deterministic reason fields for an accepted interruption", () => {
		const result = decideBargeIn({ ...input(true, "hello") }, options);
		expect(result).toMatchObject({
			decision: "interrupt",
			shouldInterrupt: true,
			audioPassed: true,
			transcriptChars: 5,
			cooldownActive: false,
		});
		expect(result.echoSimilarity).toBe(0);
	});

	test("suppresses transcripts shorter than the configured minimum", () => {
		const result = decideBargeIn(input(true, "hi"), options);
		expect(result.decision).toBe("continue");
		expect(result.reason).toBe("min_chars");
		expect(result.transcriptChars).toBe(2);
	});

	test("suppresses an interruption during cooldown and allows it at the boundary", () => {
		const active = decideBargeIn({ ...input(true, "hello"), previousInterruptionAtMs: 950 }, options);
		expect(active.cooldownActive).toBe(true);
		expect(active.decision).toBe("continue");
		const boundary = decideBargeIn({ ...input(true, "hello"), previousInterruptionAtMs: 900 }, options);
		expect(boundary.cooldownActive).toBe(false);
		expect(boundary.decision).toBe("interrupt");
	});

	test("suppresses a normalized echo at or above the configured similarity", () => {
		const result = decideBargeIn({ ...input(true, "Hello, world!"), playedText: "hello world" }, options);
		expect(result.echoSimilarity).toBe(1);
		expect(result.reason).toBe("echo");
		expect(result.decision).toBe("continue");
	});

	test("recognizes effect-only transcripts as non-text", () => {
		expect(isRealTranscript("[cough]")).toBe(false);
		expect(isRealTranscript("ㅋㅋㅋ")).toBe(false);
		expect(isRealTranscript("hello there")).toBe(true);
	});

	test("stateful gate records the previous interruption without a clock side channel", () => {
		const gate = new BargeInGate(options);
		expect(gate.decide({ ...input(true, "hello") }).decision).toBe("interrupt");
		expect(gate.decide({ ...input(true, "again") }).reason).toBe("cooldown");
		gate.reset();
		expect(gate.decide({ ...input(true, "again") }).decision).toBe("interrupt");
	});
});
