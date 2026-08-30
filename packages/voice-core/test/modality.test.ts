import { describe, expect, test } from "bun:test";
import { ModalityRegistry } from "../src/modality";

describe("ModalityRegistry", () => {
	test("resolves voice and announces it once", () => {
		const registry = new ModalityRegistry();
		expect(registry.register("voice-turn", "voice", 0)).toBe(true);
		expect(registry.resolve("voice-turn")).toBe("voice");
		expect(registry.announce("voice-turn")).toBe(true);
		expect(registry.announce("voice-turn")).toBe(false);
		expect(registry.get("voice-turn")?.announced).toBe(true);
	});

	test("text, unknown, and concurrent turn ids fail closed to text", () => {
		const registry = new ModalityRegistry();
		registry.register("text-turn", "text", 0);
		registry.register("voice-turn", "voice", 0);
		expect(registry.resolve("text-turn")).toBeUndefined();
		expect(registry.resolve("missing-turn")).toBeUndefined();
		expect(registry.resolve("other-concurrent-turn")).toBeUndefined();
	});

	test("repeated final fragments do not evict the voice entry", () => {
		const registry = new ModalityRegistry();
		registry.register("voice-turn", "voice", 0);
		expect(registry.recordPart("voice-turn", 10, true)).toBe(true);
		expect(registry.recordPart("voice-turn", 20, true)).toBe(true);
		expect(registry.recordPart("voice-turn", 30, true)).toBe(true);
		expect(registry.resolve("voice-turn")).toBe("voice");
		expect(registry.get("voice-turn")?.parts).toBe(3);
	});

	test("turn-end deletes the entry and repeated registration does not reset it", () => {
		const registry = new ModalityRegistry();
		registry.register("voice-turn", "voice", 0);
		registry.announce("voice-turn");
		registry.recordPart("voice-turn", 10);
		expect(registry.register("voice-turn", "voice", 20)).toBe(false);
		expect(registry.get("voice-turn")?.parts).toBe(1);
		expect(registry.get("voice-turn")?.announced).toBe(true);
		expect(registry.turnEnd("voice-turn")).toBe(true);
		expect(registry.resolve("voice-turn")).toBeUndefined();
	});

	test("TTL sweep removes lost-event entries but not a recently delivered fragment", () => {
		const registry = new ModalityRegistry();
		registry.register("stale", "voice", 0);
		registry.register("fresh", "voice", 0);
		registry.recordPart("fresh", 50, true);
		const result = registry.sweep(100, 100);
		expect(result).toEqual({ cleared: 1, turnIds: ["stale"] });
		expect(registry.resolve("stale")).toBeUndefined();
		expect(registry.resolve("fresh")).toBe("voice");
	});
});
