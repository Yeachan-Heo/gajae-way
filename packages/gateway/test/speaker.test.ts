import { describe, expect, test } from "bun:test";
import { composeSpeakerLabel } from "../src/server/speaker";

describe("composeSpeakerLabel", () => {
	test("leads with the server display name", () => {
		expect(
			composeSpeakerLabel({ authorId: "660473980301344768", authorName: "형님", authorHandle: "yeachanheo" }),
		).toBe("형님 (@yeachanheo)");
	});

	test("does not repeat the handle when it equals the display name", () => {
		expect(composeSpeakerLabel({ authorId: "1", authorName: "yeachanheo", authorHandle: "yeachanheo" })).toBe(
			"yeachanheo",
		);
	});

	test("uses the display name alone when no handle was sent", () => {
		expect(composeSpeakerLabel({ authorId: "1", authorName: "형님" })).toBe("형님");
	});

	test("falls back to the handle before the opaque id", () => {
		expect(composeSpeakerLabel({ authorId: "1", authorHandle: "yeachanheo" })).toBe("yeachanheo");
	});

	test("falls back to the id when nothing else is available", () => {
		expect(composeSpeakerLabel({ authorId: "1" })).toBe("1");
	});

	test("blank names never reach the header", () => {
		expect(composeSpeakerLabel({ authorId: "1", authorName: "  ", authorHandle: "" })).toBe("1");
	});

	test("undefined engagement yields no speaker", () => {
		expect(composeSpeakerLabel(undefined)).toBeUndefined();
	});
});
