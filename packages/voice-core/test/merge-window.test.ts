import { describe, expect, test } from "bun:test";
import { mergeUtterances } from "../src/merge-window";

const utterance = (speakerId: string, text: string, endedAtMs: number) => ({ speakerId, text, endedAtMs });

describe("mergeUtterances", () => {
	test("merges multiple speakers that end inside the configured window", () => {
		const turns = mergeUtterances(
			[utterance("alice", "first", 1000), utterance("bob", "second", 1500), utterance("carol", "third", 2000)],
			1000,
		);
		expect(turns).toHaveLength(1);
		expect(turns[0]?.lines.map((line) => [line.speakerId, line.text])).toEqual([
			["alice", "first"],
			["bob", "second"],
			["carol", "third"],
		]);
		expect(turns[0]?.text).toBe("alice: first\nbob: second\ncarol: third");
	});

	test("keeps utterances outside the window in separate turns", () => {
		const turns = mergeUtterances([utterance("alice", "first", 1000), utterance("bob", "later", 2001)], 1000);
		expect(turns).toHaveLength(2);
		expect(turns.map((turn) => turn.text)).toEqual(["alice: first", "bob: later"]);
	});

	test("combines consecutive utterances from the same speaker without losing order", () => {
		const turns = mergeUtterances(
			[utterance("alice", "one", 100), utterance("alice", "two", 300), utterance("bob", "three", 400)],
			500,
		);
		expect(turns).toHaveLength(1);
		expect(turns[0]?.lines).toHaveLength(2);
		expect(turns[0]?.lines[0]?.text).toBe("one two");
		expect(turns[0]?.lines[0]?.utterances.map((item) => item.text)).toEqual(["one", "two"]);
	});

	test("sorts out-of-order endings before applying the window", () => {
		const turns = mergeUtterances([utterance("bob", "later", 150), utterance("alice", "first", 0)], 200);
		expect(turns[0]?.utterances.map((item) => item.speakerId)).toEqual(["alice", "bob"]);
	});
});
