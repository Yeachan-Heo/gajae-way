import { expect, test } from "bun:test";
import { parseAuthoredArray } from "../src/monitors/propagate";

test("authoring answers are accepted as bare JSON, fenced JSON, or prose around the array; garbage still fails", () => {
	const arr = [{ eventId: "e1", note: "posted" }];
	expect(parseAuthoredArray(JSON.stringify(arr))).toEqual(arr);
	expect(parseAuthoredArray("Posted the reply.\n\n```json\n" + JSON.stringify(arr) + "\n```\n")).toEqual(arr);
	expect(
		parseAuthoredArray("Clear picture now. Both X and Threads are working.\n" + JSON.stringify(arr) + "\nDone."),
	).toEqual(arr);
	expect(parseAuthoredArray('note with [brackets] inside: [{"eventId":"e1","note":"see [ref] here"}]')).toEqual([
		{ eventId: "e1", note: "see [ref] here" },
	]);
	expect(() => parseAuthoredArray("no array here")).toThrow(/JSON is unparseable/);
	expect(() => parseAuthoredArray('{"eventId":"e1","note":"object not array"}')).toThrow(/not a JSON array/);
});
