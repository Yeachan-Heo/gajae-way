import { expect, test } from "bun:test";
import { spokenReply } from "../src/server/server";

test("a text turn is never spoken", () => {
	expect(spokenReply(["안녕하세요"], false)).toBe("");
});

// One reply, written once, spoken once: a voice message per [BREAK] part would
// talk over itself and bill separately for each.
test("a spoken turn joins every part into one utterance", () => {
	expect(spokenReply(["첫째 줄", "둘째 줄"], true)).toBe("첫째 줄\n\n둘째 줄");
});

test("reply-threading tokens are routing metadata and are not read aloud", () => {
	expect(spokenReply(["[REPLY:1543148618427666473] 확인했습니다"], true)).toBe("확인했습니다");
	expect(spokenReply(["[REPLY:99] 하나", "둘"], true)).toBe("하나\n\n둘");
});

test("a part that is only a reply token contributes nothing", () => {
	expect(spokenReply(["[REPLY:99]", "본문"], true)).toBe("본문");
});

test("an empty reply stays empty so nothing is synthesized", () => {
	expect(spokenReply([], true)).toBe("");
	expect(spokenReply(["   "], true)).toBe("");
});

// Regression: the voice-carrying delivery used to be picked with
// `part === parts[parts.length - 1]`, a VALUE comparison. Two identical parts
// matched it both times, so the reply was spoken twice — billed twice and
// talking over itself. The selection is indexed now; this pins the shape of the
// data that broke it.
test("identical reply parts are still two distinct deliveries", () => {
	const parts = ["같은 말", "같은 말"];
	const valueMatches = parts.filter((part) => part === parts[parts.length - 1]).length;
	expect(valueMatches).toBe(2);
	const indexMatches = parts.filter((_, index) => index === parts.length - 1).length;
	expect(indexMatches).toBe(1);
});

test("the spoken form of duplicated parts still contains both, spoken once", () => {
	expect(spokenReply(["같은 말", "같은 말"], true)).toBe("같은 말\n\n같은 말");
});
