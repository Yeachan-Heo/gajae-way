import { describe, expect, test } from "bun:test";
import { resolveAuthorNames, resolveDisplayName } from "../src/author";

const author = { id: "u1", username: "yeachanheo", globalName: "Yeachan Heo" };

describe("resolveDisplayName", () => {
	test("prefers the guild nickname the room actually shows", () => {
		expect(resolveDisplayName(author, { nickname: "형님", displayName: "형님" })).toBe("형님");
	});

	test("falls back to the member display name when no nickname is set", () => {
		expect(resolveDisplayName(author, { nickname: null, displayName: "Yeachan Heo" })).toBe("Yeachan Heo");
	});

	test("falls back to the global name with no member, e.g. a DM", () => {
		expect(resolveDisplayName(author, null)).toBe("Yeachan Heo");
		expect(resolveDisplayName(author)).toBe("Yeachan Heo");
	});

	test("uses the handle only when nothing else exists", () => {
		expect(resolveDisplayName({ id: "u1", username: "yeachanheo" })).toBe("yeachanheo");
	});

	test("skips blank and whitespace-only values instead of reporting them", () => {
		expect(
			resolveDisplayName({ id: "u1", username: "yeachanheo", globalName: "" }, { nickname: "   ", displayName: "" }),
		).toBe("yeachanheo");
	});

	test("returns undefined when the platform provides no name at all", () => {
		expect(resolveDisplayName({ id: "u1" })).toBeUndefined();
		expect(resolveDisplayName(undefined)).toBeUndefined();
	});

	test("the same account resolves differently per server", () => {
		expect(resolveDisplayName(author, { nickname: "형님" })).toBe("형님");
		expect(resolveDisplayName(author, { nickname: "Chan" })).toBe("Chan");
	});
});

describe("resolveAuthorNames", () => {
	test("returns the display name and the handle separately", () => {
		expect(resolveAuthorNames(author, { nickname: "형님" })).toEqual({
			displayName: "형님",
			handle: "yeachanheo",
		});
	});

	test("omits the handle when the platform sends none", () => {
		expect(resolveAuthorNames({ id: "u1", globalName: "Only Global" })).toEqual({
			displayName: "Only Global",
		});
	});

	test("omits both for an author with no names", () => {
		expect(resolveAuthorNames({ id: "u1" })).toEqual({});
	});

	test("display name equals the handle when that is all there is", () => {
		const names = resolveAuthorNames({ id: "u1", username: "solo" });
		expect(names.displayName).toBe("solo");
		expect(names.handle).toBe("solo");
	});
});
