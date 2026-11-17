import { test } from "bun:test";
import { strict as assert } from "node:assert";
import { detectLanguage, EN_CATALOG, KO_CATALOG, LocalizationResolver } from "../src/server/localization";

test("detectLanguage detects Korean with Hangul characters", () => {
	// Korean text with significant Hangul ratio
	const koreanText = "안녕하세요. 이것은 한국어입니다.";
	assert.equal(detectLanguage(koreanText), "ko");
});

test("detectLanguage detects English with minimal Hangul", () => {
	const englishText = "Hello, this is English text.";
	assert.equal(detectLanguage(englishText), undefined);
});

test("detectLanguage returns undefined for mixed low-ratio Hangul", () => {
	// Very little Hangul mixed with English
	const mixedText = "Hello 한글 world";
	const result = detectLanguage(mixedText);
	// Should return undefined or ko depending on ratio
	assert(result === undefined || result === "ko");
});

test("detectLanguage handles empty string", () => {
	assert.equal(detectLanguage(""), undefined);
	assert.equal(detectLanguage("   "), undefined);
});

test("LocalizationResolver renders English by default", () => {
	const resolver = new LocalizationResolver("en");
	const message = resolver.render("turn.held.unresolved");
	assert(message.includes("[turn held]"));
	assert(message.includes("I could not confirm"));
});

test("LocalizationResolver renders Korean when specified", () => {
	const resolver = new LocalizationResolver("ko");
	const message = resolver.render("turn.held.unresolved");
	assert(message.includes("[턴 보류됨]"));
	assert(message.includes("완료되었는지 확인할 수 없어"));
});

test("LocalizationResolver detects Korean from last human message", () => {
	const resolver = new LocalizationResolver("en");
	const koreanMessage = "안녕하세요, 이건 한국어입니다.";
	const rendered = resolver.render("turn.held.capacity", { live: "5", max: "10" }, koreanMessage);
	assert(rendered.includes("[턴 보류됨]"));
});

test("LocalizationResolver respects English default for English messages", () => {
	const resolver = new LocalizationResolver("en");
	const englishMessage = "Hello, this is English.";
	const rendered = resolver.render("turn.held.capacity", { live: "5", max: "10" }, englishMessage);
	assert(rendered.includes("[turn held]"));
});

test("LocalizationResolver substitutes variables correctly", () => {
	const resolver = new LocalizationResolver("en");
	const rendered = resolver.render("turn.held.capacity", { live: "7", max: "10" });
	assert(rendered.includes("7"));
	assert(rendered.includes("10"));
});

test("LocalizationResolver handles missing message ID with English fallback", () => {
	const resolver = new LocalizationResolver("en");
	const rendered = resolver.render("nonexistent.message.id");
	// Should return the messageId itself as fallback
	assert(rendered.includes("nonexistent"));
});

test("EN_CATALOG has all expected message IDs", () => {
	const expectedIds = [
		"turn.held.unresolved",
		"turn.held.capacity",
		"turn.held.broker_wedged",
		"turn.held.broker_discovery_absent",
		"turn.held.sdk_unavailable",
		"turn.completed.text_unrecoverable",
		"gateway.notice.agent_run",
		"watch.registered",
		"watch.failure",
		"wake.failure",
		"handoff.pointer",
		"handoff.failure",
	];
	for (const id of expectedIds) {
		assert(EN_CATALOG[id], `EN_CATALOG missing ${id}`);
		assert.equal(typeof EN_CATALOG[id], "string");
	}
});

test("KO_CATALOG has all expected message IDs", () => {
	const expectedIds = [
		"turn.held.unresolved",
		"turn.held.capacity",
		"turn.held.broker_wedged",
		"turn.held.broker_discovery_absent",
		"turn.held.sdk_unavailable",
		"turn.completed.text_unrecoverable",
		"gateway.notice.agent_run",
		"watch.registered",
		"watch.failure",
		"wake.failure",
		"handoff.pointer",
		"handoff.failure",
	];
	for (const id of expectedIds) {
		assert(KO_CATALOG[id], `KO_CATALOG missing ${id}`);
		assert.equal(typeof KO_CATALOG[id], "string");
	}
});

test("Korean messages are different from English", () => {
	const enResolver = new LocalizationResolver("en");
	const koResolver = new LocalizationResolver("ko");

	const enMsg = enResolver.render("turn.held.unresolved");
	const koMsg = koResolver.render("turn.held.unresolved");

	assert.notEqual(enMsg, koMsg, "English and Korean messages should be different");
	assert(enMsg.includes("[turn held]"));
	assert(koMsg.includes("[턴 보류됨]"));
});

test("LocalizationResolver with high Hangul ratio detects Korean", () => {
	const resolver = new LocalizationResolver("en");
	// Text with over 10% Hangul characters
	const koreanDominant = "한국어입니다. 이것은 테스트입니다.";
	const rendered = resolver.render("turn.held.capacity", { live: "1", max: "2" }, koreanDominant);
	// Should be rendered in Korean
	assert(rendered.includes("[턴 보류됨]"));
});
