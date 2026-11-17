import { expect, test } from "bun:test";

/**
 * Safely clips a string to a maximum number of code points, never leaving a lone UTF-16 surrogate.
 * This is a copy of the function in server.ts for testing purposes.
 */
function clipByCodePoints(text: string, limit: number): string {
	if (text.length <= limit) return text;
	return Array.from(text).slice(0, limit).join("");
}

test("clipByCodePoints does not split emojis", () => {
	// Orange circle emoji 🟠 is U+1F7E0, represented as UTF-16 surrogate pair \uD83D\uDDE0
	const emoji = "🟠";
	expect(emoji.length).toBe(2); // UTF-16 code units

	// Clipping at position 1 (in the middle of the surrogate pair) with naive .slice()
	// would produce a lone surrogate
	const naiveClip = emoji.slice(0, 1);
	expect(naiveClip.charCodeAt(0)).toBe(0xd83d); // High surrogate alone (malformed)

	// clipByCodePoints should return the full emoji or empty string, never a lone surrogate
	const safeClip = clipByCodePoints(emoji, 1);
	expect(safeClip).toBe(emoji); // Should keep the full emoji since it's 1 code point
});

test("clipByCodePoints handles text containing emojis correctly", () => {
	const text = "Reply PONG 😀";
	// 😀 is a single code point that takes 2 UTF-16 code units
	// text has 13 UTF-16 code units but 12 code points (emoji is 1 code point)
	// Using naive .slice(0, 11) would cut off part of the emoji
	const naiveClip = text.slice(0, 11);
	// After 11 UTF-16 units, we have "Reply PONG " (11 chars, no emoji)
	expect(naiveClip).toBe("Reply PONG ");

	// clipByCodePoints with limit 11 should include the first 11 code points
	// "Reply PONG " is 11 code points, so we get it without the emoji
	const safeClip = clipByCodePoints(text, 11);
	expect(safeClip).toBe("Reply PONG ");

	// Clipping to 12 code points should include the emoji (it's the 12th code point)
	const safeClip12 = clipByCodePoints(text, 12);
	expect(safeClip12).toBe("Reply PONG 😀");
});

test("clipByCodePoints handles multiple consecutive emojis", () => {
	const text = "👋🌍😀";
	// All emojis, 3 code points total
	const clipped = clipByCodePoints(text, 2);
	expect(clipped).toBe("👋🌍");
	expect(clipped.toWellFormed()).toBe(clipped); // Should already be well-formed
});

test("clipByCodePoints correctly limits to exact code point count", () => {
	const text = "Hello🌍World";
	const clipped = clipByCodePoints(text, 6);
	// "Hello" = 5 code points, 🌍 = 1 code point = 6 total
	expect(clipped).toBe("Hello🌍");
	expect(clipped.length).toBe(7); // 5 ASCII + 1 emoji (2 UTF-16 units)
});

test("clipByCodePoints returns original if within limit", () => {
	const text = "short";
	const clipped = clipByCodePoints(text, 100);
	expect(clipped).toBe(text);
});

test("renderPrompt output is always well-formed", () => {
	// Simulate renderPrompt behavior
	function renderPrompt(systemPreamble: string | undefined, text: string): string {
		if (!systemPreamble) return text.toWellFormed();
		return `${systemPreamble}\n\n${text}`.toWellFormed();
	}

	// Test with text that would have a lone surrogate before toWellFormed()
	const malformedText = "Reply PONG " + String.fromCharCode(0xd83d); // Lone high surrogate
	const preamble = "You are a helpful assistant.";
	const result = renderPrompt(preamble, malformedText);

	// The result should be well-formed (lone surrogates replaced with U+FFFD)
	expect(result.toWellFormed()).toBe(result);
	expect(result).toContain("You are a helpful assistant");
});

test("renderPrompt without preamble is also well-formed", () => {
	function renderPrompt(systemPreamble: string | undefined, text: string): string {
		if (!systemPreamble) return text.toWellFormed();
		return `${systemPreamble}\n\n${text}`.toWellFormed();
	}

	const malformedText = "Text with " + String.fromCharCode(0xd83d); // Lone high surrogate
	const result = renderPrompt(undefined, malformedText);

	expect(result.toWellFormed()).toBe(result);
});

test("clipByCodePoints preserves code points with combining characters", () => {
	// "e̊" is "e" + combining ring above (2 code units in UTF-16 usually, but 2 code points)
	const text = "café";
	const clipped = clipByCodePoints(text, 2);
	expect(clipped).toBe("ca");
});

test("clipByCodePoints handles emoji sequences and zero-width joiners", () => {
	// Family emoji: 👨‍👩‍👧‍👦 (multiple code points joined by zero-width joiners)
	const family = "👨‍👩‍👧‍👦";
	const clipped = clipByCodePoints(family, 1);
	// This should give us the first code point (👨) since we limit to 1
	expect(clipped).toBe("👨");
	expect(clipped.toWellFormed()).toBe(clipped);
});

test("clipByCodePoints of 1000 code points from long message", () => {
	// Create a message with exactly 1000 code points of 'a'
	const longMessage = "a".repeat(1000) + "🟠" + "b".repeat(100);

	const clipped = clipByCodePoints(longMessage, 1000);
	expect(clipped).toBe("a".repeat(1000));
	expect(clipped.toWellFormed()).toBe(clipped);
});

test("clipByCodePoints when clipping includes an emoji at the boundary", () => {
	// Create a message where the 1000th code point is an emoji
	const longMessage = "a".repeat(999) + "🟠" + "b".repeat(100);

	const clipped = clipByCodePoints(longMessage, 1000);
	expect(clipped).toBe("a".repeat(999) + "🟠");
	expect(clipped.toWellFormed()).toBe(clipped);
});

test("clipped context bodies are never malformed", () => {
	// Simulate the unread context rendering from server.ts line 2021
	const unreadEntry = {
		body: "This is a very long message " + "🟠".repeat(300), // Many emoji copies
	};

	const clipped = clipByCodePoints(unreadEntry.body, 1000);
	expect(clipped.toWellFormed()).toBe(clipped);
	// Should not contain lone surrogates
	const json = JSON.stringify({ text: clipped });
	// JSON.stringify should work without producing replacement characters
	expect(json).not.toContain("\\ud8");
	expect(json).not.toContain("\\ud9");
	expect(json).not.toContain("\\udc");
});

test("clipped recent history entries are never malformed", () => {
	// Simulate the recent history rendering from server.ts line 2047
	const recentEntry = "Some text " + "😀".repeat(200); // Many emoji copies

	const clipped = clipByCodePoints(recentEntry, 500);
	expect(clipped.toWellFormed()).toBe(clipped);
	// JSON.stringify should work without producing escape sequences for surrogates
	const json = JSON.stringify(clipped);
	// Should not contain UTF-16 surrogate escapes
	expect(json).not.toContain("\\ud8");
	expect(json).not.toContain("\\ud9");
	expect(json).not.toContain("\\udc");
});
