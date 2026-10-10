import { test, expect } from "bun:test";

// This implementation must match the one in server.ts
function clipContextBody(body: string, maxChars: number): string {
	if (body.length <= maxChars) return body;
	const clipped = body.slice(0, maxChars);
	const totalChars = body.length;
	return `${clipped}…[clipped: first ${maxChars} of ${totalChars} chars shown; the message itself is complete]`;
}

test("clipContextBody: returns text unchanged when under limit", () => {
	const short = "hello world";
	expect(clipContextBody(short, 100)).toBe(short);
});

test("clipContextBody: returns text unchanged when at exact limit", () => {
	const exact = "hello";
	expect(clipContextBody(exact, 5)).toBe(exact);
});

test("clipContextBody: clips and adds marker when over limit", () => {
	const long = "a".repeat(1000);
	const result = clipContextBody(long, 100);
	expect(result).toContain("…[clipped:");
	expect(result).toContain("first 100 of 1000 chars shown");
	expect(result.length).toBeLessThan(long.length);
});

test("clipContextBody: preserves text content before clip marker", () => {
	const text = "abc".repeat(100); // 300 chars
	const result = clipContextBody(text, 100);
	const content = result.slice(0, 100);
	// First 100 chars of "abcabc..." is 33 repetitions of "abc" (99 chars) + "a" (1 char)
	expect(content).toBe("abc".repeat(33) + "a"); // First 100 chars
});

test("clipContextBody: shows correct character count in marker", () => {
	const text = "hello world".repeat(50); // 550 chars
	const result = clipContextBody(text, 200);
	expect(result).toContain("first 200 of 550 chars shown");
});

test("clipContextBody: handles empty string", () => {
	expect(clipContextBody("", 100)).toBe("");
});

test("clipContextBody: handles limit of 1", () => {
	const result = clipContextBody("hello", 1);
	expect(result).toContain("h");
	expect(result).toContain("…[clipped:");
});

test("clipContextBody: handles unicode text", () => {
	const emoji = "👍".repeat(100); // Each emoji is multiple bytes
	const result = clipContextBody(emoji, 50);
	expect(result).toContain("…[clipped:");
	expect(result).toContain("first 50");
});

test("clipContextBody: marker indicates message is complete", () => {
	const result = clipContextBody("very long message text".repeat(100), 50);
	expect(result).toContain("the message itself is complete");
});
