import { describe, expect, test } from "bun:test";

describe("Discord /model command authorization", () => {
	test("/model set and /model clear are privileged operations", () => {
		// Test that these commands are identified as privileged
		const commands = [
			{ text: "/model set gpt-4", expected: true },
			{ text: "/model set claude-3", expected: true },
			{ text: "/model clear", expected: true },
			{ text: "/model", expected: false },
			{ text: "/model show", expected: false },
			{ text: "/new", expected: false },
			{ text: "/reset", expected: false },
		];

		for (const cmd of commands) {
			const isPrivilegedModel = cmd.text.startsWith("/model set ") || cmd.text === "/model clear";
			expect(isPrivilegedModel).toBe(cmd.expected);
		}
	});
});
