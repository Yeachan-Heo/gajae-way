import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { ANSIRenderer, KeyboardInput, redact, type TerminalInput, terminalDimensions } from "../src/tui-core";

function fixture() {
	const input = new PassThrough();
	let output = "";
	const keyboard = new KeyboardInput(input, {
		write(text) {
			output += text;
		},
	});
	return { input, keyboard, output: () => output };
}

describe("ANSI renderer", () => {
	test("emits styles, movement, and reset sequences", () => {
		let output = "";
		const renderer = new ANSIRenderer({
			write(text) {
				output += text;
			},
		});
		renderer.clear();
		renderer.moveCursor(2, 3);
		renderer.write("ok", "red", "blue", true, true);
		renderer.clearLine();
		renderer.hideCursor();
		renderer.showCursor();
		renderer.resetStyle();
		expect(output).toBe("\x1b[2J\x1b[H\x1b[2;3H\x1b[0;31;44;1;4mok\x1b[0m\x1b[2K\r\x1b[?25l\x1b[?25h\x1b[0m");
		expect(() => renderer.moveCursor(0, 1)).toThrow(RangeError);
	});
});

describe("keyboard", () => {
	test("decodes arrows, modifiers, Unicode and queued input", async () => {
		const f = fixture();
		try {
			f.input.write("\x1b[A\x03Aé\r");
			expect((await f.keyboard.readKey()).key).toBe("up");
			expect(await f.keyboard.readKey()).toMatchObject({ key: "c", isCtrl: true });
			expect(await f.keyboard.readKey()).toMatchObject({ key: "A", isShift: true });
			expect((await f.keyboard.readKey()).code).toBe("é");
			expect((await f.keyboard.readKey()).key).toBe("enter");
		} finally {
			f.keyboard.close();
		}
	});
	test("secret editing does not echo any secret characters", async () => {
		const f = fixture();
		try {
			const line = f.keyboard.readSecretLine("Password: ");
			f.input.write("super-secretX\x7f\r");
			expect(await line).toBe("super-secret");
			expect(f.output()).toBe("\x1b[0mPassword: \x1b[0m\r\n");
		} finally {
			f.keyboard.close();
		}
	});
	test("line editing echoes and cancellation rejects without revealing input", async () => {
		const f = fixture();
		try {
			const line = f.keyboard.readLine();
			f.input.write("ab\x7fc\r");
			expect(await line).toBe("ac");
			expect(f.output()).toContain("ab\x1b[1D\x1b[Kc");
			const secret = f.keyboard.readSecretLine();
			f.input.write("hidden\x03");
			await expect(secret).rejects.toThrow("Input cancelled");
			expect(f.output()).not.toContain("hidden");
		} finally {
			f.keyboard.close();
		}
	});
	test("close restores previous TTY state and rejects pending readers", async () => {
		const input: TerminalInput = new PassThrough();
		input.isTTY = true;
		input.isRaw = false;
		const states: boolean[] = [];
		input.setRawMode = (enabled) => {
			input.isRaw = enabled;
			states.push(enabled);
		};
		const keyboard = new KeyboardInput(input, { write() {} });
		const pending = keyboard.readKey();
		keyboard.close();
		keyboard.close();
		await expect(pending).rejects.toThrow("closed");
		expect(states).toEqual([true, false]);
	});
	test("EOF rejects pending reads", async () => {
		const f = fixture();
		try {
			const pending = f.keyboard.readKey();
			f.input.end();
			await expect(pending).rejects.toThrow("Input ended");
		} finally {
			f.keyboard.close();
		}
	});
});

test("redaction is literal, longest-first, handles short values and leaves replacement text intact", () => {
	expect(redact("abc ab a [x]", ["a", "abc", "ab", "[x]", ""])).toBe("[redacted] [redacted] [redacted] [redacted]");
	expect(redact("safe", [])).toBe("safe");
	expect(terminalDimensions().width).toBeGreaterThan(0);
});
