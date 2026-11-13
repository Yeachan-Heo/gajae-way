import { describe, expect, test } from "bun:test";
import { ProgressBar, SelectInput, Table, TextInput } from "../src/tui-components";
import { ANSIRenderer, type KeyPressEvent } from "../src/tui-core";

function key(key: string, isCtrl = false): KeyPressEvent {
	return { key, code: key, isCtrl, isAlt: false, isShift: false };
}
function capture() {
	const chunks: string[] = [];
	return { renderer: new ANSIRenderer({ write: (text) => chunks.push(text) }), output: () => chunks.join("") };
}

describe("TextInput", () => {
	test("edits at a Unicode cursor, enforces maxLength, and leaves Tab for focus navigation", () => {
		const input = new TextInput({ initial: "A😀B", maxLength: 3 });
		input.handleKey(key("left"));
		input.handleKey(key("backspace"));
		input.handleKey(key("界"));
		input.handleKey(key("!"));
		expect(input.getValue()).toBe("A界B");
		input.handleKey(key("delete"));
		expect(input.getValue()).toBe("A界");
		expect(input.handleKey(key("tab"))).toBe("unhandled");
		expect(input.handleKey(key("c", true))).toBe("unhandled");
		input.setValue("12345\n");
		expect(input.getValue()).toBe("123");
	});
	test("masks secrets and shows caret at positioned, clipped viewport", () => {
		const input = new TextInput({ label: "Key", initial: "very-secret", hidden: true });
		const screen = capture();
		input.render(screen.renderer, { row: 3, col: 7 }, 9);
		expect(screen.output()).toContain("\x1b[3;7H");
		expect(screen.output()).toContain("Key ****|");
		expect(screen.output()).not.toContain("very-secret");
		expect(input.getValue()).toBe("very-secret");
	});
	test("zero-height render is silent and controls cannot inject terminal sequences", () => {
		const screen = capture();
		const input = new TextInput({ initial: "ok\x1b[2J\n" });
		input.render(screen.renderer, { row: 1, col: 1 }, 10, 0);
		expect(screen.output()).toBe("");
		input.render(screen.renderer, { row: 1, col: 1 });
		expect(screen.output()).not.toContain("\x1b[2J");
	});
});

describe("SelectInput", () => {
	test("navigates, confirms, highlights, and scrolls within allocated height", () => {
		const select = new SelectInput({
			label: "Choice",
			items: [
				{ label: "One", value: 1 },
				{ label: "Two", value: 2 },
			],
		});
		select.handleKey(key("down"));
		select.handleKey(key("enter"));
		expect(select.getValue()).toBe(2);
		expect(select.isConfirmed()).toBe(true);
		const screen = capture();
		select.render(screen.renderer, { row: 4, col: 2 }, 10, 2);
		expect(screen.output()).toContain("\x1b[5;2H");
		expect(screen.output()).toContain("\x1b[0;30;46m> Two");
		expect(screen.output()).not.toContain("One");
		select.setValue(1);
		expect(select.isConfirmed()).toBe(false);
		expect(() => select.setValue(3)).toThrow(RangeError);
	});
	test("empty selections remain undefined and unhandled", () => {
		const select = new SelectInput({ items: [] });
		expect(select.getValue()).toBeUndefined();
		expect(select.handleKey(key("enter"))).toBe("unhandled");
	});
	test("multi-select toggles independently of focused value", () => {
		const select = new SelectInput({
			multiSelect: true,
			initialSelected: [2],
			items: [
				{ label: "One", value: 1 },
				{ label: "Two", value: 2 },
			],
		});
		select.handleKey(key(" "));
		expect(select.getSelectedValues()).toEqual([1, 2]);
		select.handleKey(key("down"));
		select.handleKey(key(" "));
		expect(select.getSelectedValues()).toEqual([1]);
		expect(select.getValue()).toBe(2);
	});
});

describe("ProgressBar and Table", () => {
	test("renders bar proportion and clamps finite progress", () => {
		const bar = new ProgressBar({ label: "Build", width: 8 });
		bar.setValue(50);
		const screen = capture();
		bar.render(screen.renderer, { row: 2, col: 3 });
		expect(screen.output()).toContain("Build [████░░░░] 50%");
		bar.setValue(200);
		expect(bar.getValue()).toBe(100);
		bar.setValue(-1);
		expect(bar.getValue()).toBe(0);
		expect(() => bar.setValue(Number.NaN)).toThrow(RangeError);
	});
	test("table auto-sizes, clips wide cells, and respects bounds", () => {
		const table = new Table({ columns: [{ header: "Name", width: 4 }, { header: "N" }], rows: [["界界界", 123]] });
		const screen = capture();
		table.render(screen.renderer, { row: 5, col: 8 }, 10, 2);
		expect(screen.output()).toContain("Name | N  ");
		expect(screen.output()).toContain("界界 | 123");
		expect(screen.output()).toContain("\x1b[6;8H");
		const rows = table.getValue();
		rows[0]?.push("outside");
		expect(table.getValue()).toEqual([["界界界", 123]]);
	});
});
