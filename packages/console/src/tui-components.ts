import type { ANSIRenderer as CoreRenderer, KeyboardInput } from "./tui-core";

type KeyPressEvent = Awaited<ReturnType<KeyboardInput["readKey"]>>;
type ANSIRenderer = Pick<CoreRenderer, "moveCursor" | "write">;

/** One-based terminal coordinates. */
export interface Position {
	row: number;
	col: number;
}

export interface Component<T = unknown> {
	render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void;
	handleKey(event: KeyPressEvent): "handled" | "unhandled";
	getValue(): T;
	setValue(value: T): void;
}

function singleLine(value: string): string {
	return Array.from(value)
		.filter((character) => {
			const code = character.codePointAt(0) ?? 0;
			return code >= 32 && (code < 127 || code > 159);
		})
		.join("");
}

function size(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new RangeError(`${name} must be a non-negative safe integer`);
	}
	return value;
}

function clip(value: string, width: number): string {
	let result = "";
	let used = 0;
	for (const character of value) {
		const cells = Bun.stringWidth(character);
		if (used + cells > width) break;
		result += character;
		used += cells;
	}
	return result;
}

function line(renderer: ANSIRenderer, at: Position, text: string, width?: number, selected = false): void {
	const visible = width === undefined ? text : clip(text, size(width, "width"));
	const padded = width === undefined ? visible : visible + " ".repeat(width - Bun.stringWidth(visible));
	renderer.moveCursor(at.row, at.col);
	renderer.write(padded, selected ? "black" : undefined, selected ? "cyan" : undefined);
}

export interface TextInputOptions {
	label?: string;
	initial?: string;
	maxLength?: number;
	hidden?: boolean;
}

export class TextInput implements Component<string> {
	private value: string[] = [];
	private cursor = 0;
	private readonly label: string;
	private readonly maxLength: number;
	private readonly hidden: boolean;

	constructor(options: TextInputOptions = {}) {
		this.label = singleLine(options.label ?? "");
		this.maxLength = options.maxLength === undefined ? Number.MAX_SAFE_INTEGER : size(options.maxLength, "maxLength");
		this.hidden = options.hidden ?? false;
		this.setValue(options.initial ?? "");
	}

	getValue(): string {
		return this.value.join("");
	}

	setValue(value: string): void {
		this.value = Array.from(singleLine(value)).slice(0, this.maxLength);
		this.cursor = this.value.length;
	}

	handleKey(event: KeyPressEvent): "handled" | "unhandled" {
		if (event.isCtrl || event.isAlt || event.key === "tab") return "unhandled";
		switch (event.key) {
			case "left":
				this.cursor = Math.max(0, this.cursor - 1);
				return "handled";
			case "right":
				this.cursor = Math.min(this.value.length, this.cursor + 1);
				return "handled";
			case "home":
				this.cursor = 0;
				return "handled";
			case "end":
				this.cursor = this.value.length;
				return "handled";
			case "backspace":
				if (this.cursor > 0) this.value.splice(--this.cursor, 1);
				return "handled";
			case "delete":
				this.value.splice(this.cursor, 1);
				return "handled";
			default:
				if (Array.from(event.key).length !== 1 || singleLine(event.key) !== event.key) return "unhandled";
				if (this.value.length < this.maxLength) this.value.splice(this.cursor++, 0, event.key);
				return "handled";
		}
	}

	render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void {
		if (height !== undefined && size(height, "height") === 0) return;
		const prefix = this.label ? `${this.label} ` : "";
		const displayed = this.hidden ? this.value.map(() => "*") : this.value;
		const before = displayed.slice(0, this.cursor).join("");
		const after = displayed.slice(this.cursor).join("");
		let text = `${prefix}${before}|${after}`;
		if (width !== undefined) {
			size(width, "width");
			// Scroll the content horizontally so the caret remains visible.
			const visiblePrefix = clip(prefix, Math.max(0, width - 1));
			const available = Math.max(0, width - Bun.stringWidth(visiblePrefix) - 1);
			let tail = before;
			while (Bun.stringWidth(tail) > available) tail = Array.from(tail).slice(1).join("");
			text = `${visiblePrefix}${tail}|${after}`;
		}
		line(renderer, at, text, width);
	}
}

export interface SelectInputOptions<T> {
	label?: string;
	items: readonly { label: string; value: T }[];
	initial?: T;
	multiSelect?: boolean;
	initialSelected?: readonly T[];
}

export class SelectInput<T = string> implements Component<T | undefined> {
	private readonly label: string;
	private readonly items: { label: string; value: T }[];
	private selected = 0;
	private confirmed = false;
	private readonly multiSelect: boolean;
	private readonly checked = new Set<number>();

	constructor(options: SelectInputOptions<T>) {
		this.label = singleLine(options.label ?? "");
		this.items = options.items.map((item) => ({ label: singleLine(item.label), value: item.value }));
		this.multiSelect = options.multiSelect ?? false;
		if (options.initial !== undefined) this.setValue(options.initial);
		for (const value of options.initialSelected ?? []) {
			const index = this.items.findIndex((item) => Object.is(item.value, value));
			if (index < 0) throw new RangeError("Value is not a selection item");
			this.checked.add(index);
		}
	}

	getValue(): T | undefined {
		return this.items[this.selected]?.value;
	}
	isConfirmed(): boolean {
		return this.confirmed;
	}
	getSelectedValues(): T[] {
		return this.items.filter((_, index) => this.checked.has(index)).map((item) => item.value);
	}

	setValue(value: T | undefined): void {
		const index = this.items.findIndex((item) => Object.is(item.value, value));
		if (index < 0) {
			if (value === undefined && this.items.length === 0) return;
			throw new RangeError("Value is not a selection item");
		}
		this.selected = index;
		this.confirmed = false;
	}

	handleKey(event: KeyPressEvent): "handled" | "unhandled" {
		if (event.isCtrl || event.isAlt || this.items.length === 0) return "unhandled";
		if (event.key === "enter") {
			this.confirmed = true;
			return "handled";
		}
		if (this.multiSelect && event.key === " ") {
			if (this.checked.has(this.selected)) this.checked.delete(this.selected);
			else this.checked.add(this.selected);
			this.confirmed = false;
			return "handled";
		}
		if (event.key !== "up" && event.key !== "down") return "unhandled";
		this.selected = Math.max(0, Math.min(this.items.length - 1, this.selected + (event.key === "up" ? -1 : 1)));
		this.confirmed = false;
		return "handled";
	}

	render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void {
		const rows = height === undefined ? this.items.length + (this.label ? 1 : 0) : size(height, "height");
		let offset = 0;
		if (this.label && rows > 0) line(renderer, { row: at.row + offset++, col: at.col }, this.label, width);
		const count = Math.max(0, rows - offset);
		const start = Math.max(0, this.selected - count + 1);
		for (let index = start; index < Math.min(this.items.length, start + count); index++) {
			const check = this.multiSelect ? `[${this.checked.has(index) ? "x" : " "}] ` : "";
			line(
				renderer,
				{ row: at.row + offset++, col: at.col },
				`${index === this.selected ? ">" : " "} ${check}${this.items[index]?.label ?? ""}`,
				width,
				index === this.selected,
			);
		}
	}
}

export interface ProgressBarOptions {
	label?: string;
	width?: number;
}

export class ProgressBar implements Component<number> {
	private value = 0;
	private readonly label: string;
	private readonly width: number;

	constructor(options: ProgressBarOptions = {}) {
		this.label = singleLine(options.label ?? "");
		this.width = size(options.width ?? 20, "width");
	}
	getValue(): number {
		return this.value;
	}
	setValue(value: number): void {
		if (!Number.isFinite(value)) throw new RangeError("Progress must be finite");
		this.value = Math.max(0, Math.min(100, value));
	}
	handleKey(_event: KeyPressEvent): "unhandled" {
		return "unhandled";
	}
	render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void {
		if (height !== undefined && size(height, "height") === 0) return;
		const prefix = this.label ? `${this.label} ` : "";
		const suffix = `] ${Math.round(this.value)}%`;
		const barWidth =
			width === undefined
				? this.width
				: Math.max(0, Math.min(this.width, size(width, "width") - Bun.stringWidth(prefix) - 1 - suffix.length));
		const filled = Math.round((barWidth * this.value) / 100);
		line(renderer, at, `${prefix}[${"█".repeat(filled)}${"░".repeat(barWidth - filled)}${suffix}`, width);
	}
}

export type TableCell = string | number | boolean | null | undefined;
export interface TableOptions {
	columns: readonly { header: string; width?: number }[];
	rows: readonly (readonly TableCell[])[];
}

export class Table implements Component<readonly (readonly TableCell[])[]> {
	private readonly columns: { header: string; width?: number }[];
	private rows: TableCell[][] = [];

	constructor(options: TableOptions) {
		this.columns = options.columns.map((column) => ({
			header: singleLine(column.header),
			width: column.width === undefined ? undefined : size(column.width, "column width"),
		}));
		this.setValue(options.rows);
	}
	getValue(): TableCell[][] {
		return this.rows.map((row) => [...row]);
	}
	setValue(value: readonly (readonly TableCell[])[]): void {
		this.rows = value.map((row) => [...row]);
	}
	handleKey(_event: KeyPressEvent): "unhandled" {
		return "unhandled";
	}
	render(renderer: ANSIRenderer, at: Position, width?: number, height?: number): void {
		const rows = [
			this.columns.map((column) => column.header),
			...this.rows.map((row) => this.columns.map((_, index) => singleLine(String(row[index] ?? "")))),
		];
		const widths = this.columns.map(
			(column, index) => column.width ?? rows.reduce((max, row) => Math.max(max, Bun.stringWidth(row[index] ?? "")), 0),
		);
		const count = height === undefined ? rows.length : Math.min(rows.length, size(height, "height"));
		for (let index = 0; index < count; index++) {
			const text = this.columns
				.map((_, col) => {
					const cell = clip(rows[index]?.[col] ?? "", widths[col] ?? 0);
					return cell + " ".repeat((widths[col] ?? 0) - Bun.stringWidth(cell));
				})
				.join(" | ");
			line(renderer, { row: at.row + index, col: at.col }, text, width);
		}
	}
}
