import { emitKeypressEvents, type Key } from "node:readline";
import { Readable } from "node:stream";

export type Color = "black" | "red" | "green" | "yellow" | "blue" | "magenta" | "cyan" | "white" | "reset";
export const BLACK: Color = "black";
export const RED: Color = "red";
export const GREEN: Color = "green";
export const YELLOW: Color = "yellow";
export const BLUE: Color = "blue";
export const MAGENTA: Color = "magenta";
export const CYAN: Color = "cyan";
export const WHITE: Color = "white";
export const RESET: Color = "reset";
const colors: Record<Color, number> = {
	black: 30,
	red: 31,
	green: 32,
	yellow: 33,
	blue: 34,
	magenta: 35,
	cyan: 36,
	white: 37,
	reset: 39,
};

export interface Dimensions {
	width: number;
	height: number;
}
export interface KeyPressEvent {
	key: string;
	code: string;
	isShift: boolean;
	isCtrl: boolean;
	isAlt: boolean;
}
export interface TerminalOutput {
	write(text: string): unknown;
	flush?(): unknown;
}
export type TerminalInput = Readable & { isTTY?: boolean; isRaw?: boolean; setRawMode?(enabled: boolean): unknown };

function outputText(output: TerminalOutput, text: string): void {
	output.write(text);
	output.flush?.();
}

function printable(text: string): boolean {
	return [...text].every((char) => {
		const code = char.codePointAt(0) ?? 0;
		return code >= 32 && !(code >= 127 && code <= 159);
	});
}

export class ANSIRenderer {
	constructor(private readonly output: TerminalOutput = Bun.stdout.writer()) {}
	clear(): void {
		outputText(this.output, "\x1b[2J\x1b[H");
	}
	clearLine(): void {
		outputText(this.output, "\x1b[2K\r");
	}
	moveCursor(row: number, col: number): void {
		if (!Number.isInteger(row) || !Number.isInteger(col) || row < 1 || col < 1)
			throw new RangeError("Cursor coordinates must be positive integers");
		outputText(this.output, `\x1b[${row};${col}H`);
	}
	write(text: string, color?: Color, background?: Color, bold = false, underline = false): void {
		const codes: number[] = [0];
		if (color) codes.push(colors[color]);
		if (background) codes.push(colors[background] + 10);
		if (bold) codes.push(1);
		if (underline) codes.push(4);
		outputText(this.output, `\x1b[${codes.join(";")}m${text}\x1b[0m`);
	}
	resetStyle(): void {
		outputText(this.output, "\x1b[0m");
	}
	hideCursor(): void {
		outputText(this.output, "\x1b[?25l");
	}
	showCursor(): void {
		outputText(this.output, "\x1b[?25h");
	}
}

/** Raw-mode lifetime is owned by this reader; callers must close it in finally. */
export class KeyboardInput {
	private readonly input: TerminalInput;
	private readonly raw: TerminalInput;
	private readonly ownsInput: boolean;
	private readonly wasRaw: boolean;
	private readonly wasPaused: boolean;
	private readonly queue: KeyPressEvent[] = [];
	private readonly pending: { resolve(event: KeyPressEvent): void; reject(error: Error): void }[] = [];
	private closed = false;
	private ended = false;
	private lineActive = false;
	private failure: Error | undefined;

	constructor(
		input?: TerminalInput,
		private readonly output: TerminalOutput = Bun.stdout.writer(),
	) {
		this.ownsInput = input === undefined;
		this.input = input ?? (Readable.fromWeb(Bun.stdin.stream() as unknown as import("node:stream/web").ReadableStream<Uint8Array>) as TerminalInput);		// Bun stream -> Web Streams API -> Node Readable
		this.raw = input ?? process.stdin;
		this.wasRaw = this.raw.isRaw ?? false;
		this.wasPaused = this.input.isPaused();
		if (this.raw.isTTY) {
			if (!this.raw.setRawMode) throw new Error("Terminal does not support raw mode");
			this.raw.setRawMode(true);
		}
		emitKeypressEvents(this.input);
		this.input.on("keypress", this.onKey);
		this.input.on("end", this.onEnd);
		this.input.on("close", this.onEnd);
		this.input.on("error", this.onError);
		this.input.resume();
	}

	private readonly onKey = (text: string | undefined, key: Key): void => {
		const event: KeyPressEvent = {
			key:
				key.name === "return" || key.name === "enter"
					? "enter"
					: !key.ctrl && !key.meta && text && printable(text)
						? text
						: (key.name ?? "unknown"),
			code: key.sequence ?? text ?? "",
			isShift: key.shift ?? false,
			isCtrl: key.ctrl ?? false,
			isAlt: key.meta ?? false,
		};
		const waiter = this.pending.shift();
		if (waiter) waiter.resolve(event);
		else this.queue.push(event);
	};
	private readonly onEnd = (): void => {
		this.ended = true;
		this.rejectPending(new Error("Input ended"));
		this.restoreRaw();
	};
	private readonly onError = (): void => {
		this.failure = new Error("Terminal input failed");
		this.rejectPending(this.failure);
		this.restoreRaw();
	};
	private rejectPending(error: Error): void {
		for (const waiter of this.pending.splice(0)) waiter.reject(error);
	}
	private restoreRaw(): void {
		if (this.raw.isTTY && this.raw.isRaw !== this.wasRaw) this.raw.setRawMode?.(this.wasRaw);
	}
	private nextKey(): Promise<KeyPressEvent> {
		if (this.closed) return Promise.reject(new Error("Keyboard input is closed"));
		if (this.failure) return Promise.reject(this.failure);
		const event = this.queue.shift();
		if (event) return Promise.resolve(event);
		if (this.ended) return Promise.reject(new Error("Input ended"));
		return new Promise((resolve, reject) => this.pending.push({ resolve, reject }));
	}
	readKey(): Promise<KeyPressEvent> {
		if (this.lineActive) return Promise.reject(new Error("A line read is already active"));
		return this.nextKey();
	}
	readLine(prompt = ""): Promise<string> {
		return this.readText(prompt, false);
	}
	readSecretLine(prompt = ""): Promise<string> {
		return this.readText(prompt, true);
	}
	private async readText(prompt: string, secret: boolean): Promise<string> {
		if (this.lineActive || this.pending.length) throw new Error("An input read is already active");
		if (this.closed) throw new Error("Keyboard input is closed");
		this.lineActive = true;
		const value: string[] = [];
		try {
			outputText(this.output, `\x1b[0m${prompt}\x1b[0m`);
			while (true) {
				const event = await this.nextKey();
				if (event.isCtrl && (event.key === "c" || event.key === "d")) throw new Error("Input cancelled");
				if (event.key === "enter") {
					outputText(this.output, "\r\n");
					return value.join("");
				}
				if (event.key === "backspace") {
					if (value.length) {
						value.pop();
						if (!secret) outputText(this.output, "\x1b[1D\x1b[K");
					}
				} else if (!event.isCtrl && !event.isAlt && event.code && printable(event.code)) {
					value.push(...event.code);
					if (!secret) outputText(this.output, event.code);
				}
			}
		} finally {
			value.fill("");
			this.lineActive = false;
		}
	}
	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.input.off("keypress", this.onKey);
		this.input.off("end", this.onEnd);
		this.input.off("close", this.onEnd);
		this.input.off("error", this.onError);
		this.restoreRaw();
		this.input.pause();
		if (this.ownsInput) this.input.destroy();
		if (!this.wasPaused && this.input.listenerCount("data") > 1) this.input.resume();
		this.queue.length = 0;
		this.rejectPending(new Error("Keyboard input is closed"));
	}
}

export function terminalDimensions(): Dimensions {
	return {
		width: process.stdout.columns > 0 ? process.stdout.columns : 80,
		height: process.stdout.rows > 0 ? process.stdout.rows : 24,
	};
}
export function sleep(ms: number): Promise<void> {
	return Bun.sleep(ms);
}

/** Uses setup's literal matching, including short secrets and overlapping values. */
export function redact(text: string, secrets: Iterable<string>): string {
	const values = [...new Set(secrets)].filter(Boolean).sort((a, b) => b.length - a.length);
	if (!values.length) return text;
	const pattern = values.map((value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
	return text.replace(new RegExp(pattern, "g"), "[redacted]");
}
