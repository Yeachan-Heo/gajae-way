import { stdin, stdout } from "node:process";
import type { ConsoleEventFrame, ConsoleTerminal } from "../console";
import { ConsoleInputEditor } from "./editor";
import { ConsoleTuiRenderer } from "./renderer";

export const MAX_RAW_CONSOLE_QUEUED_LINES = 16;
export const MAX_RAW_CONSOLE_QUEUED_BYTES = 64 * 1024;
export const MAX_RAW_CONSOLE_LINE_BYTES = 8 * 1024;

const ALTERNATE_SCREEN_ENTER = "\x1b[?1049h\x1b[?2004h\x1b[?25l";
const ALTERNATE_SCREEN_EXIT = "\x1b[?2026l\x1b[?25h\x1b[?2004l\x1b[?1049l";
const BRACKETED_PASTE_START = "\x1b[200~";
const BRACKETED_PASTE_END = "\x1b[201~";
const ESCAPE_TIMEOUT_MS = 25;

export type RawConsoleRefusalCause = "oversized-line" | "queue-full-lines" | "queue-full-bytes";

type RawConsoleRefusalCounts = Record<RawConsoleRefusalCause, number>;

interface RawConsoleRefusal {
	readonly counts: RawConsoleRefusalCounts;
	revision: number;
}

export interface RawConsoleInputStream {
	readonly isTTY?: boolean;
	readonly isRaw?: boolean;
	setEncoding(encoding: BufferEncoding): unknown;
	setRawMode?(mode: boolean): unknown;
	resume(): unknown;
	pause(): unknown;
	on(event: "data", listener: (chunk: string | Buffer) => void): unknown;
	off(event: "data", listener: (chunk: string | Buffer) => void): unknown;
}

export interface RawConsoleOutputStream {
	readonly isTTY?: boolean;
	readonly columns?: number;
	readonly rows?: number;
	write(text: string, callback: (error?: Error | null) => void): boolean;
	once(event: "drain", listener: () => void): unknown;
	on?(event: "resize", listener: () => void): unknown;
	off?(event: "resize", listener: () => void): unknown;
}

export interface RawConsoleTerminalOptions {
	readonly input?: RawConsoleInputStream;
	readonly output?: RawConsoleOutputStream;
}

function writeToStream(stream: RawConsoleOutputStream, text: string): Promise<void> {
	if (!text) return Promise.resolve();
	return new Promise((resolve, reject) => {
		let callbackDone = false;
		let drainDone = true;
		let writeReturned = false;
		let settled = false;
		const finish = () => {
			if (settled || !writeReturned || !callbackDone || !drainDone) return;
			settled = true;
			resolve();
		};
		const fail = (error: Error) => {
			if (settled) return;
			settled = true;
			reject(error);
		};
		const onDrain = () => {
			drainDone = true;
			finish();
		};
		try {
			const accepted = stream.write(text, (error) => {
				if (error) {
					fail(error);
					return;
				}
				callbackDone = true;
				finish();
			});
			drainDone = accepted;
			writeReturned = true;
			if (!accepted) stream.once("drain", onDrain);
			finish();
		} catch (error) {
			fail(error instanceof Error ? error : new Error(String(error)));
		}
	});
}

function firstTerminalDimension(value: number | undefined, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.trunc(value) : fallback;
}

/**
 * Full-screen terminal adapter for the owner console. It owns alternate-screen
 * setup/teardown, a durable in-memory transcript, a bottom-pinned status rail,
 * and a single-line editor. All RPC-originated text arrives through
 * `writeTrusted`, after ConsoleOutput has serialized and sanitized the frame.
 */
export class RawConsoleTerminal implements ConsoleTerminal {
	readonly #input: RawConsoleInputStream;
	readonly #output: RawConsoleOutputStream;
	readonly #editor = new ConsoleInputEditor();
	readonly #renderer = new ConsoleTuiRenderer();
	readonly #queuedLines: Array<{ readonly text: string; readonly bytes: number }> = [];
	readonly #exitListeners = new Set<() => void>();
	#queuedBytes = 0;
	#pendingRefusal: RawConsoleRefusal | undefined;
	#discardingOversizeLine = false;
	#discardingRefusedLine = false;
	#prompt = "gajaeway> ";
	#resolveLine: ((line: string | undefined) => void) | undefined;
	#exitRequested = false;
	#inputPaused = false;
	#editorDirty = true;
	#rawPublicationPending = false;
	#writeTail: Promise<void> = Promise.resolve();
	#closed = false;
	#alternateScreenEntered = false;
	#initialRender = true;
	#restoredTerminal = false;
	#inputBuffer = "";
	#bracketedPaste = false;
	#escapeTimer: ReturnType<typeof setTimeout> | undefined;
	readonly #wasRaw: boolean;
	readonly #isProcessTerminal: boolean;
	#exitRestoreListener: (() => void) | undefined;
	#crashRestoreListener: ((error: Error) => void) | undefined;
	readonly #signalRestoreListeners = new Map<NodeJS.Signals, () => void>();

	constructor(options: RawConsoleTerminalOptions = {}) {
		this.#input = options.input ?? stdin;
		this.#output = options.output ?? stdout;
		if (!this.#input.isTTY || !this.#output.isTTY || !this.#input.setRawMode) {
			throw new Error("gajaeway console requires an interactive TTY on stdin and stdout.");
		}
		this.#wasRaw = this.#input.isRaw === true;
		this.#isProcessTerminal = this.#input === stdin && this.#output === stdout;
		this.#renderer.resize(
			firstTerminalDimension(this.#output.columns, 80),
			firstTerminalDimension(this.#output.rows, 24),
		);
		this.#input.setEncoding("utf8");
		this.#input.setRawMode(true);
		this.#input.resume();
		this.#input.on("data", this.onData);
		this.#output.on?.("resize", this.onResize);
		this.#installEmergencyRestore();
		this.#scheduleRawPublication();
	}

	get queuedLineCount(): number {
		return this.#queuedLines.length;
	}

	get queuedInputBytes(): number {
		return this.#queuedBytes;
	}

	/** Bytes retained for the currently edited raw input line. */
	get bufferedInputBytes(): number {
		return this.#editor.byteLength;
	}

	get inputPaused(): boolean {
		return this.#inputPaused;
	}

	/** At most one coalesced editor redraw is retained while stdout is busy. */
	get pendingEchoRedrawCount(): number {
		return this.#editorDirty ? 1 : 0;
	}

	/** At most one refusal diagnostic is retained until publication or episode end. */
	get pendingRefusalPublicationCount(): number {
		return this.#pendingRefusal ? 1 : 0;
	}

	/** The adapter has one serialized renderer publication at a time. */
	get rawPublicationPending(): boolean {
		return this.#rawPublicationPending;
	}

	get alternateScreenActive(): boolean {
		return this.#alternateScreenEntered && !this.#restoredTerminal;
	}

	onExitRequested(listener: () => void): () => void {
		this.#exitListeners.add(listener);
		if (this.#exitRequested) listener();
		return () => this.#exitListeners.delete(listener);
	}

	setDeliveryState(state: "fenced" | "ready" | "unavailable" | "stopping"): void {
		this.#renderer.setDeliveryState(state);
		this.#requestEditorRender();
	}

	observeEvent(event: ConsoleEventFrame): void {
		this.#renderer.observeEvent(event);
	}

	async writeTrusted(text: string): Promise<void> {
		if (this.#closed) throw new Error("Console terminal is closed.");
		if (text.startsWith("Gateway status\n")) this.#renderer.setStatusSummary(text);
		if (text.startsWith("Delivery unavailable;")) this.#renderer.setDeliveryState("unavailable");
		if (text) this.#renderer.appendFrame(text);
		this.#editorDirty = false;
		try {
			await this.#enqueuePublication(async () => await this.#publishRendererFrame());
		} catch (error) {
			this.close();
			throw error;
		}
	}

	async readLine(prompt: string): Promise<string | undefined> {
		if (this.#closed || this.#exitRequested) return undefined;
		if (this.#resolveLine) throw new Error("Console already has a pending input line.");
		this.#prompt = prompt;
		const queued = this.#queuedLines.shift();
		if (queued) {
			this.#queuedBytes -= queued.bytes;
			this.#requestEditorRender();
			return queued.text;
		}
		const line = new Promise<string | undefined>((resolve) => {
			this.#resolveLine = resolve;
		});
		this.#requestEditorRender();
		return await line;
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#inputPaused = true;
		this.#editorDirty = false;
		this.#pendingRefusal = undefined;
		if (this.#escapeTimer) clearTimeout(this.#escapeTimer);
		this.#input.off("data", this.onData);
		this.#output.off?.("resize", this.onResize);
		this.#input.pause();
		this.#input.setRawMode?.(this.#wasRaw);
		this.#finishLine(undefined);
		this.#removeEmergencyRestore();
		this.#restoreTerminal();
	}

	private onData = (chunk: string | Buffer): void => {
		if (this.#closed) return;
		this.#inputBuffer += String(chunk);
		if (this.#inputBuffer.includes("\u0003") || this.#inputBuffer.includes("\u0004")) {
			this.#inputBuffer = "";
			this.#requestExit();
			return;
		}
		this.#drainInputBuffer();
	};

	private onResize = (): void => {
		if (this.#closed) return;
		this.#renderer.resize(
			firstTerminalDimension(this.#output.columns, this.#renderer.columns),
			firstTerminalDimension(this.#output.rows, this.#renderer.rows),
		);
		this.#requestEditorRender();
	};

	#drainInputBuffer(): void {
		while (!this.#closed && this.#inputBuffer.length > 0) {
			if (this.#bracketedPaste) {
				const end = this.#inputBuffer.indexOf(BRACKETED_PASTE_END);
				if (end >= 0) {
					const content = this.#inputBuffer.slice(0, end);
					this.#inputBuffer = this.#inputBuffer.slice(end + BRACKETED_PASTE_END.length);
					this.#bracketedPaste = false;
					this.#processPastedText(content);
					continue;
				}
				const retained = this.#prefixSuffixLength(this.#inputBuffer, BRACKETED_PASTE_END);
				const content = this.#inputBuffer.slice(0, this.#inputBuffer.length - retained);
				this.#inputBuffer = this.#inputBuffer.slice(this.#inputBuffer.length - retained);
				this.#processPastedText(content);
				return;
			}
			if (this.#inputBuffer.startsWith("\x1b")) {
				if (this.#inputBuffer.startsWith(BRACKETED_PASTE_START)) {
					this.#inputBuffer = this.#inputBuffer.slice(BRACKETED_PASTE_START.length);
					this.#bracketedPaste = true;
					continue;
				}
				if (BRACKETED_PASTE_START.startsWith(this.#inputBuffer)) return;
				const escape = this.#takeEscapeSequence();
				if (escape === undefined) return;
				if (escape === "") continue;
				this.#handleEscapeSequence(escape);
				continue;
			}
			if (this.#processPrintablePrefix()) continue;
			const character = this.#inputBuffer[Symbol.iterator]().next().value as string;
			this.#inputBuffer = this.#inputBuffer.slice(character.length);
			this.#processInputCharacter(character);
		}
	}

	#takeEscapeSequence(): string | undefined {
		const value = this.#inputBuffer;
		const exactSequences = [
			"\x1b[A",
			"\x1b[B",
			"\x1b[C",
			"\x1b[D",
			"\x1b[H",
			"\x1b[F",
			"\x1bOH",
			"\x1bOF",
			"\x1bOA",
			"\x1bOB",
			"\x1bOC",
			"\x1bOD",
			"\x1b[1~",
			"\x1b[4~",
			"\x1b[3~",
			"\x1b[5~",
			"\x1b[6~",
			"\x1b[1;3D",
			"\x1b[1;3C",
			"\x1b[1;5D",
			"\x1b[1;5C",
			"\x1b[3;5~",
			"\x1b\x7f",
			"\x1bb",
			"\x1bd",
		];
		for (const sequence of exactSequences) {
			if (value.startsWith(sequence)) {
				this.#inputBuffer = value.slice(sequence.length);
				return sequence;
			}
		}
		if (exactSequences.some((sequence) => sequence.startsWith(value))) {
			this.#armEscapeTimer();
			return undefined;
		}
		if (value.startsWith("\x1b[")) {
			const complete = /^\x1b\[[0-?]*[ -/]*[@-~]/u.exec(value);
			if (complete) {
				this.#inputBuffer = value.slice(complete[0].length);
				return complete[0];
			}
			if (value.length <= 64) return undefined;
		}
		if (value.startsWith("\x1bO") && value.length < 3) {
			this.#armEscapeTimer();
			return undefined;
		}
		if (value.length === 1) {
			this.#armEscapeTimer();
			return undefined;
		}
		this.#inputBuffer = value.slice(1);
		return "";
	}

	#armEscapeTimer(): void {
		if (this.#escapeTimer) return;
		this.#escapeTimer = setTimeout(() => {
			this.#escapeTimer = undefined;
			if (!this.#inputBuffer.startsWith("\x1b")) return;
			this.#inputBuffer = this.#inputBuffer.slice(1);
			this.#drainInputBuffer();
		}, ESCAPE_TIMEOUT_MS);
	}

	#handleEscapeSequence(sequence: string): void {
		switch (sequence) {
			case "\x1b[A":
			case "\x1bOA":
				this.#editor.historyPrevious();
				break;
			case "\x1b[B":
			case "\x1bOB":
				this.#editor.historyNext();
				break;
			case "\x1b[C":
			case "\x1bOC":
				this.#editor.moveRight();
				break;
			case "\x1b[D":
			case "\x1bOD":
				this.#editor.moveLeft();
				break;
			case "\x1b[H":
			case "\x1bOH":
			case "\x1b[1~":
				this.#editor.moveStart();
				break;
			case "\x1b[F":
			case "\x1bOF":
			case "\x1b[4~":
				this.#editor.moveEnd();
				break;
			case "\x1b[3~":
				this.#editor.deleteForward();
				break;
			case "\x1b[1;3D":
			case "\x1b[1;5D":
			case "\x1bb":
				this.#editor.moveWordLeft();
				break;
			case "\x1b[1;3C":
			case "\x1b[1;5C":
				this.#editor.moveWordRight();
				break;
			case "\x1b[3;5~":
			case "\x1bd":
				this.#editor.deleteWordForward();
				break;
			case "\x1b\x7f":
				this.#editor.deleteWordBackward();
				break;
			case "\x1b[5~":
				this.#renderer.scrollBy(Math.max(1, this.#renderer.rows - 4));
				break;
			case "\x1b[6~":
				this.#renderer.scrollBy(-Math.max(1, this.#renderer.rows - 4));
				break;
			default:
				return;
		}
		this.#requestEditorRender();
	}

	#processPastedText(value: string): void {
		let offset = 0;
		while (offset < value.length) {
			const suffix = value.slice(offset);
			const control = this.#firstControlIndex(suffix);
			if (control === -1) {
				this.#processPrintableText(suffix);
				return;
			}
			if (control > 0) {
				this.#processPrintableText(suffix.slice(0, control));
				offset += control;
				continue;
			}
			const character = suffix[Symbol.iterator]().next().value as string;
			offset += character.length;
			if (character === "\u0003" || character === "\u0004") {
				this.#requestExit();
				return;
			}
			this.#processInputCharacter(character);
			if (this.#closed || this.#exitRequested) return;
		}
	}

	#processPrintablePrefix(): boolean {
		const control = this.#firstControlIndex(this.#inputBuffer);
		if (control === 0) return false;
		const end = control === -1 ? this.#inputBuffer.length : control;
		this.#processPrintableText(this.#inputBuffer.slice(0, end));
		this.#inputBuffer = this.#inputBuffer.slice(end);
		return true;
	}

	#firstControlIndex(value: string): number {
		for (let index = 0; index < value.length; index += 1) {
			const code = value.charCodeAt(index);
			if (code < 0x20 || code === 0x7f) return index;
		}
		return -1;
	}

	#processPrintableText(text: string): void {
		if (!text || this.#discardingOversizeLine || this.#discardingRefusedLine) return;
		const queueFullCause = this.#resolveLine ? undefined : this.#queueFullRefusalCause(0);
		if (queueFullCause) {
			this.#discardingRefusedLine = true;
			this.#editor.clear();
			this.#reportInputRefusal(queueFullCause);
			return;
		}
		if (this.#editor.byteLength + Buffer.byteLength(text) > MAX_RAW_CONSOLE_LINE_BYTES) {
			this.#discardingOversizeLine = true;
			this.#editor.clear();
			this.#editorDirty = false;
			this.#reportInputRefusal("oversized-line");
			return;
		}
		this.#editor.insert(text);
		this.#requestEditorRender();
	}

	#processInputCharacter(character: string): void {
		if (character === "\r" || character === "\n") {
			if (this.#discardingOversizeLine || this.#discardingRefusedLine) {
				this.#discardingOversizeLine = false;
				this.#discardingRefusedLine = false;
				this.#editor.clear();
				this.#requestEditorRender();
				return;
			}
			this.#submitEditor();
			return;
		}
		if (character === "\u007f" || character === "\b") {
			if (this.#discardingOversizeLine || this.#discardingRefusedLine) return;
			this.#editor.backspace();
			this.#requestEditorRender();
			return;
		}
		switch (character) {
			case "\u0001":
				this.#editor.moveStart();
				this.#requestEditorRender();
				return;
			case "\u0005":
				this.#editor.moveEnd();
				this.#requestEditorRender();
				return;
			case "\u0017":
				this.#editor.deleteWordBackward();
				this.#requestEditorRender();
				return;
			case "\u0015":
				this.#editor.deleteToStart();
				this.#requestEditorRender();
				return;
			case "\u000b":
				this.#editor.deleteToEnd();
				this.#requestEditorRender();
				return;
		}
		if (character < " ") return;
		this.#processPrintableText(character);
	}

	#submitEditor(): void {
		const line = this.#editor.value;
		if (this.#acceptLine(line)) {
			if (line) this.#renderer.appendFrame(`You:\n${line}\n`);
			this.#editor.takeSubmission();
		} else {
			this.#editor.clear();
		}
		this.#requestEditorRender();
	}

	#acceptLine(line: string): boolean {
		if (this.#resolveLine) {
			this.#finishLine(line);
			return true;
		}
		const bytes = Buffer.byteLength(line);
		const queueFullCause = this.#queueFullRefusalCause(bytes);
		if (queueFullCause) {
			this.#reportInputRefusal(queueFullCause);
			return false;
		}
		this.#queuedLines.push({ text: line, bytes });
		this.#queuedBytes += bytes;
		return true;
	}

	#queueFullRefusalCause(additionalBytes: number): "queue-full-lines" | "queue-full-bytes" | undefined {
		if (this.#queuedLines.length >= MAX_RAW_CONSOLE_QUEUED_LINES) return "queue-full-lines";
		if (this.#queuedBytes + additionalBytes > MAX_RAW_CONSOLE_QUEUED_BYTES) return "queue-full-bytes";
		return undefined;
	}

	#requestExit(): void {
		if (this.#exitRequested) return;
		this.#exitRequested = true;
		this.#editorDirty = false;
		this.#pendingRefusal = undefined;
		this.#editor.clear();
		this.#refreshInputFlow();
		this.#finishLine(undefined);
		for (const listener of [...this.#exitListeners]) listener();
	}

	#refreshInputFlow(): void {
		const shouldPause = this.#closed || this.#exitRequested;
		if (shouldPause === this.#inputPaused) return;
		this.#inputPaused = shouldPause;
		if (shouldPause) this.#input.pause();
		else this.#input.resume();
	}

	#requestEditorRender(): void {
		if (this.#closed || this.#exitRequested) return;
		this.#editorDirty = true;
		this.#scheduleRawPublication();
	}

	#reportInputRefusal(cause: RawConsoleRefusalCause): void {
		if (this.#closed || this.#exitRequested) return;
		const refusal = this.#pendingRefusal ?? {
			counts: {
				"oversized-line": 0,
				"queue-full-lines": 0,
				"queue-full-bytes": 0,
			},
			revision: 0,
		};
		refusal.counts[cause] += 1;
		refusal.revision += 1;
		this.#pendingRefusal = refusal;
		this.#scheduleRawPublication();
	}

	#snapshotRefusalCounts(counts: RawConsoleRefusalCounts): RawConsoleRefusalCounts {
		return {
			"oversized-line": counts["oversized-line"],
			"queue-full-lines": counts["queue-full-lines"],
			"queue-full-bytes": counts["queue-full-bytes"],
		};
	}

	#renderInputRefusal(counts: RawConsoleRefusalCounts): string {
		const causes: string[] = [];
		if (counts["oversized-line"] > 0) {
			causes.push(
				`oversized-line=${counts["oversized-line"]} (Input line exceeds ${MAX_RAW_CONSOLE_LINE_BYTES} bytes and was refused.)`,
			);
		}
		if (counts["queue-full-lines"] > 0) {
			causes.push(
				`queue-full-lines=${counts["queue-full-lines"]} (Input queue is full (${MAX_RAW_CONSOLE_QUEUED_LINES} lines / ${MAX_RAW_CONSOLE_QUEUED_BYTES} bytes); additional pasted input was refused.)`,
			);
		}
		if (counts["queue-full-bytes"] > 0) {
			causes.push(
				`queue-full-bytes=${counts["queue-full-bytes"]} (Input queue byte capacity is full (${MAX_RAW_CONSOLE_QUEUED_BYTES} bytes); additional pasted input was refused.)`,
			);
		}
		return `Input refused: ${causes.join("; ")}\n`;
	}

	#settlePublishedRefusal(
		refusal: RawConsoleRefusal,
		publishedCounts: RawConsoleRefusalCounts,
		publishedRevision: number,
	): void {
		if (this.#pendingRefusal !== refusal) return;
		if (refusal.revision === publishedRevision) {
			this.#pendingRefusal = undefined;
			return;
		}
		for (const cause of ["oversized-line", "queue-full-lines", "queue-full-bytes"] as const) {
			refusal.counts[cause] -= publishedCounts[cause];
		}
	}

	#scheduleRawPublication(): void {
		if (this.#closed || this.#rawPublicationPending || (!this.#editorDirty && !this.#pendingRefusal)) return;
		this.#rawPublicationPending = true;
		void this.#enqueuePublication(async () => await this.#publishRawPublication()).catch(() => this.close());
	}

	async #publishRawPublication(): Promise<void> {
		try {
			const refusal = this.#pendingRefusal;
			if (refusal) {
				const counts = this.#snapshotRefusalCounts(refusal.counts);
				const revision = refusal.revision;
				this.#renderer.appendFrame(this.#renderInputRefusal(counts));
				await this.#publishRendererFrame();
				this.#settlePublishedRefusal(refusal, counts, revision);
				return;
			}
			if (!this.#editorDirty || this.#closed || this.#exitRequested) return;
			this.#editorDirty = false;
			await this.#publishRendererFrame();
		} finally {
			this.#rawPublicationPending = false;
			this.#scheduleRawPublication();
		}
	}

	async #publishRendererFrame(): Promise<void> {
		if (this.#closed) return;
		const prefix = this.#initialRender ? ALTERNATE_SCREEN_ENTER : "";
		this.#initialRender = false;
		if (prefix) this.#alternateScreenEntered = true;
		await writeToStream(this.#output, `${prefix}${this.#renderer.render(this.#editor, this.#prompt)}`);
	}

	#enqueuePublication(publish: () => Promise<void>): Promise<void> {
		const publication = this.#writeTail.then(publish);
		this.#writeTail = publication.catch(() => undefined);
		return publication;
	}

	#finishLine(line: string | undefined): void {
		const resolve = this.#resolveLine;
		this.#resolveLine = undefined;
		resolve?.(line);
	}

	#prefixSuffixLength(value: string, prefix: string): number {
		const max = Math.min(value.length, prefix.length - 1);
		for (let length = max; length > 0; length -= 1) {
			if (value.endsWith(prefix.slice(0, length))) return length;
		}
		return 0;
	}

	#installEmergencyRestore(): void {
		if (!this.#isProcessTerminal) return;
		this.#exitRestoreListener = () => this.#restoreTerminal();
		process.once("exit", this.#exitRestoreListener);
		this.#crashRestoreListener = () => this.#restoreTerminal();
		process.once("uncaughtExceptionMonitor", this.#crashRestoreListener);
		for (const signal of ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"] as const) {
			const listener = () => {
				this.#restoreTerminal();
				process.removeListener(signal, listener);
				process.kill(process.pid, signal);
			};
			this.#signalRestoreListeners.set(signal, listener);
			process.once(signal, listener);
		}
	}

	#removeEmergencyRestore(): void {
		if (this.#exitRestoreListener) process.removeListener("exit", this.#exitRestoreListener);
		if (this.#crashRestoreListener) process.removeListener("uncaughtExceptionMonitor", this.#crashRestoreListener);
		for (const [signal, listener] of this.#signalRestoreListeners) process.removeListener(signal, listener);
		this.#signalRestoreListeners.clear();
		this.#exitRestoreListener = undefined;
		this.#crashRestoreListener = undefined;
	}

	#restoreTerminal(): void {
		if (this.#restoredTerminal || !this.#alternateScreenEntered) return;
		this.#restoredTerminal = true;
		try {
			this.#output.write(ALTERNATE_SCREEN_EXIT, () => undefined);
		} catch {
			// A vanished terminal cannot be restored further.
		}
	}
}
