const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });

export interface ConsoleEditorViewport {
	readonly text: string;
	readonly cursorColumn: number;
}

function previousGraphemeStart(value: string, cursor: number): number {
	if (cursor <= 0) return 0;
	const segments = [...segmenter.segment(value.slice(0, cursor))];
	return cursor - (segments.at(-1)?.segment.length ?? Math.min(1, cursor));
}

function nextGraphemeEnd(value: string, cursor: number): number {
	if (cursor >= value.length) return value.length;
	const first = segmenter.segment(value.slice(cursor))[Symbol.iterator]().next().value?.segment;
	return Math.min(value.length, cursor + (first?.length ?? 1));
}

function graphemeAt(value: string, start: number, end: number): string {
	return value.slice(start, end);
}

function wordKind(value: string): "space" | "word" | "punctuation" {
	if (/^\s$/u.test(value)) return "space";
	if (/^[\p{L}\p{N}_]$/u.test(value)) return "word";
	return "punctuation";
}

function width(value: string): number {
	return Bun.stringWidth(value);
}

/** A bounded, single-line input model with terminal-compatible editing operations. */
export class ConsoleInputEditor {
	#value = "";
	#cursor = 0;
	#byteLength = 0;
	readonly #history: string[] = [];
	#historyIndex = -1;
	#historyDraft = "";
	readonly #historyLimit: number;

	constructor(historyLimit = 100) {
		this.#historyLimit = Math.max(1, historyLimit);
	}

	get value(): string {
		return this.#value;
	}

	get cursor(): number {
		return this.#cursor;
	}

	get byteLength(): number {
		return this.#byteLength;
	}

	setValue(value: string, cursor = value.length): void {
		this.#value = value.normalize("NFC");
		this.#cursor = Math.max(0, Math.min(this.#value.length, cursor));
		this.#byteLength = Buffer.byteLength(this.#value);
		this.#resetHistoryNavigation();
	}

	clear(): void {
		this.#value = "";
		this.#cursor = 0;
		this.#byteLength = 0;
		this.#resetHistoryNavigation();
	}

	insert(text: string): void {
		if (!text) return;
		const normalized = text.normalize("NFC");
		const before = this.#value.slice(0, this.#cursor);
		const after = this.#value.slice(this.#cursor);
		this.#value = before + normalized + after;
		this.#cursor = before.length + normalized.length;
		this.#byteLength += Buffer.byteLength(normalized);
		this.#resetHistoryNavigation();
	}

	backspace(): void {
		if (this.#cursor === 0) return;
		const start = previousGraphemeStart(this.#value, this.#cursor);
		this.#byteLength -= Buffer.byteLength(this.#value.slice(start, this.#cursor));
		this.#value = this.#value.slice(0, start) + this.#value.slice(this.#cursor);
		this.#cursor = start;
		this.#resetHistoryNavigation();
	}

	deleteForward(): void {
		if (this.#cursor >= this.#value.length) return;
		const end = nextGraphemeEnd(this.#value, this.#cursor);
		this.#byteLength -= Buffer.byteLength(this.#value.slice(this.#cursor, end));
		this.#value = this.#value.slice(0, this.#cursor) + this.#value.slice(end);
		this.#resetHistoryNavigation();
	}

	moveLeft(): void {
		this.#cursor = previousGraphemeStart(this.#value, this.#cursor);
	}

	moveRight(): void {
		this.#cursor = nextGraphemeEnd(this.#value, this.#cursor);
	}

	moveStart(): void {
		this.#cursor = 0;
	}

	moveEnd(): void {
		this.#cursor = this.#value.length;
	}

	moveWordLeft(): void {
		let cursor = this.#cursor;
		while (cursor > 0) {
			const start = previousGraphemeStart(this.#value, cursor);
			if (wordKind(graphemeAt(this.#value, start, cursor)) !== "space") break;
			cursor = start;
		}
		if (cursor === 0) {
			this.#cursor = 0;
			return;
		}
		const end = cursor;
		const start = previousGraphemeStart(this.#value, cursor);
		const kind = wordKind(graphemeAt(this.#value, start, end));
		cursor = start;
		while (cursor > 0) {
			const nextStart = previousGraphemeStart(this.#value, cursor);
			if (wordKind(graphemeAt(this.#value, nextStart, cursor)) !== kind) break;
			cursor = nextStart;
		}
		this.#cursor = cursor;
	}

	moveWordRight(): void {
		let cursor = this.#cursor;
		while (cursor < this.#value.length) {
			const end = nextGraphemeEnd(this.#value, cursor);
			if (wordKind(graphemeAt(this.#value, cursor, end)) !== "space") break;
			cursor = end;
		}
		if (cursor >= this.#value.length) {
			this.#cursor = this.#value.length;
			return;
		}
		const end = nextGraphemeEnd(this.#value, cursor);
		const kind = wordKind(graphemeAt(this.#value, cursor, end));
		cursor = end;
		while (cursor < this.#value.length) {
			const nextEnd = nextGraphemeEnd(this.#value, cursor);
			if (wordKind(graphemeAt(this.#value, cursor, nextEnd)) !== kind) break;
			cursor = nextEnd;
		}
		this.#cursor = cursor;
	}

	deleteWordBackward(): void {
		const end = this.#cursor;
		this.moveWordLeft();
		this.#byteLength -= Buffer.byteLength(this.#value.slice(this.#cursor, end));
		this.#value = this.#value.slice(0, this.#cursor) + this.#value.slice(end);
		this.#resetHistoryNavigation();
	}

	deleteWordForward(): void {
		const start = this.#cursor;
		this.moveWordRight();
		const end = this.#cursor;
		this.#byteLength -= Buffer.byteLength(this.#value.slice(start, end));
		this.#value = this.#value.slice(0, start) + this.#value.slice(end);
		this.#cursor = start;
		this.#resetHistoryNavigation();
	}

	deleteToStart(): void {
		if (this.#cursor === 0) return;
		this.#byteLength -= Buffer.byteLength(this.#value.slice(0, this.#cursor));
		this.#value = this.#value.slice(this.#cursor);
		this.#cursor = 0;
		this.#resetHistoryNavigation();
	}

	deleteToEnd(): void {
		if (this.#cursor >= this.#value.length) return;
		this.#byteLength -= Buffer.byteLength(this.#value.slice(this.#cursor));
		this.#value = this.#value.slice(0, this.#cursor);
		this.#resetHistoryNavigation();
	}

	historyPrevious(): boolean {
		if (this.#history.length === 0) return false;
		if (this.#historyIndex === -1) {
			this.#historyDraft = this.#value;
			this.#historyIndex = this.#history.length - 1;
		} else if (this.#historyIndex > 0) {
			this.#historyIndex -= 1;
		} else {
			return false;
		}
		this.#setHistoryValue(this.#history[this.#historyIndex] as string);
		return true;
	}

	historyNext(): boolean {
		if (this.#historyIndex === -1) return false;
		if (this.#historyIndex >= this.#history.length - 1) {
			this.#historyIndex = -1;
			this.#setHistoryValue(this.#historyDraft);
			return true;
		}
		this.#historyIndex += 1;
		this.#setHistoryValue(this.#history[this.#historyIndex] as string);
		return true;
	}

	/** Moves the current line into history and clears the editor for the next input. */
	takeSubmission(): string {
		const submitted = this.#value;
		if (submitted.trim() && this.#history.at(-1) !== submitted) {
			this.#history.push(submitted);
			if (this.#history.length > this.#historyLimit) this.#history.shift();
		}
		this.#value = "";
		this.#cursor = 0;
		this.#byteLength = 0;
		this.#resetHistoryNavigation();
		return submitted;
	}

	viewport(availableWidth: number): ConsoleEditorViewport {
		const widthLimit = Math.max(1, availableWidth);
		const displayed = this.#cursor >= this.#value.length ? `${this.#value} ` : this.#value;
		const cursorPrefix = displayed.slice(0, this.#cursor);
		const cursorWidth = width(cursorPrefix);
		const cursorEnd = nextGraphemeEnd(displayed, this.#cursor);
		const cursorGrapheme = displayed.slice(this.#cursor, cursorEnd) || " ";
		const cursorWidthValue = Math.max(1, width(cursorGrapheme));
		let start = 0;
		if (width(displayed) > widthLimit) {
			const desired = Math.max(0, cursorWidth - Math.floor(widthLimit / 2));
			start = this.#boundaryAtColumn(displayed, desired);
			while (width(displayed.slice(start, this.#cursor)) > Math.max(0, widthLimit - cursorWidthValue)) {
				start = nextGraphemeEnd(displayed, start);
			}
		}
		let text = "";
		let end = start;
		while (end < displayed.length) {
			const next = nextGraphemeEnd(displayed, end);
			const candidate = displayed.slice(start, next);
			if (width(candidate) > widthLimit && end > start) break;
			text = candidate;
			end = next;
		}
		const cursorColumn = Math.max(0, width(displayed.slice(start, this.#cursor)));
		return { text, cursorColumn };
	}

	#boundaryAtColumn(value: string, targetColumn: number): number {
		let cursor = 0;
		while (cursor < value.length) {
			const next = nextGraphemeEnd(value, cursor);
			if (width(value.slice(0, next)) > targetColumn) break;
			cursor = next;
		}
		return cursor;
	}

	#setHistoryValue(value: string): void {
		this.#value = value;
		this.#cursor = value.length;
		this.#byteLength = Buffer.byteLength(value);
	}

	#resetHistoryNavigation(): void {
		this.#historyIndex = -1;
		this.#historyDraft = "";
	}
}
