import type { ConsoleInputEditor } from "./editor";

const RESET = "\x1b[0m";
const STATUS_STYLE = "\x1b[48;5;238m\x1b[38;5;255m";
const INPUT_STYLE = "\x1b[38;5;81m";
const graphemeSegmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const MAX_TRANSCRIPT_FRAMES = 512;
const MAX_TRANSCRIPT_LINES = 4_096;
const MAX_TRANSCRIPT_FRAME_BYTES = 128 * 1024;
const MAX_TRANSCRIPT_BYTES = 1_024 * 1_024;

export interface ConsoleTuiEvent {
	readonly seq: string | number;
	readonly kind: string;
	readonly payload: unknown;
}

interface TranscriptFrame {
	readonly lines: readonly string[];
	readonly bytes: number;
}

function safeTerminalText(value: string): string {
	let output = "";
	for (const character of value) {
		const codePoint = character.codePointAt(0) as number;
		if (codePoint === 0x0a) {
			output += "\\n";
			continue;
		}
		if (codePoint === 0x0d) {
			output += "\\r";
			continue;
		}
		if (codePoint === 0x09) {
			output += "\\t";
			continue;
		}
		if (codePoint === 0x1b) {
			output += "\\x1B";
			continue;
		}
		if ((codePoint >= 0x00 && codePoint <= 0x1f) || (codePoint >= 0x7f && codePoint <= 0x9f)) {
			output += `\\u${codePoint.toString(16).toUpperCase().padStart(4, "0")}`;
			continue;
		}
		output += character;
	}
	return output;
}

function recordValue(value: unknown): Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function displayWidth(value: string): number {
	return Bun.stringWidth(value);
}

function nextGraphemeEnd(value: string, start: number): number {
	if (start >= value.length) return value.length;
	if (value.charCodeAt(start) < 0x80) return start + 1;
	const segment = graphemeSegmenter.segment(value.slice(start))[Symbol.iterator]().next().value?.segment;
	return Math.min(value.length, start + (segment?.length ?? 1));
}

function truncateToWidth(value: string, width: number): string {
	if (width <= 0) return "";
	if (displayWidth(value) <= width) return value;
	if (width === 1) return "…";
	let end = 0;
	while (end < value.length) {
		const next = nextGraphemeEnd(value, end);
		if (displayWidth(value.slice(0, next)) > width - 1) break;
		end = next;
	}
	return `${value.slice(0, end)}…`;
}

function wrapLine(value: string, width: number): string[] {
	const lineWidth = Math.max(1, width);
	if (!value) return [""];
	const result: string[] = [];
	let start = 0;
	let end = 0;
	let currentWidth = 0;
	while (end < value.length) {
		const next = nextGraphemeEnd(value, end);
		const graphemeWidth = displayWidth(value.slice(end, next));
		if (currentWidth + graphemeWidth > lineWidth && end > start) {
			result.push(value.slice(start, end));
			start = end;
			currentWidth = 0;
			continue;
		}
		currentWidth += graphemeWidth;
		end = next;
	}
	result.push(value.slice(start, end));
	return result;
}

function boundedFrameText(value: string): string {
	if (Buffer.byteLength(value) <= MAX_TRANSCRIPT_FRAME_BYTES) return value;
	const prefix = Buffer.from(value).subarray(0, MAX_TRANSCRIPT_FRAME_BYTES).toString("utf8");
	return `${prefix}\n[Console transcript frame truncated at ${MAX_TRANSCRIPT_FRAME_BYTES} bytes.]`;
}

function replaceField(line: string, field: string, value: string): string {
	const expression = new RegExp(`\\b${field}=\\S+`);
	return expression.test(line) ? line.replace(expression, `${field}=${value}`) : `${line} ${field}=${value}`;
}

/**
 * Terminal-grid renderer modeled after gajae-code's split between durable output,
 * a bottom-pinned status rail, and a focused editor. It keeps logical transcript
 * frames independent from terminal dimensions so resize is a pure reflow.
 */
export class ConsoleTuiRenderer {
	readonly #frames: TranscriptFrame[] = [];
	#transcriptLines = 0;
	#transcriptBytes = 0;
	#layoutRevision = 0;
	#cachedLayoutRevision = -1;
	#cachedLayoutColumns = 0;
	#cachedTranscriptLines: string[] = [];
	#columns = 80;
	#rows = 24;
	#scrollOffset = 0;
	#daemon = "daemon: status=unknown state=unknown";
	#main = "main: resumed=unknown session_id=unknown turn_state=unknown follow_up_queue_depth=unknown";
	#journal = "journal: head_cursor=unknown degraded=unknown";
	#lock = "lock: held=unknown holder=none queue_len=unknown stuck=unknown quarantined=unknown write_mode=unknown";
	#reconcile = "reconcile: freshness=unknown last_ok_at=none cycle_ms=unknown drift_count=unknown";
	#delivery = "fenced";
	#consumer = "idle";
	#consumers = "consumers: none";

	get columns(): number {
		return this.#columns;
	}

	get rows(): number {
		return this.#rows;
	}

	get scrollOffset(): number {
		return this.#scrollOffset;
	}

	get transcriptFrameCount(): number {
		return this.#frames.length;
	}

	resize(columns: number | undefined, rows: number | undefined): void {
		if (typeof columns === "number" && Number.isFinite(columns)) {
			const nextColumns = Math.max(20, Math.trunc(columns));
			if (nextColumns !== this.#columns) {
				this.#columns = nextColumns;
				this.#invalidateLayout();
			}
		}
		if (typeof rows === "number" && Number.isFinite(rows)) this.#rows = Math.max(5, Math.trunc(rows));
	}

	appendFrame(text: string): void {
		const normalized = text.replace(/\r\n?/gu, "\n");
		const sanitized = boundedFrameText(
			normalized
				.split("\n")
				.map(safeTerminalText)
				.join("\n"),
		);
		const lines = sanitized.split("\n");
		if (lines.at(-1) === "") lines.pop();
		const frame = { lines, bytes: Buffer.byteLength(sanitized) } satisfies TranscriptFrame;
		this.#frames.push(frame);
		this.#transcriptLines += frame.lines.length + 1;
		this.#transcriptBytes += frame.bytes;
		while (
			this.#frames.length > MAX_TRANSCRIPT_FRAMES ||
			this.#transcriptLines > MAX_TRANSCRIPT_LINES ||
			this.#transcriptBytes > MAX_TRANSCRIPT_BYTES
		) {
			const discarded = this.#frames.shift();
			if (!discarded) break;
			this.#transcriptLines -= discarded.lines.length + 1;
			this.#transcriptBytes -= discarded.bytes;
		}
		this.#invalidateLayout();
		const liveCapacity = this.#transcriptCapacity();
		this.#scrollOffset = Math.min(this.#scrollOffset, Math.max(0, this.#logicalTranscriptLines().length - liveCapacity));
	}

	setStatusSummary(summary: string): void {
		for (const line of summary.replace(/\r\n?/gu, "\n").split("\n")) {
			const trimmed = safeTerminalText(line.trim());
			if (trimmed.startsWith("daemon:")) this.#daemon = trimmed;
			else if (trimmed.startsWith("main:")) this.#main = trimmed;
			else if (trimmed.startsWith("journal:")) this.#journal = trimmed;
			else if (trimmed.startsWith("lock:")) this.#lock = trimmed;
			else if (trimmed.startsWith("reconcile:")) this.#reconcile = trimmed;
			else if (trimmed.startsWith("consumers:")) this.#consumers = trimmed;
		}
	}

	setDeliveryState(state: "fenced" | "ready" | "unavailable" | "stopping"): void {
		this.#delivery = state;
		if (state === "ready") this.#consumer = "streaming";
		if (state === "unavailable") this.#consumer = "stopped";
	}

	observeEvent(event: ConsoleTuiEvent): void {
		const sequence = safeTerminalText(String(event.seq));
		this.#journal = replaceField(this.#journal, "head_cursor", sequence);
		this.#consumer = "streaming";
		switch (event.kind) {
			case "turn_start":
				this.#main = replaceField(this.#main, "turn_state", "busy");
				break;
			case "turn_end":
				this.#main = replaceField(this.#main, "turn_state", "idle");
				break;
			case "health_change": {
				const payload = recordValue(event.payload);
				const state = typeof payload.state === "string" ? payload.state : typeof payload.status === "string" ? payload.status : "unknown";
				this.#daemon = replaceField(this.#daemon, "state", safeTerminalText(state));
				break;
			}
			case "lock_event":
				this.#lock = `${this.#lock} event=changed`;
				break;
		}
	}

	scrollBy(lines: number): boolean {
		const capacity = this.#transcriptCapacity();
		const maxOffset = Math.max(0, this.#logicalTranscriptLines().length - capacity);
		const next = Math.max(0, Math.min(maxOffset, this.#scrollOffset + lines));
		if (next === this.#scrollOffset) return false;
		this.#scrollOffset = next;
		return true;
	}

	followLive(): boolean {
		if (this.#scrollOffset === 0) return false;
		this.#scrollOffset = 0;
		return true;
	}

	visibleLines(editor: ConsoleInputEditor, prompt: string): readonly string[] {
		const statusLines = this.#statusLines();
		const transcriptCapacity = Math.max(1, this.#rows - statusLines.length - 1);
		const transcript = this.#logicalTranscriptLines();
		const end = Math.max(0, transcript.length - this.#scrollOffset);
		const start = Math.max(0, end - transcriptCapacity);
		const shown = transcript.slice(start, end);
		while (shown.length < transcriptCapacity) shown.unshift("");
		const editorViewport = editor.viewport(Math.max(1, this.#columns - displayWidth(prompt)));
		const input = `${prompt}${editorViewport.text}`;
		return [...shown, ...statusLines, input];
	}

	render(editor: ConsoleInputEditor, prompt: string): string {
		const logicalLines = this.visibleLines(editor, prompt);
		const statusLineCount = this.#statusLines().length;
		const inputRow = logicalLines.length;
		const editorViewport = editor.viewport(Math.max(1, this.#columns - displayWidth(prompt)));
		const cursorColumn = Math.min(this.#columns, displayWidth(prompt) + editorViewport.cursorColumn + 1);
		const transcriptCount = logicalLines.length - statusLineCount - 1;
		const styledLines = logicalLines.map((line, index) => {
			if (index >= transcriptCount && index < transcriptCount + statusLineCount) {
				return `${STATUS_STYLE}${truncateToWidth(line, this.#columns)}${RESET}`;
			}
			if (index === logicalLines.length - 1) {
				const editorText = truncateToWidth(editorViewport.text, Math.max(1, this.#columns - displayWidth(prompt)));
				return `${INPUT_STYLE}${prompt}${RESET}${editorText}`;
			}
			return truncateToWidth(line, this.#columns);
		});
		return `\x1b[?2026h\x1b[?25l\x1b[H\x1b[2J${styledLines.join("\n")}\x1b[${inputRow};${cursorColumn}H\x1b[?25h\x1b[?2026l`;
	}

	#statusLines(): string[] {
		const scroll = this.#scrollOffset > 0 ? ` scroll=${this.#scrollOffset}` : "";
		const source = [
			`GATEWAY COCKPIT | ${this.#daemon} | ${this.#main}`,
			`${this.#journal} | ${this.#lock}`,
			`${this.#reconcile} | ${this.#consumers} | delivery=${this.#delivery} consumer=${this.#consumer}${scroll}`,
		];
		const wrapped = source.flatMap((line) => wrapLine(line, this.#columns));
		const maximumStatusRows = Math.max(1, this.#rows - 2);
		if (wrapped.length <= maximumStatusRows) return wrapped;
		return source.slice(0, maximumStatusRows).map((line) => truncateToWidth(line, this.#columns));
	}

	#logicalTranscriptLines(): string[] {
		if (this.#cachedLayoutRevision === this.#layoutRevision && this.#cachedLayoutColumns === this.#columns) {
			return this.#cachedTranscriptLines;
		}
		const lines: string[] = [];
		for (const frame of this.#frames) {
			for (const line of frame.lines) lines.push(...wrapLine(line, this.#columns));
			lines.push("");
		}
		if (lines.at(-1) === "") lines.pop();
		this.#cachedLayoutRevision = this.#layoutRevision;
		this.#cachedLayoutColumns = this.#columns;
		this.#cachedTranscriptLines = lines;
		return lines;
	}

	#invalidateLayout(): void {
		this.#layoutRevision += 1;
	}

	#transcriptCapacity(): number {
		return Math.max(1, this.#rows - this.#statusLines().length - 1);
	}
}
