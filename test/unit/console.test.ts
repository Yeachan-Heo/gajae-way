import { expect, test } from "bun:test";
import {
	ConsoleOutput,
	MAX_RAW_CONSOLE_LINE_BYTES,
	MAX_RAW_CONSOLE_QUEUED_BYTES,
	MAX_RAW_CONSOLE_QUEUED_LINES,
	RawConsoleTerminal,
	consoleStartupDecision,
	renderConsoleStatusSummary,
	sanitizeConsoleText,
} from "../../src/console/console";

class RawInputHarness {
	readonly isTTY = true;
	isRaw = false;
	isPaused = false;
	pauseCalls = 0;
	resumeCalls = 0;
	readonly #listeners = new Set<(chunk: string | Buffer) => void>();
	readonly #blockedChunks: string[] = [];

	get blockedChunkCount(): number {
		return this.#blockedChunks.length;
	}

	setEncoding(_encoding: BufferEncoding): void {}

	setRawMode(enabled: boolean): void {
		this.isRaw = enabled;
	}

	resume(): void {
		this.resumeCalls += 1;
		if (!this.isPaused) return;
		this.isPaused = false;
		while (!this.isPaused && this.#blockedChunks.length > 0) {
			const chunk = this.#blockedChunks.shift() as string;
			this.deliver(chunk);
		}
	}

	pause(): void {
		this.pauseCalls += 1;
		this.isPaused = true;
	}

	on(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.add(listener);
	}

	off(_event: "data", listener: (chunk: string | Buffer) => void): void {
		this.#listeners.delete(listener);
	}

	send(chunk: string): void {
		if (this.isPaused) {
			this.#blockedChunks.push(chunk);
			return;
		}
		this.deliver(chunk);
	}

	private deliver(chunk: string): void {
		for (const listener of [...this.#listeners]) listener(chunk);
	}
}

class RawOutputHarness {
	readonly isTTY = true;
	readonly writes: string[] = [];
	onWrite: ((text: string) => void) | undefined;

	write(text: string, callback: (error?: Error | null) => void): boolean {
		this.writes.push(text);
		this.onWrite?.(text);
		callback();
		return true;
	}

	once(_event: "drain", _listener: () => void): void {}
}

class BackpressuredRawOutputHarness {
	readonly isTTY = true;
	readonly writes: string[] = [];
	#stalled = false;
	readonly #pending: Array<{ callback: (error?: Error | null) => void; drain?: () => void }> = [];

	get pendingWriteCount(): number {
		return this.#pending.length;
	}

	stall(): void {
		this.#stalled = true;
	}

	write(text: string, callback: (error?: Error | null) => void): boolean {
		this.writes.push(text);
		if (!this.#stalled) {
			callback();
			return true;
		}
		this.#pending.push({ callback });
		return false;
	}

	once(_event: "drain", listener: () => void): void {
		const pending = this.#pending.at(-1);
		if (!pending) throw new Error("drain listener was registered without a pending write");
		pending.drain = listener;
	}

	releaseOne(): void {
		const pending = this.#pending.shift();
		if (!pending) throw new Error("no stalled write to release");
		pending.callback();
		pending.drain?.();
	}

	recover(): void {
		this.#stalled = false;
		while (this.#pending.length > 0) this.releaseOne();
	}
}

async function eventually(read: () => boolean, description: string, timeoutMs = 500): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (read()) return;
		await Bun.sleep(1);
	}
	throw new Error(description);
}

function rawScreen(writes: readonly string[]): string {
	const lines: string[] = [];
	let current = "";
	for (const write of writes) {
		for (let index = 0; index < write.length; index += 1) {
			const character = write[index] as string;
			if (character === "\r") {
				current = "";
				continue;
			}
			if (character === "\x1b" && write.slice(index, index + 4) === "\x1b[2K") {
				current = "";
				index += 3;
				continue;
			}
			if (character === "\n") {
				lines.push(current);
				current = "";
				continue;
			}
			current += character;
		}
	}
	return [...lines, current].join("\n");
}

const healthyHealth = {
	status: "healthy",
	state: "running",
	main: { resumed: true, session_id: "main-session" },
};

const healthyStatus = {
	...healthyHealth,
	turn_state: "busy",
	follow_up_queue_depth: 2,
	journal: { head_cursor: "7:42", degraded: false },
	lock: {
		held: true,
		holder: { session_id: "main-session" },
		queue_len: 1,
		stuck: false,
		quarantined: true,
	},
	write_mode: false,
	reconcile: { last_ok_at: 10_000, cycle_ms: 5_000, drift_count: 3 },
};

test("console maps health and status into the owner-visible summary", () => {
	expect(consoleStartupDecision(healthyHealth, healthyStatus)).toEqual({ interactive: true });
	const summary = renderConsoleStatusSummary(healthyHealth, healthyStatus, 15_000);
	expect(summary).toContain("daemon: status=healthy state=running");
	expect(summary).toContain("main: resumed=true session_id=main-session turn_state=busy follow_up_queue_depth=2");
	expect(summary).toContain("journal: head_cursor=7:42 degraded=false");
	expect(summary).toContain(
		"lock: held=true holder=session=main-session queue_len=1 stuck=false quarantined=true write_mode=false",
	);
	expect(summary).toContain("reconcile: freshness=fresh last_ok_at=10000 age_ms=5000 cycle_ms=5000 drift_count=3");
});

test("console refuses a failed-closed or unhealthy daemon before interactive input", () => {
	const failedClosed = {
		status: "unhealthy",
		state: "failed_closed",
		reason: "profile_drift",
		main: { resumed: false, session_id: null },
	};
	const failedClosedDecision = consoleStartupDecision(failedClosed, failedClosed);
	expect(failedClosedDecision).toMatchObject({ interactive: false });
	expect(failedClosedDecision.refusal).toContain("failed closed");
	expect(failedClosedDecision.refusal).toContain("profile_drift");
	expect(failedClosedDecision.refusal).toContain("fenced");

	const unavailableDecision = consoleStartupDecision(
		{ status: "unhealthy", state: "degraded", main: { resumed: true } },
		{ status: "unhealthy", state: "degraded", main: { resumed: true } },
	);
	expect(unavailableDecision).toMatchObject({ interactive: false });
	expect(unavailableDecision.refusal).toContain("not healthy");
});

test("untrusted gateway text escapes CSI, OSC 52, C0, and C1 controls before terminal publication", async () => {
	const hostile = "readable \x1b[2J CSI \x1b]52;c;SGVsbG8=\u0007 OSC52 \u0000\b\t\n\r\u009b1A C1";
	const sanitized = sanitizeConsoleText(hostile);
	expect(sanitized).toContain("readable \\x1B[2J CSI");
	expect(sanitized).toContain("\\x1B]52;c;SGVsbG8=\\u0007 OSC52");
	expect(sanitized).toContain("\\u0000\\u0008\\t\\n\\r\\u009B1A C1");
	expect(sanitized).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);

	const writes: string[] = [];
	const output = new ConsoleOutput((text) => {
		writes.push(text);
	});
	await output.writeTrusted("\x1b[2K");
	await output.writeUntrusted(hostile);
	expect(writes).toEqual(["\x1b[2K", sanitized]);
	expect(writes[1]).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);

	const hostileRefusal = consoleStartupDecision(
		{ status: "unhealthy", state: "failed_closed", reason: "\x1b]52;c;SGVsbG8=\u0007" },
		{ status: "unhealthy", state: "failed_closed", reason: "\x1b]52;c;SGVsbG8=\u0007" },
	).refusal;
	expect(hostileRefusal).toContain("\\x1B]52;c;SGVsbG8=\\u0007");
	expect(hostileRefusal).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
});

test("console output serializes complete frames from concurrent publishers", async () => {
	const writes: string[] = [];
	let releaseFirst: (() => void) | undefined;
	let signalFirst: (() => void) | undefined;
	const firstBlocked = new Promise<void>((resolve) => {
		releaseFirst = resolve;
	});
	const firstStarted = new Promise<void>((resolve) => {
		signalFirst = resolve;
	});
	const output = new ConsoleOutput(async (text) => {
		writes.push(text);
		if (text === "first frame\n") {
			signalFirst?.();
			await firstBlocked;
		}
	});
	const first = output.writeFrame("first frame\n");
	await firstStarted;
	const second = output.writeFrame("second frame\n");
	await Bun.sleep(10);
	expect(writes).toEqual(["first frame\n"]);
	releaseFirst?.();
	await Promise.all([first, second]);
	expect(writes).toEqual(["first frame\n", "second frame\n"]);
});

test("raw terminal publishes one production frame before an injected concurrent keystroke", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const pendingLine = terminal.readLine("way> ");
	let injected = false;
	output.onWrite = (text) => {
		if (!injected && text.includes("assistant frame must remain whole")) {
			injected = true;
			input.send("x");
		}
	};
	try {
		await terminal.writeTrusted("Assistant:\nassistant frame must remain whole\n");
		await Bun.sleep(0);
		const frameWrites = output.writes.filter((write) => write.includes("assistant frame must remain whole"));
		expect(frameWrites).toEqual(["\r\x1b[2KAssistant:\nassistant frame must remain whole\nway> "]);
		expect(output.writes.indexOf("x")).toBeGreaterThan(output.writes.indexOf(frameWrites[0] as string));
		const screen = rawScreen(output.writes);
		expect(screen).toContain("Assistant:\nassistant frame must remain whole");
		expect(screen.endsWith("way> x")).toBe(true);
	} finally {
		terminal.close();
		await pendingLine;
	}
});

test("raw terminal queues every complete line in a multi-line input chunk", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	try {
		const first = terminal.readLine("way> ");
		input.send("first\nsecond\nthird\n");
		expect(await first).toBe("first");
		expect(await terminal.readLine("way> ")).toBe("second");
		expect(await terminal.readLine("way> ")).toBe("third");
	} finally {
		terminal.close();
	}
});

test("raw terminal bounds saturated queued input, refuses excess lines visibly, and preserves FIFO delivery", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const accepted = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `queued-${index}`);
	const rejected = Array.from({ length: 4 }, (_value, index) => `rejected-${index}`);
	try {
		const first = terminal.readLine("way> ");
		input.send("first\n");
		expect(await first).toBe("first");
		for (const line of accepted) input.send(`${line}\n`);
		for (const line of rejected) input.send(`${line}\n`);
		await terminal.writeTrusted("");
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.inputPaused).toBe(false);
		expect(input.isPaused).toBe(false);
		expect(input.pauseCalls).toBe(0);
		expect(input.blockedChunkCount).toBe(0);
		expect(output.writes.join("")).toContain("Input queue is full");
		expect(output.writes.join("")).toContain(`queue-full-lines=${rejected.length}`);

		const received: string[] = [];
		for (let index = 0; index < accepted.length; index += 1) {
			received.push((await terminal.readLine("way> ")) as string);
		}
		expect(received).toEqual(accepted);
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
	} finally {
		terminal.close();
	}
});

test("raw terminal visibly refuses excess lines from a single paste after its bounded queue fills", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const pasted = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES + 2 }, (_value, index) => `paste-${index}`).join(
		"\n",
	);
	try {
		const first = terminal.readLine("way> ");
		input.send("first\n");
		expect(await first).toBe("first");
		input.send(`${pasted}\n`);
		await terminal.writeTrusted("");
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.inputPaused).toBe(false);
		expect(input.isPaused).toBe(false);
		expect(output.writes.join("")).toContain("Input queue is full");
		expect(output.writes.join("")).toContain("additional pasted input was refused");
	} finally {
		terminal.close();
	}
});

test("raw terminal retains one refusal through transient backpressure without echoing refused input", async () => {
	const input = new RawInputHarness();
	const output = new BackpressuredRawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const accepted = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `accepted-${index}`);
	const refusedPrefix = "REFUSED_PAYLOAD_MUST_NOT_ECHO";
	const refusedBurst = Array.from({ length: 256 }, (_value, index) => `${refusedPrefix}-${index}`).join("\n");
	try {
		const direct = terminal.readLine("way> ");
		input.send("direct\n");
		expect(await direct).toBe("direct");
		await terminal.writeTrusted("");
		await eventually(() => !terminal.rawPublicationPending, "initial raw echo did not flush");

		output.stall();
		const writesBeforeStall = output.writes.length;
		input.send(`${accepted[0]}\n`);
		await eventually(() => output.pendingWriteCount === 1, "first stalled echo did not begin publication");
		input.send(`${accepted.slice(1).join("\n")}\n${refusedBurst}\n`);

		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.bufferedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_LINE_BYTES);
		expect(terminal.pendingEchoRedrawCount).toBe(1);
		expect(terminal.rawPublicationPending).toBe(true);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(output.pendingWriteCount).toBe(1);
		expect(output.writes.slice(writesBeforeStall)).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(refusedPrefix);

		await Bun.sleep(300);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(output.pendingWriteCount).toBe(1);
		output.recover();
		await eventually(
			() => terminal.pendingRefusalPublicationCount === 0 && !terminal.rawPublicationPending,
			"retained refusal did not publish after output recovery",
		);
		expect(output.writes.filter((write) => write.includes("Input queue is full"))).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(refusedPrefix);

		const delivered: string[] = [];
		for (let index = 0; index < accepted.length; index += 1) {
			delivered.push((await terminal.readLine("way> ")) as string);
		}
		expect(delivered).toEqual(accepted);
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
	} finally {
		terminal.close();
	}
});

test("raw terminal publishes a same-chunk oversized-line refusal and keeps the pending read usable", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const usableLine = "normal line after oversized paste";
	const fifoLines = ["first queued line after oversized paste", "second queued line after oversized paste"];
	const pendingLine = terminal.readLine("way> ");
	try {
		input.send(`${oversized}\n`);
		await eventually(
			() => output.writes.join("").includes(`Input line exceeds ${MAX_RAW_CONSOLE_LINE_BYTES} bytes and was refused.`),
			"same-chunk oversized input did not render a refusal",
		);
		expect(output.writes.filter((write) => write.includes("Input line exceeds"))).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
		expect(terminal.bufferedInputBytes).toBe(0);
		expect(terminal.pendingRefusalPublicationCount).toBe(0);

		input.send(`${usableLine}\n`);
		expect(await pendingLine).toBe(usableLine);
		input.send(`${fifoLines.join("\n")}\n`);
		expect(await terminal.readLine("way> ")).toBe(fifoLines[0]);
		expect(await terminal.readLine("way> ")).toBe(fifoLines[1]);
	} finally {
		terminal.close();
	}
});

test("raw terminal retains a same-chunk oversized-line refusal through transient output backpressure", async () => {
	const input = new RawInputHarness();
	const output = new BackpressuredRawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const usableLine = "normal line after recovered output";
	const pendingLine = terminal.readLine("way> ");
	try {
		await eventually(() => output.writes.includes("way> "), "raw terminal did not render its prompt");
		output.stall();
		input.send(`${oversized}\n`);
		await eventually(() => output.pendingWriteCount === 1, "oversized refusal did not begin its stalled publication");
		await Bun.sleep(300);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(terminal.pendingEchoRedrawCount).toBe(0);
		expect(terminal.rawPublicationPending).toBe(true);
		expect(terminal.queuedLineCount).toBe(0);
		expect(terminal.queuedInputBytes).toBe(0);
		expect(terminal.bufferedInputBytes).toBe(0);

		output.recover();
		await eventually(
			() => terminal.pendingRefusalPublicationCount === 0 && !terminal.rawPublicationPending,
			"retained oversized refusal did not publish after output recovery",
		);
		expect(output.writes.filter((write) => write.includes("Input line exceeds"))).toHaveLength(1);
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));

		input.send(`${usableLine}\n`);
		expect(await pendingLine).toBe(usableLine);
	} finally {
		terminal.close();
	}
});

test("raw terminal coalesces mixed refusal causes through transient output backpressure", async () => {
	const input = new RawInputHarness();
	const output = new BackpressuredRawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_LINES }, (_value, index) => `accepted-${index}`);
	const oversized = "x".repeat(MAX_RAW_CONSOLE_LINE_BYTES + 1);
	const dropped = "MIXED_QUEUE_FULL_PAYLOAD_MUST_NOT_ECHO";
	try {
		const direct = terminal.readLine("way> ");
		input.send("direct\n");
		expect(await direct).toBe("direct");
		await terminal.writeTrusted("");
		await eventually(() => !terminal.rawPublicationPending, "initial raw echo did not flush");

		output.stall();
		input.send(`${oversized}\n${queued.join("\n")}\n${dropped}\n`);
		await eventually(() => output.pendingWriteCount === 1, "combined refusal did not begin its stalled publication");
		expect(terminal.pendingRefusalPublicationCount).toBe(1);
		expect(terminal.pendingEchoRedrawCount).toBe(1);
		expect(terminal.rawPublicationPending).toBe(true);
		expect(terminal.queuedLineCount).toBe(MAX_RAW_CONSOLE_QUEUED_LINES);
		expect(terminal.queuedInputBytes).toBeLessThanOrEqual(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.bufferedInputBytes).toBe(0);
		await Bun.sleep(300);
		expect(terminal.pendingRefusalPublicationCount).toBe(1);

		output.recover();
		await eventually(
			() => terminal.pendingRefusalPublicationCount === 0 && !terminal.rawPublicationPending,
			"combined refusal did not publish after output recovery",
		);
		const refusalFrames = output.writes.filter((write) => write.includes("Input refused:"));
		expect(refusalFrames).toHaveLength(1);
		const refusal = refusalFrames[0] as string;
		expect(refusal).toContain("oversized-line=1");
		expect(refusal).toContain("queue-full-lines=1");
		expect(refusal).not.toContain("queue-full-bytes=");
		expect(output.writes.join("")).not.toContain(oversized.slice(0, 64));
		expect(output.writes.join("")).not.toContain(dropped);

		const delivered: string[] = [];
		for (let index = 0; index < queued.length; index += 1) {
			delivered.push((await terminal.readLine("way> ")) as string);
		}
		expect(delivered).toEqual(queued);
	} finally {
		terminal.close();
	}
});

test("raw terminal reports byte-capacity refusals with a distinct bounded cause", async () => {
	const input = new RawInputHarness();
	const output = new RawOutputHarness();
	const terminal = new RawConsoleTerminal({ input, output });
	const queued = Array.from({ length: MAX_RAW_CONSOLE_QUEUED_BYTES / MAX_RAW_CONSOLE_LINE_BYTES }, (_value, index) =>
		`${index}`.padEnd(MAX_RAW_CONSOLE_LINE_BYTES, "b"),
	);
	const dropped = "BYTE_CAPACITY_PAYLOAD_MUST_NOT_ECHO";
	try {
		const direct = terminal.readLine("way> ");
		input.send("direct\n");
		expect(await direct).toBe("direct");
		await terminal.writeTrusted("");
		input.send(`${queued.join("\n")}\n${dropped}\n`);
		await eventually(
			() => output.writes.some((write) => write.includes("Input refused:")),
			"byte-capacity refusal did not render",
		);
		const refusalFrames = output.writes.filter((write) => write.includes("Input refused:"));
		expect(refusalFrames).toHaveLength(1);
		const refusal = refusalFrames[0] as string;
		expect(refusal).toContain("queue-full-bytes=1");
		expect(refusal).not.toContain("queue-full-lines=");
		expect(refusal).not.toContain("oversized-line=");
		expect(output.writes.join("")).not.toContain(dropped);
		expect(terminal.queuedLineCount).toBe(queued.length);
		expect(terminal.queuedInputBytes).toBe(MAX_RAW_CONSOLE_QUEUED_BYTES);
		expect(terminal.bufferedInputBytes).toBe(0);

		const delivered: string[] = [];
		for (let index = 0; index < queued.length; index += 1) {
			delivered.push((await terminal.readLine("way> ")) as string);
		}
		expect(delivered).toEqual(queued);
	} finally {
		terminal.close();
	}
});

test("raw terminal retains Ctrl-C as an exit request while no line read is pending", async () => {
	const input = new RawInputHarness();
	const terminal = new RawConsoleTerminal({ input, output: new RawOutputHarness() });
	let exits = 0;
	terminal.onExitRequested(() => {
		exits += 1;
	});
	try {
		input.send("\u0003");
		expect(exits).toBe(1);
		expect(terminal.inputPaused).toBe(true);
		expect(await terminal.readLine("way> ")).toBeUndefined();
	} finally {
		terminal.close();
	}
});

test("raw terminal retains Ctrl-D as an exit request while no line read is pending", async () => {
	const input = new RawInputHarness();
	const terminal = new RawConsoleTerminal({ input, output: new RawOutputHarness() });
	let exits = 0;
	terminal.onExitRequested(() => {
		exits += 1;
	});
	try {
		input.send("\u0004");
		expect(exits).toBe(1);
		expect(terminal.inputPaused).toBe(true);
		expect(await terminal.readLine("way> ")).toBeUndefined();
	} finally {
		terminal.close();
	}
});
