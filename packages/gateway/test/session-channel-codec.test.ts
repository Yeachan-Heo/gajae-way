import { expect, test } from "bun:test";
import { SessionChannel, type ChannelTransport } from "../src/orchestrator/session-channel";
import { TailRunner, type TailStream } from "../src/orchestrator/tail-runner";

async function flush() {
	for (let i = 0; i < 40; i++) await Promise.resolve();
}
class Clock {
	time = 0;
	id = 0;
	timers = new Map<number, { at: number; work: () => void }>();
	now = () => this.time;
	setTimeout = (work: () => void, delay: number) => {
		const id = ++this.id;
		this.timers.set(id, { at: this.time + delay, work });
		return id;
	};
	clearTimeout = (id: unknown) => {
		this.timers.delete(id as number);
	};
	async advance(ms: number) {
		const end = this.time + ms;
		for (;;) {
			const next = [...this.timers].filter(([, timer]) => timer.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
			if (!next) break;
			this.time = next[1].at;
			this.timers.delete(next[0]);
			next[1].work();
			await flush();
		}
		this.time = end;
		await flush();
	}
}
function memory(write?: ChannelTransport["write"]) {
	const frames: Array<Record<string, unknown>> = [];
	let sink = (_line: string) => {};
	const transport: ChannelTransport = {
		write(line) {
			frames.push(JSON.parse(line));
			return write?.(line);
		},
		onLine(listener) {
			sink = listener;
			return () => {
				sink = () => {};
			};
		},
	};
	return {
		frames,
		transport,
		emit: (frame: Record<string, unknown> | string) => sink(typeof frame === "string" ? frame : JSON.stringify(frame)),
		answer(index: number, result: Record<string, unknown> = {}) {
			const frame = frames[index]!;
			sink(
				JSON.stringify({
					type: frame.type === "control_request" ? "control_response" : "query_response",
					id: frame.id,
					ok: true,
					result,
				}),
			);
		},
	};
}
function setup(write?: ChannelTransport["write"]) {
	const clock = new Clock();
	const mem = memory(write);
	const channel = new SessionChannel({
		sessionId: "s",
		transport: mem.transport,
		now: clock.now,
		setTimeout: clock.setTimeout,
		clearTimeout: clock.clearTimeout,
	});
	return { clock, mem, channel };
}

test("correlates interleaved query/control ids and counts wrong-kind and orphan responses", async () => {
	const { channel, mem } = setup();
	const query = channel.query("turn.result");
	const control = channel.control("turn.prompt", { clientRef: "once" });
	await flush();
	mem.emit({ type: "control_response", id: mem.frames[0]!.id, ok: true });
	expect(channel.stats().inFlight).toBe(2);
	mem.answer(1, { commandId: "c" });
	mem.emit({ type: "turn_stream", text: "live" });
	mem.answer(0, { status: "completed" });
	expect((await control).result).toEqual({ commandId: "c" });
	expect((await query).result).toEqual({ status: "completed" });
	mem.emit({ type: "query_response", id: "orphan" });
	expect(channel.orphanFrames).toBe(2);
	expect(channel.consumes('{"type":"turn_stream"}')).toBe(false);
	channel.close();
});

test("writer queue serializes asynchronous writes and skips requests closed before writing", async () => {
	let release = () => {};
	const { channel, mem } = setup(
		() =>
			new Promise<void>((resolve) => {
				release = resolve;
			}),
	);
	const a = channel.query("a").catch((error) => error);
	const b = channel.control("b", {}).catch((error) => error);
	await flush();
	expect(mem.frames.map((frame) => frame.query ?? frame.op)).toEqual(["a"]);
	release();
	await flush();
	expect(mem.frames.map((frame) => frame.query ?? frame.op)).toEqual(["a", "b"]);
	const c = channel.query("c").catch((error) => error);
	channel.close();
	release();
	await flush();
	expect(mem.frames).toHaveLength(2);
	expect((await a).code).toBe("channel_closed");
	expect((await b).code).toBe("channel_closed");
	expect(await c).toMatchObject({ code: "channel_closed", bytesWritten: 0 });
});

test("eight in-flight requests cap admission; settlement admits another", async () => {
	const { channel, mem } = setup();
	const requests = Array.from({ length: 8 }, () => channel.query("status").catch((error) => error));
	await expect(channel.query("ninth")).rejects.toMatchObject({ code: "channel_busy", bytesWritten: 0 });
	await flush();
	expect(mem.frames).toHaveLength(8);
	mem.answer(3);
	await requests[3];
	const admitted = channel.query("admitted").catch((error) => error);
	await flush();
	expect(mem.frames).toHaveLength(9);
	channel.close();
	await Promise.all([...requests, admitted]);
});

test("outbound limit measures UTF-8 NDJSON bytes and includes the exact boundary", async () => {
	const mem = memory();
	const channel = new SessionChannel({ sessionId: "s", transport: mem.transport, newId: () => "fixed" });
	const overhead = Buffer.byteLength(
		JSON.stringify({
			type: "control_request",
			op: "turn.prompt",
			operation: "turn.prompt",
			input: { text: "" },
			id: "fixed",
		}) + "\n",
	);
	await expect(channel.control("turn.prompt", { text: "x".repeat(256 * 1024 - overhead + 1) })).rejects.toMatchObject({
		code: "channel_frame_too_large",
		bytesWritten: 0,
	});
	await expect(channel.control("turn.prompt", { text: "한".repeat(100_000) })).rejects.toMatchObject({
		code: "channel_frame_too_large",
	});
	expect(mem.frames).toHaveLength(0);
	const accepted = channel.control("turn.prompt", { text: "x".repeat(256 * 1024 - overhead) });
	await flush();
	mem.answer(0);
	await accepted;
	expect(channel.lastWrite?.bytesWritten).toBe(256 * 1024);
	channel.close();
});

test("write errors carry exact partial bytes or conservatively attempted bytes", async () => {
	for (const count of [0, 7, undefined]) {
		const { channel, mem } = setup(() => {
			throw Object.assign(new Error("broken pipe"), count === undefined ? {} : { bytesWritten: count });
		});
		const error = await channel.control("turn.prompt", {}).catch((error) => error);
		expect(error.code).toBe("channel_write_failed");
		expect(error.bytesWritten).toBe(count ?? Buffer.byteLength(JSON.stringify(mem.frames[0]) + "\n"));
		expect(channel.lastWrite?.bytesWritten).toBe(error.bytesWritten);
		channel.close();
	}
});

test("query deadline is 10s, prompt receipt 30s, and each request can override", async () => {
	const { channel, clock } = setup();
	const query = channel.query("q").catch((error) => error);
	const prompt = channel.control("turn.prompt", {}).catch((error) => error);
	await flush();
	await clock.advance(9_999);
	expect(channel.stats().inFlight).toBe(2);
	await clock.advance(1);
	expect((await query).code).toBe("channel_timeout");
	await clock.advance(19_999);
	expect(channel.stats().inFlight).toBe(1);
	await clock.advance(1);
	expect((await prompt).code).toBe("channel_timeout");
	for (const kind of ["query", "control"]) {
		const fresh = setup();
		const request = (
			kind === "query"
				? fresh.channel.query("q", {}, undefined, { timeoutMs: 17 })
				: fresh.channel.control("turn.prompt", {}, { timeoutMs: 17 })
		).catch((error) => error);
		await flush();
		await fresh.clock.advance(17);
		expect((await request).code).toBe("channel_timeout");
		fresh.channel.close();
	}
});

test("two consecutive timeouts fault once; a correlated response resets the streak", async () => {
	const { channel, clock, mem } = setup();
	const faults: string[] = [];
	channel.onFault((reason) => faults.push(reason));
	async function timeout() {
		const p = channel.query("q", {}, undefined, { timeoutMs: 5 }).catch((error) => error);
		await flush();
		await clock.advance(5);
		await p;
	}
	await timeout();
	const ok = channel.query("q");
	await flush();
	mem.answer(1);
	await ok;
	await timeout();
	expect(faults).toEqual([]);
	await timeout();
	expect(faults).toEqual(["timeouts"]);
	channel.fault("again");
	expect(channel.stats().faults).toBe(1);
	expect(channel.health().healthy).toBe(false);
});

test("idle checkpoint probes use an independent 10s timer reset by live activity", async () => {
	const { channel, clock, mem } = setup();
	channel.startIdleProbe();
	channel.startIdleProbe();
	await clock.advance(9_000);
	mem.emit({ type: "activity", state: "running" });
	await clock.advance(9_999);
	expect(mem.frames).toHaveLength(0);
	await clock.advance(1);
	expect(mem.frames[0]!.query).toBe("session.checkpoint");
	mem.answer(0, { checkpointToken: "t" });
	await flush();
	await clock.advance(10_000);
	expect(mem.frames).toHaveLength(2);
	expect(channel.health().lastProbeAt).toBe(29_000);
	mem.answer(1, { checkpointToken: "t" });
	await flush();
	channel.stopIdleProbe();
	await clock.advance(20_000);
	expect(mem.frames).toHaveLength(2);
	channel.close();
	expect(clock.timers.size).toBe(0);
});

test("transport_error diagnostics fault and close pending requests", async () => {
	for (const line of [
		'{"type":"transport_error","code":"endpoint_stale"}',
		"stderr transport_error: connection refused",
	]) {
		const { channel, mem } = setup();
		const faults: string[] = [];
		channel.onFault((reason) => faults.push(reason));
		const p = channel.query("q").catch((error) => error);
		await flush();
		mem.emit(line);
		expect(faults).toEqual(["transport_error"]);
		expect((await p).code).toBe("channel_closed");
		expect(channel.stats().inFlight).toBe(0);
	}
});

function streamDouble() {
	let done = false;
	const queued: string[] = [];
	let wake = () => {};
	const written: Array<Record<string, unknown>> = [];
	const stream: TailStream = {
		lines: (async function* () {
			while (!done) {
				const line = queued.shift();
				if (line !== undefined) yield line;
				else
					await new Promise<void>((resolve) => {
						wake = resolve;
					});
			}
		})(),
		write(line) {
			written.push(JSON.parse(line));
		},
		close() {
			done = true;
			wake();
		},
	};
	return {
		stream,
		written,
		emit(frame: Record<string, unknown>) {
			queued.push(JSON.stringify(frame));
			wake();
		},
	};
}

test("tail channel kill/diagnostic/blackhole faults reopen at bounded delays and settle one turn", async () => {
	const clock = new Clock();
	const streams: ReturnType<typeof streamDouble>[] = [];
	const delays: number[] = [];
	const delivered: string[] = [];
	let release = () => {};
	const runner = new TailRunner({
		repo: "/tmp",
		now: clock.now,
		setTimeout: clock.setTimeout,
		clearTimeout: clock.clearTimeout,
		run: async () => ({ stdout: JSON.stringify({ ok: true, result: { items: [] } }), stderr: "", exitCode: 0 }),
		stream: () => {
			const stream = streamDouble();
			streams.push(stream);
			return stream.stream;
		},
		sleep: (ms) => {
			delays.push(ms);
			return new Promise<void>((resolve) => {
				release = resolve;
			});
		},
	});
	const handle = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp",
		onFrame: (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
		},
	});
	await flush();
	await handle.beginTurn("once");
	await handle.markAccepted("once");
	handle.setTurnRunning(true);
	const prompt = handle.channel!.control("turn.prompt", { clientRef: "once", text: "go" }).catch((error) => error);
	await flush();
	for (let i = 0; i < 7; i++) {
		const current = streams[i]!;
		if (i === 0) current.stream.close();
		else if (i === 1) current.emit({ type: "transport_error", code: "endpoint_stale" });
		else if (i === 2) await clock.advance(30_000);
		else current.stream.close();
		await flush();
		expect(runner.channelFaults).toBe(i + 1);
		expect(runner.residentChannels).toBe(0);
		release();
		await flush();
		expect(runner.residentChannels).toBe(1);
	}
	expect(delays).toEqual([250, 500, 1000, 2000, 4000, 5000, 5000]);
	expect(runner.channelRestarts).toBe(7);
	expect(handle.channel?.stats().restarts).toBe(7);
	expect(await prompt).toMatchObject({ code: "channel_closed" });
	expect((await prompt).bytesWritten).toBeGreaterThan(0);
	expect(streams.flatMap((stream) => stream.written).filter((frame) => frame.op === "turn.prompt")).toHaveLength(1);
	const final = streams[7]!;
	const result = handle.channel!.turnResult("once");
	await flush();
	final.emit({ type: "query_response", id: final.written[0]!.id, ok: true, result: { status: "completed" } });
	final.emit({ type: "turn_stream", phase: "finalized", messageRef: "answer", text: "settled", clientRef: "once" });
	final.emit({ type: "turn_stream", phase: "finalized", messageRef: "answer", text: "settled", clientRef: "once" });
	final.emit({ type: "activity", state: "idle" });
	final.emit({ type: "query_response", id: "orphan", ok: true });
	await flush();
	expect(await result).toEqual({ status: "completed" });
	expect(delivered).toEqual(["settled"]);
	expect(handle.channel!.orphanFrames).toBe(1);
	// close() parks the healthy resident relay (warm reuse); terminateAll tears it down.
	await handle.close();
	await flush();
	expect(runner.residentChannels).toBe(1);
	await runner.terminateAll();
	await flush();
	expect(runner.residentChannels).toBe(0);
	expect(runner.channelFaults).toBe(7);
	expect(clock.timers.size).toBe(0);
});

test("failed resident spawn is bounded and reattachment discards an unusable cursor before resync", async () => {
	const clock = new Clock();
	const argsSeen: string[][] = [];
	const delays: number[] = [];
	const stream = streamDouble();
	let spawns = 0;
	let discarded = 0;
	let release = () => {};
	const runner = new TailRunner({
		repo: "/tmp",
		now: clock.now,
		setTimeout: clock.setTimeout,
		clearTimeout: clock.clearTimeout,
		run: async (args) => {
			argsSeen.push([...args]);
			if (argsSeen.length === 2)
				return { stdout: JSON.stringify({ ok: false, error: { code: "invalid_cursor" } }), stderr: "", exitCode: 1 };
			return { stdout: JSON.stringify({ ok: true, result: { items: [] } }), stderr: "", exitCode: 0 };
		},
		stream: () => {
			if (++spawns === 1) throw new Error("spawn refused");
			return stream.stream;
		},
		sleep: (ms) => {
			delays.push(ms);
			return new Promise<void>((resolve) => {
				release = resolve;
			});
		},
	});
	const handle = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp",
		cursor: "opaque-old",
		onCursorDiscarded: () => {
			discarded++;
		},
	});
	await flush();
	expect(delays).toEqual([250]);
	expect(runner.channelFaults).toBe(1);
	release();
	await flush();
	expect(argsSeen[1]).toContain("opaque-old");
	expect(argsSeen[1]).toContain("--strict");
	expect(argsSeen[2]).not.toContain("--cursor");
	expect(argsSeen[2]).not.toContain("--strict");
	expect(discarded).toBe(1);
	expect(handle.cursor).toBeUndefined();
	expect(runner.residentChannels).toBe(1);
	expect(runner.channelRestarts).toBe(1);
	expect(spawns).toBe(2);
	await runner.terminateAll();
	await flush();
	expect(clock.timers.size).toBe(0);
});

test("resident reuse: a parked healthy relay is adopted by the next attach for the same session/owner and evicted after idleTtlMs", async () => {
	const clock = new Clock();
	let spawns = 0;
	const streams: ReturnType<typeof streamDouble>[] = [];
	const runner = new TailRunner({
		repo: "/tmp",
		now: clock.now,
		setTimeout: clock.setTimeout,
		clearTimeout: clock.clearTimeout,
		run: async () => ({ stdout: JSON.stringify({ ok: true, result: { items: [] } }), stderr: "", exitCode: 0 }),
		stream: () => {
			spawns++;
			const double = streamDouble();
			streams.push(double);
			return double.stream;
		},
		idleTtlMs: 60_000,
	});
	const attach = (originKey: string) => runner.attach({ sessionId: "s", originKey, brokerGeneration: 1, repo: "/tmp" });
	const first = await attach("o");
	await flush();
	expect(spawns).toBe(1);
	expect(first.channel?.health().healthy).toBe(true);
	await first.close();
	await flush();
	expect(runner.residentChannels).toBe(1);
	const second = await attach("o");
	expect(second).toBe(first);
	expect(spawns).toBe(1);
	await second.close();
	await flush();
	// A different owner never adopts someone else's relay.
	const other = await attach("other");
	expect(other).not.toBe(first);
	expect(spawns).toBe(2);
	await other.close();
	await flush();
	await clock.advance(61_000);
	expect(await runner.reapIdle()).toBe(2);
	await flush();
	expect(runner.residentChannels).toBe(0);
});
