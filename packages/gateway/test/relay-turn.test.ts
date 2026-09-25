import { expect, test } from "bun:test";
import type { SessionRelayStream } from "../src/orchestrator/broker";
import {
	decodeStreamLine,
	RelayClosedError,
	RelayRefusedError,
	RelayRequestTimeoutError,
	type TailFrame,
	TailRunner,
} from "../src/orchestrator/tail-runner";

/**
 * A scripted `gjc sdk serve --stdio` host: answers hello with a connectionId,
 * records every frame the gateway writes, and lets the test push host frames.
 */
class FakeRelay implements SessionRelayStream {
	readonly written: Record<string, unknown>[] = [];
	readonly lines: AsyncIterable<string>;
	closed = false;
	#push: (line: string | null) => void = () => {};
	#queue: Array<string | null> = [];
	#waiters: Array<(line: string | null) => void> = [];

	constructor(
		readonly connectionId = "connection:1",
		readonly options: { readonly hello?: boolean } = {},
	) {
		this.#push = (line) => {
			const waiter = this.#waiters.shift();
			if (waiter) waiter(line);
			else this.#queue.push(line);
		};
		const next = () =>
			new Promise<string | null>((resolve) => {
				const queued = this.#queue.shift();
				if (queued !== undefined) resolve(queued);
				else this.#waiters.push(resolve);
			});
		this.lines = {
			[Symbol.asyncIterator]: () => ({
				next: async () => {
					const line = await next();
					return line === null ? { done: true, value: undefined } : { done: false, value: line };
				},
			}),
		};
	}

	write(line: string): void {
		if (this.closed) throw new Error("relay closed");
		const frame = JSON.parse(line) as Record<string, unknown>;
		this.written.push(frame);
		if (frame.type === "hello" && this.options.hello !== false)
			this.host({ type: "hello", protocolVersion: 3, connectionId: this.connectionId });
	}

	host(frame: Record<string, unknown>): void {
		this.#push(JSON.stringify(frame));
	}

	/** A raw (non-JSON) line, as an older gjc prints its uncaught refusal. */
	hostLine(line: string): void {
		this.#push(line);
	}

	/** The relay process ends (host hangup / crash). */
	end(): void {
		this.#push(null);
	}

	close(): void {
		this.closed = true;
		this.#push(null);
	}

	/** The last request frame the gateway wrote of a given type. */
	lastRequest(type: string): Record<string, unknown> | undefined {
		return [...this.written].reverse().find((frame) => frame.type === type);
	}
}

function runner(
	spawn: (sessionId: string) => SessionRelayStream,
	extra: Partial<ConstructorParameters<typeof TailRunner>[0]> = {},
) {
	return new TailRunner({ stream: spawn, repo: "/tmp/repo", sleep: async () => {}, log: () => {}, ...extra });
}

const CORRELATION = { commandId: "cmd-1", turnId: "turn-1" };

function messageEnd(role: string, text: string, correlation = CORRELATION, id = `m-${text}`) {
	return {
		type: "event",
		kind: "message_end",
		payload: {
			event_type: "message_end",
			event: { type: "message_end", message: { role, id, content: [{ type: "text", text }] } },
		},
		...correlation,
	};
}

test("attach exchanges hello and stamps every request with the host's connectionId", async () => {
	const relay = new FakeRelay("connection:7");
	const handle = await runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	expect(relay.written[0]).toMatchObject({ type: "hello", protocolVersion: 3, capabilities: ["tool_activity_v2"] });
	const pending = handle.query("turn.result", { kind: "prompt", clientRef: "op-1" });
	await Bun.sleep(0);
	const request = relay.lastRequest("query_request")!;
	expect(request).toMatchObject({
		query: "turn.result",
		connectionId: "connection:7",
		input: { kind: "prompt", clientRef: "op-1" },
	});
	relay.host({ type: "query_response", id: request.id, ok: true, result: { status: "in_flight" } });
	expect(await pending).toEqual({ ok: true, result: { status: "in_flight" } });
	await handle.close();
	expect(relay.closed).toBe(true);
});

test("control responses resolve by id; a refused control carries its error code", async () => {
	const relay = new FakeRelay();
	const handle = await runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	const first = handle.control("turn.prompt", { text: "a", clientRef: "op-a" });
	const second = handle.control("turn.prompt", { text: "b", clientRef: "op-b" });
	await Bun.sleep(0);
	const [requestA, requestB] = relay.written.filter((frame) => frame.type === "control_request");
	expect(requestA).toMatchObject({ operation: "turn.prompt", confirm: false, input: { clientRef: "op-a" } });
	// Out-of-order answers land on the right waiter.
	relay.host({
		type: "control_response",
		id: requestB!.id,
		ok: false,
		error: { code: "busy", message: "turn running" },
	});
	relay.host({
		type: "control_response",
		id: requestA!.id,
		ok: true,
		result: { commandId: "c", turnId: "t", accepted: true },
	});
	expect(await first).toMatchObject({ ok: true, result: { commandId: "c" } });
	expect(await second).toEqual({ ok: false, error: { code: "busy", message: "turn running" } });
	await handle.close();
});

test("the turn's own content is delivered once, in order, and foreign correlations are dropped", async () => {
	const relay = new FakeRelay();
	const frames: TailFrame[] = [];
	const diagnostics: string[] = [];
	const handle = await runner(() => relay).attach({
		sessionId: "s1",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onFrame: (frame) => {
			frames.push(frame);
		},
		onDiagnostic: (line) => diagnostics.push(line),
	});
	handle.beginTurn("op-1");
	// Before any correlation is known, content is not yet attributable.
	relay.host(messageEnd("assistant", "stale answer", { commandId: "cmd-0", turnId: "turn-0" }));
	handle.correlate("op-1", CORRELATION);
	relay.host({ type: "agent_start", ...CORRELATION });
	relay.host(messageEnd("user", "the prompt"));
	relay.host(messageEnd("assistant", "working on it"));
	relay.host({
		type: "event",
		kind: "tool_execution_start",
		payload: {
			event_type: "tool_execution_start",
			event: { toolCallId: "tc-1", toolName: "bash", args: { command: "ls" } },
		},
		...CORRELATION,
	});
	relay.host({
		type: "event",
		kind: "message_update",
		payload: { event_type: "message_update", event: {} },
		...CORRELATION,
	});
	relay.host({
		type: "event",
		kind: "tool_execution_end",
		payload: { event_type: "tool_execution_end", event: { toolCallId: "tc-1", toolName: "bash", result: "" } },
		...CORRELATION,
	});
	relay.host(messageEnd("toolResult", "(no output)"));
	relay.host(messageEnd("assistant", "done"));
	// Another turn's answer on the same session must never reach this turn.
	relay.host(messageEnd("assistant", "someone else's answer", { commandId: "cmd-9", turnId: "turn-9" }));
	relay.host({ type: "identity_header", sessionId: "s1" });
	relay.host({ type: "turn_stream", phase: "finalized", finalAnswer: true, text: "working on it\n\ndone" });
	relay.host({ type: "agent_end", ...CORRELATION, outcome: { reason: "end_turn" } });
	await Bun.sleep(10);
	expect(
		frames.map((frame) => [frame.rawKind, frame.assistantText ?? frame.payload.toolName ?? frame.payload.role]),
	).toEqual([
		["agent_start", undefined],
		["message_end", "user"],
		["message_end", "working on it"],
		["tool_execution_start", "bash"],
		["message_update", undefined],
		["tool_execution_end", "bash"],
		["message_end", "toolResult"],
		["message_end", "done"],
		["agent_end", undefined],
	]);
	const assistant = frames.filter((frame) => frame.assistantText);
	expect(assistant.map((frame) => frame.steerEcho)).toEqual([false, false]);
	expect(frames.find((frame) => frame.payload.role === "user")?.steerEcho).toBe(true);
	expect(frames.find((frame) => frame.rawKind === "tool_execution_start")?.payload).toMatchObject({
		toolName: "bash",
		toolCallStarted: true,
		args: { command: "ls" },
	});
	expect(frames.at(-1)?.idle).toBe(true);
	expect(diagnostics.filter((line) => line.startsWith("tail_frame_foreign"))).toHaveLength(1);
	await handle.close();
});

/**
 * Frame sequence captured from gjc 0.17.6 (`sdk serve --stdio`, 2026-09-25): a
 * turn running `sleep 45` in the foreground is steered. The host folds the bash
 * command into the background, echoes the steer as a user message, and answers
 * it ("Four.") under the SAME turn correlation. Every lifecycle frame arrives
 * twice: top-level and as a ring-sequenced `{type:"event"}` mirror.
 */
function captured0176SteerTurn(sessionId: string): Record<string, unknown>[] {
	const correlation = {
		commandId: "80ae507d-ac3a-49e6-ae8e-9489e551dfc8",
		turnId: "7d150346-7eaa-4ae1-b4bc-9b9e054f6527",
	};
	const mirror = (kind: string, payload: Record<string, unknown>, seq: number) => ({
		kind,
		payload: { type: kind, sessionId, ...payload },
		type: "event",
		generation: 1,
		seq,
	});
	const message = (role: string, content: unknown[], id: string) => ({
		type: "event",
		kind: "message_end",
		payload: { event_type: "message_end", event: { type: "message_end", message: { role, id, content } } },
		...correlation,
	});
	return [
		message(
			"user",
			[{ type: "text", text: "Run exactly this bash command in the foreground: sleep 45; echo done." }],
			"u-1",
		),
		mirror("agent_start", correlation, 3),
		{ type: "activity", sessionId, state: "busy" },
		{ type: "agent_start", sessionId, ...correlation },
		mirror("activity", { state: "busy" }, 4),
		message("assistant", [{ type: "thinking", thinking: "run it" }], "a-1"),
		{
			type: "event",
			kind: "tool_execution_start",
			payload: {
				event_type: "tool_execution_start",
				event: { toolCallId: "tc-1", toolName: "bash", args: { command: "sleep 45; echo done" } },
			},
			...correlation,
		},
		mirror("bash_folded", { jobId: "bg_1" }, 5),
		{
			type: "event",
			kind: "tool_execution_end",
			payload: {
				event_type: "tool_execution_end",
				event: { toolCallId: "tc-1", toolName: "bash", result: "Background job bg_1 started" },
			},
			...correlation,
		},
		message("toolResult", [{ type: "text", text: "Background job bg_1 started: sleep 45; echo done" }], "t-1"),
		message(
			"user",
			[{ type: "text", text: "Quick question while you wait: what is 2+2? Answer in words now." }],
			"u-2",
		),
		message(
			"assistant",
			[
				{ type: "thinking", thinking: "four" },
				{ type: "text", text: "Four." },
			],
			"a-2",
		),
		mirror("agent_end", { ...correlation, finalText: "Four.", outcome: { kind: "stopped", reason: "end_turn" } }, 6),
		{
			type: "agent_end",
			sessionId,
			...correlation,
			finalText: "Four.",
			outcome: { kind: "stopped", reason: "end_turn" },
		},
		mirror("activity", { state: "idle" }, 7),
		{ type: "activity", sessionId, state: "idle" },
	];
}

test("a captured gjc 0.17.6 steered turn: the steer answer reaches the turn once, lifecycle mirrors are not double-applied or reported unknown", async () => {
	const relay = new FakeRelay();
	const frames: TailFrame[] = [];
	const diagnostics: string[] = [];
	const handle = await runner(() => relay).attach({
		sessionId: "898ea744",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onFrame: (frame) => {
			frames.push(frame);
		},
		onDiagnostic: (line) => diagnostics.push(line),
	});
	handle.beginTurn("steer-probe");
	handle.setTurnRunning(true);
	for (const frame of captured0176SteerTurn("898ea744")) relay.host(frame);
	await Bun.sleep(10);

	// The answer to the steer is delivered to the owning turn, exactly once.
	expect(frames.filter((frame) => frame.assistantText).map((frame) => frame.assistantText)).toEqual(["Four."]);
	// The steer is attributed to the turn as an echo, never as speech.
	expect(frames.filter((frame) => frame.steerEcho && frame.payload.role === "user")).toHaveLength(2);
	// Each lifecycle transition is applied once, from the top-level frame only.
	expect(frames.filter((frame) => frame.rawKind === "agent_start")).toHaveLength(1);
	expect(frames.filter((frame) => frame.rawKind === "agent_end")).toHaveLength(1);
	expect(frames.find((frame) => frame.rawKind === "agent_end")).toMatchObject({ idle: true });
	// The top-level idle marker still reaches the actor exactly once.
	expect(frames.filter((frame) => frame.rawKind === "activity" && frame.idle)).toHaveLength(1);
	// No mirror or host notice is reported as an unrecognised protocol frame.
	expect(diagnostics.filter((line) => line.startsWith("unknown_runtime_event"))).toEqual([]);
	expect(diagnostics.filter((line) => line.startsWith("tail_frame_foreign"))).toEqual([]);
	await handle.close();
});

test("the first correlated frame after beginTurn adopts the turn when the accept receipt has not landed yet", async () => {
	const relay = new FakeRelay();
	const frames: TailFrame[] = [];
	const handle = await runner(() => relay).attach({
		sessionId: "s1",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onFrame: (frame) => {
			frames.push(frame);
		},
	});
	handle.beginTurn("op-1");
	relay.host({ type: "agent_start", ...CORRELATION });
	relay.host(messageEnd("assistant", "hi", { commandId: "cmd-other", turnId: "turn-other" }));
	relay.host(messageEnd("assistant", "mine"));
	await Bun.sleep(10);
	expect(frames.map((frame) => frame.assistantText ?? frame.rawKind)).toEqual(["agent_start", "mine"]);
	await handle.close();
});

test("a relay that ends mid-turn reports the loss, fails pending requests, and reopens with backoff", async () => {
	const relays: FakeRelay[] = [];
	const lost: unknown[] = [];
	const sleeps: number[] = [];
	const handle = await runner(
		() => {
			const relay = new FakeRelay(`connection:${relays.length + 1}`);
			relays.push(relay);
			return relay;
		},
		{ sleep: async (ms) => void sleeps.push(ms) },
	).attach({
		sessionId: "s1",
		brokerGeneration: 3,
		repo: "/tmp/repo",
		onRelayLost: (input) => {
			lost.push(input);
		},
	});
	handle.beginTurn("op-1", CORRELATION);
	handle.setTurnRunning(true);
	const pending = handle.query("turn.result", { kind: "prompt", clientRef: "op-1" });
	relays[0]!.end();
	await expect(pending).rejects.toBeInstanceOf(RelayClosedError);
	await Bun.sleep(10);
	expect(lost).toEqual([{ sessionId: "s1", brokerGeneration: 3 }]);
	expect(relays).toHaveLength(2);
	expect(sleeps.length).toBeGreaterThan(0);
	// The reopened relay is usable for commands again.
	const after = handle.query("turn.result", { kind: "prompt", clientRef: "op-1" });
	await Bun.sleep(0);
	const request = relays[1]!.lastRequest("query_request")!;
	expect(request.connectionId).toBe("connection:2");
	relays[1]!.host({ type: "query_response", id: request.id, ok: true, result: { status: "terminal_ok" } });
	expect(await after).toMatchObject({ ok: true });
	await handle.close();
});

test("a request with no answer times out; a request on a closed handle fails closed", async () => {
	const relay = new FakeRelay();
	const handle = await runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	await expect(handle.query("turn.result", {}, { timeoutMs: 5 })).rejects.toBeInstanceOf(RelayRequestTimeoutError);
	await handle.close();
	await expect(handle.control("turn.prompt", { text: "x", clientRef: "y" })).rejects.toBeInstanceOf(RelayClosedError);
});

test("attach fails when the host never says hello", async () => {
	const relay = new FakeRelay("connection:1", { hello: false });
	const attach = runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	relay.end();
	await expect(attach).rejects.toThrow();
});

test("stall alarms fire exactly at the threshold and only while a turn runs", async () => {
	let now = 0;
	const relay = new FakeRelay();
	const stalls: number[] = [];
	const tail = runner(() => relay, { now: () => now, stallTimeoutMs: 1_000 });
	const handle = await tail.attach({
		sessionId: "s1",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onStall: ({ elapsedMs }) => {
			stalls.push(elapsedMs);
		},
	});
	now = 5_000;
	tail.checkStalls();
	expect(stalls).toEqual([]);
	now = 1_000;
	handle.setTurnRunning(true);
	now = 1_999;
	tail.checkStalls();
	expect(stalls).toEqual([]);
	now = 2_000;
	tail.checkStalls();
	expect(stalls).toEqual([1_000]);
	tail.checkStalls();
	expect(stalls).toEqual([1_000]);
	handle.setTurnRunning(false);
	await handle.close();
});

test("decodeStreamLine: notifications frames are not turn content; thinking blocks never become speech", () => {
	expect(
		decodeStreamLine(JSON.stringify({ type: "turn_stream", phase: "finalized", finalAnswer: true, text: "x" })),
	).toEqual([]);
	expect(decodeStreamLine(JSON.stringify({ type: "action_needed", kind: "idle" }))).toEqual([]);
	expect(decodeStreamLine("not json")).toEqual([]);
	const [frame] = decodeStreamLine(
		JSON.stringify({
			type: "event",
			kind: "message_end",
			payload: {
				event: {
					message: {
						role: "assistant",
						content: [
							{ type: "thinking", text: "private" },
							{ type: "text", text: "public" },
						],
					},
				},
			},
			commandId: "c",
			turnId: "t",
		}),
	);
	expect(frame).toMatchObject({
		kind: "message_end",
		assistantText: "public",
		commandId: "c",
		turnId: "t",
		steerEcho: false,
	});
	const [idle] = decodeStreamLine(JSON.stringify({ type: "activity", state: "idle" }));
	expect(idle).toMatchObject({ kind: "activity", idle: true });
});

test("a relay that dies six times in a row is declared dead: the handle closes and reports onRelayDead once", async () => {
	const relays: FakeRelay[] = [];
	const dead: unknown[] = [];
	const lost: unknown[] = [];
	const handle = await runner(() => {
		const relay = new FakeRelay(`connection:${relays.length + 1}`);
		relays.push(relay);
		return relay;
	}).attach({
		sessionId: "s1",
		brokerGeneration: 2,
		repo: "/tmp/repo",
		onRelayLost: (input) => {
			lost.push(input);
		},
		onRelayDead: (input) => {
			dead.push(input);
		},
	});
	handle.beginTurn("op-1", CORRELATION);
	handle.setTurnRunning(true);
	for (let i = 0; i < 8; i++) {
		relays.at(-1)!.end();
		await Bun.sleep(5);
	}
	expect(dead).toEqual([{ sessionId: "s1", brokerGeneration: 2 }]);
	expect(lost.length).toBeGreaterThanOrEqual(1);
	expect(relays.length).toBeLessThanOrEqual(7);
	await expect(handle.query("turn.result", {})).rejects.toBeInstanceOf(RelayClosedError);
});

test("request replies do not count as turn progress: a silent turn still stalls under continuous status polling", async () => {
	let now = 0;
	const relay = new FakeRelay();
	const stalls: number[] = [];
	const tail = runner(() => relay, { now: () => now, stallTimeoutMs: 1_000 });
	const handle = await tail.attach({
		sessionId: "s1",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onStall: ({ elapsedMs }) => {
			stalls.push(elapsedMs);
		},
	});
	handle.beginTurn("op-1", CORRELATION);
	handle.setTurnRunning(true);
	for (let step = 1; step <= 4; step++) {
		now = step * 400;
		const pending = handle.query("turn.result", { kind: "prompt", clientRef: "op-1" });
		await Bun.sleep(0);
		relay.host({
			type: "query_response",
			id: relay.lastRequest("query_request")!.id,
			ok: true,
			result: { status: "in_flight" },
		});
		await pending;
		tail.checkStalls();
	}
	expect(stalls).toEqual([1_200]);
	// Real turn content resets the clock.
	relay.host(messageEnd("assistant", "still here"));
	await Bun.sleep(5);
	now = 2_500;
	tail.checkStalls();
	expect(stalls).toEqual([1_200]);
	await handle.close();
});

test("a spawner that throws counts toward give-up like a relay that dies", async () => {
	let spawns = 0;
	const dead: unknown[] = [];
	const first = new FakeRelay();
	const handle = await runner(() => {
		spawns += 1;
		if (spawns === 1) return first;
		throw new Error("spawn refused");
	}).attach({
		sessionId: "s1",
		brokerGeneration: 1,
		repo: "/tmp/repo",
		onRelayDead: (input) => {
			dead.push(input);
		},
	});
	first.end();
	await Bun.sleep(20);
	expect(dead).toHaveLength(1);
	expect(spawns).toBeLessThanOrEqual(8);
	await handle.close();
});

test("a serve refusal envelope (endpoint_stale) rejects attach as session_unavailable instead of waiting for hello", async () => {
	const relay = new FakeRelay("connection:1", { hello: false });
	const attach = runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	relay.host({ ok: false, error: { code: "endpoint_stale", message: "session s1 endpoint is not live" } });
	relay.end();
	const error = await attach.catch((e: unknown) => e);
	expect(error).toBeInstanceOf(RelayRefusedError);
	expect((error as RelayRefusedError).code).toBe("session_unavailable");
});

test("a gjc 0.16 textual refusal ('[Uncaught Exception] Error: endpoint_stale: …') rejects attach as session_unavailable", async () => {
	const relay = new FakeRelay("connection:1", { hello: false });
	const attach = runner(() => relay).attach({ sessionId: "s1", brokerGeneration: 1, repo: "/tmp/repo" });
	relay.hostLine("[Uncaught Exception] Error: endpoint_stale: session s1 endpoint is not live");
	relay.hostLine("    at BEh (/$bunfs/root/gjc-darwin-arm64:29876:2324)");
	relay.end();
	const error = await attach.catch((e: unknown) => e);
	expect(error).toBeInstanceOf(RelayRefusedError);
	expect((error as RelayRefusedError).code).toBe("session_unavailable");
});
