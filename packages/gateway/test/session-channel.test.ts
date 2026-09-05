/**
 * I4a: the complete transcript read runs over the ONE resident relay. Acceptance
 * from the plan: 50 pages + an oversized Unicode row read completely with the
 * snapshot identity, and zero child launches beyond the resident child.
 */
import { expect, test } from "bun:test";
import { join } from "node:path";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { ChannelQueryError, SessionChannel, TranscriptIncompleteError } from "../src/orchestrator/session-channel";
import { createFakeGjc, runFakeGjc } from "./fixtures/fake-gjc.mjs";

const FIXTURE = join(import.meta.dir, "fixtures", "fake-gjc.mjs");

function rows(count: number, oversizedAt?: number) {
	const out: Array<Record<string, unknown>> = [];
	for (let i = 0; i < count; i++) {
		const oversized = i === oversizedAt;
		const body = oversized ? "한글 유니코드 본문 ".repeat(20_000) : `row_${i} body`;
		out.push({
			id: i.toString(16).padStart(8, "0"),
			role: i % 2 === 0 ? "user" : "assistant",
			ts: `2026-09-05T00:00:${String(i % 60).padStart(2, "0")}.000Z`,
			textSummary: body.slice(0, 32),
			body,
			...(oversized ? { oversized: true } : {}),
		});
	}
	return out;
}

/** Spawns the fixture relay once and counts every spawn so the zero-extra-launch claim is real. */
function residentRelay(modes: string) {
	let spawns = 0;
	const spawner = (sessionId: string) => {
		spawns += 1;
		const child = Bun.spawn(["bun", FIXTURE, "sdk", "serve", "--stdio", "--session", sessionId], {
			stdin: "pipe",
			stdout: "pipe",
			stderr: "inherit",
			env: { ...process.env, GAJAEWAY_FAKE_GJC_MODES: modes },
		});
		const lines = (async function* () {
			const reader = child.stdout.getReader();
			const decoder = new TextDecoder();
			let buffer = "";
			for (;;) {
				const { value, done } = await reader.read();
				if (done) break;
				buffer += decoder.decode(value, { stream: true });
				let nl = buffer.indexOf("\n");
				while (nl >= 0) {
					yield buffer.slice(0, nl);
					buffer = buffer.slice(nl + 1);
					nl = buffer.indexOf("\n");
				}
			}
		})();
		return {
			lines,
			write(line: string) {
				child.stdin.write(line);
				child.stdin.flush();
			},
			close() {
				child.kill();
			},
		};
	};
	return { spawner, spawns: () => spawns };
}

test("reads 50 pages plus an oversized Unicode row completely over the resident relay with zero extra launches", async () => {
	const transcript = rows(50, 37);
	const modes = `serve:bidirectional,transcript:rows=${JSON.stringify(transcript)}`;
	const relay = residentRelay(modes);
	const runner = new TailRunner({
		run: (args) => runFakeGjc(args, createFakeGjc({ modes })),
		stream: relay.spawner,
		repo: "/tmp/ws",
	});
	const handle = await runner.attach({ sessionId: "stub-session-1", brokerGeneration: 1, repo: "/tmp/ws" });
	try {
		await handle.ready;
		const channel = await waitFor(() => handle.channel, "channel did not open");
		const snapshot = await channel.readTranscript();
		expect(snapshot.complete).toBe(true);
		expect(snapshot.source).toBe("channel");
		expect(snapshot.revision).toBe("fixture-revision-1");
		expect(snapshot.rows).toHaveLength(50);
		expect(snapshot.rows.map((row) => row.id)).toEqual(transcript.map((row) => String(row.id)));
		const big = snapshot.rows[37]!;
		expect(big.role).toBe("assistant");
		expect(big.body).toBe(transcript[37]!.body as string);
		expect(Buffer.byteLength(big.body, "utf8")).toBeGreaterThan(256 * 1024);
		expect(relay.spawns()).toBe(1);
		expect(channel.orphanFrames).toBe(0);
	} finally {
		await handle.close();
	}
});

test("turn.result and session.checkpoint round-trip by id on the shared connection", async () => {
	const modes = "serve:bidirectional,status:content";
	const relay = residentRelay(modes);
	const runner = new TailRunner({
		run: (args) => runFakeGjc(args, createFakeGjc({ modes })),
		stream: relay.spawner,
		repo: "/tmp/ws",
	});
	const handle = await runner.attach({ sessionId: "stub-session-1", brokerGeneration: 1, repo: "/tmp/ws" });
	try {
		await handle.ready;
		const channel = await waitFor(() => handle.channel, "channel did not open");
		const [checkpoint, result] = await Promise.all([channel.checkpoint(), channel.turnResult("client-ref-1")]);
		expect(checkpoint.checkpointToken).toBe("fixture-checkpoint");
		expect(checkpoint.revisionId).toBe("fixture-revision-1");
		expect(result).toBeDefined();
		expect(relay.spawns()).toBe(1);
	} finally {
		await handle.close();
	}
});

test("an incomplete page without a continuation cursor is refused, never returned partially", async () => {
	const transport = memoryTransport();
	const channel = new SessionChannel({ sessionId: "s", transport: transport.transport, requestTimeoutMs: 500 });
	const read = channel.readTranscript();
	const checkpoint = await transport.next();
	transport.answer(checkpoint.id, { ok: true, result: { checkpointToken: "t", revisionId: "r1" } });
	const page = await transport.next();
	transport.answer(page.id, {
		ok: true,
		page: { items: [{ id: "a", role: "user", body: "x" }], complete: false, revision: "r1" },
	});
	await expect(read).rejects.toBeInstanceOf(TranscriptIncompleteError);
	channel.close();
});

test("a revision change mid-read is refused", async () => {
	const transport = memoryTransport();
	const channel = new SessionChannel({ sessionId: "s", transport: transport.transport, requestTimeoutMs: 500 });
	const read = channel.readTranscript();
	transport.answer((await transport.next()).id, { ok: true, result: { checkpointToken: "t", revisionId: "r1" } });
	transport.answer((await transport.next()).id, {
		ok: true,
		page: { items: [{ id: "a", role: "user", body: "x" }], complete: false, revision: "r1", continuationCursor: "c1" },
	});
	transport.answer((await transport.next()).id, {
		ok: true,
		page: { items: [{ id: "b", role: "assistant", body: "y" }], complete: true, revision: "r2" },
	});
	await expect(read).rejects.toThrow(/revision changed/);
	channel.close();
});

test("query errors carry the runtime code and a timeout is typed", async () => {
	const transport = memoryTransport();
	const channel = new SessionChannel({ sessionId: "s", transport: transport.transport, requestTimeoutMs: 50 });
	const failing = channel.query("session.checkpoint");
	transport.answer((await transport.next()).id, { ok: false, error: { code: "session_unavailable" } });
	await expect(failing).rejects.toMatchObject({ name: "ChannelQueryError", code: "session_unavailable" });
	await expect(channel.query("turn.result", { kind: "prompt", clientRef: "x" })).rejects.toMatchObject({
		code: "channel_timeout",
	});
	expect(channel.orphanFrames).toBe(0);
	transport.emit(JSON.stringify({ type: "query_response", id: "nobody", ok: true }));
	expect(channel.orphanFrames).toBe(1);
	expect(channel.consumes(JSON.stringify({ type: "turn_stream", id: "irrelevant" }))).toBe(false);
	channel.close();
	await expect(channel.query("session.checkpoint")).rejects.toBeInstanceOf(ChannelQueryError);
});

function memoryTransport() {
	const listeners = new Set<(line: string) => void>();
	const written: Array<{ id: string; query: string; input: unknown; cursor?: string }> = [];
	const waiters: Array<(frame: (typeof written)[number]) => void> = [];
	return {
		transport: {
			write(line: string) {
				const frame = JSON.parse(line) as (typeof written)[number];
				const waiter = waiters.shift();
				if (waiter) waiter(frame);
				else written.push(frame);
			},
			onLine(listener: (line: string) => void) {
				listeners.add(listener);
				return () => listeners.delete(listener);
			},
		},
		next(): Promise<(typeof written)[number]> {
			const queued = written.shift();
			if (queued) return Promise.resolve(queued);
			return new Promise((resolve) => waiters.push(resolve));
		},
		answer(id: string, payload: Record<string, unknown>) {
			for (const listener of listeners) listener(JSON.stringify({ type: "query_response", id, ...payload }));
		},
		emit(line: string) {
			for (const listener of listeners) listener(line);
		},
	};
}

async function waitFor<T>(read: () => T | undefined, message: string, timeoutMs = 5_000): Promise<T> {
	const end = Date.now() + timeoutMs;
	while (Date.now() < end) {
		const value = read();
		if (value !== undefined) return value;
		await Bun.sleep(10);
	}
	throw new Error(message);
}
