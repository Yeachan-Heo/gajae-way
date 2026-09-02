import { expect, test } from "bun:test";
import type { CliRunner } from "@gajaeway/subsession";
import { TailRunner } from "../src/orchestrator/tail-runner";

type Deferred = {
	readonly wait: Promise<void>;
	release(): void;
};

function deferred(): Deferred {
	let resolve!: () => void;
	return {
		wait: new Promise<void>((done) => {
			resolve = done;
		}),
		release: () => resolve(),
	};
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function tailResult(items: readonly Record<string, unknown>[], cursor?: string, terminal = false) {
	return {
		exitCode: 0,
		stdout: JSON.stringify({ ok: true, result: { items, ...(cursor ? { cursor } : {}), terminal } }),
		stderr: "",
	};
}

function assistantFrame(id: string, text: string): Record<string, unknown> {
	return { kind: "transcript", id, payload: { role: "assistant", content: [{ text }] } };
}

test("red-team: receipt-bound tail frames flush before later live frames, exactly once and before their cursors", async () => {
	const releaseFirstDelivery = deferred();
	const releaseLivePoll = deferred();
	const delivered: string[] = [];
	const cursors: string[] = [];
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1)
			return tailResult([assistantFrame("pre-a", "pre-a"), assistantFrame("pre-b", "pre-b")], "cursor-pre");
		if (calls === 2) {
			await releaseLivePoll.wait;
			return tailResult([assistantFrame("post", "post")], "cursor-post");
		}
		return tailResult([], undefined, true);
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-tail-gen3-order", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "tail-order",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-gen3-order",
		onFrame: async (frame) => {
			delivered.push(frame.assistantText ?? "");
			if (frame.eventId === "pre-a") await releaseFirstDelivery.wait;
		},
		onCursorCommitted: (cursor) => {
			cursors.push(cursor);
		},
	});
	try {
		await eventually(() => calls === 2, "tail did not enter the controlled live poll");
		await tail.markAccepted("turn-order");
		releaseLivePoll.release();
		await eventually(() => delivered.includes("pre-a"), "first buffered frame was not delivered");
		expect(delivered).toEqual(["pre-a"]);
		expect(cursors).toEqual([]);

		releaseFirstDelivery.release();
		await eventually(
			() => delivered.length === 3 && cursors.length === 2,
			"tail did not drain both sides of the receipt boundary",
		);
		expect(delivered).toEqual(["pre-a", "pre-b", "post"]);
		expect(new Set(delivered).size).toBe(3);
		expect(cursors).toEqual(["cursor-pre", "cursor-post"]);
	} finally {
		releaseFirstDelivery.release();
		releaseLivePoll.release();
		await tail.close();
	}
});

test("red-team: a failed delivery keeps its cursor uncommitted while later tail frames remain deliverable", async () => {
	const releaseLivePoll = deferred();
	const delivered: string[] = [];
	const cursors: string[] = [];
	const diagnostics: string[] = [];
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1) return tailResult([assistantFrame("failed", "lost if cursor advances")], "cursor-failed");
		if (calls === 2) {
			await releaseLivePoll.wait;
			return tailResult([assistantFrame("later", "later delivery")], "cursor-later");
		}
		return tailResult([], undefined, true);
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-tail-gen3-failure", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "tail-failure",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-gen3-failure",
		onFrame: async (frame) => {
			if (frame.eventId === "failed") throw new Error("delivery sink rejected the frame");
			delivered.push(frame.assistantText ?? "");
		},
		onCursorCommitted: (cursor) => {
			cursors.push(cursor);
		},
		onDiagnostic: (line) => {
			diagnostics.push(line);
		},
	});
	try {
		await eventually(() => calls === 2, "tail did not enter the controlled follow-up poll");
		await tail.markAccepted("turn-failure");
		await eventually(
			() => diagnostics.some((line) => line.startsWith("tail_flush_failed session=tail-failure")),
			"failed frame did not become a bounded tail diagnostic",
		);
		releaseLivePoll.release();
		await eventually(() => delivered.includes("later delivery"), "a failed callback poisoned delivery of the later frame");
		// A checkpoint after the rejected frame would make its side effect unrecoverable.
		expect(cursors).toEqual([]);
	} finally {
		releaseLivePoll.release();
		await tail.close();
	}
});

test("red-team: closing a tail during a buffered delivery fences later frames and cursor commits", async () => {
	const releaseFirstDelivery = deferred();
	const delivered: string[] = [];
	const cursors: string[] = [];
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1)
			return tailResult([assistantFrame("first", "first"), assistantFrame("after-close", "after close")], "cursor-close");
		return tailResult([], undefined, true);
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-tail-gen3-close", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "tail-close",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-gen3-close",
		onFrame: async (frame) => {
			delivered.push(frame.assistantText ?? "");
			if (frame.eventId === "first") await releaseFirstDelivery.wait;
		},
		onCursorCommitted: (cursor) => {
			cursors.push(cursor);
		},
	});
	try {
		await tail.markAccepted("turn-close");
		await eventually(() => delivered.includes("first"), "first buffered delivery did not begin");
		await tail.close();
		releaseFirstDelivery.release();
		await Bun.sleep(25);
		// close() ends this owner’s authority; a queued frame cannot leak to the old owner.
		expect(delivered).toEqual(["first"]);
		expect(cursors).toEqual([]);
	} finally {
		releaseFirstDelivery.release();
		await tail.close();
	}
});

test("red-team: an empty terminal poll never checkpoints past buffered frames still being delivered", async () => {
	const releaseFirstDelivery = deferred();
	const delivered: string[] = [];
	const cursors: string[] = [];
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1) return tailResult([assistantFrame("buffered", "buffered")], "cursor-buffered");
		// Every later poll is an empty terminal page carrying a newer checkpoint.
		return tailResult([], "cursor-empty-terminal", true);
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-tail-gen4-empty", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "tail-empty-terminal",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-gen4-empty",
		onFrame: async (frame) => {
			delivered.push(frame.assistantText ?? "");
			if (frame.eventId === "buffered") await releaseFirstDelivery.wait;
		},
		onCursorCommitted: (cursor) => {
			cursors.push(cursor);
		},
	});
	try {
		await eventually(() => calls >= 2, "tail did not reach the empty terminal polls");
		await tail.markAccepted("turn-empty");
		await eventually(() => delivered.includes("buffered"), "buffered frame delivery did not begin");
		// Side effect still in flight: no checkpoint may be persisted yet.
		await Bun.sleep(20);
		expect(cursors).toEqual([]);
		releaseFirstDelivery.release();
		await eventually(() => cursors.length > 0, "cursor never committed after the side effect finished");
		expect(delivered).toEqual(["buffered"]);
	} finally {
		releaseFirstDelivery.release();
		await tail.close();
	}
});

test("a cursorless attach polls non-strict, treats the runtime's pre-attach gap as diagnostic, and still delivers live frames", async () => {
	const argv: string[][] = [];
	const delivered: string[] = [];
	const diagnostics: string[] = [];
	const gapHolds: unknown[] = [];
	let calls = 0;
	const run: CliRunner = async (args) => {
		argv.push([...args]);
		calls++;
		// gjc 0.16.0 (measured): a cursorless tail answers ok:true with BOTH a
		// diagnostic retention gap and the frames after the resync point.
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				result: {
					checkpoint: { revision: 4, generation: 1, seq: 0 },
					gap: { code: "retention_gap", resync: { revision: 4, generation: 1, seq: 0 } },
					items: calls === 1 ? [{ kind: "transcript", id: "live-1", payload: { role: "assistant", content: [{ text: "live" }] } }] : [],
					terminal: calls > 1,
				},
			}),
			stderr: "",
		};
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-tail-cursorless", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "tail-cursorless",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-cursorless",
		onFrame: async (frame) => {
			delivered.push(frame.assistantText ?? "");
		},
		onRetentionGap: async (gap) => {
			gapHolds.push(gap);
		},
		onDiagnostic: (line) => {
			diagnostics.push(line);
		},
	});
	try {
		await tail.markAccepted("turn-cursorless");
		await eventually(() => delivered.includes("live"), "live frame after the diagnostic gap was not delivered");
		expect(argv[0]).not.toContain("--strict");
		expect(argv[0]).not.toContain("--cursor");
		expect(gapHolds).toEqual([]);
		expect(diagnostics.some((line) => line.startsWith("tail_gap_nonstrict session=tail-cursorless"))).toBe(true);
	} finally {
		await tail.close();
	}
});

test("event-driven: after one backfill poll, host stream frames are delivered as emitted with no further polls", async () => {
	const argv: string[][] = [];
	const delivered: string[] = [];
	const terminals: string[] = [];
	let pushLine: ((line: string) => void) | undefined;
	let closeStream: (() => void) | undefined;
	let opened = 0;
	const run: CliRunner = async (args) => {
		argv.push([...args]);
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { items: [], terminal: true } }), stderr: "" };
	};
	const runner = new TailRunner({
		run,
		repo: "/tmp/gajaeway-tail-stream",
		pollIntervalMs: 1,
		sleep: (ms) => Bun.sleep(ms),
		stream: () => {
			opened++;
			const queue: string[] = [];
			let notify: (() => void) | undefined;
			let done = false;
			pushLine = (line) => {
				queue.push(line);
				notify?.();
			};
			closeStream = () => {
				done = true;
				notify?.();
			};
			const lines = (async function* () {
				for (;;) {
					if (queue.length > 0) {
						yield queue.shift()!;
						continue;
					}
					if (done) return;
					await new Promise<void>((resolve) => {
						notify = resolve;
					});
					notify = undefined;
				}
			})();
			return { lines, close: () => closeStream?.() };
		},
	});
	const tail = await runner.attach({
		sessionId: "stream-1",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-stream",
		onFrame: async (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
			if (frame.rawKind === "agent_end" || frame.idle) terminals.push(frame.rawKind);
		},
	});
	try {
		await tail.markAccepted("turn-stream");
		await eventually(() => opened === 1, "stream was not opened after the backfill poll");
		const pollsAfterOpen = argv.length;
		pushLine!(JSON.stringify({ type: "hello", protocolVersion: 3 }));
		pushLine!(JSON.stringify({ type: "activity", sessionId: "stream-1", state: "busy" }));
		pushLine!(JSON.stringify({ type: "turn_stream", sessionId: "stream-1", phase: "finalized", text: "LIVE_OK", finalAnswer: true, messageRef: "7" }));
		pushLine!(JSON.stringify({ type: "agent_end", sessionId: "stream-1", turnId: "t-1" }));
		pushLine!(JSON.stringify({ type: "activity", sessionId: "stream-1", state: "idle" }));
		await eventually(() => delivered.includes("LIVE_OK") && terminals.includes("agent_end"), "live frames were not delivered from the stream");
		await Bun.sleep(30);
		// No interval polling while the stream is healthy.
		expect(argv.length).toBe(pollsAfterOpen);
	} finally {
		await tail.close();
	}
});
