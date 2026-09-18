import { expect, test } from "bun:test";
import type { CliRunner } from "@gajaeway/subsession";
import { TailRunner } from "../src/orchestrator/tail-runner";

/**
 * A resumed poll replays the event ring, and the ring holds every earlier
 * turn's `turn_stream/finalized` answer. Those frames carry no host timestamp
 * (the actor's time fence cannot see them), only a ring position. The handle
 * snapshots the ring high-water mark when a turn begins; a positioned ring
 * event at or below it predates the prompt and is never handed to the actor.
 *
 * Live, 2026-09-18: one trigger + four steers on a session with eight earlier
 * turns → 15 messages delivered, 14 of them earlier turns' finalized answers.
 */

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 400; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

const ok = (result: Record<string, unknown>) => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});
const finalized = (seq: number, text: string, messageRef: string) => ({
	kind: "turn_stream",
	generation: 1,
	seq,
	revision: 5,
	payload: { type: "turn_stream", phase: "finalized", finalAnswer: true, text, messageRef },
});

test("earlier turns' finalized answers replayed by a resumed poll never reach the actor", async () => {
	const delivered: string[] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		polls++;
		// Every poll (backfill and each resume) replays the same eight earlier answers,
		// exactly as the live ring did; the current turn's answer arrives at seq 99.
		const earlier = Array.from({ length: 8 }, (_, i) => finalized(10 + i, `earlier answer ${i}`, String(i)));
		if (polls === 1) return ok({ items: earlier, cursor: "c1", terminal: true });
		if (polls < 4) return ok({ items: earlier, cursor: `c${polls}`, terminal: false });
		return ok({ items: [...earlier, finalized(99, "this turn's answer", "99")], cursor: `c${polls}`, terminal: true });
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-floor", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-floor",
		onFrame: async (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
		},
	});
	try {
		// Backfill observed seq 10..17 while idle. The turn begins: floor = 17.
		await tail.beginTurn("op-1");
		await tail.markAccepted("op-1");
		tail.setTurnRunning(true);
		await eventually(() => delivered.length >= 1, "this turn's answer was not delivered");
		await Bun.sleep(30);
		expect(delivered).toEqual(["this turn's answer"]);
	} finally {
		await tail.close();
	}
});

test("a ring event above the floor that is NOT this turn's is still fenced by op-ref attribution, as before", async () => {
	const delivered: string[] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		polls++;
		if (polls === 1) return ok({ items: [], cursor: "c1", terminal: true });
		return ok({
			items: [
				{
					kind: "turn_stream",
					generation: 1,
					seq: 50,
					payload: {
						type: "turn_stream",
						phase: "finalized",
						finalAnswer: true,
						text: "someone else's",
						opRef: "op-other",
					},
				},
				{
					kind: "turn_stream",
					generation: 1,
					seq: 51,
					payload: { type: "turn_stream", phase: "finalized", finalAnswer: true, text: "mine", opRef: "op-1" },
				},
			],
			cursor: `c${polls}`,
			terminal: true,
		});
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-floor", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-floor",
		onFrame: async (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
		},
	});
	try {
		await tail.beginTurn("op-1");
		await tail.markAccepted("op-1");
		tail.setTurnRunning(true);
		await eventually(() => delivered.length >= 1, "no delivery");
		await Bun.sleep(30);
		expect(delivered).toEqual(["mine"]);
	} finally {
		await tail.close();
	}
});

test("the previous turn's answer, delivered on the relay without a position, is not re-delivered by the next turn's resumed poll", async () => {
	// Live, 2026-09-18 06:02: "캐치업 완료했습니다" shipped as the terminal answer,
	// then 66 s later as the NEXT turn's interim. The relay strips (generation,
	// seq), so the frame never advanced the ring mark; the next turn's floor sat
	// below it; the resumed poll replayed it above the floor.
	const delivered: string[] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		polls++;
		// The host's ring: the previous answer sits at seq 40; the checkpoint says so.
		const previous = finalized(40, "캐치업 완료했습니다", "40");
		if (polls === 1)
			return ok({
				items: [],
				checkpoint: { revision: 1, generation: 1, seq: 39, idle: true },
				cursor: "c1",
				terminal: true,
			});
		// Turn-start checkpoint poll: ring end is 40 now (the previous answer landed).
		if (polls === 2)
			return ok({
				items: [previous],
				checkpoint: { revision: 2, generation: 1, seq: 40, idle: true },
				cursor: "c2",
				terminal: true,
			});
		// Resumed side polls replay it; this turn's answer arrives at 55.
		if (polls < 5)
			return ok({
				items: [previous],
				checkpoint: { revision: 2, generation: 1, seq: 40, idle: false },
				cursor: `c${polls}`,
				terminal: false,
			});
		return ok({
			items: [previous, finalized(55, "이번 턴 답", "55")],
			checkpoint: { revision: 3, generation: 1, seq: 55, idle: true },
			cursor: `c${polls}`,
			terminal: true,
		});
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-floor", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-floor",
		onFrame: async (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
		},
	});
	try {
		// Turn 1's answer came over the relay: a frame with text but no position.
		// receive() is the stream's entry, not part of the actor-facing interface.
		await (tail as unknown as { receive(frame: unknown): Promise<void> }).receive({
			kind: "turn_stream",
			rawKind: "turn_stream",
			payload: { phase: "finalized", finalAnswer: true, text: "캐치업 완료했습니다", messageRef: "40" },
			assistantText: "캐치업 완료했습니다",
			idle: false,
			terminal: true,
		});
		// Turn 2.
		await tail.beginTurn("op-2");
		await tail.markAccepted("op-2");
		tail.setTurnRunning(true);
		await eventually(() => delivered.some((t) => t === "이번 턴 답"), "this turn's answer was not delivered");
		await Bun.sleep(30);
		expect(delivered.filter((t) => t === "캐치업 완료했습니다")).toHaveLength(0);
		expect(delivered).toEqual(["이번 턴 답"]);
	} finally {
		await tail.close();
	}
});
