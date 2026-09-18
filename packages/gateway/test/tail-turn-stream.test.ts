import { expect, test } from "bun:test";
import type { CliRunner } from "@gajae-gateway/subsession";
import { TailRunner } from "../src/orchestrator/tail-runner";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("turn_stream emits only its explicit finalized final answer; live drafts and non-finalized text are progress", async () => {
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				result: {
					items:
						calls === 1
							? [
									{ kind: "turn_stream", id: "draft", payload: { phase: "live", text: "draft", finalAnswer: false } },
									{
										kind: "turn_stream",
										id: "not-final",
										payload: { phase: "finalized", text: "reasoning", finalAnswer: false },
									},
									{
										kind: "turn_stream",
										id: "final",
										payload: { phase: "finalized", text: "actual answer", finalAnswer: true },
									},
								]
							: [],
					terminal: calls > 1,
				},
			}),
			stderr: "",
		};
	};
	const observed: Array<{ id?: string; text?: string; rawKind: string }> = [];
	const runner = new TailRunner({
		run,
		repo: "/tmp/gajaeway-turn-stream",
		pollIntervalMs: 1,
		sleep: (ms) => Bun.sleep(ms),
	});
	const tail = await runner.attach({
		sessionId: "turn-stream-session",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-turn-stream",
		onFrame: (frame) => {
			observed.push({ id: frame.eventId, text: frame.assistantText, rawKind: frame.rawKind });
		},
	});
	try {
		await tail.markAccepted("turn-op");
		await eventually(() => observed.length === 3, "turn_stream frames were not observed");
		expect(observed.map((frame) => [frame.id, frame.text, frame.rawKind])).toEqual([
			["draft", undefined, "turn_stream"],
			["not-final", undefined, "turn_stream"],
			["final", "actual answer", "turn_stream"],
		]);
	} finally {
		await tail.close();
	}
});
