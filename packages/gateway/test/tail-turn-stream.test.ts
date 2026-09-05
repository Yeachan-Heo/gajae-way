import { expect, test } from "bun:test";
import type { CliRunner } from "@gajaeway/subsession";
import { decodeStreamLine, TailRunner } from "../src/orchestrator/tail-runner";

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

test("stream frames retain revision and qualify synthetic generation/seq identity", () => {
	const frames = decodeStreamLine(
		JSON.stringify({
			type: "turn_stream",
			revision: 7,
			generation: 1,
			seq: 2,
			phase: "finalized",
			text: "stream answer",
			finalAnswer: true,
		}),
	);
	expect(frames).toHaveLength(1);
	expect(frames[0]).toMatchObject({
		revision: 7,
		generation: 1,
		seq: 2,
		eventId: "7:1:2",
		assistantText: "stream answer",
	});
});

test("revision-qualified generation/seq identities deliver successive finals exactly once", async () => {
	let calls = 0;
	let releaseSecondPoll!: () => void;
	const secondPoll = new Promise<void>((resolve) => {
		releaseSecondPoll = resolve;
	});
	const answer = (revision: number, text: string) => ({
		kind: "transcript",
		revision,
		generation: 1,
		seq: 1,
		payload: { role: "assistant", content: [{ text }] },
	});
	const page = (items: readonly Record<string, unknown>[], terminal = false) => ({
		exitCode: 0,
		stdout: JSON.stringify({ ok: true, result: { items, terminal } }),
		stderr: "",
	});
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1) return page([]);
		if (calls === 2) {
			await secondPoll;
			return page([answer(1, "revision one")]);
		}
		if (calls === 3) return page([answer(2, "revision two")]);
		if (calls === 4) return page([answer(1, "revision one"), answer(2, "revision two")]);
		return page([], true);
	};
	const observed: Array<{ revision?: number; id?: string; text?: string }> = [];
	const runner = new TailRunner({
		run,
		repo: "/tmp/gajaeway-tail-revision-identity",
		pollIntervalMs: 1,
		sleep: (ms) => Bun.sleep(ms),
	});
	const tail = await runner.attach({
		sessionId: "revision-identity-session",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-revision-identity",
		onFrame: (frame) => {
			if (frame.assistantText) observed.push({ revision: frame.revision, id: frame.eventId, text: frame.assistantText });
		},
	});
	try {
		await tail.markAccepted("revision-identity-op");
		releaseSecondPoll();
		await eventually(() => observed.length === 2 && calls >= 5, "revision-qualified finals were not delivered exactly once");
		expect(observed).toEqual([
			{ revision: 1, id: "1:1:1", text: "revision one" },
			{ revision: 2, id: "2:1:1", text: "revision two" },
		]);
	} finally {
		releaseSecondPoll();
		await tail.close();
	}
});

test("a deliverable old-provider frame without revision is rejected while lifecycle frames remain usable", async () => {
	let calls = 0;
	const diagnostics: string[] = [];
	const observed: string[] = [];
	const run: CliRunner = async () => {
		calls++;
		const items =
			calls === 1
				? [
						{
							kind: "transcript",
							generation: 1,
							seq: 1,
							payload: { role: "assistant", content: [{ text: "old answer" }] },
						},
				  ]
				: calls === 2
					? [{ kind: "agent_end", payload: {} }]
					: [];
		return {
			exitCode: 0,
			stdout: JSON.stringify({ ok: true, result: { items, terminal: calls >= 2 } }),
			stderr: "",
		};
	};
	const runner = new TailRunner({
		run,
		repo: "/tmp/gajaeway-tail-old-provider",
		pollIntervalMs: 1,
		sleep: (ms) => Bun.sleep(ms),
	});
	const tail = await runner.attach({
		sessionId: "old-provider-session",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-tail-old-provider",
		onFrame: (frame) => observed.push(frame.assistantText ?? frame.rawKind),
		onDiagnostic: (line) => diagnostics.push(line),
	});
	try {
		await tail.markAccepted("old-provider-op");
		await eventually(
			() => diagnostics.some((line) => line.includes("missing_revision_qualified_identity")),
			"missing revision was not diagnosed",
		);
		await eventually(() => observed.includes("agent_end"), "lifecycle frame was not delivered");
		expect(observed).toEqual(["agent_end"]);
	} finally {
		await tail.close();
	}
});
