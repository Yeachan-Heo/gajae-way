import { expect, test } from "bun:test";
import type { CliRunner } from "@gajaeway/subsession";
import { TailRunner } from "../src/orchestrator/tail-runner";

function deferred() {
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

const final = (id: string, text: string) => ({
	kind: "turn_stream",
	id,
	payload: { phase: "finalized", finalAnswer: true, text, messageRef: id },
});

test("fresh-turn boundary discards historical turn_stream finals but preserves a fast final emitted after dispatch", async () => {
	const currentMayArrive = deferred();
	let calls = 0;
	const run: CliRunner = async () => {
		calls++;
		if (calls === 1)
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { items: [final("old", "previous answer")], cursor: "old-cursor" },
				}),
				stderr: "",
			};
		// beginTurn reads the ring's end with one bounded poll before it snapshots
		// the turn floor; it must not be the one this test holds back.
		if (calls === 2)
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						items: [],
						checkpoint: { revision: 1, generation: 1, seq: 1, idle: true },
						cursor: "start-cursor",
						terminal: true,
					},
				}),
				stderr: "",
			};
		if (calls === 3) {
			await currentMayArrive.wait;
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { items: [final("current", "current answer")], cursor: "new-cursor" },
				}),
				stderr: "",
			};
		}
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { items: [], terminal: true } }), stderr: "" };
	};
	const delivered: string[] = [];
	const cursors: string[] = [];
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-fresh-boundary", pollIntervalMs: 1 });
	const tail = await runner.attach({
		sessionId: "fresh-boundary",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-fresh-boundary",
		onFrame: (frame) => {
			if (frame.assistantText) delivered.push(frame.assistantText);
		},
		onCursorCommitted: (cursor) => {
			cursors.push(cursor);
		},
	});
	try {
		await tail.beginTurn("op-current");
		await tail.markAccepted("op-current");
		currentMayArrive.release();
		await eventually(() => delivered.length === 1, "current final was not delivered");
		expect(delivered).toEqual(["current answer"]);
		expect(cursors).toEqual(["start-cursor", "new-cursor"]);
	} finally {
		await tail.close();
	}
});
