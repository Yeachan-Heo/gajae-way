/**
 * Plan I4b acceptance matrix (a)-(j). Every case feeds a truncated `turn.result`
 * witness plus one complete transcript snapshot served over an in-memory
 * SessionChannel, then asserts which text (if any) is delivered and the
 * watermark row written by the terminalization transaction. Boundaries are
 * resolved as snapshot positions only: id spelling, timestamps and "next user
 * row" never select.
 */
import { expect, test } from "bun:test";
import { bodyHash } from "../src/orchestrator/persona-session";
import { SessionChannel, type TranscriptRow } from "../src/orchestrator/session-channel";
import { renderPrompt } from "../src/orchestrator/session-port";
import { eventually, harness, KEY } from "./red-first-harness";
import { ScriptedSessionPort } from "./session-port.fake";

const BIG = "긴 답변 ".repeat(4_000); // > 16 KiB
const PREFIX = BIG.slice(0, 2_048);

type Row = TranscriptRow & { readonly role: "user" | "assistant" };

function snapshotChannel(sessionId: string, rowsOf: () => readonly Row[], revision: string | undefined) {
	const listeners = new Set<(line: string) => void>();
	const transport = {
		write(line: string) {
			const frame = JSON.parse(line) as { id: string; query: string; cursor?: string };
			const answer = (payload: Record<string, unknown>) => {
				const encoded = JSON.stringify({ type: "query_response", id: frame.id, ...payload });
				queueMicrotask(() => {
					for (const listener of listeners) listener(encoded);
				});
			};
			if (frame.query === "session.checkpoint")
				answer({ ok: true, result: { checkpointToken: "tok", ...(revision ? { revisionId: revision } : {}) } });
			else if (frame.query === "transcript.list") {
				const rows = rowsOf();
				const offset = frame.cursor ? Number(frame.cursor) : 0;
				const complete = offset + 1 >= rows.length;
				answer({
					ok: true,
					page: {
						items: rows.slice(offset, offset + 1),
						complete,
						...(revision ? { revision } : {}),
						...(complete ? {} : { continuationCursor: String(offset + 1) }),
					},
				});
			} else answer({ ok: false, error: { code: "unsupported" } });
		},
		onLine(listener: (line: string) => void) {
			listeners.add(listener);
			return () => listeners.delete(listener);
		},
	};
	return new SessionChannel({ sessionId, transport, requestTimeoutMs: 2_000 });
}

async function run(options: {
	rows: (ctx: { promptHash: string; promptBody: string; steer?: string }) => Row[];
	steer?: string;
	priorWatermark?: { entryId: string; at?: string };
	revision?: string | undefined;
	terminalAt?: number;
}) {
	const port = new ScriptedSessionPort();
	port.truncateStatusContent = true;
	const h = await harness(port, {
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				h.deliveries.push({ text });
			},
		}),
	});
	let sessionId = "";
	let rows: Row[] = [];
	port.channelFor = (id) => {
		sessionId = id;
		return snapshotChannel(id, () => rows, "revision" in options ? options.revision : "rev-1");
	};
	try {
		h.enqueue("trigger");
		await h.manager.notifyInbound(KEY);
		await eventually(() => port.sends.length === 1, "prompt was not sent");
		const send = port.sends[0]!;
		const promptBody = renderPrompt(send.systemPreamble, send.text);
		const promptHash = bodyHash(promptBody);
		let steerText: string | undefined;
		if (options.steer) {
			h.enqueue(options.steer);
			await h.manager.notifyInbound(KEY);
			await eventually(() => port.steers.length === 1, "steer was not issued");
			steerText = port.steers[0]!.text;
		}
		if (options.priorWatermark) {
			h.database.terminalizeTurn({
				opRef: "op-previous",
				sessionId,
				textSource: "transcript",
				disposition: "delivered",
				terminalAt: options.priorWatermark.at ?? "2026-09-05T00:00:00.000Z",
				watermark: {
					snapshotRevision: "rev-0",
					snapshotGeneration: 1,
					triggerSeq: 0,
					lastSteerSeq: undefined,
					terminalSeq: 1,
					terminalEntryId: options.priorWatermark.entryId,
					terminalTs: "2026-09-05T00:00:00.000Z",
				},
			});
		}
		rows = options.rows({ promptHash, promptBody, ...(steerText ? { steer: steerText } : {}) });
		port.completeWithoutAnswerFrame(send.opRef, BIG);
		await Bun.sleep(50);
		await h.manager.tick(KEY);
		await Bun.sleep(150);
		const watermark = h.database.transcriptWatermark(sessionId);
		const holds = h.logs.filter((line) => line.includes("reason=no_terminal_text"));
		return { deliveries: h.deliveries, holds, watermark, send, steerText, sessionId };
	} finally {
		await h.close();
	}
}

const user = (id: string, body: string, ts = "2026-09-05T00:01:00.000Z"): Row => ({ id, role: "user", ts, body });
const assistant = (id: string, body: string, ts = "2026-09-05T00:02:00.000Z"): Row => ({
	id,
	role: "assistant",
	ts,
	body,
});

test("(a) >16 KiB unique prefix selects the one assistant row after the trigger", async () => {
	const r = await run({ rows: ({ promptBody }) => [trigger(promptBody, "u1"), assistant("a1", BIG)] });
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({
		snapshot_revision: "rev-1",
		trigger_seq: 0,
		terminal_seq: 1,
		terminal_entry_id: "a1",
	});
});

test("(b) trigger -> interim -> accepted steer -> final: the truncated witness selects the final row only", async () => {
	const r = await run({
		steer: "steer-1",
		rows: ({ promptBody, steer }) => [
			trigger(promptBody, "u1"),
			assistant("a-interim", `${PREFIX} interim`),
			user("u-steer", steer!),
			assistant("a-final", BIG),
		],
	});
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({
		trigger_seq: 0,
		last_steer_seq: 2,
		terminal_seq: 3,
		terminal_entry_id: "a-final",
	});
});

test("(c) interim after the floor without a matching final row holds", async () => {
	const r = await run({
		rows: ({ promptBody }) => [trigger(promptBody, "u1"), assistant("a-interim", "something else")],
	});
	expect(r.deliveries).toEqual([]);
	expect(r.holds.length).toBeGreaterThan(0);
	expect(r.holds[0]).toContain("candidates=0");
	expect(r.watermark).toBeUndefined();
});

test("(d) late prior output is rejected by pos_prev", async () => {
	const r = await run({
		priorWatermark: { entryId: "a-prev" },
		rows: ({ promptBody }) => [assistant("a-prev", BIG), trigger(promptBody, "u1"), assistant("a-now", BIG)],
	});
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({ op_ref: r.send.opRef, terminal_entry_id: "a-now", terminal_seq: 2 });
});

test("(e) two rows sharing the prefix refuse", async () => {
	const r = await run({
		rows: ({ promptBody }) => [trigger(promptBody, "u1"), assistant("a1", BIG), assistant("a2", BIG)],
	});
	expect(r.deliveries).toEqual([]);
	expect(r.holds[0]).toContain("candidates=2");
});

test("(f) decreasing ids: fffffffe trigger, 90000000 steer, 00000002 final -> final selected", async () => {
	const r = await run({
		steer: "steer-1",
		rows: ({ promptBody, steer }) => [
			trigger(promptBody, "fffffffe"),
			user("90000000", steer!),
			assistant("00000002", BIG),
		],
	});
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({
		trigger_seq: 0,
		last_steer_seq: 1,
		terminal_seq: 2,
		terminal_entry_id: "00000002",
	});
});

test("(g) random 8-hex ids in arbitrary lexical order -> same selection as (b)", async () => {
	const r = await run({
		steer: "steer-1",
		rows: ({ promptBody, steer }) => [
			trigger(promptBody, "9c1e0f77"),
			assistant("0000a1b2", `${PREFIX} interim`),
			user("ffee00aa", steer!),
			assistant("13579bdf", BIG),
		],
	});
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({ terminal_entry_id: "13579bdf", terminal_seq: 3 });
});

test("(h) steer after trigger with a prior watermark between them: boundary is the steer, prior answer excluded", async () => {
	const r = await run({
		steer: "steer-1",
		priorWatermark: { entryId: "a-prev" },
		rows: ({ promptBody, steer }) => [
			trigger(promptBody, "u1"),
			assistant("a-prev", BIG),
			user("u-steer", steer!),
			assistant("a-final", BIG),
		],
	});
	expect(r.deliveries.map((d) => d.text)).toEqual([BIG]);
	expect(r.watermark).toMatchObject({ op_ref: r.send.opRef, last_steer_seq: 2, terminal_entry_id: "a-final" });
});

test("(i) watermark id absent from the snapshot refuses -> hold", async () => {
	const r = await run({
		priorWatermark: { entryId: "vanished" },
		rows: ({ promptBody }) => [trigger(promptBody, "u1"), assistant("a1", BIG)],
	});
	expect(r.deliveries).toEqual([]);
	expect(r.holds[0]).toContain("previous_watermark_absent_from_snapshot");
});

test("(j) snapshot identity missing disables case (2) -> hold", async () => {
	const r = await run({
		revision: undefined,
		rows: ({ promptBody }) => [trigger(promptBody, "u1"), assistant("a1", BIG)],
	});
	expect(r.deliveries).toEqual([]);
	expect(r.holds[0]).toContain("snapshot_incomplete");
});

/** The user row the runtime wrote for this prompt: its body is the rendered prompt verbatim. */
function trigger(promptBody: string, id: string): Row {
	return user(id, promptBody);
}
