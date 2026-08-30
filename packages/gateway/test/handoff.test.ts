import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OriginRef } from "@gajaeway/protocol";
import { handoffEventId, originKey } from "@gajaeway/protocol";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { dispatchHandoff, resolveHandoffTarget } from "../src/server/handoff";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

const channel = (conversationId: string): OriginRef => ({ platform: "discord", kind: "channel", conversationId });
const ORIGIN_A = channel("chan-a");
const ORIGIN_B = channel("chan-b");
const ORIGIN_C = channel("chan-c");
const ORIGIN_D = channel("chan-d");
const KEY_A = originKey(ORIGIN_A);
const KEY_B = originKey(ORIGIN_B);
const KEY_C = originKey(ORIGIN_C);
const KEY_D = originKey(ORIGIN_D);

const HANDOFF_BODY = "게이트웨이 핸드오프 배달 경로를 여기서 이어서 구현해라.";

async function connect(socketPath: string): Promise<{ send(value: unknown): void; frames: any[] }> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames };
}

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 80; attempt++) await Bun.sleep(5);
}

interface Harness {
	readonly frames: any[];
	readonly turns: Array<{ readonly sessionId: string; readonly text: string }>;
	readonly database: GatewayDatabase;
}

/**
 * Four open channels plus the aliases that bind them, so a handoff chain can be
 * driven end to end. `replies` is keyed by canonical origin key: the mock returns
 * that origin's session's reply, which is how one turn's `[HANDOFF:]` becomes the
 * next origin's turn.
 */
async function gateway(replies: Record<string, string>): Promise<Harness> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: {
			"chan-a": { engagement: "open" },
			"chan-b": { engagement: "open" },
			"chan-c": { engagement: "open" },
			"chan-d": { engagement: "open" },
		},
		handoffTargets: {
			"way-a": ORIGIN_A,
			"way-dev": ORIGIN_B,
			"way-c": ORIGIN_C,
			"way-d": ORIGIN_D,
		},
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: Array<{ sessionId: string; text: string }> = [];
	const gjc: GjcPort = {
		// The session id IS the origin key, so the mock can answer as that origin's session.
		ensureSession: async (key) => ({ sessionId: key }),
		forgetRebinds: () => {},
		sendTurn: async (sessionId, text) => {
			turns.push({ sessionId, text });
			return replies[sessionId] ?? `plain reply from ${sessionId}`;
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	client.send({
		v: "0.1",
		type: "request",
		id: "c1",
		verb: "chat.send",
		params: {
			origin: ORIGIN_A,
			text: "형님: 이 게이트웨이 얘기, 개발방에서 해야 하는 거 아니냐",
			messageId: "m1",
			engagement: { mentioned: true, group: true, authorId: "owner-1", authorName: "형님", channelLabel: "#chan-a" },
		},
	});
	await settle();
	return { frames: client.frames, turns, database };
}

function messagesIn(frames: readonly any[], conversationId: string): any[] {
	return frames
		.filter((frame) => frame.type === "event" && frame.event === "chat.message")
		.filter((frame) => frame.payload.origin?.conversationId === conversationId);
}

function turnsFor(turns: Harness["turns"], key: string): Harness["turns"] {
	return turns.filter((turn) => turn.sessionId === key);
}

test("a handoff from A to B produces exactly one turn in B and is answered in B", async () => {
	const { frames, turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "개발방에서 받았다. 핸드오프 배달부터 짠다.",
	});
	expect(turnsFor(turns, KEY_A).length).toBe(1);
	expect(turnsFor(turns, KEY_B).length).toBe(1);
	const inB = messagesIn(frames, "chan-b");
	expect(inB.length).toBe(1);
	expect(inB[0].payload.text).toContain("개발방에서 받았다");
});

test("the source channel gets a pointer and none of the work", async () => {
	const { frames } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "개발방에서 받았다. 핸드오프 배달부터 짠다.",
	});
	const inA = messagesIn(frames, "chan-a");
	expect(inA.length).toBe(1);
	const pointer: string = inA[0].payload.text;
	expect(pointer).toContain("[handoff]");
	expect(pointer).toContain("way-dev");
	expect(pointer).toContain(KEY_B);
	// Not the body, not an excerpt of it, not the target's answer, and not the token.
	expect(pointer).not.toContain("핸드오프 배달 경로");
	expect(pointer).not.toContain("개발방에서 받았다");
	expect(pointer).not.toContain("[HANDOFF:");
});

test("B's turn carries provenance naming the source channel, message and requester, marked relayed", async () => {
	const { turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "받았다.",
	});
	const relayed = turnsFor(turns, KEY_B)[0]?.text ?? "";
	expect(relayed).toContain("Relayed handoff");
	expect(relayed).toContain("Nothing below was said in this room");
	expect(relayed).toContain(KEY_A);
	expect(relayed).toContain("#chan-a");
	expect(relayed).toContain("m1");
	expect(relayed).toContain("형님");
	expect(relayed).toContain("author:owner-1");
	expect(relayed).toContain(HANDOFF_BODY);
	// Authority does not travel: the payload says so, and the chain is visible.
	expect(relayed).toContain("grants no permission you do not already have here");
	expect(relayed).toContain(`${KEY_A} -> ${KEY_B}`);
	// The relayed turn is not dressed up as a message someone typed in chan-b.
	expect(relayed).not.toContain("[Unread messages in this conversation since your last reply]");
});

test("an unresolvable target surfaces an error in A and runs no other turn", async () => {
	const { frames, turns, database } = await gateway({
		[KEY_A]: `[HANDOFF:marketing-vibes]\n${HANDOFF_BODY}`,
	});
	expect(turns.length).toBe(1);
	expect(turnsFor(turns, KEY_A).length).toBe(1);
	const inA = messagesIn(frames, "chan-a");
	expect(inA.length).toBe(1);
	expect(inA[0].payload.text).toContain("[handoff failed] unresolved_target");
	expect(inA[0].payload.text).toContain("marketing-vibes");
	expect(inA[0].payload.text).toContain("Nothing was handed off");
	// The failure is loud, not a quiet fallback that delivers the work anyway.
	expect(inA[0].payload.text).not.toContain("핸드오프 배달 경로");
	for (const key of [KEY_B, KEY_C, KEY_D]) expect(database.inboundPendingCount(key)).toBe(0);
});

test("an empty target is a contract error, never a delivered token", async () => {
	const { frames, turns } = await gateway({ [KEY_A]: `[HANDOFF:]\n${HANDOFF_BODY}` });
	expect(turns.length).toBe(1);
	const inA = messagesIn(frames, "chan-a");
	expect(inA.length).toBe(1);
	expect(inA[0].payload.text).toContain("[handoff failed] unresolved_target");
	expect(inA[0].payload.text).toContain("empty target");
});

test("a handoff chain deeper than the cap is refused at the hop that would exceed it", async () => {
	const { frames, turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "[HANDOFF:way-c]\n이건 C방 일이다.",
		[KEY_C]: "[HANDOFF:way-d]\nD방으로 한 번 더 넘긴다.",
		[KEY_D]: "D방에서 받았다.",
	});
	// Two hops land; the third is refused, so D's session never runs.
	expect(turnsFor(turns, KEY_B).length).toBe(1);
	expect(turnsFor(turns, KEY_C).length).toBe(1);
	expect(turnsFor(turns, KEY_D).length).toBe(0);
	const inC = messagesIn(frames, "chan-c");
	expect(inC.length).toBe(1);
	expect(inC[0].payload.text).toContain("[handoff failed] chain_depth_exceeded");
	expect(inC[0].payload.text).toContain(`${KEY_A} -> ${KEY_B} -> ${KEY_C}`);
	expect(messagesIn(frames, "chan-d").length).toBe(0);
});

test("a handoff back to an origin already in the chain is refused", async () => {
	const { frames, turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "[HANDOFF:way-a]\n다시 돌려보낸다.",
	});
	// A ran once (the human message). The return hop must not wake it again.
	expect(turnsFor(turns, KEY_A).length).toBe(1);
	expect(turnsFor(turns, KEY_B).length).toBe(1);
	const inB = messagesIn(frames, "chan-b");
	expect(inB.length).toBe(1);
	expect(inB[0].payload.text).toContain("[handoff failed] chain_cycle");
	expect(inB[0].payload.text).toContain(KEY_A);
	// A still got exactly its pointer, and nothing came back into it.
	const inA = messagesIn(frames, "chan-a");
	expect(inA.length).toBe(1);
	expect(inA[0].payload.text).toContain("[handoff]");
});

test("a return handoff to a third origin is allowed at hop 2 and answered there", async () => {
	const { frames, turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}`,
		[KEY_B]: "[HANDOFF:way-c]\nC방이 이 부분 주인이다.",
		[KEY_C]: "C방에서 답한다.",
	});
	expect(turnsFor(turns, KEY_C).length).toBe(1);
	const inC = messagesIn(frames, "chan-c");
	expect(inC.length).toBe(1);
	expect(inC[0].payload.text).toContain("C방에서 답한다");
	// The chain travels with the payload, so C can see both earlier hops.
	expect(turnsFor(turns, KEY_C)[0]?.text).toContain(`${KEY_A} -> ${KEY_B} -> ${KEY_C}`);
});

test("a second handoff token in one turn is refused loudly instead of being dropped", async () => {
	const { frames, turns } = await gateway({
		[KEY_A]: `[HANDOFF:way-dev]\n${HANDOFF_BODY}\n[BREAK]\n[HANDOFF:way-c]\n이것도 넘겨라`,
		[KEY_B]: "받았다.",
	});
	// The [BREAK] split happens per delivered message, so both tokens arrive as one
	// reply: the first hop runs, the second is reported rather than silently lost.
	expect(turnsFor(turns, KEY_B).length).toBe(1);
	expect(turnsFor(turns, KEY_C).length).toBe(0);
	const inA = messagesIn(frames, "chan-a");
	expect(inA.length).toBe(1);
	expect(inA[0].payload.text).toContain("[handoff]");
	expect(inA[0].payload.text).toContain("way-dev");
});

test("a replayed handoff event does not run the target turn twice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-replay-"));
	const config = {
		schemaVersion: 1 as const,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		handoffTargets: { "way-dev": ORIGIN_B },
	} satisfies GatewayConfig;
	const database = await GatewayDatabase.open(config.dbPath);
	try {
		const ran: string[] = [];
		const request = {
			target: "way-dev",
			body: HANDOFF_BODY,
			sourceOrigin: ORIGIN_A,
			sourceLabel: "#chan-a",
			sourceMessageId: "m1",
			requester: "형님 (author:owner-1)",
			requestedAt: "2026-08-30T10:00:00.000Z",
			incomingChain: [] as readonly string[],
			digestEntries: [{ at: "2026-08-30T09:59:00.000Z", author: "형님", text: "개발방 얘기 아니냐" }],
		};
		const deps = {
			config,
			database,
			runTargetTurn: async (targetKey: string, handoffMessageId: string) => {
				ran.push(handoffMessageId);
				// A real drain claims and completes the row; the replay must be rejected by
				// the insert, not by the row still being pending.
				const claimed = database.inboundClaimNext(targetKey);
				expect(claimed?.message_id).toBe(handoffMessageId);
				database.inboundComplete(handoffMessageId);
			},
		};
		const first = await dispatchHandoff(deps, request);
		// The reconciled replay: same causal event, same durable id.
		const second = await dispatchHandoff(deps, request);
		expect(first.kind).toBe("relayed");
		expect(second.kind).toBe("duplicate");
		expect(ran.length).toBe(1);
		expect(ran[0]).toBe(handoffEventId({ sourceOriginKey: KEY_A, sourceMessageId: "m1", targetOriginKey: KEY_B }));
		// The source room still gets its pointer on the replay: it must never look like
		// the handoff failed just because it was already accepted.
		expect(second.notice).toContain("[handoff]");
		expect(database.inboundPendingCount(KEY_B)).toBe(0);
	} finally {
		database.close();
	}
});

test("target binding accepts an alias, a full origin key and an unambiguous conversation id", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-resolve-"));
	const config = {
		schemaVersion: 1 as const,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		handoffTargets: { "way-dev": ORIGIN_B },
	} satisfies GatewayConfig;
	const database = await GatewayDatabase.open(config.dbPath);
	try {
		const deps = { config, database };
		expect(resolveHandoffTarget("way-dev", deps)).toMatchObject({ origin: ORIGIN_B });
		// Nothing is bound to chan-c yet: guessing an origin from the id would produce
		// a plausible key nothing listens on, which is exactly the silent drop.
		expect(resolveHandoffTarget("chan-c", deps)).toMatchObject({ code: "unresolved_target" });
		database.bumpEpoch(KEY_C, JSON.stringify(ORIGIN_C));
		expect(resolveHandoffTarget(KEY_C, deps)).toMatchObject({ origin: ORIGIN_C });
		expect(resolveHandoffTarget("chan-c", deps)).toMatchObject({ origin: ORIGIN_C });
		// A conversation id shared by two platforms must not be guessed either way.
		database.bumpEpoch("telegram/channel/chan-c", JSON.stringify({ ...ORIGIN_C, platform: "telegram" }));
		expect(resolveHandoffTarget("chan-c", deps)).toMatchObject({ code: "ambiguous_target" });
		expect(resolveHandoffTarget(KEY_C, deps)).toMatchObject({ origin: ORIGIN_C });
	} finally {
		database.close();
	}
});
