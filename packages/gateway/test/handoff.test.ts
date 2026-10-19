import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigError, type GatewayConfig, parseConfigFile } from "../src/config";
import {
	buildHandoffDigest,
	extendHandoffChain,
	extractRoutingBlocks,
	extractWakeLines,
	extractWatchLines,
	HANDOFF_DIGEST_MAX_LENGTH,
	handoffMessageId,
	parseWatchTtl,
	resolveHandoffTarget,
	WATCH_MAX_TTL_MS,
} from "../src/server/handoff";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const A = { platform: "discord", kind: "channel", conversationId: "chan-a" } as const;
const B = { platform: "discord", kind: "channel", conversationId: "chan-b" } as const;
const C = { platform: "discord", kind: "channel", conversationId: "chan-c" } as const;
const KEY_A = "discord/channel/chan-a";
const KEY_B = "discord/channel/chan-b";
const KEY_C = "discord/channel/chan-c";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

interface Client {
	send(value: unknown): void;
	frames: any[];
}

async function connect(socketPath: string): Promise<Client> {
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

type Replies = Record<string, (turnText: string) => string>;

/** Three open channels; each origin's session answers with its own scripted reply. */
async function gateway(replies: Replies) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-a": { engagement: "open" }, "chan-b": { engagement: "open" }, "chan-c": { engagement: "open" } },
		handoffTargets: { "gajae-way-dev": B, ops: C, marketing: A },
	};
	const database = await Store.open(config.dbPath);
	const turns: Array<{ originKey: string; text: string }> = [];
	const sessions = new Map<string, string>();
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => {
			const sessionId = `session-${originKey.replaceAll("/", "_")}-${epoch}`;
			sessions.set(sessionId, originKey);
			return sessionId;
		},
		respond: async (sessionId, text) => {
			const originKey = sessions.get(sessionId) as string;
			turns.push({ originKey, text });
			return replies[originKey]?.(text) ?? "[SILENT]";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	return { client, database, turns };
}

function say(client: Client, origin: typeof A | typeof B | typeof C, messageId: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id: `req-${messageId}`,
		verb: "chat.send",
		params: {
			origin,
			text,
			messageId,
			engagement: { mentioned: true, group: true, authorId: "owner-1", authorName: "형님" },
		},
	});
}

function delivered(client: Client, conversationId: string): string[] {
	return client.frames
		.filter(
			(frame) =>
				frame.type === "event" &&
				frame.event === "chat.message" &&
				frame.payload.origin?.conversationId === conversationId,
		)
		.map((frame) => frame.payload.text as string);
}

test("a handoff from A to B runs exactly one turn in B, answered in B, and A gets only a pointer", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev]\nGateway tail stalls on reconnect; please triage the relay.",
		[KEY_B]: () => "Triaging the relay stall here.",
	});
	say(client, A, "m1", "이거 게이트웨이 tail 끊기는 거 왜 그래?");
	await settle();

	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	const relayed = targetTurns[0]?.text ?? "";
	// Provenance: source channel, source message, requester, and the relayed marker.
	expect(relayed).toContain("RELAYED HANDOFF");
	expect(relayed).toContain(KEY_A);
	expect(relayed).toContain("msg:m1");
	expect(relayed).toContain("형님");
	expect(relayed).toContain("not the requester's or the owner's instruction");
	expect(relayed).toContain("Gateway tail stalls on reconnect");
	// The digest carries the source conversation.
	expect(relayed).toContain("tail 끊기는 거");

	expect(delivered(client, "chan-b")).toEqual(["Triaging the relay stall here."]);
	const pointer = delivered(client, "chan-a");
	expect(pointer).toHaveLength(1);
	expect(pointer[0]).toStartWith("Moved to gajae-way-dev");
	expect(pointer[0]).not.toContain("[HANDOFF:");
	expect(pointer[0]).not.toContain("please triage the relay");
});

test("an unresolvable target surfaces an error in A and runs nothing anywhere else", async () => {
	const { client, turns } = await gateway({ [KEY_A]: () => "[HANDOFF:nowhere]\nplease take this" });
	say(client, A, "m1", "work item");
	await settle();
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A]);
	const notice = delivered(client, "chan-a");
	expect(notice).toHaveLength(1);
	expect(notice[0]).toContain("Handoff to nowhere failed");
	expect(notice[0]).toContain("unknown handoff target");
	expect(delivered(client, "chan-b")).toEqual([]);
});

test("a chain deeper than the cap is refused at the hop that would exceed it", async () => {
	// A -> B -> C is two hops; C cannot hand on to a fourth room.
	const D = { platform: "discord", kind: "channel", conversationId: "chan-d" } as const;
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev]\nfirst hop",
		[KEY_B]: () => "[HANDOFF:ops]\nsecond hop",
		[KEY_C]: () => `[HANDOFF:discord:${D.conversationId}]\nthird hop`,
	});
	say(client, A, "m1", "start");
	await settle();
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A, KEY_B, KEY_C]);
	expect(turns[2]?.text).toContain(`${KEY_A} -> ${KEY_B} -> this conversation`);
	const refusal = delivered(client, "chan-c");
	expect(refusal).toHaveLength(1);
	expect(refusal[0]).toContain("would exceed 2 hops");
	expect(delivered(client, "chan-d")).toEqual([]);
});

test("a handoff to an origin already in the chain is refused", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev]\ntake it",
		[KEY_B]: () => "[HANDOFF:marketing]\nback to you",
	});
	say(client, A, "m1", "start");
	await settle();
	// A ran once (the human's message), B once (the handoff); nothing ran A again.
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A, KEY_B]);
	const refusal = delivered(client, "chan-b");
	expect(refusal).toHaveLength(1);
	expect(refusal[0]).toContain("already in this handoff chain");
	expect(delivered(client, "chan-a")).toHaveLength(1);
});

test("a replayed handoff for the same source message does not run the target turn twice", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev]\ntake it",
		[KEY_B]: () => "on it",
	});
	say(client, A, "m1", "start");
	await settle();
	// The edit re-runs A's turn for the SAME source message; A hands off again.
	client.send({
		v: "0.1",
		type: "request",
		id: "edit-1",
		verb: "chat.edit",
		params: {
			origin: A,
			messageId: "m1",
			text: "start (edited)",
			engagement: { mentioned: true, group: true, authorId: "owner-1", authorName: "형님" },
		},
	});
	await settle();
	expect(turns.filter((turn) => turn.originKey === KEY_A)).toHaveLength(2);
	expect(turns.filter((turn) => turn.originKey === KEY_B)).toHaveLength(1);
	expect(delivered(client, "chan-b")).toEqual(["on it"]);
});

test("routing blocks parse anywhere, repeat, and close on any close token", () => {
	expect(extractRoutingBlocks("[HANDOFF:dev]\nbody\n[HANDOFFDONE]")).toEqual({
		text: "",
		handoffs: [{ target: "dev", note: "body" }],
	});
	expect(extractRoutingBlocks("앞 [HANDOFF: dev ] mid note [HANDOFFDONE] 뒤")).toEqual({
		text: "앞 뒤",
		handoffs: [{ target: "dev", note: "mid note" }],
	});
	expect(
		extractRoutingBlocks("[HANDOFF:a] one [HANDOFFDONE][HANDOFF:b] two [HANDOFFDONE][HANDOFF:a] three [HANDOFFDONE]"),
	).toEqual({
		text: "",
		handoffs: [
			{ target: "a", note: "one\n\nthree" },
			{ target: "b", note: "two" },
		],
	});
	// The old first-line form is an unclosed block: its note runs to the end.
	expect(extractRoutingBlocks("[HANDOFF:dev]\nbody")).toEqual({
		text: "",
		handoffs: [{ target: "dev", note: "body" }],
	});
	// An unclosed block ends at the next opener; nothing of it leaks into the reply.
	expect(extractRoutingBlocks("keep\n[HANDOFF:a] one [HANDOFF:b] two")).toEqual({
		text: "keep",
		handoffs: [
			{ target: "a", note: "one" },
			{ target: "b", note: "two" },
		],
	});
	// A mismatched close token still closes; a stray one is never delivered.
	expect(extractRoutingBlocks("[HANDOFF:a] x [POSTDONE] after [SENDDONE]")).toEqual({
		text: "after",
		handoffs: [{ target: "a", note: "x" }],
	});
	// An empty target is still a block, so the server can say why nothing was sent.
	expect(extractRoutingBlocks("[HANDOFF:]\nbody")).toEqual({ text: "", handoffs: [{ target: "", note: "body" }] });
});

test("post and send blocks become [POST:] parts in place; an unclosed one ends at [BREAK]", () => {
	expect(extractRoutingBlocks("여기 답 [SEND:ops] 배포 끝 [SENDDONE] 그리고 끝")).toEqual({
		text: "여기 답\n[BREAK]\n[POST:ops] 배포 끝\n[BREAK]\n그리고 끝",
		handoffs: [],
	});
	expect(extractRoutingBlocks("[POST:discord/channel/r] 보고\n[BREAK]\n로컬")).toEqual({
		text: "[POST:discord/channel/r] 보고\n[BREAK]\n로컬",
		handoffs: [],
	});
	expect(extractRoutingBlocks("[POST:a] one [POSTDONE][HANDOFF:b] note [HANDOFFDONE][POST:c]two[POSTDONE]")).toEqual({
		text: "[POST:a] one\n[BREAK]\n[POST:c] two",
		handoffs: [{ target: "b", note: "note" }],
	});
	// An empty post carries nothing to send.
	expect(extractRoutingBlocks("hi [POST:a] [POSTDONE]").text).toBe("hi");
});

test("tokens quoted in code are text", () => {
	const text = "Example: `[HANDOFF:inline]` and `[POST:x] y`\n\n```text\n[HANDOFF:example]\n[SEND:x] z [SENDDONE]\n```";
	expect(extractRoutingBlocks(text)).toEqual({ text, handoffs: [] });
	expect(extractRoutingBlocks("~~~\n[HANDOFF:a]\n~~~\n[HANDOFF:b] real").handoffs).toEqual([
		{ target: "b", note: "real" },
	]);
});

test("targets resolve by alias, origin key or platform:channel, and only to chat origins", () => {
	const config = { handoffTargets: { dev: B } };
	expect(resolveHandoffTarget("dev", config)).toMatchObject({ ok: true, originKey: KEY_B });
	expect(resolveHandoffTarget(KEY_C, config)).toMatchObject({ ok: true, originKey: KEY_C });
	expect(resolveHandoffTarget("slack:C9", config)).toMatchObject({ ok: true, originKey: "slack/channel/C9" });
	expect(resolveHandoffTarget("monitor/eventtype/x", config)).toMatchObject({ ok: false });
	expect(resolveHandoffTarget("loopback/loopback/loopback", config)).toMatchObject({ ok: false });
	expect(resolveHandoffTarget("dev2", config)).toMatchObject({ ok: false });
});

test("the chain bound and the idempotency key are pure functions of the hop", () => {
	expect(extendHandoffChain([], KEY_A, KEY_B)).toEqual({ ok: true, chain: [KEY_A] });
	expect(extendHandoffChain([KEY_A], KEY_B, KEY_C)).toEqual({ ok: true, chain: [KEY_A, KEY_B] });
	expect(extendHandoffChain([KEY_A, KEY_B], KEY_C, "discord/channel/d")).toMatchObject({ ok: false });
	expect(extendHandoffChain([KEY_A], KEY_B, KEY_A)).toMatchObject({ ok: false });
	expect(extendHandoffChain([], KEY_A, KEY_A)).toMatchObject({ ok: false });
	expect(handoffMessageId(KEY_A, "m1", KEY_B)).toBe(handoffMessageId(KEY_A, "m1", KEY_B));
	expect(handoffMessageId(KEY_A, "m1", KEY_B)).not.toBe(handoffMessageId(KEY_A, "m2", KEY_B));
});

test("the digest keeps the newest lines under the monitor digest ceiling", () => {
	const entries = Array.from({ length: 200 }, (_, index) => ({
		at: `2026-09-25T00:${String(index % 60).padStart(2, "0")}:00Z`,
		author: "형님",
		body: `line ${index} ${"x".repeat(200)}`,
	}));
	const digest = buildHandoffDigest(entries);
	expect(digest.length).toBeLessThanOrEqual(HANDOFF_DIGEST_MAX_LENGTH);
	expect(digest).toContain("line 199");
	expect(digest).not.toContain("line 0 ");
});

test("handoffTargets must name chat origins", () => {
	expect(parseConfigFile({ schemaVersion: 1, handoffTargets: { dev: B } }).handoffTargets).toEqual({ dev: B });
	for (const handoffTargets of [
		{ dev: { platform: "monitor", kind: "eventtype", conversationId: "x" } },
		{ dev: { platform: "discord", kind: "channel" } },
		{ "a/b": B },
		[],
	])
		expect(() => parseConfigFile({ schemaVersion: 1, handoffTargets })).toThrow(ConfigError);
});

test("a handoff token at the start of a later [BREAK] block is parsed", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "Some initial analysis.\n\n[BREAK]\n\n[HANDOFF:gajae-way-dev]\nPlease take this from here.",
		[KEY_B]: () => "Got it, handling now.",
	});
	say(client, A, "m1", "work item");
	await settle();

	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	const relayed = targetTurns[0]?.text ?? "";
	expect(relayed).toContain("RELAYED HANDOFF");
	expect(relayed).toContain("Please take this from here");

	expect(delivered(client, "chan-b")).toEqual(["Got it, handling now."]);
});

test("a token on any line hands off; the text before it is delivered here", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "Analysis.\n[HANDOFF:gajae-way-dev]\nThe note after the token.",
		[KEY_B]: () => "Received.",
	});
	say(client, A, "m1", "work item");
	await settle();

	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	expect(targetTurns[0]?.text ?? "").toContain("The note after the token.");
	const sourceMessages = delivered(client, "chan-a");
	expect(sourceMessages).toEqual([
		"Analysis.",
		"Moved to gajae-way-dev: this work belongs to that conversation, and the follow-up continues there.",
	]);
});

test("an empty handoff target is stripped from delivery and reported", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "Some analysis.\n[HANDOFF:]\nThe target above is empty.\n[HANDOFFDONE]\nAfter the block.",
	});
	say(client, A, "m1", "work item");
	await settle();

	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A]);
	const deliveredA = delivered(client, "chan-a");
	expect(deliveredA).toHaveLength(2);
	expect(deliveredA.join("\n")).not.toContain("[HANDOFF");
	expect(deliveredA[0]).toBe("Some analysis.\nAfter the block.");
	expect(deliveredA[1]).toContain("Handoff to  failed");
});

test("an inline token hands off; a token in inline code stays literal text", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () =>
			"See [HANDOFF:gajae-way-dev] relay triage [HANDOFFDONE] for more info. Use `[HANDOFF:code]` in your implementation.",
		[KEY_B]: () => "Received.",
	});
	say(client, A, "m1", "work item");
	await settle();

	expect(turns.find((turn) => turn.originKey === KEY_B)?.text ?? "").toContain("relay triage");
	const deliveredA = delivered(client, "chan-a");
	expect(deliveredA[0]).toBe("See for more info. Use `[HANDOFF:code]` in your implementation.");
	expect(deliveredA[1]).toContain("Moved to gajae-way-dev");
});

test("several handoffs, posts and local text parse from one reply", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () =>
			"[REACT:👀]\n로컬 답변입니다.\n[HANDOFF: gajae-way-dev] relay stall triage [HANDOFFDONE]\n[BREAK]\n두번째 파트\n[HANDOFF:ops]deploy check[HANDOFFDONE] [HANDOFF:nowhere] lost [HANDOFFDONE]",
		[KEY_B]: () => "B here",
		[KEY_C]: () => "C here",
	});
	say(client, A, "m1", "start");
	await settle();
	expect(turns.map((turn) => turn.originKey).sort()).toEqual([KEY_A, KEY_B, KEY_C]);
	expect(turns.find((turn) => turn.originKey === KEY_B)?.text).toContain("relay stall triage");
	expect(turns.find((turn) => turn.originKey === KEY_B)?.text).not.toContain("deploy check");
	expect(turns.find((turn) => turn.originKey === KEY_C)?.text).toContain("deploy check");
	expect(delivered(client, "chan-b")).toEqual(["B here"]);
	expect(delivered(client, "chan-c")).toEqual(["C here"]);
	const reactions = client.frames.filter(
		(frame) =>
			frame.event === "chat.message" && frame.payload.origin?.conversationId === "chan-a" && frame.payload.reaction,
	);
	expect(reactions).toHaveLength(1);
	const local = delivered(client, "chan-a").filter((text) => text !== "👀");
	expect(local[0]).toBe("로컬 답변입니다.");
	expect(local[1]).toBe("두번째 파트");
	expect(local.slice(2).join("\n")).toContain("Moved to gajae-way-dev");
	expect(local.slice(2).join("\n")).toContain("Moved to ops");
	expect(local.slice(2).join("\n")).toContain("Handoff to nowhere failed");
	expect(local.join("\n")).not.toContain("HANDOFF");
});

test("two spellings of one target are one hop carrying both notes", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev] first [HANDOFFDONE] [HANDOFF:discord:chan-b] second [HANDOFFDONE]",
		[KEY_B]: () => "ok",
	});
	say(client, A, "m1", "start");
	await settle();
	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	expect(targetTurns[0]?.text).toContain("first");
	expect(targetTurns[0]?.text).toContain("second");
	expect(delivered(client, "chan-a").filter((text) => text.startsWith("Moved to"))).toHaveLength(1);
});

test("a later-block handoff delivers the preceding text before enqueueing", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "Analysis.\n\n[BREAK]\n\n[HANDOFF:gajae-way-dev]\nHandoff note.",
		[KEY_B]: () => "Received.",
	});
	say(client, A, "m1", "work");
	await settle();

	// Check target received the handoff
	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	const relayed = targetTurns[0]?.text ?? "";
	expect(relayed).toContain("RELAYED HANDOFF");
	expect(relayed).toContain("Handoff note");

	const sourceMessages = delivered(client, "chan-a");
	expect(sourceMessages).toHaveLength(2);
	expect(sourceMessages[0]).toContain("Analysis.");
	expect(sourceMessages[1]).toContain("Moved to gajae-way-dev");
});

test("a reaction in the preceding block is delivered before the handoff", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "[REACT:👀]\nAnalysis.\n\n[BREAK]\n\n[HANDOFF:gajae-way-dev]\nForward this.",
		[KEY_B]: () => "Received.",
	});
	say(client, A, "m1", "work");
	await settle();

	const reactions = client.frames.filter(
		(frame) =>
			frame.event === "chat.message" && frame.payload.origin?.conversationId === "chan-a" && frame.payload.reaction,
	);
	expect(reactions).toHaveLength(1);
	expect(reactions[0]?.payload.reaction).toMatchObject({ targetMessageId: "m1", emoji: "👀" });
	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	expect(targetTurns[0]?.text ?? "").toContain("Forward this.");
	expect(delivered(client, "chan-a").some((text) => text?.includes("[HANDOFF:"))).toBe(false);
});

test("a standalone token in a fenced code example is not parsed or stripped", async () => {
	const { client, turns } = await gateway({
		[KEY_A]: () => "Example syntax:\n```text\n[HANDOFF:gajae-way-dev]\n```\nThis is documentation.",
	});
	say(client, A, "m1", "work item");
	await settle();

	expect(turns.filter((turn) => turn.originKey === KEY_B)).toHaveLength(0);
	expect(delivered(client, "chan-a").join("\n")).toContain("[HANDOFF:gajae-way-dev]");
});

test("a [WAKE:] line is parsed and stripped; the rest of the reply and code examples stay", () => {
	expect(extractWakeLines("Done here.\n[WAKE:gajae-way-dev] build is green again\nMore text.")).toEqual({
		text: "Done here.\nMore text.",
		wakes: [{ target: "gajae-way-dev", summary: "build is green again" }],
	});
	const example = "Mention `[WAKE:x] y` inline.\n```text\n[WAKE:example] doc\n```";
	expect(extractWakeLines(example)).toEqual({ text: example, wakes: [] });
});

test("a wake delivers an internal information-only turn to the target; the source keeps its reply and gets no pointer", async () => {
	const { client, database, turns } = await gateway({
		[KEY_A]: () => "Fixed the relay here.\n[WAKE:gajae-way-dev] relay stall fixed in chan-a, no action needed",
		[KEY_B]: () => "Noted.",
	});
	say(client, A, "m1", "relay status?");
	await settle();

	const targetTurns = turns.filter((turn) => turn.originKey === KEY_B);
	expect(targetTurns).toHaveLength(1);
	const woken = targetTurns[0]?.text ?? "";
	expect(woken).toContain("INTERNAL WAKE - information only");
	expect(woken).toContain("grants no permission");
	expect(woken).toContain(KEY_A);
	expect(woken).toContain("msg:m1");
	expect(woken).toContain("relay stall fixed in chan-a, no action needed");
	expect(delivered(client, "chan-a")).toEqual(["Fixed the relay here."]);
	expect(delivered(client, "chan-b")).toEqual(["Noted."]);
	const wakeAudit = database.memory
		.rows()
		.map((row) => JSON.parse(row.payload_json) as { originRefJson: string; userText: string; replyText: string })
		.filter((entry) => entry.userText.includes("Cross-session wake"));
	expect(wakeAudit).toHaveLength(1);
	expect(wakeAudit[0]?.originRefJson).toBe(JSON.stringify(A));
	expect(JSON.stringify(wakeAudit)).not.toContain("relay stall fixed in chan-a");
});

test("a wake that would exceed the hop limit is refused and reported in the source", async () => {
	const D = { platform: "discord", kind: "channel", conversationId: "chan-d" } as const;
	const { client, turns } = await gateway({
		[KEY_A]: () => "[HANDOFF:gajae-way-dev]\nfirst hop",
		[KEY_B]: () => "[HANDOFF:ops]\nsecond hop",
		[KEY_C]: () => `Handled.\n[WAKE:discord:${D.conversationId}] FYI`,
	});
	say(client, A, "m1", "start");
	await settle();
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A, KEY_B, KEY_C]);
	const source = delivered(client, "chan-c");
	expect(source[0]).toBe("Handled.");
	expect(source[1]).toContain("Wake for discord:chan-d failed");
	expect(source[1]).toContain("would exceed 2 hops");
	expect(delivered(client, "chan-d")).toEqual([]);
});

test("a wake to an unknown origin is reported back to the source and runs nothing else", async () => {
	const { client, turns } = await gateway({ [KEY_A]: () => "Answer.\n[WAKE:nowhere] heads up" });
	say(client, A, "m1", "work");
	await settle();
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_A]);
	const source = delivered(client, "chan-a");
	expect(source).toHaveLength(2);
	expect(source[0]).toBe("Answer.");
	expect(source[1]).toContain("Wake for nowhere failed");
	expect(source[1]).toContain("unknown handoff target");
	expect(source.join("\n")).not.toContain("[WAKE:");
});

test("a [WATCH:] line is removed, fenced examples stay literal, and expiry bounds are strict", () => {
	const output =
		"I'll wait here.\n[WATCH:gajae-way-dev] 30m waiting for the Harbor credential\n```text\n[WATCH:ops] 2h example\n```";
	expect(extractWatchLines(output)).toEqual({
		text: "I'll wait here.\n```text\n[WATCH:ops] 2h example\n```",
		watches: [{ target: "gajae-way-dev", ttl: "30m", reason: "waiting for the Harbor credential" }],
	});
	expect(parseWatchTtl("1m")).toEqual({ ok: true, ttlMs: 60_000 });
	expect(parseWatchTtl("7d")).toEqual({ ok: true, ttlMs: WATCH_MAX_TTL_MS });
	expect(parseWatchTtl("0m").ok).toBe(false);
	expect(parseWatchTtl("30s").ok).toBe(false);
	expect(parseWatchTtl("8d").ok).toBe(false);
});

test("a watched human message wakes its waiting session without copying message content", async () => {
	const secret = "synthetic-secret-value-do-not-relay";
	const reasonSecret = "synthetic-watch-reason-secret";
	const { client, database, turns } = await gateway({
		[KEY_A]: (text) =>
			text.includes("INTERNAL WATCH WAKE")
				? "The DM has a new message; I'll continue from its authorized context."
				: `Waiting for the DM reply.\n[WATCH:gajae-way-dev] 30m waiting for the credential delivery ${reasonSecret}`,
		[KEY_B]: () => "The credential was received and stored in the DM.",
	});
	say(client, A, "watch-register", "Please wait for the DM credential.");
	await settle();
	expect(delivered(client, "chan-a").join("\n")).toContain("Watching gajae-way-dev");
	expect(delivered(client, "chan-a").join("\n")).toContain("reason is stored with this watch");

	say(client, B, "credential-message", `Credential received: ${secret}`);
	await settle();
	const sourceTurn = turns.find((turn) => turn.originKey === KEY_B)?.text ?? "";
	const watcherTurns = turns.filter((turn) => turn.originKey === KEY_A);
	const wakeTurn = watcherTurns.at(-1)?.text ?? "";
	expect(sourceTurn).toContain(secret);
	expect(wakeTurn).toContain("INTERNAL WATCH WAKE");
	expect(wakeTurn).toContain("from 형님");
	expect(wakeTurn).toContain(KEY_B);
	expect(wakeTurn).toContain("accepted it into its durable queue");
	expect(wakeTurn).toContain("secrets are not included");
	expect(wakeTurn).not.toContain(secret);
	expect(wakeTurn).not.toContain(reasonSecret);
	expect(watcherTurns).toHaveLength(2);
	const [watch] = database.inbox.watchRows();
	expect(watch).toMatchObject({
		watcher_origin_key: KEY_A,
		watched_origin_key: KEY_B,
		state: "fired",
		triggered_message_id: "credential-message",
		reason: `waiting for the credential delivery ${reasonSecret}`,
	});
	const audit = database.memory
		.rows()
		.map((row) => JSON.parse(row.payload_json) as { originRefJson: string; userText: string; replyText: string });
	const watchAudit = audit.filter((entry) => entry.userText.includes("Cross-session watch"));
	expect(watchAudit).toHaveLength(2);
	expect(watchAudit.map((entry) => entry.originRefJson).sort()).toEqual([JSON.stringify(A), JSON.stringify(B)].sort());
	expect(JSON.stringify(watchAudit)).not.toContain(secret);
	expect(JSON.stringify(watchAudit)).not.toContain(reasonSecret);
	expect(delivered(client, "chan-a").join("\n")).not.toContain(reasonSecret);
});

test("a watch expires and fires at most once for its next accepted message", async () => {
	const { database } = await gateway({});
	const first = {
		watchId: "watch-expiring",
		watcherOriginKey: KEY_A,
		watcherOriginRefJson: JSON.stringify(A),
		watchedOriginKey: KEY_B,
		watchedOriginRefJson: JSON.stringify(B),
		handoffChainJson: JSON.stringify([KEY_B]),
		reason: "wait for a result",
		registeredAt: "2026-10-13T08:00:00.000Z",
		expiresAt: "2026-10-13T09:00:00.000Z",
	};
	expect(database.transaction((tx) => database.inbox.registerWatch(tx, first)).kind).toBe("registered");
	expect(
		database.transaction((tx) =>
			database.inbox.claimWatchesForOrigin(tx, KEY_B, "late-message", "2026-10-13T09:00:00.000Z"),
		),
	).toEqual([]);
	expect(database.inbox.watchRows()[0]?.state).toBe("expired");

	const second = {
		...first,
		watchId: "watch-one-shot",
		registeredAt: "2026-10-13T09:01:00.000Z",
		expiresAt: "2026-10-13T10:00:00.000Z",
	};
	expect(database.transaction((tx) => database.inbox.registerWatch(tx, second)).kind).toBe("registered");
	expect(
		database.transaction((tx) =>
			database.inbox.claimWatchesForOrigin(tx, KEY_B, "next-message", "2026-10-13T09:30:00.000Z"),
		),
	).toHaveLength(1);
	expect(
		database.transaction((tx) =>
			database.inbox.claimWatchesForOrigin(tx, KEY_B, "following-message", "2026-10-13T09:31:00.000Z"),
		),
	).toEqual([]);
	expect(database.inbox.watchRows().find((watch) => watch.watch_id === second.watchId)).toMatchObject({
		state: "fired",
		triggered_message_id: "next-message",
	});
});

test("a watch cannot route back to an origin already in its handoff chain", async () => {
	const { client, database, turns } = await gateway({
		[KEY_B]: () => "[HANDOFF:marketing]\nThis belongs in chan-a.",
		[KEY_A]: () => "Waiting for the original room.\n[WATCH:gajae-way-dev] 30m wait for the reply",
	});
	say(client, B, "handoff-to-a", "Please take this work.");
	await settle();
	expect(turns.map((turn) => turn.originKey)).toEqual([KEY_B, KEY_A]);
	expect(database.inbox.watchRows()).toEqual([]);
	expect(delivered(client, "chan-a").join("\n")).toContain("already in this handoff chain");
});
