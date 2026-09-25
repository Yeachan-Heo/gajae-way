// Issue #316: session-scoped controls and queries ride the caller's live
// `serve --stdio` relay instead of spawning `gjc sdk session raw ...`. A relay
// transport failure falls back to the CLI exactly once; a relay refusal is the
// host's answer and never reaches the CLI.
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CliResult, type CliRunner, TranscriptIncompleteError } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import {
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	type ScriptedRelayReply,
	type ScriptedRelayRequest,
	scriptedRelay,
} from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

type Mode = "answer" | "tear" | "refuse";

async function fixture(options: {
	readonly mode: Mode;
	readonly relayReply: (request: ScriptedRelayRequest) => ScriptedRelayReply;
	readonly cliReply: (args: readonly string[]) => CliResult;
}) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-relay-control-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "sdk-1", repo, originKey: "relay", epoch: 0 });
	const cli: string[][] = [];
	const run: CliRunner = async (args) => {
		cli.push([...args]);
		return options.cliReply(args);
	};
	// A torn relay never answers: the request times out (a transport failure).
	const relay = scriptedRelay((request) =>
		options.mode === "tear"
			? new Promise<ScriptedRelayReply>(() => {})
			: options.mode === "refuse"
				? { ok: false, error: { code: "invalid_params", message: "refused by host" } }
				: options.relayReply(request),
	);
	const tailRunner = new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 20 });
	const port = new BrokerSessionPort({ database, authority, cli: run, instanceId: "relay-1", tailRunner });
	const handle = await port.attachTail({ sessionId: "sdk-1", brokerGeneration: 0, repo });
	return { port, repo, relay, handle, cli };
}

const ok = (result: unknown): CliResult => ({ exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" });
const page = (value: Record<string, unknown>): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ type: "query_response", ok: true, page: value }),
	stderr: "",
});

type Case = {
	readonly name: string;
	readonly op: string;
	readonly relayReply: ScriptedRelayReply;
	readonly cliReply: CliResult;
	readonly call: (f: Awaited<ReturnType<typeof fixture>>) => Promise<unknown>;
	readonly expected: unknown;
};

const turnResult = {
	kind: "prompt",
	clientRef: "gw-out-1",
	status: "terminal_ok",
	terminalAt: 2_000,
	receiptState: "present",
	content: { version: 1, type: "text", text: "body", byteLength: 4, truncated: false },
};

const cases: readonly Case[] = [
	{
		name: "setModel (model.set)",
		op: "model.set",
		relayReply: { ok: true, result: { changed: true } },
		cliReply: ok({ changed: true }),
		call: ({ port, repo, handle }) => port.setModel({ sessionId: "sdk-1", repo, selection: "gpt-x", relay: handle }),
		expected: { changed: true },
	},
	{
		name: "setModel (model.profile.set, bare boolean receipt)",
		op: "model.profile.set",
		relayReply: { ok: true, result: false },
		cliReply: ok(false),
		call: ({ port, repo, handle }) =>
			port.setModel({ sessionId: "sdk-1", repo, selection: { preset: "heavy" }, relay: handle }),
		expected: { changed: false },
	},
	{
		name: "setServiceTier",
		op: "service_tier.set",
		relayReply: { ok: true, result: { changed: true } },
		cliReply: ok({ changed: true }),
		call: ({ port, repo, handle }) =>
			port.setServiceTier({ sessionId: "sdk-1", repo, tier: "priority", relay: handle }),
		expected: { changed: true },
	},
	{
		name: "fetchWorkerOutput",
		op: "turn.result",
		relayReply: { ok: true, result: turnResult },
		cliReply: ok(turnResult),
		call: ({ port, repo, handle }) =>
			port.fetchWorkerOutput({ sessionId: "sdk-1", repo, opRef: "gw-out-1", notBeforeMs: 1_000, relay: handle }),
		expected: expect.objectContaining({ status: "proven", text: "body" }),
	},
	{
		name: "queueEmpty",
		op: "queue.messages.list",
		relayReply: { ok: true, page: { items: [], complete: true } },
		cliReply: page({ items: [], complete: true }),
		call: ({ port, repo, handle }) => port.queueEmpty({ sessionId: "sdk-1", repo, relay: handle }),
		expected: true,
	},
	{
		name: "fetchLastAssistant",
		op: "session.last_assistant",
		relayReply: { ok: true, page: { items: ["last"], complete: true } },
		cliReply: page({ items: ["last"], complete: true }),
		call: ({ port, repo, handle }) => port.fetchLastAssistant({ sessionId: "sdk-1", repo, relay: handle }),
		expected: { text: "last", pages: 1, complete: true },
	},
	{
		name: "fetchAssistantSince",
		op: "transcript.list",
		relayReply: {
			ok: true,
			page: { items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "since" }], complete: true },
		},
		cliReply: page({
			items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "since" }],
			complete: true,
		}),
		call: ({ port, repo, handle }) =>
			port.fetchAssistantSince({ sessionId: "sdk-1", repo, notBeforeMs: 4_000, relay: handle }),
		expected: { text: "since", pages: 1, complete: true },
	},
];

for (const entry of cases) {
	test(`${entry.name}: a live relay answers and no CLI runs`, async () => {
		const f = await fixture({ mode: "answer", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		expect(await entry.call(f)).toEqual(entry.expected);
		expect(f.relay.requests.map((request) => request.operation)).toEqual([entry.op]);
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});

	test(`${entry.name}: a relay transport failure falls back to the CLI exactly once`, async () => {
		const f = await fixture({ mode: "tear", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		expect(await entry.call(f)).toEqual(entry.expected);
		expect(f.relay.requests.map((request) => request.operation)).toEqual([entry.op]);
		expect(f.cli).toHaveLength(1);
		expect(f.cli[0]).toEqual(expect.arrayContaining(["sdk", "session", "raw", "sdk-1", entry.op]));
		await f.handle.close();
	});

	test(`${entry.name}: a relay refusal is surfaced and the CLI is not called`, async () => {
		const f = await fixture({ mode: "refuse", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		const outcome = await entry.call(f).then(
			(value) => ({ value }),
			(error: unknown) => ({ error }),
		);
		if (entry.op === "turn.result") {
			// The worker-output contract reports refusals as data, never as a throw.
			expect(outcome).toEqual({ value: { status: "absent", code: "transport_error" } });
		} else if (entry.op === "queue.messages.list") {
			// A refused queue read is never proof the queue is empty.
			expect(outcome).toEqual({ value: false });
		} else {
			expect("error" in outcome && String(outcome.error)).toContain("invalid_params");
		}
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});
}

test("without a relay handle the CLI stays the transport and no relay is opened", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({ ok: true, result: { changed: true } }),
		cliReply: () => ok({ changed: true }),
	});
	const spawned = f.relay.streams.length;
	expect(await f.port.setModel({ sessionId: "sdk-1", repo: f.repo, selection: "gpt-x" })).toEqual({ changed: true });
	expect(f.relay.streams).toHaveLength(spawned);
	expect(f.relay.requests).toEqual([]);
	expect(f.cli[0]).toEqual(expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.set"]));
	await f.handle.close();
});

test("transcript.list pages over the relay with the host's continuationCursor as the top-level cursor", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: (request) =>
			request.cursor === undefined
				? {
						ok: true,
						page: {
							items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "first" }],
							complete: false,
							continuationCursor: "c-1",
						},
					}
				: {
						ok: true,
						page: { items: [{ role: "assistant", ts: new Date(6_000).toISOString(), body: "second" }], complete: true },
					},
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	expect(
		await f.port.fetchAssistantSince({ sessionId: "sdk-1", repo: f.repo, notBeforeMs: 4_000, relay: f.handle }),
	).toEqual({
		text: "second",
		pages: 2,
		complete: true,
	});
	expect(f.relay.requests.map((request) => request.cursor)).toEqual([undefined, "c-1"]);
	const written = f.relay.streams[0]!.written.filter((frame) => frame.type === "query_request");
	expect(written[1]).toMatchObject({ query: "transcript.list", input: {}, cursor: "c-1" });
	await f.handle.close();
});

test("transcript.list over the relay rejects a repeated cursor and an incomplete page without one", async () => {
	for (const second of [{ complete: false, continuationCursor: "c-1" }, { complete: false }]) {
		const f = await fixture({
			mode: "answer",
			relayReply: (request) =>
				request.cursor === undefined
					? { ok: true, page: { items: [], complete: false, continuationCursor: "c-1" } }
					: { ok: true, page: { items: [], ...second } },
			cliReply: () => {
				throw new Error("CLI must not run");
			},
		});
		const error = await f.port
			.fetchAssistantSince({ sessionId: "sdk-1", repo: f.repo, notBeforeMs: 0, relay: f.handle })
			.catch((failure: unknown) => failure);
		expect(error).toBeInstanceOf(TranscriptIncompleteError);
		expect(f.cli).toEqual([]);
		await f.handle.close();
		database?.close();
		database = undefined;
		await rm(home, { recursive: true, force: true });
		home = "";
	}
});

test("session.last_assistant pages over the relay, joining chunks until complete", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: (request) =>
			request.cursor === undefined
				? { ok: true, page: { items: ["hel"], complete: false, continuationCursor: "p-2" } }
				: { ok: true, page: { items: ["lo"], complete: true } },
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	expect(await f.port.fetchLastAssistant({ sessionId: "sdk-1", repo: f.repo, relay: f.handle })).toEqual({
		text: "hello",
		pages: 2,
		complete: true,
	});
	expect(f.relay.requests.map((request) => request.cursor)).toEqual([undefined, "p-2"]);
	await f.handle.close();
});

test("session.last_assistant over the relay rejects an incomplete page without a continuation cursor", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({ ok: true, page: { items: ["partial"], complete: false } }),
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	const error = await f.port
		.fetchLastAssistant({ sessionId: "sdk-1", repo: f.repo, relay: f.handle })
		.catch((failure: unknown) => failure);
	expect(error).toBeInstanceOf(TranscriptIncompleteError);
	expect(f.cli).toEqual([]);
	await f.handle.close();
});
