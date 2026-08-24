import { expect, test } from "bun:test";
import { GatewayToolError, GatewayToolController, PERSONA_SAY_MAX_PER_WINDOW } from "../../src/main-session/tools";
import { handleMcpRequest, MCP_TOOL_NAMES, runWayMcp } from "../../src/mcp";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";

class MemoryCore {
	readonly events: Array<{ seq: string; ts: number; kind: string; payloadJson: string }> = [];
	readonly idempotency = new Map<string, { requestJson: string; responseJson: string }>();
	now = 1_000_000;

	idempotencyReplay(input: { scope: string; key: string; requestJson: string }) {
		const value = this.idempotency.get(`${input.scope}\u0000${input.key}`);
		if (!value) return { replayed: false as const };
		if (value.requestJson !== input.requestJson) throw new Error("idempotency_conflict");
		return { replayed: true as const, responseJson: value.responseJson };
	}

	idempotencyStore(input: { scope: string; key: string; requestJson: string; responseJson: string }): void {
		const key = `${input.scope}\u0000${input.key}`;
		const existing = this.idempotency.get(key);
		if (existing && existing.requestJson !== input.requestJson) throw new Error("idempotency_conflict");
		this.idempotency.set(key, { requestJson: input.requestJson, responseJson: input.responseJson });
	}

	journalAppend(kind: string, payloadJson: string): { cursor: string; seq: string } {
		const seq = String(this.events.length + 1);
		this.events.push({ seq, ts: this.now, kind, payloadJson });
		return { cursor: `1:${seq}`, seq };
	}

	/** Simulates the durable journal's retention floor (MAX_RETAINED_EVENTS). */
	retentionFloor = 0n;

	journalRead(cursor = "1:0", limit = 500) {
		const after = BigInt(cursor.split(":")[1] ?? "0");
		if (after < this.retentionFloor) {
			// Mirrors the real core: reads below the floor report a gap and a resync
			// cursor rather than returning truncated history silently.
			return { events: [], nextCursor: `1:${this.retentionFloor}`, gap: { resyncCursor: `1:${this.retentionFloor}` } };
		}
		const events = this.events.filter(event => BigInt(event.seq) > after).slice(0, limit);
		const nextCursor = events.length === 0 ? cursor : `1:${events.at(-1)?.seq}`;
		return { events, nextCursor };
	}

	journalHeadCursor(): string {
		return `1:${this.events.at(-1)?.seq ?? "0"}`;
	}
}

function rpcStub(methods: Record<string, unknown>): JsonRpcClient {
	return {
		async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
			if (!(method in methods)) throw new Error(`unexpected RPC method ${method}`);
			const value = methods[method];
			return { jsonrpc: "2.0", id: 1, result: typeof value === "function" ? await (value as (params: unknown) => unknown)(params) : value };
		},
		close() {},
	};
}

function controller(core: MemoryCore, origin: string | undefined = "owner") {
	return new GatewayToolController({
		core,
		profile: { ownerSurfaces: [{ id: "owner", platform: "test", kind: "dm", sessionKind: "main" }], knownSurfaces: [{ id: "owner", platform: "test", kind: "dm", sessionKind: "main" }, { id: "guest", platform: "test", kind: "channel", sessionKind: "conversation" }] },
		host: { get turnOriginSurfaceId() { return origin; } },
		now: () => core.now,
	});
}

test("MCP advertises a strict four-tool allowlist and refuses generic RPC names", async () => {
	const rpc = rpcStub({});
	const initialized = await handleMcpRequest({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }, rpc);
	expect(initialized).toMatchObject({ result: { capabilities: { tools: {} } } });
	const listed = await handleMcpRequest({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }, rpc);
	expect((listed?.result as { tools: Array<{ name: string }> }).tools.map(tool => tool.name)).toEqual([...MCP_TOOL_NAMES]);
	const forbidden = await handleMcpRequest({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "main.submit", arguments: {} } }, rpc);
	expect(forbidden).toMatchObject({ error: { code: -32602 } });
});


test("MCP initialized notification is accepted without an id", async () => {
	expect(await handleMcpRequest({ jsonrpc: "2.0", method: "notifications/initialized" }, rpcStub({}))).toBeUndefined();
});

test("MCP bounds oversized tool output instead of emitting unbounded state", async () => {
	const rpc = rpcStub({
		"way.health": {},
		"way.status": { state: "x".repeat(70_000) },
	});
	const result = await handleMcpRequest({ jsonrpc: "2.0", id: 5, method: "tools/call", params: { name: "way_status", arguments: {} } }, rpc);
	expect(result).toMatchObject({ result: { isError: true, content: [{ text: expect.stringContaining("tool_output_too_large") }] } });
});

test("way_status, way_surfaces, and way_turn_origin use only their narrow gateway methods", async () => {
	const calls: string[] = [];
	const rpc = rpcStub({
		"way.health": () => { calls.push("way.health"); return { status: "healthy", state: "running" }; },
		"way.status": () => { calls.push("way.status"); return { journal: { head_cursor: "1:4" }, write_mode: true }; },
		"main.surfaces": () => { calls.push("main.surfaces"); return [{ id: "guest", platform: "test", kind: "channel", sessionKind: "conversation" }]; },
		"main.turn.origin": () => { calls.push("main.turn.origin"); return { surface_id: "guest" }; },
	});
	const status = await handleMcpRequest({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "way_status", arguments: {} } }, rpc);
	expect(status).toMatchObject({ result: { content: [{ type: "text" }] } });
	const surfaces = await handleMcpRequest({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "way_surfaces", arguments: {} } }, rpc);
	expect(surfaces).toMatchObject({ result: { content: [{ text: '[{"id":"guest","platform":"test","kind":"channel","sessionKind":"conversation"}]' }] } });
	const origin = await handleMcpRequest({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "way_turn_origin", arguments: {} } }, rpc);
	expect(origin).toMatchObject({ result: { content: [{ text: '{"surface_id":"guest"}' }] } });
	expect(calls).toEqual(["way.health", "way.status", "main.surfaces", "main.turn.origin"]);
});

test("way_say is surface-bound, idempotent, and never creates a broker turn", async () => {
	const core = new MemoryCore();
	const tools = controller(core);
	const first = await tools.say({ text: "persona direct output", surface_id: "guest", idempotency_key: "say-1" });
	const retry = await tools.say({ text: "persona direct output", surface_id: "guest", idempotency_key: "say-1" });
	expect(first).toEqual(retry);
	expect(core.events).toHaveLength(1);
	const restarted = await controller(core).say({ text: "persona direct output", surface_id: "guest", idempotency_key: "say-1" });
	expect(restarted).toEqual(first);
	expect(JSON.parse(core.events[0]?.payloadJson ?? "{}")).toMatchObject({
		finalized: true,
		origin: "persona",
		persona_initiated: true,
		surface_id: "guest",
		idempotency_key: "say-1",
	});
	await expect(tools.say({ text: "conflict", surface_id: "owner", idempotency_key: "say-1" })).rejects.toMatchObject({ code: 1500, message: "idempotency_conflict" } satisfies Partial<GatewayToolError>);
});


test("way_say refuses unknown surfaces and bounds persona output", async () => {
	const core = new MemoryCore();
	const tools = controller(core);
	await expect(tools.say({ text: "bad route", surface_id: "unknown", idempotency_key: "bad-route" })).rejects.toMatchObject({ code: 1300, message: "unknown_surface" } satisfies Partial<GatewayToolError>);
	for (let index = 0; index < PERSONA_SAY_MAX_PER_WINDOW; index += 1) {
		await tools.say({ text: `bounded-${index}`, surface_id: "guest", idempotency_key: `bounded-${index}` });
	}
	await expect(tools.say({ text: "one too many", surface_id: "guest", idempotency_key: "bounded-overflow" })).rejects.toMatchObject({ code: 1404, message: "persona_output_rate_limited" } satisfies Partial<GatewayToolError>);
});

test("way_turn_origin fails honestly for autonomous context", () => {
	const tools = controller(new MemoryCore(), "");
	expect(() => tools.turnOrigin()).toThrow(new GatewayToolError(1403, "turn_origin_unavailable"));
});

test("way_say MCP call is routed to main.say and returns a structured tool result", async () => {
	const calls: Array<{ method: string; params: unknown }> = [];
	const rpc = rpcStub({ "main.say": (params: unknown) => { calls.push({ method: "main.say", params }); return { accepted: true, origin: "persona", surface_id: "guest", event_cursor: "1:9", event_seq: "9", idempotency_key: "mcp-say" }; } });
	const result = await handleMcpRequest({ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "way_say", arguments: { text: "hello", surface_id: "guest", idempotency_key: "mcp-say" } } }, rpc);
	expect(result).toMatchObject({ result: { content: [{ text: expect.stringContaining('"origin":"persona"') }] } });
	expect(calls).toEqual([{ method: "main.say", params: { text: "hello", surface_id: "guest", idempotency_key: "mcp-say" } }]);
});

test("way mcp runs the stdio JSON-RPC server over the owner UDS client", async () => {
	const output: string[] = [];
	async function* input(): AsyncGenerator<string> {
		yield `${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} })}\n`;
		yield `${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} })}\n`;
	}
	await runWayMcp(
		{ stateDir: "/state" },
		{
			rpcConnect: async () => rpcStub({}),
			input: input(),
			output: { write(chunk: string) { output.push(chunk); } },
			errorOutput: { write() {} },
		},
	);
	expect(output).toHaveLength(2);
	expect(JSON.parse(output[1] ?? "{}")).toMatchObject({ result: { tools: expect.any(Array) } });
});

test("way_say survives a journal that has rotated past the retention floor", async () => {
	const core = new MemoryCore();
	const tools = controller(core);
	const first = await tools.say({ text: "before rotation", surface_id: "guest", idempotency_key: "rotate-1" });

	// Rotate: the journal advances far past the retained window and the floor moves
	// above sequence 0, which previously made every scan throw
	// main_say_journal_retention_gap forever.
	for (let index = 0; index < 40; index += 1) core.journalAppend("registry_change", JSON.stringify({ n: index }));
	core.retentionFloor = 30n;

	// New persona output must still work after rotation.
	const after = await tools.say({ text: "after rotation", surface_id: "guest", idempotency_key: "rotate-2" });
	expect(after).toMatchObject({ accepted: true, origin: "persona", surface_id: "guest", idempotency_key: "rotate-2" });

	// And replay of a key whose frame has rotated away must still return the
	// original response from the durable idempotency store rather than duplicating.
	const eventsBefore = core.events.length;
	const replayed = await tools.say({ text: "before rotation", surface_id: "guest", idempotency_key: "rotate-1" });
	expect(replayed).toEqual(first);
	expect(core.events).toHaveLength(eventsBefore);
});

test("way_say can target a derived thread surface that admission also accepts", async () => {
	const core = new MemoryCore();
	// The persona could be spoken TO in a thread but not answer proactively in one,
	// because way_say used an exact-match lookup while main.submit resolved derived
	// thread surfaces. Both now go through the shared routing SSOT.
	const tools = new GatewayToolController({
		core,
		profile: {
			ownerSurfaces: [{ id: "owner", platform: "test", kind: "dm", sessionKind: "main" }],
			knownSurfaces: [
				{ id: "owner", platform: "test", kind: "dm", sessionKind: "main" },
				{ id: "guest", platform: "discord", kind: "channel", sessionKind: "conversation" },
			],
		},
		host: { get turnOriginSurfaceId() { return "guest"; } },
		now: () => core.now,
	});
	const threadSurface = "guest/thread:1493635653441945762";
	const said = await tools.say({ text: "answering inside the thread", surface_id: threadSurface, idempotency_key: "thread-say" });
	expect(said).toMatchObject({ accepted: true, origin: "persona", surface_id: threadSurface });
	expect(JSON.parse(core.events[0]?.payloadJson ?? "{}")).toMatchObject({ finalized: true, surface_id: threadSurface });

	// An unconfigured parent is still refused.
	await expect(tools.say({ text: "no parent", surface_id: "nope/thread:1", idempotency_key: "bad-thread" }))
		.rejects.toMatchObject({ code: 1300, message: "unknown_surface" });
});
