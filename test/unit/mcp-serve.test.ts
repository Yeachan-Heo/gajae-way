import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
	dispatchMcp,
	MCP_READ_TOOLS,
	MCP_WRITE_TOOL,
	type McpRequest,
	mcpTools,
	mcpWriteEnabled,
	readMcpLines,
	runMcpServer,
} from "../../src/mcp/serve";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";

class FakeGateway implements JsonRpcClient {
	readonly calls: Array<{ method: string; params: unknown }> = [];
	closed = false;
	#error: { code: number; data?: unknown } | undefined;

	constructor(error?: { code: number; data?: unknown }) {
		this.#error = error;
	}

	close(): void {
		this.closed = true;
	}

	async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
		this.calls.push({ method, params });
		if (this.#error) {
			return {
				jsonrpc: "2.0",
				id: 1,
				error: { code: this.#error.code, message: "refused", data: this.#error.data },
			} as JsonRpcResponse;
		}
		return { jsonrpc: "2.0", id: 1, result: { ok: true, method } } as JsonRpcResponse;
	}
}

function options(rpc: JsonRpcClient, writeEnabled: boolean) {
	return { rpc, writeEnabled, serverVersion: "test" };
}

async function call(request: McpRequest, rpc: JsonRpcClient, writeEnabled: boolean): Promise<Record<string, unknown>> {
	return (await dispatchMcp(request, options(rpc, writeEnabled))) as Record<string, unknown>;
}

test("initialize advertises the protocol and server identity", async () => {
	const response = await call({ jsonrpc: "2.0", id: 1, method: "initialize" }, new FakeGateway(), false);
	const result = response.result as Record<string, unknown>;
	expect(result.protocolVersion).toBe("2024-11-05");
	expect((result.serverInfo as Record<string, unknown>).name).toBe("gajaeway");
});

/**
 * An unavailable tool must be ABSENT, not advertised-then-erroring: telling a
 * client a capability exists that it can never use is worse than withholding it.
 */
test("tools/list returns exactly the five read tools when write is disabled", async () => {
	const response = await call({ jsonrpc: "2.0", id: 2, method: "tools/list" }, new FakeGateway(), false);
	const names = (response.result as { tools: Array<{ name: string }> }).tools.map((tool) => tool.name);
	expect(names).toEqual(["way_status", "journal_read", "transcript_read", "registry_list", "schedule_list"]);
	expect(names).not.toContain("main_submit");
});

test("tools/list adds main_submit only when write_enabled is true", async () => {
	const response = await call({ jsonrpc: "2.0", id: 3, method: "tools/list" }, new FakeGateway(), true);
	const names = (response.result as { tools: Array<{ name: string }> }).tools.map((tool) => tool.name);
	expect(names).toHaveLength(6);
	expect(names).toContain("main_submit");
});

test("calling main_submit while write is disabled is refused as an unknown tool", async () => {
	const gateway = new FakeGateway();
	const response = await call(
		{ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "main_submit", arguments: { text: "hi" } } },
		gateway,
		false,
	);
	expect((response.error as { code: number }).code).toBe(-32602);
	// Crucially, nothing reached the gateway.
	expect(gateway.calls).toEqual([]);
});

/**
 * Passing a consumer_id would advance a real adapter's durable checkpoint and
 * silently drop that adapter's traffic.
 */
test("journal_read sends a bare cursor and never a consumer_id", async () => {
	const gateway = new FakeGateway();
	await call(
		{ jsonrpc: "2.0", id: 5, method: "tools/call", params: { name: "journal_read", arguments: { cursor: "1:0" } } },
		gateway,
		false,
	);
	const params = gateway.calls[0]?.params as Record<string, unknown>;
	expect(gateway.calls[0]?.method).toBe("main.events.read");
	expect(params.cursor).toBe("1:0");
	expect(params).not.toHaveProperty("consumer_id");
});

test("transcript_read also never sends a consumer_id", async () => {
	const gateway = new FakeGateway();
	await call(
		{ jsonrpc: "2.0", id: 6, method: "tools/call", params: { name: "transcript_read", arguments: {} } },
		gateway,
		false,
	);
	expect(gateway.calls[0]?.params).not.toHaveProperty("consumer_id");
});

test("no read tool sends a consumer_id under any argument", async () => {
	for (const tool of MCP_READ_TOOLS) {
		const params = tool.params({ cursor: "1:0", limit: 10, consumer_id: "gajaeway-discord" });
		expect(params).not.toHaveProperty("consumer_id");
	}
});

/**
 * Tools inherit fail-closed from their backing method and surface the daemon's
 * own reason. A second gate here could drift from the first.
 */
test("a fail-closed refusal becomes isError carrying the daemon's reason", async () => {
	const gateway = new FakeGateway({ code: 1000, data: { reason: "profile_drift" } });
	const response = await call(
		{ jsonrpc: "2.0", id: 7, method: "tools/call", params: { name: "registry_list", arguments: {} } },
		gateway,
		false,
	);
	const result = response.result as { isError: boolean; content: Array<{ text: string }> };
	expect(result.isError).toBe(true);
	expect(result.content[0]?.text).toContain("1000");
	expect(result.content[0]?.text).toContain("profile_drift");
});

test("a successful call reports isError false and carries the result", async () => {
	const response = await call(
		{ jsonrpc: "2.0", id: 8, method: "tools/call", params: { name: "way_status", arguments: {} } },
		new FakeGateway(),
		false,
	);
	const result = response.result as { isError: boolean; content: Array<{ text: string }> };
	expect(result.isError).toBe(false);
	expect(result.content[0]?.text).toContain("way.status");
});

test("an unsupported argument is refused rather than forwarded", async () => {
	const gateway = new FakeGateway();
	const response = await call(
		{ jsonrpc: "2.0", id: 9, method: "tools/call", params: { name: "way_status", arguments: { consumer_id: "x" } } },
		gateway,
		false,
	);
	expect((response.error as { code: number }).code).toBe(-32602);
	expect(gateway.calls).toEqual([]);
});

test("write_enabled is read from the tunable and only an explicit true enables it", () => {
	const make = (mcp: unknown) => ({ tunables: { tunables: mcp === undefined ? {} : { mcp } } }) as never;
	expect(mcpWriteEnabled(make(undefined))).toBe(false);
	expect(mcpWriteEnabled(make({}))).toBe(false);
	expect(mcpWriteEnabled(make({ write_enabled: "true" }))).toBe(false);
	expect(mcpWriteEnabled(make({ write_enabled: true }))).toBe(true);
	expect(mcpTools(false)).toHaveLength(5);
	expect(mcpTools(true)).toHaveLength(6);
});

test("stdio framing splits newline-delimited messages and tolerates a partial chunk", async () => {
	async function* chunks(): AsyncGenerator<Uint8Array> {
		const encoder = new TextEncoder();
		yield encoder.encode('{"a":1}\n{"b":');
		yield encoder.encode('2}\n\n{"c":3}');
	}
	const lines: string[] = [];
	for await (const line of readMcpLines(chunks())) lines.push(line);
	expect(lines).toEqual(['{"a":1}', '{"b":2}', '{"c":3}']);
});

test("an end-to-end stdio session initializes, lists tools, and closes the client", async () => {
	const gateway = new FakeGateway();
	const written: string[] = [];
	async function* input(): AsyncGenerator<Uint8Array> {
		const encoder = new TextEncoder();
		yield encoder.encode(
			`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize" })}\n` +
				`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n` +
				`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list" })}\n` +
				"not json\n",
		);
	}

	await runMcpServer({
		stateDir: "/tmp/unused",
		profilePath: "/tmp/unused.toml",
		profile: { tunables: { tunables: {} } } as never,
		input: input(),
		write: (line) => written.push(line),
		rpcConnect: async () => gateway,
	});

	const parsed = written.map((line) => JSON.parse(line) as Record<string, unknown>);
	// A notification produces no reply.
	expect(parsed).toHaveLength(3);
	expect((parsed[1]?.result as { tools: unknown[] }).tools).toHaveLength(5);
	expect((parsed[2]?.error as { code: number }).code).toBe(-32700);
	expect(gateway.closed).toBe(true);
});

test("package.json declares no runtime dependencies", () => {
	const manifest = JSON.parse(
		fs.readFileSync(path.join(import.meta.dir, "..", "..", "package.json"), "utf8"),
	) as Record<string, unknown>;
	// The MCP framing is hand-written precisely to keep this true.
	expect(manifest.dependencies).toBeUndefined();
});

/**
 * The UDS peer-credential check is the ONE authority boundary. `mcp serve`
 * must not re-implement or soften it: a different UID has to fail at the
 * socket, before any tool is dispatched.
 *
 * Asserted structurally because a genuine cross-UID connection needs a second
 * OS user (the crate covers that in
 * `rpc::auth::tests::linux_second_os_user_is_rejected_by_real_so_peercred_when_enabled`,
 * gated behind GAJAEWAY_CROSS_UID_TEST). What matters here is that this module
 * introduces no second gate and no bypass.
 */
test("mcp serve inherits the UDS gate and adds no authorization of its own", () => {
	const source = fs.readFileSync(path.join(import.meta.dir, "..", "..", "src", "mcp", "serve.ts"), "utf8");

	// It connects to the gateway socket and lets the daemon authorize.
	expect(source).toContain("rpc.sock");
	// No local uid/peer-credential logic, and no way to skip the connection.
	for (const forbidden of ["getuid", "peercred", "SO_PEERCRED", "process.getuid", "allowUnauthenticated", "skipAuth"]) {
		expect(source).not.toContain(forbidden);
	}
});

test("a connection failure at the socket aborts the session before any tool runs", async () => {
	const dispatched: string[] = [];
	async function* input(): AsyncGenerator<Uint8Array> {
		yield new TextEncoder().encode(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" })}\n`);
	}

	// A refused UDS connection (what a foreign UID sees) must propagate, not
	// degrade into an unauthenticated local session.
	await expect(
		runMcpServer({
			stateDir: "/tmp/gajaeway-nonexistent",
			profilePath: "/tmp/unused.toml",
			profile: { tunables: { tunables: {} } } as never,
			input: input(),
			write: (line) => dispatched.push(line),
			rpcConnect: async () => {
				throw new Error("connect EPERM: peer credential rejected");
			},
		}),
	).rejects.toThrow(/peer credential rejected/u);

	// Nothing was served.
	expect(dispatched).toEqual([]);
});

/**
 * The write tool must require an explicit surface and idempotency key. A
 * defaulted surface would let a caller submit as the owner without saying so,
 * and a missing key would make a retried submit a second turn.
 */
test("main_submit requires text, surface_id, and idempotency_key with no extras", () => {
	const schema = MCP_WRITE_TOOL.inputSchema as {
		required: string[];
		additionalProperties: boolean;
		properties: Record<string, unknown>;
	};
	expect(schema.required.sort()).toEqual(["idempotency_key", "surface_id", "text"]);
	expect(schema.additionalProperties).toBe(false);
	expect(Object.keys(schema.properties).sort()).toEqual(["idempotency_key", "surface_id", "text"]);

	// It traverses the ordinary admission path, not a privileged one.
	expect(MCP_WRITE_TOOL.method).toBe("main.submit");
	const params = MCP_WRITE_TOOL.params({ text: "hi", surface_id: "owner-dm", idempotency_key: "k1" });
	expect(params).toEqual({ text: "hi", surface_id: "owner-dm", idempotency_key: "k1" });
});
