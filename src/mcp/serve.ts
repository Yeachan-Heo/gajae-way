import { loadWayProfile, type WayProfile } from "../profile";
import type { JsonRpcClient } from "../rpc-client";
import { RpcClient } from "../rpc-client";

export const MCP_PROTOCOL_VERSION = "2024-11-05";

export interface McpTool {
	readonly name: string;
	readonly description: string;
	readonly inputSchema: Record<string, unknown>;
	/** Backing gateway RPC method. */
	readonly method: string;
	/** Maps tool arguments to RPC params. */
	readonly params: (input: Record<string, unknown>) => Record<string, unknown>;
}

/**
 * Read tools. Every one is a projection of a gateway RPC the operator UID can
 * already call directly, so this surface grants no new authority.
 */
export const MCP_READ_TOOLS: readonly McpTool[] = [
	{
		name: "way_status",
		description: "Current gateway status: health, turn state, lock, scheduler, and raised alerts.",
		inputSchema: { type: "object", properties: {}, additionalProperties: false },
		method: "way.status",
		params: () => ({}),
	},
	{
		name: "journal_read",
		description: "Read durable journal events from a cursor. Does not move any consumer checkpoint.",
		inputSchema: {
			type: "object",
			properties: {
				cursor: { type: "string", description: 'Journal cursor, e.g. "1:0".' },
				limit: { type: "integer", minimum: 1, maximum: 200 },
			},
			additionalProperties: false,
		},
		method: "main.events.read",
		// A bare cursor and NEVER a consumer_id: passing one would advance a real
		// adapter's durable checkpoint and silently drop that adapter's traffic.
		params: (input) => ({
			...(typeof input.cursor === "string" ? { cursor: input.cursor } : {}),
			limit: clampLimit(input.limit, 50),
		}),
	},
	{
		name: "transcript_read",
		description: "Read the main session transcript tail. Does not move any consumer checkpoint.",
		inputSchema: {
			type: "object",
			properties: {
				cursor: { type: "string" },
				limit: { type: "integer", minimum: 1, maximum: 200 },
			},
			additionalProperties: false,
		},
		method: "main.events.read",
		params: (input) => ({
			...(typeof input.cursor === "string" ? { cursor: input.cursor } : {}),
			limit: clampLimit(input.limit, 50),
			kinds: ["assistant_message", "turn_start", "turn_end"],
		}),
	},
	{
		name: "registry_list",
		description: "List durable broker session registry rows.",
		inputSchema: {
			type: "object",
			properties: { limit: { type: "integer", minimum: 1, maximum: 200 } },
			additionalProperties: false,
		},
		method: "registry.list",
		params: (input) => ({ limit: clampLimit(input.limit, 50) }),
	},
	{
		name: "schedule_list",
		description: "List durable scheduler jobs and their next fire times.",
		inputSchema: {
			type: "object",
			properties: { limit: { type: "integer", minimum: 1, maximum: 200 } },
			additionalProperties: false,
		},
		method: "schedule.list",
		params: (input) => ({ limit: clampLimit(input.limit, 50) }),
	},
];

/**
 * The single write tool.
 *
 * It traverses `main.submit` with a digest-bound owner surface, the same
 * admission path, and the same peer-credential gate, so it grants no authority
 * the operator UID lacks. One gate, not two.
 */
export const MCP_WRITE_TOOL: McpTool = {
	name: "main_submit",
	description: "Submit operator text to the main session through the durable admission path.",
	inputSchema: {
		type: "object",
		properties: {
			text: { type: "string", minLength: 1 },
			surface_id: { type: "string" },
			idempotency_key: { type: "string" },
		},
		required: ["text", "surface_id", "idempotency_key"],
		additionalProperties: false,
	},
	method: "main.submit",
	params: (input) => ({
		text: input.text,
		surface_id: input.surface_id,
		idempotency_key: input.idempotency_key,
	}),
};

export function mcpWriteEnabled(profile: WayProfile): boolean {
	const mcp = (profile.tunables.tunables as Record<string, unknown> | undefined)?.mcp as
		| Record<string, unknown>
		| undefined;
	// Only an explicit `true` enables the write tool.
	return mcp?.write_enabled === true;
}

/**
 * An unavailable tool must not be advertised. Returning `main_submit` and then
 * erroring would tell a client a capability exists that it can never use.
 */
export function mcpTools(writeEnabled: boolean): readonly McpTool[] {
	return writeEnabled ? [...MCP_READ_TOOLS, MCP_WRITE_TOOL] : MCP_READ_TOOLS;
}

export interface McpRequest {
	readonly jsonrpc: "2.0";
	readonly id?: string | number | null;
	readonly method: string;
	readonly params?: unknown;
}

export interface McpDispatchOptions {
	readonly rpc: JsonRpcClient;
	readonly writeEnabled: boolean;
	readonly serverVersion: string;
}

/** Handles one MCP request. Returns undefined for a notification. */
export async function dispatchMcp(request: McpRequest, options: McpDispatchOptions): Promise<unknown | undefined> {
	const id = request.id ?? null;
	switch (request.method) {
		case "initialize":
			return reply(id, {
				protocolVersion: MCP_PROTOCOL_VERSION,
				capabilities: { tools: {} },
				serverInfo: { name: "gajaeway", version: options.serverVersion },
			});
		case "notifications/initialized":
			return undefined;
		case "tools/list":
			return reply(id, {
				tools: mcpTools(options.writeEnabled).map((tool) => ({
					name: tool.name,
					description: tool.description,
					inputSchema: tool.inputSchema,
				})),
			});
		case "tools/call":
			return await callTool(id, request.params, options);
		case "ping":
			return reply(id, {});
		default:
			return errorReply(id, -32601, `method not found: ${request.method}`);
	}
}

async function callTool(id: string | number | null, rawParams: unknown, options: McpDispatchOptions): Promise<unknown> {
	const params = isRecord(rawParams) ? rawParams : {};
	const name = typeof params.name === "string" ? params.name : "";
	const tool = mcpTools(options.writeEnabled).find((candidate) => candidate.name === name);
	if (!tool) return errorReply(id, -32602, `unknown tool: ${name || "(missing)"}`);
	const input = isRecord(params.arguments) ? params.arguments : {};
	for (const key of Object.keys(input)) {
		const schema = tool.inputSchema.properties as Record<string, unknown> | undefined;
		if (!schema || !(key in schema)) return errorReply(id, -32602, `unsupported argument for ${name}: ${key}`);
	}

	const response = await options.rpc.request(tool.method, tool.params(input));
	if (response.error) {
		// Fail-closed and every other refusal is INHERITED from the backing
		// method and surfaced as a tool error carrying the daemon's own reason.
		// Inventing a second gate here would let the two drift.
		const detail = isRecord(response.error.data) ? response.error.data : undefined;
		const reason = typeof detail?.reason === "string" ? `: ${detail.reason}` : "";
		return reply(id, {
			isError: true,
			content: [{ type: "text", text: `${tool.name} refused (${response.error.code})${reason}` }],
		});
	}
	return reply(id, {
		isError: false,
		content: [{ type: "text", text: JSON.stringify(response.result ?? null) }],
	});
}

function reply(id: string | number | null, result: unknown): Record<string, unknown> {
	return { jsonrpc: "2.0", id, result };
}

function errorReply(id: string | number | null, code: number, message: string): Record<string, unknown> {
	return { jsonrpc: "2.0", id, error: { code, message } };
}

function clampLimit(value: unknown, fallback: number): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value)) return fallback;
	return Math.min(Math.max(value, 1), 200);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Newline-delimited JSON framing, written by hand to keep `dependencies`
 * absent from package.json. Each line is one JSON-RPC message.
 */
export async function* readMcpLines(stream: AsyncIterable<Uint8Array>): AsyncGenerator<string> {
	const decoder = new TextDecoder();
	let buffer = "";
	for await (const chunk of stream) {
		buffer += decoder.decode(chunk, { stream: true });
		let newline = buffer.indexOf("\n");
		while (newline >= 0) {
			const line = buffer.slice(0, newline).trim();
			buffer = buffer.slice(newline + 1);
			if (line) yield line;
			newline = buffer.indexOf("\n");
		}
	}
	const tail = buffer.trim();
	if (tail) yield tail;
}

export interface RunMcpServerOptions {
	readonly stateDir: string;
	readonly profilePath: string;
	readonly input?: AsyncIterable<Uint8Array>;
	readonly write?: (line: string) => void;
	readonly rpcConnect?: (socketPath: string) => Promise<JsonRpcClient>;
	readonly profile?: WayProfile;
	readonly serverVersion?: string;
}

/** Runs `gajaeway mcp serve` over stdio against the local gateway UDS. */
export async function runMcpServer(options: RunMcpServerOptions): Promise<void> {
	const profile = options.profile ?? loadWayProfile(options.profilePath);
	const writeEnabled = mcpWriteEnabled(profile);
	const connect = options.rpcConnect ?? RpcClient.connect;
	// The UDS peer-credential check is the authority boundary; a different UID
	// fails here rather than being re-checked in this process.
	const rpc = await connect(`${options.stateDir}/rpc.sock`);
	const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
	const input = options.input ?? (process.stdin as unknown as AsyncIterable<Uint8Array>);
	try {
		for await (const line of readMcpLines(input)) {
			let request: McpRequest;
			try {
				request = JSON.parse(line) as McpRequest;
			} catch {
				write(JSON.stringify(errorReply(null, -32700, "parse error")));
				continue;
			}
			const response = await dispatchMcp(request, {
				rpc,
				writeEnabled,
				serverVersion: options.serverVersion ?? "0.1.0",
			});
			if (response !== undefined) write(JSON.stringify(response));
		}
	} finally {
		rpc.close();
	}
}
