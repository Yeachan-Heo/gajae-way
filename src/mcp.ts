import * as path from "node:path";
import { RpcClient, rpcResult, type JsonRpcClient, type JsonRpcResponse } from "./rpc-client";
import type { WayConfig } from "./config";
import { PERSONA_SAY_MAX_TEXT_LENGTH, PERSONA_SAY_MAX_PER_WINDOW, PERSONA_SAY_WINDOW_MS, toolSurfaceSchema } from "./main-session/tools";

export const MCP_PROTOCOL_VERSION = "2024-11-05";
export const MCP_TOOL_NAMES = ["way_status", "way_surfaces", "way_turn_origin", "way_say"] as const;
export const MAX_MCP_OUTPUT_BYTES = 64 * 1024;
export type McpToolName = (typeof MCP_TOOL_NAMES)[number];

export interface McpStdioDependencies {
	readonly rpcConnect?: (socketPath: string) => Promise<JsonRpcClient>;
	readonly input?: AsyncIterable<string | Uint8Array>;
	readonly output?: { write(chunk: string): void };
	readonly errorOutput?: { write(chunk: string): void };
}

interface JsonRpcRequest {
	readonly jsonrpc?: unknown;
	readonly id?: string | number | null;
	readonly method?: unknown;
	readonly params?: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function response(id: string | number | null, result: unknown): JsonRpcResponse {
	return { jsonrpc: "2.0", id, result };
}

function errorResponse(id: string | number | null, code: number, message: string, data?: unknown): Record<string, unknown> {
	return { jsonrpc: "2.0", id, error: { code, message, ...(data === undefined ? {} : { data }) } };
}

function jsonText(value: unknown): string {
	return JSON.stringify(value);
}

function toolDefinitions(): readonly Record<string, unknown>[] {
	return [
		{
			name: "way_status",
			description: "Read gateway health, adopted-session state, journal head, lock/write mode, and reconciliation freshness.",
			inputSchema: { type: "object", properties: {}, required: [], additionalProperties: false },
		},
		{
			name: "way_surfaces",
			description: "List gateway-configured owner and known surfaces with platform and kind.",
			inputSchema: { type: "object", properties: {}, required: [], additionalProperties: false },
		},
		{
			name: "way_turn_origin",
			description: "Return the configured surface that originated the one currently busy, unambiguous admitted turn; autonomous or ambiguous turns fail honestly.",
			inputSchema: { type: "object", properties: {}, required: [], additionalProperties: false },
		},
		{
			name: "way_say",
			description: "Publish one persona-initiated, surface-attributed message without creating a new persona turn.",
			inputSchema: toolSurfaceSchema(),
			annotations: { destructiveHint: false, readOnlyHint: false, idempotentHint: true },
		},
	];
}

function textResult(value: unknown, isError = false): Record<string, unknown> {
	const text = jsonText(value);
	if (Buffer.byteLength(text, "utf8") > MAX_MCP_OUTPUT_BYTES) {
		return {
			content: [{ type: "text", text: jsonText({ error: "tool_output_too_large", max_bytes: MAX_MCP_OUTPUT_BYTES }) }],
			isError: true,
		};
	}
	return { content: [{ type: "text", text }], ...(isError ? { isError: true } : {}) };
}

function toolName(value: unknown): McpToolName | undefined {
	return typeof value === "string" && (MCP_TOOL_NAMES as readonly string[]).includes(value) ? (value as McpToolName) : undefined;
}

function logTool(errorOutput: { write(chunk: string): void }, name: string, outcome: "ok" | "error"): void {
	errorOutput.write(`gateway_tool_call tool=${name} outcome=${outcome}\n`);
}

async function callTool(rpc: JsonRpcClient, name: McpToolName, args: unknown): Promise<unknown> {
	switch (name) {
		case "way_status": {
			if (!isRecord(args) || Object.keys(args).length !== 0) throw new Error("way_status accepts no arguments.");
			const [health, status] = await Promise.all([
				rpcResult(await rpc.request("way.health", {}), "way.health"),
				rpcResult(await rpc.request("way.status", {}), "way.status"),
			]);
			return { health, status };
		}
		case "way_surfaces":
			if (!isRecord(args) || Object.keys(args).length !== 0) throw new Error("way_surfaces accepts no arguments.");
			return rpcResult(await rpc.request("main.surfaces", {}), "main.surfaces");
		case "way_turn_origin":
			if (!isRecord(args) || Object.keys(args).length !== 0) throw new Error("way_turn_origin accepts no arguments.");
			return rpcResult(await rpc.request("main.turn.origin", {}), "main.turn.origin");
		case "way_say":
			if (!isRecord(args)) throw new Error("way_say arguments must be an object.");
			return rpcResult(await rpc.request("main.say", args), "main.say");
	}
}

export async function handleMcpRequest(request: unknown, rpc: JsonRpcClient, errorOutput: { write(chunk: string): void } = process.stderr): Promise<Record<string, unknown> | undefined> {
	if (!isRecord(request) || request.jsonrpc !== "2.0" || typeof request.method !== "string") {
		return errorResponse(null, -32600, "Invalid Request");
	}
	const method = request.method;
	if (method === "notifications/initialized" || method === "notifications/cancelled") return undefined;
	if (typeof request.id !== "string" && typeof request.id !== "number" && request.id !== null) return errorResponse(null, -32600, "Invalid Request");
	const id = request.id as string | number | null;
	if (method === "ping") return response(id, {});
	if (method === "initialize") {
		return response(id, {
			protocolVersion: MCP_PROTOCOL_VERSION,
			capabilities: { tools: {} },
			serverInfo: { name: "gajaeway", version: "1" },
			instructions: "Gateway-mediated tools only. The persona has no platform credentials and way_say never creates a new turn.",
		});
	}
	if (method === "tools/list") return response(id, { tools: toolDefinitions() });
	if (method !== "tools/call") return errorResponse(id, -32601, `Method not found: ${method}`);
	const params = request.params;
	if (!isRecord(params)) return errorResponse(id, -32602, "tools/call params must be an object");
	const name = toolName(params.name);
	if (!name) return errorResponse(id, -32602, "Unknown tool");
	const args = params.arguments ?? {};
	try {
		const result = await callTool(rpc, name, args);
		logTool(errorOutput, name, "ok");
		return response(id, textResult(result));
	} catch (error) {
		logTool(errorOutput, name, "error");
		return response(id, textResult({ error: error instanceof Error ? error.message : String(error) }, true));
	}
}

async function* lines(input: AsyncIterable<string | Uint8Array>): AsyncGenerator<string> {
	let buffer = "";
	for await (const chunk of input) {
		buffer += typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
		for (;;) {
			const index = buffer.indexOf("\n");
			if (index < 0) break;
			const line = buffer.slice(0, index);
			buffer = buffer.slice(index + 1);
			if (line.trim()) yield line;
		}
	}
	if (buffer.trim()) yield buffer;
}

export async function runWayMcp(config: Pick<WayConfig, "stateDir">, dependencies: McpStdioDependencies = {}): Promise<void> {
	const input = dependencies.input ?? process.stdin;
	const output = dependencies.output ?? process.stdout;
	const errorOutput = dependencies.errorOutput ?? process.stderr;
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(path.join(config.stateDir, "rpc.sock"));
	try {
		for await (const line of lines(input)) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(line) as unknown;
			} catch {
				output.write(`${JSON.stringify(errorResponse(null, -32700, "Parse error"))}\n`);
				continue;
			}
			const result = await handleMcpRequest(parsed, rpc, errorOutput);
			if (result !== undefined) output.write(`${JSON.stringify(result)}\n`);
		}
	} finally {
		rpc.close();
	}
}

export const MCP_LIMITS = {
	maxPersonaSayTextLength: PERSONA_SAY_MAX_TEXT_LENGTH,
	personaSayWindowMs: PERSONA_SAY_WINDOW_MS,
	personaSayMaxPerWindow: PERSONA_SAY_MAX_PER_WINDOW,
	maxOutputBytes: MAX_MCP_OUTPUT_BYTES,
} as const;