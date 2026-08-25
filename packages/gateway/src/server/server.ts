import { unlink } from "node:fs/promises";
import {
	CAPABILITIES,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
	type LOOPBACK_ORIGIN,
	negotiate,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	type RequestFrame,
	validateOriginRef,
} from "@gajaeway/protocol";
import type { GatewayConfig } from "../config";
import type { GjcPort } from "../orchestrator/gjc-client";
import type { GatewayDatabase } from "../store/db";

interface Connection {
	readonly decoder: FrameDecoder;
	negotiated: boolean;
	write(frame: Frame): void;
	close(): void;
}

export interface GatewayServer {
	stop(reason?: string): Promise<void>;
}

export interface GatewayServerOptions {
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	readonly gjc: GjcPort;
	readonly startedAt?: string;
	readonly onStop?: () => void | Promise<void>;
}

export async function startUnixServer(options: GatewayServerOptions): Promise<GatewayServer> {
	try {
		await unlink(options.config.socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	const connections = new Set<Connection>();
	let stopping = false;
	let listener: ReturnType<typeof Bun.listen>;
	const stop = async (reason = "shutdown requested"): Promise<void> => {
		if (stopping) return;
		stopping = true;
		for (const connection of connections)
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
		listener.stop(true);
		await options.onStop?.();
	};
	listener = Bun.listen<{ connection: Connection }>({
		unix: options.config.socketPath,
		socket: {
			open(socket) {
				const connection: Connection = {
					decoder: new FrameDecoder(),
					negotiated: false,
					write: (frame) => socket.write(encodeFrame(frame)),
					close: () => socket.end(),
				};
				socket.data = { connection };
				connections.add(connection);
			},
			data(socket, data) {
				const connection = socket.data.connection;
				try {
					for (const frame of connection.decoder.feed(Buffer.from(data).toString())) {
						void handleFrame(connection, frame, options, stop, () => stopping);
					}
				} catch (error) {
					writeError(connection, error);
				}
			},
			close(socket) {
				connections.delete(socket.data.connection);
			},
			error(_socket, error) {
				console.error(`gateway socket error: ${error.message}`);
			},
		},
	});
	return { stop };
}

/** Runs the same protocol state machine over stdin/stdout for supervised launches. */
export function startStdioServer(options: GatewayServerOptions): GatewayServer {
	const connection: Connection = {
		decoder: new FrameDecoder(),
		negotiated: false,
		write: (frame) => process.stdout.write(encodeFrame(frame)),
		close: () => process.stdin.pause(),
	};
	let stopping = false;
	const stop = async (reason = "shutdown requested"): Promise<void> => {
		if (stopping) return;
		stopping = true;
		connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
		connection.close();
		await options.onStop?.();
	};
	process.stdin.on("data", (data: Buffer) => {
		try {
			for (const frame of connection.decoder.feed(data.toString()))
				void handleFrame(connection, frame, options, stop, () => stopping);
		} catch (error) {
			writeError(connection, error);
		}
	});
	return { stop };
}

async function handleFrame(
	connection: Connection,
	frame: Frame,
	options: GatewayServerOptions,
	stop: (reason?: string) => Promise<void>,
	isStopping: () => boolean,
): Promise<void> {
	try {
		if (!connection.negotiated) {
			if (frame.type !== "hello") throw new ProtocolError("negotiation_required", "send hello before requests");
			const hello = frame.payload as HelloPayload;
			if (
				!Array.isArray(hello?.supportedVersions) ||
				!hello.supportedVersions.every((version) => typeof version === "string")
			) {
				throw new ProtocolError("malformed_frame", "hello requires supportedVersions string array");
			}
			const result = negotiate(hello);
			if (!result.ok)
				throw new ProtocolError(result.code, result.detail, {
					supportedVersions: result.supportedVersions,
					capabilities: result.capabilities,
				});
			connection.negotiated = true;
			connection.write({ v: PROFILE_VERSION, type: "negotiated", payload: result.negotiated });
			return;
		}
		if (frame.type !== "request") return;
		if (isStopping()) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
		await handleRequest(connection, frame, options, stop);
	} catch (error) {
		writeError(connection, error, frame.type === "request" ? frame.id : undefined);
	}
}

async function handleRequest(
	connection: Connection,
	request: RequestFrame,
	options: GatewayServerOptions,
	stop: (reason?: string) => Promise<void>,
): Promise<void> {
	switch (request.verb) {
		case "gateway.status":
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: {
					profileVersion: PROFILE_VERSION,
					capabilities: CAPABILITIES,
					pid: process.pid,
					startedAt: options.startedAt ?? new Date().toISOString(),
					schemaVersion: options.database.schemaVersion,
					sessions: { active: options.database.activeSessionCount },
				},
			});
			return;
		case "gateway.shutdown":
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { stopping: true } });
			await stop("shutdown requested");
			return;
		case "chat.send": {
			const params = request.params as { origin?: unknown; text?: unknown } | undefined;
			if (!params || typeof params.text !== "string" || params.text.length === 0)
				throw new ProtocolError("invalid_params", "chat.send requires non-empty text");
			let origin: ReturnType<typeof validateOriginRef>;
			try {
				origin = validateOriginRef(params.origin as typeof LOOPBACK_ORIGIN);
			} catch {
				throw new ProtocolError("invalid_params", "chat.send requires a valid loopback origin");
			}
			if (origin.platform !== "loopback" || origin.kind !== "loopback" || origin.conversationId !== "loopback")
				throw new ProtocolError("invalid_params", "only the loopback origin is available in P0");
			const turnId = crypto.randomUUID();
			const { sessionId } = await options.gjc.ensureSession(originKey(origin));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId } });
			const text = await options.gjc.sendTurn(sessionId, params.text);
			connection.write({
				v: PROFILE_VERSION,
				type: "event",
				event: "chat.message",
				id: request.id,
				payload: { turnId, origin, role: "assistant", text, final: true },
			});
			return;
		}
		default:
			throw new ProtocolError("unknown_verb", `unknown verb: ${request.verb}`);
	}
}

function writeError(connection: Connection, error: unknown, id?: string): void {
	const protocol = error instanceof ProtocolError ? error : new ProtocolError("verb_failed", "gateway request failed");
	connection.write({ v: PROFILE_VERSION, type: "error", ...(id ? { id } : {}), error: protocol.toPayload() });
}
