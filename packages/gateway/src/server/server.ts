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
import { DeliveryService } from "../delivery/delivery";
import { decideEngagement } from "../engagement/policy";
import { ACTION_GUARD_SYSTEM_NOTICE } from "../guard/action-guard";
import type { GjcPort } from "../orchestrator/gjc-client";
import { PersonaLoader } from "../persona/persona";
import type { GatewayDatabase } from "../store/db";
import { DeliveryLedger } from "../store/ledger";

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
	readonly persona?: PersonaLoader;
}
interface Runtime {
	readonly delivery: DeliveryService;
	readonly persona: PersonaLoader;
	readonly connections: Set<Connection>;
}

export async function startUnixServer(options: GatewayServerOptions): Promise<GatewayServer> {
	try {
		await unlink(options.config.socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	const runtime = createRuntime(options);
	let stopping = false;
	let listener: ReturnType<typeof Bun.listen>;
	const stop = async (reason = "shutdown requested") => {
		if (stopping) return;
		stopping = true;
		for (const connection of runtime.connections)
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
				runtime.connections.add(connection);
			},
			data(socket, data) {
				const connection = socket.data.connection;
				try {
					for (const frame of connection.decoder.feed(Buffer.from(data).toString()))
						void handleFrame(connection, frame, options, runtime, stop, () => stopping);
				} catch (error) {
					writeError(connection, error);
				}
			},
			close(socket) {
				runtime.connections.delete(socket.data.connection);
			},
			error(_socket, error) {
				console.error(`gateway socket error: ${error.message}`);
			},
		},
	});
	return { stop };
}

export function startStdioServer(options: GatewayServerOptions): GatewayServer {
	const runtime = createRuntime(options);
	const connection: Connection = {
		decoder: new FrameDecoder(),
		negotiated: false,
		write: (frame) => process.stdout.write(encodeFrame(frame)),
		close: () => process.stdin.pause(),
	};
	runtime.connections.add(connection);
	let stopping = false;
	const stop = async (reason = "shutdown requested") => {
		if (stopping) return;
		stopping = true;
		connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
		connection.close();
		await options.onStop?.();
	};
	process.stdin.on("data", (data: Buffer) => {
		try {
			for (const frame of connection.decoder.feed(data.toString()))
				void handleFrame(connection, frame, options, runtime, stop, () => stopping);
		} catch (error) {
			writeError(connection, error);
		}
	});
	return { stop };
}

function createRuntime(options: GatewayServerOptions): Runtime {
	return {
		delivery: new DeliveryService(new DeliveryLedger(options.database)),
		persona: options.persona ?? new PersonaLoader(options.config.home),
		connections: new Set(),
	};
}
async function handleFrame(
	connection: Connection,
	frame: Frame,
	options: GatewayServerOptions,
	runtime: Runtime,
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
			)
				throw new ProtocolError("malformed_frame", "hello requires supportedVersions string array");
			const result = negotiate(hello);
			if (!result.ok)
				throw new ProtocolError(result.code, result.detail, {
					supportedVersions: result.supportedVersions,
					capabilities: result.capabilities,
				});
			connection.negotiated = true;
			connection.write({ v: PROFILE_VERSION, type: "negotiated", payload: result.negotiated });
			for (const payload of runtime.delivery.redeliveries())
				connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
			return;
		}
		if (frame.type === "request") {
			if (isStopping()) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
			await handleRequest(connection, frame, options, runtime, stop);
		}
	} catch (error) {
		writeError(connection, error, frame.type === "request" ? frame.id : undefined);
	}
}
async function handleRequest(
	connection: Connection,
	request: RequestFrame,
	options: GatewayServerOptions,
	runtime: Runtime,
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
					delivery: runtime.delivery.status(),
				},
			});
			return;
		case "gateway.shutdown":
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { stopping: true } });
			await stop();
			return;
		case "delivery.confirm": {
			const id = (request.params as { deliveryId?: unknown } | undefined)?.deliveryId;
			if (typeof id !== "string" || !runtime.delivery.confirm(id))
				throw new ProtocolError("invalid_params", "unknown deliveryId");
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { settled: true } });
			return;
		}
		case "delivery.fail": {
			const params = request.params as { deliveryId?: unknown; reason?: unknown; ambiguous?: unknown } | undefined;
			if (
				!params ||
				typeof params.deliveryId !== "string" ||
				typeof params.reason !== "string" ||
				(typeof params.ambiguous !== "undefined" && typeof params.ambiguous !== "boolean") ||
				!runtime.delivery.fail(params.deliveryId, params.ambiguous)
			)
				throw new ProtocolError("invalid_params", "invalid delivery failure");
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { recorded: true } });
			return;
		}
		case "chat.send":
			await sendChat(connection, request, options, runtime);
			return;
		default:
			throw new ProtocolError("unknown_verb", `unknown verb: ${request.verb}`);
	}
}
async function sendChat(
	connection: Connection,
	request: RequestFrame,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<void> {
	const params = request.params as
		| { origin?: unknown; text?: unknown; engagement?: { mentioned?: unknown; group?: unknown; authorId?: unknown } }
		| undefined;
	if (!params || typeof params.text !== "string" || !params.text)
		throw new ProtocolError("invalid_params", "chat.send requires non-empty text");
	let origin: ReturnType<typeof validateOriginRef>;
	try {
		origin = validateOriginRef(params.origin as typeof LOOPBACK_ORIGIN);
	} catch {
		throw new ProtocolError("invalid_params", "chat.send requires a valid origin");
	}
	if (origin.platform !== "loopback" && origin.platform !== "discord")
		throw new ProtocolError("invalid_params", "only loopback and discord origins are available in P1");
	const nonLoopback = origin.platform !== "loopback";
	if (
		nonLoopback &&
		(!params.engagement ||
			typeof params.engagement.mentioned !== "boolean" ||
			typeof params.engagement.group !== "boolean" ||
			typeof params.engagement.authorId !== "string")
	)
		throw new ProtocolError("invalid_params", "non-loopback chat.send requires engagement");
	const engaged = decideEngagement(origin, params.engagement as never, options.config).engaged;
	if (!engaged) {
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: null, engaged: false },
		});
		return;
	}
	const turnId = crypto.randomUUID();
	const { sessionId } = await options.gjc.ensureSession(originKey(origin));
	connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId, engaged: true } });
	const preamble = `${await runtime.persona.systemPreamble()}\n\n${ACTION_GUARD_SYSTEM_NOTICE}`;
	const text = await options.gjc.sendTurn(sessionId, params.text, preamble);
	if (!nonLoopback) {
		connection.write({
			v: PROFILE_VERSION,
			type: "event",
			event: "chat.message",
			id: request.id,
			payload: { turnId, origin, role: "assistant", text, final: true },
		});
		return;
	}
	const payload = runtime.delivery.prepare(turnId, origin, text);
	if (!payload) return;
	for (const recipient of runtime.connections)
		if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
	runtime.delivery.markInflight(payload.deliveryId as string);
}
function writeError(connection: Connection, error: unknown, id?: string): void {
	const protocol = error instanceof ProtocolError ? error : new ProtocolError("verb_failed", "gateway request failed");
	connection.write({ v: PROFILE_VERSION, type: "error", ...(id ? { id } : {}), error: protocol.toPayload() });
}
