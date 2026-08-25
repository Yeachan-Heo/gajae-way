import { unlink } from "node:fs/promises";
import {
	CAPABILITIES,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
	LOOPBACK_ORIGIN,
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
import { MemoryClosureQueue } from "../memory/closure";
import { initializeMemory } from "../memory/doctrine";
import { searchMemory } from "../memory/retrieve";
import { validateMemory } from "../memory/validator";
import { MonitorPropagator } from "../monitors/propagate";
import { MonitorRegistry } from "../monitors/registry";
import { MonitorRuntime } from "../monitors/runtime";
import { backupDatabase, integrityDatabase } from "../ops/backup";
import type { GjcPort } from "../orchestrator/gjc-client";
import { PersonaLoader } from "../persona/persona";
import type { GatewayDatabase } from "../store/db";
import { DeliveryLedger } from "../store/ledger";
import { KeyedQueue } from "./keyed-queue";

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
	readonly memory: MemoryClosureQueue;
	readonly registry: MonitorRegistry;
	readonly monitors: MonitorPropagator;
	readonly monitorRuntime: MonitorRuntime;
	readonly reconcileTimer: ReturnType<typeof setInterval>;
	readonly turns: KeyedQueue;
}

export async function startUnixServer(options: GatewayServerOptions): Promise<GatewayServer> {
	try {
		await unlink(options.config.socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	const runtime = createRuntime(options);
	void runtime.memory.initialize();
	void runtime.monitors.reconcile();
	await runtime.monitorRuntime.start();
	let stopping = false;
	let listener: ReturnType<typeof Bun.listen>;
	const stop = async (reason = "shutdown requested") => {
		if (stopping) return;
		stopping = true;
		for (const connection of runtime.connections)
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
		listener.stop(true);
		clearInterval(runtime.reconcileTimer);
		await runtime.monitorRuntime.stop();
		await runtime.memory.initialize();
		await runtime.memory.drain();
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
	void runtime.memory.initialize();
	void runtime.monitors.reconcile();
	void runtime.monitorRuntime.start();
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
		clearInterval(runtime.reconcileTimer);
		await runtime.monitorRuntime.stop();
		connection.close();
		await runtime.memory.initialize();
		await runtime.memory.drain();
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
	const connections = new Set<Connection>();
	const delivery = new DeliveryService(new DeliveryLedger(options.database));
	const registry = new MonitorRegistry(options.database);
	const memory = new MemoryClosureQueue(options.database, options.config.home);
	const monitors = new MonitorPropagator({
		database: options.database,
		registry,
		gjc: options.gjc,
		memory,
		delivery,
		emit: (payload) => {
			for (const connection of connections)
				if (connection.negotiated)
					connection.write({ v: PROFILE_VERSION, type: "event", event: "monitor.event", payload });
		},
	});
	const monitorRuntime = new MonitorRuntime(options.config, registry, monitors);
	const reconcileTimer = setInterval(() => void monitors.reconcile(), 60_000);
	return {
		delivery,
		persona: options.persona ?? new PersonaLoader(options.config.home),
		connections,
		memory,
		registry,
		monitors,
		monitorRuntime,
		reconcileTimer,
		turns: new KeyedQueue(),
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
		// Non-protocol failures are sanitized on the wire; keep the real cause in the
		// daemon log or turn failures are undiagnosable (live P1 drill finding).
		if (!(error instanceof ProtocolError))
			console.error(
				`gateway request failed${frame.type === "request" ? ` (${frame.verb})` : ""}: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
			);
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
		case "ops.backup": {
			try {
				const result = await backupDatabase(
					options.database,
					options.config.dbPath,
					(request.params as { path?: unknown } | undefined)?.path,
				);
				connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result });
			} catch (error) {
				throw new ProtocolError("invalid_params", error instanceof Error ? error.message : "invalid backup path");
			}
			return;
		}
		case "ops.integrity":
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: integrityDatabase(options.database),
			});
			return;
		case "session.list": {
			const sessions = options.database.sessionRows().map((row) => ({
				origin: row.origin_ref_json ? JSON.parse(row.origin_ref_json) : LOOPBACK_ORIGIN,
				createdAt: row.created_at,
				lastActivityAt: row.last_activity_at,
				epoch: row.epoch,
			}));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { sessions } });
			return;
		}
		case "session.recall": {
			const params = (request.params ?? {}) as { query?: unknown; limit?: unknown; requestingOrigin?: unknown };
			const requesting = params.requestingOrigin
				? originKey(validateOriginRef(params.requestingOrigin as never))
				: undefined;
			const query = typeof params.query === "string" ? params.query.toLowerCase().split(/\s+/).filter(Boolean) : [];
			const limit = Math.min(10, Math.max(0, typeof params.limit === "number" ? Math.floor(params.limit) : 10));
			const rows = options.database
				.recallRows()
				.filter((r) => r.origin_key !== requesting)
				.map((r) => ({ ...r, score: query.length ? query.filter((t) => r.text.toLowerCase().includes(t)).length : 0 }));
			rows.sort((a, b) => b.score - a.score || b.at.localeCompare(a.at));
			const snippets = rows
				.slice(0, limit)
				.map((r) => ({ origin: JSON.parse(r.origin_ref_json), text: r.text.slice(0, 500), at: r.at }));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { snippets } });
			return;
		}
		case "memory.audit": {
			const root = await initializeMemory(options.config.home);
			const issues = await validateMemory(root);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { ok: issues.length === 0, issues },
			});
			return;
		}
		case "memory.search": {
			const params = (request.params ?? {}) as { query?: unknown; limit?: unknown };
			if (typeof params.query !== "string") throw new ProtocolError("invalid_params", "memory.search requires query");
			const root = await initializeMemory(options.config.home);
			const limit = typeof params.limit === "number" ? params.limit : 10;
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { hits: await searchMemory(root, params.query, limit) },
			});
			return;
		}
		case "monitor.add": {
			try {
				const monitor = runtime.registry.add(request.params as never);
				connection.write({
					v: PROFILE_VERSION,
					type: "response",
					id: request.id,
					result: { monitorId: monitor.monitorId },
				});
				void runtime.monitorRuntime.refresh();
			} catch (error) {
				throw new ProtocolError("invalid_params", error instanceof Error ? error.message : "invalid monitor");
			}
			return;
		}
		case "monitor.list":
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { monitors: runtime.registry.list() },
			});
			return;
		case "monitor.inspect": {
			const monitorId = (request.params as { monitorId?: unknown } | undefined)?.monitorId;
			if (typeof monitorId !== "string") throw new ProtocolError("invalid_params", "unknown monitorId");
			const monitor = runtime.registry.get(monitorId);
			if (!monitor) throw new ProtocolError("invalid_params", "unknown monitorId");
			const recentEvents = options.database
				.monitorEventRows(monitorId)
				.slice(0, 100)
				.map((row) => ({
					eventId: row.event_id,
					monitorId: row.monitor_id,
					eventType: row.event_type,
					firedAt: row.fired_at,
					stage: row.stage,
				}));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { monitor, recentEvents } });
			return;
		}
		case "monitor.test": {
			const params = request.params as { monitorId?: unknown; eventType?: unknown; payload?: unknown } | undefined;
			if (!params || typeof params.monitorId !== "string")
				throw new ProtocolError("invalid_params", "monitor.test requires monitorId");
			const monitor = runtime.registry.get(params.monitorId);
			if (!monitor) throw new ProtocolError("invalid_params", "unknown monitorId");
			const eventId = runtime.monitors.submit(
				params.monitorId,
				typeof params.eventType === "string" ? params.eventType : monitor.eventTypes[0]!,
				params.payload ?? {},
			);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { eventId } });
			return;
		}
		case "monitor.remove": {
			const monitorId = (request.params as { monitorId?: unknown } | undefined)?.monitorId;
			if (typeof monitorId !== "string" || !runtime.registry.remove(monitorId))
				throw new ProtocolError("invalid_params", "unknown monitorId");
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { removed: true } });
			void runtime.monitorRuntime.refresh();
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
	const userText: string = params.text;
	let origin: ReturnType<typeof validateOriginRef>;
	try {
		origin = validateOriginRef(params.origin as typeof LOOPBACK_ORIGIN);
	} catch {
		throw new ProtocolError("invalid_params", "chat.send requires a valid origin");
	}
	const key = originKey(origin);
	if (params.text === "/new" || params.text === "/reset") {
		options.database.withTransaction(() => options.database.bumpEpoch(key, JSON.stringify(origin)));
		const payload = {
			turnId: crypto.randomUUID(),
			origin,
			role: "assistant" as const,
			text: "Started a fresh session.",
			final: true,
		};
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: payload.turnId, engaged: true },
		});
		if (origin.platform === "loopback")
			connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", id: request.id, payload });
		else {
			const delivery = runtime.delivery.prepare(payload.turnId, origin, payload.text);
			if (delivery) {
				runtime.delivery.markInflight(delivery.deliveryId as string);
				for (const recipient of runtime.connections)
					if (recipient.negotiated)
						recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload: delivery });
			}
		}
		return;
	}
	if (origin.platform !== "loopback" && origin.platform !== "discord" && origin.platform !== "telegram")
		throw new ProtocolError("invalid_params", "unsupported origin platform");
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
	connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId, engaged: true } });
	// One origin == one gjc session: serialize turns per origin key so a burst of
	// inbound messages can never race two `gjc --resume` processes on one session.
	let text: string;
	try {
		text = await runtime.turns.run(key, async () => {
			const epoch = options.database.getSessionRecord(key)?.epoch ?? 0;
			const { sessionId } = await options.gjc.ensureSession(key, epoch);
			const preamble = `${await runtime.persona.systemPreamble()}\n\n${ACTION_GUARD_SYSTEM_NOTICE}`;
			return options.gjc.sendTurn(sessionId, userText, preamble);
		});
	} catch (error) {
		// Never ghost a platform conversation: a failed turn still produces a visible,
		// ledgered notice (live P1 drill finding: timeouts looked like silent ignores).
		if (nonLoopback) {
			const notice = runtime.delivery.prepare(
				turnId,
				origin,
				"[turn failed] The reply could not be produced (timeout or runtime error). Try again, or send /new to rebind this conversation.",
			);
			if (notice) {
				runtime.delivery.markInflight(notice.deliveryId as string);
				for (const recipient of runtime.connections)
					if (recipient.negotiated)
						recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload: notice });
			}
		}
		throw error;
	}
	options.database.withTransaction(() => {
		options.database.updateActivity(key, JSON.stringify(origin));
		options.database.addRecall(
			key,
			JSON.stringify(origin),
			`user: ${userText.slice(0, 500)}\nassistant: ${text.slice(0, 500)}`,
		);
	});
	if (!nonLoopback) {
		connection.write({
			v: PROFILE_VERSION,
			type: "event",
			event: "chat.message",
			id: request.id,
			payload: { turnId, origin, role: "assistant", text, final: true },
		});
		// Durable intent is persisted synchronously; closure work deliberately does not delay delivery.
		runtime.memory.enqueue({ kind: "daily_capture", originRefJson: JSON.stringify(origin), userText, replyText: text });
		return;
	}
	const payload = runtime.delivery.prepare(turnId, origin, text);
	if (!payload) return;
	runtime.delivery.markInflight(payload.deliveryId as string);
	for (const recipient of runtime.connections)
		if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
	// Durable intent is persisted synchronously; closure work deliberately does not delay delivery.
	runtime.memory.enqueue({ kind: "daily_capture", originRefJson: JSON.stringify(origin), userText, replyText: text });
}
function writeError(connection: Connection, error: unknown, id?: string): void {
	const protocol = error instanceof ProtocolError ? error : new ProtocolError("verb_failed", "gateway request failed");
	connection.write({ v: PROFILE_VERSION, type: "error", ...(id ? { id } : {}), error: protocol.toPayload() });
}
