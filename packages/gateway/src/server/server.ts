import { createHash } from "node:crypto";
import { unlink } from "node:fs/promises";
import { join } from "node:path";
import {
	CAPABILITIES,
	type ChatMessagePayload,
	type ChatProgressActivity,
	containsSilenceToken,
	describeChatPlatforms,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
	isChatPlatform,
	isPlatformMessageId,
	isSilenceToken,
	LOOPBACK_ORIGIN,
	negotiate,
	type OriginRef,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	parseReactionReply,
	platformSupportsReaction,
	REACTIONS_PER_MESSAGE_CAP,
	REACTIONS_PER_TURN_CAP,
	type ReactionRef,
	type RequestFrame,
	reactionAllowlistDescription,
	resolveReactionEmoji,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import { parseLaneJobRecord } from "@gajae-gateway/subsession";
import { type ConfigOverrides, type GatewayConfig, type ReloadResult, reloadConfig } from "../config";
import { DeliveryService } from "../delivery/delivery";
import { ReactionBudget } from "../delivery/reaction-budget";
import { type KevShadowInput, kevShadowEnabled, recordKevShadow } from "../engagement/kev-shadow";
import {
	BotAudienceTurnGuard,
	decideEngagement,
	resolveBotAudienceLimits,
	threadFollowUpEngaged,
} from "../engagement/policy";
import { isAbstentionNarration, preTurnSkip, speechGateApplies } from "../engagement/speech-gate";
import { ACTION_GUARD_SYSTEM_NOTICE } from "../guard/action-guard";
import { autolinkCorpus } from "../memory/autolink";
import { MemoryClosureQueue } from "../memory/closure";
import { initializeMemory } from "../memory/doctrine";
import { searchMemory } from "../memory/retrieve";
import { validateMemory } from "../memory/validator";
import { MonitorPropagator } from "../monitors/propagate";
import { MonitorRegistry } from "../monitors/registry";
import { MonitorRuntime } from "../monitors/runtime";
import { backupDatabase, integrityDatabase } from "../ops/backup";
import { RuntimeCycleProjector } from "../ops/cycle";
import type { GlobalGjcClient } from "../orchestrator/broker";
import { LaneGovernor } from "../orchestrator/lane-governor";
import {
	type PersonaBindHoldInput,
	type PersonaFailureInput,
	PersonaSessionManager,
	type PersonaSteerInput,
	type PersonaTailFrameInput,
	type PersonaTerminalInput,
	type PersonaTurnLifecycle,
	type PersonaTurnSettledInput,
	type PersonaTurnStartInput,
} from "../orchestrator/persona-session";
import { formatFailureNotice, sanitizeDiagnostic } from "../orchestrator/rebind";
import type { SessionPort } from "../orchestrator/session-port";
import { deterministicInterimDeliveryId, deterministicTerminalDeliveryId } from "../orchestrator/tail-runner";
import { WorkLaneManager } from "../orchestrator/work-lane";
import { buildSessionBootstrap } from "../persona/bootstrap";
import { PersonaLoader } from "../persona/persona";
import type { GatewayDatabase, InboundMessageRow, MonitorEventStage } from "../store/db";
import { DeliveryLedger, type ExpiredDeliveryRow } from "../store/ledger";
import { deriveActivity } from "./activity";
import { ATTACHMENT_SCOPE_NOTICE, redactHistoricalAttachments } from "./attachment-scope";
import { OrderedFrameWriter } from "./frame-writer";
import { applyModelCommand } from "./model-command";
import { composeSpeakerLabel, composeTurnHeader } from "./speaker";

/** Persona tail stall heartbeat; well under the 120s stallTimeoutMs so alarms land within one interval of the threshold. */
const DEFAULT_STALL_CHECK_INTERVAL_MS = 5_000;
const DEFAULT_DELIVERY_SWEEP_INTERVAL_MS = 15_000;
/** /restart: hard-exit budget after the ordered stop begins. */
const RESTART_HARD_EXIT_MS = 15_000;
/** Thread history shown to a freshly started session: everything (humans, bots, self) in the last 24h, capped. */
const RECENT_HISTORY_WINDOW_MS = 24 * 60 * 60_000;
const RECENT_HISTORY_MAX = 300;
// 16 turns is where the shadow's separation stopped improving (help on answer-me
// messages: 0.465 context-free, 0.574 at 3, 0.668 at 8, 0.729 at 16) and it costs
// ~1.4k tokens / ~520 ms on the local judge, which nothing waits for.
const KEV_SHADOW_CONTEXT_TURNS = 16;
const KEV_SHADOW_CONTEXT_WINDOW_MS = 6 * 60 * 60_000;
const RESTART_EXIT_CODE = 75;
interface Connection {
	readonly decoder: FrameDecoder;
	negotiated: boolean;
	/** Reported client identity and process generation, recorded at negotiation. */
	client?: { readonly name: string; readonly startedAt?: string; readonly connectedAt: string };
	write(frame: Frame): void;
	close(): void;
	settle(): Promise<void>;
}
export interface GatewayServer {
	stop(reason?: string): Promise<void>;
}

/**
 * Generation census for the connected stack.
 *
 * A gateway restart does not kill an adapter: it reconnects and keeps serving,
 * which is why the pending-delivery count and the schema version both look
 * healthy while replies lag a beat behind (issue #251). `staleGeneration` is
 * the signal that
 * previously required comparing process start times by hand: the client process
 * predates this gateway process, so it is still the old generation's adapter.
 */
function connectedClients(
	connections: Iterable<Connection>,
	gatewayStartedAt: string,
): readonly {
	readonly name: string;
	readonly startedAt?: string;
	readonly connectedAt: string;
	readonly staleGeneration: boolean;
}[] {
	const gatewayStart = Date.parse(gatewayStartedAt);
	const clients = [];
	for (const connection of connections) {
		if (!connection.negotiated || !connection.client) continue;
		const startedAt = connection.client.startedAt;
		const clientStart = startedAt === undefined ? Number.NaN : Date.parse(startedAt);
		clients.push({
			name: connection.client.name,
			...(startedAt === undefined ? {} : { startedAt }),
			connectedAt: connection.client.connectedAt,
			// Unknown generation is not reported as stale: a client that never sent a
			// start time is a diagnostic gap, not evidence of a mismatch.
			staleGeneration: Number.isFinite(clientStart) && Number.isFinite(gatewayStart) && clientStart < gatewayStart,
		});
	}
	return clients;
}

async function settleConnection(connection: Connection, timeoutMs: number): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	let timedOut = false;
	await Promise.race([
		connection.settle(),
		new Promise<void>((resolve) => {
			timer = setTimeout(() => {
				timedOut = true;
				resolve();
			}, timeoutMs);
		}),
	]);
	if (timer !== undefined) clearTimeout(timer);
	if (timedOut) {
		connection.close();
		await connection.settle();
	}
}

export interface GatewayServerOptions {
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	/** Production and tests both inject the sole broker-backed turn transport. */
	readonly sessionPort: SessionPort;
	readonly startedAt?: string;
	readonly onStop?: () => void | Promise<void>;
	readonly persona?: PersonaLoader;
	/** Only gateway-owned SDK commands and relays are closed after runtime work drains. */
	readonly broker?: GlobalGjcClient;
	/** Per-process CLI overrides, reapplied on every live reload so they survive it. */
	readonly overrides?: ConfigOverrides;
	/** Test seam for chat.progress throttling; production uses the 15s defaults. */
	readonly progress?: { readonly firstAfterMs?: number; readonly intervalMs?: number };
	/** Test seam: how /restart ends the process after the ordered stop (default process.exit). */
	readonly exitProcess?: (code: number) => void;
	/** Test seam for the persona tail stall heartbeat; production uses the 5s default. */
	readonly stallCheckIntervalMs?: number;
	/** Test seam for periodic delivery recovery; production sweeps every 15s. */
	readonly deliverySweepIntervalMs?: number;
	/** Mid-work speech pacing (issue #71). */
}
interface InboundContext {
	readonly turnId: string;
	readonly requestId: string;
	readonly connection: Connection;
	/**
	 * True when the platform message that started this turn was spoken.
	 *
	 * Deliberately held with the in-flight turn rather than persisted on the
	 * inbound row: it decides how the *reply* is delivered, and a delivery
	 * recovered after a restart should ship as text rather than resurrect a
	 * voice reply to a conversation that has moved on. Losing it degrades to
	 * text-only, which is the safe direction.
	 */
	readonly voice?: boolean;
}
/**
 * The ONE reload implementation, shared by the SIGHUP handler and the
 * `gateway.reloadConfig` verb: changing a single mention-allowlist entry used to
 * require a full gateway restart, and restarting is exactly what poisons session
 * keys and produced the outage this branch also fixes.
 *
 * Fail-safe by construction: `reloadConfig` validates the whole file before
 * publishing and returns the previous config on any error, so a failed reload
 * cannot leave the daemon half-applied. Every outcome is logged with the fields
 * applied and the fields refused as restart-only.
 */
async function applyConfigReload(
	runtime: Runtime,
	options: GatewayServerOptions,
	trigger: string,
): Promise<ReloadResult> {
	const result = await reloadConfig(runtime.config, options.overrides);
	if (!result.ok) {
		console.error(
			`gateway config reload (${trigger}) FAILED; keeping the previous config: ${result.diagnostics
				.map((diagnostic) => `${diagnostic.code}: ${diagnostic.message}`)
				.join("; ")}`,
		);
		return result;
	}
	runtime.config = result.config;
	runtime.personaSessions.setStallTimeoutMs(result.config.stallTimeoutMs);
	console.error(
		`gateway config reload (${trigger}) ok; applied=[${result.changed.join(",")}] restart-required=[${result.restartRequired.join(",")}] ignored=[${result.ignored.join(",")}]`,
	);
	return result;
}

interface Runtime {
	/** The live config republished by SIGHUP/reload. */
	config: GatewayConfig;
	readonly delivery: DeliveryService;
	readonly persona: PersonaLoader;
	readonly sessionPort: SessionPort;
	readonly personaSessions: PersonaSessionManager;
	/** Ordered shutdown, wired after construction; owner `/restart` uses it. */
	stop?: (reason?: string) => Promise<void>;
	readonly connections: Set<Connection>;
	readonly memory: MemoryClosureQueue;
	readonly registry: MonitorRegistry;
	readonly monitors: MonitorPropagator;
	readonly monitorRuntime: MonitorRuntime;
	readonly reconcileTimer: ReturnType<typeof setInterval>;
	readonly deliverySweepTimer: ReturnType<typeof setInterval>;
	readonly stallTimer: ReturnType<typeof setInterval>;
	readonly contextMaintenanceTimer: ReturnType<typeof setInterval>;
	readonly stopBrokerGenerationListener?: () => void;
	/** Per-turn / per-message reaction caps shared by chat.react and the reply-token path. */
	readonly reactions: ReactionBudget;
	/** Configurable consecutive-turn budget plus the always-on runaway rate limit for bot authors. */
	readonly botAudienceTurns: BotAudienceTurnGuard;
	/** Accepted-but-not-yet-dispatched inbound messages, keyed by message id. */
	readonly inbound: Map<string, InboundContext>;
	/** Every admitted request except the shutdown request itself, so stop() can quiesce all writers. */
	readonly requests: Set<Promise<void>>;
	/** Read-only runtime-cycle projection (ops.cycle); owns no writes. */
	readonly cycle: RuntimeCycleProjector;
	/** Admission cap and retirement for `work.run` lanes. */
	readonly lanes: LaneGovernor;
	readonly work: WorkLaneManager;
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
	let stopPromise: Promise<void> | undefined;
	let listener: ReturnType<typeof Bun.listen>;
	// SIGHUP is what an operator reaches for; the reload verb is the same code path
	// for the console. Registered here and removed on stop so the handler never
	// outlives the daemon it belongs to.
	const onHup = () =>
		void applyConfigReload(runtime, options, "SIGHUP").catch((error: unknown) =>
			console.error(`gateway config reload (SIGHUP) crashed: ${diagnostic(error)}`),
		);
	process.on("SIGHUP", onHup);
	// Concurrent stop() calls (shutdown verb + owner teardown) must all await the
	// SAME settling run: an early-returning duplicate let callers proceed while
	// memory closure was still writing, racing filesystem teardown (live flake).
	const stop = (reason = "shutdown requested") => {
		if (stopPromise) return stopPromise;
		stopping = true;
		stopPromise = (async () => {
			process.off("SIGHUP", onHup);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.deliverySweepTimer);
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			// Stop accepting new sockets first, but keep existing sockets alive. Then
			// quiesce every admitted producer before taking the final writer snapshot.
			listener.stop(false);
			runtime.stopBrokerGenerationListener?.();
			await runtime.work.stop();
			await Promise.all([...runtime.requests]);
			await runtime.personaSessions.drain();
			await runtime.personaSessions.stop();
			await runtime.monitorRuntime.stop();
			// Drain claimed monitor writers before broker/database teardown (#64).
			await runtime.monitors.drain();
			// The broker has no recovery policy; it is only stopped after all current
			// producers and monitor/tail-like runtime work have drained.
			await options.broker?.stop();
			for (const connection of runtime.connections)
				connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			await Promise.all([...runtime.connections].map((connection) => settleConnection(connection, 5_000)));
			listener.stop(true);
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
	};
	runtime.stop = stop;
	listener = Bun.listen<{ connection: Connection; writer: OrderedFrameWriter }>({
		unix: options.config.socketPath,
		socket: {
			open(socket) {
				const writer = new OrderedFrameWriter(
					{ write: (bytes) => socket.write(bytes), close: () => socket.end() },
					(error) => console.error(`gateway socket write failed: ${diagnostic(error)}`),
				);
				const connection: Connection = {
					decoder: new FrameDecoder(),
					negotiated: false,
					write: (frame) => writer.write(frame),
					close: () => writer.close(),
					settle: () => writer.settled(),
				};
				socket.data = { connection, writer };
				runtime.connections.add(connection);
			},
			data(socket, data) {
				const connection = socket.data.connection;
				try {
					for (const frame of connection.decoder.feed(Buffer.from(data).toString())) {
						const task = handleFrame(connection, frame, options, runtime, stop, () => stopping);
						if (frame.type === "request" && frame.verb === "gateway.shutdown") continue;
						runtime.requests.add(task);
						void task.then(
							() => runtime.requests.delete(task),
							() => runtime.requests.delete(task),
						);
					}
				} catch (error) {
					writeError(connection, error);
				}
			},
			drain(socket) {
				socket.data.writer.drain();
			},
			close(socket) {
				runtime.work.detachWaiters(socket.data.connection);
				socket.data.writer.close();
				runtime.connections.delete(socket.data.connection);
			},
			error(socket, error) {
				runtime.work.detachWaiters(socket.data.connection);
				socket.data.writer.fail(error);
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
		settle: async () => {},
	};
	runtime.connections.add(connection);
	let stopping = false;
	let stopPromise: Promise<void> | undefined;
	// The stdio daemon gets the SAME reload trigger as the Unix server: an
	// operator (or supervisor) may signal either form, and deployment.md claims
	// SIGHUP for a running gateway without qualifying the transport.
	const onHup = () =>
		void applyConfigReload(runtime, options, "SIGHUP").catch((error: unknown) =>
			console.error(`gateway config reload (SIGHUP) crashed: ${diagnostic(error)}`),
		);
	process.on("SIGHUP", onHup);
	const stop = (reason = "shutdown requested") => {
		if (stopPromise) return stopPromise;
		stopping = true;
		stopPromise = (async () => {
			process.off("SIGHUP", onHup);
			process.stdin.off("data", onData);
			process.stdin.off("end", onEnd);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.deliverySweepTimer);
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			runtime.stopBrokerGenerationListener?.();
			await runtime.work.stop();
			// Same producer quiescence as the Unix server: in-flight stdio requests are
			// tracked and awaited before persona/tail/broker teardown.
			await Promise.all([...runtime.requests]);
			await runtime.personaSessions.drain();
			await runtime.personaSessions.stop();
			await runtime.monitorRuntime.stop();
			// Drain claimed monitor writers before broker/database teardown (#64).
			await runtime.monitors.drain();
			await options.broker?.stop();
			connection.close();
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
	};
	runtime.stop = stop;
	const onEnd = () => runtime.work.detachWaiters(connection);
	const onData = (data: Buffer) => {
		try {
			for (const frame of connection.decoder.feed(data.toString())) {
				const task = handleFrame(connection, frame, options, runtime, stop, () => stopping);
				// Same guard as the Unix server: the shutdown request awaits stop(),
				// which awaits runtime.requests, so tracking it would self-await forever.
				if (frame.type === "request" && frame.verb === "gateway.shutdown") continue;
				runtime.requests.add(task);
				task.then(
					() => runtime.requests.delete(task),
					() => runtime.requests.delete(task),
				);
			}
		} catch (error) {
			writeError(connection, error);
		}
	};
	process.stdin.on("data", onData);
	process.stdin.on("end", onEnd);
	return { stop };
}

/**
 * Shutdown must never be blocked by the memory subsystem. Startup fires initialize() without
 * awaiting it, so a failure there stays latent until shutdown awaits the memoized promise and
 * throws mid-teardown. Intents are durable SQLite rows recovered on the next boot, so a failed
 * settle is logged and teardown continues.
 */
async function settleMemory(runtime: Runtime): Promise<void> {
	try {
		await runtime.memory.initialize();
		await runtime.memory.drain();
	} catch (error) {
		console.error(
			`gateway memory settle failed during shutdown; intents remain durable for next boot: ${diagnostic(error)}`,
		);
	}
}

function createRuntime(options: GatewayServerOptions): Runtime {
	const sessionPort = options.sessionPort;
	const connections = new Set<Connection>();
	const inbound = new Map<string, InboundContext>();
	const delivery = new DeliveryService(new DeliveryLedger(options.database));
	const botAudienceTurns = new BotAudienceTurnGuard(options.database);
	const registry = new MonitorRegistry(options.database);
	const memory = new MemoryClosureQueue(options.database, options.config.home);
	let runtime!: Runtime;
	const personaSessions = new PersonaSessionManager({
		database: options.database,
		port: sessionPort,
		instanceId: options.database.instanceId,
		repo: join(options.config.home, "workspace"),
		sessionModel: options.config.model,
		stallTimeoutMs: options.config.stallTimeoutMs,
		brokerGeneration: () => options.broker?.generation ?? 0,
		brokerLiveness: options.broker ? () => options.broker!.judgeLiveness() : undefined,
		onBindHold: ({ originKey, trigger, notice }: PersonaBindHoldInput) => {
			let origin: OriginRef;
			try {
				origin = validateOriginRef(JSON.parse(trigger.origin_ref_json) as OriginRef);
			} catch (error) {
				console.error(`persona bind hold delivery skipped origin=${originKey} detail=${diagnostic(error)}`);
				return;
			}
			if (origin.platform === "loopback") {
				console.error(notice);
				return;
			}
			const context = runtime.inbound.get(trigger.message_id);
			const deliveryId = deterministicBindHoldDeliveryId(originKey, trigger.message_id);
			const payload = runtime.delivery.prepare(
				context?.turnId ?? crypto.randomUUID(),
				origin,
				notice,
				undefined,
				deliveryId,
			);
			if (payload) broadcastDelivery(runtime, payload);
		},
		onTurnStart: async (input) => await createInboundTurnLifecycle(input, options, runtime),
		// A steer whose acceptance was learnt after its turn's lifecycle is gone
		// (resolved at terminal or after a restart) is finalized exactly like a
		// live one: read context, ownership released.
		heldSteerContextMessageId: (row) =>
			(JSON.parse(row.origin_ref_json) as { platform?: string }).platform === "loopback"
				? undefined
				: (editedMessageId(row.message_id) ?? row.message_id),
		onHeldSteerAccepted: ({ row }) => {
			runtime.inbound.delete(row.message_id);
		},
		onInboundDiscard: (messageIds) => {
			for (const messageId of messageIds) inbound.delete(messageId);
		},
	});
	const monitors = new MonitorPropagator({
		database: options.database,
		registry,
		sessionPort,
		memory,
		delivery,
		ownerTarget: options.config.ownerTarget,
		contextFailureRollThreshold: options.config.monitorContextFailureRollThreshold,
		model: options.config.model,
		serviceTier: options.config.serviceTier,
		repo: join(options.config.home, "workspace"),
		// AC7: the ONE production compaction seam. Native compaction runs through the
		// broker-bound SessionPort, whose authenticated control receipt is the only
		// affirmative compaction observation (logged by the TailRunner); the monitor
		// propagator never re-implements compaction locally.
		compaction: {
			run: async (sessionId) =>
				await sessionPort.runCompaction({
					sessionId,
					repo: join(options.config.home, "workspace"),
					originKey: `monitor/session/${sessionId}`,
				}),
		},
		emit: (payload) => {
			for (const connection of connections)
				if (connection.negotiated)
					connection.write({ v: PROFILE_VERSION, type: "event", event: "monitor.event", payload });
		},
		deliver: (payload) => {
			for (const connection of connections)
				if (connection.negotiated)
					connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
		},
	});
	const monitorRuntime = new MonitorRuntime(options.config, registry, monitors);
	const lanes = new LaneGovernor({
		database: options.database,
		sessionPort,
		maxLanes: options.config.work?.maxLanes,
		idleRetireMs: options.config.work?.idleRetireMs,
	});
	const work = new WorkLaneManager({
		database: options.database,
		port: sessionPort,
		lanes,
		ownerTarget: () => runtime.config.ownerTarget?.origin,
		brokerGeneration: () => options.broker?.generation ?? 0,
		deliver: (payload) => broadcastDelivery(runtime, payload),
	});
	const reconcileTimer = setInterval(() => {
		void monitors.reconcile();
		void personaSessions
			.recover()
			.catch((error: unknown) => console.error(`persona recovery sweep failed: ${diagnostic(error)}`));
		void work
			.recover()
			.then(() => lanes.sweep())
			.catch((error: unknown) => console.error(`lane recovery/sweep failed: ${diagnostic(error)}`));
	}, 60_000);
	const deliverySweepTimer = setInterval(() => {
		if (![...connections].some((connection) => connection.negotiated)) return;
		try {
			const sweep = delivery.sweep();
			for (const expired of sweep.expired) reportDeliveryExpired(runtime, expired, "age");
			for (const payload of sweep.payloads) broadcastDelivery(runtime, payload);
		} catch (error) {
			console.error(`delivery sweep failed: ${diagnostic(error)}`);
		}
	}, options.deliverySweepIntervalMs ?? DEFAULT_DELIVERY_SWEEP_INTERVAL_MS);
	// AC6: the 120s stall alarm is a running-server obligation, not only a
	// generic-request polling side effect. This heartbeat drives every persona
	// tail's threshold check; it never aborts a turn (alarm overlay only).
	const stallTimer = setInterval(() => {
		try {
			personaSessions.checkStalls();
		} catch (error) {
			console.error(`persona stall check failed: ${diagnostic(error)}`);
		}
	}, options.stallCheckIntervalMs ?? DEFAULT_STALL_CHECK_INTERVAL_MS);
	options.database.contextMaintain();
	const contextMaintenanceTimer = setInterval(
		() => {
			try {
				options.database.contextMaintain();
			} catch (error) {
				console.error(`gateway context maintenance failed: ${diagnostic(error)}`);
			}
		},
		60 * 60 * 1000,
	);
	const brokerWithGeneration = options.broker as
		| (GlobalGjcClient & { onGeneration?: GlobalGjcClient["onGeneration"] })
		| undefined;
	const stopBrokerGenerationListener =
		typeof brokerWithGeneration?.onGeneration === "function"
			? brokerWithGeneration.onGeneration((generation) => {
					void work
						.onBrokerGeneration()
						.catch((error: unknown) => console.error(`work broker-generation recovery failed: ${diagnostic(error)}`));
					void personaSessions
						.onBrokerGeneration(generation)
						.catch((error: unknown) =>
							console.error(`persona broker-generation reconciliation failed: ${diagnostic(error)}`),
						);
				})
			: undefined;
	runtime = {
		config: options.config,
		delivery,
		persona: options.persona ?? new PersonaLoader(options.config.home),
		sessionPort,
		personaSessions,
		connections,
		memory,
		registry,
		monitors,
		monitorRuntime,
		reconcileTimer,
		deliverySweepTimer,
		stallTimer,
		contextMaintenanceTimer,
		...(stopBrokerGenerationListener ? { stopBrokerGenerationListener } : {}),
		reactions: new ReactionBudget(),
		botAudienceTurns,
		cycle: new RuntimeCycleProjector(options.database, memory, { maxLanes: lanes.maxLanes }),
		lanes,
		work,
		inbound,
		requests: new Set(),
	};
	void work.recover().catch((error: unknown) => console.error(`work startup recovery failed: ${diagnostic(error)}`));
	void personaSessions
		.recover()
		.catch((error: unknown) => console.error(`persona startup recovery failed: ${diagnostic(error)}`));
	return runtime;
}
/**
 * Monitor-batch settlement (issue #29 defect 2): the delivery ledger row for a
 * monitor batch carries turn_id === batch_id. When the adapter confirms that
 * delivery, every event of the batch that has already reached `authored`
 * advances to `delivered`. Failure paths never mark `delivered`: on
 * delivery.fail the events stay `authored` (distinguishable, operator-visible)
 * while the ledger row itself records failed/ambiguous.
 */
function settleMonitorBatch(database: GatewayDatabase, deliveryId: string, stage: MonitorEventStage): void {
	const delivery = database.deliveryRows().find((row) => row.delivery_id === deliveryId);
	if (!delivery) return;
	const batchId = delivery.turn_id;
	const events = database.monitorEventRows().filter((row) => row.batch_id === batchId);
	if (!events.length) return;
	database.withTransaction(() => {
		for (const event of events) database.monitorEventSettle(event.event_id, stage as "delivered" | "authored");
	});
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
			const info = hello.clientInfo;
			connection.client = {
				name: typeof info?.name === "string" && info.name ? info.name : "unidentified",
				...(typeof info?.startedAt === "string" && info.startedAt ? { startedAt: info.startedAt } : {}),
				connectedAt: new Date().toISOString(),
			};
			connection.write({ v: PROFILE_VERSION, type: "negotiated", payload: result.negotiated });
			const sweep = runtime.delivery.sweep(Date.now(), true);
			for (const expired of sweep.expired) reportDeliveryExpired(runtime, expired, "age");
			for (const payload of sweep.payloads)
				connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
			return;
		}
		if (frame.type === "request") {
			if (isStopping()) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
			await handleRequest(connection, frame, options, runtime, stop);
		}
	} catch (error) {
		// Non-protocol failures are sanitized on the wire and in daemon logs: SDK
		// envelopes can carry provider text containing credentials.
		if (!(error instanceof ProtocolError))
			console.error(
				`gateway request failed${frame.type === "request" ? ` (${frame.verb})` : ""}: ${diagnostic(error)}`,
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
	const gatewayStartedAt = options.startedAt ?? new Date().toISOString();
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
					startedAt: gatewayStartedAt,
					schemaVersion: options.database.schemaVersion,
					sessions: { active: options.database.activeSessionCount },
					delivery: runtime.delivery.status(),
					contextDiff: options.database.contextDiagnostics(),
					engagement: {
						botAudienceDeclines: runtime.botAudienceTurns.botAudienceDeclines(),
						botAudienceRateLimited: runtime.botAudienceTurns.botAudienceRateLimited(),
					},
					clients: connectedClients(runtime.connections, gatewayStartedAt),
				},
			});
			return;
		case "gateway.shutdown":
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { stopping: true } });
			await stop();
			return;
		case "gateway.reloadConfig": {
			// Same implementation as SIGHUP; the console gets it without signals.
			const result = await applyConfigReload(runtime, options, `verb ${request.id}`);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: result.ok
					? { ok: true, changed: result.changed, restartRequired: result.restartRequired, ignored: result.ignored }
					: { ok: false, diagnostics: result.diagnostics },
			});
			return;
		}
		case "delivery.confirm": {
			const id = (request.params as { deliveryId?: unknown } | undefined)?.deliveryId;
			if (typeof id !== "string") throw new ProtocolError("invalid_params", "unknown deliveryId");
			// unknown -> invalid_params; already-terminal -> idempotent no-op ack.
			const confirmOutcome = options.database.deliveryConfirmWithSettle(id, "delivered");
			if (confirmOutcome === "unknown") throw new ProtocolError("invalid_params", "unknown deliveryId");
			// Monitor batch settlement: a confirmed delivery for a monitor batch
			// (turn_id === the events' batch_id) advances its authored events to
			// `delivered` — only AFTER the adapter confirmed (issue #29 defect 2),
			// and NEVER when the ledger row is expired: a late confirm on an expired
			// delivery must not mark monitor events delivered (round-4 blocker 3);
			// confirmation + settlement are now ONE transaction (terminal-critic
			// blocker 2), so no split-state repair window exists.
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { settled: true } });
			return;
		}
		case "delivery.fail": {
			const params = request.params as { deliveryId?: unknown; reason?: unknown; ambiguous?: unknown } | undefined;
			if (
				!params ||
				typeof params.deliveryId !== "string" ||
				typeof params.reason !== "string" ||
				(typeof params.ambiguous !== "undefined" && typeof params.ambiguous !== "boolean")
			)
				throw new ProtocolError("invalid_params", "invalid delivery failure");
			// unknown -> invalid_params; already-terminal -> idempotent no-op ack (the
			// adapter may be retrying a stale outcome).
			const failOutcome = runtime.delivery.fail(params.deliveryId, params.ambiguous);
			if (failOutcome === "unknown") throw new ProtocolError("invalid_params", "unknown deliveryId");
			if (failOutcome === "transitioned") {
				const failedRow = runtime.delivery.get(params.deliveryId);
				if (failedRow?.state === "expired")
					reportDeliveryExpired(runtime, failedRow, safeDiagnosticField(params.reason));
			}
			// A failed monitor-batch delivery stays distinguishable: its events keep
			// stage `authored` (or `batched` before authoring) so reconcile and the
			// operator projection show them as unsettled; the ledger row carries the
			// failed/ambiguous state. Never silently `delivered`. Monotonic: only a
			// transitioned fail touches still-unsettled events.
			if (failOutcome === "transitioned" && typeof params.deliveryId === "string") {
				settleMonitorBatch(options.database, params.deliveryId, "authored");
			}
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
				throw new ProtocolError("invalid_params", diagnostic(error) || "invalid backup path");
			}
			return;
		}
		case "ops.redeliver": {
			const params = request.params as { deliveryId?: unknown; since?: unknown } | undefined;
			if (!params || typeof params !== "object" || Array.isArray(params))
				throw new ProtocolError("invalid_params", "exactly one of deliveryId or since is required");
			const hasDeliveryId = Object.hasOwn(params, "deliveryId");
			const hasSince = Object.hasOwn(params, "since");
			if (hasDeliveryId === hasSince)
				throw new ProtocolError("invalid_params", "exactly one of deliveryId or since is required");
			if (hasDeliveryId && (typeof params.deliveryId !== "string" || !params.deliveryId))
				throw new ProtocolError("invalid_params", "deliveryId must be a non-empty string");
			const since = hasSince ? parseReceivedAt(params.since) : undefined;
			const result = runtime.delivery.requeue(hasDeliveryId ? (params.deliveryId as string) : undefined, since);
			if (hasDeliveryId && result.requeued.length === 0 && !runtime.delivery.get(params.deliveryId as string))
				throw new ProtocolError("invalid_params", "unknown deliveryId");
			for (const payload of result.payloads) broadcastDelivery(runtime, payload);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { requeued: result.requeued },
			});
			return;
		}
		case "work.start":
		case "work.run":
		case "work.status":
		case "work.steer": {
			const result =
				request.verb === "work.start"
					? await runtime.work.start(request.params)
					: request.verb === "work.run"
						? await runtime.work.run(request.params, connection)
						: request.verb === "work.status"
							? await runtime.work.status(request.params)
							: await runtime.work.steer(request.params);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result });
			return;
		}
		case "work.jobs": {
			// Operator projection over durable lane jobs (issue #10): survives the
			// gateway restart that would otherwise erase in-flight work knowledge.
			// Each row is re-validated against the authoritative record JSON: a
			// corrupt row is flagged as corrupt for the operator, never shown as
			// healthy and never silently dropped.
			const laneByKey = new Map(
				options.database.workLaneRows().map((lane) => [`work-${lane.origin_key.slice("work/task/".length)}`, lane]),
			);
			const jobs = options.database.laneJobRows(true).map((row) => {
				const lane = laneByKey.get(row.lane_key);
				const bound = {
					...row,
					session_id: lane?.gjc_session_id ?? "",
					last_activity_at: lane?.last_activity_at ?? null,
					...(options.database.isBrokerQuarantined("work", row.job_id)
						? { quarantined: true, reason: "broker_authority_quarantined" }
						: {}),
				};
				try {
					const record = parseLaneJobRecord(options.database.laneJobJson(row.job_id) ?? "");
					return {
						...bound,
						attempts: record.attempts.length,
						checkpoints: record.checkpoints.length,
						escalations: record.escalations.length,
					};
				} catch (error) {
					return {
						...bound,
						state: "corrupt" as const,
						corrupt: true as const,
						error: diagnostic(error),
					};
				}
			});
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { jobs },
			});
			return;
		}
		case "work.retire": {
			const params = request.params as { name?: unknown } | undefined;
			if (typeof params?.name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(params.name))
				throw new ProtocolError("invalid_params", "work.retire requires name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}");
			await runtime.work.recover();
			const outcome = await runtime.lanes.retire(params.name, "operator");
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: outcome });
			return;
		}
		case "ops.cycle":
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: runtime.cycle.project(),
			});
			return;
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
				bootstrap: bootstrapProjection(row),
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
		case "memory.autolink": {
			// Deterministic crosslink sweep: alias index from canonical filenames,
			// titles, and frontmatter aliases; first mention per file gets linked.
			const root = await initializeMemory(options.config.home);
			const report = await autolinkCorpus(root);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: report });
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
				throw new ProtocolError("invalid_params", diagnostic(error) || "invalid monitor");
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
				.monitorEventRows(monitorId, "newest", true)
				.slice(0, 100)
				.map((row) => ({
					eventId: row.event_id,
					monitorId: row.monitor_id,
					eventType: row.event_type,
					firedAt: row.fired_at,
					stage: row.stage,
					...(options.database.isBrokerQuarantined("monitor", row.event_id)
						? { quarantined: true, reason: "broker_authority_quarantined" }
						: {}),
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
		case "chat.react": {
			// React to ONE named message. The target id is mandatory: "react to the last
			// message" is unimplementable without racing whoever spoke next, so it is not
			// expressible in the params at all.
			const params = request.params as { origin?: unknown; targetMessageId?: unknown; emoji?: unknown } | undefined;
			let origin: ReturnType<typeof validateOriginRef>;
			try {
				origin = validateOriginRef(params?.origin as typeof LOOPBACK_ORIGIN);
			} catch {
				throw new ProtocolError("invalid_params", "chat.react requires a valid origin");
			}
			// Only the chat platforms have messages to react to; chat.send guards the same
			// way. A monitor origin would otherwise produce a ledger row no adapter can settle.
			if (!isChatPlatform(origin.platform))
				throw new ProtocolError("invalid_params", `chat.react requires a ${describeChatPlatforms()} origin`);
			if (typeof params?.targetMessageId !== "string" || !isPlatformMessageId(params.targetMessageId.trim()))
				throw new ProtocolError(
					"invalid_params",
					"chat.react requires targetMessageId to be a platform message id ([A-Za-z0-9._:-], 1-64 chars)",
				);
			if (typeof params.emoji !== "string") throw new ProtocolError("invalid_params", "chat.react requires an emoji");
			const resolved = resolveReactionEmoji(params.emoji);
			if (!resolved)
				throw new ProtocolError(
					"invalid_params",
					`emoji ${JSON.stringify(params.emoji)} is outside the reaction allowlist: ${reactionAllowlistDescription(origin.platform)}`,
				);
			// Allowlisted is not the same as deliverable: Telegram accepts only its own
			// reaction set, so asking for one it cannot express would be a guaranteed dead
			// delivery. Refuse it here instead of letting the persona believe it acknowledged.
			if (!platformSupportsReaction(origin.platform, resolved.name))
				throw new ProtocolError(
					"invalid_params",
					`${origin.platform} cannot react with ${resolved.unicode} (${resolved.name}); it accepts: ${reactionAllowlistDescription(origin.platform)}`,
				);
			const reaction: ReactionRef = {
				targetMessageId: params.targetMessageId.trim(),
				emoji: resolved.unicode,
				emojiName: resolved.name,
			};
			const rejection = runtime.reactions.claim({
				originKey: originKey(origin),
				targetMessageId: reaction.targetMessageId,
				emoji: reaction.emoji,
			});
			// Cap violations are reported, never silently dropped: the caller must be able
			// to tell "not sent" from "sent and invisible".
			if (rejection)
				throw new ProtocolError("invalid_params", `reaction rejected (${rejection.reason}): ${rejection.detail}`);
			const payload = runtime.delivery.prepareReaction(crypto.randomUUID(), origin, reaction);
			broadcastDelivery(runtime, payload);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { deliveryId: payload.deliveryId, emoji: reaction.emoji },
			});
			return;
		}
		case "engagement.reaction": {
			// Inbound reaction: engagement metadata, NEVER a turn. It is recorded in the
			// conversation-context ledger so the next engaged turn reads it as part of the
			// unread diff, and it deliberately never touches inboundEnqueue/drainOrigin —
			// a reaction must not wake the persona and make it speak.
			const params = request.params as
				| {
						origin?: unknown;
						targetMessageId?: unknown;
						emoji?: unknown;
						action?: unknown;
						engagement?: { authorId?: unknown; authorName?: unknown };
				  }
				| undefined;
			let origin: ReturnType<typeof validateOriginRef>;
			try {
				origin = validateOriginRef(params?.origin as typeof LOOPBACK_ORIGIN);
			} catch {
				throw new ProtocolError("invalid_params", "engagement.reaction requires a valid origin");
			}
			if (!isChatPlatform(origin.platform))
				throw new ProtocolError("invalid_params", `engagement.reaction requires a ${describeChatPlatforms()} origin`);
			if (typeof params?.targetMessageId !== "string" || !isPlatformMessageId(params.targetMessageId.trim()))
				throw new ProtocolError(
					"invalid_params",
					"engagement.reaction requires targetMessageId to be a platform message id ([A-Za-z0-9._:-], 1-64 chars)",
				);
			if (typeof params.emoji !== "string" || !params.emoji.trim())
				throw new ProtocolError("invalid_params", "engagement.reaction requires a non-empty emoji");
			if (params.action !== "add" && params.action !== "remove")
				throw new ProtocolError("invalid_params", "engagement.reaction action must be add or remove");
			if (typeof params.engagement?.authorId !== "string" || !params.engagement.authorId)
				throw new ProtocolError("invalid_params", "engagement.reaction requires engagement.authorId");
			// The reactor's emoji is untrusted text that ends up in the turn's context
			// block: bound it and strip control characters so it cannot forge extra lines
			// (a newline here would look like another context entry to the persona).
			const emoji = stripControlCharacters(params.emoji).trim().slice(0, 64);
			if (!emoji) throw new ProtocolError("invalid_params", "engagement.reaction requires a non-empty emoji");
			const targetMessageId = params.targetMessageId.trim();
			const authorId = params.engagement.authorId;
			const actor = typeof params.engagement.authorName === "string" ? params.engagement.authorName : undefined;
			// A REMOVAL means the reactor took the signal back. It is recorded as its own
			// entry instead of erasing the add, because the persona may already have read
			// the add: the honest record is "reacted, then un-reacted", not "never reacted".
			// The synthetic id ends in a random suffix, not just a timestamp: two reactions
			// in the same millisecond (a reaction storm, or an add/remove/add burst) would
			// otherwise collide on the primary key and the later ones would be dropped by
			// the ON CONFLICT DO NOTHING insert, silently losing reaction history.
			options.database.contextRecord({
				messageId: `reaction/${params.action}/${targetMessageId}/${authorId}/${emoji}/${new Date().toISOString()}/${crypto.randomUUID().slice(0, 8)}`,
				originKey: originKey(origin),
				authorId,
				...(actor ? { authorName: actor } : {}),
				body:
					params.action === "add"
						? `[reaction] reacted ${emoji} to message ${targetMessageId}`
						: `[reaction] removed their ${emoji} reaction from message ${targetMessageId}`,
			});
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { recorded: true, engaged: false },
			});
			return;
		}
		case "chat.send":
			await sendChat(connection, request, options, runtime);
			return;
		case "chat.edit":
			await editChat(connection, request, options, runtime);
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
		| {
				origin?: unknown;
				text?: unknown;
				messageId?: unknown;
				receivedAt?: unknown;
				voice?: unknown;
				engagement?: { mentioned?: unknown; group?: unknown; authorId?: unknown };
		  }
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
	const threadFollowUp = origin.kind === "thread" && threadFollowUpEngaged(origin, key, options.database);
	// `/model` is a privileged control path. Apply the same direct-message and
	// group authorisation policy as ordinary engagement before inspecting or
	// mutating the durable override.
	if (userText === "/model" || userText.startsWith("/model ")) {
		if (!commandAuthorised(origin, runtime.config, params.engagement, threadFollowUp)) {
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { turnId: null, engaged: false },
			});
			return;
		}
		const outcome = applyModelCommand(userText, key, origin, options.database, runtime.config.model);
		if (outcome.rebind) {
			const selection = outcome.rebind.kind === "set" ? outcome.rebind.selection : runtime.config.model;
			if (!selection) throw new Error("/model clear produced a rebind without a configured gateway default");
			await runtime.personaSessions.rebindModel(key, selection);
		}
		const payload = {
			turnId: crypto.randomUUID(),
			origin,
			role: "assistant" as const,
			text: outcome.text,
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
	if (params.text === "/restart") {
		// Owner-only: restarts the gateway process. The supervisor (launchd
		// KeepAlive / systemd Restart=always) brings it back; sessions are durable
		// and resume through recovery, so the persona keeps its transcript.
		const owner = ownerPeerIdOf(runtime.config);
		const authorId = (params.engagement as { authorId?: string } | undefined)?.authorId;
		if (
			!commandAuthorised(origin, runtime.config, params.engagement, threadFollowUp) ||
			owner === undefined ||
			authorId !== owner
		) {
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { turnId: null, engaged: false },
			});
			return;
		}
		const payload = {
			turnId: crypto.randomUUID(),
			origin,
			role: "assistant" as const,
			text: "Restarting the gateway; back in a few seconds.",
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
		console.error(`gateway restart requested by owner via ${key}`);
		// Let the ack leave the socket, then exit cleanly; the supervisor restarts us.
		setTimeout(() => {
			// Exit non-zero on purpose: launchd KeepAlive=true and systemd
			// Restart=on-failure only relaunch after an unsuccessful exit. A wedged
			// ordered stop still exits within the hard budget. Durable inbound and
			// session state recover on boot.
			const exit = options.exitProcess ?? ((code: number) => process.exit(code));
			void runtime.stop?.("owner /restart").then(
				() => exit(RESTART_EXIT_CODE),
				() => exit(RESTART_EXIT_CODE),
			);
			setTimeout(() => exit(RESTART_EXIT_CODE), RESTART_HARD_EXIT_MS).unref();
		}, 1_500);
		return;
	}
	if (params.text === "/new" || params.text === "/reset") {
		// Session resets are privileged control paths: an unauthorized DM must not
		// erase the caller's session merely because commands bypass normal dispatch.
		if (!commandAuthorised(origin, runtime.config, params.engagement, threadFollowUp)) {
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { turnId: null, engaged: false },
			});
			return;
		}
		await runtime.personaSessions.reset(key, JSON.stringify(origin));
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
	if (origin.platform !== "loopback" && !isChatPlatform(origin.platform))
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
	const engagementDecision = decideEngagement(origin, params.engagement as never, runtime.config, threadFollowUp);
	const authorIsBot = (params.engagement as { authorIsBot?: unknown } | undefined)?.authorIsBot === true;
	if (!authorIsBot) runtime.botAudienceTurns.recordHumanMessage(key);
	const engagement = params.engagement as
		| {
				mentioned?: boolean;
				authorId?: string;
				authorName?: unknown;
				channelLabel?: string;
				serverLabel?: string;
				replyTo?: { fromSelf?: boolean };
		  }
		| undefined;
	const botAudienceAdmission = engagementDecision.botAudienceAdmission
		? runtime.botAudienceTurns.canAdmit(key, resolveBotAudienceLimits(origin, runtime.config))
		: undefined;
	const botAudienceGuardSpent = botAudienceAdmission !== undefined && !botAudienceAdmission.admit;
	if (botAudienceAdmission !== undefined && !botAudienceAdmission.admit) {
		const addressed = authorIsBot && (engagement?.mentioned === true || threadFollowUp);
		runtime.botAudienceTurns.recordBotAudienceDecline(addressed, botAudienceAdmission.reason);
		if (addressed || botAudienceAdmission.reason === "rate_limited")
			console.error(
				`gateway bot audience admission declined origin=${key} message=${typeof params.messageId === "string" ? params.messageId : "unidentified"} reason=${botAudienceAdmission.reason} consecutive=${runtime.botAudienceTurns.consecutiveTurns(key)} window=${runtime.botAudienceTurns.windowedTurns(key)} declines=${runtime.botAudienceTurns.botAudienceDeclines()} rateLimited=${runtime.botAudienceTurns.botAudienceRateLimited()}`,
			);
	}
	const engaged = engagementDecision.engaged && !botAudienceGuardSpent;
	const inboundMessageId = typeof params.messageId === "string" && params.messageId ? params.messageId : undefined;
	const receivedAt = parseReceivedAt(params.receivedAt);
	// Declined messages are still context, never commands (protocol contract): every
	// platform message lands in the conversation-context ledger so the next engaged
	// turn reads the full unread diff since the persona's last reply.
	if (nonLoopback && inboundMessageId) {
		const engagement = params.engagement as { authorId?: string; authorName?: unknown } | undefined;
		options.database.contextRecord({
			messageId: inboundMessageId,
			originKey: key,
			authorId: typeof engagement?.authorId === "string" ? engagement.authorId : undefined,
			authorName: typeof engagement?.authorName === "string" ? engagement.authorName : undefined,
			body: userText,
			...(receivedAt ? { receivedAt } : {}),
		});
	}
	if (!engaged) {
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: null, engaged: false },
		});
		return;
	}
	// Traffic nobody aimed at the persona may be silenced in code (#260): the
	// prompt-level "[SILENT] only" rule does not hold. Owner, bots, DMs,
	// mentions, replies to the persona and its own threads always bypass.
	const speechGated = speechGateApplies({
		originKind: origin.kind,
		mentioned: engagement?.mentioned === true,
		replyToSelf: engagement?.replyTo?.fromSelf === true,
		threadFollowUp,
		authorId: engagement?.authorId,
		authorIsBot,
		ownerId: ownerPeerIdOf(runtime.config),
	});
	// The value score on top of the authority decision. Measured in shadow on
	// every engaged message; awaited, and able to skip the turn, only when the
	// speech gate is enforced for this message. No-op unless KEV_SHADOW_URL is set.
	if (kevShadowEnabled()) {
		// A DM, an @mention, or a reply to this bot is traffic aimed at it, and which of
		// the three it is changes how a short message reads.
		const addressedBy =
			origin.kind === "dm"
				? "dm"
				: engagement?.mentioned === true
					? "mention"
					: engagement?.replyTo?.fromSelf === true
						? "reply"
						: undefined;
		// Judged context-free, a short follow-up ("잘되냐 이제") reads as chatter: the
		// question it continues is not in the text. The same message with its real
		// history scores 0.24 -> 0.80. What people said, not what this bot answered:
		// its own replies are walls of text and including them cost 5 of 14 real
		// owner messages a false skip (#243).
		const earlier = nonLoopback
			? options.database
					.recentInbound(
						key,
						KEV_SHADOW_CONTEXT_TURNS,
						new Date(Date.now() - KEV_SHADOW_CONTEXT_WINDOW_MS).toISOString(),
					)
					// The message under judgement was just recorded as context above.
					.filter((entry) => entry.id === undefined || entry.id !== inboundMessageId)
					.map((entry) => ({ author: entry.author, body: entry.body, at: entry.at }))
			: [];
		const shadowInput: KevShadowInput = {
			originKey: key,
			text: userText,
			authorLabel: typeof engagement?.authorName === "string" ? engagement.authorName : undefined,
			place:
				[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") ||
				`${origin.platform} ${origin.kind}`,
			earlier,
			addressed: addressedBy !== undefined,
			...(addressedBy ? { addressedBy } : {}),
			authorIsBot,
		};
		if (!speechGated) void recordKevShadow(shadowInput);
		else if (await preTurnSkip(shadowInput)) {
			// Already recorded as context above; the next engaged turn reads it.
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { turnId: null, engaged: false },
			});
			return;
		}
	}
	const messageId = inboundMessageId ?? crypto.randomUUID();
	const turnId = crypto.randomUUID();
	// Persist before dispatch: this insert is the durable acceptance boundary. The
	// per-origin actor receives the notification only after this transaction wins.
	const accepted = options.database.inboundEnqueue({
		messageId,
		originKey: key,
		originRefJson: JSON.stringify(origin),
		body: userText,
		// The delivery-side gate needs the same bypass decision after a restart,
		// so it travels with the durable row.
		engagementJson: params.engagement
			? JSON.stringify(speechGated ? { ...params.engagement, speechGated: true } : params.engagement)
			: undefined,
	});
	if (!accepted) {
		// Duplicate message id: already accepted once, so acknowledge without dispatching.
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: null, engaged: true },
		});
		return;
	}
	if (engagementDecision.botAudienceAdmission) runtime.botAudienceTurns.recordBotAdmission(key, messageId);
	runtime.inbound.set(messageId, {
		turnId,
		requestId: request.id,
		connection,
		...(params.voice === true ? { voice: true } : {}),
	});
	connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId, engaged: true } });
	await runtime.personaSessions.notifyInbound(key);
}
/**
 * A message the user edited after the gateway ingested it. The edit is not a
 * new message: it is streamed into the same session as an update of a
 * `[MESSAGE POINTER: <id>]`, steered into the running turn or sent as the next
 * one by the same actor rule as any other inbound row. Edits of messages the
 * gateway never saw are ignored; the same edit event twice is one row.
 */
export const MESSAGE_POINTER_PREFIX = "[MESSAGE POINTER: ";

export function renderMessageEdit(messageId: string, text: string): string {
	return `${MESSAGE_POINTER_PREFIX}${messageId}] (the user edited this earlier message; this is its new content)\n${text}`;
}

/** One durable row per (message, edit content): a replayed edit event collides, a further edit does not. */
export function messageEditId(messageId: string, text: string): string {
	return `edit:${messageId}:${createHash("sha256").update(text).digest("hex").slice(0, 16)}`;
}

/** The platform message an edit row points at, or undefined for an ordinary inbound row. */
export function editedMessageId(inboundMessageId: string): string | undefined {
	const match = /^edit:(.+):[0-9a-f]{16}$/.exec(inboundMessageId);
	return match?.[1];
}

async function editChat(
	connection: Connection,
	request: RequestFrame,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<void> {
	const params = request.params as
		| {
				origin?: unknown;
				messageId?: unknown;
				text?: unknown;
				receivedAt?: unknown;
				engagement?: { mentioned?: unknown; group?: unknown; authorId?: unknown };
		  }
		| undefined;
	if (!params || typeof params.text !== "string" || !params.text)
		throw new ProtocolError("invalid_params", "chat.edit requires non-empty text");
	if (typeof params.messageId !== "string" || !params.messageId)
		throw new ProtocolError("invalid_params", "chat.edit requires the edited messageId");
	let origin: ReturnType<typeof validateOriginRef>;
	try {
		origin = validateOriginRef(params.origin as typeof LOOPBACK_ORIGIN);
	} catch {
		throw new ProtocolError("invalid_params", "chat.edit requires a valid origin");
	}
	if (origin.platform !== "loopback" && !isChatPlatform(origin.platform))
		throw new ProtocolError("invalid_params", "unsupported origin platform");
	const nonLoopback = origin.platform !== "loopback";
	if (
		nonLoopback &&
		(!params.engagement ||
			typeof params.engagement.mentioned !== "boolean" ||
			typeof params.engagement.group !== "boolean" ||
			typeof params.engagement.authorId !== "string")
	)
		throw new ProtocolError("invalid_params", "non-loopback chat.edit requires engagement");
	const key = originKey(origin);
	const declined = () =>
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: null, engaged: false },
		});
	if (!options.database.inboundKnownMessage(key, params.messageId)) {
		// Never ingested: nothing to point at. Not context either - a message the
		// gateway never saw must not appear by way of its edit.
		declined();
		return;
	}
	// The recorded body now says what the message says now.
	options.database.contextUpdateBody(key, params.messageId, params.text);
	const threadFollowUp = origin.kind === "thread" && threadFollowUpEngaged(origin, key, options.database);
	const engagementDecision = decideEngagement(origin, params.engagement as never, runtime.config, threadFollowUp);
	const authorIsBot = (params.engagement as { authorIsBot?: unknown } | undefined)?.authorIsBot === true;
	if (!authorIsBot) runtime.botAudienceTurns.recordHumanMessage(key);
	const engagement = params.engagement as { mentioned?: boolean } | undefined;
	const botAudienceAdmission = engagementDecision.botAudienceAdmission
		? runtime.botAudienceTurns.canAdmit(key, resolveBotAudienceLimits(origin, runtime.config))
		: undefined;
	const botAudienceGuardSpent = botAudienceAdmission !== undefined && !botAudienceAdmission.admit;
	if (botAudienceAdmission !== undefined && !botAudienceAdmission.admit) {
		const addressed = authorIsBot && (engagement?.mentioned === true || threadFollowUp);
		runtime.botAudienceTurns.recordBotAudienceDecline(addressed, botAudienceAdmission.reason);
		if (addressed || botAudienceAdmission.reason === "rate_limited")
			console.error(
				`gateway bot audience admission declined origin=${key} message=${params.messageId} reason=${botAudienceAdmission.reason} consecutive=${runtime.botAudienceTurns.consecutiveTurns(key)} window=${runtime.botAudienceTurns.windowedTurns(key)} declines=${runtime.botAudienceTurns.botAudienceDeclines()} rateLimited=${runtime.botAudienceTurns.botAudienceRateLimited()}`,
			);
	}
	if (!engagementDecision.engaged || botAudienceGuardSpent) {
		declined();
		return;
	}
	const messageId = messageEditId(params.messageId, params.text);
	const turnId = crypto.randomUUID();
	const accepted = options.database.inboundEnqueue({
		messageId,
		originKey: key,
		originRefJson: JSON.stringify(origin),
		body: renderMessageEdit(params.messageId, params.text),
		engagementJson: params.engagement ? JSON.stringify(params.engagement) : undefined,
		...(parseReceivedAt(params.receivedAt) ? { receivedAt: parseReceivedAt(params.receivedAt) } : {}),
	});
	if (!accepted) {
		// The same edit event delivered twice: acknowledged once, dispatched once.
		connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId: null, engaged: true } });
		return;
	}
	if (engagementDecision.botAudienceAdmission) runtime.botAudienceTurns.recordBotAdmission(key, messageId);
	runtime.inbound.set(messageId, { turnId, requestId: request.id, connection });
	connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId, engaged: true } });
	await runtime.personaSessions.notifyInbound(key);
}

async function createInboundTurnLifecycle(
	input: PersonaTurnStartInput,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<PersonaTurnLifecycle> {
	// One inbound message is one turn; it carries the live requester/voice context.
	const row = input.trigger;
	const context = runtime.inbound.get(row.message_id);
	runtime.inbound.delete(row.message_id);
	const connection = context?.connection ?? [...runtime.connections][0];
	const turnId = context?.turnId ?? crypto.randomUUID();
	const voiceTurn = context?.voice === true;
	const key = row.origin_key;
	const origin = validateOriginRef(JSON.parse(row.origin_ref_json) as typeof LOOPBACK_ORIGIN);
	const userText = row.body;
	const nonLoopback = origin.platform !== "loopback";
	const engagement = row.engagement_json
		? (JSON.parse(row.engagement_json) as {
				mentioned?: boolean;
				group?: boolean;
				authorId?: string;
				authorIsBot?: boolean;
				authorName?: string;
				authorHandle?: string;
				authorServerTag?: string;
				channelLabel?: string;
				serverLabel?: string;
				replyTo?: { messageId?: string; authorName?: string; fromSelf?: boolean; excerpt?: string };
				/** Set at intake by the speech gate (#260); absent means the reply is never judged. */
				speechGated?: boolean;
			})
		: undefined;
	const speaker = composeSpeakerLabel(engagement);
	const place =
		[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") ||
		`${origin.platform} ${origin.kind} ${origin.conversationId}`;
	const bootstrapState = options.database.getSessionBootstrap(key);
	let turnText = userText;
	let contextMessageIds: readonly string[] = [];
	let contextOmissionRevision = 0;
	if (nonLoopback) {
		const prepared = options.database.contextWindow(key, row.message_id);
		// A pointer-update turn reads its ORIGINAL message's row (the edited body
		// lives there); commit that id, not the synthetic edit row.
		contextMessageIds = [...prepared.selectedMessageIds, editedMessageId(row.message_id) ?? row.message_id];
		contextOmissionRevision = prepared.omissionRevision;
		const lines = prepared.rows.map(
			(entry) =>
				`- [${entry.received_at}] ${entry.author_name ?? "unknown"} (author:${entry.author_id ?? "?"}, msg:${entry.message_id}): ${entry.body.slice(0, 1000)}`,
		);
		const omitted = prepared.expiredCount + prepared.truncatedCount;
		const omittedRange =
			prepared.omittedOldestAt && prepared.omittedNewestAt
				? `; timestamps ${prepared.omittedOldestAt}..${prepared.omittedNewestAt}`
				: "";
		const droppedNote =
			omitted > 0
				? `[${omitted} older unread message(s) omitted: ${prepared.expiredCount} expired outside floor ${prepared.effectiveFloor}, ${prepared.truncatedCount} truncated by the newest-${prepared.rows.length} window${omittedRange}]\n`
				: "";
		// A fresh session (new epoch) also gets the recent thread it is joining,
		// not only the unread diff: without it the persona answers as if the
		// conversation had just started.
		const isFreshSession = !bootstrapState || bootstrapState.lastBootstrappedEpoch < input.epoch;
		const recent = isFreshSession
			? options.database.recentConversation(
					key,
					origin.conversationId,
					RECENT_HISTORY_MAX,
					new Date(Date.now() - RECENT_HISTORY_WINDOW_MS).toISOString(),
				)
			: [];
		const inWindowIds = new Set(prepared.selectedMessageIds);
		const recentLines = recent
			.filter((entry) => entry.id === undefined || (!inWindowIds.has(entry.id) && entry.id !== row.message_id))
			.map((entry) => `- [${entry.at}] ${entry.author}: ${redactHistoricalAttachments(entry.body).slice(0, 500)}`);
		const recentBlock = recentLines.length
			? `[Recent conversation history, last 24h (this session just started; already answered unless listed as unread below)]\n${recentLines.join("\n")}\n\n`
			: "";
		const header = `${recentBlock}${
			lines.length
				? `[Unread messages in this conversation since your last reply]\n${droppedNote}${lines.join("\n")}\n\n`
				: droppedNote
					? `${droppedNote}\n`
					: ""
		}`;
		turnText = `${header}${speaker ? `${composeTurnHeader({ speaker, place, authorId: engagement?.authorId, messageId: row.message_id, engagement })}\n` : ""}${userText}`;
	}

	const bootstrap =
		!bootstrapState || bootstrapState.lastBootstrappedEpoch < input.epoch
			? await buildSessionBootstrap({
					home: runtime.config.home,
					origin,
					epoch: input.epoch,
					engagement,
					config: runtime.config,
				})
			: undefined;
	const systemPreamble = [
		await runtime.persona.systemPreamble(),
		currentConversationNotice(origin),
		...(bootstrap ? [bootstrap.text] : []),
		ATTACHMENT_SCOPE_NOTICE,
		ACTION_GUARD_SYSTEM_NOTICE,
	].join("\n\n");
	const modelOverride = options.database.conversationModelGet(key)?.selection;
	const effectiveModel = modelOverride ?? runtime.config.model;

	const deliveredParts: string[] = [];
	let assistantDeliveryStarted = false;
	let reactionTokensSeen = false;
	const maxTurnParts = 10;
	/**
	 * Raw messages whose reaction tokens have already been claimed this turn. The
	 * terminal path re-runs over text the tail already shipped as interim (to
	 * record the per-part terminal claim); its reactions were claimed on that
	 * first pass and must not be claimed - or rejected as duplicates - again.
	 */
	const reactionsClaimedFor = new Set<string>();
	/**
	 * One verdict per part text: the terminal pass re-reads text the tail already
	 * judged as interim, and must reach the same answer without a second call.
	 */
	const narrationVerdicts = new Map<string, Promise<boolean>>();
	const isNarration = (part: string): Promise<boolean> => {
		let verdict = narrationVerdicts.get(part);
		if (!verdict) {
			verdict = isAbstentionNarration(key, userText, part);
			narrationVerdicts.set(part, verdict);
		}
		return verdict;
	};
	const deliverAssistantText = async (rawMessage: string, source: "interim" | "terminal") => {
		if (!nonLoopback) return;
		let message = rawMessage;
		const reactionReply = parseReactionReply(message);
		if (reactionReply && reactionsClaimedFor.has(rawMessage)) {
			message = reactionReply.body;
			if (!message) return;
		} else if (reactionReply) {
			reactionsClaimedFor.add(rawMessage);
			reactionTokensSeen = true;
			for (const wanted of reactionReply.reactions) {
				if (!platformSupportsReaction(origin.platform, wanted.emojiName)) {
					console.error(
						`gateway reaction skipped for ${key}: ${origin.platform} cannot react with ${wanted.emoji} (${wanted.emojiName})`,
					);
					continue;
				}
				const targetMessageId = wanted.targetMessageId ?? row.message_id;
				const rejection = runtime.reactions.claim({ turnId, originKey: key, targetMessageId, emoji: wanted.emoji });
				if (rejection) {
					console.error(
						`gateway reaction rejected (${rejection.reason}) for ${key} message ${targetMessageId}: ${rejection.detail}`,
					);
					continue;
				}
				const payload = runtime.delivery.prepareReaction(crypto.randomUUID(), origin, {
					targetMessageId,
					emoji: wanted.emoji,
					emojiName: wanted.emojiName,
				});
				assistantDeliveryStarted = true;
				broadcastDelivery(runtime, payload);
			}
			message = reactionReply.body;
			if (!message) return;
		}
		// Control tokens are internal protocol, never user-visible. Models routinely
		// wrap them in a "reasoning" preamble ("...nothing to add.\n\n[SILENT]"), so a
		// part is judged by whether it CONTAINS the token, not whether it equals it:
		// any part carrying a silence token is dropped whole, and [REPLY:id] is
		// honoured wherever it appears and always stripped from the delivered text.
		const spokenParts = message
			.split(/\n\s*\[BREAK\]\s*\n?/)
			.map((part) => part.trim())
			.filter((part) => part.length > 0 && !isSilenceToken(part) && !containsSilenceToken(part))
			.slice(0, 5);
		// A persona that decided not to speak often says so instead of emitting
		// the token (#260). On gated traffic, such a part is dropped like one.
		const parts =
			engagement?.speechGated === true
				? (await Promise.all(spokenParts.map(async (part) => ((await isNarration(part)) ? undefined : part)))).filter(
						(part): part is string => part !== undefined,
					)
				: spokenParts;
		// Slack replies default INTO a thread when the persona names no target:
		// - a channel mention is answered in a thread rooted at the triggering
		//   message (the room stays readable; the conversation continues in the
		//   thread, which is its own session);
		// - a DM that arrived inside a thread keeps its DM session identity (the
		//   origin has no thread), so the root survives only in the trigger's
		//   engagement metadata and is reused here.
		// Thread origins already carry their root; explicit [REPLY:…] always wins.
		// Slack only: a Discord DM's reply metadata is an ordinary "replied to X"
		// note whose delivery would otherwise turn into a quoted reply nobody asked for.
		const inboundThreadRoot =
			origin.platform !== "slack"
				? undefined
				: origin.kind === "channel" &&
						nonLoopback &&
						isSlackMessageId(editedMessageId(row.message_id) ?? row.message_id)
					? (editedMessageId(row.message_id) ?? row.message_id)
					: origin.kind === "dm" && typeof engagement?.replyTo?.messageId === "string" && engagement.replyTo.messageId
						? engagement.replyTo.messageId
						: undefined;
		const planned: Array<{ readonly body: string; readonly replyTo?: string }> = [];
		for (const part of parts) {
			if (planned.length >= maxTurnParts) break;
			// Reply-threading: a part may open with [REPLY:<platform message id>] to
			// answer a specific message; mentions are plain <@author id> in the text.
			const replyMatch = part.match(/\[REPLY:([^\]\s]+)\]/);
			const body = (replyMatch ? part.replace(/\s*\[REPLY:[^\]\s]+\]\s*/g, " ") : part)
				.replace(/\s*\[BREAK\]\s*/g, " ")
				.trim();
			if (!body) continue;
			const replyTo = replyMatch?.[1] || inboundThreadRoot;
			planned.push({ body, ...(replyTo ? { replyTo } : {}) });
		}
		const spoken = spokenReply(
			planned.map((step) => step.body),
			voiceTurn,
		);
		for (let index = 0; index < planned.length; index++) {
			// The per-turn part budget bounds MID-WORK speech only. The terminal
			// answer always records its claim: with an ungated stream a chatty turn
			// could otherwise exhaust the budget on interims and lose its final.
			if (source === "interim" && deliveredParts.length >= maxTurnParts) return;
			const step = planned[index] as { body: string; replyTo?: string };
			// Interim parts are keyed on (trigger, text, part): distinct findings get
			// distinct rows, a replayed finding (stream backfill, id-less frame after
			// restart) collides. The terminal reply owns ONE slot per part keyed on
			// (trigger, part) only, so a regenerated or reconciled second answer for
			// the same inbound message can never post, whatever its text.
			const deliveryId =
				source === "terminal"
					? deterministicTerminalDeliveryId(key, input.turn.triggerMessageId, index)
					: deterministicInterimDeliveryId(key, input.turn.triggerMessageId, step.body, index);
			if (source === "terminal") {
				// A finalized answer that already shipped on the tail satisfied the
				// slot; the durable claim makes that survive a gateway restart.
				const interimId = deterministicInterimDeliveryId(key, input.turn.triggerMessageId, step.body, index);
				const priorRow = runtime.delivery.get(interimId);
				const owner = options.database.inboundTurnClaimTerminal(
					input.turn.opRef,
					index,
					priorRow ? interimId : deliveryId,
				);
				if (owner !== deliveryId) continue;
			}
			const payload = runtime.delivery.prepare(crypto.randomUUID(), origin, step.body, step.replyTo, deliveryId);
			if (!payload) continue;
			deliveredParts.push(step.body);
			assistantDeliveryStarted = true;
			const isLast = index === planned.length - 1;
			broadcastDelivery(runtime, isLast && spoken !== "" ? { ...payload, voiceText: spoken } : payload);
		}
	};

	const startedAt = Date.now();
	const firstAfterMs = options.progress?.firstAfterMs ?? 10_000;
	const intervalMs = options.progress?.intervalMs ?? 10_000;
	let lastProgressAt = 0;
	let lastKnown = { toolCalls: 0, outputTokens: 0 };
	let activity: ChatProgressActivity | undefined;
	/** Heartbeats present the most recent tail observation; they never invent progress. */
	let tailActivitySeen = false;
	let ended = false;
	const emitProgress = (progress: { toolCalls: number; outputTokens: number }, final = false, prompt = false) => {
		lastKnown = progress;
		const now = Date.now();
		// A change of activity (the first tool starting, a new tool) is worth
		// announcing as soon as the turn is old enough to show a hint at all -
		// that is the "what is it doing" signal - but never more often than half
		// the interval, so a tool-per-second turn cannot become a request storm.
		const minGap = prompt ? intervalMs / 2 : intervalMs;
		const due = now - startedAt >= firstAfterMs && now - lastProgressAt >= minGap;
		if (!final && (!tailActivitySeen || !due)) return;
		// `final` is UNCONDITIONAL. It is the adapter's only signal that the turn
		// stopped (typing hint, "working" status), and a turn that answered fast,
		// stayed silent, or failed before its first tail frame never announced
		// progress - gating final on a prior announcement left Discord "typing…"
		// for the full 330s cap after every such turn (2026-09-03, local).
		lastProgressAt = now;
		const payload = {
			turnId,
			origin,
			elapsedMs: now - startedAt,
			toolCalls: progress.toolCalls,
			outputTokens: progress.outputTokens,
			...(activity ? { activity } : {}),
			...(final ? { final: true } : {}),
		};
		for (const recipient of runtime.connections)
			if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.progress", payload });
	};
	// Counters come only from the live tail (onFrame): tool_activity frames and
	// finalized assistant text. gjc 0.16.0's stream carries no token counters,
	// and polling transcript.list/usage.get cost two gjc spawns (~1s CPU each)
	// every interval per running turn, which starved the broker health probe
	// under load. The heartbeat now only re-presents the last tail observation.
	const heartbeat = setInterval(() => {
		if (tailActivitySeen) emitProgress(lastKnown);
	}, intervalMs);
	const endProgress = () => {
		if (ended) return;
		ended = true;
		clearInterval(heartbeat);
		emitProgress(lastKnown, true);
	};

	const onFrame = async ({ frame, sessionId }: PersonaTailFrameInput) => {
		if (ended) return false;
		tailActivitySeen = true;
		// Every assistant message the owned relay delivers before the final answer
		// is mid-work speech. The persona's own instructions decide what it says
		// mid-turn; the gateway delivers it. The stream delivers each message
		// once, so there is nothing to de-duplicate here.
		if (frame.assistantText?.trim() && !frame.steerEcho) {
			lastKnown = {
				toolCalls: lastKnown.toolCalls,
				outputTokens: lastKnown.outputTokens + Math.ceil(frame.assistantText.length / 4),
			};
			try {
				await deliverAssistantText(frame.assistantText, "interim");
			} catch (error) {
				console.error(`gateway intermediate delivery failed (${turnId}): ${diagnostic(error)}`);
			}
		}
		const previousActivity = activity;
		activity = deriveActivity(frame, activity);
		const activityChanged =
			activity !== undefined &&
			(previousActivity === undefined ||
				previousActivity.kind !== activity.kind ||
				previousActivity.label !== activity.label ||
				previousActivity.detail !== activity.detail);
		const reportedTools = frame.payload.toolCalls;
		const reportedTokens = frame.payload.outputTokens;
		lastKnown = {
			toolCalls:
				typeof reportedTools === "number" && Number.isFinite(reportedTools)
					? Math.max(lastKnown.toolCalls, reportedTools)
					: frame.payload.toolCallStarted === true
						? lastKnown.toolCalls + 1
						: lastKnown.toolCalls,
			outputTokens:
				typeof reportedTokens === "number" && Number.isFinite(reportedTokens)
					? Math.max(lastKnown.outputTokens, reportedTokens)
					: lastKnown.outputTokens,
		};
		emitProgress(lastKnown, false, activityChanged);
		return assistantDeliveryStarted;
	};

	const renderSteer = (steered: InboundMessageRow): string => {
		const steerEngagement = steered.engagement_json
			? (JSON.parse(steered.engagement_json) as NonNullable<typeof engagement>)
			: undefined;
		const steerSpeaker = composeSpeakerLabel(steerEngagement);
		return steerSpeaker
			? `${composeTurnHeader({ speaker: steerSpeaker, place, authorId: steerEngagement?.authorId, messageId: steered.message_id, engagement: steerEngagement })}\n${steered.body}`
			: steered.body;
	};
	// The steered message reached the model inside THIS turn: it is read context
	// now, not unread for the next turn. A pointer update names the ORIGINAL
	// message (that is where the edited body now lives). Consumed in the same
	// transaction as the acceptance; only transient ownership is released here.
	const steerContextMessageId = (steered: InboundMessageRow): string | undefined =>
		nonLoopback ? (editedMessageId(steered.message_id) ?? steered.message_id) : undefined;
	const onSteerAccepted = ({ row: steered }: PersonaSteerInput) => {
		runtime.inbound.delete(steered.message_id);
	};
	const onTerminal = async ({ text }: PersonaTerminalInput) => {
		try {
			if (nonLoopback) options.database.contextCommitWindow(key, contextMessageIds, contextOmissionRevision);
			if (bootstrap)
				options.database.markSessionBootstrapped(key, input.epoch, {
					includedSections: bootstrap.includedSections,
					byteCount: bootstrap.byteCount,
					truncated: bootstrap.truncated,
					diagnostics: bootstrap.diagnostics,
				});
			const replyText = deliveredParts.length > 0 ? deliveredParts.join("\n") : text;
			options.database.withTransaction(() => {
				options.database.updateActivity(key, JSON.stringify(origin));
				options.database.addRecall(
					key,
					JSON.stringify(origin),
					`user: ${userText.slice(0, 500)}\nassistant: ${replyText.slice(0, 500)}`,
				);
			});
			if (deliveredParts.length === 0 && isSilenceToken(text)) return;
			if (!nonLoopback) {
				if (connection)
					connection.write({
						v: PROFILE_VERSION,
						type: "event",
						event: "chat.message",
						...(context ? { id: context.requestId } : {}),
						payload: { turnId, origin, role: "assistant", text, final: true },
					});
				runtime.memory.enqueue({
					kind: "daily_capture",
					originRefJson: JSON.stringify(origin),
					userText,
					replyText: text,
				});
				return;
			}
			const capturedUser = speaker ? `${speaker} @ ${place}: ${userText}` : userText;
			// Always run the terminal path, even when the tail already shipped this
			// exact text as an interim part. Its job here is not to re-send (the
			// per-part claim points at the interim row and the send is skipped) but
			// to RECORD that claim: without it a regenerated or reconciled second
			// answer for the same trigger finds every slot unowned and posts. The
			// former "same text → skip" shortcut left that hole whenever the
			// finalized frame arrived on the tail before onTerminal.
			await deliverAssistantText(text, "terminal");
			if (deliveredParts.length === 0) {
				if (reactionTokensSeen)
					runtime.memory.enqueue({
						kind: "daily_capture",
						originRefJson: JSON.stringify(origin),
						userText: capturedUser,
						replyText: text,
					});
				return;
			}
			runtime.memory.enqueue({
				kind: "daily_capture",
				originRefJson: JSON.stringify(origin),
				userText: capturedUser,
				replyText,
			});
		} finally {
			endProgress();
		}
	};

	const onFailure = async ({ error }: PersonaFailureInput) => {
		try {
			const failureNotice = formatFailureNotice(error);
			console.error(failureNotice);
			if (nonLoopback && assistantDeliveryStarted)
				options.database.contextCommitWindow(key, contextMessageIds, contextOmissionRevision);
			if (nonLoopback && !assistantDeliveryStarted) {
				// Recovery can observe the same failed terminal after the notice was
				// persisted but before the trigger was settled. Reuse the trigger's
				// terminal delivery identity instead of emitting another failure.
				const notice = runtime.delivery.prepare(
					turnId,
					origin,
					failureNotice,
					undefined,
					deterministicTerminalDeliveryId(key, input.turn.triggerMessageId, 0),
				);
				if (notice) {
					runtime.delivery.markInflight(notice.deliveryId as string);
					broadcastDelivery(runtime, notice);
				}
			}
		} finally {
			endProgress();
		}
	};
	const onSettled = ({ terminalDeliveryId }: PersonaTurnSettledInput) => {
		if (engagement?.authorIsBot !== true || terminalDeliveryId !== null) return;
		// A delivered `[turn failed]` notice is a diagnostic, not an answer: it is
		// deliberately emitted under a different delivery/turn identity, so it does
		// not claim this trigger's terminal reply slot. Refund from the durable slot
		// state rather than from the diagnostic text.
		runtime.botAudienceTurns.releaseUnansweredAdmission(key, input.turn.triggerMessageId);
	};

	return {
		text: turnText,
		systemPreamble,
		...(effectiveModel ? { effectiveModel } : {}),
		...(runtime.config.serviceTier ? { effectiveServiceTier: runtime.config.serviceTier } : {}),
		renderSteer,
		steerContextMessageId,
		onSteerAccepted,
		onFrame,
		onTerminal,
		onFailure,
		onSettled,
		// Neither path ever reaches onTerminal/onFailure for THIS lifecycle: a
		// `/new` retire fences the answer, a release re-dispatches the trigger
		// under a new lifecycle. Without the final tick the 10s heartbeat kept
		// the adapter's "working…" status alive for hours (live: 286m, 2026-09-05).
		onRetired: endProgress,
		onReleased: endProgress,
		onStall: ({ elapsedMs }) =>
			console.error(`gateway persona turn stalled (${turnId}) after ${elapsedMs}ms; retaining status reconciliation.`),
	};
}

function parseReceivedAt(value: unknown): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !value)
		throw new ProtocolError("invalid_params", "receivedAt must be an ISO timestamp");
	const timestamp = Date.parse(value);
	if (!Number.isFinite(timestamp)) throw new ProtocolError("invalid_params", "receivedAt must be an ISO timestamp");
	return new Date(timestamp).toISOString();
}

function bootstrapProjection(row: {
	readonly epoch: number;
	readonly last_bootstrapped_epoch: number;
	readonly bootstrap_applied_at: string | null;
	readonly bootstrap_sections_json: string;
	readonly bootstrap_byte_count: number;
	readonly bootstrap_truncated: number;
	readonly bootstrap_diagnostics_json: string;
}): {
	readonly epoch: number;
	readonly pending: boolean;
	readonly appliedAt: string | null;
	readonly includedSections: readonly string[];
	readonly byteCount: number;
	readonly truncated: boolean;
	readonly diagnostics: readonly string[];
} {
	const strings = (value: string): readonly string[] => {
		try {
			const parsed = JSON.parse(value);
			return Array.isArray(parsed) && parsed.every((item) => typeof item === "string")
				? parsed
				: ["projection_corrupt"];
		} catch {
			return ["projection_corrupt"];
		}
	};
	return {
		epoch: row.epoch,
		pending: row.last_bootstrapped_epoch < row.epoch,
		appliedAt: row.bootstrap_applied_at,
		includedSections: strings(row.bootstrap_sections_json),
		byteCount: row.bootstrap_byte_count,
		truncated: row.bootstrap_truncated === 1,
		diagnostics: strings(row.bootstrap_diagnostics_json),
	};
}

/**
 * Replaces C0 control characters and DEL with a space. An inbound reaction emoji is
 * rendered into the turn's context block line by line, so a stray newline there
 * would read as another entry the persona was told about.
 */
function stripControlCharacters(value: string): string {
	let stripped = "";
	for (const character of value) {
		const code = character.codePointAt(0) ?? 0;
		stripped += code < 0x20 || code === 0x7f ? " " : character;
	}
	return stripped;
}

/**
 * The single spoken form of a whole reply, or "" when this turn is not spoken.
 *
 * The owner's rule is one reply, written once, delivered in both modalities when
 * the question was spoken — so every part is joined into ONE utterance rather
 * than one voice message per `[BREAK]`, which would talk over itself and bill
 * per part.
 *
 * `[REPLY:...]` prefixes are stripped: the token is routing metadata, and
 * reading a platform message id aloud is noise the listener cannot use.
 */
export function spokenReply(parts: readonly string[], voiceTurn: boolean): string {
	if (!voiceTurn) return "";
	return parts
		.map((part) => part.replace(/^\[REPLY:[^\]\s]+\]\s*/, "").trim())
		.filter((body) => body.length > 0)
		.join("\n\n");
}

/** Marks a prepared delivery in flight and fans it out to every negotiated adapter. */
function broadcastDelivery(runtime: Runtime, payload: ChatMessagePayload): void {
	runtime.delivery.markInflight(payload.deliveryId as string);
	for (const recipient of runtime.connections)
		if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
}

function reportDeliveryExpired(
	runtime: Runtime,
	expired: Pick<ExpiredDeliveryRow, "deliveryId" | "originKey" | "attempts">,
	reason: string,
): void {
	const deliveryId = safeDiagnosticField(expired.deliveryId);
	const origin = safeDiagnosticField(expired.originKey);
	const attempts = Number.isSafeInteger(expired.attempts) && expired.attempts >= 0 ? expired.attempts : 0;
	console.error(
		`delivery_expired deliveryId=${deliveryId} origin=${origin} attempts=${attempts} reason=${safeDiagnosticField(reason)}`,
	);
	if (expired.deliveryId.startsWith("gw-x-")) return;
	const ownerTarget = runtime.config.ownerTarget?.origin;
	if (!ownerTarget) return;
	const noticeId = deterministicDeliveryExpiredNoticeId(expired.deliveryId);
	const notice = `[delivery lost] ${sanitizeDiagnostic(expired.originKey).slice(0, 160)} 응답 전달이 ${attempts}회 실패해 만료됐습니다. 재전송: gajaeway ops redeliver ${deliveryId}`;
	const payload = runtime.delivery.prepare(noticeId, ownerTarget, notice, undefined, noticeId);
	if (payload) broadcastDelivery(runtime, payload);
}

function safeDiagnosticField(value: string): string {
	return sanitizeDiagnostic(value).slice(0, 160).replace(/\s+/g, "_") || "unknown_error";
}

function deterministicDeliveryExpiredNoticeId(deliveryId: string): string {
	return `gw-x-${createHash("sha256").update(deliveryId).digest("hex").slice(0, 32)}`;
}

function deterministicBindHoldDeliveryId(originKey: string, triggerMessageId: string): string {
	return `gw-h-${createHash("sha256").update(`${originKey}|${triggerMessageId}|bind_hold`).digest("hex").slice(0, 32)}`;
}

/**
 * Session-context grounding (live finding: without it the persona could not
 * tell which conversation it was in and imported other origins' memory as if
 * it had been said here).
 */
export function currentConversationNotice(origin: OriginRef): string {
	const where =
		origin.kind === "dm"
			? `a PRIVATE direct-message conversation (${origin.platform} DM ${origin.conversationId}, peer ${origin.peerId})`
			: origin.kind === "loopback"
				? "the local loopback console"
				: `a ${origin.kind === "channel" ? "PUBLIC/group channel" : origin.kind} (${origin.platform} ${origin.kind} ${origin.conversationId})`;
	return [
		"## Current conversation",
		`You are replying inside ${where}. This session is bound to exactly this one conversation.`,
		`Shared memory (memory/daily and canonical axes) records EVERY conversation, each entry tagged with its origin. Entries whose canonical origin key differs from ${originKey(origin)} happened elsewhere: treat them as background knowledge only, never as something said here, and do not import their topics or in-flight work into this conversation unprompted.`,
		// No per-turn "you were / were not addressed" verdict. Stamping every
		// untagged message in an `open` room as "NOT addressed: default to [SILENT]"
		// made the persona treat people talking to it as none of its business and go
		// quiet on them (live, playground-ko, 2026-09-06). Whether to speak is the
		// persona's judgement from the conversation; the runtime only names the tool.
		...(origin.kind !== "dm" && origin.kind !== "loopback"
			? [
					"To stay quiet on a message that is not for you, reply with exactly [SILENT] and nothing else — that suppresses delivery while the message stays recorded.",
				]
			: []),
		// Reply-threading was implemented end to end (parser, ledger, Slack/Discord
		// adapters) but never named in the session context, so the persona answered
		// threaded messages at the conversation root and looked like it ignored the
		// thread (live, slack DM, 2026-09-17).
		...(isChatPlatform(origin.platform)
			? [
					"Threaded replies: start a reply part with [REPLY:<message id>] to answer that specific message; the token is routing metadata and never appears in the delivered text. Message ids are in each incoming message header (msg:<id>). When the message you are answering is itself inside a thread, target the thread's parent message id so your answer lands in that thread instead of the conversation root.",
				]
			: []),
		// The third reply mode: acknowledge without speaking. Kept next to the silence
		// guidance because the persona chooses between exactly these three shapes.
		...(isChatPlatform(origin.platform)
			? [
					`Reaction replies: start your reply with [REACT:<emoji>] to react to the message that triggered this turn, or [REACT:<emoji>@<message id>] to react to a specific message. With nothing after the token you acknowledge with a reaction and say nothing; text after the token is sent as well. Emoji ${origin.platform} can actually deliver: ${reactionAllowlistDescription(origin.platform)}. At most ${REACTIONS_PER_TURN_CAP} reactions per turn and ${REACTIONS_PER_MESSAGE_CAP} per message.`,
				]
			: []),
	].join("\n");
}
/** A Slack platform message id is `channel:ts`; synthetic trigger ids (`slash-…`, `edit:…`) never thread. */
function isSlackMessageId(value: string): boolean {
	return /^[A-Z][A-Z0-9]+:\d+\.\d+$/.test(value);
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}

function writeError(connection: Connection, error: unknown, id?: string): void {
	const protocol = error instanceof ProtocolError ? error : new ProtocolError("verb_failed", "gateway request failed");
	connection.write({ v: PROFILE_VERSION, type: "error", ...(id ? { id } : {}), error: protocol.toPayload() });
}

/**
 * Command authorization is exactly ordinary engagement authorization: loopback,
 * DM policy, owner identity, allowlist, and group gates apply before `/new`,
 * `/reset`, or `/model` can mutate persistent session state.
 */
function ownerPeerIdOf(config: GatewayConfig): string | undefined {
	const owner = config.ownerTarget?.origin;
	return owner && "peerId" in owner ? (owner as { peerId?: string }).peerId : undefined;
}

function commandAuthorised(
	origin: Parameters<typeof decideEngagement>[0],
	config: GatewayConfig,
	engagement: unknown,
	threadFollowUp: boolean,
): boolean {
	return decideEngagement(origin, engagement as Parameters<typeof decideEngagement>[1], config, threadFollowUp).engaged;
}
