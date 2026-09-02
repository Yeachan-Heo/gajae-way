import { unlink } from "node:fs/promises";
import { join } from "node:path";
import {
	CAPABILITIES,
	type ChatMessagePayload,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
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
} from "@gajaeway/protocol";
import {
	acknowledgeHold,
	appendAttempt,
	applyReconciliation,
	closeAttempt,
	createLaneJobRecord,
	hasNewCommit,
	LaneJobError,
	type LaneJobRecord,
	newOpRef,
	parseLaneJobRecord,
} from "@gajaeway/subsession";
import { type ConfigOverrides, type GatewayConfig, type ReloadResult, reloadConfig } from "../config";
import { DeliveryService } from "../delivery/delivery";
import { ReactionBudget } from "../delivery/reaction-budget";
import { decideEngagement } from "../engagement/policy";
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
import type { BrokerSupervisor } from "../orchestrator/broker";
import {
	type PersonaFailureInput,
	PersonaSessionManager,
	type PersonaTailFrameInput,
	type PersonaTerminalInput,
	type PersonaTurnLifecycle,
	type PersonaTurnStartInput,
} from "../orchestrator/persona-session";
import { formatFailureNotice, sanitizeDiagnostic } from "../orchestrator/rebind";
import { type SessionPort, SessionRequestTimeoutError } from "../orchestrator/session-port";
import { deterministicTailDeliveryId } from "../orchestrator/tail-runner";
import { buildSessionBootstrap } from "../persona/bootstrap";
import { PersonaLoader } from "../persona/persona";
import type { GatewayDatabase, InboundMessageRow, MonitorEventStage } from "../store/db";
import { DeliveryLedger } from "../store/ledger";
import { ATTACHMENT_SCOPE_NOTICE } from "./attachment-scope";
import { OrderedFrameWriter } from "./frame-writer";
import { InterimSpeechGate, type InterimSpeechLimits } from "./interim-speech";
import { applyModelCommand } from "./model-command";
import { composeSpeakerLabel, composeTurnHeader } from "./speaker";

/**
 * Durable lane-job persistence for delegated work (issue #10).
 *
 * Every `work.run` call is one attempt against a job whose identity outlives
 * the turn. Its caller-supplied operation reference, accepted receipt, and
 * terminal transition persist through the broker-backed SessionPort, so a
 * gateway restart leaves an auditable record instead of an orphaned operation.
 */
function laneJobIdentity(workName: string): { jobId: string; laneKey: string } {
	const laneKey = `work-${workName}`;
	// The jobId is INJECTIVE: each UTF-8 byte of the exact name becomes two hex
	// digits, so distinct names (Foo/foo, a.b/a_b) always map to distinct ids
	// while staying within the [a-z0-9-] jobId alphabet. 64-byte names cap at
	// 128 hex chars + the prefix, inside the schema's id length bound.
	const hex = Buffer.from(workName, "utf8").toString("hex");
	return { jobId: `lanejob-${hex}`, laneKey };
}

function persistLaneJob(database: GatewayDatabase, record: LaneJobRecord, laneKey: string): void {
	database.putLaneJob({
		jobId: record.jobId,
		laneKey,
		state: record.state,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
		lane: record.lane,
		json: JSON.stringify(record),
	});
}

/** Reads the actual branch HEAD and dirtiness of the worktree (read-only git). */
async function collectRepoFacts(
	worktreePath: string,
): Promise<{ headSha?: string; dirtyFiles: number; branch?: string } | undefined> {
	try {
		const head = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "HEAD"], { stdout: "pipe", stderr: "pipe" });
		const branch = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const status = Bun.spawnSync(["git", "-C", worktreePath, "status", "--porcelain"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const headSha = head.stdout.toString().trim();
		if (!/^[0-9a-f]{40}$/.test(headSha)) return undefined;
		const dirtyFiles = status.stdout
			.toString()
			.split("\n")
			.filter((line) => line.trim()).length;
		const branchName = branch.stdout.toString().trim();
		return { headSha, dirtyFiles, ...(branchName ? { branch: branchName } : {}) };
	} catch {
		return undefined;
	}
}

async function loadOrCreateLaneJob(
	database: GatewayDatabase,
	workName: string,
	workCwd: string,
): Promise<LaneJobRecord> {
	const { jobId, laneKey } = laneJobIdentity(workName);
	// The exact lane key (original name, case- and dot-faithful) is the
	// authoritative lookup; the derived jobId is only a storage id.
	const stored = database.laneJobJsonByLaneKey(laneKey) ?? database.laneJobJson(jobId);
	if (stored !== undefined) {
		const existing = parseLaneJobRecord(stored);
		if (existing.lane.worktreePath !== workCwd) {
			// Same worker name re-pointed at a different worktree is a lane
			// mismatch: fail closed instead of reconciling one lane's commits
			// into another lane's history.
			throw new LaneJobError(
				`worker ${workName} is bound to worktree ${existing.lane.worktreePath}, not ${workCwd}; use a different name or restore the original cwd`,
			);
		}
		return existing;
	}
	// The lane block describes the REAL lane: actual worktree, actual branch
	// when git can tell us, deterministic fallback otherwise. The starting
	// HEAD is stored as the BASELINE - it is context, never worker progress.
	const facts = await collectRepoFacts(workCwd);
	const created = createLaneJobRecord({
		jobId,
		branch: facts?.branch ?? `work/${workName.toLowerCase().replace(/[^a-z0-9._/-]+/g, "-")}`,
		worktreePath: workCwd,
		baselineSha: facts?.headSha,
	});
	persistLaneJob(database, created, laneKey);
	return created;
}

/** Persona tail stall heartbeat; well under the 120s stallTimeoutMs so alarms land within one interval of the threshold. */
const DEFAULT_STALL_CHECK_INTERVAL_MS = 5_000;
/** /restart: hard-exit budget after the ordered stop begins. */
const RESTART_HARD_EXIT_MS = 15_000;
/** Thread history shown to a freshly started session: everything (humans, bots, self) in the last 24h, capped. */
const RECENT_HISTORY_WINDOW_MS = 24 * 60 * 60_000;
const RECENT_HISTORY_MAX = 300;
const RESTART_EXIT_CODE = 75;
interface Connection {
	readonly decoder: FrameDecoder;
	negotiated: boolean;
	write(frame: Frame): void;
	close(): void;
	settle(): Promise<void>;
}
export interface GatewayServer {
	stop(reason?: string): Promise<void>;
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
	/** Broker ownership is released after request/turn/tail-like runtime work has drained. */
	readonly broker?: BrokerSupervisor;
	/** Per-process CLI overrides, reapplied on every live reload so they survive it. */
	readonly overrides?: ConfigOverrides;
	/** Test seam for chat.progress throttling; production uses the 15s defaults. */
	readonly progress?: { readonly firstAfterMs?: number; readonly intervalMs?: number };
	/** Test seam: how /restart ends the process after the ordered stop (default process.exit). */
	readonly exitProcess?: (code: number) => void;
	/** Test seam for the persona tail stall heartbeat; production uses the 5s default. */
	readonly stallCheckIntervalMs?: number;
	/** Mid-work speech pacing (issue #71). */
	readonly interimSpeech?: Partial<InterimSpeechLimits>;
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
	runtime.personaSessions.setMaxInboundAgeMs(result.config.maxInboundAgeMs);
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
	readonly stallTimer: ReturnType<typeof setInterval>;
	readonly contextMaintenanceTimer: ReturnType<typeof setInterval>;
	readonly stopBrokerGenerationListener?: () => void;
	/** Per-turn / per-message reaction caps shared by chat.react and the reply-token path. */
	readonly reactions: ReactionBudget;
	/** Accepted-but-not-yet-dispatched inbound messages, keyed by message id. */
	readonly inbound: Map<string, InboundContext>;
	/** Every admitted request except the shutdown request itself, so stop() can quiesce all writers. */
	readonly requests: Set<Promise<void>>;
	/** Read-only runtime-cycle projection (ops.cycle); owns no writes. */
	readonly cycle: RuntimeCycleProjector;
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
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			// Stop accepting new sockets first, but keep existing sockets alive. Then
			// quiesce every admitted producer before taking the final writer snapshot.
			listener.stop(false);
			await Promise.all([...runtime.requests]);
			await runtime.personaSessions.drain();
			await runtime.personaSessions.stop();
			runtime.stopBrokerGenerationListener?.();
			await runtime.monitorRuntime.stop();
			// Cancels pending monitor burst timers so a closed database is never touched.
			runtime.monitors.dispose();
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
				socket.data.writer.close();
				runtime.connections.delete(socket.data.connection);
			},
			error(socket, error) {
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
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			// Same producer quiescence as the Unix server: in-flight stdio requests are
			// tracked and awaited before persona/tail/broker teardown.
			await Promise.all([...runtime.requests]);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			await runtime.personaSessions.drain();
			await runtime.personaSessions.stop();
			runtime.stopBrokerGenerationListener?.();
			await runtime.monitorRuntime.stop();
			// Cancels pending monitor burst timers so a closed database is never touched.
			runtime.monitors.dispose();
			await options.broker?.stop();
			connection.close();
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
	};
	runtime.stop = stop;
	process.stdin.on("data", (data: Buffer) => {
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
	});
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
	// Legacy v16 processing rows can exist before the additive actor cutover. Batch
	// rows stay visible for the actor and are never reset by this recovery pass.
	const recovered = options.database.inboundRecoverProcessing();
	console.error(`gateway recovered ${recovered} inbound message(s) stranded in processing.`);
	const delivery = new DeliveryService(new DeliveryLedger(options.database));
	const registry = new MonitorRegistry(options.database);
	const memory = new MemoryClosureQueue(options.database, options.config.home);
	let runtime!: Runtime;
	const personaSessions = new PersonaSessionManager({
		database: options.database,
		port: sessionPort,
		instanceId: options.database.instanceId,
		repo: join(options.config.home, "workspace"),
		settleWindowMs: options.config.settleWindowMs,
		settleWindowFor: (row) => settleWindowFor(row, runtime.config),
		stallTimeoutMs: options.config.stallTimeoutMs,
		maxInboundAgeMs: options.config.maxInboundAgeMs,
		brokerGeneration: () => options.broker?.generation ?? 0,
		onTurnStart: async (input) => await createInboundTurnLifecycle(input, options, runtime),
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
	const reconcileTimer = setInterval(() => {
		void monitors.reconcile();
		void personaSessions
			.recover()
			.catch((error: unknown) => console.error(`persona recovery sweep failed: ${diagnostic(error)}`));
	}, 60_000);
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
		| (BrokerSupervisor & { onGeneration?: BrokerSupervisor["onGeneration"] })
		| undefined;
	const stopBrokerGenerationListener =
		typeof brokerWithGeneration?.onGeneration === "function"
			? brokerWithGeneration.onGeneration((generation) => {
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
		stallTimer,
		contextMaintenanceTimer,
		...(stopBrokerGenerationListener ? { stopBrokerGenerationListener } : {}),
		reactions: new ReactionBudget(),
		cycle: new RuntimeCycleProjector(options.database, memory),
		inbound,
		requests: new Set(),
	};
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
					contextDiff: options.database.contextDiagnostics(),
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
		case "work.run": {
			// First-class delegated work: a named worker SDK session in the coding
			// register, bound to a caller-chosen cwd and serialized per worker name.
			// Its broker-backed request/response operation returns the terminal body
			// directly without spawning a one-off child.
			const params = request.params as { name?: unknown; text?: unknown; cwd?: unknown; resume?: unknown } | undefined;
			if (typeof params?.name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(params.name))
				throw new ProtocolError("invalid_params", "work.run requires name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}");
			if (typeof params.text !== "string" || !params.text)
				throw new ProtocolError("invalid_params", "work.run requires non-empty text");
			if (params.cwd !== undefined && (typeof params.cwd !== "string" || !params.cwd.startsWith("/")))
				throw new ProtocolError("invalid_params", "work.run cwd must be an absolute path");
			const workName = params.name;
			const workText = params.text;
			const workCwd = params.cwd as string | undefined;
			const sessionKey = `work/task/${workName}`;
			const effectiveCwd = workCwd ?? process.cwd();
			const result = await runtime.sessionPort.runExclusive(sessionKey, async () => {
				const epoch = options.database.getSessionRecord(sessionKey)?.epoch ?? 0;
				const { sessionId } = await runtime.sessionPort.bind({
					originKey: sessionKey,
					epoch,
					repo: effectiveCwd,
					codingRegister: true,
				});
				options.database.updateActivity(
					sessionKey,
					JSON.stringify({ platform: "work", kind: "task", conversationId: workName }),
				);
				// Issue #10: this delegated call is one ATTEMPT against a durable
				// job. The receipt and its terminal transition persist even when
				// this process dies mid-turn, so nothing depends on a reply body.
				const { jobId, laneKey } = laneJobIdentity(workName);
				const prior = await loadOrCreateLaneJob(options.database, workName, effectiveCwd);
				// A serialized worker cannot have two live turns, so an attempt still
				// open at call time belongs to a crashed predecessor: record it as
				// exactly what we know - terminally uncertain - and then HOLD. The
				// predecessor's session may still be running after a restart; the
				// deterministic planner path for an uncertain attempt is an operator
				// hold, not an automatic continuation. `resume: true` is the explicit
				// operator acknowledgement that lets the next attempt start.
				// The awaiting_operator/stalled holds are STICKY: they survive
				// restarts and every subsequent call until an operator explicitly
				// resumes, so a genuinely uncertain or repeatedly stalling job can
				// never be silently re-driven.
				if ((prior.state === "awaiting_operator" || prior.state === "stalled") && params.resume !== true) {
					return {
						held: true as const,
						jobId,
						state: prior.state,
						reason: `the job is ${prior.state} (uncertain attempt or stalled continuations); reconcile, then re-run with resume: true`,
					};
				}
				const openPrior = prior.attempts.find((attempt) => attempt.endedAt === undefined);
				let uncertain: LaneJobRecord | undefined;
				if (openPrior) {
					uncertain = closeAttempt({
						record: prior,
						opRef: openPrior.opRef,
						endState: "terminal_uncertain",
						endedAt: new Date().toISOString(),
					});
					persistLaneJob(options.database, uncertain, laneKey);
					if (params.resume !== true) {
						return {
							held: true as const,
							jobId,
							state: uncertain.state,
							reason: `attempt ${openPrior.opRef} is terminally uncertain after a crash/restart; reconcile, then re-run with resume: true`,
						};
					}
				}
				// resume:true is an explicit operator acknowledgement: the sticky
				// hold (stalled budget or uncertain predecessor) is CLEARED and
				// the acknowledgement is audited in the escalation trail BEFORE
				// the new attempt starts. Without this, a successfully resumed
				// job would stay sticky-stalled and be held forever.
				let acknowledged = uncertain ?? prior;
				if (acknowledged.state === "stalled" || acknowledged.state === "awaiting_operator") {
					acknowledged = acknowledgeHold({
						record: acknowledged,
						note: "operator resumed the lane via work.run resume:true",
						at: new Date().toISOString(),
					});
					persistLaneJob(options.database, acknowledged, laneKey);
				}
				const opRef = newOpRef(`work-${workName.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`);
				let job = appendAttempt(acknowledged, {
					opRef,
					sessionId,
					startedAt: new Date().toISOString(),
				});
				persistLaneJob(options.database, job, laneKey);
				try {
					const reply = (
						await runtime.sessionPort.request({
							sessionId,
							repo: effectiveCwd,
							originKey: sessionKey,
							text: workText,
							opRef,
							codingRegister: true,
						})
					).assistant.text;
					job = closeAttempt({
						record: job,
						opRef,
						endState: "completed",
						endedAt: new Date().toISOString(),
					});
					// Repository first: whatever the reply said, a HEAD move on the
					// worktree is the authoritative checkpoint.
					const facts = await collectRepoFacts(effectiveCwd);
					if (facts) {
						const progressed = hasNewCommit({
							headSha: facts.headSha,
							dirtyFiles: facts.dirtyFiles,
							observedAt: new Date().toISOString(),
							knownCheckpoints: job.checkpoints,
							baselineSha: job.baselineSha,
						});
						job = applyReconciliation({
							record: job,
							repository: {
								...(facts.headSha ? { headSha: facts.headSha } : {}),
								dirtyFiles: facts.dirtyFiles,
								observedAt: new Date().toISOString(),
							},
							classification: progressed ? "progressed" : "held",
						});
					}
					persistLaneJob(options.database, job, laneKey);
					return { held: false as const, text: reply, jobId, opRef };
				} catch (error) {
					const failureMessage = diagnostic(error);
					// A bounded response wait never aborts the SDK turn. It records an
					// attempt-ended durable boundary so the lane remains reconcilable.
					const reaped = error instanceof SessionRequestTimeoutError;
					job = closeAttempt({
						record: job,
						opRef,
						endState: reaped ? "attempt_ended" : "failed",
						errorCode: reaped ? "gateway_turn_reaped" : undefined,
						endedAt: new Date().toISOString(),
					});
					// Repository first HERE TOO: the measured #9 shape is commits
					// landing right up to (or past) the kill; the repo, not the
					// failure, decides whether progress happened.
					const facts = await collectRepoFacts(effectiveCwd);
					if (facts) {
						const progressed = hasNewCommit({
							headSha: facts.headSha,
							dirtyFiles: facts.dirtyFiles,
							observedAt: new Date().toISOString(),
							knownCheckpoints: job.checkpoints,
							baselineSha: job.baselineSha,
						});
						job = applyReconciliation({
							record: job,
							repository: {
								...(facts.headSha ? { headSha: facts.headSha } : {}),
								dirtyFiles: facts.dirtyFiles,
								observedAt: new Date().toISOString(),
							},
							classification: progressed ? "progressed" : "stalled",
						});
					}
					persistLaneJob(options.database, job, laneKey);
					throw new Error(failureMessage);
				}
			});
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: result.held ? result : { ...result, sessionKey },
			});
			return;
		}
		case "work.jobs": {
			// Operator projection over durable lane jobs (issue #10): survives the
			// gateway restart that would otherwise erase in-flight work knowledge.
			// Each row is re-validated against the authoritative record JSON: a
			// corrupt row is flagged as corrupt for the operator, never shown as
			// healthy and never silently dropped.
			const jobs = options.database.laneJobRows().map((row) => {
				try {
					const record = parseLaneJobRecord(options.database.laneJobJson(row.job_id) ?? "");
					return {
						...row,
						attempts: record.attempts.length,
						checkpoints: record.checkpoints.length,
						escalations: record.escalations.length,
					};
				} catch (error) {
					return {
						...row,
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
			if (origin.platform !== "discord" && origin.platform !== "telegram")
				throw new ProtocolError("invalid_params", "chat.react requires a discord or telegram origin");
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
			if (origin.platform !== "discord" && origin.platform !== "telegram")
				throw new ProtocolError("invalid_params", "engagement.reaction requires a discord or telegram origin");
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
	// `/model` is a privileged control path. Apply the same direct-message and
	// group authorisation policy as ordinary engagement before inspecting or
	// mutating the durable override.
	if (userText === "/model" || userText.startsWith("/model ")) {
		if (!commandAuthorised(origin, runtime.config, params.engagement)) {
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
		if (!commandAuthorised(origin, runtime.config, params.engagement) || owner === undefined || authorId !== owner) {
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
		if (!commandAuthorised(origin, runtime.config, params.engagement)) {
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
	const engaged = decideEngagement(origin, params.engagement as never, runtime.config).engaged;
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
	const messageId = inboundMessageId ?? crypto.randomUUID();
	const turnId = crypto.randomUUID();
	// Persist before dispatch: this insert is the durable acceptance boundary. The
	// per-origin actor receives the notification only after this transaction wins.
	const accepted = options.database.inboundEnqueue({
		messageId,
		originKey: key,
		originRefJson: JSON.stringify(origin),
		body: userText,
		engagementJson: params.engagement ? JSON.stringify(params.engagement) : undefined,
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
	runtime.inbound.set(messageId, {
		turnId,
		requestId: request.id,
		connection,
		...(params.voice === true ? { voice: true } : {}),
	});
	connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { turnId, engaged: true } });
	await runtime.personaSessions.notifyInbound(key);
}
async function createInboundTurnLifecycle(
	input: PersonaTurnStartInput,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<PersonaTurnLifecycle> {
	// The newest message carries the live requester/voice context. Earlier members
	// remain in the durable context window and are included in the composed prompt.
	const row = input.rows[input.rows.length - 1] as InboundMessageRow;
	for (const member of input.rows) if (member !== row) runtime.inbound.delete(member.message_id);
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
				authorName?: string;
				authorHandle?: string;
				authorServerTag?: string;
				channelLabel?: string;
				serverLabel?: string;
				replyTo?: { messageId?: string; authorName?: string; fromSelf?: boolean; excerpt?: string };
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
		contextMessageIds = [...prepared.selectedMessageIds, row.message_id];
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
			.map((entry) => `- [${entry.at}] ${entry.author}: ${entry.body.slice(0, 500)}`);
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
		// AC2: every coalesced batch member is part of THIS turn's text, whether or
		// not the adapter recorded it in the unread-context ledger (a member without
		// a platform messageId never reaches that ledger). Members already present
		// in the ledger window are not repeated.
		const inWindow = new Set(prepared.selectedMessageIds);
		const members = input.rows
			.filter((member) => member !== row && !inWindow.has(member.message_id))
			.map((member) => `- [${member.received_at}] (msg:${member.message_id}): ${member.body.slice(0, 1000)}`);
		const memberBlock = members.length ? `[Earlier messages in this same turn]\n${members.join("\n")}\n\n` : "";
		turnText = `${header}${memberBlock}${speaker ? `${composeTurnHeader({ speaker, place, authorId: engagement?.authorId, messageId: row.message_id, engagement })}\n` : ""}${userText}`;
	} else if (input.rows.length > 1) {
		turnText = input.rows.map((member) => member.body).join("\n");
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
		currentConversationNotice(origin, engagement),
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
	const interimSpeech = new InterimSpeechGate(options.interimSpeech);
	let lastDeliveredRaw: string | undefined;
	const deliverAssistantText = (
		rawMessage: string,
		tailEvent?: { readonly sessionId: string; readonly eventId: string },
	) => {
		if (!nonLoopback) return;
		lastDeliveredRaw = rawMessage;
		let message = rawMessage;
		const reactionReply = parseReactionReply(message);
		if (reactionReply) {
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
		const parts = message
			.split(/\n\s*\[BREAK\]\s*\n?/)
			.map((part) => part.trim())
			.filter((part) => part.length > 0 && !isSilenceToken(part))
			.slice(0, 5);
		const planned: Array<{ readonly body: string; readonly replyTo?: string }> = [];
		for (const part of parts) {
			if (planned.length >= maxTurnParts) break;
			const replyMatch = part.match(/^\[REPLY:([^\]\s]+)\]\s*/);
			const body = replyMatch ? part.slice(replyMatch[0].length).trim() : part;
			if (body) planned.push({ body, ...(replyMatch?.[1] ? { replyTo: replyMatch[1] } : {}) });
		}
		const spoken = spokenReply(
			planned.map((step) => step.body),
			voiceTurn,
		);
		for (let index = 0; index < planned.length; index++) {
			if (deliveredParts.length >= maxTurnParts) return;
			const step = planned[index] as { body: string; replyTo?: string };
			const deliveryId =
				tailEvent === undefined
					? undefined
					: deterministicTailDeliveryId(
							tailEvent.sessionId,
							index === 0 ? tailEvent.eventId : `${tailEvent.eventId}:${index}`,
						);
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
	/** Heartbeats present the most recent tail observation; they never invent progress. */
	let tailActivitySeen = false;
	let progressAnnounced = false;
	let ended = false;
	const emitProgress = (progress: { toolCalls: number; outputTokens: number }, final = false) => {
		lastKnown = progress;
		const now = Date.now();
		if (!final && (!tailActivitySeen || now - startedAt < firstAfterMs || now - lastProgressAt < intervalMs)) return;
		if (final && !progressAnnounced) return;
		if (!final) progressAnnounced = true;
		lastProgressAt = now;
		const payload = {
			turnId,
			origin,
			elapsedMs: now - startedAt,
			toolCalls: progress.toolCalls,
			outputTokens: progress.outputTokens,
			...(final ? { final: true } : {}),
		};
		for (const recipient of runtime.connections)
			if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.progress", payload });
	};
	// Session-cumulative counters at turn start; progress reports the delta for THIS turn.
	let baseline: { toolCalls: number; outputTokens: number } | undefined;
	let polling = false;
	const pollProgress = async () => {
		if (polling || !options.sessionPort.progress) return;
		polling = true;
		try {
			const snapshot = await options.sessionPort.progress({
				sessionId: input.sessionId,
				repo: join(options.config.home, "workspace"),
			});
			if (!snapshot) return;
			baseline ??= snapshot;
			lastKnown = {
				toolCalls: Math.max(lastKnown.toolCalls, snapshot.toolCalls - baseline.toolCalls),
				outputTokens: Math.max(lastKnown.outputTokens, snapshot.outputTokens - baseline.outputTokens),
			};
		} finally {
			polling = false;
		}
	};
	void pollProgress();
	const heartbeat = setInterval(() => {
		void pollProgress().then(() => {
			if (tailActivitySeen) emitProgress(lastKnown);
		});
	}, intervalMs);
	const endProgress = () => {
		if (ended) return;
		ended = true;
		clearInterval(heartbeat);
		emitProgress(lastKnown, true);
	};

	const onFrame = async ({ frame, sessionId }: PersonaTailFrameInput) => {
		if (ended) return;
		tailActivitySeen = true;
		if (frame.assistantText && !frame.steerEcho) {
			lastKnown = {
				toolCalls: lastKnown.toolCalls,
				outputTokens: lastKnown.outputTokens + Math.ceil(frame.assistantText.length / 4),
			};
			try {
				const decision = interimSpeech.admit(frame.assistantText, Date.now(), { toolCallsSoFar: lastKnown.toolCalls });
				if (!decision.deliver) console.error(`gateway mid-work speech suppressed (${turnId}, ${decision.reason}).`);
				else
					deliverAssistantText(frame.assistantText, frame.eventId ? { sessionId, eventId: frame.eventId } : undefined);
			} catch (error) {
				console.error(`gateway intermediate delivery failed (${turnId}): ${diagnostic(error)}`);
			}
		}
		const reportedTools = frame.payload.toolCalls;
		const reportedTokens = frame.payload.outputTokens;
		lastKnown = {
			toolCalls:
				typeof reportedTools === "number" && Number.isFinite(reportedTools)
					? Math.max(lastKnown.toolCalls, reportedTools)
					: frame.payload.toolCallStarted === true || (/tool/i.test(frame.rawKind) && frame.rawKind !== "tool_activity")
						? lastKnown.toolCalls + 1
						: lastKnown.toolCalls,
			outputTokens:
				typeof reportedTokens === "number" && Number.isFinite(reportedTokens)
					? Math.max(lastKnown.outputTokens, reportedTokens)
					: lastKnown.outputTokens,
		};
		emitProgress(lastKnown);
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
			if (lastDeliveredRaw !== text) deliverAssistantText(text);
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
				const notice = runtime.delivery.prepare(turnId, origin, failureNotice);
				if (notice) {
					runtime.delivery.markInflight(notice.deliveryId as string);
					broadcastDelivery(runtime, notice);
				}
			}
		} finally {
			endProgress();
		}
	};

	return {
		text: turnText,
		systemPreamble,
		...(effectiveModel ? { effectiveModel } : {}),
		onFrame,
		onTerminal,
		onFailure,
		onStall: ({ elapsedMs }) =>
			console.error(`gateway persona turn stalled (${turnId}) after ${elapsedMs}ms; retaining status reconciliation.`),
	};
}

function settleWindowFor(row: InboundMessageRow, config: GatewayConfig): number {
	const origin = JSON.parse(row.origin_ref_json) as { platform?: string; conversationId?: string };
	if (origin.platform === "loopback") return 0;
	const channel =
		config.channels?.[`${origin.platform}:${origin.conversationId}`] ??
		(origin.platform === "discord" ? config.channels?.[origin.conversationId ?? ""] : undefined);
	return channel?.settleWindowMs ?? config.settleWindowMs ?? 2_000;
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

/**
 * Session-context grounding (live finding: without it the persona could not
 * tell which conversation it was in and imported other origins' memory as if
 * it had been said here).
 */
function currentConversationNotice(origin: OriginRef, engagement?: { mentioned?: boolean }): string {
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
		...(origin.kind !== "dm" && origin.kind !== "loopback"
			? [
					engagement?.mentioned
						? "You were explicitly addressed here: reply."
						: "You were NOT addressed: you are listening in on a room. Unless this message clearly needs you or adds real value for you to answer, reply with exactly [SILENT] and nothing else — that suppresses delivery while the message stays recorded. Do not respond to every message.",
				]
			: []),
		// The third reply mode: acknowledge without speaking. Kept next to the silence
		// guidance because the persona chooses between exactly these three shapes.
		...(origin.platform === "discord" || origin.platform === "telegram"
			? [
					`Reaction replies: start your reply with [REACT:<emoji>] to react to the message that triggered this turn, or [REACT:<emoji>@<message id>] to react to a specific message. With nothing after the token you acknowledge with a reaction and say nothing; text after the token is sent as well. Emoji ${origin.platform} can actually deliver: ${reactionAllowlistDescription(origin.platform)}. At most ${REACTIONS_PER_TURN_CAP} reactions per turn and ${REACTIONS_PER_MESSAGE_CAP} per message.`,
				]
			: []),
	].join("\n");
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
	origin: { readonly platform: string; readonly kind: string; readonly conversationId: string },
	config: GatewayConfig,
	engagement: unknown,
): boolean {
	return decideEngagement(origin, engagement as Parameters<typeof decideEngagement>[1], config).engaged;
}
