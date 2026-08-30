import { unlink } from "node:fs/promises";
import {
	CAPABILITIES,
	type ChatMessagePayload,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
	HANDOFF_DEPTH_CAP,
	type HandoffDigestEntry,
	handoffOriginLabel,
	type HandoffProvenance,
	isPlatformMessageId,
	parseHandoffReply,
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
import { MemoryClosureQueue } from "../memory/closure";
import { initializeMemory } from "../memory/doctrine";
import { searchMemory } from "../memory/retrieve";
import { validateMemory } from "../memory/validator";
import { MonitorPropagator } from "../monitors/propagate";
import { MonitorRegistry } from "../monitors/registry";
import { MonitorRuntime } from "../monitors/runtime";
import { backupDatabase, integrityDatabase } from "../ops/backup";
import { RuntimeCycleProjector } from "../ops/cycle";
import type { GjcPort } from "../orchestrator/gjc-client";
import { formatFailureNotice } from "../orchestrator/rebind";
import { buildSessionBootstrap, type SessionBootstrap } from "../persona/bootstrap";
import { PersonaLoader } from "../persona/persona";
import type { GatewayDatabase, InboundMessageRow, MonitorEventStage } from "../store/db";
import { DeliveryLedger } from "../store/ledger";
import { KeyedQueue } from "./keyed-queue";
import { dispatchHandoff, inboundHandoffProvenance } from "./handoff";
import { composeSpeakerLabel, composeTurnHeader } from "./speaker";

/**
 * Durable lane-job persistence for delegated work (issue #10).
 *
 * Every `work.run` call is one attempt against a job whose identity outlives
 * the turn: the attempt receipt, end state, failure code, and repository
 * checkpoints land in SQLite before the reply is produced, so a gateway
 * restart or a reaped turn still leaves an auditable trail. Corrupt stored
 * state fails the verb loudly instead of being silently replaced.
 *
 * Boundary note: this surface drives gjc via spawn-per-turn (`--resume`), which
 * has no broker op accounting to poll - so attempts carry gateway-local opRefs
 * and end states, while broker-driven op receipts/status reconciliation remain
 * the G001 library surface for sdk-session lanes.
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

/**
 * Completed turns per epoch before the session is rotated. Every `gjc --resume`
 * replays the whole transcript, so an unrotated busy origin gets slower forever;
 * durable memory (daily capture + recall) carries continuity across epochs.
 */
const SESSION_TURN_LIMIT = 50;

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
	/** Per-process CLI overrides, reapplied on every live reload so they survive it. */
	readonly overrides?: ConfigOverrides;
	/** Test seam for chat.progress throttling; production uses the 15s defaults. */
	readonly progress?: { readonly firstAfterMs?: number; readonly intervalMs?: number };
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
	console.error(
		`gateway config reload (${trigger}) ok; applied=[${result.changed.join(",")}] restart-required=[${result.restartRequired.join(",")}] ignored=[${result.ignored.join(",")}]`,
	);
	return result;
}

interface Runtime {
	/**
	 * The live config. Mutable on purpose: SIGHUP and the reload verb republish it
	 * here, and every per-request read goes through it, so a reloadable field
	 * takes effect on the next turn without a restart.
	 */
	config: GatewayConfig;
	readonly delivery: DeliveryService;
	readonly persona: PersonaLoader;
	readonly connections: Set<Connection>;
	readonly memory: MemoryClosureQueue;
	readonly registry: MonitorRegistry;
	readonly monitors: MonitorPropagator;
	readonly monitorRuntime: MonitorRuntime;
	readonly reconcileTimer: ReturnType<typeof setInterval>;
	readonly contextMaintenanceTimer: ReturnType<typeof setInterval>;
	readonly turns: KeyedQueue;
	/** Per-turn / per-message reaction caps shared by chat.react and the reply-token path. */
	readonly reactions: ReactionBudget;
	/** Accepted-but-not-yet-dispatched inbound messages, keyed by message id. */
	readonly inbound: Map<string, InboundContext>;
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
			console.error(
				`gateway config reload (SIGHUP) crashed: ${error instanceof Error ? error.message : String(error)}`,
			),
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
			for (const connection of runtime.connections)
				connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			listener.stop(true);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			// In-flight turn drains still touch the database; close it under them and
			// their completion bookkeeping crashes ("Cannot use a closed database").
			await runtime.turns.settle();
			await runtime.monitorRuntime.stop();
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
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
	let stopPromise: Promise<void> | undefined;
	// The stdio daemon gets the SAME reload trigger as the Unix server: an
	// operator (or supervisor) may signal either form, and deployment.md claims
	// SIGHUP for a running gateway without qualifying the transport.
	const onHup = () =>
		void applyConfigReload(runtime, options, "SIGHUP").catch((error: unknown) =>
			console.error(
				`gateway config reload (SIGHUP) crashed: ${error instanceof Error ? error.message : String(error)}`,
			),
		);
	process.on("SIGHUP", onHup);
	const stop = (reason = "shutdown requested") => {
		if (stopPromise) return stopPromise;
		stopping = true;
		stopPromise = (async () => {
			process.off("SIGHUP", onHup);
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			await runtime.turns.settle();
			await runtime.monitorRuntime.stop();
			connection.close();
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
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
			`gateway memory settle failed during shutdown; intents remain durable for next boot: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
}

function createRuntime(options: GatewayServerOptions): Runtime {
	const connections = new Set<Connection>();
	// A turn killed mid-flight leaves its claimed message stranded; recover before serving.
	const recovered = options.database.inboundRecoverProcessing();
	console.error(`gateway recovered ${recovered} inbound message(s) stranded in processing.`);
	const delivery = new DeliveryService(new DeliveryLedger(options.database));
	const registry = new MonitorRegistry(options.database);
	const memory = new MemoryClosureQueue(options.database, options.config.home);
	const monitors = new MonitorPropagator({
		database: options.database,
		registry,
		gjc: options.gjc,
		memory,
		delivery,
		ownerTarget: options.config.ownerTarget,
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
	const reconcileTimer = setInterval(() => void monitors.reconcile(), 60_000);
	options.database.contextMaintain();
	const contextMaintenanceTimer = setInterval(
		() => {
			try {
				options.database.contextMaintain();
			} catch (error) {
				console.error(`gateway context maintenance failed: ${error instanceof Error ? error.message : String(error)}`);
			}
		},
		60 * 60 * 1000,
	);
	return {
		config: options.config,
		delivery,
		persona: options.persona ?? new PersonaLoader(options.config.home),
		connections,
		memory,
		registry,
		monitors,
		monitorRuntime,
		reconcileTimer,
		contextMaintenanceTimer,
		turns: new KeyedQueue(),
		reactions: new ReactionBudget(),
		cycle: new RuntimeCycleProjector(options.database, memory),
		inbound: new Map(),
	};
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
				throw new ProtocolError("invalid_params", error instanceof Error ? error.message : "invalid backup path");
			}
			return;
		}
		case "work.run": {
			// First-class delegated work: a named worker gjc session in the coding
			// register, bound to a caller-chosen cwd, serialized per worker name and
			// resumable across calls (the persona's hand-rolled subsession spawning
			// kept losing the reply body — this returns it directly).
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
			const result = await runtime.turns.run(sessionKey, async () => {
				const turnOptions = { ...(workCwd ? { cwd: workCwd } : {}), codingRegister: true };
				const epoch = options.database.getSessionRecord(sessionKey)?.epoch ?? 0;
				const { sessionId } = await options.gjc.ensureSession(sessionKey, epoch, turnOptions);
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
					const reply = await options.gjc.sendTurn(sessionId, workText, undefined, undefined, turnOptions);
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
					const failureMessage = error instanceof Error ? error.message : String(error);
					// The gateway's own inactivity reaper produces the deadline-kill
					// shape: the ATTEMPT ends, the job stays continuable. Anything
					// else is a plain attempt failure.
					const reaped = /made no progress|timed out/.test(failureMessage);
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
						error: error instanceof Error ? error.message : String(error),
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
	if (params.text === "/new" || params.text === "/reset") {
		// Session resets are commands: in group surfaces they obey the mention
		// allowlist, or any room member could wipe the persona's conversation state.
		const allowlist = runtime.config.mentionAllowlist;
		const authorId = (params.engagement as { authorId?: unknown } | undefined)?.authorId;
		if (
			origin.platform !== "loopback" &&
			origin.kind !== "dm" &&
			allowlist &&
			allowlist.length > 0 &&
			(typeof authorId !== "string" || !allowlist.includes(authorId))
		) {
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { turnId: null, engaged: false },
			});
			return;
		}
		const floorAt = new Date().toISOString();
		let discardedInbound: string[] = [];
		options.database.withTransaction(() => {
			options.database.bumpEpoch(key, JSON.stringify(origin));
			options.database.contextSetFloor(key, floorAt);
			discardedInbound = options.database.inboundDiscardBefore(key, floorAt);
		});
		for (const messageId of discardedInbound) runtime.inbound.delete(messageId);
		// An explicit reset is the manual form of a rebind, so it also restores the
		// automatic rebind budget: otherwise an origin that spent its cap would stay
		// capped even after the operator did exactly what the notice asked for.
		options.gjc.forgetRebinds(key);
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
	// Persist before dispatch: the insert is the acceptance boundary. A message that arrives
	// while a turn for the same origin is in flight stays durable and is drained afterwards
	// instead of being dropped outright (five real owner messages were lost that way).
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
	await drainOrigin(key, messageId, connection, options, runtime);
}
// One origin == one gjc session: serialize turns per origin key so a burst of inbound
// messages can never race two `gjc --resume` processes on one session. The drain runs
// inside that serialization, and inboundClaimNext is the atomic hand-off, so a message
// enqueued mid-turn is dispatched exactly once — either by the in-flight drain or by its
// own queued slot.
async function drainOrigin(
	key: string,
	ownMessageId: string,
	fallback: Connection,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<void> {
	let ownFailure: unknown;
	await runtime.turns.run(key, async () => {
		for (let row = options.database.inboundClaimNext(key); row; row = options.database.inboundClaimNext(key)) {
			const resetFloor = options.database.contextFloorAt(key);
			if (resetFloor && row.received_at <= resetFloor) {
				runtime.inbound.delete(row.message_id);
				options.database.inboundComplete(row.message_id);
				continue;
			}
			// Debounce: a burst of messages becomes ONE turn carrying the whole diff.
			// The window is per-channel configurable; newer arrivals during the wait
			// are folded into this batch, with the newest message as the trigger.
			// The window counts from the message's ARRIVAL, not from claim time: a
			// message that already waited behind an earlier turn has served its
			// debounce, and sleeping again inside the serialized drain was adding
			// flat latency to every drained batch (live gajaeway-play finding).
			const debounceMs = debounceFor(row, runtime.config);
			const waitedMs = Date.now() - Date.parse(row.received_at);
			const remainingMs = Number.isFinite(waitedMs) ? Math.max(0, debounceMs - waitedMs) : debounceMs;
			if (remainingMs > 0) await Bun.sleep(remainingMs);
			const batch = [row];
			for (let more = options.database.inboundClaimNext(key); more; more = options.database.inboundClaimNext(key))
				batch.push(more);
			try {
				await runInboundTurn(batch, fallback, options, runtime);
			} catch (error) {
				// The originating request reports its own failure; a drained message has no
				// live requester left, so its failure only belongs in the daemon log.
				if (batch.some((member) => member.message_id === ownMessageId)) ownFailure = error;
				else
					console.error(
						`gateway inbound turn failed (${batch[batch.length - 1]?.message_id}): ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
					);
			} finally {
				for (const member of batch) {
					try {
						options.database.inboundComplete(member.message_id);
					} catch (error) {
						// One failed row must not strand the rest; startup recovery re-queues stragglers.
						console.error(
							`gateway inbound completion failed (${member.message_id}): ${error instanceof Error ? error.message : String(error)}`,
						);
					}
				}
			}
		}
	});
	if (ownFailure) throw ownFailure;
}

function debounceFor(row: InboundMessageRow, config: GatewayServerOptions["config"]): number {
	const origin = JSON.parse(row.origin_ref_json) as { platform?: string; conversationId?: string };
	if (origin.platform === "loopback") return 0;
	const channel =
		config.channels?.[`${origin.platform}:${origin.conversationId}`] ??
		(origin.platform === "discord" ? config.channels?.[origin.conversationId ?? ""] : undefined);
	return channel?.debounceMs ?? config.debounceMs ?? 0;
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
async function runInboundTurn(
	batch: readonly InboundMessageRow[],
	fallback: Connection,
	options: GatewayServerOptions,
	runtime: Runtime,
): Promise<void> {
	// The newest message triggers the turn; older batch members are part of the
	// unread diff. Their live requesters (if any) already got their responses.
	const row = batch[batch.length - 1] as InboundMessageRow;
	for (const member of batch) if (member !== row) runtime.inbound.delete(member.message_id);
	const context = runtime.inbound.get(row.message_id);
	runtime.inbound.delete(row.message_id);
	const connection = context?.connection ?? fallback;
	const turnId = context?.turnId ?? crypto.randomUUID();
	// Absent for a delivery recovered after a restart, which is why that case
	// degrades to text-only rather than speaking into a stale conversation.
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
				channelLabel?: string;
				serverLabel?: string;
				replyTo?: {
					messageId?: string;
					authorName?: string;
					fromSelf?: boolean;
					excerpt?: string;
				};
			})
		: undefined;
	const speaker = composeSpeakerLabel(engagement);
	// "channel | server" when the platform labels both (e.g. "#playground-ko | GAJAE").
	const place =
		[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") ||
		`${origin.platform} ${origin.kind} ${origin.conversationId}`;
	// Compose the turn: everything said in this conversation since the persona's
	// last reply (the read-cursor diff), then the triggering message with speaker
	// attribution — so the persona always reads "the messages above".
	let turnText = userText;
	let contextMessageIds: readonly string[] = [];
	let contextOmissionRevision = 0;
	// Provenance when this very turn was handed to us by another origin's session
	// (issue #72). A relayed row is not a platform message: it consumes no read
	// cursor, gets no speaker header (nobody said it in this room), and its chain
	// is what bounds any further handoff from here.
	const relayed = inboundHandoffProvenance(engagement);
	// Source material for a bounded digest if THIS turn decides to hand work on.
	const digestEntries: HandoffDigestEntry[] = [];
	if (nonLoopback && !relayed) {
		const prepared = options.database.contextWindow(key, row.message_id);
		const unread = prepared.rows;
		contextMessageIds = [...prepared.selectedMessageIds, row.message_id];
		contextOmissionRevision = prepared.omissionRevision;
		for (const entry of unread)
			digestEntries.push({
				at: entry.received_at,
				author: `${entry.author_name ?? "unknown"} (author:${entry.author_id ?? "?"}, msg:${entry.message_id})`,
				text: entry.body,
			});
		digestEntries.push({
			at: row.received_at,
			author: `${speaker || engagement?.authorName || "unknown"} (author:${engagement?.authorId ?? "?"}, msg:${row.message_id})`,
			text: userText,
		});
		const lines = unread.map(
			(entry) =>
				`- [${entry.received_at}] ${entry.author_name ?? "unknown"} (author:${entry.author_id ?? "?"}, msg:${entry.message_id}): ${entry.body.slice(0, 1000)}`,
		);
		// Truncation is stated, never silent: a persona that cannot see it was given
		// a partial view will treat the oldest surviving line as the beginning of the
		// conversation.
		const omitted = prepared.expiredCount + prepared.truncatedCount;
		const omittedRange =
			prepared.omittedOldestAt && prepared.omittedNewestAt
				? `; timestamps ${prepared.omittedOldestAt}..${prepared.omittedNewestAt}`
				: "";
		const droppedNote =
			omitted > 0
				? `[${omitted} older unread message(s) omitted: ${prepared.expiredCount} expired outside floor ${prepared.effectiveFloor}, ${prepared.truncatedCount} truncated by the newest-${prepared.rows.length} window${omittedRange}]\n`
				: "";
		const header = lines.length
			? `[Unread messages in this conversation since your last reply]\n${droppedNote}${lines.join("\n")}\n\n`
			: droppedNote
				? `${droppedNote}\n`
				: "";
		turnText = `${header}${speaker ? `${composeTurnHeader({ speaker, place, authorId: engagement?.authorId, messageId: row.message_id, engagement })}\n` : ""}${userText}`;
	}
	// A loopback console turn and a relayed turn have no read-cursor diff, but a
	// handoff from here still needs SOME source material or the digest would be a
	// bare "(no source messages available)".
	if (digestEntries.length === 0)
		digestEntries.push({
			at: row.received_at,
			author: relayed ? `relayed from ${relayed.sourceOriginKey}` : (speaker || "requester"),
			text: userText,
		});
	// One handoff per turn: the first `[HANDOFF:<target>]` wins and every later one
	// is reported in the source room, because a dropped handoff is worse than none.
	let handoffRequest: { readonly target: string; readonly body: string } | undefined;
	const extraHandoffTargets: string[] = [];
	let text: string;
	// Human-sized chat: the persona may split one message into several short parts
	// with a line containing exactly [BREAK]; each part ships as its own delivery.
	// Long agentic turns also produce SEVERAL assistant messages (one per model
	// step); each is delivered the moment it completes instead of after process
	// exit, so a multi-minute turn talks while it works (live gajaeway-play
	// finding: turns showed nothing but "working…" until the very end).
	const deliveredParts: string[] = [];
	let assistantDeliveryStarted = false;
	let reactionTokensSeen = false;
	const maxTurnParts = 10;
	const deliverAssistantText = (rawMessage: string) => {
		// Handoff reply mode (fourth mode, issue #72): a message whose FIRST LINE is
		// `[HANDOFF:<target>]` is not a message at all, so it is never delivered here
		// — not even on loopback, where delivering it would print control syntax at
		// the console. It is executed after the turn, once, outside the stream.
		const handoff = parseHandoffReply(rawMessage);
		if (handoff) {
			if (handoffRequest) extraHandoffTargets.push(handoff.target);
			else handoffRequest = handoff;
			return;
		}
		if (!nonLoopback) return;
		let message = rawMessage;
		// Reaction reply mode (third mode next to text and silence, parsed per delivered
		// assistant message so streamed intermediates carry it too): a message may open
		// with [REACT:<emoji-or-name>] tokens, optionally targeting one message with
		// `@<platform message id>`. With nothing left after the tokens the message
		// acknowledges with a reaction and ships no text. Parsing is all-or-nothing: a
		// malformed or non-allowlisted token yields undefined here, and the message is
		// delivered verbatim as text — a bad token can cost the reaction, never the reply.
		const reactionReply = parseReactionReply(message);
		if (reactionReply) {
			reactionTokensSeen = true;
			for (const wanted of reactionReply.reactions) {
				// Untargeted tokens react to the message that triggered this turn; that id is
				// the platform's own message id whenever the adapter supplied one.
				// A platform that cannot express this emoji would swallow the acknowledgement:
				// skip it loudly rather than queue a delivery that can only ever fail.
				if (!platformSupportsReaction(origin.platform, wanted.emojiName)) {
					console.error(
						`gateway reaction skipped for ${key}: ${origin.platform} cannot react with ${wanted.emoji} (${wanted.emojiName})`,
					);
					continue;
				}
				const targetMessageId = wanted.targetMessageId ?? row.message_id;
				const rejection = runtime.reactions.claim({ turnId, originKey: key, targetMessageId, emoji: wanted.emoji });
				if (rejection) {
					// Never a silent no-op: an owner who does not see the reaction can find the
					// reason in the daemon log.
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
		// Which deliveries actually happen is decided BEFORE the loop, because the
		// audio has to ride the last one. Deciding inside the loop cannot: a part
		// may be dropped for an empty body, and the part budget may cut the loop
		// off early, so "the last element of `parts`" is not "the last delivery".
		const planned: Array<{ readonly body: string; readonly replyTo?: string }> = [];
		for (const part of parts) {
			if (planned.length >= maxTurnParts) break;
			// Reply-threading: a part may open with [REPLY:<platform message id>] to
			// answer a specific message; mentions are plain <@author id> in the text.
			const replyMatch = part.match(/^\[REPLY:([^\]\s]+)\]\s*/);
			const body = replyMatch ? part.slice(replyMatch[0].length).trim() : part;
			if (!body) continue;
			planned.push({ body, ...(replyMatch?.[1] ? { replyTo: replyMatch[1] } : {}) });
		}
		// A spoken turn is answered in both modalities, and the whole reply is spoken
		// ONCE rather than once per part. Built from the PLANNED bodies so the audio
		// never reads out text the part budget dropped.
		const spoken = spokenReply(
			planned.map((step) => step.body),
			voiceTurn,
		);
		for (let index = 0; index < planned.length; index++) {
			if (deliveredParts.length >= maxTurnParts) return;
			const step = planned[index] as { body: string; replyTo?: string };
			const payload = runtime.delivery.prepare(crypto.randomUUID(), origin, step.body, step.replyTo);
			if (!payload) continue;
			deliveredParts.push(step.body);
			assistantDeliveryStarted = true;
			// Indexed, not compared by value: two identical parts made `part ===
			// parts.at(-1)` true for both and the reply was spoken twice, billed
			// twice, and talked over itself.
			const isLast = index === planned.length - 1;
			broadcastDelivery(runtime, isLast && spoken !== "" ? { ...payload, voiceText: spoken } : payload);
		}
	};
	// Long turns announce liveness instead of dying: throttled chat.progress events
	// let adapters render a "working…" status while the persona runs. A heartbeat
	// timer keeps the status ticking every interval even when the gjc stream is
	// silent (e.g. a long tool run producing no events).
	const startedAt = Date.now();
	const firstAfterMs = options.progress?.firstAfterMs ?? 10_000;
	const intervalMs = options.progress?.intervalMs ?? 10_000;
	let lastProgressAt = 0;
	let lastKnown = { toolCalls: 0, outputTokens: 0 };
	let progressAnnounced = false;
	const emitProgress = (progress: { toolCalls: number; outputTokens: number }, final = false) => {
		lastKnown = progress;
		const now = Date.now();
		// A final event is never throttled: it is what tells an adapter to remove the
		// temporary "working" message. Throttling it would leave that message behind.
		if (!final && (now - startedAt < firstAfterMs || now - lastProgressAt < intervalMs)) return;
		// Nothing was ever announced, so there is no status to clear: stay quiet rather
		// than emitting a lone terminal event for a fast turn.
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
	const heartbeat = setInterval(() => emitProgress(lastKnown), intervalMs);
	const runTurn = async () => {
		const bootstraps = new Map<number, SessionBootstrap>();
		const preambleForEpoch = async (epoch: number): Promise<string> => {
			const state = options.database.getSessionBootstrap(key);
			const bootstrap =
				!state || state.lastBootstrappedEpoch < epoch
					? await buildSessionBootstrap({
							home: runtime.config.home,
							origin,
							epoch,
							engagement,
							config: runtime.config,
						})
					: undefined;
			if (bootstrap) bootstraps.set(epoch, bootstrap);
			return [
				await runtime.persona.systemPreamble(),
				currentConversationNotice(origin, engagement, {
					// Only origins an operator actually declared are offered: an invented
					// target resolves to nothing and costs the room a loud failure.
					aliases: Object.keys(runtime.config.handoffTargets ?? {}).filter(
						(alias) => originKey(validateOriginRef(runtime.config.handoffTargets?.[alias] as OriginRef)) !== key,
					),
					...(relayed ? { relayed } : {}),
				}),
				...(bootstrap ? [bootstrap.text] : []),
				ACTION_GUARD_SYSTEM_NOTICE,
			].join("\n\n");
		};
		const requestedEpoch = options.database.getSessionRecord(key)?.epoch ?? 0;
		const { sessionId } = await options.gjc.ensureSession(key, requestedEpoch);
		// session.create itself may condemn and rebind the requested epoch. The
		// persisted row is the effective binding authority, so the first preamble
		// on that fresh session must use the rebound epoch rather than a stale ID.
		const boundEpoch = options.database.getSessionRecord(key)?.epoch ?? requestedEpoch;
		const preamble = await preambleForEpoch(boundEpoch);
		const result = await options.gjc.sendTurn(sessionId, turnText, preamble, emitProgress, {
			systemPreambleForEpoch: preambleForEpoch,
			onAssistantText: (message) => {
				try {
					deliverAssistantText(message);
				} catch (error) {
					// Delivery bookkeeping must never abort a running turn mid-stream.
					console.error(
						`gateway intermediate delivery failed (${turnId}): ${error instanceof Error ? error.message : String(error)}`,
					);
				}
			},
		});
		const terminalEpoch = options.database.getSessionRecord(key)?.epoch ?? boundEpoch;
		const applied = bootstraps.get(terminalEpoch);
		if (applied)
			options.database.markSessionBootstrapped(key, terminalEpoch, {
				includedSections: applied.includedSections,
				byteCount: applied.byteCount,
				truncated: applied.truncated,
				diagnostics: applied.diagnostics,
			});
		return result;
	};
	try {
		text = await runTurn();
		if (nonLoopback) options.database.contextCommitWindow(key, contextMessageIds, contextOmissionRevision);
	} catch (error) {
		// A prose-only "session not found" failure (gjc emits no structured code
		// for it) is deliberately NOT auto-rebound: #13 mandates exact-code-only
		// classification, and every retry against the same dead key is guaranteed
		// silence. The failure surfaces through #14's structured notice instead,
		// where the operator's remedy — /new — is one message away.
		// Never ghost a platform conversation: a failed turn still produces a visible,
		// ledgered notice (live P1 drill finding: timeouts looked like silent ignores).
		// The notice carries the runtime's OWN code and message (#14): one opaque line
		// cost ~2h of muteness because nobody could tell a poisoned session key from a
		// timeout. The identical string is logged, so a channel transcript is enough
		// to triage without shell access.
		// When intermediate messages already reached the room, the persona visibly
		// spoke; a trailing "[turn failed]" would disavow real replies, so the
		// failure stays in the daemon log only.
		const failureNotice = formatFailureNotice(error);
		console.error(failureNotice);
		// A runtime failure before any assistant delivery leaves selected context
		// unread for retry. Once a real text/reaction delivery has started, retrying
		// the same window could duplicate an answer, so that window is consumed.
		if (nonLoopback && assistantDeliveryStarted)
			options.database.contextCommitWindow(key, contextMessageIds, contextOmissionRevision);
		if (nonLoopback && !assistantDeliveryStarted) {
			const notice = runtime.delivery.prepare(turnId, origin, failureNotice);
			if (notice) {
				runtime.delivery.markInflight(notice.deliveryId as string);
				for (const recipient of runtime.connections)
					if (recipient.negotiated)
						recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload: notice });
			}
		}
		throw error;
	} finally {
		clearInterval(heartbeat);
		// Every exit path from here on must clear the adapter's temporary status: a
		// delivered reply, a failure notice, and - the case this fixes - a turn that
		// ends in a silence token and delivers nothing at all.
		emitProgress(lastKnown, true);
	}
	// Ports without intermediate streaming (and the test stub) only ever produce the
	// final text, so the handoff token has to be recognised here as well — before
	// anything is delivered, captured, or handed to another session.
	if (deliveredParts.length === 0 && !handoffRequest) handoffRequest = parseHandoffReply(text);
	// Handoff (issue #72): this reply is not a message. Bind the target, hand ONE
	// durable inbound event to that origin's queue, and let the target session
	// answer in its own room. The target turn runs nested inside this origin's turn
	// lock, which is safe only because the chain check refuses any target already in
	// the chain — no hop can ever wait on a lock its own call stack holds.
	let handoffNotice: string | undefined;
	if (handoffRequest) {
		const request = handoffRequest;
		const outcome = await dispatchHandoff(
			{
				config: runtime.config,
				database: options.database,
				runTargetTurn: (targetKey, handoffMessageId) =>
					drainOrigin(targetKey, handoffMessageId, fallback, options, runtime),
			},
			{
				target: request.target,
				body: request.body,
				sourceOrigin: origin,
				sourceLabel: nonLoopback ? place : handoffOriginLabel(origin),
				sourceMessageId: row.message_id,
				requester: speaker
					? `${speaker}${engagement?.authorId ? ` (author:${engagement.authorId})` : ""}`
					: relayed
						? `${relayed.requester} (relayed via ${relayed.sourceOriginKey})`
						: "the local console operator",
				requestedAt: row.received_at,
				incomingChain: relayed?.chain ?? [],
				digestEntries,
			},
		);
		handoffNotice = outcome.notice;
		if (outcome.kind !== "relayed")
			console.error(`gateway handoff ${outcome.kind} for ${key} -> "${request.target}": ${outcome.notice}`);
		// A second token in the same turn is never silently dropped: it is refused in
		// the source room, on the record.
		if (extraHandoffTargets.length > 0)
			handoffNotice = `${handoffNotice}\n[handoff failed] one_handoff_per_turn: also asked to hand off to ${extraHandoffTargets.map((target) => `"${target}"`).join(", ")}; those were NOT handed off. Hand off once per turn.`;
	}
	const replyText = handoffNotice ?? (deliveredParts.length > 0 ? deliveredParts.join("\n") : text);
	options.database.withTransaction(() => {
		options.database.updateActivity(key, JSON.stringify(origin));
		options.database.addRecall(
			key,
			JSON.stringify(origin),
			`user: ${userText.slice(0, 500)}\nassistant: ${replyText.slice(0, 500)}`,
		);
		// Session growth bound: every `gjc --resume` replays the whole transcript,
		// so turns get slower forever on a busy origin. Rotate the epoch after a
		// fixed number of turns; durable memory and recall provide continuity.
		if (options.database.incrementTurnCount(key) >= SESSION_TURN_LIMIT) {
			options.database.bumpEpoch(key, JSON.stringify(origin));
			console.error(`gateway rotated session epoch for ${key} after ${SESSION_TURN_LIMIT} turns.`);
		}
	});
	// The source room gets the pointer (or the loud failure) and NOTHING of the work
	// itself: keeping the thread out of the wrong room is the whole point. Memory
	// records the same pointer, so this session's own transcript says where the work
	// went instead of re-carrying it.
	if (handoffNotice) {
		if (nonLoopback) {
			const pointer = runtime.delivery.prepare(turnId, origin, handoffNotice);
			if (pointer) broadcastDelivery(runtime, pointer);
		} else
			connection.write({
				v: PROFILE_VERSION,
				type: "event",
				event: "chat.message",
				...(context ? { id: context.requestId } : {}),
				payload: { turnId, origin, role: "assistant", text: handoffNotice, final: true },
			});
		runtime.memory.enqueue({
			kind: "daily_capture",
			originRefJson: JSON.stringify(origin),
			userText: speaker ? `${speaker} @ ${place}: ${userText}` : userText,
			replyText: handoffNotice,
		});
		return;
	}
	// Spec fact 22: a reply that is exactly a silence token means the persona chose not to
	// speak. The observation is already recorded above, so nothing is delivered and no daily
	// capture is written. This is what makes an `open` channel usable: the persona can read
	// every message in the room without answering all of them.
	if (deliveredParts.length === 0 && isSilenceToken(text)) return;
	if (!nonLoopback) {
		connection.write({
			v: PROFILE_VERSION,
			type: "event",
			event: "chat.message",
			...(context ? { id: context.requestId } : {}),
			payload: { turnId, origin, role: "assistant", text, final: true },
		});
		// Durable intent is persisted synchronously; closure work deliberately does not delay delivery.
		runtime.memory.enqueue({ kind: "daily_capture", originRefJson: JSON.stringify(origin), userText, replyText: text });
		return;
	}
	// Memory carries who spoke and where, so canonicalization keeps provenance.
	const capturedUser = speaker ? `${speaker} @ ${place}: ${userText}` : userText;
	// Ports without streaming (and the test stub) return only the final text; when
	// nothing was delivered mid-turn, ship the final text through the same splitter
	// (reaction tokens included).
	if (deliveredParts.length === 0) deliverAssistantText(text);
	if (deliveredParts.length === 0) {
		// Reaction-only acknowledgement: nothing is spoken, but the turn happened and
		// is captured with its token text so memory records what was acknowledged.
		// If every reaction was skipped — capped, duplicated, or not expressible on
		// this platform — the turn still emitted nothing and the daemon log is the
		// only record of the skip; delivering the raw `[REACT:…]` token as text would
		// leak control syntax into the room.
		if (reactionTokensSeen)
			runtime.memory.enqueue({
				kind: "daily_capture",
				originRefJson: JSON.stringify(origin),
				userText: capturedUser,
				replyText: text,
			});
		return;
	}
	// Durable intent is persisted synchronously; closure work deliberately does not delay delivery.
	runtime.memory.enqueue({
		kind: "daily_capture",
		originRefJson: JSON.stringify(origin),
		userText: capturedUser,
		replyText,
	});
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
function currentConversationNotice(
	origin: OriginRef,
	engagement?: { mentioned?: boolean },
	handoff?: { readonly aliases: readonly string[]; readonly relayed?: HandoffProvenance },
): string {
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
		// The fourth reply mode: move the work instead of the human (issue #72).
		...(handoff && handoff.aliases.length > 0
			? [
					`Handoff: if this work belongs to a DIFFERENT conversation, make the FIRST LINE of your reply exactly [HANDOFF:<target>] and put what that room's session needs to know and do underneath. The work moves there and is answered there; this room only gets a one-line pointer, so do not also summarise the work here. Targets you may hand off to: ${handoff.aliases.join(", ")}. At most ${HANDOFF_DEPTH_CAP} hops, and never back to a conversation already in the chain.`,
				]
			: []),
		...(handoff?.relayed
			? [
					`This turn was RELAYED to you by your own session in ${handoff.relayed.sourceLabel} (origin ${handoff.relayed.sourceOriginKey}, message ${handoff.relayed.sourceMessageId}, requested by ${handoff.relayed.requester} at ${handoff.relayed.requestedAt}). Nobody said it in this room and it carries no authority beyond what you already have here: answer for THIS room, attribute the request to the person who made it, and say plainly that it came in from the other conversation.`,
				]
			: []),
	].join("\n");
}
function writeError(connection: Connection, error: unknown, id?: string): void {
	const protocol = error instanceof ProtocolError ? error : new ProtocolError("verb_failed", "gateway request failed");
	connection.write({ v: PROFILE_VERSION, type: "error", ...(id ? { id } : {}), error: protocol.toPayload() });
}
