import { createHash } from "node:crypto";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { readFile, rename, unlink, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import {
	CAPABILITIES,
	type ChatMessagePayload,
	type ChatProgressActivity,
	describeChatPlatforms,
	type EngagementContext,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type HelloPayload,
	isChatPlatform,
	isPlatformMessageId,
	isShutupToken,
	isSilentOutput,
	LOOPBACK_ORIGIN,
	type MonitorEventOperatorResult,
	type MonitorEventRecord,
	type MonitorRecord,
	type MonitorScheduleProjection,
	negotiate,
	OPS_CYCLE_DEFAULT_LIMIT,
	OPS_CYCLE_MAX_LIMIT,
	OrderedFrameWriter,
	type OriginRef,
	OUTBOUND_FILE_MAX_BYTES,
	OUTBOUND_FILES_PER_TURN_CAP,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	parseFileReply,
	parseOriginKey,
	parseReactionReply,
	platformSupportsFiles,
	platformSupportsReaction,
	REACTIONS_PER_MESSAGE_CAP,
	REACTIONS_PER_TURN_CAP,
	type ReactionRef,
	type RequestFrame,
	reactionAllowlistDescription,
	resolveReactionEmoji,
	validateOriginRef,
	WORK_JOBS_DEFAULT_LIMIT,
	WORK_JOBS_MAX_LIMIT,
} from "@gajae-gateway/protocol";
import { parseLaneJobRecord } from "@gajae-gateway/subsession";
import {
	type ConfigOverrides,
	DEFAULT_SESSIONS_MAX_LIVE_HOSTS,
	type GatewayConfig,
	type ReloadResult,
	reloadConfig,
} from "../config";
import { DeliveryService } from "../delivery/delivery";
import { ReactionBudget } from "../delivery/reaction-budget";
import { type KevShadowInput, kevShadowEnabled, recordKevShadow } from "../engagement/kev-shadow";
import {
	BotAudienceTurnGuard,
	decideEngagement,
	resolveBotAudienceLimits,
	resolveChannelPolicy,
	threadFollowUpEngaged,
	threadMuteAddressed,
	threadMuteKey,
} from "../engagement/policy";
import { isAbstentionNarration, preTurnSkip, speechGateApplies } from "../engagement/speech-gate";
import {
	FunctionalReadiness,
	FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
	FUNCTIONAL_READINESS_WINDOW_MS,
} from "../functional-readiness";
import { ACTION_GUARD_SYSTEM_NOTICE } from "../guard/action-guard";
import { SECRET_GUARD_SYSTEM_NOTICE, SecretGuard, secretSourcesFromEnv } from "../guard/secret-guard";
import { leaseHeld } from "@gajae-gateway/home";
import { autolinkCorpus } from "../memory/autolink";
import { MemoryClosureQueue } from "../memory/closure";
import { DEFAULT_DEBLOAT_BYTE_THRESHOLD } from "../memory/debloat";
import { initializeMemory, memoryRoot } from "../memory/doctrine";
import { searchMemory } from "../memory/retrieve";
import { validateMemory } from "../memory/validator";
import { MONITOR_REPORT_MESSAGE_ID_PREFIX, MonitorPropagator } from "../monitors/propagate";
import { MonitorRegistry } from "../monitors/registry";
import { MonitorRuntime } from "../monitors/runtime";
import { DEFAULT_CRON_CATCH_UP, nextCronFire } from "../monitors/triggers/cron";
import { backupDatabase, integrityDatabase } from "../ops/backup";
import { RuntimeCycleProjector } from "../ops/cycle";
import { OriginSilentAlerter } from "../ops/origin-silent";
import { DEFAULT_SLO_DAYS, evaluateSlo, type SloRecorder } from "../ops/slo";
import { defaultPidAlive, type GlobalGjcClient } from "../orchestrator/broker";
import { ghCheckStatusClient } from "../orchestrator/gh-checks";
import { LaneGovernor } from "../orchestrator/lane-governor";
import {
	PERSONA_RESTART_DRAIN_MAX_WAIT_MS,
	PERSONA_RESTART_DRAIN_WAIT_MS,
	type PersonaBindHoldInput,
	type PersonaFailureInput,
	PersonaSessionManager,
	type PersonaSteerInput,
	type PersonaTailFrameInput,
	type PersonaTerminalInput,
	type PersonaTurnLifecycle,
	type PersonaTurnSettledInput,
	type PersonaTurnStartInput,
	UNRESOLVED_HOLD_ESCALATED,
} from "../orchestrator/persona-session";
import {
	formatFailureChatNotice,
	formatFailureNotice,
	sanitizeDiagnostic,
	sanitizeDiagnosticDetail,
} from "../orchestrator/rebind";
import type { SessionPort } from "../orchestrator/session-port";
import {
	deterministicHeldPostDeliveryId,
	deterministicInterimDeliveryId,
	deterministicPostDeliveryId,
	deterministicTerminalDeliveryId,
} from "../orchestrator/tail-runner";
import { getServiceDirectory, laneLastCommits, utf8Prefix, WorkLaneManager } from "../orchestrator/work-lane";
import { buildSessionBootstrap } from "../persona/bootstrap";
import { PersonaLoader } from "../persona/persona";
import type { Store } from "../store";
import {
	editedOriginalId,
	type InboundMessageRow,
	type TerminalUnlinkedReason,
	terminalDeliveryIds,
} from "../store/groups/inbox";
import type { MonitorEventStage } from "../store/groups/monitors";
import type { ExpiredDeliveryRow } from "../store/groups/outbox";
import { deriveActivity } from "./activity";
import { ATTACHMENT_SCOPE_NOTICE, redactHistoricalAttachments } from "./attachment-scope";
import {
	buildHandoffDigest,
	extendHandoffChain,
	extractRoutingBlocks,
	extractWakeLines,
	extractWatchLines,
	type HandoffProvenance,
	type HandoffReply,
	handoffMessageId,
	parseWatchTtl,
	readHandoffProvenance,
	renderHandoffFailure,
	renderHandoffPointer,
	renderHandoffTurn,
	renderWakeFailure,
	renderWakeTurn,
	renderWatchedInputTurn,
	renderWatchFailure,
	renderWatchRegistered,
	resolveHandoffTarget,
	WATCH_REASON_MAX_LENGTH,
	type WakeLine,
	type WatchLine,
	wakeMessageId,
	watchRegistrationId,
	watchWakeMessageId,
} from "./handoff";
import { InterimSpeechGate } from "./interim-speech";
import { applyModelCommand, listModelChoices, ProviderModelCatalog } from "./model-command";
import { checkOutboundFile, deterministicFileDeliveryId } from "./outbound-files";
import { type PanelResponseKind, PermissionPanels } from "./permission-panels";
import { RestartNotices, restartNoticeText } from "./restart-notice";
import { composeSpeakerLabel, composeTurnHeader } from "./speaker";

/** Persona tail stall heartbeat; well under the 120s stallTimeoutMs so alarms land within one interval of the threshold. */
const DEFAULT_STALL_CHECK_INTERVAL_MS = 5_000;
const DEFAULT_DELIVERY_SWEEP_INTERVAL_MS = 15_000;
const RETENTION_SWEEP_INTERVAL_MS = 15 * 60 * 1000;
const CONNECTION_SETTLE_TIMEOUT_MS = 5_000;
/** /restart allows the 15s persona drain plus ordered monitor, connection, and storage teardown. */
const RESTART_HARD_EXIT_MS = 35_000;
/** Bounded grace for a superseded supervisor to finish its current service-manager command. */
const OWNER_RESTART_SUPERVISOR_CANCEL_WAIT_MS = 30_000;
/** Owner restart gives the supervisor the rest of the drain maximum to finish or settle. */
const OWNER_RESTART_SUPERVISOR_WAIT_MS = PERSONA_RESTART_DRAIN_MAX_WAIT_MS - OWNER_RESTART_SUPERVISOR_CANCEL_WAIT_MS;
/** Bounded grace for a persisted restart drain on boot; once it settles or times out and its supervisor has exited, a non-indeterminate drain is released and cleared. */
const PERSISTED_RESTART_DRAIN_BOOT_TIMEOUT_MS = 30_000;
/** How often boot re-checks whether the restart-stack supervisor that owns an inherited drain is still alive. */
const PERSISTED_RESTART_SUPERVISOR_POLL_MS = 1_000;
/** Thread history shown to a freshly started session: everything (humans, bots, self) in the last 24h, capped. */
const RECENT_HISTORY_WINDOW_MS = 24 * 60 * 60_000;
const RECENT_HISTORY_MAX = 300;
// 16 turns is where the shadow's separation stopped improving (help on answer-me
// messages: 0.465 context-free, 0.574 at 3, 0.668 at 8, 0.729 at 16) and it costs
// ~1.4k tokens / ~520 ms on the local judge, which nothing waits for.
const KEV_SHADOW_CONTEXT_TURNS = 16;
const KEV_SHADOW_CONTEXT_WINDOW_MS = 6 * 60 * 60_000;
const RESTART_EXIT_CODE = 75;
const AGENTS_MD_MISSING_DIGEST = "missing";
const UPDATED_AGENTS_MD_HEADING =
	"## AGENTS.md (updated since this session started; supersedes the project-context copy loaded at session start)";

interface AgentsMdSnapshot {
	readonly digest: string;
	readonly text?: string;
}

async function readWorkspaceAgentsMd(home: string): Promise<AgentsMdSnapshot> {
	try {
		const content = await readFile(join(home, "workspace", "AGENTS.md"));
		return {
			digest: createHash("sha256").update(content).digest("hex"),
			text: content.toString("utf8"),
		};
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		return { digest: AGENTS_MD_MISSING_DIGEST };
	}
}

function updatedAgentsMdSection(snapshot: AgentsMdSnapshot): string {
	return `${UPDATED_AGENTS_MD_HEADING}\n${snapshot.text ?? "[AGENTS.md was deleted from the workspace after this session started.]"}`;
}

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
	readonly database: Store;
	/** Production and tests both inject the sole broker-backed turn transport. */
	readonly sessionPort: SessionPort;
	/** Canonical persona workspace (symlinks resolved); defaults to `<home>/workspace`. */
	readonly workspace?: string;
	readonly startedAt?: string;
	readonly onStop?: () => void | Promise<void>;
	readonly persona?: PersonaLoader;
	/** Durable SLO recorder started only after the Unix socket is listening. */
	readonly slo?: SloRecorder;
	/** Only gateway-owned SDK commands and relays are closed after runtime work drains. */
	readonly broker?: GlobalGjcClient;
	/** Test seam: `/model set` provider models; production asks gjc through the broker (`gjc --list-models`). */
	readonly listProviderModels?: () => Promise<readonly string[]>;
	/** Per-process CLI overrides, reapplied on every live reload so they survive it. */
	readonly overrides?: ConfigOverrides;
	/** Test seam for chat.progress throttling; production uses the 15s defaults. */
	readonly progress?: { readonly firstAfterMs?: number; readonly intervalMs?: number };
	/** Test seam: how /restart ends the process after the ordered stop (default process.exit). */
	readonly exitProcess?: (code: number) => void;
	/** Test seam for the owner restart's wait on an active stack supervisor. */
	readonly ownerRestartSupervisorWaitMs?: number;
	/** Test seam for the owner restart's wait for a superseded service command to settle. */
	readonly ownerRestartSupervisorCancelWaitMs?: number;
	/** Test seam for the owner restart's persona-turn delivery drain. */
	readonly ownerRestartDrainWaitMs?: number;
	/** Test seam for the persona tail stall heartbeat; production uses the 5s default. */
	readonly stallCheckIntervalMs?: number;
	/** Test seam for bounded persona recovery deadlines; production uses the system clock. */
	readonly personaNow?: () => number;
	/** Test seam for periodic delivery recovery; production sweeps every 15s. */
	readonly deliverySweepIntervalMs?: number;
	/** Mid-work speech gating configuration (issue #71). */
	readonly interimSpeech?: { readonly maxPerTurn?: number; readonly minGapMs?: number };
	/** Test seam: outbound secret redaction; production reads its sources from the process env and the broker agent dir. */
	readonly secretGuard?: SecretGuard;
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
	runtime.personaSessions.setHostLimits(result.config.sessions);
	options.broker?.setMaxPoolSize(result.config.sessions?.maxLiveHosts ?? DEFAULT_SESSIONS_MAX_LIVE_HOSTS);
	console.info(
		`gateway config reload (${trigger}) ok; applied=[${result.changed.join(",")}] restart-required=[${result.restartRequired.join(",")}] ignored=[${result.ignored.join(",")}]`,
	);
	return result;
}

/**
 * Prune expired memory operations and cap the map size to prevent unbounded growth.
 * Extracted for testability; called periodically during operation and in tests.
 */
export function pruneExpiredMemoryOps(memoryOps: Map<string, MemoryOpStatus>, nowMs: number): void {
	// Remove expired operations (past their 24h TTL)
	for (const [opRef, op] of Array.from(memoryOps.entries())) {
		if (op.status !== "pending" && new Date(op.expireAt).getTime() < nowMs) {
			memoryOps.delete(opRef);
		}
	}

	// Cap the map size to prevent unbounded memory growth
	if (memoryOps.size > 10_000) {
		const entries = Array.from(memoryOps.entries())
			.filter(([, op]) => op.status !== "pending")
			.sort(([, a], [, b]) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime());
		for (const [opRef] of entries.slice(0, memoryOps.size - 5_000)) {
			memoryOps.delete(opRef);
		}
	}
}

/**
 * Get or create a pending memory operation of a specific kind.
 * Returns opRef and created flag: created=true means a new operation was just created
 * and the caller should start the sweep; created=false means it's reusing an existing pending op.
 * Ensures kind-specific deduplication: audit pending ops never dedup autolink requests and vice versa.
 */
export function getOrCreatePendingMemoryOp(
	memoryOps: Map<string, MemoryOpStatus>,
	kind: "audit" | "autolink",
): { opRef: string; created: boolean } {
	// Check for existing pending operation of the SAME kind
	for (const [opRef, op] of Array.from(memoryOps.entries())) {
		if (op.status === "pending") {
			// Distinguish by opRef prefix: gw-ma- (audit), gw-ml- (autolink)
			const opKind = opRef.startsWith("gw-ma-") ? "audit" : "autolink";
			if (opKind === kind) {
				return { opRef, created: false }; // Reuse existing pending operation of same kind
			}
		}
	}

	// No pending operation of this kind; create new one
	const prefix = kind === "audit" ? "gw-ma" : "gw-ml";
	const opRef = `${prefix}-${crypto.randomUUID()}`;
	const started = new Date().toISOString();
	const expireAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
	memoryOps.set(opRef, { status: "pending", startedAt: started, expireAt });
	return { opRef, created: true };
}

interface MemoryOpStatus {
	status: "pending" | "completed" | "failed";
	result?: unknown;
	error?: string;
	elapsedMs?: number;
	startedAt: string;
	expireAt: string; // For pruning old operations
}

function startMemoryOperation(runtime: Runtime, opRef: string, work: () => Promise<unknown>): void {
	let task: Promise<void>;
	task = Promise.resolve()
		.then(work)
		.then((result) => {
			const operation = runtime.memoryOps.get(opRef);
			if (operation) {
				operation.status = "completed";
				operation.result = result;
				operation.elapsedMs = Math.max(0, Date.now() - Date.parse(operation.startedAt));
			}
		})
		.catch((error: unknown) => {
			const operation = runtime.memoryOps.get(opRef);
			if (operation) {
				operation.status = "failed";
				operation.error = diagnostic(error);
				operation.elapsedMs = Math.max(0, Date.now() - Date.parse(operation.startedAt));
			}
		})
		.finally(() => runtime.memoryOperationTasks.delete(task));
	runtime.memoryOperationTasks.add(task);
}

interface Runtime {
	/** The live config republished by SIGHUP/reload. */
	config: GatewayConfig;
	/** Canonical persona workspace shared by session binding and outbound file checks. */
	readonly workspace: string;
	readonly delivery: DeliveryService;
	readonly persona: PersonaLoader;
	readonly sessionPort: SessionPort;
	readonly personaSessions: PersonaSessionManager;
	readonly readiness: FunctionalReadiness;
	/** `/model set` autocomplete's cached provider models; absent without a broker. */
	readonly providerModels?: ProviderModelCatalog;
	/** Ordered shutdown, wired after construction; owner `/restart` uses it. */
	stop?: (reason?: string) => Promise<void>;
	/** Mirrors producer quiescence so restart paths can defer to a live supervisor. */
	stopping: boolean;
	/** Owner restart currently waiting to take over a cancelled stack drain. */
	pendingOwnerRestartId?: string;
	readonly connections: Set<Connection>;
	readonly memory: MemoryClosureQueue;
	/** Tracked memory operations (autolink, audit) keyed by opRef. */
	readonly memoryOps: Map<string, MemoryOpStatus>;
	/** Running memory operations are drained before gateway shutdown. */
	readonly memoryOperationTasks: Set<Promise<void>>;
	readonly registry: MonitorRegistry;
	readonly monitors: MonitorPropagator;
	readonly monitorRuntime: MonitorRuntime;
	readonly reconcileTimer: ReturnType<typeof setInterval>;
	readonly deliverySweepTimer: ReturnType<typeof setInterval>;
	readonly panelExpirySweepTimer: ReturnType<typeof setInterval>;
	readonly stallTimer: ReturnType<typeof setInterval>;
	readonly contextMaintenanceTimer: ReturnType<typeof setInterval>;
	readonly memoryOpsCleanupTimer: ReturnType<typeof setInterval>;
	readonly stopRetention: () => void;
	readonly stopBrokerGenerationListener?: () => void;
	/** Per-turn / per-message reaction caps shared by chat.react and the reply-token path. */
	readonly reactions: ReactionBudget;
	/** Configurable consecutive-turn budget plus the always-on runaway rate limit for bot authors. */
	readonly botAudienceTurns: BotAudienceTurnGuard;
	/** Accepted-but-not-yet-dispatched inbound messages, keyed by message id. */
	readonly inbound: Map<string, InboundContext>;
	/** Requests stop() quiesces; shutdown and restart-drain barriers are deliberately excluded. */
	readonly requests: Set<Promise<void>>;
	/** Supervisor socket that owns each restart drain; disconnect cancels an uncommitted drain. */
	readonly restartDrainOwners: Map<string, Connection>;
	/** Terminal reply deliveries remain a restart barrier after their actor settles. */
	readonly restartTerminalDeliveryIds: Set<string>;
	/** Read-only runtime-cycle projection (ops.cycle); owns no writes. */
	readonly cycle: RuntimeCycleProjector;
	/** Admission cap and retirement for `work.run` lanes. */
	readonly lanes: LaneGovernor;
	readonly work: WorkLaneManager;
	readonly permissionPanels: PermissionPanels;
	/** Redacts secrets from every chat-bound frame at the connection writer; not configurable from chat or config.json. */
	readonly secretGuard: SecretGuard;
}

function cancelRestartDrainsForConnection(runtime: Runtime, connection: Connection): void {
	for (const [restartId, owner] of runtime.restartDrainOwners) {
		if (owner !== connection) continue;
		runtime.restartDrainOwners.delete(restartId);
		if (restartReceiptKeepsDrain(runtime.config.home, restartId)) continue;
		void releaseOrTransferRestartDrain(runtime, restartId).catch((error: unknown) =>
			console.error(`restart drain cancellation failed id=${restartId} detail=${diagnostic(error)}`),
		);
	}
}

function restartReceiptKeepsDrain(home: string, restartId?: string): boolean {
	try {
		const receipt = JSON.parse(readFileSync(join(home, "restart-stack.json"), "utf8")) as {
			id?: unknown;
			state?: unknown;
			drainPrepared?: unknown;
		};
		return (
			receipt.id === restartId &&
			(receipt.state === "indeterminate" || (receipt.state === "running" && receipt.drainPrepared === true))
		);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		// A corrupt receipt cannot prove the service-manager operation settled.
		return true;
	}
}

function persistedRestartDrainId(home: string): string | undefined {
	try {
		const receipt = JSON.parse(readFileSync(join(home, "restart-stack.json"), "utf8")) as {
			id?: unknown;
			state?: unknown;
			drainPrepared?: unknown;
		};
		if (typeof receipt.id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(receipt.id)) return undefined;
		if (receipt.state === "indeterminate" || (receipt.state === "running" && receipt.drainPrepared === true))
			return receipt.id;
		return undefined;
	} catch {
		return undefined;
	}
}

type RestartDrainReleaseOutcome = "cancelled" | "transferred" | "not_active";

async function releaseOrTransferRestartDrain(runtime: Runtime, restartId: string): Promise<RestartDrainReleaseOutcome> {
	const ownerRestartId = runtime.pendingOwnerRestartId;
	if (ownerRestartId && ownerRestartId !== restartId) {
		if (runtime.personaSessions.transferRestartDrain(restartId, ownerRestartId)) return "transferred";
	}
	return (await runtime.personaSessions.cancelRestartDrain(restartId)) ? "cancelled" : "not_active";
}

async function waitForRestartTerminalDelivery(runtime: Runtime, terminalDeliveryId: string | null): Promise<void> {
	const deliveryIds = terminalDeliveryId === null ? [] : terminalDeliveryIds(terminalDeliveryId);
	if (deliveryIds.length === 0) return;
	for (const id of deliveryIds) runtime.restartTerminalDeliveryIds.add(id);
	let probeFailureLogged = false;
	while (!runtime.stopping) {
		try {
			const deliveries = runtime.delivery.getMany(deliveryIds);
			const settled =
				deliveries.length === deliveryIds.length &&
				deliveries.every((delivery) => delivery.state === "confirmed" || delivery.state === "expired");
			for (const delivery of deliveries)
				if (delivery.state === "confirmed" || delivery.state === "expired")
					runtime.restartTerminalDeliveryIds.delete(delivery.deliveryId);
			if (settled) return;
		} catch (error) {
			if (!probeFailureLogged) {
				console.error(`restart terminal delivery probe failed: ${diagnostic(error)}`);
				probeFailureLogged = true;
			}
		}
		if (!runtime.personaSessions.restartDrainActive) return;
		await Bun.sleep(25);
	}
}

async function waitForRestartTerminalDeliveries(runtime: Runtime, database: Store, waitMs: number): Promise<boolean> {
	const deadline = Date.now() + waitMs;
	let probeFailureLogged = false;
	try {
		for (const id of database.outbox.unsettledTerminalDeliveryIds()) runtime.restartTerminalDeliveryIds.add(id);
		const deliveries = runtime.delivery.getMany([...runtime.restartTerminalDeliveryIds]);
		for (const delivery of deliveries)
			if (delivery.state === "confirmed" || delivery.state === "expired")
				runtime.restartTerminalDeliveryIds.delete(delivery.deliveryId);
	} catch (error) {
		console.error(`restart terminal delivery index failed: ${diagnostic(error)}`);
		return false;
	}
	while (!runtime.stopping) {
		const deliveryIds = [...runtime.restartTerminalDeliveryIds];
		if (deliveryIds.length === 0) return true;
		try {
			const deliveries = runtime.delivery.getMany(deliveryIds);
			for (const delivery of deliveries)
				if (delivery.state === "confirmed" || delivery.state === "expired")
					runtime.restartTerminalDeliveryIds.delete(delivery.deliveryId);
			if (deliveries.length === deliveryIds.length && deliveries.every((delivery) => delivery.state === "confirmed"))
				return true;
		} catch (error) {
			if (!probeFailureLogged) {
				console.error(`restart terminal delivery probe failed: ${diagnostic(error)}`);
				probeFailureLogged = true;
			}
		}
		const remainingMs = deadline - Date.now();
		if (remainingMs <= 0) return false;
		await Bun.sleep(Math.min(25, remainingMs));
	}
	return false;
}

function notifyOwnerRestartCancelled(
	connection: Connection,
	runtime: Runtime,
	requestId: string,
	origin: OriginRef,
	reason = "a persona reply was not confirmed as delivered before the drain deadline",
): void {
	const payload = {
		turnId: crypto.randomUUID(),
		origin,
		role: "assistant" as const,
		text: `Restart cancelled: ${reason}.`,
		final: true,
	};
	if (origin.platform === "loopback") {
		connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", id: requestId, payload });
		return;
	}
	const delivery = runtime.delivery.prepare(payload.turnId, origin, payload.text);
	if (!delivery) return;
	runtime.delivery.markInflight(delivery.deliveryId as string);
	broadcastDelivery(runtime, delivery);
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
		runtime.stopping = true;
		stopPromise = (async () => {
			process.off("SIGHUP", onHup);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.deliverySweepTimer);
			clearInterval(runtime.panelExpirySweepTimer);
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			clearInterval(runtime.memoryOpsCleanupTimer);
			runtime.stopRetention();
			// Stop accepting new sockets first, but keep existing sockets alive. Then
			// quiesce every admitted producer before taking the final writer snapshot.
			listener.stop(false);
			runtime.stopBrokerGenerationListener?.();
			runtime.personaSessions.beginDrain();
			await runtime.work.stop();
			await Promise.all([...runtime.requests]);
			await Promise.all([...runtime.memoryOperationTasks]);
			await runtime.monitorRuntime.stop();
			// Stop persona admission before draining both producers; monitor results
			// can still persist inbound rows, but they remain queued for the next boot.
			await Promise.all([runtime.personaSessions.drain(), runtime.monitors.drain()]);
			await runtime.personaSessions.stop();
			// The broker has no recovery policy; it is only stopped after all current
			// producers and monitor/tail-like runtime work have drained.
			await options.broker?.stop();
			for (const connection of runtime.connections)
				connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			await Promise.all(
				[...runtime.connections].map((connection) => settleConnection(connection, CONNECTION_SETTLE_TIMEOUT_MS)),
			);
			listener.stop(true);
			await settleMemory(runtime);
			await options.onStop?.();
		})();
		return stopPromise;
	};
	runtime.stop = stop;
	listener = Bun.listen<{ connection: Connection; writer: OrderedFrameWriter; decoder: TextDecoder }>({
		unix: options.config.socketPath,
		socket: {
			open(socket) {
				const writer = new OrderedFrameWriter(
					{ write: (bytes) => socket.write(bytes), close: () => socket.end() },
					(error) => console.error(`gateway socket write failed: ${diagnostic(error)}`),
				);
				const decoder = new TextDecoder();
				const connection: Connection = {
					decoder: new FrameDecoder(),
					negotiated: false,
					write: (frame) => writer.write(runtime.secretGuard.guardFrame(frame)),
					close: () => writer.close(),
					settle: () => writer.settled(),
				};
				socket.data = { connection, writer, decoder };
				runtime.connections.add(connection);
			},
			data(socket, data) {
				const connection = socket.data.connection;
				try {
					for (const frame of connection.decoder.feed(socket.data.decoder.decode(data, { stream: true }))) {
						const task = handleFrame(connection, frame, options, runtime, stop, () => stopping);
						if (
							frame.type === "request" &&
							(frame.verb === "gateway.shutdown" || frame.verb === "gateway.restart.prepare")
						)
							continue;
						runtime.requests.add(task);
						void task.then(
							() => runtime.requests.delete(task),
							() => runtime.requests.delete(task),
						);
					}
				} catch (error) {
					if (
						error instanceof ProtocolError &&
						(error.code === "malformed_frame" || error.code === "payload_too_large")
					) {
						console.error(`gateway frame decode error: ${diagnostic(error)}`);
					}
					writeError(connection, error);
				}
			},
			drain(socket) {
				socket.data.writer.drain();
			},
			close(socket) {
				runtime.work.detachWaiters(socket.data.connection);
				if (!stopping) cancelRestartDrainsForConnection(runtime, socket.data.connection);
				socket.data.writer.close();
				runtime.connections.delete(socket.data.connection);
			},
			error(socket, error) {
				runtime.work.detachWaiters(socket.data.connection);
				if (!stopping) cancelRestartDrainsForConnection(runtime, socket.data.connection);
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
	const pendingStdoutWrites = new Set<Promise<void>>();
	const connection: Connection = {
		decoder: new FrameDecoder(),
		negotiated: false,
		write: (frame) => {
			const pending = new Promise<void>((resolve, reject) => {
				process.stdout.write(encodeFrame(runtime.secretGuard.guardFrame(frame)), (error) => {
					if (error) reject(error);
					else resolve();
				});
			}).catch((error: unknown) => {
				console.error(`gateway stdio write failed: ${diagnostic(error)}`);
			});
			pendingStdoutWrites.add(pending);
			void pending.finally(() => pendingStdoutWrites.delete(pending));
		},
		close: () => process.stdin.pause(),
		settle: async () => {
			const deadline = Date.now() + CONNECTION_SETTLE_TIMEOUT_MS;
			while (pendingStdoutWrites.size > 0) {
				const remainingMs = deadline - Date.now();
				if (remainingMs <= 0) break;
				let timer: ReturnType<typeof setTimeout> | undefined;
				const settled = await Promise.race([
					Promise.all([...pendingStdoutWrites]).then(() => true),
					new Promise<boolean>((resolve) => {
						timer = setTimeout(() => resolve(false), remainingMs);
					}),
				]);
				if (timer !== undefined) clearTimeout(timer);
				if (!settled) break;
			}
			if (pendingStdoutWrites.size > 0)
				console.error(`gateway stdio output flush timed out pending=${pendingStdoutWrites.size}`);
		},
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
		runtime.stopping = true;
		stopPromise = (async () => {
			process.off("SIGHUP", onHup);
			process.stdin.off("data", onData);
			process.stdin.off("end", onEnd);
			clearInterval(runtime.reconcileTimer);
			clearInterval(runtime.deliverySweepTimer);
			clearInterval(runtime.panelExpirySweepTimer);
			clearInterval(runtime.stallTimer);
			clearInterval(runtime.contextMaintenanceTimer);
			clearInterval(runtime.memoryOpsCleanupTimer);
			runtime.stopRetention();
			// Announce the start of stdio shutdown; the stream remains open during teardown.
			connection.write({ v: PROFILE_VERSION, type: "event", event: "gateway.stopping", payload: { reason } });
			runtime.stopBrokerGenerationListener?.();
			runtime.personaSessions.beginDrain();
			await runtime.work.stop();
			// Same producer quiescence as the Unix server: in-flight stdio requests are
			// tracked and awaited before persona/tail/broker teardown.
			await Promise.all([...runtime.requests]);
			await Promise.all([...runtime.memoryOperationTasks]);
			await runtime.monitorRuntime.stop();
			// Stop persona admission before draining both producers; monitor results
			// can still persist inbound rows, but they remain queued for the next boot.
			await Promise.all([runtime.personaSessions.drain(), runtime.monitors.drain()]);
			await runtime.personaSessions.stop();
			await options.broker?.stop();
			connection.close();
			await settleMemory(runtime);
			await options.onStop?.();
			await connection.settle();
		})();
		return stopPromise;
	};
	runtime.stop = stop;
	const onEnd = () => {
		runtime.work.detachWaiters(connection);
		if (!stopping) cancelRestartDrainsForConnection(runtime, connection);
	};
	const onData = (data: Buffer) => {
		try {
			for (const frame of connection.decoder.feed(data.toString())) {
				const task = handleFrame(connection, frame, options, runtime, stop, () => stopping);
				// Same guard as the Unix server: shutdown awaits stop(), and the
				// restart-drain barrier owns its own long wait rather than stop's request set.
				if (frame.type === "request" && (frame.verb === "gateway.shutdown" || frame.verb === "gateway.restart.prepare"))
					continue;
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

/** A row whose stored origin ref names another conversation is logged once, not on every sweep. */
const idleResetMismatchesLogged = new Set<string>();

/**
 * Rotate idle persona sessions like `/new`. This drops the old GJC context while
 * leaving the next turn to bind a fresh session. Host retirement is different:
 * it ends only the host and preserves the session history. Loopback and monitor
 * origins, active turns, and rows with no bound GJC session are never reset.
 */
export async function sweepIdleSessions(
	runtime: Pick<Runtime, "config" | "personaSessions">,
	database: Pick<Store, "bindings">,
	now: number = Date.now(),
): Promise<readonly string[]> {
	const idleMs = runtime.config.sessions?.idleResetMs;
	if (idleMs === undefined) return [];
	const rotated: string[] = [];
	for (const row of database.bindings.sessionIdentityRows()) {
		if (!row.origin_ref_json || row.gjc_session_id === "") continue;
		let origin: OriginRef;
		try {
			origin = validateOriginRef(JSON.parse(row.origin_ref_json) as OriginRef);
		} catch {
			continue;
		}
		if (origin.platform === "loopback" || origin.platform === "monitor") continue;
		const lastActivity = Date.parse(row.last_activity_at ?? row.created_at);
		if (!Number.isFinite(lastActivity) || now - lastActivity < idleMs) continue;
		const key = originKey(origin);
		if (key !== row.origin_key) {
			if (!idleResetMismatchesLogged.has(row.origin_key)) {
				idleResetMismatchesLogged.add(row.origin_key);
				console.error(`session_idle_reset_skipped origin=${row.origin_key} ref=${key} reason=origin_ref_mismatch`);
			}
			continue;
		}
		if (runtime.personaSessions.state(key) === "turn-running") continue;
		try {
			await runtime.personaSessions.reset(key, JSON.stringify(origin));
			console.error(`session_idle_reset origin=${key} epoch=${row.epoch} idleMs=${now - lastActivity}`);
			rotated.push(key);
		} catch (error) {
			console.error(`session_idle_reset_failed origin=${key} detail=${diagnostic(error)}`);
		}
	}
	return rotated;
}

function createRuntime(options: GatewayServerOptions): Runtime {
	const sessionPort = options.sessionPort;
	const connections = new Set<Connection>();
	const restartDrainOwners = new Map<string, Connection>();
	const restartTerminalDeliveryIds = new Set<string>();
	const inbound = new Map<string, InboundContext>();
	const delivery = new DeliveryService(options.database);
	const botAudienceTurns = new BotAudienceTurnGuard(options.database.core);
	const registry = new MonitorRegistry(options.database);
	const memory = new MemoryClosureQueue(options.database, options.config.home);
	const readiness = new FunctionalReadiness();
	const broker = options.broker;
	const listProviderModels = options.listProviderModels ?? (broker ? () => broker.listAvailableModels() : undefined);
	const providerModels = listProviderModels ? new ProviderModelCatalog(listProviderModels) : undefined;
	let runtime!: Runtime;
	const workspacePath = options.workspace ?? join(options.config.home, "workspace");
	// gjc rejects a session cwd that is a symlink: `session.create` comes back
	// `operation_failed` and every persona bind fails, while the same directory
	// reached by its real path works. An operator may legitimately point the
	// gateway's workspace at the SSOT directory with a symlink, so resolve it
	// before it reaches gjc.
	const workspace = existsSync(workspacePath) ? realpathSync(workspacePath) : workspacePath;
	const restartDrainId = persistedRestartDrainId(options.config.home);
	// Config reloads replace runtime.config; the notices read whichever is current.
	const currentConfig = (): GatewayConfig => (runtime === undefined ? options.config : runtime.config);
	const restartNotices = new RestartNotices({
		database: options.database,
		delivery,
		enabled: () => currentConfig().restartNotice?.enabled !== false,
		text: (phase) => restartNoticeText(currentConfig().restartNotice, phase),
		broadcast: (payloads) => {
			for (const payload of payloads) {
				delivery.markInflight(payload.deliveryId as string);
				for (const recipient of connections)
					if (recipient.negotiated)
						recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
			}
		},
		...(restartDrainId === undefined ? {} : { inheritedRestartId: restartDrainId }),
	});
	const personaSessions = new PersonaSessionManager({
		database: options.database,
		port: sessionPort,
		instanceId: options.database.instanceId,
		repo: workspace,
		brokerAgentDir: options.broker?.agentDir,
		sessionModel: options.config.model,
		stallTimeoutMs: options.config.stallTimeoutMs,
		...(options.personaNow ? { now: options.personaNow } : {}),
		maxLiveHosts: options.config.sessions?.maxLiveHosts,
		hostIdleRetireMs: options.config.sessions?.idleRetireMs,
		busyStallMs: options.config.sessions?.busyStallMs,
		unresolvedHoldMs: options.config.sessions?.unresolvedHoldMs,
		brokerGeneration: () => options.broker?.generation ?? 0,
		brokerAvailable: () => options.broker?.isAvailable ?? true,
		// biome-ignore lint/style/noNonNullAssertion: broker is non-null when lambda is called
		brokerLiveness: options.broker ? () => options.broker!.judgeLiveness() : undefined,
		onBindHold: ({ originKey, trigger, notice, reason }: PersonaBindHoldInput) => {
			let origin: OriginRef;
			try {
				origin = validateOriginRef(JSON.parse(trigger.origin_ref_json) as OriginRef);
			} catch (error) {
				console.error(`persona bind hold delivery skipped origin=${originKey} detail=${diagnostic(error)}`);
				return;
			}
			if (origin.platform === "loopback") {
				console.warn(notice);
				return;
			}
			const context = runtime.inbound.get(trigger.message_id);
			const deliveryId = deterministicBindHoldDeliveryId(originKey, trigger.message_id, reason);
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
			row.source === "lane_report"
				? undefined
				: (JSON.parse(row.origin_ref_json) as { platform?: string }).platform === "loopback"
					? undefined
					: (editedOriginalId(row.message_id) ?? row.message_id),
		onHeldSteerAccepted: ({ row, abandoned }) => {
			const turnId = runtime.inbound.get(row.message_id)?.turnId;
			runtime.inbound.delete(row.message_id);
			// A steer the runtime recorded after its turn ended still gets its 👀;
			// one closed because its session is gone was never seen, so it gets none.
			if (!abandoned) acknowledgeSteer(runtime, row, turnId);
		},
		onInboundDiscard: (messageIds) => {
			for (const messageId of messageIds) inbound.delete(messageId);
		},
		onReverseRequest: async ({ originKey, triggerAuthorId, tail, input }) => {
			const origin = options.database.bindings.getOriginByKey(originKey);
			await permissionPanels.open({
				origin: origin ? validateOriginRef(origin as unknown as OriginRef) : undefined,
				triggerAuthorId,
				tail,
				request: input,
			});
		},
		onSloEvent: (code, at) => options.slo?.note(code, at),
		onBindFailure: ({ originKey, detail, at }) =>
			readiness.recordFailure({
				category: "session_create",
				code: "persona_bind_failed",
				detail: sanitizeDiagnosticDetail(detail, 200) || "session creation failed",
				origin: originKey,
				at,
			}),
		onSessionCreateFailure: ({ originKey, code, detail, at }) =>
			readiness.recordFailure({
				category: "session_create",
				code,
				detail: sanitizeDiagnosticDetail(detail, 200) || "session attachment failed",
				origin: originKey,
				at,
			}),
		onPreSendFailure: ({ originKey, detail, at }) =>
			readiness.recordFailure({
				category: "turn",
				code: "pre_send_configuration_failed",
				detail: sanitizeDiagnosticDetail(detail, 200) || "pre-send turn configuration failed",
				origin: originKey,
				at,
			}),
		onSessionReady: () => readiness.recordSuccess("session_create"),
		onRestartDrainStarted: (restartId) => restartNotices.restarting(restartId),
		onRestartDrainCancelled: (restartId) => restartNotices.released(restartId),
		onRestartDrainTransferred: (restartId, nextRestartId) => restartNotices.transferred(restartId, nextRestartId),
	});
	if (restartDrainId !== undefined) {
		const stackPath = join(options.config.home, "restart-stack.json");
		const readStack = (): { id?: unknown; state?: unknown; supervisorPid?: unknown } | undefined => {
			try {
				return JSON.parse(readFileSync(stackPath, "utf8")) as {
					id?: unknown;
					state?: unknown;
					supervisorPid?: unknown;
				};
			} catch {
				return undefined;
			}
		};
		if (readStack()?.state === "indeterminate")
			console.warn(`persisted restart drain in indeterminate state on boot id=${restartDrainId}`);
		void (async () => {
			try {
				const outcome = await personaSessions.drainForRestart(restartDrainId, PERSISTED_RESTART_DRAIN_BOOT_TIMEOUT_MS);
				if (outcome !== "timed_out" && outcome !== "drained") return;
				// During a normal restart-stack or update, the supervisor is still restarting dependent services
				// and releases this drain itself once the sequence finishes. Only a drain whose supervisor
				// exited without releasing it, with no update lease left, is orphaned.
				for (;;) {
					const stack = readStack();
					if (stack?.id !== restartDrainId || personaSessions.restartDrainId !== restartDrainId) return;
					const supervisorAlive = typeof stack.supervisorPid === "number" && defaultPidAlive(stack.supervisorPid);
					// An `update` holds its lease across the whole stack restart, including after a supervisor
					// handoff; the lease is a flock, so it also clears when its holder dies.
					if (!supervisorAlive && !(await leaseHeld(options.config.home, "update"))) break;
					await Bun.sleep(PERSISTED_RESTART_SUPERVISOR_POLL_MS);
				}
				// An indeterminate drain stays held for operator recovery (`ops restart-stack --recover`)
				// and is reported as `restart_drain_held` readiness.
				if (readStack()?.state === "indeterminate") return;
				if (!(await personaSessions.cancelRestartDrain(restartDrainId))) return;
				try {
					const receipt = JSON.parse(readFileSync(stackPath, "utf8")) as Record<string, unknown>;
					delete receipt.id;
					const temporary = `${stackPath}.${process.pid}.tmp`;
					await writeFile(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
					await rename(temporary, stackPath);
					console.info(
						`orphaned persisted restart drain released on boot outcome=${outcome} id=${restartDrainId} supervisorPid=dead updateLease=released`,
					);
				} catch (error) {
					console.error(`persisted restart drain clear failed id=${restartDrainId} detail=${diagnostic(error)}`);
				}
			} catch (error: unknown) {
				console.error(`persisted restart drain failed id=${restartDrainId} detail=${diagnostic(error)}`);
			}
		})();
	}
	const permissionPanels = new PermissionPanels({
		deliver: (payload) => {
			if (options.database.transaction((tx) => runtime.delivery.persistInTransaction(tx, payload))) {
				if (payload.deliveryId) runtime.delivery.markInflight(payload.deliveryId);
				broadcastDelivery(runtime, payload);
			}
		},
	});
	const monitors = new MonitorPropagator({
		database: options.database,
		registry,
		sessionPort,
		memory,
		delivery,
		ownerTarget: options.config.ownerTarget,
		notifyInbound: (originKey) => personaSessions.notifyInbound(originKey),
		contextFailureRollThreshold: options.config.monitorContextFailureRollThreshold,
		model: options.config.model,
		serviceTier: options.config.serviceTier,
		repo: workspace,
		memoryDebloat: {
			root: memoryRoot(options.config.home),
			thresholdBytes: options.config.memory?.debloatByteThreshold ?? DEFAULT_DEBLOAT_BYTE_THRESHOLD,
		},
		onReadinessFailure: ({ code, detail, originKey, at }) =>
			readiness.recordFailure({
				category: "monitor_delivery",
				code,
				detail,
				origin: originKey,
				at,
			}),
		onReadinessSuccess: () => readiness.recordSuccess("monitor_delivery"),
		// AC7: the ONE production compaction seam. Native compaction runs through the
		// broker-bound SessionPort, whose authenticated control receipt is the only
		// affirmative compaction observation (logged by the TailRunner); the monitor
		// propagator never re-implements compaction locally.
		compaction: {
			run: async (sessionId) =>
				await sessionPort.runCompaction({
					sessionId,
					repo: workspace,
					originKey: `monitor/session/${sessionId}`,
				}),
		},
		emit: (payload) => broadcastMonitorEvent(runtime, payload),
		deliver: (payload) => {
			for (const connection of connections)
				if (connection.negotiated)
					connection.write({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload });
		},
	});
	const monitorRuntime = new MonitorRuntime(options.config, registry, monitors, {
		catchUp: { ...DEFAULT_CRON_CATCH_UP, ...options.config.monitorCatchUp },
	});
	const lanes = new LaneGovernor({
		database: options.database,
		sessionPort,
		laneRoot: options.config.home,
		maxLanes: options.config.work?.maxLanes,
		idleRetireMs: options.config.work?.idleRetireMs,
	});
	const work = new WorkLaneManager({
		database: options.database,
		port: sessionPort,
		lanes,
		gajaewayHome: options.config.home,
		serviceDirectory: getServiceDirectory(),
		ownerTarget: () => runtime.config.ownerTarget?.origin,
		brokerGeneration: () => options.broker?.generation ?? 0,
		allowNested: () => runtime.config.work?.allowNested === true,
		personaHold: (originKey) => personaSessions.admissionHold(originKey),
		notifyPersona: (originKey) => {
			void personaSessions.notifyInbound(originKey);
		},
		deliverFallback: (payload) => broadcastDelivery(runtime, payload),
		checkStatusClient: ghCheckStatusClient(),
	});
	const originSilent = new OriginSilentAlerter(options.database, () => runtime.config.ownerTarget?.origin);
	const reconcileTimer = setInterval(() => {
		alertSilentOrigins(runtime, originSilent);
		void monitors.reconcile();
		void personaSessions
			.recover()
			.then(async () => {
				await personaSessions.sweepIdleHosts();
				await sweepIdleSessions(runtime, options.database);
			})
			.catch((error: unknown) => console.error(`persona recovery sweep failed: ${diagnostic(error)}`));
		void work
			.recover()
			.then(async () => {
				await lanes.sweep();
				// Also release dead/disowned lanes via reconciliation
				await lanes.retireAllDead();
			})
			.catch((error: unknown) => console.error(`lane recovery/sweep failed: ${diagnostic(error)}`));
	}, 60_000);
	const deliverySweepTimer = setInterval(() => {
		try {
			// The age TTL runs with or without an adapter: an unsettled row must reach
			// a terminal state even while nothing is connected to retry it (#171).
			const connected = [...connections].some((connection) => connection.negotiated);
			const sweep = delivery.sweep(Date.now(), false, connected);
			for (const expired of sweep.expired) reportDeliveryExpired(runtime, options.database, expired, "age");
			for (const payload of sweep.payloads) broadcastDelivery(runtime, payload);
		} catch (error) {
			console.error(`delivery sweep failed: ${diagnostic(error)}`);
		}
	}, options.deliverySweepIntervalMs ?? DEFAULT_DELIVERY_SWEEP_INTERVAL_MS);
	// Expired permission panels are answered as cancelled so gjc never waits on a dead panel.
	const panelExpirySweepTimer = setInterval(() => {
		void permissionPanels
			.sweep()
			.catch((error: unknown) => console.error(`panel expiry sweep failed: ${diagnostic(error)}`));
	}, options.deliverySweepIntervalMs ?? DEFAULT_DELIVERY_SWEEP_INTERVAL_MS);
	// AC6: tail-stall detection is a running-server obligation, not only a
	// generic-request polling side effect. A stall starts status reconciliation;
	// only a still-nonterminal operation after the hard grace is cut off.
	const stallTimer = setInterval(() => {
		try {
			personaSessions.checkStalls();
		} catch (error) {
			console.error(`persona stall check failed: ${diagnostic(error)}`);
		}
	}, options.stallCheckIntervalMs ?? DEFAULT_STALL_CHECK_INTERVAL_MS);
	options.database.transaction((tx) => options.database.inbox.contextMaintain(tx));
	reportTerminalLinkAudit(options.database);
	const contextMaintenanceTimer = setInterval(
		() => {
			try {
				options.database.transaction((tx) => options.database.inbox.contextMaintain(tx));
			} catch (error) {
				console.error(`gateway context maintenance failed: ${diagnostic(error)}`);
			}
			reportTerminalLinkAudit(options.database);
		},
		60 * 60 * 1000,
	);
	// Clean up expired memory operations to prevent unbounded growth (#473)
	const memoryOpsCleanupTimer = setInterval(
		() => {
			try {
				pruneExpiredMemoryOps(runtime.memoryOps, Date.now());
			} catch (error) {
				console.error(`memory operations cleanup failed: ${diagnostic(error)}`);
			}
		},
		5 * 60 * 1000,
	); // Every 5 minutes
	// History GC: bounded batches with a macrotask yield between them so a large
	// backlog never holds the event loop (and the single writer) for long.
	let retentionRunning = false;
	let retentionStopped = false;
	const runRetention = async (): Promise<void> => {
		if (retentionRunning || retentionStopped) return;
		retentionRunning = true;
		try {
			for (let pass = 0; pass < 200 && !retentionStopped; pass++) {
				const result = options.database.sweep();
				const total = Object.values(result.deleted).reduce((sum, n) => sum + n, 0);
				if (total > 0) console.error(JSON.stringify({ retention: result.deleted }));
				if (!result.more) break;
				await new Promise((resolve) => setTimeout(resolve, 50));
			}
			if (!retentionStopped) options.database.core.maintain();
		} catch (error) {
			if (!retentionStopped) console.error(`gateway retention sweep failed: ${diagnostic(error)}`);
		} finally {
			retentionRunning = false;
		}
	};
	const retentionTimer = setInterval(() => void runRetention(), RETENTION_SWEEP_INTERVAL_MS);
	const retentionBootTimer = setTimeout(() => void runRetention(), 30_000);
	const stopRetention = (): void => {
		retentionStopped = true;
		clearInterval(retentionTimer);
		clearTimeout(retentionBootTimer);
	};
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
					// A new broker is the first observable sign of a gjc upgrade; re-check its version.
					if (typeof brokerWithGeneration.refreshVersion === "function")
						void brokerWithGeneration
							.refreshVersion()
							.catch((error: unknown) => console.error(`gjc version refresh failed: ${diagnostic(error)}`));
				})
			: undefined;
	runtime = {
		config: options.config,
		stopping: false,
		workspace,
		delivery,
		persona: options.persona ?? new PersonaLoader(options.config.home),
		sessionPort,
		personaSessions,
		readiness,
		providerModels,
		connections,
		memory,
		registry,
		monitors,
		monitorRuntime,
		reconcileTimer,
		deliverySweepTimer,
		panelExpirySweepTimer,
		stallTimer,
		contextMaintenanceTimer,
		memoryOpsCleanupTimer,
		stopRetention,
		...(stopBrokerGenerationListener ? { stopBrokerGenerationListener } : {}),
		reactions: new ReactionBudget(),
		botAudienceTurns,
		cycle: new RuntimeCycleProjector(options.database, memory, {
			maxLanes: lanes.maxLanes,
			agentDir: options.config.gjc.agentDir,
			home: options.config.home,
			memoryRoot: memoryRoot(options.config.home),
			gjcVersion: () => options.broker?.gjcVersion,
			brokerRespawnChurn: () =>
				typeof options.broker?.respawnChurn === "function" ? options.broker.respawnChurn() : false,
		}),
		lanes,
		work,
		inbound,
		requests: new Set(),
		restartDrainOwners,
		restartTerminalDeliveryIds,
		permissionPanels,
		memoryOps: new Map(),
		memoryOperationTasks: new Set(),
		secretGuard:
			options.secretGuard ??
			new SecretGuard(secretSourcesFromEnv(process.env, options.broker?.agentDir, options.config.home)),
	};
	try {
		restartNotices.resumeAfterBoot();
	} catch (error) {
		console.error(`restart notice recovery failed: ${diagnostic(error)}`);
	}
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
function settleMonitorBatch(database: Store, deliveryId: string, stage: MonitorEventStage): void {
	const delivery = database.outbox.get(deliveryId);
	if (!delivery) return;
	const batchId = delivery.turnId;
	const events = database.monitors.eventsByBatch(batchId);
	if (!events.length) return;
	database.transaction((tx) => {
		for (const event of events) database.monitors.eventSettle(tx, event.event_id, stage as "delivered" | "authored");
	});
}

/**
 * Recent conversation for a fresh session: the last `limit` context rows
 * (consumed or not) plus the persona's own confirmed replies, oldest first. A
 * reply posted here from another conversation ([POST:]) appears once, as its
 * `post/<delivery id>` context row, not also as a confirmed delivery - so this
 * reads context rows WITH posts (the judge path uses recentInbound instead).
 */
function recentConversation(
	store: Store,
	originKey: string,
	conversationId: string,
	limit: number,
	sinceIso: string,
): Array<{ id?: string; at: string; author: string; body: string }> {
	const inbound = store.inbox.recentContext(originKey, limit, sinceIso);
	const floorAt = [store.inbox.contextFloorAt(originKey) ?? "", sinceIso].sort().at(-1) ?? sinceIso;
	const replies = store.outbox
		.confirmedConversation(conversationId, floorAt, limit)
		.map((row) => {
			const text = (JSON.parse(row.payload_json) as { text?: unknown }).text;
			return { at: row.created_at, author: "you", body: typeof text === "string" ? text : "" };
		})
		.filter((row) => row.body.length > 0);
	return [...inbound, ...replies].sort((a, b) => a.at.localeCompare(b.at)).slice(-limit);
}

function confirmAndSettleMonitorDelivery(
	store: Store,
	deliveryId: string,
	stage: "delivered" | "authored",
): "unknown" | "transitioned" | "already_terminal" {
	return store.transaction((tx) => {
		const delivery = store.outbox.get(deliveryId);
		if (!delivery) return "unknown";
		if (delivery.state === "confirmed" || delivery.state === "expired") {
			if (delivery.state === "confirmed") store.monitors.settleForTurn(tx, delivery.turnId, stage);
			return "already_terminal";
		}
		store.outbox.confirm(tx, deliveryId);
		store.monitors.settleForTurn(tx, delivery.turnId, stage);
		return "transitioned";
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
			for (const expired of sweep.expired) reportDeliveryExpired(runtime, options.database, expired, "age");
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

function localCronFireTime(at: Date, timezone: string): string {
	const parts = new Intl.DateTimeFormat("en-CA", {
		timeZone: timezone,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hourCycle: "h23",
	}).formatToParts(at);
	const part = (type: Intl.DateTimeFormatPartTypes): string => {
		const value = parts.find((entry) => entry.type === type)?.value;
		if (!value) throw new Error(`missing ${type} in cron timestamp`);
		return value;
	};
	return `${part("year")}-${part("month")}-${part("day")} ${part("hour")}:${part("minute")}:${part("second")}`;
}

function scheduleProjection(monitor: MonitorRecord, now: Date): MonitorScheduleProjection {
	if (monitor.trigger.kind !== "cron") return { effectiveTimezone: null, nextFireAt: null };
	const timezone = monitor.trigger.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone;
	if (!monitor.enabled) return { effectiveTimezone: timezone, nextFireAt: null };
	const next = nextCronFire(monitor.trigger.schedule, now, timezone);
	return {
		effectiveTimezone: timezone,
		nextFireAt: next ? { local: localCronFireTime(next, timezone), utc: next.toISOString() } : null,
	};
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
					readiness: (() => {
						const drainId = runtime.personaSessions.restartDrainId;
						if (drainId !== undefined) {
							return {
								ready: false,
								state: "degraded" as const,
								windowMs: FUNCTIONAL_READINESS_WINDOW_MS,
								failureThreshold: FUNCTIONAL_READINESS_FAILURE_THRESHOLD,
								reason: `gateway: restart_drain_held (restarts fenced, active personas blocked)`,
								failure: {
									category: "session_create" as const,
									code: "restart_drain_held",
									detail: `restart drain held id=${drainId}; new persona dispatches are blocked`,
									at: new Date().toISOString(),
									consecutiveFailures: 1,
								},
							};
						}
						return runtime.readiness.snapshot();
					})(),
					sessions: { active: options.database.bindings.activeSessionCount },
					delivery: runtime.delivery.status(),
					contextDiff: options.database.inbox.contextDiagnostics(),
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
		case "gateway.restart.prepare": {
			const params = request.params as { restartId?: unknown; waitMs?: unknown } | undefined;
			const restartId = params?.restartId;
			const waitMs = params?.waitMs ?? PERSONA_RESTART_DRAIN_WAIT_MS;
			if (typeof restartId !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(restartId))
				throw new ProtocolError("invalid_params", "restartId must be a non-empty safe identifier");
			if (
				typeof waitMs !== "number" ||
				!Number.isSafeInteger(waitMs) ||
				waitMs < 0 ||
				waitMs > PERSONA_RESTART_DRAIN_MAX_WAIT_MS
			)
				throw new ProtocolError("invalid_params", "waitMs is outside the allowed drain interval");
			if (runtime.pendingOwnerRestartId !== undefined && runtime.pendingOwnerRestartId !== restartId)
				throw new ProtocolError("verb_failed", "restart drain is being superseded by an owner restart");
			const owner = runtime.restartDrainOwners.get(restartId);
			if (owner !== undefined && owner !== connection)
				throw new ProtocolError("verb_failed", "restart drain is already owned by another supervisor");
			if (runtime.personaSessions.restartDrainId !== undefined && runtime.personaSessions.restartDrainId !== restartId)
				throw new ProtocolError("verb_failed", "restart drain ownership is no longer active");
			runtime.restartDrainOwners.set(restartId, connection);
			try {
				const deadline = Date.now() + waitMs;
				let outcome = await runtime.personaSessions.drainForRestart(restartId, waitMs);
				if (
					outcome === "drained" &&
					!(await waitForRestartTerminalDeliveries(runtime, options.database, Math.max(0, deadline - Date.now())))
				)
					outcome = "timed_out";
				if (outcome === "stopped") {
					runtime.restartDrainOwners.delete(restartId);
					throw new ProtocolError("gateway_shutting_down", "gateway stopped before persona turns drained");
				}
				if (outcome === "timed_out")
					throw new ProtocolError(
						"verb_failed",
						"persona turn delivery was not confirmed before the restart drain deadline",
					);
				connection.write({
					v: PROFILE_VERSION,
					type: "response",
					id: request.id,
					result: { ready: true, drained: true },
				});
			} catch (error) {
				if (
					runtime.restartDrainOwners.get(restartId) === connection &&
					(runtime.stopping || runtime.personaSessions.restartDrainId !== restartId)
				)
					runtime.restartDrainOwners.delete(restartId);
				throw error;
			}
			return;
		}
		case "gateway.restart.cancel": {
			const restartId = (request.params as { restartId?: unknown } | undefined)?.restartId;
			if (typeof restartId !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(restartId))
				throw new ProtocolError("invalid_params", "restartId must be a non-empty safe identifier");
			const owner = runtime.restartDrainOwners.get(restartId);
			if (owner !== undefined && owner !== connection)
				throw new ProtocolError("verb_failed", "restart drain is owned by another supervisor");
			const outcome = await releaseOrTransferRestartDrain(runtime, restartId);
			if (owner === connection) runtime.restartDrainOwners.delete(restartId);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result:
					outcome === "transferred" ? { cancelled: false, transferred: true } : { cancelled: outcome === "cancelled" },
			});
			return;
		}
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
			const delivery = options.database.outbox.get(id);
			const batchEvents = delivery ? options.database.monitors.eventsByBatch(delivery.turnId) : [];
			const hasFailedMonitorEvent = batchEvents.some(
				({ stage }) => stage === "failed" || stage === "failed_no_retry" || stage === "contract_unmet",
			);
			const authoredEvents = batchEvents.filter((row) => row.stage === "authored").map((row) => row.event_id);
			const confirmOutcome = confirmAndSettleMonitorDelivery(options.database, id, "delivered");
			if (confirmOutcome === "unknown") throw new ProtocolError("invalid_params", "unknown deliveryId");
			if (runtime.delivery.get(id)?.state === "confirmed") runtime.restartTerminalDeliveryIds.delete(id);
			if (confirmOutcome === "transitioned") {
				runtime.readiness.recordSuccess("delivery");
				if (batchEvents.length > 0 && !hasFailedMonitorEvent) runtime.readiness.recordSuccess("monitor_delivery");
			}
			if (confirmOutcome === "transitioned" && authoredEvents.length > 0) {
				const authoredEventIds = new Set(authoredEvents);
				for (const row of options.database.monitors.eventsByIds(authoredEvents)) {
					if (authoredEventIds.has(row.event_id) && row.stage === "delivered") {
						broadcastMonitorEvent(runtime, {
							eventId: row.event_id,
							monitorId: row.monitor_id,
							eventType: row.event_type,
							firedAt: row.fired_at,
							stage: row.stage,
						});
					}
				}
			}
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
			const pendingDelivery = runtime.delivery.get(params.deliveryId);
			const batchEvents = pendingDelivery ? options.database.monitors.eventsByBatch(pendingDelivery.turnId) : [];
			const failOutcome = runtime.delivery.fail(params.deliveryId, params.ambiguous, params.reason);
			if (failOutcome === "unknown") throw new ProtocolError("invalid_params", "unknown deliveryId");
			if (failOutcome === "transitioned") {
				const failedRow = runtime.delivery.get(params.deliveryId);
				const code = failedRow?.lastError ?? "other";
				runtime.readiness.recordFailure({
					category: "delivery",
					code,
					detail: `delivery failed (${code})`,
					...(failedRow ? { origin: failedRow.originKey } : {}),
				});
				if (batchEvents.length > 0) {
					runtime.readiness.recordFailure({
						category: "monitor_delivery",
						code,
						detail: `monitor delivery failed (${code})`,
						...(failedRow ? { origin: failedRow.originKey } : {}),
					});
				}
				if (failedRow?.state === "expired")
					reportDeliveryExpired(runtime, options.database, failedRow, safeDiagnosticField(params.reason), true);
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
					options.database.core,
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
		case "work.directive": {
			const result = await runtime.work.directive(request.params);
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
				options.database.bindings
					.workLaneRows()
					.map((lane) => [`work-${lane.origin_key.slice("work/task/".length)}`, lane]),
			);
			const value = request.params ?? {};
			if (typeof value !== "object" || value === null || Array.isArray(value))
				throw new ProtocolError("invalid_params", "work.jobs parameters must be an object");
			const { limit = WORK_JOBS_DEFAULT_LIMIT, offset = 0 } = value as { limit?: unknown; offset?: unknown };
			if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > WORK_JOBS_MAX_LIMIT)
				throw new ProtocolError("invalid_params", `limit must be an integer from 1 to ${WORK_JOBS_MAX_LIMIT}`);
			if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0)
				throw new ProtocolError("invalid_params", "offset must be a non-negative integer");
			const allRows = options.database.work.laneJobRows(true);
			// Page before the per-row git and record reads: their cost scales with the page.
			const jobRows = allRows.slice(offset, offset + limit);
			const commits = await laneLastCommits(jobRows.map((row) => row.worktree_path));
			const jobs = jobRows.map((row) => {
				const lane = laneByKey.get(row.lane_key);
				const bound = {
					...row,
					session_id: lane?.gjc_session_id ?? "",
					last_activity_at: lane?.last_activity_at ?? null,
					// Repository evidence (issue #67): a lane whose op died but whose HEAD
					// moved is progressing; it is read from the worktree, not the record.
					last_commit: commits.get(row.worktree_path) ?? null,
					reports: row.lane_key.startsWith("work-")
						? options.database.work.laneReportCounts(row.lane_key.slice("work-".length))
						: { pending: 0, claimed: 0, held: 0, undeliverable: 0 },
					...(options.database.bindings.isQuarantined("work", row.job_id)
						? { quarantined: true, reason: "broker_authority_quarantined" }
						: {}),
				};
				try {
					const record = parseLaneJobRecord(options.database.work.laneJobJson(row.job_id) ?? "");
					const attempt = record.attempts.at(-1);
					return {
						...bound,
						// The job is the primary object; the current attempt is a detail.
						accepted_at: record.createdAt,
						attempt: attempt
							? {
									op_ref: attempt.opRef,
									started_at: attempt.startedAt,
									...(attempt.endedAt ? { ended_at: attempt.endedAt } : {}),
								}
							: null,
						attempts: record.attempts.length,
						checkpoints: record.checkpoints.length,
						escalations: record.escalations.length,
					};
				} catch (error) {
					return {
						...bound,
						state: "corrupt" as const,
						corrupt: true as const,
						// Bounded so one corrupt row cannot push a page past the frame cap.
						error: utf8Prefix(diagnostic(error), 512),
					};
				}
			});
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { total: allRows.length, offset, jobs },
			});
			return;
		}
		case "work.retire": {
			const params = request.params as { name?: unknown; force?: unknown; allDead?: unknown } | undefined;
			const force = params?.force === true;
			const allDead = params?.allDead === true;
			if (force && allDead) throw new ProtocolError("invalid_params", "force and allDead are mutually exclusive");
			if (allDead) {
				// Retire all dead lanes
				await runtime.work.recover();
				const { count, names } = await runtime.lanes.retireAllDead();
				// Return result with count and names of retired lanes
				const result: Record<string, unknown> = {
					retired: count > 0,
					count,
					names,
					sessionKey: "work/*",
					sessionId: "",
					closed: true,
				};
				connection.write({
					v: PROFILE_VERSION,
					type: "response",
					id: request.id,
					result,
				});
				return;
			}
			if (force) {
				if (typeof params?.name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(params.name))
					throw new ProtocolError(
						"invalid_params",
						"work.retire --force requires name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}",
					);
				await runtime.work.recover();
				const outcome = await runtime.lanes.forceRetire(params.name);
				connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: outcome });
				return;
			}
			if (typeof params?.name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(params.name))
				throw new ProtocolError("invalid_params", "work.retire requires name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}");
			await runtime.work.recover();
			const outcome = await runtime.lanes.retire(params.name, "operator");
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: outcome });
			return;
		}
		case "ops.deliveries": {
			const value = request.params ?? {};
			if (typeof value !== "object" || value === null || Array.isArray(value))
				throw new ProtocolError("invalid_params", "ops.deliveries parameters must be an object");
			const params = value as { since?: unknown; state?: unknown; limit?: unknown };
			const since = parseReceivedAt(params.since);
			if (params.state !== undefined && typeof params.state !== "string")
				throw new ProtocolError("invalid_params", "state must be a string");
			const limit = params.limit === undefined ? 50 : params.limit;
			if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > 500)
				throw new ProtocolError("invalid_params", "limit must be an integer from 1 to 500");
			const state = params.state as string | undefined;

			const rows = runtime.delivery
				.list({
					...(since === undefined ? {} : { since }),
					...(state === undefined ? {} : { state }),
					limit,
				})
				.map((row) => {
					const projection = deliveryProjection(row.payloadJson);
					return {
						deliveryId: row.deliveryId,
						turnId: row.turnId,
						originKey: row.originKey,
						conversationId: projection.conversationId,
						state: row.state,
						createdAt: row.createdAt,
						lastError: row.lastError,
						textPrefix: projection.text.slice(0, 80),
					};
				});
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { rows } });
			return;
		}
		case "ops.slo": {
			const value = request.params ?? {};
			if (typeof value !== "object" || value === null || Array.isArray(value))
				throw new ProtocolError("invalid_params", "ops.slo parameters must be an object");
			const days = (value as { days?: unknown }).days ?? DEFAULT_SLO_DAYS;
			if (typeof days !== "number" || !Number.isSafeInteger(days) || days < 1 || days > 3_650)
				throw new ProtocolError("invalid_params", "days must be an integer from 1 to 3650");
			const now = new Date();
			const result = options.slo
				? await options.slo.evaluate(days, now)
				: evaluateSlo(options.database.core, days, now);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result });
			return;
		}
		case "ops.cycle": {
			const parameterless = request.params === undefined;
			const value = parameterless ? {} : request.params;
			if (typeof value !== "object" || value === null || Array.isArray(value))
				throw new ProtocolError("invalid_params", "ops.cycle parameters must be an object");
			const { limit = OPS_CYCLE_DEFAULT_LIMIT, offset = 0 } = value as { limit?: unknown; offset?: unknown };
			if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > OPS_CYCLE_MAX_LIMIT)
				throw new ProtocolError("invalid_params", `limit must be an integer from 1 to ${OPS_CYCLE_MAX_LIMIT}`);
			if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0)
				throw new ProtocolError("invalid_params", "offset must be a non-negative integer");
			const result = runtime.cycle.project(new Date(), { limit, offset });
			if (parameterless && result.sessionsTotal > result.sessions.length)
				throw new ProtocolError(
					"invalid_params",
					`ops.cycle has ${result.sessionsTotal} sessions; request limit/offset paging to retrieve a complete session list`,
				);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result,
			});
			return;
		}
		case "ops.integrity":
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: integrityDatabase(options.database.core),
			});
			return;
		case "session.list": {
			const sessions = options.database.bindings.sessionRows().map((row) => ({
				origin: row.origin_ref_json ? JSON.parse(row.origin_ref_json) : LOOPBACK_ORIGIN,
				createdAt: row.created_at,
				lastActivityAt: row.last_activity_at,
				epoch: row.epoch,
				bootstrap: bootstrapProjection(row),
			}));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { sessions } });
			return;
		}
		case "session.modelChoices": {
			const choices = await listModelChoices(
				options.broker?.agentDir,
				runtime.config.model,
				await (runtime.providerModels?.list() ?? []),
			);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { choices } });
			return;
		}
		case "session.recall": {
			const params = (request.params ?? {}) as { query?: unknown; limit?: unknown; requestingOrigin?: unknown };
			const requesting = params.requestingOrigin
				? originKey(validateOriginRef(params.requestingOrigin as never))
				: undefined;
			const query = typeof params.query === "string" ? params.query.toLowerCase().split(/\s+/).filter(Boolean) : [];
			const limit = Math.min(10, Math.max(0, typeof params.limit === "number" ? Math.floor(params.limit) : 10));
			const rows = options.database.inbox
				.recallRows()
				.filter((r) => r.origin_key !== requesting)
				.map((r) => ({ ...r, score: query.length ? query.filter((t) => r.text.toLowerCase().includes(t)).length : 0 }));
			rows.sort((a, b) => b.score - a.score || b.at.localeCompare(a.at));
			const snippets = rows
				.slice(0, limit)
				.map((r) => ({ origin: JSON.parse(r.origin_ref_json), text: clipByCodePoints(r.text, 500), at: r.at }));
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { snippets } });
			return;
		}
		case "memory.audit": {
			// Track async memory operations to prevent client timeouts during long operations (#473).
			// Kind-specific deduplication: audit requests don't dedup with autolink operations.
			const { opRef, created } = getOrCreatePendingMemoryOp(runtime.memoryOps, "audit");

			if (!created) {
				// Already running; return existing opRef
				connection.write({
					v: PROFILE_VERSION,
					type: "response",
					id: request.id,
					result: { opRef },
				});
				return;
			}

			startMemoryOperation(runtime, opRef, async () => {
				const root = await initializeMemory(options.config.home);
				const issues = await validateMemory(root);
				return { ok: issues.length === 0, issues };
			});

			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { opRef },
			});
			return;
		}
		case "memory.autolink": {
			// Track async memory operations to prevent client timeouts during long operations (#473).
			// Deterministic crosslink sweep: alias index from canonical filenames,
			// titles, and frontmatter aliases; first mention per file gets linked.
			// Runs through shared lock to serialize with intent commits (#341).
			// Kind-specific deduplication: autolink requests don't dedup with audit operations.
			const { opRef, created } = getOrCreatePendingMemoryOp(runtime.memoryOps, "autolink");

			if (!created) {
				// Already running; return existing opRef
				connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { opRef } });
				return;
			}

			startMemoryOperation(runtime, opRef, async () => {
				const root = await initializeMemory(options.config.home);
				return autolinkCorpus(root, runtime.memory);
			});

			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { opRef } });
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
		case "memory.status": {
			const params = (request.params ?? {}) as { opRef?: unknown };
			if (typeof params.opRef !== "string") throw new ProtocolError("invalid_params", "memory.status requires opRef");
			const op = runtime.memoryOps.get(params.opRef);
			if (!op) throw new ProtocolError("invalid_params", `unknown opRef: ${params.opRef}`);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: {
					status: op.status,
					elapsedMs: op.elapsedMs ?? Math.max(0, Date.now() - Date.parse(op.startedAt)),
					...(op.result !== undefined ? { result: op.result } : {}),
					...(op.error ? { error: op.error } : {}),
				},
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
		case "monitor.update": {
			try {
				const monitor = runtime.registry.update(request.params as never);
				if (!monitor) throw new Error("unknown monitorId");
				connection.write({
					v: PROFILE_VERSION,
					type: "response",
					id: request.id,
					result: { monitorId: monitor.monitorId },
				});
				void runtime.monitorRuntime.refresh();
			} catch (error) {
				throw new ProtocolError("invalid_params", diagnostic(error) || "invalid monitor update");
			}
			return;
		}
		case "monitor.list": {
			const now = new Date();
			const monitors = runtime.registry.list();
			const schedules = Object.fromEntries(
				monitors.map((monitor) => [monitor.monitorId, scheduleProjection(monitor, now)] as const),
			);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { monitors, schedules },
			});
			return;
		}
		case "monitor.inspect": {
			const monitorId = (request.params as { monitorId?: unknown } | undefined)?.monitorId;
			if (typeof monitorId !== "string") throw new ProtocolError("invalid_params", "unknown monitorId");
			const monitor = runtime.registry.get(monitorId);
			if (!monitor) throw new ProtocolError("invalid_params", "unknown monitorId");
			const recentEvents = options.database.monitors.eventRows(monitorId, "newest", true, 100).map((row) => {
				const recovery = options.database.monitors.eventRecovery(row.event_id);
				return {
					eventId: row.event_id,
					monitorId: row.monitor_id,
					eventType: row.event_type,
					firedAt: row.fired_at,
					stage: row.stage,
					...(recovery ? { recovery } : {}),
					...(row.procedure_json ? { procedure: JSON.parse(row.procedure_json) } : {}),
					...(row.skipped_by ? { skippedBy: row.skipped_by } : {}),
					...(options.database.bindings.isQuarantined("monitor", row.event_id)
						? { quarantined: true, reason: "broker_authority_quarantined" }
						: {}),
				};
			});
			const catchUp = runtime.registry.cronState(monitorId);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: {
					monitor,
					schedule: scheduleProjection(monitor, new Date()),
					recentEvents,
					...(catchUp ? { catchUp: { skippedTotal: catchUp.skippedTotal, lastSkip: catchUp.lastSkip } } : {}),
				},
			});
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
				// biome-ignore lint/style/noNonNullAssertion: monitor.eventTypes is guaranteed to be non-empty when monitor exists
				typeof params.eventType === "string" ? params.eventType : monitor.eventTypes[0]!,
				params.payload ?? {},
			);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result: { eventId } });
			return;
		}
		case "monitor.settle":
		case "monitor.retry": {
			const eventId = (request.params as { eventId?: unknown } | undefined)?.eventId;
			if (typeof eventId !== "string" || eventId.trim().length === 0)
				throw new ProtocolError("invalid_params", `${request.verb} requires a non-empty eventId`);
			const result: MonitorEventOperatorResult =
				request.verb === "monitor.settle" ? runtime.monitors.settle(eventId) : runtime.monitors.retry(eventId);
			connection.write({ v: PROFILE_VERSION, type: "response", id: request.id, result });
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
			const params = request.params as
				| { origin?: unknown; targetMessageId?: unknown; emoji?: unknown; action?: unknown }
				| undefined;
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
			const action = (params?.action as string | undefined) ?? "add";
			if (action !== "add" && action !== "remove")
				throw new ProtocolError(
					"invalid_params",
					`chat.react action must be "add" or "remove", not ${JSON.stringify(action)}`,
				);
			const resolved = resolveReactionEmoji(params.emoji);
			if (!resolved)
				throw new ProtocolError(
					"invalid_params",
					`emoji ${JSON.stringify(params.emoji)} is outside the reaction allowlist or custom emoji rules: ${reactionAllowlistDescription(origin.platform)}`,
				);
			// Custom emoji are allowed on Discord and Slack; Telegram rejects them as unsupported.
			const isCustom = resolved.isCustom === true;
			if (isCustom) {
				if (origin.platform === "telegram")
					throw new ProtocolError(
						"invalid_params",
						`${origin.platform} does not support custom emoji; it accepts: ${reactionAllowlistDescription(origin.platform)}`,
					);
				if (origin.platform === "discord" && origin.kind === "dm")
					throw new ProtocolError("invalid_params", "Discord custom emoji reactions require a guild channel or thread");
			} else {
				// Allowlisted is not the same as deliverable: Telegram accepts only its own
				// reaction set, so asking for one it cannot express would be a guaranteed dead
				// delivery. Refuse it here instead of letting the persona believe it acknowledged.
				if (!platformSupportsReaction(origin.platform, resolved.name))
					throw new ProtocolError(
						"invalid_params",
						`${origin.platform} cannot react with ${resolved.unicode} (${resolved.name}); it accepts: ${reactionAllowlistDescription(origin.platform)}`,
					);
			}
			const unicode = resolved.isCustom === true ? "" : resolved.unicode;
			const emojiLabel = unicode || `:${resolved.name}:`;
			const reaction: ReactionRef = {
				targetMessageId: params.targetMessageId.trim(),
				emoji: unicode,
				emojiName: resolved.name,
				action,
			};
			if (action === "remove") {
				// Verify the persona owns this reaction before removing it
				const owned = runtime.reactions.release({
					originKey: originKey(origin),
					targetMessageId: reaction.targetMessageId,
					emoji: reaction.emoji,
					emojiName: reaction.emojiName,
				});
				if (!owned)
					throw new ProtocolError(
						"invalid_params",
						`cannot remove ${emojiLabel}: the persona did not add this reaction`,
					);
			} else {
				const rejection = runtime.reactions.claim({
					originKey: originKey(origin),
					targetMessageId: reaction.targetMessageId,
					emoji: reaction.emoji,
					emojiName: reaction.emojiName,
				});
				// Cap violations are reported, never silently dropped: the caller must be able
				// to tell "not sent" from "sent and invisible".
				if (rejection)
					throw new ProtocolError("invalid_params", `reaction rejected (${rejection.reason}): ${rejection.detail}`);
			}
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
			// Slack custom emoji names can be 80 chars; preserve all of `custom:<name>`.
			const emoji = clipByCodePoints(stripControlCharacters(params.emoji).trim(), 87);
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
			options.database.transaction((tx) =>
				options.database.inbox.contextRecord(tx, {
					messageId: `reaction/${params.action}/${targetMessageId}/${authorId}/${emoji}/${new Date().toISOString()}/${crypto.randomUUID().slice(0, 8)}`,
					originKey: originKey(origin),
					authorId,
					...(actor ? { authorName: actor } : {}),
					body:
						params.action === "add"
							? `[reaction] reacted ${emoji} to message ${targetMessageId}`
							: `[reaction] removed their ${emoji} reaction from message ${targetMessageId}`,
				}),
			);
			connection.write({
				v: PROFILE_VERSION,
				type: "response",
				id: request.id,
				result: { recorded: true, engaged: false },
			});
			return;
		}
		case "engagement.panel_response": {
			// Inbound panel response: user answered an interactive panel question.
			// Validates authorization, resolves the pending gjc coordinator question,
			// and records the response in the context ledger.
			const params = request.params as
				| {
						origin?: unknown;
						panelId?: unknown;
						responseKind?: unknown;
						responderId?: unknown;
						selectedOptionId?: unknown;
						engagement?: unknown;
				  }
				| undefined;
			let origin: OriginRef;
			try {
				origin = validateOriginRef(params?.origin as OriginRef);
			} catch {
				throw new ProtocolError("invalid_params", "engagement.panel_response requires a valid origin");
			}
			if (!isChatPlatform(origin.platform))
				throw new ProtocolError(
					"invalid_params",
					`engagement.panel_response requires a ${describeChatPlatforms()} origin`,
				);

			if (typeof params?.panelId !== "string" || !(params.panelId as string).trim())
				throw new ProtocolError("invalid_params", "engagement.panel_response requires a non-empty panelId");
			if (
				typeof params?.responseKind !== "string" ||
				!["option_selected", "approved", "denied"].includes(params.responseKind as string)
			)
				throw new ProtocolError(
					"invalid_params",
					"engagement.panel_response requires responseKind to be option_selected, approved, or denied",
				);
			if (typeof params?.responderId !== "string" || !(params.responderId as string).trim())
				throw new ProtocolError("invalid_params", "engagement.panel_response requires a non-empty responderId");
			if (
				(params?.responseKind as string) === "option_selected" &&
				(typeof params?.selectedOptionId !== "string" || !(params.selectedOptionId as string).trim())
			)
				throw new ProtocolError(
					"invalid_params",
					"engagement.panel_response requires selectedOptionId for option_selected responses",
				);

			const engagement = params?.engagement as Record<string, unknown> | undefined;
			if (typeof engagement?.authorId !== "string" || !(engagement?.authorId as string))
				throw new ProtocolError("invalid_params", "engagement.panel_response requires engagement.authorId");

			const panelId = ((params?.panelId as string) || "").trim().slice(0, 256);
			const responderId = ((params?.responderId as string) || "").trim().slice(0, 256);
			const responseKind = (params?.responseKind as string) || "";
			const selectedOptionId =
				(params?.responseKind as string) === "option_selected"
					? ((params?.selectedOptionId as string) || "").trim().slice(0, 256)
					: undefined;
			const authorId = (engagement?.authorId as string) || "";
			const actor = typeof engagement?.authorName === "string" ? (engagement.authorName as string) : undefined;

			const coordinatorError = await runtime.permissionPanels
				.respond({ panelId, responderId, responseKind: responseKind as PanelResponseKind, selectedOptionId })
				.catch((error: unknown) => `response_error: ${diagnostic(error)}`);

			// Record the panel response in the context ledger (audit trail)
			const responseBody =
				responseKind === "option_selected"
					? `[panel] answered question ${panelId} with option ${selectedOptionId}${coordinatorError ? ` (error: ${coordinatorError})` : ""}`
					: `[panel] ${responseKind} panel ${panelId}${coordinatorError ? ` (error: ${coordinatorError})` : ""}`;

			options.database.transaction((tx) =>
				options.database.inbox.contextRecord(tx, {
					messageId: `panel/${responseKind}/${panelId}/${responderId}/${new Date().toISOString()}/${crypto.randomUUID().slice(0, 8)}`,
					originKey: originKey(origin),
					authorId,
					...(actor ? { authorName: actor } : {}),
					body: responseBody,
				}),
			);

			// Report result to adapter
			if (coordinatorError) {
				throw new ProtocolError("verb_failed", `Failed to submit panel response: ${coordinatorError}`);
			}

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
function isInternalReportMessageId(messageId: string): boolean {
	return messageId.startsWith("lane-report-") || messageId.startsWith(MONITOR_REPORT_MESSAGE_ID_PREFIX);
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
	if (typeof params.messageId === "string" && isInternalReportMessageId(params.messageId))
		throw new ProtocolError("invalid_params", "internal report identifiers cannot be sent to chat");
	const userText: string = params.text;
	let origin: ReturnType<typeof validateOriginRef>;
	try {
		origin = validateOriginRef(params.origin as typeof LOOPBACK_ORIGIN);
	} catch {
		throw new ProtocolError("invalid_params", "chat.send requires a valid origin");
	}
	const key = originKey(origin);
	const threadFollowUp = origin.kind === "thread" && threadFollowUpEngaged(origin, key, options.database.inbox);
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
		console.info(`gateway restart requested by owner via ${key}`);
		// The acknowledgement is already queued on the socket/outbox; establish
		// the dispatch fence immediately so no new turn slips into the restart.
		if (runtime.pendingOwnerRestartId) return;
		const ownerRestartId = `owner-${crypto.randomUUID()}`;
		runtime.pendingOwnerRestartId = ownerRestartId;
		const exit = options.exitProcess ?? ((code: number) => process.exit(code));
		void (async () => {
			// A detached stack restart owns an existing drain. Supersede it, then
			// wait for its current command to settle before taking over the lease.
			const supervisorDeadline =
				Date.now() + (options.ownerRestartSupervisorWaitMs ?? OWNER_RESTART_SUPERVISOR_WAIT_MS);
			let supervisorCancelDeadline: number | undefined;
			while (!runtime.stopping) {
				const activeId = runtime.personaSessions.restartDrainId;
				if (activeId === undefined || activeId === ownerRestartId) break;
				const now = Date.now();
				if (now >= supervisorDeadline && supervisorCancelDeadline === undefined) {
					const supervisor = runtime.restartDrainOwners.get(activeId);
					supervisor?.write({
						v: PROFILE_VERSION,
						type: "event",
						event: "gateway.restart.superseded",
						payload: { restartId: activeId, ownerRestartId },
					});
					supervisorCancelDeadline =
						now + (options.ownerRestartSupervisorCancelWaitMs ?? OWNER_RESTART_SUPERVISOR_CANCEL_WAIT_MS);
					console.warn(`owner restart requested a settled handoff from stack supervisor id=${activeId}`);
				}
				if (supervisorCancelDeadline !== undefined && now >= supervisorCancelDeadline) {
					console.warn("owner restart cancelled; stack supervisor did not settle its active service command");
					notifyOwnerRestartCancelled(
						connection,
						runtime,
						request.id,
						origin,
						"the stack supervisor did not finish its current service command before the takeover deadline",
					);
					return;
				}
				const deadline = supervisorCancelDeadline ?? supervisorDeadline;
				await Bun.sleep(Math.min(25, Math.max(1, deadline - now)));
			}
			if (runtime.stopping) return;
			const drainWaitMs = options.ownerRestartDrainWaitMs ?? PERSONA_RESTART_DRAIN_WAIT_MS;
			const drainDeadline = Date.now() + drainWaitMs;
			let outcome = await runtime.personaSessions.drainForRestart(ownerRestartId, drainWaitMs);
			if (
				outcome === "drained" &&
				!(await waitForRestartTerminalDeliveries(runtime, options.database, Math.max(0, drainDeadline - Date.now())))
			)
				outcome = "timed_out";
			if (outcome === "stopped") return exit(RESTART_EXIT_CODE);
			if (outcome === "timed_out") {
				console.warn("owner restart cancelled; persona reply delivery was not confirmed before the drain deadline");
				await runtime.personaSessions.cancelRestartDrain(ownerRestartId);
				notifyOwnerRestartCancelled(connection, runtime, request.id, origin);
				return;
			}
			// Exit non-zero on purpose: launchd KeepAlive=true and systemd
			// Restart=on-failure only relaunch after an unsuccessful exit. The
			// stop budget begins after the bounded drain succeeds.
			void runtime.stop?.("owner /restart").then(
				() => exit(RESTART_EXIT_CODE),
				() => exit(RESTART_EXIT_CODE),
			);
			setTimeout(() => exit(RESTART_EXIT_CODE), RESTART_HARD_EXIT_MS).unref();
		})()
			.catch((error: unknown) => console.error(`owner restart drain failed: ${diagnostic(error)}`))
			.finally(() => {
				if (runtime.pendingOwnerRestartId === ownerRestartId) runtime.pendingOwnerRestartId = undefined;
			});
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
	// A muted thread (#625) admits only a message that addresses the persona, which also unmutes it.
	const muteKey = threadMuteKey(origin, key, params.engagement as never);
	const mutedThread = muteKey !== undefined && options.database.inbox.isThreadMuted(muteKey);
	const muteAddressed = mutedThread && threadMuteAddressed(params.engagement as never, origin, runtime.config);
	const threadFollowUpForEngagement = threadFollowUp && (!mutedThread || muteAddressed);
	const engagementDecision = decideEngagement(
		origin,
		params.engagement as never,
		runtime.config,
		threadFollowUpForEngagement,
		mutedThread,
	);
	const authorIsBot = (params.engagement as { authorIsBot?: unknown } | undefined)?.authorIsBot === true;
	if (!authorIsBot) runtime.botAudienceTurns.recordHumanMessage(key);
	const engagement = params.engagement as
		| {
				mentioned?: boolean;
				authorId?: string;
				authorName?: unknown;
				authorHandle?: string;
				authorServerTag?: string;
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
		const addressed = authorIsBot && (engagement?.mentioned === true || threadFollowUpForEngagement);
		runtime.botAudienceTurns.recordBotAudienceDecline(addressed, botAudienceAdmission.reason);
		if (addressed || botAudienceAdmission.reason === "rate_limited")
			console.warn(
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
		ingestOrReportDrop(key, inboundMessageId, engagementDecision.engaged, () =>
			options.database.transaction((tx) =>
				options.database.inbox.contextRecord(tx, {
					messageId: inboundMessageId,
					originKey: key,
					authorId: typeof engagement?.authorId === "string" ? engagement.authorId : undefined,
					authorName: typeof engagement?.authorName === "string" ? engagement.authorName : undefined,
					body: userText,
					...(receivedAt ? { receivedAt } : {}),
				}),
			),
		);
	}
	if (!engaged) {
		if (mutedThread && !muteAddressed)
			console.info(`gateway engagement-muted skip origin=${key} message=${inboundMessageId ?? "unidentified"}`);
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
			? options.database.inbox
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
	// per-origin actor and any one-shot watch targets are notified only after this
	// transaction commits. Auto-wakes carry metadata, never the source message.
	const admission = ingestOrReportDrop(key, messageId, true, () =>
		options.database.transaction((tx) => {
			const accepted = options.database.inbox.enqueue(tx, {
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
			// The unmute commits with the addressed message it answers, so a crash can
			// never leave an accepted unmute action behind a still-muted thread.
			if (accepted && muteKey !== undefined && muteAddressed) options.database.inbox.unmuteThread(tx, muteKey);
			if (!accepted || !nonLoopback || authorIsBot)
				return { accepted, watchWakeOrigins: [] as string[], memoryIntentIds: [] as string[] };

			const matchedAt = new Date().toISOString();
			const sourceAt = receivedAt ?? matchedAt;
			const watchSender = composeSpeakerLabel({
				authorId: engagement?.authorId,
				authorName: typeof engagement?.authorName === "string" ? engagement.authorName : undefined,
				authorHandle: engagement?.authorHandle,
				authorServerTag: engagement?.authorServerTag,
			});
			const watchWakeOrigins: string[] = [];
			const memoryIntentIds: string[] = [];
			for (const watch of options.database.inbox.claimWatchesForOrigin(tx, key, messageId, matchedAt)) {
				const watcherOrigin = validateOriginRef(JSON.parse(watch.watcher_origin_ref_json) as OriginRef);
				if (originKey(watcherOrigin) !== watch.watcher_origin_key)
					throw new Error(`watch ${watch.watch_id} origin key does not match its origin reference`);
				const chain = JSON.parse(watch.handoff_chain_json) as unknown;
				if (!Array.isArray(chain) || !chain.every((entry) => typeof entry === "string"))
					throw new Error(`watch ${watch.watch_id} has invalid handoff provenance`);
				const provenance: HandoffProvenance = {
					chain: chain as string[],
					sourceOriginKey: key,
					sourceMessageId: messageId,
					...(engagement?.authorId ? { requesterId: engagement.authorId } : {}),
					...(watchSender ? { requesterName: watchSender } : {}),
					at: sourceAt,
				};
				const wakeId = watchWakeMessageId(watch.watch_id, messageId);
				const wakeAccepted = options.database.inbox.enqueue(tx, {
					messageId: wakeId,
					originKey: watch.watcher_origin_key,
					originRefJson: watch.watcher_origin_ref_json,
					body: renderWatchedInputTurn({
						provenance,
						sourcePlace:
							[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") ||
							`${origin.platform} ${origin.kind} ${origin.conversationId}`,
						sender: watchSender ?? "unknown sender",
					}),
					engagementJson: JSON.stringify({ mentioned: false, group: watcherOrigin.kind !== "dm", handoff: provenance }),
					receivedAt: matchedAt,
				});
				if (!wakeAccepted) throw new Error(`watch wake ${wakeId} already exists before watch ${watch.watch_id} fired`);
				const intentId = `watch-audit:fired:${watch.watch_id}`;
				options.database.memory.createIntent(tx, {
					id: intentId,
					kind: "daily_capture",
					payloadJson: JSON.stringify({
						kind: "daily_capture",
						originRefJson: JSON.stringify(origin),
						userText: `Cross-session watch ${watch.watch_id} matched msg:${messageId} from ${watchSender ?? "unknown sender"}`,
						replyText: `An information-only wake was enqueued for ${watch.watcher_origin_key}. Message text, attachments, watch reason, and secrets were omitted.`,
					}),
				});
				watchWakeOrigins.push(watch.watcher_origin_key);
				memoryIntentIds.push(intentId);
			}
			return { accepted, watchWakeOrigins, memoryIntentIds };
		}),
	);
	if (!admission.accepted) {
		// Duplicate message id: already accepted once, so acknowledge without dispatching.
		connection.write({
			v: PROFILE_VERSION,
			type: "response",
			id: request.id,
			result: { turnId: null, engaged: true },
		});
		return;
	}
	for (const intentId of admission.memoryIntentIds) runtime.memory.enqueueExistingId(intentId);
	for (const watchOriginKey of admission.watchWakeOrigins)
		void runtime.personaSessions
			.notifyInbound(watchOriginKey)
			.catch((error) => console.error(`gateway watch wake dispatch deferred to recovery: ${diagnostic(error)}`));
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
	if (isInternalReportMessageId(params.messageId))
		throw new ProtocolError("invalid_params", "internal report identifiers cannot be edited");
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
	if (!options.database.inbox.knownMessage(key, params.messageId)) {
		// Never ingested: nothing to point at. Not context either - a message the
		// gateway never saw must not appear by way of its edit.
		declined();
		return;
	}
	// The recorded body now says what the message says now.
	options.database.transaction((tx) =>
		options.database.inbox.updateContextBody(tx, key, params.messageId as string, params.text as string),
	);
	const threadFollowUp = origin.kind === "thread" && threadFollowUpEngaged(origin, key, options.database.inbox);
	// A muted thread (#625) admits only a message that addresses the persona, which also unmutes it.
	const muteKey = threadMuteKey(origin, key, params.engagement as never);
	const mutedThread = muteKey !== undefined && options.database.inbox.isThreadMuted(muteKey);
	const muteAddressed = mutedThread && threadMuteAddressed(params.engagement as never, origin, runtime.config);
	const threadFollowUpForEngagement = threadFollowUp && (!mutedThread || muteAddressed);
	const engagementDecision = decideEngagement(
		origin,
		params.engagement as never,
		runtime.config,
		threadFollowUpForEngagement,
		mutedThread,
	);
	const authorIsBot = (params.engagement as { authorIsBot?: unknown } | undefined)?.authorIsBot === true;
	if (!authorIsBot) runtime.botAudienceTurns.recordHumanMessage(key);
	const engagement = params.engagement as { mentioned?: boolean } | undefined;
	const botAudienceAdmission = engagementDecision.botAudienceAdmission
		? runtime.botAudienceTurns.canAdmit(key, resolveBotAudienceLimits(origin, runtime.config))
		: undefined;
	const botAudienceGuardSpent = botAudienceAdmission !== undefined && !botAudienceAdmission.admit;
	if (botAudienceAdmission !== undefined && !botAudienceAdmission.admit) {
		const addressed = authorIsBot && (engagement?.mentioned === true || threadFollowUpForEngagement);
		runtime.botAudienceTurns.recordBotAudienceDecline(addressed, botAudienceAdmission.reason);
		if (addressed || botAudienceAdmission.reason === "rate_limited")
			console.warn(
				`gateway bot audience admission declined origin=${key} message=${params.messageId} reason=${botAudienceAdmission.reason} consecutive=${runtime.botAudienceTurns.consecutiveTurns(key)} window=${runtime.botAudienceTurns.windowedTurns(key)} declines=${runtime.botAudienceTurns.botAudienceDeclines()} rateLimited=${runtime.botAudienceTurns.botAudienceRateLimited()}`,
			);
	}
	if (!engagementDecision.engaged || botAudienceGuardSpent) {
		if (mutedThread && !muteAddressed)
			console.info(`gateway engagement-muted skip origin=${key} message=${params.messageId}`);
		declined();
		return;
	}
	const messageId = messageEditId(params.messageId, params.text);
	const turnId = crypto.randomUUID();
	const { messageId: editedId, text: edit } = params;
	const accepted = ingestOrReportDrop(key, messageId, true, () =>
		options.database.transaction((tx) => {
			const enqueued = options.database.inbox.enqueue(tx, {
				messageId,
				originKey: key,
				originRefJson: JSON.stringify(origin),
				body: renderMessageEdit(editedId, edit),
				engagementJson: params.engagement ? JSON.stringify(params.engagement) : undefined,
				...(parseReceivedAt(params.receivedAt) ? { receivedAt: parseReceivedAt(params.receivedAt) } : {}),
			});
			if (enqueued && muteKey !== undefined && muteAddressed) options.database.inbox.unmuteThread(tx, muteKey);
			return enqueued;
		}),
	);
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
	const laneReport = row.source === "lane_report";
	// The requester context (turnId, connection, voice) outlives this lifecycle:
	// a turn released before its prompt landed (attach/model/send refusal, or
	// recovery requeueing an unaccepted turn) re-dispatches the same trigger under
	// a new lifecycle, which must still answer the original request. It is
	// dropped once the trigger settles (onSettled) or is discarded (onInboundDiscard).
	const context = runtime.inbound.get(row.message_id);
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
				/** Present when this row is a relayed handoff from another conversation (#72). */
				handoff?: unknown;
			})
		: undefined;
	const incomingHandoff = readHandoffProvenance(engagement?.handoff);
	const speaker = composeSpeakerLabel(engagement);
	const place =
		[engagement?.channelLabel, engagement?.serverLabel].filter(Boolean).join(" | ") ||
		`${origin.platform} ${origin.kind} ${origin.conversationId}`;
	const bootstrapState = options.database.bindings.getSessionBootstrap(key);
	const bootstrapPending = !bootstrapState || bootstrapState.lastBootstrappedEpoch < input.epoch;
	const agentsMdSnapshot = await readWorkspaceAgentsMd(runtime.config.home);
	const hasAgentsMdBaseline = bootstrapState?.agentsMdEpoch === input.epoch;
	const legacyBootstrappedSession = !bootstrapPending && !hasAgentsMdBaseline;
	const agentsMdChanged = hasAgentsMdBaseline
		? bootstrapState.agentsMdDigest !== agentsMdSnapshot.digest
		: legacyBootstrappedSession;
	// The baseline advances only when a turn carrying this AGENTS.md state
	// succeeds (with the bootstrap, below). Recording it here, before the send,
	// let a pre-send retry in the same epoch see the update as already delivered
	// and drop it for good (#676 review).
	// A recovered settlement cannot know which AGENTS.md state its prompt
	// carried, so it never advances the baseline; a later turn resends a change.
	const agentsMdBaselinePending = input.sentContextIds === undefined && (!hasAgentsMdBaseline || agentsMdChanged);
	const agentsMdSection = agentsMdChanged ? updatedAgentsMdSection(agentsMdSnapshot) : undefined;
	const internalTriggerText = () =>
		row.message_id.startsWith("agent-run/") ? userText : laneReportTriggerText(userText);
	let turnText = laneReport ? internalTriggerText() : userText;
	let contextMessageIds: readonly string[] = [];
	let contextOmissionRevision = 0;
	if (nonLoopback && input.sentContextIds !== undefined) {
		// Settling a turn that was already sent: it owns exactly what its prompt
		// claimed. Preparing a window now would expire/truncate rows and hand it
		// unread context it never saw (#686). No omission revision is committed.
		contextMessageIds = [...input.sentContextIds];
		contextOmissionRevision = -1;
	} else if (nonLoopback) {
		const prepared = options.database.transaction((tx) =>
			options.database.inbox.contextWindow(tx, key, row.message_id),
		);
		// A pointer-update turn reads its ORIGINAL message's row (the edited body
		// lives there); commit that id, not the synthetic edit row.
		contextMessageIds = laneReport
			? [...prepared.selectedMessageIds]
			: [...prepared.selectedMessageIds, editedOriginalId(row.message_id) ?? row.message_id];
		contextOmissionRevision = prepared.omissionRevision;
		const lines = prepared.rows.map(
			(entry) =>
				`- [${entry.received_at}] ${entry.author_name ?? "unknown"} (author:${entry.author_id ?? "?"}, msg:${entry.message_id}): ${clipContextBody(entry.body, 4000)}`,
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
		const isFreshSession = bootstrapPending;
		const recent = isFreshSession
			? recentConversation(
					options.database,
					key,
					origin.conversationId,
					RECENT_HISTORY_MAX,
					new Date(Date.now() - RECENT_HISTORY_WINDOW_MS).toISOString(),
				)
			: [];
		const inWindowIds = new Set(prepared.selectedMessageIds);
		const recentLines = recent
			.filter((entry) => entry.id === undefined || (!inWindowIds.has(entry.id) && entry.id !== row.message_id))
			.map(
				(entry) => `- [${entry.at}] ${entry.author}: ${clipContextBody(redactHistoricalAttachments(entry.body), 500)}`,
			);
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
		turnText = `${header}${laneReport ? internalTriggerText() : `${speaker ? `${composeTurnHeader({ speaker, place, authorId: engagement?.authorId, messageId: row.message_id, engagement })}\n` : ""}${userText}`}`;
	}

	const bootstrap = bootstrapPending
		? await buildSessionBootstrap({
				home: runtime.config.home,
				origin,
				epoch: input.epoch,
				engagement,
				config: runtime.config,
			})
		: undefined;
	// Stable per origin: the persona actor sends it only when the session's
	// context may not hold it. The bootstrap and a changed-AGENTS.md section are
	// one-shot and ride their own send.
	const sessionBootstrap = [bootstrap?.text, agentsMdSection].filter(Boolean).join("\n\n");
	const systemPreamble = [
		await runtime.persona.systemPreamble(),
		currentConversationNotice(origin),
		ATTACHMENT_SCOPE_NOTICE,
		ACTION_GUARD_SYSTEM_NOTICE,
		SECRET_GUARD_SYSTEM_NOTICE,
	].join("\n\n");
	const modelOverride = options.database.bindings.modelGet(key)?.selection;
	const effectiveModel = modelOverride ?? runtime.config.model;

	const deliveredParts: string[] = [];
	/** Ledger ids of every text part and reaction this turn put on the wire, in order. */
	const deliveredIds: string[] = [];
	/**
	 * Closes the trigger's terminal link. Terminal parts claim their own slots as
	 * they ship; a turn whose visible output was interim text or a reaction (then
	 * a silent or failed terminal) is linked to the last delivery it made, and a
	 * turn that delivered nothing records why, so `done` never leaves it NULL (#247).
	 */
	const closeTerminalLink = (reason: TerminalUnlinkedReason) => {
		const last = deliveredIds.at(-1);
		if (last === undefined)
			options.database.transaction((tx) => options.database.inbox.markUnlinked(tx, input.turn.opRef, reason));
		else if (
			terminalDeliveryIds(options.database.inbox.turnRow(input.turn.opRef)?.terminal_delivery_id ?? null).length === 0
		)
			options.database.transaction((tx) => options.database.inbox.claimTerminal(tx, input.turn.opRef, 0, last));
	};
	let assistantDeliveryStarted = false;
	let reactionTokensSeen = false;
	const maxTurnParts = 10;
	// Mid-work speech gate (issue #71/#351): holds back pure procedural narration and
	// near-repeats; count/pacing limits apply only when configured.
	const interimSpeech = new InterimSpeechGate(options.interimSpeech);
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
	/**
	 * Delivers one line into THIS conversation under the trigger's terminal slot,
	 * so a replayed or reconciled answer can never post it twice.
	 */
	const deliverTerminalLine = (text: string, partIndex = 0) => {
		const deliveryId = deterministicTerminalDeliveryId(key, input.turn.triggerMessageId, partIndex);
		if (
			options.database.transaction((tx) =>
				options.database.inbox.claimTerminal(tx, input.turn.opRef, partIndex, deliveryId),
			) !== deliveryId
		)
			return;
		const payload = runtime.delivery.prepare(crypto.randomUUID(), origin, text, undefined, deliveryId);
		if (!payload) return;
		deliveredParts.push(text);
		assistantDeliveryStarted = true;
		broadcastDelivery(runtime, payload);
	};
	/**
	 * A `[HANDOFF:<target>]` answer (#72): the target conversation's session gets
	 * one durable, relayed inbound row and answers there; this conversation gets
	 * a pointer. Anything that cannot be handed off is said here, and nothing runs.
	 */
	/** The target and hop for a relay out of this conversation, or the refusal reason. */
	const planRelay = (target: string) => {
		const resolved = resolveHandoffTarget(target, runtime.config);
		if (!resolved.ok) return resolved;
		const hop = extendHandoffChain(incomingHandoff?.chain ?? [], key, resolved.originKey);
		return hop.ok ? { ...resolved, chain: hop.chain } : hop;
	};
	const relaySourceMessageId = editedOriginalId(row.message_id) ?? row.message_id;
	const relayProvenance = (chain: readonly string[]): HandoffProvenance => ({
		chain,
		sourceOriginKey: key,
		sourceMessageId: relaySourceMessageId,
		...(incomingHandoff
			? {
					...(incomingHandoff.requesterId ? { requesterId: incomingHandoff.requesterId } : {}),
					...(incomingHandoff.requesterName ? { requesterName: incomingHandoff.requesterName } : {}),
				}
			: {
					...(engagement?.authorId ? { requesterId: engagement.authorId } : {}),
					...(speaker ? { requesterName: speaker } : {}),
				}),
		at: row.received_at,
	});
	const handOff = async (handoff: HandoffReply) => {
		const relay = planRelay(handoff.target);
		if (!relay.ok) {
			console.error(
				`gateway handoff refused origin=${key} target=${safeDiagnosticField(handoff.target)}: ${relay.reason}`,
			);
			deliverTerminalLine(renderHandoffFailure(handoff.target, relay.reason), deliveredParts.length);
			return;
		}
		const provenance = relayProvenance(relay.chain);
		const digest = buildHandoffDigest(
			recentConversation(
				options.database,
				key,
				origin.conversationId,
				RECENT_HISTORY_MAX,
				new Date(Date.now() - RECENT_HISTORY_WINDOW_MS).toISOString(),
			),
		);
		// No author identity on the relayed row: nobody in the target conversation
		// sent it, so it must not read (or authorise) as the requester speaking there.
		const accepted = options.database.transaction((tx) =>
			options.database.inbox.enqueue(tx, {
				messageId: handoffMessageId(key, relaySourceMessageId, relay.originKey),
				originKey: relay.originKey,
				originRefJson: JSON.stringify(relay.origin),
				body: renderHandoffTurn({ provenance, sourcePlace: place, note: handoff.note, digest }),
				engagementJson: JSON.stringify({ mentioned: false, group: relay.origin.kind !== "dm", handoff: provenance }),
			}),
		);
		if (accepted) {
			console.error(`gateway handoff enqueued origin=${key} target=${relay.originKey} chain=${relay.chain.length}`);
			// Not awaited: the target actor runs its own turn under its own serialization.
			void runtime.personaSessions
				.notifyInbound(relay.originKey)
				.catch((error) => console.error(`gateway handoff dispatch deferred to recovery: ${diagnostic(error)}`));
		}
		deliverTerminalLine(renderHandoffPointer(handoff.target), deliveredParts.length);
	};
	/**
	 * Every handoff block of one final answer. Spellings that resolve to the same
	 * conversation are one hop: the source message may enqueue one row there, so
	 * their notes travel together instead of the later ones being dropped.
	 */
	const handOffAll = async (handoffs: readonly HandoffReply[]) => {
		const merged = new Map<string, HandoffReply>();
		for (const handoff of handoffs) {
			const resolved = resolveHandoffTarget(handoff.target, runtime.config);
			const id = resolved.ok ? resolved.originKey : `unresolved:${handoff.target}`;
			const prior = merged.get(id);
			merged.set(
				id,
				prior ? { target: prior.target, note: [prior.note, handoff.note].filter(Boolean).join("\n\n") } : handoff,
			);
		}
		for (const handoff of merged.values()) await handOff(handoff);
	};
	/**
	 * A `[WAKE:<origin>] <summary>` line (#442): the target conversation's session
	 * gets one durable internal turn carrying the summary as information only, over
	 * the handoff relay (same hop chain and limit). This conversation keeps its
	 * reply and gets no pointer; only a refused wake is said here.
	 */
	const wake = (line: WakeLine) => {
		const relay = planRelay(line.target);
		if (!relay.ok) {
			console.error(`gateway wake refused origin=${key} target=${safeDiagnosticField(line.target)}: ${relay.reason}`);
			deliverTerminalLine(renderWakeFailure(line.target, relay.reason), deliveredParts.length);
			return;
		}
		const provenance = relayProvenance(relay.chain);
		// Like a handoff row: no author identity, so it never reads as anyone speaking there.
		const wakeId = wakeMessageId(key, relaySourceMessageId, relay.originKey, line.summary);
		const outcome = options.database.transaction((tx) => {
			const accepted = options.database.inbox.enqueue(tx, {
				messageId: wakeId,
				originKey: relay.originKey,
				originRefJson: JSON.stringify(relay.origin),
				body: renderWakeTurn({ provenance, sourcePlace: place, summary: line.summary }),
				engagementJson: JSON.stringify({ mentioned: false, group: relay.origin.kind !== "dm", handoff: provenance }),
			});
			if (!accepted) return { accepted, auditIntentId: undefined };
			const auditIntentId = `wake-audit:enqueued:${wakeId}`;
			options.database.memory.createIntent(tx, {
				id: auditIntentId,
				kind: "daily_capture",
				payloadJson: JSON.stringify({
					kind: "daily_capture",
					originRefJson: JSON.stringify(origin),
					userText: `Cross-session wake enqueued for ${relay.originKey}`,
					replyText: `Information-only wake ${wakeId} was created from ${key}. Wake summary omitted from audit.`,
				}),
			});
			return { accepted, auditIntentId };
		});
		if (!outcome.accepted) return;
		if (outcome.auditIntentId) runtime.memory.enqueueExistingId(outcome.auditIntentId);
		console.error(`gateway wake enqueued origin=${key} target=${relay.originKey} chain=${relay.chain.length}`);
		void runtime.personaSessions
			.notifyInbound(relay.originKey)
			.catch((error) => console.error(`gateway wake dispatch deferred to recovery: ${diagnostic(error)}`));
	};
	/** A one-shot [WATCH:] directive waits for the next human input at another origin. */
	const watch = (line: WatchLine) => {
		const relay = resolveHandoffTarget(line.target, runtime.config);
		if (!relay.ok) {
			deliverTerminalLine(renderWatchFailure(line.target, relay.reason), deliveredParts.length);
			return;
		}
		const ttl = parseWatchTtl(line.ttl);
		if (!ttl.ok) {
			deliverTerminalLine(renderWatchFailure(line.target, ttl.reason), deliveredParts.length);
			return;
		}
		const reason = stripControlCharacters(line.reason).replace(/\s+/g, " ").trim();
		if (!reason || Array.from(reason).length > WATCH_REASON_MAX_LENGTH) {
			deliverTerminalLine(
				renderWatchFailure(line.target, `reason must be 1-${WATCH_REASON_MAX_LENGTH} characters after trimming`),
				deliveredParts.length,
			);
			return;
		}
		if (incomingHandoff?.chain.includes(relay.originKey)) {
			deliverTerminalLine(
				renderWatchFailure(
					line.target,
					`${relay.originKey} is already in this handoff chain (${incomingHandoff.chain.join(" -> ")})`,
				),
				deliveredParts.length,
			);
			return;
		}
		const hop = extendHandoffChain(incomingHandoff?.chain ?? [], relay.originKey, key);
		if (!hop.ok) {
			deliverTerminalLine(renderWatchFailure(line.target, hop.reason), deliveredParts.length);
			return;
		}
		const registeredAtDate = new Date();
		const registeredAt = registeredAtDate.toISOString();
		const expiresAt = new Date(registeredAtDate.getTime() + ttl.ttlMs).toISOString();
		const watchId = watchRegistrationId(key, relaySourceMessageId, relay.originKey, line.ttl);
		const outcome = options.database.transaction((tx) => {
			const registration = options.database.inbox.registerWatch(tx, {
				watchId,
				watcherOriginKey: key,
				watcherOriginRefJson: JSON.stringify(origin),
				watchedOriginKey: relay.originKey,
				watchedOriginRefJson: JSON.stringify(relay.origin),
				handoffChainJson: JSON.stringify(hop.chain),
				reason,
				registeredAt,
				expiresAt,
			});
			if (registration.kind !== "registered") return { registration, auditIntentId: undefined };
			const auditIntentId = `watch-audit:registered:${watchId}`;
			options.database.memory.createIntent(tx, {
				id: auditIntentId,
				kind: "daily_capture",
				payloadJson: JSON.stringify({
					kind: "daily_capture",
					originRefJson: JSON.stringify(origin),
					userText: `Cross-session watch ${watchId} registered for ${relay.originKey}`,
					replyText: `Waiting for the next human message until ${expiresAt}. Watch reason is stored in the durable watch record.`,
				}),
			});
			return { registration, auditIntentId };
		});
		if (outcome.auditIntentId) runtime.memory.enqueueExistingId(outcome.auditIntentId);
		if (outcome.registration.kind === "already_watching") {
			deliverTerminalLine(
				renderWatchFailure(
					line.target,
					`this conversation is already watching until ${outcome.registration.watch.expires_at}`,
				),
				deliveredParts.length,
			);
			return;
		}
		if (outcome.registration.kind === "replayed" && outcome.registration.watch.state !== "pending") {
			deliverTerminalLine(
				`Watch for ${line.target} was already handled (${outcome.registration.watch.state}); it was not reopened.`,
				deliveredParts.length,
			);
			return;
		}
		deliverTerminalLine(
			renderWatchRegistered(line.target, outcome.registration.watch.expires_at),
			deliveredParts.length,
		);
	};
	/** [POST:] parts seen in mid-work speech, keyed `<target key>\n<body>`, posted at the turn's end. */
	const heldPosts = new Map<string, { body: string; post: NonNullable<PlannedPart["post"]> }>();
	/** [POST:] parts the final answer itself carried; a held copy of one is not posted twice. */
	const terminalPosts = new Set<string>();
	/** A refused [POST:] part is logged once, not on every pass over the same text. */
	const postRefusalsLogged = new Set<string>();
	/** Requested paths this turn already handled, so the terminal pass and the cap see each file once. */
	const filesHandled = new Set<string>();
	/**
	 * Uploads the reply's MEDIA:<path> attachments into the same place its text
	 * went. Each file is its own ledger delivery under a deterministic id; a file
	 * that cannot be sent becomes a short visible note instead of vanishing.
	 */
	const deliverFiles = async (paths: readonly string[], replyTo: string | undefined) => {
		const notify = (deliveryId: string, text: string) => {
			const payload = runtime.delivery.prepare(crypto.randomUUID(), origin, text, replyTo, deliveryId, false);
			if (!payload) return;
			deliveredIds.push(payload.deliveryId as string);
			assistantDeliveryStarted = true;
			broadcastDelivery(runtime, payload);
		};
		for (const requested of paths) {
			if (filesHandled.has(requested)) continue;
			filesHandled.add(requested);
			const deliveryId = deterministicFileDeliveryId(key, input.turn.triggerMessageId, requested);
			if (filesHandled.size > OUTBOUND_FILES_PER_TURN_CAP) {
				notify(
					`${deliveryId}-x`,
					`(file not sent: ${basename(requested)} - at most ${OUTBOUND_FILES_PER_TURN_CAP} files per reply)`,
				);
				continue;
			}
			const checked = await checkOutboundFile(requested, {
				home: runtime.config.home,
				workspace: runtime.workspace,
				...(runtime.config.outboundFileRoots ? { roots: runtime.config.outboundFileRoots } : {}),
			});
			if (!checked.ok) {
				console.error(
					`gateway file refused origin=${key} file=${safeDiagnosticField(checked.name)}: ${checked.reason}`,
				);
				notify(`${deliveryId}-x`, `(file not sent: ${checked.name} - ${checked.reason})`);
				continue;
			}
			const payload = runtime.delivery.prepareFile(crypto.randomUUID(), origin, checked.file, deliveryId, replyTo);
			if (!payload) continue;
			deliveredIds.push(payload.deliveryId as string);
			assistantDeliveryStarted = true;
			broadcastDelivery(runtime, payload);
		}
	};
	const deliverTurnText = async (rawMessage: string, source: "interim" | "terminal") => {
		if (!nonLoopback) return;
		// Routing blocks may sit anywhere and repeat. Handoff blocks never post
		// here and leave only with the final answer, after this conversation's own
		// parts; post blocks come back as ordinary [POST:] parts.
		const routed = extractRoutingBlocks(rawMessage);
		const handoffsAfterDelivery = source === "terminal" ? routed.handoffs : [];
		let message = routed.text;
		if (!message) {
			await handOffAll(handoffsAfterDelivery);
			return;
		}
		const reactionReply = parseReactionReply(message);
		if (reactionReply && reactionsClaimedFor.has(rawMessage)) {
			message = reactionReply.body;
			if (!message) {
				await handOffAll(handoffsAfterDelivery);
				return;
			}
		} else if (reactionReply) {
			reactionsClaimedFor.add(rawMessage);
			reactionTokensSeen = true;
			for (const wanted of reactionReply.reactions) {
				const unsupportedDiscordDmCustom =
					wanted.isCustom === true && origin.platform === "discord" && origin.kind === "dm";
				if (!platformSupportsReaction(origin.platform, wanted.emojiName) || unsupportedDiscordDmCustom) {
					const reason = unsupportedDiscordDmCustom
						? "Discord custom emoji reactions require a guild channel or thread"
						: `${origin.platform} cannot react with ${wanted.emoji || wanted.emojiName} (${wanted.emojiName})`;
					console.warn(`gateway reaction skipped for ${key}: ${reason}`);
					continue;
				}
				if (wanted.action === "remove") {
					// Removal path: release from budget and send remove delivery
					if (wanted.targetMessageId === undefined) {
						console.error(`gateway reaction remove skipped for ${key}: target message id required for remove action`);
						continue;
					}
					const released = runtime.reactions.release({
						originKey: key,
						targetMessageId: wanted.targetMessageId,
						emoji: wanted.emoji,
						emojiName: wanted.emojiName,
					});
					if (!released) {
						console.error(
							`gateway reaction remove rejected for ${key} message ${wanted.targetMessageId}: persona did not add this reaction`,
						);
						continue;
					}
					const payload = runtime.delivery.prepareReaction(crypto.randomUUID(), origin, {
						targetMessageId: wanted.targetMessageId,
						emoji: wanted.emoji,
						emojiName: wanted.emojiName,
						action: "remove",
					});
					assistantDeliveryStarted = true;
					deliveredIds.push(payload.deliveryId as string);
					broadcastDelivery(runtime, payload);
				} else {
					// Add path: claim budget and send add delivery
					if (laneReport && wanted.targetMessageId === undefined) {
						console.error(`gateway reaction skipped for internal lane report origin=${key} reason=no_trigger_message`);
						continue;
					}
					const targetMessageId = wanted.targetMessageId ?? row.message_id;
					const rejection = runtime.reactions.claim({
						turnId,
						originKey: key,
						targetMessageId,
						emoji: wanted.emoji,
						emojiName: wanted.emojiName,
					});
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
					deliveredIds.push(payload.deliveryId as string);
					broadcastDelivery(runtime, payload);
				}
			}
			message = reactionReply.body;
			if (!message) {
				await handOffAll(handoffsAfterDelivery);
				return;
			}
		}
		// MEDIA:<path> lines become their own deliveries after the text parts, on
		// platforms whose upload goes through the ledger. Elsewhere the lines stay in
		// the text for an adapter that attaches them natively.
		const fileReply = platformSupportsFiles(origin.platform) ? parseFileReply(message) : undefined;
		if (fileReply) message = fileReply.body;
		// Control tokens are internal protocol, never user-visible. Models routinely
		// wrap them in a "reasoning" preamble ("...nothing to add.\n\n[SILENT]"), so a
		// part is judged by whether it CLOSES with the token, not whether it equals
		// it: a part whose last line ends in a silence token is dropped whole. A
		// token merely mentioned (mid-sentence, in code, or followed by more prose)
		// is prose: a persona explaining its own rules lost a whole answer when any
		// occurrence counted (live, 2026-10-12). [REPLY:id] is honoured wherever it
		// appears and always stripped from the delivered text.
		const spokenParts = message
			.split(/\n\s*\[BREAK\]\s*\n?/)
			.map((part) => part.trim())
			.filter((part) => part.length > 0 && !isSilentOutput(part) && !isShutupToken(part))
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
		const inboundThreadRoot = laneReport
			? undefined
			: origin.platform !== "slack"
				? undefined
				: origin.kind === "channel" &&
						nonLoopback &&
						isSlackMessageId(editedOriginalId(row.message_id) ?? row.message_id)
					? (editedOriginalId(row.message_id) ?? row.message_id)
					: origin.kind === "dm" && typeof engagement?.replyTo?.messageId === "string" && engagement.replyTo.messageId
						? engagement.replyTo.messageId
						: undefined;
		const planned: PlannedPart[] = [];
		for (const rawPart of parts) {
			if (planned.length >= maxTurnParts) break;
			// Cross-room post: a part opening with [POST:<target>] (block-form posts
			// and [SEND:] arrive here as such parts) goes to that configured channel
			// instead of this conversation. A refused target keeps the part here
			// (token stripped) so its content is never lost.
			let part = rawPart;
			let post: PlannedPart["post"];
			const refused = (target: string, reason: string) => {
				if (reason === "current_conversation" || postRefusalsLogged.has(rawPart)) return;
				postRefusalsLogged.add(rawPart);
				console.error(
					`gateway post target refused origin=${key} target=${JSON.stringify(sanitizeDiagnostic(target).slice(0, 160))} reason=${reason}; delivering the part here`,
				);
			};
			const postMatch = rawPart.match(POST_TOKEN);
			if (postMatch) {
				part = rawPart.slice(postMatch[0].length);
				const resolved = resolvePostTarget(postMatch[1] ?? "", origin, runtime.config);
				if (resolved.ok) post = { origin: resolved.target, key: resolved.key };
				else refused(postMatch[1] ?? "", resolved.reason);
			}
			// Reply-threading: a part may open with [REPLY:<platform message id>] to
			// answer a specific message; mentions are plain <@author id> in the text.
			// A reply target belongs to this conversation, so a post ignores it.
			const replyMatch = part.match(/\[REPLY:([^\]\s]+)\]/);
			const body = (replyMatch ? part.replace(/\s*\[REPLY:[^\]\s]+\]\s*/g, " ") : part)
				.replace(/\s*\[BREAK\]\s*/g, " ")
				.trim();
			if (!body) continue;
			let replyTarget = replyMatch?.[1];
			if (!post && replyTarget) {
				// [REPLY:<channel>] is a post; as a message id it would land here while
				// the persona believes it spoke in that channel.
				const conversation = replyTargetConversation(replyTarget, origin, runtime.config);
				if (conversation) {
					if (conversation.ok) post = { origin: conversation.target, key: conversation.key };
					else refused(replyTarget, conversation.reason);
					replyTarget = undefined;
				}
			}
			if (post) {
				planned.push({ body, post });
				continue;
			}
			const replyTo = replyTarget || inboundThreadRoot;
			planned.push({ body, ...(replyTo ? { replyTo } : {}) });
		}
		const local = planned.filter((step) => step.post === undefined);
		const spoken = spokenReply(
			local.map((step) => step.body),
			voiceTurn,
		);
		for (let index = 0; index < planned.length; index++) {
			// The per-turn part budget bounds MID-WORK speech only. The terminal
			// answer always records its claim: with an ungated stream a chatty turn
			// could otherwise exhaust the budget on interims and lose its final.
			if (source === "interim" && deliveredParts.length >= maxTurnParts) break;
			const step = planned[index] as PlannedPart;
			if (step.post) {
				// A post leaves this conversation, so it is sent once, from the
				// finished answer: mid-work speech only holds it for the turn's end.
				const postKey = `${step.post.key}\n${step.body}`;
				if (source === "interim") {
					heldPosts.set(postKey, { body: step.body, post: step.post });
					continue;
				}
				terminalPosts.add(postKey);
				const postId = deterministicPostDeliveryId(key, step.post.key, input.turn.triggerMessageId, index);
				const claimed = options.database.transaction((tx) =>
					options.database.inbox.claimTerminal(tx, input.turn.opRef, index, postId),
				);
				if (claimed !== postId) continue;
				postElsewhere(step.body, step.post, postId);
				continue;
			}
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
				const owner = options.database.transaction((tx) =>
					options.database.inbox.claimTerminal(tx, input.turn.opRef, index, priorRow ? interimId : deliveryId),
				);
				if (owner !== deliveryId) continue;
			}
			// Mid-work speech is not the end of the turn: `final: false` keeps the
			// adapter's working status (typing, presence, Slack status line) alive
			// while the persona keeps streaming. Only the terminal reply - or the
			// unconditional final progress tick - tears it down.
			const payload = runtime.delivery.prepare(
				crypto.randomUUID(),
				origin,
				step.body,
				step.replyTo,
				deliveryId,
				source === "terminal",
			);
			if (!payload) continue;
			deliveredParts.push(step.body);
			deliveredIds.push(payload.deliveryId as string);
			assistantDeliveryStarted = true;
			const isLast = step === local.at(-1);
			broadcastDelivery(runtime, isLast && spoken !== "" ? { ...payload, voiceText: spoken } : payload);
		}
		if (fileReply) await deliverFiles(fileReply.paths, planned.at(-1)?.replyTo ?? inboundThreadRoot);
		await handOffAll(handoffsAfterDelivery);
	};
	/**
	 * Sends one [POST:] part to its target conversation through the ordinary
	 * ledger, and records it there as the persona's own words: a context row the
	 * target session reads as unread (and, fresh, as recent history) plus the
	 * target's daily memory. `final: false` - the post must not end a working
	 * status that belongs to a turn running in the target room.
	 */
	const postElsewhere = (body: string, post: NonNullable<PlannedPart["post"]>, deliveryId: string) => {
		const payload = runtime.delivery.prepare(crypto.randomUUID(), post.origin, body, undefined, deliveryId, false);
		if (!payload) return;
		options.database.transaction((tx) =>
			options.database.inbox.contextRecord(tx, {
				messageId: `post/${deliveryId}`,
				originKey: post.key,
				authorName: `you (posted here from ${place})`,
				body,
			}),
		);
		runtime.memory.enqueue({
			kind: "daily_capture",
			originRefJson: JSON.stringify(post.origin),
			userText: `(posted here from ${place})`,
			replyText: body,
		});
		console.error(`gateway post origin=${key} target=${post.key} delivery=${deliveryId}`);
		deliveredParts.push(`[posted to ${post.key}] ${body}`);
		deliveredIds.push(deliveryId);
		assistantDeliveryStarted = true;
		broadcastDelivery(runtime, payload);
	};
	/**
	 * The turn's text passes: mid-work speech holds its [POST:] parts; the
	 * terminal pass posts its own and then any held one the final answer did not
	 * repeat (text-keyed id, so a second terminal pass cannot post it again).
	 */
	const deliverAssistantText = async (rawMessage: string, source: "interim" | "terminal") => {
		// Cross-session directives never reach this conversation; only final replies
		// register watches or enqueue wakes.
		const wakeOutput = extractWakeLines(rawMessage);
		const watchOutput = extractWatchLines(wakeOutput.text);
		await deliverTurnText(watchOutput.text, source);
		if (source !== "terminal" || !nonLoopback) return;
		for (const line of watchOutput.watches) watch(line);
		for (const line of wakeOutput.wakes) wake(line);
		for (const [postKey, held] of heldPosts) {
			if (terminalPosts.has(postKey)) continue;
			postElsewhere(
				held.body,
				held.post,
				deterministicHeldPostDeliveryId(key, held.post.key, input.turn.triggerMessageId, held.body),
			);
		}
	};

	const startedAt = Date.now();
	const firstAfterMs = options.progress?.firstAfterMs ?? 10_000;
	const intervalMs = options.progress?.intervalMs ?? 10_000;
	let lastProgressAt = 0;
	let lastKnown = { toolCalls: 0, outputTokens: 0 };
	let activity: ChatProgressActivity | undefined;
	let ended = false;
	const emitProgress = (
		progress: { toolCalls: number; outputTokens: number },
		final = false,
		prompt = false,
		fromHeartbeat = false,
	) => {
		lastKnown = progress;
		const now = Date.now();
		// A change of activity (the first tool starting, a new tool) is worth
		// announcing as soon as the turn is old enough to show a hint at all -
		// that is the "what is it doing" signal - but never more often than half
		// the interval, so a tool-per-second turn cannot become a request storm.
		const minGap = prompt ? intervalMs / 2 : intervalMs;
		const due = now - startedAt >= firstAfterMs && now - lastProgressAt >= minGap;
		if (!final && !due) return;
		// `final` is UNCONDITIONAL. It is the adapter's only signal that the turn
		// stopped (typing hint, "working" status), and a turn that answered fast,
		// stayed silent, or failed before its first tail frame never announced
		// progress - gating final on a prior announcement left Discord "typing…"
		// for the full 330s cap after every such turn (2026-09-03, local).
		// Heartbeats must not consume the throttle window so that real tail activity
		// can be announced promptly once it arrives.
		if (!fromHeartbeat) {
			lastProgressAt = now;
		}
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
	//
	// Emit from turn start (with initial 0,0 counters) to keep the adapter's
	// stale timeout from firing during long thinking phases. The emitProgress
	// function's `due` gate prevents spam: only emits after firstAfterMs has
	// passed and then every intervalMs thereafter, respecting minGap.
	const heartbeat = setInterval(() => {
		emitProgress(lastKnown, false, false, true);
	}, intervalMs);
	const endProgress = () => {
		if (ended) return;
		ended = true;
		clearInterval(heartbeat);
		emitProgress(lastKnown, true);
	};

	// biome-ignore lint/correctness/noUnusedFunctionParameters: sessionId provided by caller
	const onFrame = async ({ frame, sessionId }: PersonaTailFrameInput) => {
		if (ended) return false;
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
				const decision = interimSpeech.admit(frame.assistantText, Date.now());
				if (decision.deliver) await deliverAssistantText(frame.assistantText, "interim");
				else console.error(`gateway mid-work speech suppressed (${turnId}, ${decision.reason}).`);
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
		if (steered.source === "lane_report") return steered.body;
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
		steered.source === "lane_report"
			? undefined
			: nonLoopback
				? (editedOriginalId(steered.message_id) ?? steered.message_id)
				: undefined;
	// A Slack channel session carries several bot-rooted threads, so [SHUTUP]
	// mutes the thread of the latest chat message this turn took in, not the
	// trigger's: the persona answers the newest message a steer brought in.
	let muteEngagement: EngagementContext | undefined = engagement as EngagementContext | undefined;
	/** Issue #625: an answer of [SHUTUP] mutes the thread it answered, however that answer reached the gateway. */
	const applyShutup = (answer: string) => {
		const muteKey = nonLoopback ? threadMuteKey(origin, key, muteEngagement) : undefined;
		if (muteKey !== undefined && isShutupToken(answer))
			options.database.transaction((tx) => options.database.inbox.muteThread(tx, muteKey));
	};
	const onSteerAccepted = ({ row: steered }: PersonaSteerInput) => {
		if (steered.source !== "lane_report" && steered.engagement_json)
			muteEngagement = JSON.parse(steered.engagement_json) as EngagementContext;
		runtime.inbound.delete(steered.message_id);
		acknowledgeSteer(runtime, steered, turnId);
	};
	const onTerminal = async ({ text, alreadyPosted }: PersonaTerminalInput) => {
		runtime.readiness.recordSuccess("turn");
		let threw = false;
		try {
			const replyText = deliveredParts.length > 0 ? deliveredParts.join("\n") : text;
			options.database.transaction((tx) => {
				if (nonLoopback)
					options.database.inbox.contextCommitWindow(tx, key, contextMessageIds, contextOmissionRevision);
				if (agentsMdBaselinePending)
					options.database.bindings.recordSessionAgentsBaseline(tx, key, input.epoch, agentsMdSnapshot.digest);
				if (bootstrap)
					options.database.bindings.markSessionBootstrapped(tx, key, input.epoch, {
						includedSections: bootstrap.includedSections,
						byteCount: bootstrap.byteCount,
						truncated: bootstrap.truncated,
						diagnostics: bootstrap.diagnostics,
					});
				options.database.bindings.touch(tx, key, JSON.stringify(origin));
				options.database.inbox.addRecall(
					tx,
					key,
					JSON.stringify(origin),
					`user: ${clipByCodePoints(userText, 500)}\nassistant: ${clipByCodePoints(replyText, 500)}`,
				);
			});
			applyShutup(text);
			if (deliveredParts.length === 0 && isSilentOutput(text)) return;
			// Recovered from the transcript after it already reached the room under
			// another turn: recorded above, never posted a second time.
			if (alreadyPosted) return;
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
		} catch (error) {
			threw = true;
			throw error;
		} finally {
			try {
				if (!threw) closeTerminalLink(nonLoopback ? (alreadyPosted ? "no_delivery" : "silent") : "loopback");
			} finally {
				endProgress();
			}
		}
	};

	const onModelRefusal = ({ error }: PersonaFailureInput) => {
		runtime.readiness.recordFailure({
			category: "turn",
			code: "model_not_selected",
			detail: sanitizeDiagnosticDetail(error.message, 200) || "model not selected",
			origin: key,
		});
		const notice = `${formatFailureNotice(error)} Send /model <id> or /new`;
		console.error(notice);
		if (nonLoopback) {
			// A refused prompt is still pending. Its diagnostic must not occupy
			// the eventual answer's delivery identity or terminal reply slot.
			const deliveryId = `gw-m-${createHash("sha256")
				.update(JSON.stringify([key, input.turn.triggerMessageId, "model_not_selected"]))
				.digest("hex")
				.slice(0, 32)}`;
			const payload = runtime.delivery.prepare(turnId, origin, notice, undefined, deliveryId);
			if (payload) {
				runtime.delivery.markInflight(deliveryId);
				broadcastDelivery(runtime, payload);
			}
		} else if (connection) {
			connection.write({
				v: PROFILE_VERSION,
				type: "event",
				event: "chat.message",
				...(context ? { id: context.requestId } : {}),
				payload: { turnId, origin, role: "assistant", text: notice, final: true },
			});
		}
	};

	const onFailure = async ({ error, recoveredText, forceFailureNotice }: PersonaFailureInput) => {
		runtime.readiness.recordFailure({
			category: "turn",
			code: "turn_failed",
			detail: sanitizeDiagnosticDetail(error.message, 200) || "turn failed",
			origin: key,
		});
		try {
			const failureNotice = formatFailureNotice(error);
			console.error(failureNotice);
			// Nothing answered this turn: a later `/new` must carry its messages (#409).
			if (nonLoopback && !recoveredText && !assistantDeliveryStarted)
				options.database.core.metaSet(`turn-failed:${key}:${input.turn.opRef}`, new Date().toISOString());
			// The turn wrote its answer before it failed (#210): deliver it through
			// the ordinary terminal slot. It then counts as a visible reply, and the
			// failure stays in the operator log instead of replacing the answer.
			if (recoveredText) applyShutup(recoveredText);
			if (nonLoopback && recoveredText && !assistantDeliveryStarted)
				await deliverAssistantText(recoveredText, "terminal");
			if (nonLoopback && assistantDeliveryStarted)
				options.database.transaction((tx) =>
					options.database.inbox.contextCommitWindow(tx, key, contextMessageIds, contextOmissionRevision),
				);
			if (nonLoopback && (!assistantDeliveryStarted || forceFailureNotice)) {
				// Recovery can observe the same failed terminal after the notice was
				// persisted but before the trigger was settled. Reuse the trigger's
				// terminal delivery identity instead of emitting another failure. A
				// hard stall cutoff also sends this notice after interim speech: that
				// speech did not answer the trigger.
				const notice = runtime.delivery.prepare(
					turnId,
					origin,
					formatFailureChatNotice(error),
					undefined,
					deterministicTerminalDeliveryId(key, input.turn.triggerMessageId, 0),
				);
				if (notice) {
					runtime.delivery.markInflight(notice.deliveryId as string);
					broadcastDelivery(runtime, notice);
				}
			}
			// A loopback requester waits for a final chat.message on its turnId; without
			// this it waits out its whole turn timeout for a turn that already failed.
			if (!nonLoopback && connection)
				connection.write({
					v: PROFILE_VERSION,
					type: "event",
					event: "chat.message",
					...(context ? { id: context.requestId } : {}),
					payload: { turnId, origin, role: "assistant", text: failureNotice, final: true },
				});
			// The `[turn failed]` notice is a diagnostic, not an answer: it never
			// claims the slot, but visible interim output before the failure does.
			closeTerminalLink(nonLoopback ? "turn_failed" : "loopback");
		} finally {
			endProgress();
		}
	};
	const onSettled = async ({ terminalDeliveryId }: PersonaTurnSettledInput) => {
		if (runtime.inbound.get(row.message_id) === context) runtime.inbound.delete(row.message_id);
		if (nonLoopback) await waitForRestartTerminalDelivery(runtime, terminalDeliveryId);
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
		...(sessionBootstrap ? { sessionBootstrap } : {}),
		...(effectiveModel ? { effectiveModel } : {}),
		...(runtime.config.serviceTier ? { effectiveServiceTier: runtime.config.serviceTier } : {}),
		contextMessageIds: new Set(contextMessageIds),
		renderSteer,
		steerContextMessageId,
		onSteerAccepted,
		onFrame,
		onTerminal,
		onModelRefusal,
		onFailure,
		onSettled,
		// Neither path ever reaches onTerminal/onFailure for THIS lifecycle: a
		// `/new` retire fences the answer, a release re-dispatches the trigger
		// under a new lifecycle. Without the final tick the 10s heartbeat kept
		// the adapter's "working…" status alive for hours (live: 286m, 2026-09-05).
		onRetired: endProgress,
		onReleased: endProgress,
		onStall: ({ elapsedMs }) =>
			console.error(`gateway persona turn stalled (${turnId}) after ${elapsedMs}ms; reconciling status.`),
	};
}

/**
 * #247 invariant: every `done` trigger names the delivery that closed it or
 * the reason none did. Reports the last 24 h at startup and hourly, so a
 * dispatch path that drops the link is visible without a manual ledger audit.
 */
function reportTerminalLinkAudit(database: Store): void {
	try {
		const { done, unlinked } = database.inbox.terminalLinkAudit(
			new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
		);
		if (unlinked > 0)
			console.error(
				`gateway terminal link audit: ${unlinked}/${done} done turns in the last 24h have no terminal_delivery_id`,
			);
	} catch (error) {
		console.error(`gateway terminal link audit failed: ${diagnostic(error)}`);
	}
}

function laneReportTriggerText(body: string): string {
	return `[Internal report from another Gajae session, not from a human; no human has seen it. Tell this conversation only what it needs by replying normally, or answer [SILENT].]\n\n${body}`;
}
function deliveryProjection(payloadJson: string): { readonly conversationId: string; readonly text: string } {
	try {
		const payload: unknown = JSON.parse(payloadJson);
		if (typeof payload !== "object" || payload === null || Array.isArray(payload))
			return { conversationId: "", text: "" };
		const value = payload as { origin?: unknown; text?: unknown };
		const origin = value.origin;
		const conversationId =
			typeof origin === "object" && origin !== null
				? (origin as { conversationId?: unknown }).conversationId
				: undefined;
		return {
			conversationId: typeof conversationId === "string" ? conversationId : "",
			text: typeof value.text === "string" ? value.text : "",
		};
	} catch {
		return { conversationId: "", text: "" };
	}
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
 * The 👀 acknowledgement tells a human their message to the persona landed. In
 * a group room most steers are ambient chatter the persona merely reads, so only
 * a message that explicitly addressed it (a mention, or any DM) gets one; bot
 * authors never do. A row without engagement metadata (non-chat) gets none.
 */
function steerAddressedPersona(steered: InboundMessageRow): boolean {
	if (!steered.engagement_json) return false;
	const engagement = JSON.parse(steered.engagement_json) as Partial<EngagementContext>;
	if (engagement.authorIsBot === true) return false;
	return engagement.mentioned === true || engagement.group === false;
}

/**
 * A steer lands inside a running turn whose model may take minutes to say
 * anything (a folded foreground wait, a long tool). The owner must see at once
 * that the message arrived, independent of the model: the gateway acknowledges
 * an accepted steer with 👀 on the steered message itself, through the same
 * ledger and budget as every reaction. It runs AFTER the steer is durably
 * accepted, so it must never throw: a cap, a duplicate (the model reacting 👀
 * too) or a storage error is logged and the acceptance stands.
 */
function acknowledgeSteer(runtime: Runtime, steered: InboundMessageRow, turnId: string | undefined): void {
	try {
		const origin = validateOriginRef(JSON.parse(steered.origin_ref_json) as OriginRef);
		if (!isChatPlatform(origin.platform)) return;
		if (!steerAddressedPersona(steered)) return;
		const targetMessageId = editedOriginalId(steered.message_id) ?? steered.message_id;
		if (!isPlatformMessageId(targetMessageId)) return;
		const eyes = resolveReactionEmoji("👀");
		if (!eyes || eyes.isCustom === true || !platformSupportsReaction(origin.platform, eyes.name)) return;
		const key = originKey(origin);
		const rejection = runtime.reactions.claim({
			...(turnId ? { turnId } : {}),
			originKey: key,
			targetMessageId,
			emoji: eyes.unicode,
			emojiName: eyes.name,
		});
		if (rejection) {
			console.error(
				`gateway steer acknowledgement skipped (${rejection.reason}) for ${key} message ${targetMessageId}: ${rejection.detail}`,
			);
			return;
		}
		broadcastDelivery(
			runtime,
			runtime.delivery.prepareReaction(crypto.randomUUID(), origin, {
				targetMessageId,
				emoji: eyes.unicode,
				emojiName: eyes.name,
			}),
		);
	} catch (error) {
		console.error(`gateway steer acknowledgement failed for message ${steered.message_id}: ${diagnostic(error)}`);
	}
}

/** Fans a committed monitor-event stage transition out to every negotiated adapter. */
function broadcastMonitorEvent(runtime: Runtime, payload: MonitorEventRecord): void {
	for (const recipient of runtime.connections)
		if (recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "monitor.event", payload });
}

/** Owner DM per `origin_silent` incident and when it clears (#688); each DM is queued with its incident transition. */
function alertSilentOrigins(runtime: Runtime, alerter: OriginSilentAlerter): void {
	let transitions: ReturnType<OriginSilentAlerter["sweep"]>;
	try {
		transitions = alerter.sweep(Date.now());
	} catch (error) {
		console.error(`origin silent sweep failed: ${diagnostic(error)}`);
		return;
	}
	for (const transition of transitions) {
		console.warn(
			`origin_silent_${transition.kind} origin=${transition.originKey} delivery=${transition.deliveryId}${transition.payload ? "" : " owner=unconfigured"}`,
		);
		if (transition.payload) broadcastDelivery(runtime, transition.payload);
	}
}

function reportDeliveryExpired(
	runtime: Runtime,
	database: Store,
	expired: Pick<ExpiredDeliveryRow, "deliveryId" | "originKey" | "attempts">,
	reason: string,
	readinessFailureRecorded = false,
): void {
	// The expired row is terminal and will eventually be pruned from the outbox.
	runtime.restartTerminalDeliveryIds.delete(expired.deliveryId);
	const deliveryId = safeDiagnosticField(expired.deliveryId);
	const origin = safeDiagnosticField(expired.originKey);
	const attempts = Number.isSafeInteger(expired.attempts) && expired.attempts >= 0 ? expired.attempts : 0;
	console.error(
		`delivery_expired deliveryId=${deliveryId} origin=${origin} attempts=${attempts} reason=${safeDiagnosticField(reason)}`,
	);
	// A monitor batch riding on this delivery can never be confirmed now: fail its
	// events terminally with the delivery evidence instead of leaving them
	// `authored` forever (#94).
	const expiration = database.transaction((tx) => {
		const current = database.outbox.get(expired.deliveryId);
		if (current?.state !== "expired") return undefined;
		const hasMonitorEvents = database.monitors.eventsByBatch(current.turnId).length > 0;
		database.monitors.eventsFailExpiredDelivery(
			tx,
			current.turnId,
			current.deliveryId,
			current.attempts,
			safeDiagnosticField(reason),
		);
		return { current, hasMonitorEvents };
	});
	if (!expiration) return;
	if (!readinessFailureRecorded) {
		const code = expiration.current.lastError ?? "delivery_expired";
		runtime.readiness.recordFailure({
			category: "delivery",
			code,
			detail: `delivery expired before confirmation (${code})`,
			origin: expiration.current.originKey,
		});
		if (expiration.hasMonitorEvents) {
			runtime.readiness.recordFailure({
				category: "monitor_delivery",
				code,
				detail: `monitor delivery expired before confirmation (${code})`,
				origin: expiration.current.originKey,
			});
		}
	}
	if (expired.deliveryId.startsWith("gw-x-")) return;
	const ownerTarget = runtime.config.ownerTarget?.origin;
	if (!ownerTarget) return;
	const noticeId = deterministicDeliveryExpiredNoticeId(expired.deliveryId);
	const notice = `[delivery lost] ${sanitizeDiagnostic(expired.originKey).slice(0, 160)} 응답 전달이 ${attempts}회 실패해 만료됐습니다. 재전송: gajaeway ops redeliver ${deliveryId}`;
	const payload = runtime.delivery.prepare(noticeId, ownerTarget, notice, undefined, noticeId);
	if (payload) broadcastDelivery(runtime, payload);
}

/**
 * Clips context body text to maxChars UTF-16 units and, when clipped, appends a marker
 * so the persona knows the source message itself is complete.
 */
export function clipContextBody(body: string, maxChars: number): string {
	if (body.length <= maxChars) return body;
	const points = Array.from(body);
	if (points.length <= maxChars) return body;
	const clipped = points.slice(0, maxChars).join("");
	const totalChars = points.length;
	return `${clipped}\u2026[clipped: first ${maxChars} of ${totalChars} chars shown; the message itself is complete]`;
}

function safeDiagnosticField(value: string): string {
	return clipByCodePoints(sanitizeDiagnostic(value), 160).replace(/\s+/g, "_") || "unknown_error";
}

function deterministicDeliveryExpiredNoticeId(deliveryId: string): string {
	return `gw-x-${createHash("sha256").update(deliveryId).digest("hex").slice(0, 32)}`;
}

/** One bind-hold notice per trigger, and one separate notice when that trigger's unresolved hold is escalated (#686). */
function deterministicBindHoldDeliveryId(originKey: string, triggerMessageId: string, reason: string): string {
	const kind = reason === UNRESOLVED_HOLD_ESCALATED ? UNRESOLVED_HOLD_ESCALATED : "bind_hold";
	return `gw-h-${createHash("sha256").update(`${originKey}|${triggerMessageId}|${kind}`).digest("hex").slice(0, 32)}`;
}

/** One deliverable part of a persona answer: here (optionally threaded) or posted to another conversation. */
interface PlannedPart {
	readonly body: string;
	readonly replyTo?: string;
	readonly post?: { readonly origin: OriginRef; readonly key: string };
}

/** A part that opens with `[POST:<origin key>]` is meant for that conversation. */
const POST_TOKEN = /^\[POST:([^\]]*)\]\s*/;

export type PostTargetResolution =
	| { readonly ok: true; readonly target: OriginRef; readonly key: string }
	| {
			readonly ok: false;
			readonly reason:
				| "invalid_origin_key"
				| "current_conversation"
				| "other_platform"
				| "not_a_channel"
				| "unconfigured_channel";
	  };

const PLATFORM_CHANNEL = /^([a-z]+):([A-Za-z0-9_-]+)$/;
const CHANNEL_MENTION = /^<#([A-Za-z0-9_-]+)(?:\|[^>]*)?>$/;
const BARE_CHANNEL_ID = /^[A-Za-z0-9_-]+$/;

/**
 * The conversation a [POST:] / [SEND:] / [REPLY:] target spells, or undefined
 * when it spells none. Models write every form, so all are read: a canonical
 * origin key, a `handoffTargets` alias, `<platform>:<channel id>`, a channel
 * mention `<#id>`, or a bare channel id on this platform (`bare`: the one form
 * a platform message id can also take).
 */
function spelledConversation(
	raw: string,
	origin: OriginRef,
	config: Pick<GatewayConfig, "handoffTargets">,
): { readonly target: OriginRef; readonly bare: boolean } | undefined {
	const spelled = raw.trim();
	const parse = (key: string) => {
		try {
			return parseOriginKey(key);
		} catch {
			return undefined;
		}
	};
	const canonical = parse(spelled);
	if (canonical) return { target: canonical, bare: false };
	const aliased = config.handoffTargets?.[spelled];
	if (aliased) return { target: aliased, bare: false };
	const prefixed = spelled.match(PLATFORM_CHANNEL);
	const prefixedTarget = prefixed ? parse(`${prefixed[1]}/channel/${prefixed[2]}`) : undefined;
	if (prefixedTarget) return { target: prefixedTarget, bare: false };
	const mention = spelled.match(CHANNEL_MENTION);
	const mentionTarget = mention ? parse(`${origin.platform}/channel/${mention[1]}`) : undefined;
	if (mentionTarget) return { target: mentionTarget, bare: false };
	const bareTarget = BARE_CHANNEL_ID.test(spelled) ? parse(`${origin.platform}/channel/${spelled}`) : undefined;
	return bareTarget ? { target: bareTarget, bare: true } : undefined;
}

function resolveSpelledPost(target: OriginRef, origin: OriginRef, config: GatewayConfig): PostTargetResolution {
	const key = originKey(target);
	if (key === originKey(origin)) return { ok: false, reason: "current_conversation" };
	if (target.platform !== origin.platform || !isChatPlatform(target.platform))
		return { ok: false, reason: "other_platform" };
	if (target.kind !== "channel") return { ok: false, reason: "not_a_channel" };
	if (!resolveChannelPolicy(target, config)) return { ok: false, reason: "unconfigured_channel" };
	return { ok: true, target, key };
}

/**
 * Where a [POST:] part may go: a `channel` on the current platform that the
 * gateway configuration lists, however its target is spelled. DMs, threads,
 * other platforms and unlisted rooms are refused so a persona cannot write
 * into a conversation nobody configured.
 */
export function resolvePostTarget(raw: string, origin: OriginRef, config: GatewayConfig): PostTargetResolution {
	const spelled = spelledConversation(raw, origin, config);
	return spelled ? resolveSpelledPost(spelled.target, origin, config) : { ok: false, reason: "invalid_origin_key" };
}

/**
 * Models write `[REPLY:<channel>]` meaning "say this in that channel". A reply
 * target that spells a conversation is read as a post target; undefined means
 * it is a message id and threads here as before. A bare id counts as a
 * conversation only when it IS one this reply could name (a configured channel
 * or this one) - otherwise it is a message id like any other.
 */
export function replyTargetConversation(
	raw: string,
	origin: OriginRef,
	config: GatewayConfig,
): PostTargetResolution | undefined {
	const spelled = spelledConversation(raw, origin, config);
	if (!spelled) return undefined;
	const resolved = resolveSpelledPost(spelled.target, origin, config);
	if (spelled.bare && !resolved.ok && resolved.reason !== "current_conversation") return undefined;
	return resolved;
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
		...(origin.kind === "thread"
			? [
					"To mute a thread so it stops sending you messages unless explicitly mentioned or replied to, reply with exactly [SHUTUP] and nothing else. This mutes the thread for this bot and persists across restarts; mention the bot or reply to one of its messages to unmute and resume normal engagement.",
				]
			: []),
		// Slack keeps a thread rooted at the persona's own message on the channel
		// session (adapter-slack decideInbound), so the thread notice above never
		// reaches it.
		...(origin.platform === "slack" && origin.kind === "channel"
			? [
					"Replies in a Slack thread under one of your own messages arrive in this channel session. To mute such a thread so it stops sending you messages unless you are explicitly mentioned there, answer a message from that thread with exactly [SHUTUP] and nothing else. This mutes only that thread, persists across restarts, and a mention in the thread unmutes it.",
				]
			: []),
		// Reply-threading was implemented end to end (parser, ledger, Slack/Discord
		// adapters) but never named in the session context, so the persona answered
		// threaded messages at the conversation root and looked like it ignored the
		// thread (live, slack DM, 2026-09-17).
		...(isChatPlatform(origin.platform)
			? [
					"Threaded replies: start a reply part with [REPLY:<message id>] to answer that specific message; the token is routing metadata and never appears in the delivered text. Message ids are in each incoming message header (msg:<id>). When the message you are answering is itself inside a thread, target the thread's parent message id so your answer lands in that thread instead of the conversation root. To say something in another channel, use a post block (below), not a reply token.",
					`Posting to another room: wrap what that channel should read as [POST:<origin key>] the text to post [POSTDONE] (for example [POST:${origin.platform}/channel/<channel id>] ... [POSTDONE]); [SEND:<target>] ... [SENDDONE] means the same. Blocks may sit anywhere in a reply and one reply may carry several; text outside them is delivered here as usual, and the tokens never appear in the delivered text. The target may also be written as a configured handoff alias, ${origin.platform}:<channel id>, or <#channel id>. Only ${origin.platform} channels configured in the gateway are allowed - never a DM or a thread, never another platform - and a refused target is posted here with the token removed. A post is sent once, when your answer is final, and a reply-threading token inside it is ignored. In Discord and Slack text, a channel link is <#channel id>.`,
				]
			: []),
		...(isChatPlatform(origin.platform)
			? [
					"Handoffs: when work belongs to another conversation, wrap it in your final reply as [HANDOFF:<target>] what that conversation's session needs to know and do [HANDOFFDONE] (<target> is a configured handoff alias or an origin such as discord:<channel id>). Blocks may sit anywhere in the reply and one reply may hand off to several conversations; text outside the blocks is answered here as usual, and each target gets a pointer here. That session answers there. A handoff grants the other session nothing it does not already have, chains stop after 2 hops, and a target already in the chain is refused.",
					"Waking another conversation: when another conversation's session should KNOW something but the work stays here, put a line [WAKE:<target>] <one-line summary> (a configured handoff alias or an origin such as discord:<channel id>) at the start of a line in your final reply. That line is removed from what this conversation sees, the rest of your reply is delivered here as usual, and the target session gets an internal turn carrying the summary as information only - it grants that session no permission and is not an instruction. Wakes use the handoff chain: they stop after 2 hops, a target already in the chain is refused, and a refused or unknown target is reported here.",
					"Watching another conversation: when you need to wait for its next human message, put [WATCH:<target>] <ttl> <short reason> on a line in your final reply (for example [WATCH:discord/dm/peer=<id>] 30m waiting for the Harbor credential). Targets use a configured handoff alias or a canonical chat origin key; TTL is 1m-7d and the reason is 1-240 characters. The watch is one-shot and expires automatically. On a matching human message, this conversation gets an internal information-only turn with the sender, origin, reason and queue status; the original message, attachments and secrets are not copied. The source session handles the message independently, and this watch grants no permission. Only one active watch per target is allowed; the 2-hop limit and loop checks still apply.",
				]
			: []),
		// The third reply mode: acknowledge without speaking. Kept next to the silence
		// guidance because the persona chooses between exactly these three shapes.
		...(isChatPlatform(origin.platform)
			? [
					`Reaction replies: start your reply with [REACT:<emoji>] to react to the message that triggered this turn, or [REACT:<emoji>@<message id>] to react to a specific message. Remove a reaction you added earlier with [UNREACT:<emoji>@<message id>]; removal frees up your reaction budget and allows you to add a different emoji to the same message (e.g. change 🔥 to ❤). With nothing after the tokens you acknowledge with a reaction and say nothing; text after the tokens is sent as well. Emoji ${origin.platform} can actually deliver: ${reactionAllowlistDescription(origin.platform)}. Custom emoji names (Slack-valid: lowercase, digits, underscores, hyphens, 2-80 chars) are also accepted on Slack and Discord and will be validated server-side. At most ${REACTIONS_PER_TURN_CAP} reactions per turn and ${REACTIONS_PER_MESSAGE_CAP} per message.`,
				]
			: []),
		...(platformSupportsFiles(origin.platform)
			? [
					`File attachments: to upload a file into this conversation (same thread as the reply), put MEDIA:<absolute path> on a line of its own. The line is removed from the text; one line per file, at most ${OUTBOUND_FILES_PER_TURN_CAP} per reply and ${Math.floor(OUTBOUND_FILE_MAX_BYTES / (1024 * 1024))} MB each. Write the file into your workspace first: only files inside the workspace (or a configured outbound root) are sent, everything else is refused. A file that cannot be sent shows up as a short "(file not sent: ...)" note.`,
				]
			: []),
		// Live 2026-09-25: the persona held turns open for 13 minutes in a
		// foreground `gh run watch`, and answered 22 owner steers with a bare
		// [REACT:👀] while it kept polling lanes with `sleep N; gajaeway work jobs`.
		// A conversation turn is the owner's line to the persona; it must not be
		// spent waiting. Waits are backgrounded or handed to a lane that reports to its parent.
		"## Staying responsive while you work",
		'Answer in words first. Never block a conversation turn on a foreground wait: no `gh run watch`, `gh pr checks --watch`, `sleep N; <poll>` loops, or repeated `gajaeway work jobs` / `work status` polling. Run a long wait as a background job (bash with async), or delegate to a lane with `gajaeway work start <name> --cwd "$PWD" "<task>"` from the target checkout, or use `--repo <path-or-url>` to create an isolated repository worktree. Its result returns here as an internal report, not a chat post. Say what you started and end the turn.',
		`When a new message arrives while you are working, it interrupts you: any foreground command is moved to the background so you can answer. Always answer it in words, right away; a reaction alone is never the whole answer to a message from the owner.${isChatPlatform(origin.platform) ? " The gateway already marks the message 👀 when it lands, so do not spend your reply on another reaction." : ""}`,
	].join("\n");
}
/** A Slack platform message id is `channel:ts`; synthetic trigger ids (`slash-…`, `edit:…`) never thread. */
function isSlackMessageId(value: string): boolean {
	return /^[A-Z][A-Z0-9]+:\d+\.\d+$/.test(value);
}

/**
 * Runs one inbound ledger write. A failure still fails the request closed (no
 * turn is admitted without its row), but it is first logged as a drop naming
 * the message and origin (#176): the generic request-failure line carried
 * neither, so a mention lost to an ingest error was indistinguishable from
 * silence.
 */
function ingestOrReportDrop<T>(originKey: string, messageId: string, engaged: boolean, write: () => T): T {
	try {
		return write();
	} catch (error) {
		console.error(
			`inbound_dropped message=${messageId} origin=${originKey} engaged=${engaged} reason=ingest_error: ${diagnostic(error)}`,
		);
		throw error;
	}
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}

/**
 * Safely clips a string to a maximum number of code points, never leaving a lone UTF-16 surrogate.
 * Uses Array.from to iterate over code points (not code units), ensuring the result is always well-formed.
 */
function clipByCodePoints(text: string, limit: number): string {
	if (text.length <= limit) return text;
	return Array.from(text).slice(0, limit).join("");
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
