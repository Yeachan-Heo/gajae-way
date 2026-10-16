import { chmod, mkdir, realpath } from "node:fs/promises";
import { join } from "node:path";
import { acquireLease, homeLayout, type Lease, LeaseHeldError } from "@gajae-gateway/home";

import { type ConfigOverrides, loadConfig } from "./config";
import { DeliveryService } from "./delivery/delivery";
import { seedDefaultMonitors } from "./monitors/defaults";
import { MonitorRegistry } from "./monitors/registry";
import { assertNoRestoreJournal } from "./offline";
import { reapLeftoverWorkers } from "./ops/process-reaper";
import { SloRecorder } from "./ops/slo";
import {
	type BrokerWaitOptions,
	GlobalGjcClient,
	type GlobalGjcClientDependencies,
	readPinnedGjcVersion,
	waitForBroker,
} from "./orchestrator/broker";
import { sanitizeDiagnostic } from "./orchestrator/rebind";
import { BrokerSessionPort } from "./orchestrator/session-port";
import { TailRunner } from "./orchestrator/tail-runner";
import { createLaneCliRunner } from "./orchestrator/lane-cli-runner";
import { PersonaLoader } from "./persona/persona";
import { type GatewayServer, startStdioServer, startUnixServer } from "./server/server";
import { Store } from "./store";

export interface BootGatewayOptions {
	readonly stdio?: boolean;
	readonly overrides?: ConfigOverrides;
	/** Test/deployment seam for the shared user's SDK client. */
	readonly broker?: Partial<Omit<GlobalGjcClientDependencies, "agentDir">>;
	/** Explicit home is useful for isolated boot tests; normal startup uses GAJAEWAY_HOME. */
	readonly home?: string;

	/** How long boot waits for an unavailable shared broker before exiting; see {@link waitForBroker}. */
	readonly brokerWait?: BrokerWaitOptions;
}

/**
 * The persona workspace, created if missing and resolved through any symlink.
 *
 * The persona lives in its own dedicated directory, never in the gateway's
 * process cwd (which is typically the product source checkout): a session bound
 * to the app repo reports that repo's git state as its own. A deployment may
 * point that directory at another checkout through a symlink (live on
 * jip-gajae: `workspace -> ~/clawd`). gjc's `session.create` refuses the link
 * form and reports it as an unknown-outcome operation, which the gateway then
 * mistakes for a poisoned idempotency key and rotates the origin's epoch until
 * it caps — every persona turn in that chat fails. Resolving the link here
 * keeps the canonical directory in the cwd the broker client, the relays, and
 * every `session.create` use.
 */
export async function resolvePersonaWorkspace(home: string): Promise<string> {
	const workspaceDir = join(home, "workspace");
	await mkdir(workspaceDir, { recursive: true, mode: 0o700 });
	return await realpath(workspaceDir);
}

export interface BootedGateway extends GatewayServer {
	/** The shared-broker client, so the daemon can report its outage state. */
	readonly broker: GlobalGjcClient;
}

export async function bootGateway(options: BootGatewayOptions = {}): Promise<BootedGateway> {
	if (options.stdio) {
		// stdout carries protocol frames in stdio mode; every log line goes to stderr.
		console.log = console.error;
		console.info = console.error;
	}
	const config = await loadConfig({ home: options.home, overrides: options.overrides });
	await mkdir(config.home, { recursive: true, mode: 0o700 });
	await chmod(config.home, 0o700);
	// Acquire in a fixed order so gateway boot and offline memory administration
	// share the same gateway-then-memory ownership boundary.
	let gatewayLease: Lease;
	try {
		gatewayLease = await acquireLease(config.home, "gateway");
		console.info(`lease_acquired name=gateway pid=${process.pid}`);
	} catch (error) {
		logLeaseFailure("gateway", error);
		throw error;
	}
	let memoryLease: Lease;
	try {
		memoryLease = await acquireLease(config.home, "memory");
		console.info(`lease_acquired name=memory pid=${process.pid}`);
	} catch (error) {
		logLeaseFailure("memory", error);
		try {
			await gatewayLease.release();
		} catch (releaseError) {
			console.error(`gateway lease cleanup after memory lease failure failed: ${diagnostic(releaseError)}`);
		}
		throw error;
	}
	const releaseLeases = async () => {
		try {
			await memoryLease.release();
		} finally {
			await gatewayLease.release();
		}
	};
	try {
		await assertNoRestoreJournal(config.home);

		const layout = homeLayout(config.home);
		let executable = options.broker?.executable;
		if (executable === undefined) {
			const path = layout.gjcExecutable();
			try {
				executable = await realpath(path);
			} catch (error) {
				throw new Error(`Pinned gjc executable cannot be resolved at ${path}: ${diagnostic(error)}`, { cause: error });
			}
		}
		// Reap orphaned worker processes from previous boot epochs before accepting new work.
		// This prevents stale relay processes from accumulating and blocking resources.
		const reapResult = await reapLeftoverWorkers(config.home, {
			gjcExecutable: executable,
			gjcPrefix: options.broker?.executable === undefined ? layout.gjcPrefix : undefined,
			brokerAgentDir: config.gjc.agentDir,
		});
		if (reapResult.found > 0) {
			console.info(`process_reaper found=${reapResult.found} killed=${reapResult.killed} failed=${reapResult.failed}`);
		}
		const personaWorkspace = await resolvePersonaWorkspace(config.home);
		const pinnedVersion = readPinnedGjcVersion();
		let sloRecorder: SloRecorder | undefined;
		const broker = new GlobalGjcClient({
			...options.broker,
			executable,
			cwd: personaWorkspace,
			agentDir: config.gjc.agentDir,
			pinnedVersion,
			onUnavailable: (at) => {
				sloRecorder?.note("broker_unavailable", at);
				options.broker?.onUnavailable?.(at);
			},
		});
		const database = await Store.open(config.dbPath, {
			canonicalAgentDir: broker.agentDir,
			backupsDir: layout.backupsDir,
			journalMode: config.sqlite?.journalMode,
		});
		const slo = new SloRecorder(database.core, { home: config.home });
		sloRecorder = slo;
		try {
			database.reconcileWorkAttempts();
			const authority = { canonicalAgentDir: broker.agentDir, identity: `gjc:${broker.agentDir}` }; // Note: broker.agentDir has been canonicalized by GlobalGjcClient
			// Authority already checked in Store.open(); this assertion confirms immutability.
			database.bindings.assertAuthority(authority);
			// F92-C-P1-005: the Stage 0 floor is a boot gate, never an offline config check.
			const client = broker;
			await waitForBroker("preflight", () => client.preflight(), options.brokerWait);
			const persona = new PersonaLoader(config.home);
			await persona.ensureWorkspace();
			// Generic product default: memory maintenance crons exist on every fresh
			// deployment (seeded once; operator removals are never resurrected).
			seedDefaultMonitors(new MonitorRegistry(database), database);
			const ledger = new DeliveryService(database);
			const pruned = ledger.prune(7 * 24 * 60 * 60 * 1000);
			const pending = ledger.listUndelivered(24 * 60 * 60 * 1000).length;
			// The process start, not this line: preflight and database open take
			// seconds, and an adapter the service manager started alongside this
			// gateway reports a start inside that window. Measured on jip-gajae
			// 2026-09-30: every restart-stack flagged the co-started Discord adapter
			// `staleGeneration` (adapter 10:44:15.235, boot line 10:44:15.918).
			const startedAt = new Date(Date.now() - process.uptime() * 1000).toISOString();
			await waitForBroker("start", () => client.start(), options.brokerWait);
			const supervisor = broker;
			const tailRunner = new TailRunner({
				// One resident `gjc sdk serve --stdio` relay per session: commands go
				// down it, the turn's own content comes back up it. No polling.
				stream: (sessionId) => supervisor.openStream(sessionId),
				repo: personaWorkspace,
				stallTimeoutMs: config.stallTimeoutMs,
			});
			const sessionPort = new BrokerSessionPort({
				database,
				cli: broker.cli,
				tailRunner,
				authority,
			});
			// Lane sessions get a guarded CLI runner that prevents merge operations
			const laneCliRunner = createLaneCliRunner(broker.cli);
			const lanePort = new BrokerSessionPort({
				database,
				cli: laneCliRunner,
				instanceId: database.instanceId,
				tailRunner,
				authority,
			});
			const close = async () => {
				try {
					await slo.stop();
				} finally {
					try {
						database.close();
					} finally {
						await releaseLeases();
					}
				}
			};

			const server = options.stdio
				? startStdioServer({
						config,
						database,
						sessionPort,
						lanePort,
						persona,
						broker,
						slo,
						startedAt,
						onStop: close,
						overrides: options.overrides,
						interimSpeech: config.interimSpeech,
						workspace: personaWorkspace,
					})
				: await startUnixServer({
						config,
						database,
						sessionPort,
						lanePort,
						persona,
						broker,
						slo,
						startedAt,
						onStop: close,
						overrides: options.overrides,
						interimSpeech: config.interimSpeech,
						workspace: personaWorkspace,
					});
			if (!options.stdio) await slo.start();
			console.info(JSON.stringify({ recovery: { recovered: pending, pending, pruned } }));
			return { stop: (reason) => server.stop(reason), broker: supervisor };
		} catch (error) {
			try {
				await broker?.stop();
			} catch (stopError) {
				console.error(`broker cleanup after failed boot failed: ${diagnostic(stopError)}`);
			}
			database.close();
			throw error;
		}
	} catch (error) {
		await releaseLeases();
		throw error;
	}
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}
function logLeaseFailure(name: "gateway" | "memory", error: unknown): void {
	if (error instanceof LeaseHeldError) console.error(error.message);
	else console.error(`lease_failed name=${name} detail=${diagnostic(error)}`);
}
