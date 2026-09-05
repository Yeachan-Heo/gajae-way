import { chmod, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { type ConfigOverrides, loadConfig } from "./config";
import { seedDefaultMonitors } from "./monitors/defaults";
import { MonitorRegistry } from "./monitors/registry";
import { BrokerSupervisor, type BrokerSupervisorDependencies } from "./orchestrator/broker";
import { sanitizeDiagnostic } from "./orchestrator/rebind";
import { BrokerSessionPort } from "./orchestrator/session-port";
import { TailRunner } from "./orchestrator/tail-runner";
import { PersonaLoader } from "./persona/persona";
import { type GatewayServer, startStdioServer, startUnixServer } from "./server/server";
import { GatewayDatabase } from "./store/db";
import { DeliveryLedger } from "./store/ledger";
import { claimGatewayHome, defaultTakeoverPorts, releaseGatewayHome, type TakeoverPorts } from "./takeover";

export interface BootGatewayOptions {
	readonly stdio?: boolean;
	readonly overrides?: ConfigOverrides;
	/** Test/deployment seam for the broker command and lifecycle process. */
	readonly broker?: BrokerSupervisorDependencies;
	/** Explicit home is useful for isolated boot tests; normal startup uses GAJAEWAY_HOME. */
	readonly home?: string;
	/** `--only-new`: refuse to start while a live same-home gateway exists instead of waiting for it to exit. */
	readonly onlyNew?: boolean;
	/** Test seam for the pid-record/liveness ports; production reads the process table. */
	readonly takeover?: TakeoverPorts;
}

export async function bootGateway(options: BootGatewayOptions = {}): Promise<GatewayServer> {
	const config = await loadConfig({ home: options.home, overrides: options.overrides });
	await mkdir(config.home, { recursive: true, mode: 0o700 });
	await chmod(config.home, 0o700);
	// Ownership of the home is settled BEFORE the socket, the database, or the
	// broker lock are touched: a predecessor still running its ordered shutdown
	// is waited out (the service manager owns its lifecycle), never contested.
	await claimGatewayHome(
		config.home,
		{ onlyNew: options.onlyNew === true },
		options.takeover ?? defaultTakeoverPorts(),
	);
	const database = await GatewayDatabase.open(config.dbPath);
	let broker: BrokerSupervisor | undefined;
	let pruneTimer: ReturnType<typeof setInterval> | undefined;
	let unsubscribeGeneration: (() => void) | undefined;
	try {
		database.reclassifyPendingWrites();
		database.pruneTurnAttempts(30 * 24 * 60 * 60 * 1000);
		broker = new BrokerSupervisor({
			...options.broker,
			home: config.home,
			instanceId: database.instanceId,
			cwd: join(config.home, "workspace"),
		});
		unsubscribeGeneration = broker.onGeneration((generation) =>
			database.metaSet("broker_generation", String(generation)),
		);
		// F92-C-P1-005: the Stage 0 floor is a boot gate, never an offline config check.
		await broker.preflight();
		const persona = new PersonaLoader(config.home);
		await persona.ensureWorkspace();
		// Generic product default: memory maintenance crons exist on every fresh
		// deployment (seeded once; operator removals are never resurrected).
		seedDefaultMonitors(new MonitorRegistry(database), database);
		const ledger = new DeliveryLedger(database);
		const pruned = ledger.prune(7 * 24 * 60 * 60 * 1000);
		pruneTimer = setInterval(
			() => {
				ledger.prune(7 * 24 * 60 * 60 * 1000);
				database.pruneTurnAttempts(30 * 24 * 60 * 60 * 1000);
			},
			24 * 60 * 60 * 1000,
		);
		pruneTimer.unref();
		const pending = ledger.listUndelivered(24 * 60 * 60 * 1000).length;
		// The persona lives in its own dedicated workspace, never in the gateway's
		// process cwd (which is typically the product source checkout): a session
		// bound to the app repo reports that repo's git state as its own.
		const personaWorkspace = join(config.home, "workspace");
		const startedAt = new Date().toISOString();
		await broker.start();
		const supervisor = broker;
		const tailRunner = new TailRunner({
			run: supervisor.cli,
			// Event-driven: frames stream from the host as emitted; no interval polling.
			stream: (sessionId) => supervisor.openStream(sessionId),
			repo: personaWorkspace,
			stallTimeoutMs: config.stallTimeoutMs,
		});
		const sessionPort = new BrokerSessionPort({
			database,
			cli: broker.cli,
			instanceId: database.instanceId,
			tailRunner,
		});
		const close = async () => {
			clearInterval(pruneTimer);
			unsubscribeGeneration?.();
			database.close();
			await releaseGatewayHome(config.home);
		};
		const server = options.stdio
			? startStdioServer({
					config,
					database,
					sessionPort,
					persona,
					broker,
					startedAt,
					onStop: close,
					overrides: options.overrides,
				})
			: await startUnixServer({
					config,
					database,
					sessionPort,
					persona,
					broker,
					startedAt,
					onStop: close,
					overrides: options.overrides,
				});
		console.error(JSON.stringify({ recovery: { recovered: pending, pending, pruned } }));
		return server;
	} catch (error) {
		clearInterval(pruneTimer);
		unsubscribeGeneration?.();
		try {
			await broker?.stop();
		} catch (stopError) {
			console.error(`broker cleanup after failed boot failed: ${diagnostic(stopError)}`);
		}
		database.close();
		await releaseGatewayHome(config.home);
		throw error;
	}
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}
