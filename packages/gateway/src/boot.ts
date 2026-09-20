import { chmod, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { type ConfigOverrides, loadConfig } from "./config";
import { seedDefaultMonitors } from "./monitors/defaults";
import { MonitorRegistry } from "./monitors/registry";
import { GlobalGjcClient, type GlobalGjcClientDependencies } from "./orchestrator/broker";
import { sanitizeDiagnostic } from "./orchestrator/rebind";
import { BrokerSessionPort } from "./orchestrator/session-port";
import { TailRunner } from "./orchestrator/tail-runner";
import { PersonaLoader } from "./persona/persona";
import { type GatewayServer, startStdioServer, startUnixServer } from "./server/server";
import { GatewayDatabase } from "./store/db";
import { DeliveryLedger } from "./store/ledger";
import {
	acquireGatewayHome,
	claimGatewayHome,
	defaultTakeoverPorts,
	releaseGatewayHome,
	type TakeoverPorts,
} from "./takeover";

export interface BootGatewayOptions {
	readonly stdio?: boolean;
	readonly overrides?: ConfigOverrides;
	/** Test/deployment seam for the shared user's SDK client. */
	readonly broker?: GlobalGjcClientDependencies;
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
	// Fence both boot and offline administration before database/recovery/socket
	// access. The service manager owns all predecessor lifecycle decisions.
	const lease = await acquireGatewayHome(config.home);
	let claimed = false;
	try {
		await claimGatewayHome(
			config.home,
			{ onlyNew: options.onlyNew === true },
			options.takeover ?? defaultTakeoverPorts(),
		);
		claimed = true;
		const database = await GatewayDatabase.open(config.dbPath);
		let broker: GlobalGjcClient | undefined;
		try {
			await mkdir(join(config.home, "workspace"), { recursive: true, mode: 0o700 });
			broker = new GlobalGjcClient({
				...options.broker,
				cwd: join(config.home, "workspace"),
			});
			const authority = { canonicalAgentDir: broker.agentDir, identity: `gjc:${broker.agentDir}` };
			database.assertBrokerAuthority(authority, { initializeEmpty: true });
			// F92-C-P1-005: the Stage 0 floor is a boot gate, never an offline config check.
			await broker.preflight();
			const persona = new PersonaLoader(config.home);
			await persona.ensureWorkspace();
			// Generic product default: memory maintenance crons exist on every fresh
			// deployment (seeded once; operator removals are never resurrected).
			seedDefaultMonitors(new MonitorRegistry(database), database);
			const ledger = new DeliveryLedger(database);
			const pruned = ledger.prune(7 * 24 * 60 * 60 * 1000);
			const pending = ledger.listUndelivered(24 * 60 * 60 * 1000).length;
			// The persona lives in its own dedicated workspace, never in the gateway's
			// process cwd (which is typically the product source checkout): a session
			// bound to the app repo reports that repo's git state as its own.
			const personaWorkspace = join(config.home, "workspace");
			const startedAt = new Date().toISOString();
			await broker.start();
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
				instanceId: database.instanceId,
				tailRunner,
				authority,
			});
			const close = async () => {
				try {
					database.close();
					await releaseGatewayHome(config.home);
				} finally {
					await lease.release();
				}
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
			try {
				await broker?.stop();
			} catch (stopError) {
				console.error(`broker cleanup after failed boot failed: ${diagnostic(stopError)}`);
			}
			database.close();
			await releaseGatewayHome(config.home);
			throw error;
		}
	} catch (error) {
		try {
			if (claimed) await releaseGatewayHome(config.home);
		} finally {
			await lease.release();
		}
		throw error;
	}
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}
