import { chmod, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { type ConfigOverrides, loadConfig } from "./config";
import { GjcClient } from "./orchestrator/gjc-client";
import { PersonaLoader } from "./persona/persona";
import { type GatewayServer, startStdioServer, startUnixServer } from "./server/server";
import { GatewayDatabase } from "./store/db";
import { DeliveryLedger } from "./store/ledger";

export async function bootGateway(
	options: { readonly stdio?: boolean; readonly overrides?: ConfigOverrides } = {},
): Promise<GatewayServer> {
	const config = await loadConfig({ overrides: options.overrides });
	await mkdir(config.home, { recursive: true, mode: 0o700 });
	await chmod(config.home, 0o700);
	const database = await GatewayDatabase.open(config.dbPath);
	const persona = new PersonaLoader(config.home);
	await persona.ensureWorkspace();
	const ledger = new DeliveryLedger(database);
	const pruned = ledger.prune(7 * 24 * 60 * 60 * 1000);
	const pending = ledger.listUndelivered(24 * 60 * 60 * 1000).length;
	// The persona lives in its own dedicated workspace, never in the gateway's
	// process cwd (which is typically the product source checkout): a session
	// bound to the app repo reports that repo's git state as its own.
	const gjc = new GjcClient(database, config.turnTimeoutMs, join(config.home, "workspace"));
	const startedAt = new Date().toISOString();
	const close = async () => database.close();
	const server = options.stdio
		? startStdioServer({ config, database, gjc, persona, startedAt, onStop: close })
		: await startUnixServer({ config, database, gjc, persona, startedAt, onStop: close });
	console.error(JSON.stringify({ recovery: { recovered: pending, pending, pruned } }));
	return server;
}
