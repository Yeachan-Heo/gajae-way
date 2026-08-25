import { chmod, mkdir } from "node:fs/promises";
import { type ConfigOverrides, loadConfig } from "./config";
import { GjcClient } from "./orchestrator/gjc-client";
import { type GatewayServer, startStdioServer, startUnixServer } from "./server/server";
import { GatewayDatabase } from "./store/db";

export async function bootGateway(
	options: { readonly stdio?: boolean; readonly overrides?: ConfigOverrides } = {},
): Promise<GatewayServer> {
	const config = await loadConfig({ overrides: options.overrides });
	await mkdir(config.home, { recursive: true, mode: 0o700 });
	await chmod(config.home, 0o700);
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc = new GjcClient(database);
	const startedAt = new Date().toISOString();
	const close = async () => database.close();
	const server = options.stdio
		? startStdioServer({ config, database, gjc, startedAt, onStop: close })
		: await startUnixServer({ config, database, gjc, startedAt, onStop: close });
	console.error(JSON.stringify({ recovery: { recovered: 0, pending: 0, message: "nothing to recover" } }));
	return server;
}
