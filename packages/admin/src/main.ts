/**
 * Service entry for the owner console.
 *
 * The previous console had no entry point at all - `startAdminServer` was
 * exported and referenced nowhere - so there was no supported way for the owner
 * to open it. This binary holds exactly one persistent gateway connection, fans
 * its events out to browser clients, and binds loopback only.
 */

import { GajaewayClient } from "@gajaeway/sdk";
import { jsonlAuditLog } from "./audit";
import { startAdminServer } from "./server";

function gajaewayHome(): string {
	return process.env.GAJAEWAY_HOME ?? `${process.env.HOME ?? "~"}/.gajaeway`;
}

function socketPath(): string {
	return process.env.GAJAEWAY_SOCKET ?? `${gajaewayHome()}/gateway.sock`;
}

function adminPort(): number {
	const raw = process.env.GAJAEWAY_ADMIN_PORT;
	if (raw === undefined) return 8788;
	const port = Number(raw);
	if (!Number.isInteger(port) || port < 1 || port > 65_535) {
		throw new Error(`GAJAEWAY_ADMIN_PORT must be a port number, got ${JSON.stringify(raw)}`);
	}
	return port;
}

const socket = socketPath();
const port = adminPort();

let client: GajaewayClient;
try {
	client = await GajaewayClient.connectSocket(socket);
} catch (error) {
	console.error(
		`Unable to connect to gateway socket ${socket}: ${error instanceof Error ? error.message : String(error)}`,
	);
	console.error("Start the daemon out-of-band first.");
	process.exit(1);
}

const server = startAdminServer({
	request: (method, params) => client.request(method, params),
	events: (handler) => {
		const offs = ["chat.message", "chat.progress", "monitor.event", "gateway.stopping"].map((event) =>
			client.on(event, (payload) => handler(event, payload)),
		);
		return () => {
			for (const off of offs) off();
		};
	},
	auditLog: jsonlAuditLog(`${gajaewayHome()}/admin-audit.jsonl`),
	port,
});

console.error(`gajaeway console on ${server.url} (gateway ${socket})`);

const shutdown = (): void => {
	server.stop();
	void client.close();
};
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
