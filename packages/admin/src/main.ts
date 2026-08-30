/**
 * Service entry for the admin console.
 *
 * The console is an operator tool, so its process contract is deliberately
 * narrow: it takes its gateway socket and listen port from the environment,
 * binds loopback only, and writes every mutation-gate decision to stdout so the
 * service log is the audit trail. Resolution is exported as pure functions
 * because the interesting failure - a wrong port or socket reaching the bind -
 * has to be testable without a live gateway.
 *
 * This module is a process entry, deliberately outside the library surface in
 * `./index.ts`; nothing imports it.
 */

import { GajaewayClient } from "@gajaeway/sdk";
import type { AuditEntry } from "./gate";
import { type AdminServer, startAdminServer } from "./server";

/** Loopback only. The console has no authentication of its own. */
export const ADMIN_HOSTNAME = "127.0.0.1";
/** The operator's current deployment path; override it with GAJAEWAY_SOCKET on any other host. */
export const DEFAULT_SOCKET_PATH = "/Users/bellman/gajaeway-play/discord-v1/gateway.sock";
export const DEFAULT_ADMIN_PORT = 8788;
/**
 * A dead gateway connection is invisible to the console: `GajaewayClient` keeps
 * the transport set after a close, so every later request writes into a closed
 * socket and hangs for the full request timeout. Probing on an interval turns
 * that silent rot into an exit a service manager can restart.
 */
const GATEWAY_HEARTBEAT_MS = 30_000;

export type AdminService = {
	readonly server: AdminServer;
	/** Stops the listener and closes the gateway connection. */
	stop(): Promise<void>;
};

export function resolveSocketPath(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_SOCKET?.trim() || DEFAULT_SOCKET_PATH;
}

/**
 * A malformed port is a configuration error, not something to paper over with
 * the default: a service that silently listens somewhere else is worse than one
 * that refuses to start. Only plain decimal digits are accepted, so `0x2244`
 * and `1e3` are refused rather than quietly becoming 8772 and 1000.
 */
export function resolveAdminPort(env: NodeJS.ProcessEnv = process.env): number {
	const raw = env.GAJAEWAY_ADMIN_PORT?.trim();
	if (!raw) return DEFAULT_ADMIN_PORT;
	const port = /^\d+$/.test(raw) ? Number(raw) : Number.NaN;
	if (!Number.isInteger(port) || port < 1 || port > 65_535) {
		throw new Error(`GAJAEWAY_ADMIN_PORT must be an integer between 1 and 65535, got ${JSON.stringify(raw)}`);
	}
	return port;
}

export function auditToStdout(entry: AuditEntry, log: Pick<Console, "log"> = console): void {
	log.log(`admin.audit ${JSON.stringify(entry)}`);
}

/**
 * `onGatewayLost` is what makes the console supervisable: without it the
 * heartbeat is not installed and a lost gateway degrades the process silently.
 */
export async function startAdminService(
	env: NodeJS.ProcessEnv = process.env,
	onGatewayLost?: (error: Error) => void,
): Promise<AdminService> {
	const socketPath = resolveSocketPath(env);
	const port = resolveAdminPort(env);
	const client = await GajaewayClient.connectSocket(socketPath);

	let server: AdminServer;
	try {
		server = startAdminServer({
			request: (method, params) => client.request(method, params),
			hostname: ADMIN_HOSTNAME,
			port,
			gate: { audit: (entry) => auditToStdout(entry) },
		});
	} catch (error) {
		// The open gateway socket would otherwise hold the event loop open and
		// keep a process that serves nothing alive past its failed bind.
		await client.close();
		throw error;
	}

	const heartbeat = onGatewayLost
		? setInterval(() => {
				client.request("gateway.status").catch((error: unknown) => {
					clearInterval(heartbeat);
					onGatewayLost(error instanceof Error ? error : new Error(String(error)));
				});
			}, GATEWAY_HEARTBEAT_MS)
		: undefined;

	console.log(`admin console listening on ${server.url} (gateway socket ${socketPath})`);
	return {
		server,
		stop: async () => {
			if (heartbeat) clearInterval(heartbeat);
			server.stop();
			await client.close();
		},
	};
}

if (import.meta.main) {
	startAdminService(process.env, (error) => {
		console.error(`gateway connection lost: ${error.message}`);
		process.exit(1);
	})
		.then((service) => {
			for (const signal of ["SIGINT", "SIGTERM"] as const) {
				process.on(signal, () => {
					void service.stop().then(() => process.exit(0));
				});
			}
		})
		.catch((error) => {
			console.error(error instanceof Error ? error.message : String(error));
			process.exitCode = 1;
		});
}
