/**
 * Admin HTTP surface over the gateway control plane.
 *
 * Read paths are plain GETs onto existing gateway methods. Write paths go
 * through `MutationGate`, so the server itself has no way to call an
 * unallowlisted method. The gateway client is injected as a narrow request
 * function, which keeps the server testable without a live socket and prevents
 * this package from reaching for gateway internals.
 */

import { type GateOptions, MutationGate } from "./gate";
import { renderIndex } from "./ui";

export type GatewayRequest = (method: string, params?: unknown) => Promise<unknown>;

export type AdminServerOptions = {
	readonly request: GatewayRequest;
	readonly gate?: GateOptions;
	readonly port?: number;
	readonly hostname?: string;
};

export type AdminServer = {
	readonly port: number;
	readonly url: string;
	stop(): void;
};

const READ_ROUTES: Record<string, { method: string; params?: unknown }> = {
	"/api/status": { method: "gateway.status" },
	"/api/core": { method: "gateway.core" },
	"/api/sessions": { method: "session.list" },
	"/api/monitors": { method: "monitor.list" },
};

/**
 * The loopback bind is not an authentication boundary on its own: under DNS
 * rebinding a browser resolves an attacker's name to 127.0.0.1, so its requests
 * are same-origin and reach this handler with the attacker's `Host`. Pinning
 * the header closes that path, which matters because the operation ids are
 * public on `/api/operations` and the confirmation is just an echo of them.
 */
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);

function isLoopbackHost(header: string | null): boolean {
	if (header === null) return false;
	const host = header.startsWith("[") ? header.slice(0, header.indexOf("]") + 1) : (header.split(":")[0] ?? "");
	return LOOPBACK_HOSTS.has(host);
}

function json(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json; charset=utf-8" },
	});
}

export function createHandler(options: AdminServerOptions): (request: Request) => Promise<Response> {
	const gate = new MutationGate(options.gate);

	return async (request: Request): Promise<Response> => {
		const url = new URL(request.url);

		if (!isLoopbackHost(request.headers.get("host"))) {
			return json({ ok: false, error: "unexpected host header" }, 403);
		}

		if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
			return new Response(renderIndex(gate.operations), {
				headers: { "content-type": "text/html; charset=utf-8" },
			});
		}

		if (url.pathname === "/api/operations" && request.method === "GET") {
			return json({ operations: gate.operations });
		}

		const read = READ_ROUTES[url.pathname];
		if (read) {
			if (request.method !== "GET") {
				return json({ error: "read routes accept GET only" }, 405);
			}
			try {
				return json({ ok: true, result: await options.request(read.method, read.params) });
			} catch (error) {
				return json({ ok: false, error: String(error instanceof Error ? error.message : error) }, 502);
			}
		}

		if (url.pathname === "/api/mutations" && request.method === "POST") {
			let body: Record<string, unknown>;
			try {
				body = (await request.json()) as Record<string, unknown>;
			} catch {
				return json({ ok: false, error: "body must be json" }, 400);
			}
			const decision = await gate.evaluate({
				operationId: String(body.operationId ?? ""),
				...(typeof body.actor === "string" ? { actor: body.actor } : {}),
				...(typeof body.confirm === "string" ? { confirm: body.confirm } : {}),
				...(typeof body.params === "object" && body.params !== null
					? { params: body.params as Record<string, unknown> }
					: {}),
			});
			if (!decision.allowed) {
				return json({ ok: false, error: decision.reason }, decision.status);
			}
			try {
				const result = await options.request(decision.operation.method, body.params);
				return json({ ok: true, operationId: decision.operation.id, result });
			} catch (error) {
				return json({ ok: false, error: String(error instanceof Error ? error.message : error) }, 502);
			}
		}

		// Any other method on the mutation path, including a GET, is refused: a
		// mutation must never be reachable by following a link.
		if (url.pathname === "/api/mutations") {
			return json({ ok: false, error: "mutations require POST" }, 405);
		}

		return json({ ok: false, error: "not found" }, 404);
	};
}

export function startAdminServer(options: AdminServerOptions): AdminServer {
	const handler = createHandler(options);
	const server = Bun.serve({
		port: options.port ?? 0,
		hostname: options.hostname ?? "127.0.0.1",
		fetch: handler,
	});
	const port = server.port ?? options.port ?? 0;
	return {
		port,
		url: `http://${options.hostname ?? "127.0.0.1"}:${port}`,
		stop: () => server.stop(true),
	};
}
