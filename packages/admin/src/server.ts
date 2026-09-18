/**
 * Admin HTTP surface over the gateway control plane.
 *
 * Read paths are plain GETs onto verbs that exist; there is no route for a
 * method the gateway does not implement. Write paths go through `MutationGate`,
 * so the server itself has no way to call an unallowlisted method, and a
 * destructive operation carries one extra requirement layered *on top* of the
 * gate: the operator must type the target object's name, which the server
 * verifies against a live read. The gate's own contract is untouched.
 *
 * The gateway client is injected as a narrow request function and gateway events
 * as a narrow subscribe function, which keeps the whole surface testable without
 * a live socket and prevents this package from reaching into gateway internals.
 */

import type { ChatMessagePayload, ChatProgressPayload } from "@gajae-gateway/protocol";
import { type AuditLog, memoryAuditLog } from "./audit";
import { formatClockSeconds } from "./format";
import { type GateOptions, MutationGate, type MutationOperation } from "./gate";
import { StreamHub } from "./stream";
import { TurnTracker } from "./turns";
import { renderIndex } from "./ui";
import { buildMonitorConsequence, buildSnapshot, type ConsoleSnapshot, type GatewayRequest } from "./view";

export type { GatewayRequest };

/** Subscribe to gateway events. Returns an unsubscribe function. */
export type GatewayEvents = (handler: (event: string, payload: unknown) => void) => () => void;

export type AdminServerOptions = {
	readonly request: GatewayRequest;
	readonly gate?: GateOptions;
	readonly auditLog?: AuditLog;
	readonly events?: GatewayEvents;
	readonly port?: number;
	readonly hostname?: string;
	readonly now?: () => Date;
	/** Low-frequency correctness backstop; 0 disables it. */
	readonly reconcileMs?: number;
};

export type AdminServer = {
	readonly port: number;
	readonly url: string;
	stop(): void;
};

export type AdminApp = {
	readonly handler: (request: Request) => Promise<Response>;
	readonly stream: StreamHub;
	stop(): void;
};

const READ_ROUTES: Record<string, string> = {
	"/api/status": "gateway.status",
	"/api/sessions": "session.list",
	"/api/jobs": "work.jobs",
	"/api/monitors": "monitor.list",
};

/** §5: three cheap reads a minute means a silently wedged stream self-heals. */
const DEFAULT_RECONCILE_MS = 60_000;
const AUDIT_TAIL_DEFAULT = 20;
const AUDIT_TAIL_MAX = 200;

function json(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
	});
}

function message(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function readPath(params: Record<string, unknown>, path: string): unknown {
	let cursor: unknown = params;
	for (const segment of path.split(".")) {
		if (typeof cursor !== "object" || cursor === null) return undefined;
		cursor = (cursor as Record<string, unknown>)[segment];
	}
	return cursor;
}

/**
 * At least one character a human can actually see.
 *
 * `String.trim` does not strip U+200B, U+FEFF, U+2060 and the rest of the
 * zero-width format characters, so `actor: "\u200b"` would satisfy the gate's
 * "an actor is required" check while leaving the audit trail attributed to
 * nothing. Blanking it here routes it into the gate's existing 401, which audits
 * the attempt as it already does - the gate's own checks stay untouched.
 */
const VISIBLE_CHARACTER = /[^\s\p{Cf}\p{Cc}\p{Zs}\p{Zl}\p{Zp}]/u;

function visibleActor(value: unknown): string {
	return typeof value === "string" && VISIBLE_CHARACTER.test(value) ? value : "";
}

/** The `monitor-ref` field of an operation whose confirmation names its target. */
function targetRefField(operation: MutationOperation): string | null {
	const field = operation.fields.find((candidate) => candidate.kind === "monitor-ref");
	return field ? (field.path ?? field.name) : null;
}

export function createAdminApp(options: AdminServerOptions): AdminApp {
	const auditLog = options.auditLog ?? memoryAuditLog();
	const gate = new MutationGate({
		audit: (entry) => auditLog.append(entry),
		...options.gate,
	});
	const now = options.now ?? (() => new Date());
	const turns = new TurnTracker(() => now().getTime());

	// Gateway reachability is tracked from real read outcomes, so the console can
	// distinguish "the daemon is down" from "this browser lost the admin process".
	let gatewayReachable = true;
	const request: GatewayRequest = async (method, params) => {
		try {
			const result = await options.request(method, params);
			gatewayReachable = true;
			return result;
		} catch (error) {
			gatewayReachable = false;
			throw error;
		}
	};

	const snapshot = (): Promise<ConsoleSnapshot> => buildSnapshot({ request, turns, now });

	const stream = new StreamHub({
		snapshot: async () => {
			const state = await snapshot();
			gatewayReachable = state.gateway.reachable;
			return state;
		},
		gatewayReachable: () => gatewayReachable,
	});

	const pushSnapshot = (): void => {
		void stream.reconcile();
	};

	const unsubscribe = options.events?.((event, payload) => {
		switch (event) {
			case "chat.progress":
				if (turns.progress(payload as ChatProgressPayload)) pushSnapshot();
				return;
			case "chat.message":
				if (turns.final(payload as ChatMessagePayload)) pushSnapshot();
				return;
			case "monitor.event":
				stream.broadcast("monitor.event", payload);
				pushSnapshot();
				return;
			case "gateway.stopping":
				gatewayReachable = false;
				turns.clear();
				stream.broadcast("gateway.stopping", payload);
				pushSnapshot();
				return;
			default:
				return;
		}
	});

	const reconcileMs = options.reconcileMs ?? DEFAULT_RECONCILE_MS;
	const reconcileTimer =
		reconcileMs > 0
			? setInterval(() => {
					void stream.reconcile();
				}, reconcileMs)
			: undefined;
	reconcileTimer?.unref?.();

	const handler = async (httpRequest: Request): Promise<Response> => {
		const url = new URL(httpRequest.url);
		const method = httpRequest.method;

		if (method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
			return new Response(renderIndex(await snapshot(), gate.operations, gate.mutationsEnabled), {
				headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
			});
		}

		if (url.pathname === "/api/stream") {
			if (method !== "GET") return json({ ok: false, error: "the stream accepts GET only" }, 405);
			return stream.connect();
		}

		if (url.pathname === "/api/operations") {
			if (method !== "GET") return json({ ok: false, error: "read routes accept GET only" }, 405);
			return json({ operations: gate.operations, mutationsEnabled: gate.mutationsEnabled });
		}

		if (url.pathname === "/api/snapshot") {
			if (method !== "GET") return json({ ok: false, error: "read routes accept GET only" }, 405);
			return json({ ok: true, result: await snapshot() });
		}

		if (url.pathname === "/api/audit") {
			if (method !== "GET") return json({ ok: false, error: "read routes accept GET only" }, 405);
			const requested = Number(url.searchParams.get("limit") ?? AUDIT_TAIL_DEFAULT);
			const limit = Number.isFinite(requested)
				? Math.min(Math.max(1, Math.trunc(requested)), AUDIT_TAIL_MAX)
				: AUDIT_TAIL_DEFAULT;
			return json({ ok: true, result: { entries: await auditLog.tail(limit) } });
		}

		if (url.pathname === "/api/consequence") {
			if (method !== "GET") return json({ ok: false, error: "read routes accept GET only" }, 405);
			const operationId = url.searchParams.get("operationId") ?? "";
			const operation = gate.operations.find((candidate) => candidate.id === operationId);
			if (!operation) return json({ ok: false, error: `operation ${operationId} is not allowlisted` }, 404);
			const monitorId = url.searchParams.get("monitorId") ?? "";
			if (!monitorId) return json({ ok: false, error: "monitorId is required" }, 400);
			try {
				const consequence = await buildMonitorConsequence(request, monitorId, operation, now());
				return json({ ok: true, result: consequence });
			} catch (error) {
				return json({ ok: false, error: message(error) }, 502);
			}
		}

		const verb = READ_ROUTES[url.pathname];
		if (verb) {
			if (method !== "GET") return json({ ok: false, error: "read routes accept GET only" }, 405);
			try {
				return json({ ok: true, result: await request(verb) });
			} catch (error) {
				return json({ ok: false, error: message(error) }, 502);
			}
		}

		if (url.pathname === "/api/mutations" && method === "POST") {
			let body: Record<string, unknown>;
			try {
				const parsed: unknown = await httpRequest.json();
				// `null`, an array and a bare scalar are all valid JSON. Reading a
				// property off them throws, which would surface as an unhandled 500
				// on the one route that must never be sloppy.
				if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
					return json({ ok: false, error: "body must be a json object" }, 400);
				}
				body = parsed as Record<string, unknown>;
			} catch {
				return json({ ok: false, error: "body must be json" }, 400);
			}
			const operationId = String(body.operationId ?? "");
			const actor = visibleActor(body.actor);
			const params =
				typeof body.params === "object" && body.params !== null ? (body.params as Record<string, unknown>) : undefined;

			// Layered on top of the gate, never instead of it: for a destructive
			// operation the operator types the target's real name, and the server
			// checks it against a live read rather than against the form.
			const operation = gate.operations.find((candidate) => candidate.id === operationId);
			if (operation?.confirmToken === "target-name") {
				// Every refusal below is audited before it returns: a mutation attempt
				// that dies at target resolution is still an attempt.
				const refuse = async (status: number, reason: string): Promise<Response> => {
					await gate.record({
						operationId,
						actor: actor.trim() || "anonymous",
						decision: "rejected",
						reason,
						...(params === undefined ? {} : { params }),
					});
					// The body carries the reason only. The status is already on the
					// response, and the console renders "428 — <reason>" from both.
					return json({ ok: false, error: reason }, status);
				};

				const refPath = targetRefField(operation);
				const monitorId = refPath ? readPath(params ?? {}, refPath) : undefined;
				if (typeof monitorId !== "string" || monitorId.length === 0) {
					return await refuse(400, `${operationId} requires a target to confirm against`);
				}
				let expected: string;
				try {
					const inspected = (await request("monitor.inspect", { monitorId })) as { monitor: { name: string } };
					expected = inspected.monitor.name;
				} catch (error) {
					return await refuse(502, `the target could not be read: ${message(error)}`);
				}
				const typed = typeof body.targetConfirm === "string" ? body.targetConfirm : "";
				if (typed !== expected) {
					return await refuse(
						428,
						`confirmation did not match. You typed ${JSON.stringify(typed)}; expected ${JSON.stringify(expected)}.`,
					);
				}
			}

			const decision = await gate.evaluate({
				operationId,
				...(actor === "" ? {} : { actor }),
				...(typeof body.confirm === "string" ? { confirm: body.confirm } : {}),
				...(params === undefined ? {} : { params }),
			});
			if (!decision.allowed) {
				return json({ ok: false, error: decision.reason }, decision.status);
			}
			try {
				const result = await request(decision.operation.method, params);
				const at = now();
				pushSnapshot();
				return json({
					ok: true,
					operationId: decision.operation.id,
					receipt: {
						headline: `✓ ${decision.operation.summary} at ${formatClockSeconds(at)}`,
						audited: `audited: actor=${actor.trim()} · operation=${decision.operation.id} · allowed`,
						at: at.toISOString(),
					},
					result,
				});
			} catch (error) {
				return json({ ok: false, error: message(error) }, 502);
			}
		}

		// Any other method on the mutation path, including a GET, is refused: a
		// mutation must never be reachable by following a link.
		if (url.pathname === "/api/mutations") {
			return json({ ok: false, error: "mutations require POST" }, 405);
		}

		return json({ ok: false, error: "not found" }, 404);
	};

	return {
		handler,
		stream,
		stop: () => {
			unsubscribe?.();
			if (reconcileTimer) clearInterval(reconcileTimer);
			stream.close();
		},
	};
}

export function createHandler(options: AdminServerOptions): (request: Request) => Promise<Response> {
	return createAdminApp(options).handler;
}

export function startAdminServer(options: AdminServerOptions): AdminServer {
	const app = createAdminApp(options);
	const server = Bun.serve({
		port: options.port ?? 0,
		// Loopback only. The console is a single-operator surface with no
		// authentication of its own; it must never be reachable off the host.
		hostname: options.hostname ?? "127.0.0.1",
		fetch: app.handler,
	});
	const port = server.port ?? options.port ?? 0;
	return {
		port,
		url: `http://${options.hostname ?? "127.0.0.1"}:${port}`,
		stop: () => {
			app.stop();
			server.stop(true);
		},
	};
}
