import { createHmac, timingSafeEqual } from "node:crypto";

export interface WebhookMonitor {
	readonly monitorId: string;
	readonly route: string;
	readonly eventType: string;
	readonly auth?: { kind: "hmac" | "bearer"; secret: string };
}
export function startWebhook(options: {
	bind?: string;
	port: number;
	exposeNonLoopback?: boolean;
	monitors: () => readonly WebhookMonitor[];
	submit: (monitorId: string, eventType: string, payload: unknown) => string;
}): ReturnType<typeof Bun.serve> {
	const bind = options.bind ?? "127.0.0.1";
	const nonLoopback = bind !== "127.0.0.1" && bind !== "::1";
	if (nonLoopback && !options.exposeNonLoopback)
		throw new Error("non-loopback webhook binding requires exposeNonLoopback");
	const nonces = new Map<string, number>();
	return Bun.serve({
		hostname: bind,
		port: options.port,
		fetch: async (request) => {
			const url = new URL(request.url);
			if (request.method !== "POST" || !url.pathname.startsWith("/hook/"))
				return new Response("not found", { status: 404 });
			const monitor = options.monitors().find((item) => item.route === url.pathname.slice(6));
			if (!monitor) return new Response("not found", { status: 404 });
			const length = Number(request.headers.get("content-length"));
			if (Number.isFinite(length) && length > 256 * 1024) return new Response("too large", { status: 413 });
			const body = new Uint8Array(await request.arrayBuffer());
			if (body.byteLength > 256 * 1024) return new Response("too large", { status: 413 });
			if ((nonLoopback && !monitor.auth) || (monitor.auth && !authenticated(request, body, monitor.auth, nonces)))
				return new Response("unauthorized", { status: 401 });
			let payload: unknown;
			try {
				payload = JSON.parse(new TextDecoder().decode(body));
			} catch {
				payload = new TextDecoder().decode(body);
			}
			options.submit(monitor.monitorId, monitor.eventType, payload);
			return new Response(null, { status: 202 });
		},
	});
}
function authenticated(
	request: Request,
	body: Uint8Array,
	auth: { kind: "hmac" | "bearer"; secret: string },
	nonces: Map<string, number>,
): boolean {
	if (auth.kind === "bearer")
		return equal(request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "", auth.secret);
	const timestamp = Number(request.headers.get("x-gajaeway-timestamp"));
	const nonce = request.headers.get("x-gajaeway-nonce") ?? "";
	if (!Number.isFinite(timestamp) || !nonce || Math.abs(Date.now() / 1000 - timestamp) > 300 || nonces.has(nonce))
		return false;
	const signature = request.headers.get("x-gajaeway-signature") ?? "";
	const expected = createHmac("sha256", auth.secret).update(`${timestamp}.${nonce}.`).update(body).digest("hex");
	if (!equal(signature, expected)) return false;
	nonces.set(nonce, Date.now());
	for (const [key, at] of nonces) if (Date.now() - at > 300_000) nonces.delete(key);
	return true;
}
function equal(a: string, b: string): boolean {
	const aa = Buffer.from(a);
	const bb = Buffer.from(b);
	return aa.length === bb.length && timingSafeEqual(aa, bb);
}
