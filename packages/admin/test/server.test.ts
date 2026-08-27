import { describe, expect, test } from "bun:test";
import type { AuditEntry } from "../src/gate";
import { createHandler, type GatewayRequest } from "../src/server";

type Harness = {
	fetch: (path: string, init?: RequestInit) => Promise<Response>;
	calls: { method: string; params?: unknown }[];
	audit: AuditEntry[];
};

function harness(options: { request?: GatewayRequest; mutationsEnabled?: boolean } = {}): Harness {
	const calls: { method: string; params?: unknown }[] = [];
	const audit: AuditEntry[] = [];
	const request: GatewayRequest =
		options.request ??
		(async (method, params) => {
			calls.push({ method, params });
			return { method, echoed: params ?? null };
		});
	const handler = createHandler({
		request,
		gate: {
			audit: (entry) => {
				audit.push(entry);
			},
			...(options.mutationsEnabled === undefined ? {} : { mutationsEnabled: options.mutationsEnabled }),
		},
	});
	return {
		calls,
		audit,
		fetch: (path, init) => handler(new Request(`http://admin.test${path}`, init)),
	};
}

const post = (body: unknown): RequestInit => ({
	method: "POST",
	headers: { "content-type": "application/json" },
	body: JSON.stringify(body),
});

describe("read surface", () => {
	test.each([
		["/api/status", "gateway.status"],
		["/api/core", "gateway.core"],
		["/api/sessions", "session.list"],
		["/api/monitors", "monitor.list"],
	])("%s proxies %s", async (path, method) => {
		const app = harness();
		const response = await app.fetch(path);
		expect(response.status).toBe(200);
		await expect(response.json()).resolves.toMatchObject({ ok: true });
		expect(app.calls).toEqual([{ method, params: undefined }]);
	});

	test("serves the UI at the root", async () => {
		const response = await harness().fetch("/");
		expect(response.headers.get("content-type")).toContain("text/html");
		expect(await response.text()).toContain("gajae-way admin");
	});

	test("a gateway failure is reported as 502, not a fake empty view", async () => {
		const app = harness({
			request: async () => {
				throw new Error("gateway socket closed");
			},
		});
		const response = await app.fetch("/api/status");
		expect(response.status).toBe(502);
		const body = (await response.json()) as { ok: boolean; error: string };
		expect(body.ok).toBe(false);
		expect(body.error).toMatch(/socket closed/);
	});

	test("read routes refuse non-GET methods", async () => {
		expect((await harness().fetch("/api/status", post({}))).status).toBe(405);
	});

	test("an unknown path is a 404 and never reaches the gateway", async () => {
		const app = harness();
		expect((await app.fetch("/api/whatever")).status).toBe(404);
		expect(app.calls).toHaveLength(0);
	});
});

describe("mutation gate over http", () => {
	test("a GET on the mutation path cannot trigger anything", async () => {
		const app = harness();
		const response = await app.fetch("/api/mutations");
		expect(response.status).toBe(405);
		expect(app.calls).toHaveLength(0);
	});

	test("an allowlisted operation with actor and confirmation runs", async () => {
		const app = harness();
		const response = await app.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(200);
		expect(app.calls).toEqual([{ method: "ops.integrity", params: undefined }]);
		expect(app.audit.at(-1)).toMatchObject({ decision: "allowed", actor: "형님" });
	});

	test("a missing confirmation is refused with 428 and audited", async () => {
		const app = harness();
		const response = await app.fetch("/api/mutations", post({ operationId: "ops.integrity", actor: "형님" }));
		expect(response.status).toBe(428);
		expect(app.calls).toHaveLength(0);
		expect(app.audit.at(-1)).toMatchObject({ decision: "rejected" });
	});

	test("a mismatched confirmation does not count", async () => {
		const app = harness();
		const response = await app.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "yes" }),
		);
		expect(response.status).toBe(428);
		expect(app.calls).toHaveLength(0);
	});

	test("chat.send is not reachable: it is not on the allowlist", async () => {
		const app = harness();
		const response = await app.fetch(
			"/api/mutations",
			post({ operationId: "chat.send", actor: "형님", confirm: "chat.send", params: { text: "hi" } }),
		);
		expect(response.status).toBe(404);
		expect(app.calls).toHaveLength(0);
	});

	test("an anonymous caller cannot mutate", async () => {
		const app = harness();
		const response = await app.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(401);
		expect(app.calls).toHaveLength(0);
	});

	test("a deployment with mutations disabled refuses even a perfect request", async () => {
		const app = harness({ mutationsEnabled: false });
		const response = await app.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(403);
		expect(app.calls).toHaveLength(0);
	});

	test("a non-json body is rejected before the gate", async () => {
		const app = harness();
		const response = await app.fetch("/api/mutations", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: "not json",
		});
		expect(response.status).toBe(400);
		expect(app.audit).toHaveLength(0);
	});

	test("params are forwarded to the underlying method", async () => {
		const app = harness();
		await app.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.test",
				actor: "형님",
				confirm: "monitor.test",
				params: { monitorId: "abc" },
			}),
		);
		expect(app.calls).toEqual([{ method: "monitor.test", params: { monitorId: "abc" } }]);
	});

	test("/api/operations advertises exactly the allowlist", async () => {
		const response = await harness().fetch("/api/operations");
		const body = (await response.json()) as { operations: { id: string }[] };
		expect(body.operations.map((operation) => operation.id)).toEqual([
			"monitor.add",
			"monitor.remove",
			"monitor.test",
			"ops.backup",
			"ops.integrity",
		]);
		expect(body.operations.map((operation) => operation.id)).not.toContain("chat.send");
	});
});
