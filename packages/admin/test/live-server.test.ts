import { afterAll, describe, expect, test } from "bun:test";
import { startAdminServer } from "../src/server";

const calls: string[] = [];
const server = startAdminServer({
	request: async (method) => {
		calls.push(method);
		return { method };
	},
});

afterAll(() => {
	server.stop();
});

describe("startAdminServer", () => {
	test("binds a real port and serves the UI", async () => {
		expect(server.port).toBeGreaterThan(0);
		const response = await fetch(server.url);
		expect(response.status).toBe(200);
		expect(await response.text()).toContain("Mutations (double gate)");
	});

	test("serves a read route end to end", async () => {
		const response = await fetch(`${server.url}/api/status`);
		expect(await response.json()).toEqual({ ok: true, result: { method: "gateway.status" } });
		expect(calls).toContain("gateway.status");
	});

	test("refuses an unconfirmed mutation end to end", async () => {
		const response = await fetch(`${server.url}/api/mutations`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ operationId: "ops.integrity", actor: "형님" }),
		});
		expect(response.status).toBe(428);
		expect(calls).not.toContain("ops.integrity");
	});
});
