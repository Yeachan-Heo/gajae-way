import { afterAll, describe, expect, test } from "bun:test";
import { startAdminServer } from "../src/server";
import { FIXED_NOW, MONITOR, monitorEvent, SESSIONS, STATUS } from "./fixture";

const calls: string[] = [];
const server = startAdminServer({
	request: async (method, params) => {
		calls.push(method);
		switch (method) {
			case "gateway.status":
				return STATUS;
			case "session.list":
				return SESSIONS;
			case "monitor.list":
				return { monitors: [MONITOR] };
			case "monitor.inspect":
				return { monitor: MONITOR, recentEvents: [monitorEvent()] };
			default:
				return { method, echoed: params ?? null };
		}
	},
	reconcileMs: 0,
	now: () => FIXED_NOW,
});

afterAll(() => {
	server.stop();
});

describe("startAdminServer", () => {
	test("binds loopback on a real port and serves the console", async () => {
		expect(server.port).toBeGreaterThan(0);
		expect(server.url).toStartWith("http://127.0.0.1:");
		const response = await fetch(server.url);
		expect(response.status).toBe(200);
		const html = await response.text();
		expect(html).toContain("Operations");
		expect(html).toContain("weekday-review");
	});

	test("serves a read route end to end", async () => {
		const response = await fetch(`${server.url}/api/status`);
		expect(await response.json()).toEqual({ ok: true, result: STATUS });
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

	test("refuses chat.send end to end", async () => {
		const response = await fetch(`${server.url}/api/mutations`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ operationId: "chat.send", actor: "형님", confirm: "chat.send", params: { text: "hi" } }),
		});
		expect(response.status).toBe(404);
		expect(calls).not.toContain("chat.send");
	});

	test("streams events over a real socket", async () => {
		const response = await fetch(`${server.url}/api/stream`);
		expect(response.headers.get("content-type")).toContain("text/event-stream");
		const reader = response.body?.getReader();
		const decoder = new TextDecoder();
		let text = "";
		// A real socket may coalesce the greeting and the snapshot into one chunk.
		for (let i = 0; i < 8 && !text.includes("event: snapshot"); i += 1) {
			const chunk = await reader?.read();
			if (chunk?.done) break;
			text += decoder.decode(chunk?.value);
		}
		expect(text).toContain("retry: 3000");
		expect(text).toContain("event: snapshot");
		expect(text).toContain("alive 4d 6h");
		await reader?.cancel();
	});
});
