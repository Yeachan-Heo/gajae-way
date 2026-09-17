import { afterEach, describe, expect, test } from "bun:test";
import { memoryAuditLog } from "../src/audit";
import { createAdminApp } from "../src/server";
import { FIXED_NOW, type Harness, harness, MONITOR, post } from "./fixture";

let open: Harness | undefined;

function app(...args: Parameters<typeof harness>): Harness {
	open?.stop();
	open = harness(...args);
	return open;
}

afterEach(() => {
	open?.stop();
	open = undefined;
});

describe("read surface", () => {
	test.each([
		["/api/status", "gateway.status"],
		["/api/sessions", "session.list"],
		["/api/jobs", "work.jobs"],
		["/api/monitors", "monitor.list"],
	])("%s proxies %s", async (path, method) => {
		const instance = app();
		const response = await instance.fetch(path);
		expect(response.status).toBe(200);
		await expect(response.json()).resolves.toMatchObject({ ok: true });
		expect(instance.calls).toEqual([{ method, params: undefined }]);
	});

	test("there is no route onto gateway.core: it is a capability, not a verb", async () => {
		const instance = app();
		expect((await instance.fetch("/api/core")).status).toBe(404);
		expect(instance.calls).toHaveLength(0);
	});

	test("serves the console at the root", async () => {
		const response = await app().fetch("/");
		expect(response.headers.get("content-type")).toContain("text/html");
		expect(await response.text()).toContain("gajae-way console");
	});

	test("a gateway failure is reported as 502, not a fake empty view", async () => {
		const instance = app({
			request: async () => {
				throw new Error("gateway socket closed");
			},
		});
		const response = await instance.fetch("/api/status");
		expect(response.status).toBe(502);
		const body = (await response.json()) as { ok: boolean; error: string };
		expect(body.ok).toBe(false);
		expect(body.error).toMatch(/socket closed/);
	});

	test("read routes refuse non-GET methods", async () => {
		for (const path of ["/api/status", "/api/snapshot", "/api/audit", "/api/operations", "/api/consequence"]) {
			expect((await app().fetch(path, post({}))).status).toBe(405);
		}
	});

	test("an unknown path is a 404 and never reaches the gateway", async () => {
		const instance = app();
		expect((await instance.fetch("/api/whatever")).status).toBe(404);
		expect(instance.calls).toHaveLength(0);
	});

	test("the snapshot route returns the projection, not raw protocol results", async () => {
		const response = await app().fetch("/api/snapshot");
		const body = (await response.json()) as { ok: boolean; result: { status: { fields: Record<string, string> } } };
		expect(body.ok).toBe(true);
		expect(body.result.status.fields.alive).toBe("alive 4d 6h");
		expect(body.result.status.fields.attention).toBe("nothing needs you");
	});

	test("the root still renders when the gateway is unreachable", async () => {
		const instance = app({ status: null });
		const response = await instance.fetch("/");
		expect(response.status).toBe(200);
		const html = await response.text();
		expect(html).toContain("gateway unreachable");
	});
});

describe("audit route", () => {
	test("the tail is readable and newest first", async () => {
		const auditLog = memoryAuditLog();
		const instance = createAdminApp({
			request: async () => ({}),
			auditLog,
			reconcileMs: 0,
			now: () => FIXED_NOW,
		});
		try {
			await auditLog.append({
				at: "2026-08-27T00:00:00.000Z",
				operationId: "ops.backup",
				actor: "a",
				decision: "allowed",
			});
			await auditLog.append({
				at: "2026-08-27T00:00:01.000Z",
				operationId: "ops.backup",
				actor: "b",
				decision: "rejected",
			});
			const response = await instance.handler(new Request("http://admin.test/api/audit"));
			const body = (await response.json()) as { result: { entries: { actor: string }[] } };
			expect(body.result.entries.map((entry) => entry.actor)).toEqual(["b", "a"]);
		} finally {
			instance.stop();
		}
	});

	test("the limit is clamped rather than trusted", async () => {
		const response = await app().fetch("/api/audit?limit=999999");
		expect(response.status).toBe(200);
		const second = await app().fetch("/api/audit?limit=not-a-number");
		expect(second.status).toBe(200);
	});
});

describe("consequence route", () => {
	test("computes the target's real facts from a live read", async () => {
		const instance = app();
		const response = await instance.fetch(`/api/consequence?operationId=monitor.remove&monitorId=${MONITOR.monitorId}`);
		expect(response.status).toBe(200);
		const body = (await response.json()) as {
			result: { targetName: string; facts: { label: string; value: string }[]; warning: string };
		};
		expect(body.result.targetName).toBe("weekday-review");
		expect(body.result.facts.find((fact) => fact.label === "Fires")?.value).toBe("weekdays 08:30");
		expect(body.result.facts.find((fact) => fact.label === "History")?.value).toContain("1 time in the last 7 days");
		expect(body.result.warning).toContain("cannot be undone");
	});

	test("refuses an operation that is not allowlisted", async () => {
		const response = await app().fetch("/api/consequence?operationId=chat.send&monitorId=x");
		expect(response.status).toBe(404);
	});

	test("requires a target", async () => {
		expect((await app().fetch("/api/consequence?operationId=monitor.remove")).status).toBe(400);
	});
});

describe("mutation gate over http", () => {
	test("a GET on the mutation path cannot trigger anything", async () => {
		const instance = app();
		const response = await instance.fetch("/api/mutations");
		expect(response.status).toBe(405);
		expect(instance.calls).toHaveLength(0);
	});

	test("an allowlisted operation with actor and confirmation runs and returns a receipt", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(200);
		const body = (await response.json()) as { receipt: { headline: string; audited: string } };
		expect(body.receipt.headline).toMatch(/^✓ Run an integrity check at \d\d:\d\d:\d\d$/);
		expect(body.receipt.audited).toBe("audited: actor=형님 · operation=ops.integrity · allowed");
		expect(instance.calls).toContainEqual({ method: "ops.integrity", params: undefined });
		expect(instance.audit.at(-1)).toMatchObject({ decision: "allowed", actor: "형님" });
	});

	test("a missing confirmation is refused with 428 and audited", async () => {
		const instance = app();
		const response = await instance.fetch("/api/mutations", post({ operationId: "ops.integrity", actor: "형님" }));
		expect(response.status).toBe(428);
		expect(instance.calls).toHaveLength(0);
		expect(instance.audit.at(-1)).toMatchObject({ decision: "rejected" });
	});

	test("a mismatched confirmation does not count", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "yes" }),
		);
		expect(response.status).toBe(428);
		expect(instance.calls).toHaveLength(0);
	});

	test("chat.send is not reachable: it is not on the allowlist", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "chat.send", actor: "형님", confirm: "chat.send", params: { text: "hi" } }),
		);
		expect(response.status).toBe(404);
		expect(instance.calls).toHaveLength(0);
		expect(instance.audit.at(-1)).toMatchObject({ decision: "rejected", operationId: "chat.send" });
	});

	test("an anonymous caller cannot mutate", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(401);
		expect(instance.calls).toHaveLength(0);
	});

	test("a deployment with mutations disabled refuses even a perfect request", async () => {
		const instance = app({ gate: { mutationsEnabled: false } });
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "ops.integrity", actor: "형님", confirm: "ops.integrity" }),
		);
		expect(response.status).toBe(403);
		expect(instance.calls).toHaveLength(0);
	});

	test("a non-json body is rejected before the gate", async () => {
		const instance = app();
		const response = await instance.fetch("/api/mutations", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: "not json",
		});
		expect(response.status).toBe(400);
		expect(instance.audit).toHaveLength(0);
	});

	test("typed params are forwarded to the underlying method", async () => {
		const instance = app();
		await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.test",
				actor: "형님",
				confirm: "monitor.test",
				params: { monitorId: MONITOR.monitorId, eventType: "review.due" },
			}),
		);
		expect(instance.calls).toContainEqual({
			method: "monitor.test",
			params: { monitorId: MONITOR.monitorId, eventType: "review.due" },
		});
	});

	test("/api/operations advertises exactly the allowlist, with its typed metadata", async () => {
		const response = await app().fetch("/api/operations");
		const body = (await response.json()) as {
			operations: { id: string; severity: string; fields: unknown[]; confirmToken?: string }[];
		};
		expect(body.operations.map((operation) => operation.id)).toEqual([
			"monitor.add",
			"monitor.remove",
			"monitor.test",
			"ops.backup",
			"ops.integrity",
		]);
		expect(body.operations.map((operation) => operation.id)).not.toContain("chat.send");
		const remove = body.operations.find((operation) => operation.id === "monitor.remove");
		expect(remove).toMatchObject({ severity: "high", confirmToken: "target-name" });
		expect(remove?.fields).toHaveLength(1);
	});
});

describe("target-name confirmation, layered on top of the gate", () => {
	test("removing a monitor requires typing its name, not the operation id", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.remove",
				actor: "형님",
				confirm: "monitor.remove",
				targetConfirm: "weekday review",
				params: { monitorId: MONITOR.monitorId },
			}),
		);
		expect(response.status).toBe(428);
		const body = (await response.json()) as { error: string };
		// The body is the reason alone; the status code is not restated inside it.
		expect(body.error).toBe('confirmation did not match. You typed "weekday review"; expected "weekday-review".');
		expect(instance.calls).not.toContainEqual({ method: "monitor.remove", params: { monitorId: MONITOR.monitorId } });
	});

	test("a refused target confirmation is still audited: the trail has no holes", async () => {
		const instance = app();
		await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.remove",
				actor: "형님",
				confirm: "monitor.remove",
				params: { monitorId: MONITOR.monitorId },
			}),
		);
		expect(instance.audit.at(-1)).toMatchObject({
			decision: "rejected",
			operationId: "monitor.remove",
			actor: "형님",
		});
		expect(instance.audit.at(-1)?.reason).toContain("confirmation did not match");
	});

	test("the correct monitor name lets it through", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.remove",
				actor: "형님",
				confirm: "monitor.remove",
				targetConfirm: "weekday-review",
				params: { monitorId: MONITOR.monitorId },
			}),
		);
		expect(response.status).toBe(200);
		expect(instance.calls).toContainEqual({ method: "monitor.remove", params: { monitorId: MONITOR.monitorId } });
	});

	test("the gate's own echo is still required even with a correct target name", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.remove",
				actor: "형님",
				targetConfirm: "weekday-review",
				params: { monitorId: MONITOR.monitorId },
			}),
		);
		expect(response.status).toBe(428);
		expect(instance.calls).not.toContainEqual({ method: "monitor.remove", params: { monitorId: MONITOR.monitorId } });
	});

	test("a missing target is refused before anything is dispatched, and audited", async () => {
		const instance = app();
		const response = await instance.fetch(
			"/api/mutations",
			post({ operationId: "monitor.remove", actor: "형님", confirm: "monitor.remove", targetConfirm: "x" }),
		);
		expect(response.status).toBe(400);
		expect(instance.calls).toHaveLength(0);
		expect(instance.audit.at(-1)).toMatchObject({ decision: "rejected", operationId: "monitor.remove" });
	});

	test("a target that cannot be read is a 502, and the attempt is still audited", async () => {
		const instance = app({ monitors: [] });
		const response = await instance.fetch(
			"/api/mutations",
			post({
				operationId: "monitor.remove",
				actor: "형님",
				confirm: "monitor.remove",
				targetConfirm: "whatever",
				params: { monitorId: "does-not-exist" },
			}),
		);
		expect(response.status).toBe(502);
		const body = (await response.json()) as { error: string };
		expect(body.error).toBe("the target could not be read: unknown monitorId");
		expect(instance.audit.at(-1)?.reason).toContain("the target could not be read");
		expect(instance.calls).not.toContainEqual({ method: "monitor.remove", params: { monitorId: "does-not-exist" } });
	});
});

describe("event stream", () => {
	test("the stream accepts GET only", async () => {
		expect((await app().fetch("/api/stream", post({}))).status).toBe(405);
	});

	test("a connecting client is sent retry plus a full snapshot, never a delta chain", async () => {
		const instance = app();
		const response = await instance.fetch("/api/stream");
		expect(response.headers.get("content-type")).toContain("text/event-stream");
		const reader = response.body?.getReader();
		expect(reader).toBeDefined();
		const decoder = new TextDecoder();
		let text = "";
		for (let i = 0; i < 8 && !text.includes("event: snapshot"); i += 1) {
			const chunk = await reader?.read();
			if (chunk?.done) break;
			text += decoder.decode(chunk?.value);
		}
		expect(text).toContain("retry: 3000");
		expect(text).toContain("event: snapshot");
		expect(text).toContain('"alive 4d 6h"');
		await reader?.cancel();
	});

	test("a live progress event turns into a live-work row", async () => {
		const instance = app();
		instance.emit("chat.progress", {
			turnId: "turn-1",
			origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" },
			elapsedMs: 192_000,
			toolCalls: 14,
			outputTokens: 3240,
		});
		const response = await instance.fetch("/api/snapshot");
		const body = (await response.json()) as {
			result: {
				live: { rows: { key: string; fields: Record<string, string> }[] };
				status: { fields: Record<string, string> };
			};
		};
		expect(body.result.live.rows).toHaveLength(1);
		expect(body.result.live.rows[0]?.fields.evidence).toBe("14 tool calls · 3,240 tokens out");
		expect(body.result.live.rows[0]?.fields.elapsed).toBe("3m 12s / 5m");
		expect(body.result.status.fields.working).toBe("1 working");
		// The tool and its stated intent are operator detail: shown here, never in chat.
		instance.emit("chat.progress", {
			turnId: "turn-1",
			origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" },
			elapsedMs: 200_000,
			toolCalls: 15,
			outputTokens: 3300,
			activity: { kind: "tool", label: "bash", detail: "Running the tests" },
		});
		const detailed = (await (await instance.fetch("/api/snapshot")).json()) as typeof body;
		expect(detailed.result.live.rows[0]?.fields.evidence).toBe(
			"15 tool calls · 3,300 tokens out · bash — Running the tests",
		);
	});

	test.each([
		[192_000, "● working"],
		[250_000, "▲ approaching timeout"],
		// A real run can exceed the ceiling before the kill lands. Saying
		// "approaching" there would reassure the owner about a run already over
		// budget, which is exactly the fake-liveness failure this console exists
		// to avoid.
		[463_000, "▲ past the 5m ceiling"],
	])("an elapsed of %ims reads as %s", async (elapsedMs, expected) => {
		const instance = app();
		instance.emit("chat.progress", {
			turnId: "turn-1",
			origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" },
			elapsedMs,
			toolCalls: 13,
			outputTokens: 5635,
		});
		const body = (await (await instance.fetch("/api/snapshot")).json()) as {
			result: { live: { rows: { tone: string; fields: Record<string, string> }[] } };
		};
		expect(body.result.live.rows[0]?.fields.stateLabel).toBe(expected);
		expect(body.result.live.rows[0]?.tone).toBe(elapsedMs >= 300_000 ? "warn" : "active");
	});

	test("a turn only ever seen as a final message reports absence of evidence, not zeros", async () => {
		const instance = app();
		instance.emit("chat.message", {
			turnId: "fast",
			origin: { platform: "discord", kind: "channel", conversationId: "1493635653441945762" },
			role: "assistant",
			text: "done",
			final: true,
		});
		const body = (await (await instance.fetch("/api/snapshot")).json()) as {
			result: { live: { rows: { fields: Record<string, string>; meters?: unknown }[] } };
		};
		const row = body.result.live.rows[0];
		expect(row?.fields.evidence).toBe("no progress heartbeat was seen for this turn");
		expect(row?.fields.elapsed).toBe("duration not observed");
		expect(row?.meters).toBeUndefined();
	});

	test("a final chat message terminates the turn without deleting the row", async () => {
		const instance = app();
		const origin = { platform: "discord", kind: "channel", conversationId: "1493635653441945762" } as const;
		instance.emit("chat.progress", { turnId: "turn-1", origin, elapsedMs: 1000, toolCalls: 1, outputTokens: 10 });
		instance.emit("chat.message", { turnId: "turn-1", origin, role: "assistant", text: "[SILENT]", final: true });
		const body = (await (await instance.fetch("/api/snapshot")).json()) as {
			result: {
				live: { rows: { state?: string; fields: Record<string, string> }[] };
				status: { fields: Record<string, string> };
			};
		};
		expect(body.result.live.rows[0]?.state).toBe("finished");
		expect(body.result.live.rows[0]?.fields.stateLabel).toContain("nothing sent");
		expect(body.result.status.fields.working).toBe("idle");
	});

	test("a non-final chat message does not terminate anything", async () => {
		const instance = app();
		const origin = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;
		instance.emit("chat.progress", { turnId: "turn-1", origin, elapsedMs: 1000, toolCalls: 0, outputTokens: 0 });
		instance.emit("chat.message", { turnId: "turn-1", origin, role: "assistant", text: "partial", final: false });
		const body = (await (await instance.fetch("/api/snapshot")).json()) as {
			result: { live: { rows: { state?: string }[] } };
		};
		expect(body.result.live.rows[0]?.state).toBe("running");
	});
});
