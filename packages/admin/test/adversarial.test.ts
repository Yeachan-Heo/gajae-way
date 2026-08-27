/**
 * Adversarial cases over the mutation path.
 *
 * These are not variations on the happy path. Every one of them is an attempt to
 * get a gateway method dispatched, or an audit entry skipped, or markup into the
 * document, without satisfying the contract. Two of them found real holes when
 * they were first written - a JSON body of `null` threw out of the handler, and a
 * zero-width actor satisfied "an actor is required" while leaving the audit trail
 * attributed to nothing - so they stay here permanently.
 */
import { describe, expect, test } from "bun:test";
import { DEFAULT_ALLOWLIST } from "../src/gate";
import { StreamHub } from "../src/stream";
import { TurnTracker } from "../src/turns";
import { renderIndex } from "../src/ui";
import { buildSnapshot } from "../src/view";
import { FIXED_NOW, type Harness, harness, MONITOR, STATUS } from "./fixture";

/** Every allowlisted method. None may be dispatched by any case below. */
const MUTATING = ["monitor.add", "monitor.remove", "monitor.test", "ops.backup", "ops.integrity"];

function assertNothingDispatched(app: Harness): void {
	for (const call of app.calls) expect(MUTATING).not.toContain(call.method);
}

const raw = (body: string): RequestInit => ({
	method: "POST",
	headers: { "content-type": "application/json" },
	body,
});

describe("nothing dispatches without the full contract", () => {
	test.each([
		["confirm as a number", '{"operationId":"ops.integrity","actor":"a","confirm":1}'],
		["confirm as true", '{"operationId":"ops.integrity","actor":"a","confirm":true}'],
		["confirm as an array", '{"operationId":"ops.integrity","actor":"a","confirm":["ops.integrity"]}'],
		["confirm as an object", '{"operationId":"ops.integrity","actor":"a","confirm":{"v":"ops.integrity"}}'],
		["confirm as null", '{"operationId":"ops.integrity","actor":"a","confirm":null}'],
		["operationId as a number", '{"operationId":1,"actor":"a","confirm":"1"}'],
		["operationId as __proto__", '{"operationId":"__proto__","actor":"a","confirm":"__proto__"}'],
		["operationId as constructor", '{"operationId":"constructor","actor":"a","confirm":"constructor"}'],
		["operationId as toString", '{"operationId":"toString","actor":"a","confirm":"toString"}'],
		["operationId padded", '{"operationId":" ops.integrity","actor":"a","confirm":" ops.integrity"}'],
		["operationId uppercased", '{"operationId":"OPS.INTEGRITY","actor":"a","confirm":"OPS.INTEGRITY"}'],
		["actor as a number", '{"operationId":"ops.integrity","actor":7,"confirm":"ops.integrity"}'],
		["actor as whitespace", '{"operationId":"ops.integrity","actor":"   ","confirm":"ops.integrity"}'],
		["actor as a zero-width space", '{"operationId":"ops.integrity","actor":"\\u200b","confirm":"ops.integrity"}'],
		["actor as a byte-order mark", '{"operationId":"ops.integrity","actor":"\\ufeff","confirm":"ops.integrity"}'],
		["actor as a word joiner", '{"operationId":"ops.integrity","actor":"\\u2060","confirm":"ops.integrity"}'],
		["gateway.shutdown", '{"operationId":"gateway.shutdown","actor":"a","confirm":"gateway.shutdown"}'],
		["chat.send", '{"operationId":"chat.send","actor":"a","confirm":"chat.send"}'],
		["chat.send uppercased", '{"operationId":"CHAT.SEND","actor":"a","confirm":"CHAT.SEND"}'],
		["chat.send padded", '{"operationId":" chat.send","actor":"a","confirm":" chat.send"}'],
		["a bare array body", "[]"],
		["a bare number body", "3"],
		["a null body", "null"],
		["a bare string body", '"ops.integrity"'],
		["an empty object body", "{}"],
	])("%s is refused and dispatches nothing", async (_label, body) => {
		const app = harness();
		try {
			const response = await app.fetch("/api/mutations", raw(body));
			expect(response.status).toBeGreaterThanOrEqual(400);
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("an invisible actor is not attribution: it lands on the gate's 401 and is audited", async () => {
		const app = harness();
		try {
			const response = await app.fetch(
				"/api/mutations",
				raw('{"operationId":"ops.integrity","actor":"\\u200b","confirm":"ops.integrity"}'),
			);
			expect(response.status).toBe(401);
			expect(app.audit.at(-1)).toMatchObject({ decision: "rejected", actor: "anonymous" });
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("a visible actor with surrounding zero-width padding still counts", async () => {
		const app = harness();
		try {
			const response = await app.fetch(
				"/api/mutations",
				raw('{"operationId":"ops.integrity","actor":"\\u200b형님\\u200b","confirm":"ops.integrity"}'),
			);
			expect(response.status).toBe(200);
		} finally {
			app.stop();
		}
	});

	test.each([
		"HEAD",
		"PUT",
		"PATCH",
		"DELETE",
		"OPTIONS",
		"GET",
	])("%s on the mutation path is refused", async (method) => {
		const app = harness();
		try {
			const response = await app.fetch("/api/mutations", { method });
			expect(response.status).toBe(405);
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("params cannot pollute the prototype", async () => {
		const app = harness();
		try {
			await app.fetch(
				"/api/mutations",
				raw(
					'{"operationId":"monitor.test","actor":"a","confirm":"monitor.test","params":{"__proto__":{"polluted":1}}}',
				),
			);
			expect(({} as Record<string, unknown>).polluted).toBeUndefined();
		} finally {
			app.stop();
		}
	});
});

describe("no refusal path is silent", () => {
	test.each([
		["unallowlisted", '{"operationId":"chat.send","actor":"a","confirm":"chat.send"}'],
		["missing actor", '{"operationId":"ops.integrity","confirm":"ops.integrity"}'],
		["invisible actor", '{"operationId":"ops.integrity","actor":"\\u200b","confirm":"ops.integrity"}'],
		["missing confirm", '{"operationId":"ops.integrity","actor":"a"}'],
		["wrong confirm", '{"operationId":"ops.integrity","actor":"a","confirm":"nope"}'],
		["missing target", '{"operationId":"monitor.remove","actor":"a","confirm":"monitor.remove"}'],
		[
			"unreadable target",
			'{"operationId":"monitor.remove","actor":"a","confirm":"monitor.remove","targetConfirm":"x","params":{"monitorId":"ghost"}}',
		],
		[
			"wrong target name",
			'{"operationId":"monitor.remove","actor":"a","confirm":"monitor.remove","targetConfirm":"wrong","params":{"monitorId":"mon-weekday-review-0001"}}',
		],
	])("%s is audited with a reason", async (_label, body) => {
		const app = harness();
		try {
			const response = await app.fetch("/api/mutations", raw(body));
			expect(response.status).toBeGreaterThanOrEqual(400);
			expect(app.audit.at(-1)?.decision).toBe("rejected");
			expect(app.audit.at(-1)?.reason?.length ?? 0).toBeGreaterThan(0);
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("a malformed body is refused before the gate, so it cannot forge an audit entry", async () => {
		const app = harness();
		try {
			for (const body of ["not json", "null", "[]", "3"]) {
				expect((await app.fetch("/api/mutations", raw(body))).status).toBe(400);
			}
			expect(app.audit).toHaveLength(0);
		} finally {
			app.stop();
		}
	});
});

describe("the target-name check is server-side", () => {
	const remove = (targetConfirm: string | null, confirm = "monitor.remove"): RequestInit =>
		raw(
			JSON.stringify({
				operationId: "monitor.remove",
				actor: "형님",
				...(confirm === "" ? {} : { confirm }),
				...(targetConfirm === null ? {} : { targetConfirm }),
				params: { monitorId: MONITOR.monitorId },
			}),
		);

	test.each([
		["a case-different name", "WEEKDAY-REVIEW"],
		["a padded name", " weekday-review "],
		["an underscored name", "weekday_review"],
		["an empty name", ""],
		["the operation id instead of the name", "monitor.remove"],
	])("%s is refused", async (_label, typed) => {
		const app = harness();
		try {
			const response = await app.fetch("/api/mutations", remove(typed));
			expect(response.status).toBe(428);
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("the gate's own echo is still required when the target name is correct", async () => {
		const app = harness();
		try {
			const response = await app.fetch("/api/mutations", remove("weekday-review", ""));
			expect(response.status).toBe(428);
			assertNothingDispatched(app);
		} finally {
			app.stop();
		}
	});

	test("only the exact name, with the echo, gets through", async () => {
		const app = harness();
		try {
			expect((await app.fetch("/api/mutations", remove("weekday-review"))).status).toBe(200);
			expect(app.calls.map((call) => call.method)).toContain("monitor.remove");
		} finally {
			app.stop();
		}
	});
});

describe("hostile gateway data cannot become markup", () => {
	const HOSTILE = [
		"<script>alert(1)</script>",
		"</script><script>alert(1)</script>",
		"</title></head><body>",
		'" onload="alert(1)',
		"'; alert(1); '",
		"\u202eevil",
		"\u0000null-byte",
		"\ud800",
		"x".repeat(20_000),
	];

	async function documentWith(value: string): Promise<string> {
		const snapshot = await buildSnapshot({
			request: async (method) => {
				switch (method) {
					case "gateway.status":
						return { ...STATUS, profileVersion: value };
					case "session.list":
						return { sessions: [] };
					case "monitor.list":
						return { monitors: [{ ...MONITOR, name: value, eventTypes: [value] }] };
					case "monitor.inspect":
						return { monitor: { ...MONITOR, name: value }, recentEvents: [] };
					default:
						return {};
				}
			},
			turns: new TurnTracker(() => FIXED_NOW.getTime()),
			now: () => FIXED_NOW,
		});
		return renderIndex(snapshot, DEFAULT_ALLOWLIST);
	}

	test.each(
		HOSTILE.map((value, index) => [index, value] as const),
	)("payload %i introduces no script element and closes none", async (_index, value) => {
		const html = await documentWith(value);
		expect(html).not.toContain("<script>alert");
		// Exactly the three script elements this document authors: two data
		// islands and the body. Any injection would change the count.
		expect(html.match(/<script/g)?.length).toBe(3);
		expect(html.match(/<\/script>/g)?.length).toBe(3);
		const bootstrap = html.slice(html.indexOf('id="bootstrap"'));
		expect(bootstrap).not.toContain("</script><script>alert");
	});
});

describe("a hostile or broken gateway never breaks the page", () => {
	async function monitorsPanel(value: unknown) {
		const snapshot = await buildSnapshot({
			request: async (method) => {
				if (method === "monitor.list") return value;
				if (method === "gateway.status") return STATUS;
				if (method === "session.list") return { sessions: [] };
				return {};
			},
			turns: new TurnTracker(() => FIXED_NOW.getTime()),
			now: () => FIXED_NOW,
		});
		return snapshot.monitors;
	}

	test.each([
		["null", null],
		["undefined", undefined],
		["a string", "nope"],
		["a number", 42],
		["the wrong shape", { unexpected: true }],
		["an empty list", { monitors: [] }],
		["a monitor with an unparseable cron", { monitors: [{ ...MONITOR, trigger: { kind: "cron", schedule: "nope" } }] }],
	])("monitor.list returning %s degrades to a stated panel", async (_label, value) => {
		const panel = await monitorsPanel(value);
		expect(["ready", "empty", "error", "blocked"]).toContain(panel.state);
		expect(panel.note.length).toBeGreaterThan(0);
	});

	test("a read that rejects degrades its own panel and names the failure", async () => {
		const app = harness({
			request: async (method) => {
				if (method === "session.list") throw new Error("request timed out after 30000ms");
				if (method === "gateway.status") return STATUS;
				if (method === "monitor.list") return { monitors: [] };
				return {};
			},
		});
		try {
			const response = await app.fetch("/api/snapshot");
			expect(response.status).toBe(200);
			const body = (await response.json()) as { result: { sessions: { state: string; note: string } } };
			expect(body.result.sessions.state).toBe("error");
			expect(body.result.sessions.note).toContain("timed out");
		} finally {
			app.stop();
		}
	});

	test("the root route still renders a document when every read fails", async () => {
		const app = harness({
			request: async () => {
				throw new Error("gateway socket closed");
			},
		});
		try {
			const response = await app.fetch("/");
			expect(response.status).toBe(200);
			const html = await response.text();
			expect(html).toStartWith("<!doctype html>");
			expect(html).toContain("gateway unreachable");
		} finally {
			app.stop();
		}
	});
});

describe("SSE framing survives a hostile payload", () => {
	test("newlines and a literal data: line stay inside one frame", async () => {
		const hub = new StreamHub({
			snapshot: async () => ({ note: "line one\nline two\r\ndata: injected\n\nevent: fake" }),
			gatewayReachable: () => true,
		});
		try {
			const reader = hub.connect().body?.getReader();
			const decoder = new TextDecoder();
			let text = "";
			for (let i = 0; i < 8 && !text.includes("event: snapshot"); i += 1) {
				const chunk = await reader?.read();
				if (chunk?.done) break;
				text += decoder.decode(chunk?.value);
			}
			expect(text.split("\n").filter((line) => line.startsWith("data: "))).toHaveLength(1);
			expect(text.split("\n").filter((line) => line.startsWith("event: "))).toHaveLength(1);
			await reader?.cancel();
		} finally {
			hub.close();
		}
	});

	test("many clients register, a cancelled one does not poison a broadcast, and stop closes all", async () => {
		const hub = new StreamHub({ snapshot: async () => ({ at: "x" }), gatewayReachable: () => true });
		const readers = Array.from({ length: 25 }, () => hub.connect().body?.getReader());
		for (const reader of readers) {
			// Drain the greeting and the snapshot so a later broadcast is observable.
			await reader?.read();
			await reader?.read();
		}
		expect(hub.clientCount).toBe(25);
		await readers[0]?.cancel();
		expect(() => hub.broadcast("monitor.event", { eventId: "e" })).not.toThrow();
		hub.close();
		expect(hub.clientCount).toBe(0);
		for (const reader of readers.slice(1)) await reader?.cancel().catch(() => {});
	});
});
