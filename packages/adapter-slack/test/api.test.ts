import { expect, test } from "bun:test";
import { deliveryFailureIsAmbiguous, SlackApiError, SlackWebApi } from "../src/api";

function fixture() {
	const requests: { url: string; headers: Headers; body: Record<string, unknown>; method?: string }[] = [];
	let response = () => Response.json({ ok: true });
	const api = new SlackWebApi("xoxb-secret", async (input, init) => {
		requests.push({
			url: String(input),
			headers: new Headers(init?.headers),
			body: JSON.parse(String(init?.body)),
			method: init?.method,
		});
		return response();
	});
	return {
		api,
		requests,
		respond: (next: () => Response) => {
			response = next;
		},
	};
}

test("Slack calls use JSON POST and the appropriate bearer token", async () => {
	const f = fixture();
	f.respond(() => Response.json({ ok: true, value: 42 }));
	expect(await f.api.call<{ ok: boolean; value: number }>("test.method", { key: "value" })).toEqual({
		ok: true,
		value: 42,
	});
	expect(f.requests[0]?.url).toBe("https://slack.com/api/test.method");
	expect(f.requests[0]?.method).toBe("POST");
	expect(f.requests[0]?.headers.get("authorization")).toBe("Bearer xoxb-secret");
	expect(f.requests[0]?.headers.get("content-type")).toBe("application/json");
	expect(f.requests[0]?.body).toEqual({ key: "value" });
	await f.api.connectionsOpen("xapp-secret");
	expect(f.requests[1]?.url).toBe("https://slack.com/api/apps.connections.open");
	expect(f.requests[1]?.headers.get("authorization")).toBe("Bearer xapp-secret");
});

test("Slack errors preserve HTTP status and platform code", async () => {
	const f = fixture();
	for (const [status, body, code] of [
		[200, { ok: false, error: "invalid_auth" }, "invalid_auth"],
		[503, { ok: false, error: "unavailable" }, "unavailable"],
		[429, {}, "http_429"],
	] as const) {
		f.respond(() => Response.json(body, { status }));
		const error = await f.api.call("test").catch((error: unknown) => error);
		expect(error).toBeInstanceOf(SlackApiError);
		expect((error as SlackApiError).status).toBe(status);
		expect((error as SlackApiError).code).toBe(code);
	}
	for (const text of ["not JSON", "null", "[]"]) {
		f.respond(() => new Response(text));
		await expect(f.api.call("test")).rejects.toMatchObject({ code: "invalid_response" });
	}
});

test("Slack reactions are idempotent only for already_reacted", async () => {
	const f = fixture();
	f.respond(() => Response.json({ ok: false, error: "already_reacted" }));
	await f.api.addReaction("C1", "1.2", "eyes");
	expect(f.requests[0]?.body).toEqual({ channel: "C1", timestamp: "1.2", name: "eyes" });
	f.respond(() => Response.json({ ok: false, error: "invalid_name" }));
	await expect(f.api.addReaction("C1", "1.2", "bad")).rejects.toMatchObject({ code: "invalid_name" });
});

test("Slack message wrappers send optional threads and disable unfurls", async () => {
	const f = fixture();
	await f.api.postMessage("C1", "hello");
	await f.api.postMessage("C1", "reply", "1.2");
	expect(f.requests[0]?.body).toEqual({ channel: "C1", text: "hello", mrkdwn: true, unfurl_links: false });
	expect(f.requests[1]?.body.thread_ts).toBe("1.2");
	await f.api.updateMessage("C1", "1.2", "changed");
	await f.api.deleteMessage("C1", "1.2");
	expect(f.requests[2]?.body).toEqual({ channel: "C1", ts: "1.2", text: "changed" });
	expect(f.requests[3]?.body).toEqual({ channel: "C1", ts: "1.2" });
});

test("Slack identity and directory wrappers unwrap fields and history exposes cursors", async () => {
	const f = fixture();
	f.respond(() => Response.json({ ok: true, user: { id: "U1" }, channel: { id: "C1" } }));
	expect(await f.api.usersInfo("U1")).toEqual({ id: "U1" });
	expect(await f.api.conversationsInfo("C1")).toEqual({ id: "C1" });
	await f.api.authTest();
	expect(f.requests[2]?.url).toEndWith("auth.test");
	f.respond(() =>
		Response.json({ ok: true, messages: [{ ts: "1.2" }], has_more: true, response_metadata: { next_cursor: "next" } }),
	);
	expect(await f.api.conversationsHistory("C1", { limit: 3, inclusive: true })).toEqual({
		messages: [{ ts: "1.2" }],
		has_more: true,
		next_cursor: "next",
	});
	await f.api.conversationsReplies("C1", "1.2", { cursor: "next" });
	expect(f.requests[3]?.body).toEqual({ channel: "C1", limit: 3, inclusive: true });
	expect(f.requests[4]?.body).toEqual({ channel: "C1", ts: "1.2", cursor: "next" });
});

test("Slack response URLs receive JSON without leaking the bot credential", async () => {
	const f = fixture();
	f.respond(() => new Response("ok"));
	await f.api.respond("https://hooks.slack.com/commands/secret", { text: "reply" });
	expect(f.requests[0]?.url).toBe("https://hooks.slack.com/commands/secret");
	expect(f.requests[0]?.body).toEqual({ text: "reply" });
	expect(f.requests[0]?.headers.has("authorization")).toBe(false);
	f.respond(() => new Response("unavailable", { status: 503 }));
	await expect(f.api.respond("https://hooks.slack.com/commands/secret", {})).rejects.toMatchObject({
		status: 503,
		code: "http_503",
	});
});

test("Slack transport errors are ambiguous but API rejections are definitive", () => {
	expect(deliveryFailureIsAmbiguous(new SlackApiError(200, "invalid_auth"))).toBe(false);
	expect(deliveryFailureIsAmbiguous(new TypeError("network"))).toBe(true);
});
