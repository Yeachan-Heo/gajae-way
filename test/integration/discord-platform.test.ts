import { expect, test } from "bun:test";
import { DiscordGatewayPlatform, type DiscordMessage } from "../../src/adapter/discord/platform";


class GatewaySocketFixture {
	readonly readyState = 1;
	readonly sent: string[] = [];
	readonly #listeners = new Map<string, Set<(event: unknown) => void>>();

	send(data: string): void {
		this.sent.push(data);
	}

	close(code = 1_000, reason = ""): void {
		this.emit("close", { code, reason });
	}

	addEventListener(type: string, callback: (event: unknown) => void): void {
		const callbacks = this.#listeners.get(type) ?? new Set();
		callbacks.add(callback);
		this.#listeners.set(type, callbacks);
	}

	removeEventListener(type: string, callback: (event: unknown) => void): void {
		this.#listeners.get(type)?.delete(callback);
	}

	emit(type: string, event: unknown): void {
		for (const callback of this.#listeners.get(type) ?? []) callback(event);
	}
}

async function waitFor(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	throw new Error("Timed out waiting for fixture condition.");
}

test("hand-rolled Discord platform identifies, heartbeats, resumes, routes MESSAGE_CREATE, and uses REST effects", async () => {
	const sockets: GatewaySocketFixture[] = [];
	const requests: Array<{ url: string; method: string; body?: string }> = [];
	const platform = new DiscordGatewayPlatform({
		token: "test-token",
		fetch: async (input, init) => {
			const url = String(input);
			const method = init?.method ?? "GET";
			requests.push({ url, method, ...(typeof init?.body === "string" ? { body: init.body } : {}) });
			if (url.endsWith("/gateway/bot")) return new Response(JSON.stringify({ url: "wss://gateway.test" }), { status: 200 });
			if (url.endsWith("/users/@me")) return new Response(JSON.stringify({ id: "999999999999999999", username: "fixture-bot" }), { status: 200 });
			if (url.endsWith("/channels/123456789012345678/messages/888888888888888888") && method === "GET") {
				return new Response(JSON.stringify({ id: "888888888888888888", author: { id: "999999999999999999" } }), { status: 200 });
			}

			if (url.includes("/messages") && method === "POST") return new Response(JSON.stringify({ id: "platform-message-1" }), { status: 200 });
			if (url.endsWith("/channels/222222222222222222") && method === "GET") {
				return new Response(JSON.stringify({ id: "222222222222222222", type: 11, parent_id: "123456789012345678" }), { status: 200 });
			}

			return new Response(null, { status: 204 });
		},
		webSocketFactory: () => {
			const socket = new GatewaySocketFixture();
			sockets.push(socket);
			return socket;
		},
		reconnectBaseMs: 1,
		reconnectMaxMs: 2,
	});
	const messages: DiscordMessage[] = [];
	platform.onMessage(message => {
		messages.push(message);
	});

	try {
		const connecting = platform.connect();
		await waitFor(() => sockets.length === 1);
		const first = sockets[0] as GatewaySocketFixture;
		first.emit("message", { data: JSON.stringify({ op: 10, d: { heartbeat_interval: 60_000 } }) });
		expect(first.sent.map(entry => JSON.parse(entry))).toEqual(
			expect.arrayContaining([expect.objectContaining({ op: 1 }), expect.objectContaining({ op: 2, d: expect.objectContaining({ intents: 37_376 }) })]),

		);
		first.emit(
			"message",
			{ data: JSON.stringify({ op: 0, s: 7, t: "READY", d: { session_id: "session-1", resume_gateway_url: "wss://resume.test" } }) },
		);
		await connecting;
		first.emit(
			"message",
			{
				data: JSON.stringify({
					op: 0,
					s: 8,
					t: "MESSAGE_CREATE",
					d: {
						id: "incoming-1",
						channel_id: "123456789012345678",
						content: "<@999999999999999999> owner message",
						author: { id: "111111111111111111", bot: false },
						mentions: [{ id: "999999999999999999" }],
						message_reference: { message_id: "888888888888888888" },
						referenced_message: { author: { id: "999999999999999999" } },
					},

				}),
			},
		);
		await waitFor(() => messages.length === 1);
		expect(messages).toEqual([
			expect.objectContaining({
				id: "incoming-1",
				text: "<@999999999999999999> owner message",
				mentionedUserIds: ["999999999999999999"],
				messageReference: { channelId: "123456789012345678", messageId: "888888888888888888" },
				referencedMessageAuthorId: "999999999999999999",
			}),
		]);

		expect(await platform.send("123456789012345678", "reply", "nonce-1")).toBe("platform-message-1");
		await platform.ackTyping("123456789012345678");
		await platform.react("123456789012345678", "incoming-1", "👀");
		expect(await platform.resolveThreadParent("222222222222222222")).toBe("123456789012345678");
		expect(await platform.resolveThreadParent("222222222222222222")).toBe("123456789012345678");
		expect(await platform.getCurrentUser()).toEqual({ id: "999999999999999999", username: "fixture-bot" });
		expect(await platform.getCurrentUser()).toEqual({ id: "999999999999999999", username: "fixture-bot" });
		expect(await platform.resolveMessageAuthor("123456789012345678", "888888888888888888")).toBe("999999999999999999");
		expect(await platform.resolveMessageAuthor("123456789012345678", "888888888888888888")).toBe("999999999999999999");


		expect(requests).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ url: "https://discord.com/api/v10/gateway/bot", method: "GET" }),
				expect.objectContaining({ method: "POST", body: JSON.stringify({ content: "reply", nonce: "nonce-1", enforce_nonce: true }) }),
				expect.objectContaining({ url: "https://discord.com/api/v10/channels/123456789012345678/typing", method: "POST" }),
				expect.objectContaining({ url: "https://discord.com/api/v10/channels/222222222222222222", method: "GET" }),
				expect.objectContaining({ url: "https://discord.com/api/v10/users/@me", method: "GET" }),
				expect.objectContaining({ url: "https://discord.com/api/v10/channels/123456789012345678/messages/888888888888888888", method: "GET" }),


			]),
		);
		expect(requests.filter(request => request.url.endsWith("/channels/222222222222222222") && request.method === "GET")).toHaveLength(1);
		expect(requests.filter(request => request.url.endsWith("/users/@me") && request.method === "GET")).toHaveLength(1);
		expect(requests.filter(request => request.url.endsWith("/channels/123456789012345678/messages/888888888888888888") && request.method === "GET")).toHaveLength(1);

		first.close(1_006, "network");
		await waitFor(() => sockets.length === 2);
		const second = sockets[1] as GatewaySocketFixture;
		second.emit("message", { data: JSON.stringify({ op: 10, d: { heartbeat_interval: 60_000 } }) });
		expect(second.sent.map(entry => JSON.parse(entry))).toEqual(
			expect.arrayContaining([expect.objectContaining({ op: 6, d: expect.objectContaining({ session_id: "session-1", seq: 8 }) })]),
		);
		second.emit("message", { data: JSON.stringify({ op: 0, s: 8, t: "RESUMED", d: {} }) });
	} finally {
		await platform.disconnect();
	}
});

test("hello schedules the real heartbeat interval so a latent ACK cannot kill the pre-READY connection", async () => {
	const sockets: GatewaySocketFixture[] = [];
	const closes: Array<{ code: number; reason: string }> = [];
	const platform = new DiscordGatewayPlatform({
		token: "test-token",
		fetch: async input => {
			if (String(input).endsWith("/gateway/bot")) return new Response(JSON.stringify({ url: "wss://gateway.test" }), { status: 200 });
			return new Response(null, { status: 204 });
		},
		webSocketFactory: () => {
			const socket = new GatewaySocketFixture();
			const close = socket.close.bind(socket);
			socket.close = (code = 1_000, reason = "") => {
				closes.push({ code, reason });
				close(code, reason);
			};
			sockets.push(socket);
			return socket;
		},
		reconnectBaseMs: 1,
		reconnectMaxMs: 2,
	});
	try {
		const connecting = platform.connect();
		await waitFor(() => sockets.length === 1);
		const socket = sockets[0] as GatewaySocketFixture;
		socket.emit("message", { data: JSON.stringify({ op: 10, d: { heartbeat_interval: 60_000 } }) });
		// Real Discord acknowledges the first heartbeat over network latency. The
		// regression (interval scheduled as 0ms) closed the socket with
		// "Discord heartbeat ACK missing" within this window.
		await Bun.sleep(40);
		expect(closes).toEqual([]);
		const beats = socket.sent.map(entry => JSON.parse(entry)).filter(payload => payload.op === 1);
		expect(beats).toHaveLength(1);
		socket.emit("message", { data: JSON.stringify({ op: 11, d: null }) });
		socket.emit("message", { data: JSON.stringify({ op: 0, s: 1, t: "READY", d: { session_id: "session-latency" } }) });
		await connecting;
		expect(closes).toEqual([]);
	} finally {
		await platform.disconnect();
	}
});
