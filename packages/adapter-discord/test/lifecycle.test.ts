import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajaeway/protocol";
import {
	type DiscordAdapterClient,
	type DiscordAdapterInput,
	DiscordGatewayLink,
	type Generation,
	type OpenGatewayClient,
	startDiscordAdapterWithClient,
} from "../src/main";
import { recoveryCursorPath, snowflakeFromTimestamp } from "../src/recovery";

const input = (home: string, recoveryChannels: readonly string[] = []): DiscordAdapterInput => ({
	token: "token",
	recoveryChannels,
	recoveryCursorPath: recoveryCursorPath(home),
});

test("stop destroys once, closes its port, and ignores later Discord faults", async () => {
	const home = await temporaryHome();
	try {
		const discord = fakeDiscord();
		const port = fakePort();
		const handle = await startDiscordAdapterWithClient(
			input(home),
			port.gateway,
			generation(),
			discord.client,
			quietLog,
		);
		await Promise.all([handle.stop(), handle.stop()]);
		discord.emit("shardDisconnect", { code: 4004 });
		expect(discord.destroyed).toBe(1);
		expect(port.closed).toBe(1);
		await expect(handle.settled).resolves.toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("settled rejects when login rejects", async () => {
	const home = await temporaryHome();
	try {
		const discord = fakeDiscord(() => Promise.reject(new Error("invalid token")));
		const port = fakePort();
		const handle = await startDiscordAdapterWithClient(
			input(home),
			port.gateway,
			generation(),
			discord.client,
			quietLog,
		);
		await expect(handle.settled).rejects.toThrow("invalid token");
		await handle.stop();
		expect(discord.destroyed).toBe(1);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("each unrecoverable Discord close code rejects settled exactly once", async () => {
	const home = await temporaryHome();
	try {
		for (const code of [4004, 4010, 4011, 4012, 4013, 4014]) {
			const discord = fakeDiscord();
			const port = fakePort();
			const handle = await startDiscordAdapterWithClient(
				input(home),
				port.gateway,
				generation(),
				discord.client,
				quietLog,
			);
			discord.emit("shardDisconnect", { code });
			discord.emit("shardDisconnect", { code });
			await expect(handle.settled).rejects.toThrow(`discord_unrecoverable_close code=${code}`);
			await handle.stop();
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reconnecting and shard errors are logged but never fault the generation", async () => {
	const home = await temporaryHome();
	try {
		const errors: string[] = [];
		const discord = fakeDiscord();
		const port = fakePort();
		const handle = await startDiscordAdapterWithClient(input(home), port.gateway, generation(), discord.client, {
			log: () => {},
			error: (line) => errors.push(line),
		});
		discord.emit("shardReconnecting");
		discord.emit("shardError", new Error("temporary"));
		await handle.stop();
		await expect(handle.settled).resolves.toBeUndefined();
		expect(errors.join("\n")).toContain("temporary");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("stop resolves during a pending login and disposes its late result", async () => {
	const home = await temporaryHome();
	try {
		const login = deferred<unknown>();
		const discord = fakeDiscord(() => login.promise);
		const port = fakePort();
		const handle = await startDiscordAdapterWithClient(
			input(home),
			port.gateway,
			generation(),
			discord.client,
			quietLog,
		);
		await handle.stop();
		login.resolve({});
		await Bun.sleep(0);
		expect(discord.destroyed).toBe(1);
		await expect(handle.settled).resolves.toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a replay emitted by open reaches the delivery subscription", async () => {
	const home = await temporaryHome();
	try {
		const discord = fakeDiscord();
		const replay = delivery("replayed");
		const port = fakePort(replay);
		const handle = await startDiscordAdapterWithClient(
			input(home),
			port.gateway,
			generation(),
			discord.client,
			quietLog,
		);
		await until(() => discord.sent.includes("replayed"));
		expect(port.requests).toContainEqual({ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } });
		await handle.stop();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("stop clears active typing and working status UI", async () => {
	const home = await temporaryHome();
	try {
		const discord = fakeDiscord();
		const port = fakePort();
		const handle = await startDiscordAdapterWithClient(
			input(home),
			port.gateway,
			generation(),
			discord.client,
			quietLog,
		);
		port.emit("chat.progress", {
			turnId: "turn-1",
			origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
			elapsedMs: 1,
			toolCalls: 0,
			outputTokens: 0,
			final: false,
		});
		discord.emit("messageCreate", {
			id: "message-1",
			content: "hello",
			author: { id: "human" },
			channel: { id: "channel-1" },
			mentions: { has: () => false },
		});
		await until(() => discord.typing > 0 && discord.sent.some((entry) => String(entry).startsWith("⏳ working")));
		await handle.stop();
		const typingAtStop = discord.typing;
		await Bun.sleep(20);
		expect(discord.typing).toBe(typingAtStop);
		expect(discord.deleted).toBeGreaterThanOrEqual(1);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("recovery uses the injected cursor path and stop cancels a scheduled retry", async () => {
	const home = await temporaryHome();
	try {
		const cursorPath = recoveryCursorPath(home);
		const message = recoveryMessage();
		const link = new DiscordGatewayLink(
			{ request: async () => ({}) } as never,
			{ channels: { fetch: async () => ({ messages: { fetch: async () => [message] } }) } },
			input(home, ["channel-1"]),
			undefined,
			undefined,
			cursorPath,
			() => ({ id: "bot" }),
			async () => {},
			undefined,
			generation(),
		);
		await link.recoverMissedMessages();
		await link.cursorsFlushed;
		expect(await Bun.file(cursorPath).exists()).toBe(true);

		const retrying = new DiscordGatewayLink(
			{ request: async () => Promise.reject(new Error("gateway unavailable")) } as never,
			{ channels: { fetch: async () => ({ messages: { fetch: async () => [message] } }) } },
			input(home, ["channel-1"]),
			undefined,
			undefined,
			join(home, "retry.json"),
			() => ({ id: "bot" }),
			async () => {},
			undefined,
			generation(),
		);
		await retrying.recoverMissedMessages();
		expect(retrying.recoveryRetryPending).toBe(true);
		await retrying.stop();
		expect(retrying.recoveryRetryPending).toBe(false);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

const quietLog = { log: () => {}, error: () => {} };

function fakeDiscord(login: () => Promise<unknown> = async () => ({})) {
	const handlers = new Map<string, Array<(...args: unknown[]) => void>>();
	const sent: unknown[] = [];
	let destroyed = 0;
	let typing = 0;
	let deleted = 0;
	const add = (event: string, handler: (...args: unknown[]) => void): void => {
		const listeners = handlers.get(event) ?? [];
		listeners.push(handler);
		handlers.set(event, listeners);
	};
	const client: DiscordAdapterClient = {
		user: { id: "bot" },
		channels: {
			fetch: async () => ({
				send: async (payload: unknown) => {
					sent.push(payload);
					return { edit: async () => {}, delete: async () => void deleted++ };
				},
				sendTyping: async () => void typing++,
			}),
		},
		on: (event, handler) => add(event, handler),
		once: (event, handler) => {
			const once = (...args: unknown[]) => {
				handlers.set(
					event,
					(handlers.get(event) ?? []).filter((listener) => listener !== once),
				);
				handler(...args);
			};
			add(event, once);
		},
		login,
		destroy: () => void destroyed++,
	};
	return {
		client,
		emit: (event: string, ...args: unknown[]) => {
			for (const handler of handlers.get(event) ?? []) handler(...args);
		},
		get destroyed() {
			return destroyed;
		},
		get sent() {
			return sent;
		},
		get typing() {
			return typing;
		},
		get deleted() {
			return deleted;
		},
	};
}

function fakePort(replay?: ChatMessagePayload) {
	const handlers = new Map<string, Set<(payload: unknown) => void>>();
	const requests: Array<{ verb: string; params: unknown }> = [];
	let closed = 0;
	const gateway: OpenGatewayClient = {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return (verb === "chat.send" ? { engaged: true } : {}) as T;
		},
		onChatMessage: (handler) => {
			const listeners = handlers.get("chat.message") ?? new Set();
			listeners.add(handler as (payload: unknown) => void);
			handlers.set("chat.message", listeners);
			return () => listeners.delete(handler as (payload: unknown) => void);
		},
		onChatProgress: (handler) => {
			const listeners = handlers.get("chat.progress") ?? new Set();
			listeners.add(handler as (payload: unknown) => void);
			handlers.set("chat.progress", listeners);
			return () => listeners.delete(handler as (payload: unknown) => void);
		},
		open: async () => {
			if (replay) for (const handler of handlers.get("chat.message") ?? []) handler(replay);
			return { replayed: replay ? 1 : 0 };
		},
		close: () => void closed++,
	};
	return {
		gateway,
		requests,
		emit: (event: string, payload: unknown) => {
			for (const handler of handlers.get(event) ?? []) handler(payload);
		},
		get closed() {
			return closed;
		},
	};
}

function generation(): Generation {
	const controller = new AbortController();
	return {
		id: 1,
		signal: controller.signal,
		port: {} as never,
		track: (task) => task,
		sleep: async () => {},
	};
}

function delivery(text: string): ChatMessagePayload {
	return {
		turnId: "turn-1",
		origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
		role: "assistant",
		text,
		final: true,
		deliveryId: "delivery-1",
	};
}

function recoveryMessage() {
	return {
		id: snowflakeFromTimestamp(Date.now() - 1_000),
		content: "hello",
		author: { id: "human" },
		channel: { id: "channel-1" },
		mentions: { has: () => false },
	};
}

function deferred<T>() {
	let resolve!: (value: T) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

async function temporaryHome(): Promise<string> {
	return mkdtemp(join(tmpdir(), "gajaeway-discord-lifecycle-"));
}

async function until(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(1);
	}
	throw new Error("timed out waiting for lifecycle condition");
}
