import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProtocolError } from "@gajaeway/protocol";
import {
	type Generation,
	type OpenGatewayClient,
	startTelegramAdapterWithBot,
	TelegramAdapter,
	TelegramApiError,
	type TelegramUpdate,
} from "../src/main";
import { TelegramAdapterState } from "../src/state";

const update: TelegramUpdate = {
	update_id: 81,
	message: {
		message_id: 7,
		chat: { id: -100123, type: "supergroup" },
		from: { id: 42 },
		text: "hello",
	},
};

test("chat.send failure leaves the update uncommitted and the poller retries the same offset", async () => {
	const home = await temporaryHome();
	try {
		const offsets: Array<number | undefined> = [];
		const keys: string[] = [];
		let sends = 0;
		const gateway = fakeGateway(async (verb, params) => {
			if (verb === "chat.send") {
				keys.push((params as { messageId: string }).messageId);
				if (sends++ === 0) throw new Error("lost response");
			}
			return {};
		});
		const bot = {
			call: async () => ({ id: 900, username: "agent" }),
			getUpdates: async (offset?: number, signal?: AbortSignal) => {
				offsets.push(offset);
				if (offsets.length <= 2) return [update];
				return pendingUntilAbort(signal);
			},
			sendMessage: async () => ({}),
			setMessageReaction: async () => ({}),
		};
		const handle = await startTelegramAdapterWithBot({ token: "token" }, gateway, home, generation(), bot as never);
		await until(() => offsets.length >= 3);
		expect(offsets).toEqual([undefined, undefined, 82]);
		expect(keys).toEqual(["telegram:900:update:81", "telegram:900:update:81"]);
		await handle.stop();
		await expect(handle.settled).resolves.toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("commit happens after a successful request and uses the Telegram ingestion key", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		let updateAtRequest: number | undefined;
		let params: unknown;
		const adapter = new TelegramAdapter(state, "agent", "900");
		await adapter.handleUpdate(
			{
				request: async (verb: string, input?: unknown) => {
					if (verb === "chat.send") {
						updateAtRequest = state.updateId;
						params = input;
					}
					return {};
				},
			} as never,
			update,
		);
		expect(updateAtRequest).toBeUndefined();
		expect(state.updateId).toBe(81);
		expect((params as { messageId: string }).messageId).toBe("telegram:900:update:81");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a lost gateway response retries the same update with the same key", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const adapter = new TelegramAdapter(state, "agent", "900");
		const keys: string[] = [];
		let calls = 0;
		const gateway = {
			request: async (_verb: string, params?: unknown) => {
				keys.push((params as { messageId: string }).messageId);
				if (calls++ === 0) throw new Error("connection reset after dispatch");
				return {};
			},
		};
		await expect(adapter.handleUpdate(gateway as never, update)).rejects.toThrow("connection reset");
		expect(state.updateId).toBeUndefined();
		expect(await adapter.handleUpdate(gateway as never, update)).toBe(true);
		expect(keys).toEqual(["telegram:900:update:81", "telegram:900:update:81"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("invalid_params is logged and committed so one poisoned update cannot wedge polling", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const logs: string[] = [];
		const adapter = new TelegramAdapter(state, "agent", "900", { error: (line) => logs.push(line) });
		const accepted = await adapter.handleUpdate(
			{ request: async () => Promise.reject(new ProtocolError("invalid_params", "bad inbound")) } as never,
			update,
		);
		expect(accepted).toBe(true);
		expect(state.updateId).toBe(81);
		expect(logs).toEqual(["telegram_update_rejected update_id=81 code=invalid_params"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("network, 429, and 5xx bootstrap failures retry internally without rejecting settled", async () => {
	const home = await temporaryHome();
	try {
		let getMe = 0;
		const gateway = fakeGateway();
		const bot = {
			call: async () => {
				getMe++;
				if (getMe === 1) throw new Error("network");
				if (getMe === 2) throw new TelegramApiError(429, "slow down");
				if (getMe === 3) throw new TelegramApiError(500, "server error");
				return { id: 900, username: "agent" };
			},
			getUpdates: async (_offset?: number, signal?: AbortSignal) => pendingUntilAbort(signal),
			sendMessage: async () => ({}),
			setMessageReaction: async () => ({}),
		};
		const handle = await startTelegramAdapterWithBot({ token: "token" }, gateway, home, generation(), bot as never);
		await until(() => getMe === 4);
		await handle.stop();
		await expect(handle.settled).resolves.toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("401 and 404 from bootstrap reject settled", async () => {
	for (const status of [401, 404]) {
		const home = await temporaryHome();
		try {
			const handle = await startTelegramAdapterWithBot({ token: "token" }, fakeGateway(), home, generation(), {
				call: async () => Promise.reject(new TelegramApiError(status, "fatal")),
				getUpdates: async () => [],
				sendMessage: async () => ({}),
				setMessageReaction: async () => ({}),
			} as never);
			await expect(handle.settled).rejects.toBeInstanceOf(TelegramApiError);
			await handle.stop();
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
});

test("a 409 poll conflict is transient and observable", async () => {
	const home = await temporaryHome();
	try {
		const logs: string[] = [];
		let polls = 0;
		const handle = await startTelegramAdapterWithBot(
			{ token: "token" },
			fakeGateway(),
			home,
			generation(),
			{
				call: async () => ({ id: 900, username: "agent" }),
				getUpdates: async (_offset?: number, signal?: AbortSignal) => {
					if (polls++ === 0) throw new TelegramApiError(409, "another poller");
					return pendingUntilAbort(signal);
				},
				sendMessage: async () => ({}),
				setMessageReaction: async () => ({}),
			} as never,
			{ error: (line) => logs.push(line) },
		);
		await until(() => polls >= 2);
		expect(logs).toContain("telegram_poll_conflict");
		await handle.stop();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("stop aborts a pending 30-second poll without waiting for its timeout", async () => {
	const home = await temporaryHome();
	try {
		let polling = false;
		const handle = await startTelegramAdapterWithBot({ token: "token" }, fakeGateway(), home, generation(), {
			call: async () => ({ id: 900, username: "agent" }),
			getUpdates: async (_offset?: number, signal?: AbortSignal) => {
				polling = true;
				return pendingUntilAbort(signal);
			},
			sendMessage: async () => ({}),
			setMessageReaction: async () => ({}),
		} as never);
		await until(() => polling);
		const started = performance.now();
		await handle.stop();
		expect(performance.now() - started).toBeLessThan(100);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("stop during backoff resolves immediately", async () => {
	const home = await temporaryHome();
	try {
		const sleeping = deferred<void>();
		let sleepCalls = 0;
		const handle = await startTelegramAdapterWithBot(
			{ token: "token" },
			fakeGateway(),
			home,
			generation(() => {
				sleepCalls++;
				return sleeping.promise;
			}),
			{
				call: async () => Promise.reject(new Error("network")),
				getUpdates: async () => [],
				sendMessage: async () => ({}),
				setMessageReaction: async () => ({}),
			} as never,
		);
		await until(() => sleepCalls === 1);
		await Promise.race([
			handle.stop(),
			Bun.sleep(100).then(() => Promise.reject(new Error("stop waited for backoff"))),
		]);
		sleeping.resolve();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

function fakeGateway(request: (verb: string, params?: unknown) => Promise<unknown> = async () => ({})) {
	const handlers = new Map<string, Set<(payload: unknown) => void>>();
	const gateway: OpenGatewayClient = {
		request: async <T>(verb: string, params?: unknown) => (await request(verb, params)) as T,
		onChatMessage: (handler) => {
			const listeners = handlers.get("chat.message") ?? new Set();
			listeners.add(handler as (payload: unknown) => void);
			handlers.set("chat.message", listeners);
			return () => listeners.delete(handler as (payload: unknown) => void);
		},
		open: async () => ({ replayed: 0 }),
		close: () => {},
	};
	return gateway;
}

function generation(sleep: (ms: number) => Promise<void> = async () => {}): Generation {
	const controller = new AbortController();
	return {
		id: 1,
		signal: controller.signal,
		port: {} as never,
		track: (task) => task,
		sleep,
	};
}

function pendingUntilAbort(signal: AbortSignal | undefined): Promise<never> {
	return new Promise((_, reject) => {
		if (signal?.aborted) {
			reject(abortError());
			return;
		}
		signal?.addEventListener("abort", () => reject(abortError()), { once: true });
	});
}

function abortError(): Error {
	return Object.assign(new Error("aborted"), { name: "AbortError" });
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
	return mkdtemp(join(tmpdir(), "gajaeway-telegram-poller-"));
}

async function until(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(1);
	}
	throw new Error("timed out waiting for poller condition");
}
