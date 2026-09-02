import { expect, test } from "bun:test";
import type { GatewayConfig, LocalGatewayPort } from "@gajaeway/gateway";
import { adapterInputs, adminEvents, requestAfterOpen } from "../src/adapters";
import { deferred } from "./helpers";

function config(input: Partial<GatewayConfig> = {}): GatewayConfig {
	return {
		schemaVersion: 1,
		home: "/daemon-home",
		configPath: "/daemon-home/config.json",
		socketPath: "/daemon-home/gateway.sock",
		dbPath: "/daemon-home/gateway.db",
		logVerbosity: "info",
		settleWindowMs: 2_000,
		stallTimeoutMs: 120_000,
		maxInboundAgeMs: 600_000,
		...input,
	};
}

test("adapter inputs trim each snapshot credential after reading each referenced file once", async () => {
	const reads: string[] = [];
	const snapshot = config({
		credentials: {
			discord: { credentialFile: "/secrets/discord" },
			discordVoice: { credentialFile: "/secrets/voice" },
			telegram: { credentialFile: "/secrets/telegram" },
		},
		adapters: {
			discord: { intents: [1, 512], voice: { languageCode: "ko", voiceId: "voice" } },
			telegram: {},
		},
		channels: { "discord:123": {}, "bare-id": {}, "telegram:456": {} },
	});
	const values: Record<string, string> = {
		"/secrets/discord": " discord-token \n",
		"/secrets/voice": " voice-key\n",
		"/secrets/telegram": " telegram-token\n",
	};
	const inputs = await adapterInputs(snapshot, "/injected-home", {
		readText: async (path) => {
			reads.push(path);
			return values[path] ?? "";
		},
	});

	expect(reads.sort()).toEqual(["/secrets/discord", "/secrets/telegram", "/secrets/voice"]);
	expect(inputs.discord).toMatchObject({
		token: "discord-token",
		intents: [1, 512],
		recoveryChannels: ["123", "bare-id"],
		recoveryCursorPath: "/injected-home/adapters/discord/recovery-cursor.json",
		voice: { apiKey: "voice-key", apiKeyFile: "/secrets/voice", languageCode: "ko", voiceId: "voice" },
	});
	expect(inputs.telegram).toEqual({ token: "telegram-token" });
});

test("missing and empty credential files are boot errors naming the configured credential", async () => {
	const snapshot = config({
		credentials: { discord: { credentialFile: "/secrets/discord" } },
		adapters: { discord: {} },
	});
	await expect(
		adapterInputs(snapshot, "/home", { readText: async () => Promise.reject(new Error("ENOENT")) }),
	).rejects.toThrow("credentials.discord");
	await expect(adapterInputs(snapshot, "/home", { readText: async () => " \n" })).rejects.toThrow(
		"credentials.discord",
	);
});

test("recovery channel derivation strips discord prefixes, preserves bare ids, and ignores telegram", async () => {
	const snapshot = config({
		credentials: { discord: { credentialFile: "/secrets/discord" } },
		adapters: { discord: {} },
		channels: {
			"discord:111": {},
			"discord:222": {},
			bare: {},
			"telegram:333": {},
			"other:444": {},
		},
	});
	const inputs = await adapterInputs(snapshot, "/home", { readText: async () => "token" });
	expect(inputs.discord?.recoveryChannels).toEqual(["111", "222", "bare"]);
});

test("the admin request wrapper parks a request until local-port open resolves", async () => {
	const opened = deferred<void>();
	const calls: string[] = [];
	const port: Pick<LocalGatewayPort, "request"> = {
		request: async <T = unknown>(verb: string, _params?: unknown) => {
			calls.push(verb);
			return { ok: true } as T;
		},
	};
	const request = requestAfterOpen(port, opened.promise);
	const response = request<{ ok: boolean }>("gateway.status");
	await Bun.sleep(0);
	expect(calls).toEqual([]);
	opened.resolve();
	await expect(response).resolves.toEqual({ ok: true });
	expect(calls).toEqual(["gateway.status"]);
});

test("admin event wiring subscribes to every console event and tears all subscriptions down", () => {
	const subscriptions: string[] = [];
	const removed: string[] = [];
	const port: Pick<LocalGatewayPort, "on"> = {
		on: (event) => {
			subscriptions.push(event);
			return () => removed.push(event);
		},
	};
	const off = adminEvents(port)(() => {});
	expect(subscriptions).toEqual(["chat.message", "chat.progress", "monitor.event", "gateway.stopping"]);
	off();
	expect(removed).toEqual(subscriptions);
});
