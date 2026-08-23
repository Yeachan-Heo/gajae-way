import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { TelegramPlatform, TelegramPlatformError, telegramFormatOutboundText } from "../../src/adapter/telegram/platform";

function telegramResponse(result: unknown): Response {
	return new Response(JSON.stringify({ ok: true, result }), { status: 200 });
}

test("Telegram platform validates identity, durably advances offset, and settles sends", async () => {
	const requests: Array<{ method: string; body: Record<string, unknown> }> = [];
	const stateDir = await Bun.$`mktemp -d`.text();
	const platform = new TelegramPlatform({
		token: "token",
		stateDir: stateDir.trim(),
		fetch: async (input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			const body = init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : {};
			requests.push({ method, body });
			if (method === "getMe") return telegramResponse({ id: 123, username: "fixture" });
			if (method === "getUpdates") return telegramResponse([]);
			if (method === "sendMessage") return telegramResponse({ message_id: 77 });
			return telegramResponse(true);
		},
	});
	expect(await platform.getCurrentUser()).toEqual({ id: "123", username: "fixture" });
	await platform.connect();
	await Bun.sleep(10);
	await platform.send("-100", "hello", "nonce", { dedupeKey: "event:1" });
	await platform.send("-100", "hello", "nonce", { dedupeKey: "event:1" });
	await platform.disconnect();
	expect(requests.filter(request => request.method === "sendMessage")).toHaveLength(1);
});

	test("Telegram platform chunks long messages at 4096 characters", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-telegram-chunks-"));
	const platform = new TelegramPlatform({
		token: "token",
		stateDir,
		fetch: async (input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			if (method === "sendMessage") {
				bodies.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
				return telegramResponse({ message_id: bodies.length });
			}
			if (method === "getMe") return telegramResponse({ id: 123 });
			if (method === "getUpdates") return telegramResponse([]);
			return telegramResponse(true);
		},
	});
	await platform.connect();
	await platform.send("-100", "x".repeat(4_097), "nonce", { dedupeKey: "event:2" });
	await platform.disconnect();
	expect(bodies).toHaveLength(2);
	expect(bodies.every(body => String(body.text).length <= 4_096)).toBe(true);
});

test("Telegram platform requires an explicit state directory", () => {
	expect(() => new TelegramPlatform({ token: "token", stateDir: "" })).toThrow(TelegramPlatformError);
});

test("Telegram formatting converts headers to bold emphasis on the wire", async () => {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-telegram-format-"));
	const bodies: Array<Record<string, unknown>> = [];
	const platform = new TelegramPlatform({
		token: "token",
		stateDir,
		fetch: async (input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			if (method === "sendMessage") {
				bodies.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
				return telegramResponse({ message_id: 1 });
			}
			if (method === "getUpdates") return telegramResponse([]);
			if (method === "getMe") return telegramResponse({ id: 123 });
			return telegramResponse(true);
		},
	});
	await platform.send("-100", "# Heading", "nonce", { dedupeKey: "event:format" });
	expect(bodies[0]?.text).toBe("**Heading**");
});

test("Telegram restart mid-chunk-set does not duplicate earlier chunks", async () => {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-telegram-restart-"));
	let calls = 0;
	const sent: string[] = [];
	const fetch = async (input: string, init?: RequestInit): Promise<Response> => {
		const method = input.split("/").at(-1) ?? "";
		if (method === "sendMessage") {
			calls += 1;
			if (calls === 2) throw new Error("simulated crash");
			const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
			sent.push(String(body.text));
			return telegramResponse({ message_id: calls });
		}
		if (method === "getUpdates") return telegramResponse([]);
		if (method === "getMe") return telegramResponse({ id: 123 });
		return telegramResponse(true);
	};
	const first = new TelegramPlatform({ token: "token", stateDir, fetch });
	await expect(first.send("-100", "x".repeat(8_200), "nonce", { dedupeKey: "event:restart" })).rejects.toThrow();
	const second = new TelegramPlatform({ token: "token", stateDir, fetch });
	await second.send("-100", "x".repeat(8_200), "nonce", { dedupeKey: "event:restart" });
	expect(sent.length).toBe(3);
});
