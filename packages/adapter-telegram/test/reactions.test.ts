import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, ReactionRef } from "@gajae-gateway/protocol";
import { platformSupportsReaction, REACTION_ALLOWLIST, reactionAllowlistFor } from "@gajae-gateway/protocol";
import {
	describeTelegramReaction,
	type GatewayClientLike,
	settleTelegramReaction,
	TelegramAdapter,
	TelegramApiError,
	TelegramBotApi,
	type TelegramMessageReactionUpdated,
} from "../src/main";
import { telegramMessageOrigin } from "../src/origin";
import { TELEGRAM_REACTION_SET, telegramReactionFor } from "../src/reactions";
import { TelegramAdapterState } from "../src/state";

const groupChat = { id: -100123, type: "supergroup", title: "war room" } as const;
const dmOrigin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });

test("Telegram's documented reaction set has the 73 server-provided emoji", () => {
	expect(TELEGRAM_REACTION_SET).toHaveLength(73);
	expect(new Set(TELEGRAM_REACTION_SET).size).toBe(73);
	expect(TELEGRAM_REACTION_SET).toContain("\u2764");
	expect(TELEGRAM_REACTION_SET).not.toContain("\u2764\uFE0F");
});

test("maps supported allowlist emoji to Telegram spelling and normalizes U+FE0F", () => {
	expect(telegramReactionFor(reactionRef("👍", "thumbsup"))).toEqual({ emoji: "👍" });
	expect(telegramReactionFor(reactionRef("🤔", "thinking"))).toEqual({ emoji: "🤔" });
	expect(telegramReactionFor(reactionRef("🔥", "fire"))).toEqual({ emoji: "🔥" });
	expect(telegramReactionFor(reactionRef("\u2764\uFE0F", "heart"))).toEqual({ emoji: "\u2764" });
});

test("the protocol's telegram capability list is exactly what Telegram's real set rejects", () => {
	// The protocol package advertises a per-platform allowlist to the persona; the
	// authoritative 73-emoji set lives here. If either side drifts, this fails instead
	// of the persona being invited to use an emoji Telegram will never accept.
	const rejectedHere = REACTION_ALLOWLIST.filter(
		(entry) => "unsupported" in telegramReactionFor(reactionRef(entry.unicode, entry.name)),
	).map((entry) => entry.name);
	expect([...rejectedHere].sort()).toEqual(["check", "cross", "lobster"]);
	const deliverable = new Set(reactionAllowlistFor("telegram").map((entry) => entry.name));
	for (const name of rejectedHere) expect(deliverable.has(name)).toBe(false);
	for (const entry of REACTION_ALLOWLIST)
		expect(platformSupportsReaction("telegram", entry.name)).toBe(!rejectedHere.includes(entry.name));
	// Discord has no such restriction: every allowlist entry is deliverable there.
	expect(reactionAllowlistFor("discord")).toHaveLength(REACTION_ALLOWLIST.length);
});

test("reacts through setMessageReaction and confirms the delivery", async () => {
	for (const emoji of ["👍", "🤔", "🔥"]) {
		const home = await temporaryHome();
		try {
			const state = await TelegramAdapterState.load(home);
			await state.rememberOrigin(dmOrigin);
			const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
			const bot = new TelegramBotApi("not-a-real-token", async (url, init) => {
				calls.push({ url: String(url), body: JSON.parse(String(init?.body)) });
				return Response.json({ ok: true, result: true });
			});
			const requests: Array<{ verb: string; params: unknown }> = [];
			await settleTelegramReaction(mockGateway(requests), bot, state, reactionDelivery(emoji, "thumbsup"));
			expect(calls).toHaveLength(1);
			expect(calls[0]?.url.endsWith("/setMessageReaction")).toBe(true);
			expect(calls[0]?.body).toEqual({
				chat_id: "22",
				message_id: 555,
				reaction: [{ type: "emoji", emoji }],
			});
			expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
});

test("sends the bare U+2764 heart Telegram accepts, not the U+FE0F spelling", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		await state.rememberOrigin(dmOrigin);
		const sent: Array<{ chatId: string; messageId: string; emoji: string }> = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramReaction(
			mockGateway(requests),
			{ setMessageReaction: async (chatId, messageId, emoji) => void sent.push({ chatId, messageId, emoji }) },
			state,
			reactionDelivery("\u2764\uFE0F", "heart"),
		);
		expect(sent).toEqual([{ chatId: "22", messageId: "555", emoji: "\u2764" }]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("reports impossible emoji as a definitive delivery failure without calling the API", async () => {
	for (const [emoji, name] of [
		["✅", "check"],
		["🦞", "lobster"],
	]) {
		const home = await temporaryHome();
		try {
			const state = await TelegramAdapterState.load(home);
			await state.rememberOrigin(dmOrigin);
			const requests: Array<{ verb: string; params: unknown }> = [];
			let called = 0;
			await settleTelegramReaction(
				mockGateway(requests),
				{
					setMessageReaction: async () => {
						called += 1;
					},
				},
				state,
				reactionDelivery(emoji as string, name as string),
			);
			expect(called).toBe(0);
			expect(requests).toHaveLength(1);
			expect(requests[0]?.verb).toBe("delivery.fail");
			const params = requests[0]?.params as { deliveryId: string; reason: string; ambiguous: boolean };
			expect(params.deliveryId).toBe("delivery-1");
			expect(params.ambiguous).toBe(false);
			expect(params.reason).toContain(emoji as string);
			expect(params.reason).toContain("73");
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
});

test("fails the delivery when Telegram rejects the reaction", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		await state.rememberOrigin(dmOrigin);
		const bot = new TelegramBotApi("not-a-real-token", async () =>
			Response.json({ ok: false, description: "Bad Request: REACTIONS_DISABLED" }, { status: 400 }),
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramReaction(mockGateway(requests), bot, state, reactionDelivery("👍", "thumbsup"));
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: {
					deliveryId: "delivery-1",
					reason: "Bad Request: REACTIONS_DISABLED",
					ambiguous: false,
				},
			},
		]);
		expect(requests.some((request) => request.verb === "delivery.confirm")).toBe(false);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fails the delivery when no reply route was ever persisted", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramReaction(
			mockGateway(requests),
			{
				setMessageReaction: async () => {
					throw new Error("must not react without a route");
				},
			},
			state,
			reactionDelivery("👍", "thumbsup"),
		);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.verb).toBe("delivery.fail");
		const params = requests[0]?.params as { reason: string; ambiguous: boolean };
		expect(params.ambiguous).toBe(false);
		expect(params.reason).toContain("No persisted Telegram reply route for 22");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("treats a network failure after dispatch as ambiguous", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		await state.rememberOrigin(dmOrigin);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramReaction(
			mockGateway(requests),
			{ setMessageReaction: async () => Promise.reject(new Error("socket hang up")) },
			state,
			reactionDelivery("👍", "thumbsup"),
		);
		expect(requests).toEqual([
			{ verb: "delivery.fail", params: { deliveryId: "delivery-1", reason: "socket hang up", ambiguous: true } },
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("asks Telegram for message_reaction updates while keeping plain messages", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
		bodies.push(JSON.parse(String(init?.body)));
		return Response.json({ ok: true, result: [] });
	});
	await bot.getUpdates(12);
	expect(bodies[0]).toEqual({ timeout: 30, allowed_updates: ["message", "message_reaction"], offset: 12 });
});

test("describes an added reaction, a removed reaction, and ignores our own bot", () => {
	expect(describeTelegramReaction(reactionUpdate([], [{ type: "emoji", emoji: "👍" }]), "900")).toEqual({
		origin: { platform: "telegram", kind: "channel", conversationId: "-100123" },
		targetMessageId: "77",
		emoji: "👍",
		action: "add",
		engagement: { mentioned: false, group: true, authorId: "42", authorName: "reader", channelLabel: "war room" },
	});
	const before = [
		{ type: "emoji", emoji: "👍" },
		{ type: "emoji", emoji: "🔥" },
	];
	const shrunk = describeTelegramReaction(reactionUpdate(before, [{ type: "emoji", emoji: "👍" }]), "900");
	expect(shrunk?.action).toBe("remove");
	expect(shrunk?.emoji).toBe("🔥");
	expect(describeTelegramReaction(reactionUpdate([], []), "900")).toBeUndefined();
	expect(describeTelegramReaction(undefined, "900")).toBeUndefined();
	const ownBot = { ...reactionUpdate([], [{ type: "emoji", emoji: "👍" }]), user: { id: 900, username: "agent" } };
	expect(describeTelegramReaction(ownBot, "900")).toBeUndefined();
});

test("handleUpdate reports an inbound reaction as engagement and never as a turn", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const adapter = new TelegramAdapter(state, "agent", "900", { chats: { "-100123": { engagement: "open" } } });
		const requests: Array<{ verb: string; params: unknown }> = [];
		const accepted = await adapter.handleUpdate(mockGateway(requests), {
			update_id: 5,
			message_reaction: reactionUpdate([], [{ type: "emoji", emoji: "🫡" }]),
		});
		expect(accepted).toBe(true);
		expect(requests.map((request) => request.verb)).toEqual(["engagement.reaction"]);
		expect(requests[0]?.params).toEqual({
			origin: { platform: "telegram", kind: "channel", conversationId: "-100123" },
			targetMessageId: "77",
			emoji: "🫡",
			action: "add",
			engagement: { mentioned: false, group: true, authorId: "42", authorName: "reader", channelLabel: "war room" },
		});
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a Telegram API rejection stays a definitive TelegramApiError", async () => {
	const bot = new TelegramBotApi("not-a-real-token", async () =>
		Response.json({ ok: false, description: "Bad Request: message can't be reacted to" }, { status: 400 }),
	);
	await expect(bot.setMessageReaction("-100123", "77", "👍")).rejects.toBeInstanceOf(TelegramApiError);
});

function reactionRef(emoji: string, emojiName: string): ReactionRef {
	return { targetMessageId: "555", emoji, emojiName };
}

function reactionDelivery(emoji: string, emojiName: string): ChatMessagePayload {
	return {
		turnId: "turn-1",
		origin: dmOrigin,
		role: "assistant",
		text: emoji,
		final: true,
		deliveryId: "delivery-1",
		reaction: reactionRef(emoji, emojiName),
	};
}

function reactionUpdate(
	oldReaction: TelegramMessageReactionUpdated["old_reaction"],
	newReaction: TelegramMessageReactionUpdated["new_reaction"],
): TelegramMessageReactionUpdated {
	return {
		chat: groupChat,
		message_id: 77,
		user: { id: 42, username: "reader" },
		date: 1_756_000_000,
		old_reaction: oldReaction,
		new_reaction: newReaction,
	};
}

async function temporaryHome(): Promise<string> {
	return mkdtemp(join(tmpdir(), "gajaeway-telegram-reactions-"));
}

function mockGateway(requests: Array<{ verb: string; params: unknown }>): GatewayClientLike {
	return {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
		onChatMessage: () => () => {},
	};
}
