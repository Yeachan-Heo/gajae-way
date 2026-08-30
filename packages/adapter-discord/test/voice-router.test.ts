import { expect, test } from "bun:test";
import type { ChatContextResult, ChatMessagePayload, OriginRef } from "@gajaeway/protocol";
import { ModalityRegistry } from "@gajaeway/voice-core";
import {
	createVoiceDeliveryRouter,
	type DiscordDeliveryClient,
	type DiscordDeliveryGateway,
	type VoicePlaybackOutcome,
} from "../src/voice/router";

const ORIGIN: OriginRef = { platform: "discord", kind: "channel", conversationId: "channel-1" };

function message(deliveryId: string, text = deliveryId): ChatMessagePayload {
	return { turnId: "turn-1", origin: ORIGIN, role: "assistant", text, final: true, deliveryId };
}

function fakeDiscord(sent: unknown[] = []): DiscordDeliveryClient {
	return { channels: { fetch: async () => ({ send: async (payload: unknown) => void sent.push(payload) }) } };
}

function fakeGateway(requests: Array<{ verb: string; params: unknown }>): DiscordDeliveryGateway {
	return {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
	};
}

function recordedContext(): ChatContextResult {
	return { recorded: 1, dropped: 0, truncated: 0, engaged: false };
}

function voiceRouter(
	play: (value: ChatMessagePayload) => Promise<VoicePlaybackOutcome>,
	contextCalls: Array<{ origin: OriginRef; entries: readonly { messageId: string; text: string; at: string }[] }> = [],
) {
	const modality = new ModalityRegistry();
	modality.register("turn-1", "voice", 0);
	const router = createVoiceDeliveryRouter({
		modality,
		playback: { play },
		submitContext: async (params) => {
			contextCalls.push({ origin: params.origin, entries: params.entries });
			return recordedContext();
		},
		contextCap: { maxItems: 20, maxCharsPerItem: 200 },
		clock: { now: () => 10_000 },
	});
	return { router, modality };
}

test("text modality, unknown turns, and redelivery never attempt audio", async () => {
	let audioCalls = 0;
	const play = async (): Promise<VoicePlaybackOutcome> => {
		audioCalls += 1;
		return { outcome: "completed" };
	};
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: unknown[] = [];
	const textModality = new ModalityRegistry();
	textModality.register("text-turn", "text", 0);
	const router = createVoiceDeliveryRouter({
		modality: textModality,
		playback: { play },
		submitContext: async () => recordedContext(),
	});
	await router(fakeGateway(requests), fakeDiscord(sent), { ...message("text-delivery"), turnId: "text-turn" });
	await router(fakeGateway(requests), fakeDiscord(sent), { ...message("unknown-delivery"), turnId: "missing-turn" });
	await router(fakeGateway(requests), fakeDiscord(sent), { ...message("redelivery"), redelivered: true });
	expect(audioCalls).toBe(0);
	expect(requests.filter((request) => request.verb === "delivery.confirm")).toHaveLength(3);
	expect(sent).toHaveLength(3);
});

test("the same delivery id routed twice confirms exactly once", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	let release: (() => void) | undefined;
	const blocked = new Promise<void>((resolve) => {
		release = resolve;
	});
	const router = createVoiceDeliveryRouter({
		submitContext: async () => recordedContext(),
	});
	const discord: DiscordDeliveryClient = {
		channels: {
			fetch: async () => ({
				send: async () => {
					await blocked;
				},
			}),
		},
	};
	const first = router(fakeGateway(requests), discord, message("duplicate"));
	const second = router(fakeGateway(requests), discord, message("duplicate"));
	release?.();
	await Promise.all([first, second]);
	await router(fakeGateway(requests), discord, message("duplicate"));
	expect(requests.filter((request) => request.verb === "delivery.confirm")).toHaveLength(1);
});

test("serializes two fragments from one turn in publication order", async () => {
	const played: string[] = [];
	let releaseFirst: (() => void) | undefined;
	const firstComplete = new Promise<void>((resolve) => {
		releaseFirst = resolve;
	});
	const { router } = voiceRouter(async (value) => {
		played.push(value.deliveryId ?? "");
		if (value.deliveryId === "part-1") await firstComplete;
		return { outcome: "completed" };
	});
	const requests: Array<{ verb: string; params: unknown }> = [];
	const first = router(fakeGateway(requests), fakeDiscord(), message("part-1", "first"));
	await Bun.sleep(0);
	const second = router(fakeGateway(requests), fakeDiscord(), message("part-2", "second"));
	await Bun.sleep(0);
	expect(played).toEqual(["part-1"]);
	releaseFirst?.();
	await Promise.all([first, second]);
	expect(played).toEqual(["part-1", "part-2"]);
	expect(requests.filter((request) => request.verb === "delivery.confirm")).toHaveLength(2);
});

test("barge-in publishes the full response, records one truncation context row, then confirms", async () => {
	const contextCalls: Array<{
		origin: OriginRef;
		entries: readonly { messageId: string; text: string; at: string }[];
	}> = [];
	const { router } = voiceRouter(
		async () => ({ outcome: "barge_in", audioPlayed: true, remainder: "unspoken tail", atMs: 9_000 }),
		contextCalls,
	);
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: unknown[] = [];
	await router(fakeGateway(requests), fakeDiscord(sent), message("barge", "spoken and unspoken"));
	expect(sent).toEqual(["spoken and unspoken"]);
	expect(contextCalls).toHaveLength(1);
	expect(contextCalls[0]?.entries).toHaveLength(1);
	expect(contextCalls[0]?.entries[0]?.text).toBe("unspoken tail");
	expect(requests.map((request) => request.verb)).toEqual(["delivery.confirm"]);
});

test("a playback failure after partial audio fails ambiguously", async () => {
	const { router } = voiceRouter(async () => {
		throw Object.assign(new Error("speaker stopped"), { audioPlayed: true });
	});
	const requests: Array<{ verb: string; params: unknown }> = [];
	await router(fakeGateway(requests), fakeDiscord(), message("failed"));
	expect(requests).toContainEqual({
		verb: "delivery.fail",
		params: { deliveryId: "failed", reason: "speaker stopped", ambiguous: true },
	});
	expect(requests.filter((request) => request.verb === "delivery.confirm")).toHaveLength(0);
});
test("finally clears the in-flight status and typing hooks", async () => {
	const { router } = voiceRouter(async () => ({ outcome: "completed" }));
	const requests: Array<{ verb: string; params: unknown }> = [];
	let cleared = 0;
	let ended = 0;
	await router(
		fakeGateway(requests),
		fakeDiscord(),
		message("hooks"),
		{
			end: () => {
				ended += 1;
			},
		},
		{
			clear: async () => {
				cleared += 1;
			},
		},
	);
	expect(cleared).toBe(1);
	expect(ended).toBe(1);
});
