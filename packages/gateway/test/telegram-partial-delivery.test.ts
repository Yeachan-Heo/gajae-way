import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { telegramHtmlToPlainText } from "../../adapter-telegram/src/format";
import {
	type GatewayClientLike,
	subscribeTelegramDeliveries,
	TelegramApiError,
	TelegramDeliveryOrder,
} from "../../adapter-telegram/src/main";
import { TelegramAdapterState } from "../../adapter-telegram/src/state";
import { DeliveryService } from "../src/delivery/delivery";
import { Store } from "../src/store";

// The real Telegram settlement against the real delivery ledger: a reply whose
// later chunk Telegram keeps refusing must neither repost the chunks it already
// accepted nor retry for the whole freshness window.

const origin: OriginRef = { platform: "telegram", kind: "channel", conversationId: "-100123" };
const reply = `${"A".repeat(4000)}\n${"B".repeat(4000)}\n${"C".repeat(4000)}`;
const STEP_MS = 10 * 60_000;
const DAY_MS = 24 * 60 * 60_000;

async function drive(reject: (text: string, sends: number) => boolean, restartEachRetry = false) {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-tg-partial-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const ledger = new DeliveryService(database);
		const state = await TelegramAdapterState.load(directory);
		await state.rememberOrigin(origin);
		const posted: string[] = [];
		let sends = 0;
		let handler: ((message: ChatMessagePayload) => void) | undefined;
		const gateway: GatewayClientLike = {
			request: async <T>(verb: string, params?: unknown) => {
				const { deliveryId, ambiguous, reason } = params as {
					deliveryId: string;
					ambiguous?: boolean;
					reason?: string;
				};
				if (verb === "delivery.confirm") ledger.confirm(deliveryId);
				if (verb === "delivery.fail") ledger.fail(deliveryId, ambiguous === true, reason);
				return {} as T;
			},
			onChatMessage: (next) => {
				handler = next;
				return () => undefined;
			},
		};
		const order = new TelegramDeliveryOrder();
		// Each subscription starts with fresh in-memory progress, as an adapter restart does.
		const subscribe = () =>
			subscribeTelegramDeliveries(
				gateway,
				{
					sendMessage: async (_chatId, text) => {
						sends++;
						if (reject(text, sends)) throw new TelegramApiError(429, "Too Many Requests: retry after 600");
						posted.push(telegramHtmlToPlainText(text));
					},
					setMessageReaction: async () => undefined,
				},
				state,
				order,
			);
		subscribe();
		const payload = ledger.prepare("turn", origin, reply);
		if (!payload?.deliveryId) throw new Error("delivery was not prepared");
		const deliveryId = payload.deliveryId;
		const dispatch = async (message: ChatMessagePayload) => {
			ledger.markInflight(deliveryId);
			handler?.(message);
			await order.drain();
		};
		await dispatch(payload);
		const start = Date.now();
		let sweeps = 0;
		for (let now = start + STEP_MS; now <= start + DAY_MS + STEP_MS; now += STEP_MS) {
			const row = ledger.get(deliveryId);
			if (row?.state === "confirmed" || row?.state === "expired") break;
			for (const message of ledger.sweep(now).payloads) {
				sweeps++;
				if (restartEachRetry) subscribe();
				await dispatch(message);
			}
		}
		return { row: ledger.get(deliveryId), posted, sends, sweeps };
	} finally {
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
}

const count = (posted: string[], letter: string) =>
	posted.reduce((sum, text) => sum + text.split(letter).length - 1, 0);

test("a later chunk Telegram keeps refusing expires the reply without reposting its delivered prefix", async () => {
	const { row, posted, sends, sweeps } = await drive((text) => !text.includes("A"));
	expect(row?.state).toBe("expired");
	expect(count(posted, "A")).toBe(4000);
	expect(posted.join("")).not.toContain("[recovered - may be a duplicate]");
	expect(sweeps).toBeLessThan(15);
	expect(sends).toBeLessThan(20);
});

test("a retry after a transient later-chunk refusal resumes at that chunk and posts the reply exactly once", async () => {
	let refused = false;
	const { row, posted } = await drive((text) => {
		if (refused || text.includes("A")) return false;
		refused = true;
		return true;
	});
	expect(row?.state).toBe("confirmed");
	expect(posted.join("")).toBe(reply);
});

test("an adapter restart before every retry still expires a reply whose later chunk keeps being refused", async () => {
	const { row, posted, sweeps } = await drive((text) => !text.includes("A"), true);
	expect(row?.state).toBe("expired");
	// Each forgotten retry reposts the first chunk; the ledger's definitive-failure
	// cap ends them after five attempts, as it ended base's from-the-start resends.
	expect(posted).toHaveLength(5);
	expect(count(posted, "A")).toBe(4000 * 5);
	expect(posted[1]).toStartWith("[recovered - may be a duplicate] ");
	expect(sweeps).toBe(4);
});
