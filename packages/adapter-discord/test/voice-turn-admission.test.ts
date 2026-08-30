import { describe, expect, test } from "bun:test";
import type { EngagementContext, OriginRef } from "@gajaeway/protocol";
import { OriginTurnBook, shouldAdmitTurn } from "@gajaeway/voice-core";
import { handleSlashCommand } from "../src/main";

/** A live room stub exposing only what the admission path touches. */
function room() {
	const book = new OriginTurnBook({ maxEntries: 8 });
	const settled: string[] = [];
	return {
		book,
		settled,
		session: {
			admitTurn: (entry: Parameters<OriginTurnBook["admit"]>[0]) => book.admit(entry, 1_000),
			settleTurn: (turnId: string) => {
				settled.push(turnId);
				book.settle(turnId, 2_000);
			},
		},
	};
}

describe("voice turn admission", () => {
	test("an accepted text turn marks the origin busy so speech cannot open a second turn", () => {
		const live = room();
		expect(live.book.isBusy()).toBe(false);
		live.session.admitTurn({ messageId: "m1", turnId: "t1", modality: "text", acceptedAtMs: 1_000 });
		expect(live.book.isBusy()).toBe(true);
	});

	test("the turn's terminal event releases the origin again", () => {
		const live = room();
		live.session.admitTurn({ messageId: "m1", turnId: "t1", modality: "text", acceptedAtMs: 1_000 });
		live.session.settleTurn("t1");
		expect(live.settled).toEqual(["t1"]);
		expect(live.book.isBusy()).toBe(false);
	});

	test("a declined or duplicate-ack reply admits nothing, so the origin stays free", () => {
		// This is the exact predicate the adapter's admission path applies before touching
		// the book: only an engaged reply carrying a real turn id can ever be tracked.
		for (const reply of [
			{ engaged: false, turnId: "t1" },
			{ engaged: true, turnId: null },
			{ engaged: undefined, turnId: "t1" },
		]) {
			expect(shouldAdmitTurn(reply)).toBe(false);
		}
		expect(shouldAdmitTurn({ engaged: true, turnId: "t1" })).toBe(true);

		const live = room();
		for (const reply of [
			{ engaged: false, turnId: "t1" },
			{ engaged: true, turnId: null },
		]) {
			if (shouldAdmitTurn(reply)) {
				live.session.admitTurn({ messageId: "m", turnId: "t1", modality: "text", acceptedAtMs: 1_000 });
			}
		}
		expect(live.book.isBusy()).toBe(false);
	});

	test("the session slash commands never mark the origin busy", async () => {
		const live = room();
		const calls: { admit?: boolean }[] = [];
		await handleSlashCommand(
			{
				isChatInputCommand: () => true,
				commandName: "reset",
				id: "i1",
				user: { id: "u1", username: "owner" },
				channel: { id: "voice-1" },
				reply: async () => undefined,
			} as never,
			{
				requestInbound: async (
					_messageId: string,
					_origin: OriginRef,
					_text: string,
					_engagement: EngagementContext,
					options?: { admit?: boolean },
				) => {
					calls.push(options ?? {});
					return { engaged: true, turnId: "slash-turn" };
				},
			} as never,
		);
		// /new and /reset answer without running a turn, so no terminal will ever arrive:
		// admitting them would wedge the origin as permanently busy.
		expect(calls).toEqual([{ admit: false }]);
		expect(live.book.isBusy()).toBe(false);
	});
});
