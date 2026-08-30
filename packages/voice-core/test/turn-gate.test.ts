import { describe, expect, test } from "bun:test";
import type { OutstandingTurn } from "../src/ports";
import { decideTurnGate, OriginTurnBook, shouldAdmitTurn } from "../src/turn-gate";

function entry(
	turnId: string,
	acceptedAtMs: number,
	modality: "voice" | "text" = "voice",
): Omit<OutstandingTurn, "seq"> {
	return { messageId: `message-${turnId}`, turnId, modality, acceptedAtMs };
}

describe("OriginTurnBook", () => {
	test("origin-turn-book: maxEntries=1 saturation keeps busy until B terminal", () => {
		const book = new OriginTurnBook({ maxEntries: 1 });
		expect(book.admit(entry("A", 0), 0)).toBe("tracked");
		expect(book.admit(entry("B", 10), 10)).toBe("saturated");
		expect(book.counters.debt).toBe(1);
		expect(book.isBusy()).toBe(true);

		const firstTerminal = book.settle("A", 20);
		expect(firstTerminal.cleared.map((turn) => turn.turnId)).toEqual(["A"]);
		expect(firstTerminal.debtCleared).toBe(0);
		expect(book.isBusy()).toBe(true);

		const secondTerminal = book.settle("B", 30);
		expect(secondTerminal).toEqual({ cleared: [], debtCleared: 1, unknownTerminal: true });
		expect(book.isBusy()).toBe(false);
		expect(book.counters.debt).toBe(0);
	});

	test("origin-turn-book: maxEntries=1 saturation keeps busy across A/B/C terminals", () => {
		const book = new OriginTurnBook({ maxEntries: 1 });
		book.admit(entry("A", 0), 0);
		book.admit(entry("B", 10), 10);
		book.admit(entry("C", 20), 20);
		expect(book.counters.debt).toBe(2);
		expect(book.counters.saturatedAdmits).toBe(2);

		book.settle("A", 30);
		expect(book.isBusy()).toBe(true);
		expect(book.counters.debt).toBe(2);
		book.settle("B", 40);
		expect(book.isBusy()).toBe(true);
		expect(book.counters.debt).toBe(1);
		book.settle("C", 50);
		expect(book.isBusy()).toBe(false);
		expect(book.counters.debt).toBe(0);
	});

	test("origin-turn-book: saturation debt released by TTL sweep", () => {
		const book = new OriginTurnBook({ maxEntries: 1 });
		book.admit(entry("A", 0), 0);
		book.admit(entry("B", 10), 10);
		const result = book.sweep(110, 100);
		expect(result).toEqual({ cleared: [entryWithSeq("A", 0, 0)], debtCleared: 1 });
		expect(book.isBusy()).toBe(false);
		expect(book.counters.stale).toBe(2);
		expect(book.counters.debt).toBe(0);
	});

	test("admits only engaged responses with a string turn id and explicit permission", () => {
		expect(shouldAdmitTurn({ engaged: true, turnId: "turn" })).toBe(true);
		expect(shouldAdmitTurn({ engaged: false, turnId: "turn" })).toBe(false);
		expect(shouldAdmitTurn({ engaged: true, turnId: null })).toBe(false);
		expect(shouldAdmitTurn({ engaged: true, turnId: "turn", admit: false })).toBe(false);
	});

	test("known terminal clears its prefix but leaves later queued entries busy", () => {
		const book = new OriginTurnBook({ maxEntries: 3 });
		book.admit(entry("A", 0), 0);
		book.admit(entry("B", 1), 1);
		book.admit(entry("C", 2), 2);
		const result = book.settle("B", 3);
		expect(result.cleared.map((turn) => turn.turnId)).toEqual(["A", "B"]);
		expect(book.outstanding.map((turn) => turn.turnId)).toEqual(["C"]);
		expect(book.isBusy()).toBe(true);
		book.settle("C", 4);
		expect(book.isBusy()).toBe(false);
	});

	test("unknown terminal drops exactly the oldest outstanding entry", () => {
		const book = new OriginTurnBook({ maxEntries: 3 });
		book.admit(entry("A", 0), 0);
		book.admit(entry("B", 1), 1);
		const result = book.settle("missing", 2);
		expect(result.cleared.map((turn) => turn.turnId)).toEqual(["A"]);
		expect(result.unknownTerminal).toBe(true);
		expect(book.outstanding.map((turn) => turn.turnId)).toEqual(["B"]);
		expect(book.counters.unknownTerminal).toBe(1);
	});

	test("busy remains true for an in-flight text turn, so a voice utterance is held", () => {
		const book = new OriginTurnBook({ maxEntries: 2 });
		book.admit(entry("text-turn", 0, "text"), 0);
		expect(book.isBusy()).toBe(true);
		expect(decideTurnGate(book)).toBe("hold");
		expect(decideTurnGate(book.isBusy())).toBe("hold");
		expect(book.outstanding).toHaveLength(1);
	});

	test("idle books route a voice utterance to trigger", () => {
		const book = new OriginTurnBook({ maxEntries: 1 });
		expect(book.decision()).toBe("trigger");
		expect(decideTurnGate(book)).toBe("trigger");
	});
});

function entryWithSeq(turnId: string, acceptedAtMs: number, seq: number): OutstandingTurn {
	return { ...entry(turnId, acceptedAtMs), seq };
}
