import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajaeway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

test("reconciliation uses retained authored output and otherwise re-dispatches", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-reconcile-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "reconcile",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		const queued: unknown[] = [];
		const dispatches: string[] = [];
		const gjc = {
			ensureSession: async (origin: string) => {
				dispatches.push(origin);
				return { sessionId: "event-session" };
			},
			forgetRebinds: () => {},
			sendTurn: async (_id: string, text: string) =>
				JSON.stringify(
					(JSON.parse(text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "authored by model",
					})),
				),
		};
		const pipeline = new MonitorPropagator({
			database,
			registry,
			gjc,
			memory: {
				enqueue: (mutation: unknown) => {
					queued.push(mutation);
					return "intent";
				},
				enqueueExistingId: () => {},
			} as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		const retained = crypto.randomUUID();
		database.withTransaction(() => {
			database.monitorEventCreate({
				eventId: retained,
				monitorId: monitor.monitorId,
				eventType: "changed",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			database.authoredOutputCreate(retained, "retained authored knowledge");
			database.monitorEventUpdate(retained, "authored");
		});
		const missing = crypto.randomUUID();
		database.withTransaction(() =>
			database.monitorEventCreate({
				eventId: missing,
				monitorId: monitor.monitorId,
				eventType: "changed",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			}),
		);
		await pipeline.reconcile();
		const intents = database.memoryIntentRowsByRowid().filter((row) => row.kind === "monitor-event");
		expect(intents).toHaveLength(2);
		const replyTexts = intents.map((row) => (JSON.parse(row.payload_json) as { replyText: string }).replyText);
		expect(replyTexts.some((text) => text.includes("retained authored knowledge"))).toBe(true);
		expect(replyTexts.some((text) => text.includes("authored by model"))).toBe(true);
		expect(dispatches).toContain(originKey(eventTypeOrigin("changed")));
		expect(database.authoredOutput(missing)).toBe("authored by model");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("reconcile replays same-millisecond events oldest-first", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-reconcile-order-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "order",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			gjc: {
				ensureSession: async () => ({ sessionId: "s" }),
				forgetRebinds: () => {},
				sendTurn: async () => "[]",
			},
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		// One fired_at for both rows: ordering must come from insertion order, not row layout.
		const firedAt = new Date().toISOString();
		const ids = [crypto.randomUUID(), crypto.randomUUID()];
		database.withTransaction(() => {
			for (const [index, eventId] of ids.entries()) {
				database.monitorEventCreate({
					eventId,
					monitorId: monitor.monitorId,
					eventType: "changed",
					payloadJson: "{}",
					firedAt,
				});
				database.authoredOutputCreate(eventId, `authored-${index}`);
				database.monitorEventUpdate(eventId, "authored");
			}
		});
		await pipeline.reconcile();
		// Insertion order (rowid) preserves the oldest-first replay; the
		// (created_at, id) index scrambles same-millisecond UUID ties.
		const intents = database
			.memoryIntentRowsByRowid()
			.filter((row) => row.kind === "monitor-event")
			.map((row) => (JSON.parse(row.payload_json) as { replyText: string }).replyText);
		expect(intents).toEqual(["authored-0", "authored-1"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor without its own channel target reports authored notes to the ownerTarget", async () => {
	const { mkdtemp } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { GatewayDatabase } = await import("../src/store/db");
	const { MonitorRegistry } = await import("../src/monitors/registry");
	const { MonitorPropagator } = await import("../src/monitors/propagate");
	const { DeliveryService } = await import("../src/delivery/delivery");
	const { DeliveryLedger } = await import("../src/store/ledger");
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ownertarget-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "targetless",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: ["memory.canonicalize"],
	});
	const delivery = new DeliveryService(new DeliveryLedger(database));
	const pushed: unknown[] = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		gjc: {
			ensureSession: async () => ({ sessionId: "s" }),
			forgetRebinds: () => {},
			sendTurn: async (_id: string, prompt: string) =>
				JSON.stringify(
					(JSON.parse(prompt.match(/\[.*\]$/s)?.[0] ?? "[]") as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "owner-target note",
					})),
				),
		},
		memory: { enqueue: () => {}, enqueueExistingId: () => {} } as never,
		delivery,
		emit: () => {},
		deliver: (payload: unknown) => void pushed.push(payload),
		ownerTarget: {
			origin: { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: "owner" },
		},
	});
	pipeline.submit(monitor.monitorId, "memory.canonicalize", { source: "test" });
	let undelivered: ReturnType<typeof delivery.redeliveries> = [];
	for (let attempt = 0; attempt < 400; attempt++) {
		undelivered = delivery.redeliveries();
		if (undelivered.length > 0) break;
		await Bun.sleep(5);
	}
	expect(undelivered).toHaveLength(1);
	expect(undelivered[0]?.origin).toMatchObject({ platform: "discord", kind: "dm", conversationId: "owner-dm" });
	expect(undelivered[0]?.text).toBe("owner-target note");
	// The note is pushed to live adapters immediately, not just parked in the ledger.
	expect(pushed).toHaveLength(1);
	expect((pushed[0] as { text: string }).text).toBe("owner-target note");
	database.close();
});

// A monitor note is model-authored, so it carries the same control tokens a chat
// reply does, and this path gets the same treatment: silence suppresses, a reply
// token threads the delivery, [BREAK] separates what the persona meant as two
// messages, and nothing ships with the syntax visible.
//
// The live leak (2026-09-03, owner DM) that forced this: a note went out reading
// `[REPLY:1544933092823928892] ... [BREAK] [REPLY:...] ...` verbatim — neither
// threaded, nor separated, nor stripped, because the path only ever compared the
// whole note against an exact silence token.
//
// `notes` is assigned per event in batch order, so a coalesced batch can mix a
// silent note with a visible one.
type PushedNote = { text: string; replyToMessageId?: string };
async function monitorNote(...notes: string[]): Promise<{ pushed: PushedNote[]; delivered: number }> {
	const { mkdtemp } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { GatewayDatabase } = await import("../src/store/db");
	const { MonitorRegistry } = await import("../src/monitors/registry");
	const { MonitorPropagator } = await import("../src/monitors/propagate");
	const { DeliveryService } = await import("../src/delivery/delivery");
	const { DeliveryLedger } = await import("../src/store/ledger");
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-tokens-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "tokens",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: ["memory.canonicalize"],
	});
	const delivery = new DeliveryService(new DeliveryLedger(database));
	const pushed: unknown[] = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		gjc: {
			ensureSession: async () => ({ sessionId: "s" }),
			forgetRebinds: () => {},
			sendTurn: async (_id: string, prompt: string) =>
				JSON.stringify(
					(JSON.parse(prompt.match(/\[.*\]$/s)?.[0] ?? "[]") as Array<{ eventId: string }>).map(
						({ eventId }, index) => ({
							eventId,
							note: notes[index] ?? (notes[0] as string),
						}),
					),
				),
		},
		memory: { enqueue: () => {}, enqueueExistingId: () => {} } as never,
		delivery,
		emit: () => {},
		deliver: (payload: unknown) => void pushed.push(payload),
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: "owner" } },
	});
	for (const [index] of notes.entries()) pipeline.submit(monitor.monitorId, "memory.canonicalize", { source: index });
	// Bounded wait: a suppressed note never produces a row, so this cannot poll on
	// arrival and must give the pipeline a fixed window either way.
	for (let attempt = 0; attempt < 200 && delivery.redeliveries().length === 0; attempt++) await Bun.sleep(5);
	const delivered = delivery.redeliveries().length;
	database.close();
	await rm(directory, { recursive: true, force: true });
	return { pushed: pushed as PushedNote[], delivered };
}

test("a monitor note carrying a silence token delivers nothing, preamble and all", async () => {
	for (const note of ["[SILENT]", "특별한 건 없었다.\n\n[NO_REPLY]", "정리 완료 [silent]"]) {
		const { pushed, delivered } = await monitorNote(note);
		expect(delivered).toBe(0);
		expect(pushed).toHaveLength(0);
	}
});

test("a monitor note never ships with control syntax visible, and its reply target is honoured", async () => {
	const { pushed, delivered } = await monitorNote("[REPLY:1544305635179495434] 캐논 정리 [REACT:👍] 끝");
	expect(delivered).toBe(1);
	expect(pushed).toHaveLength(1);
	expect(pushed[0]?.text).toBe("캐논 정리 끝");
	// The persona named an absolute platform message id; dropping it silently
	// unthreaded every monitor answer.
	expect(pushed[0]?.replyToMessageId).toBe("1544305635179495434");
});

test("the live owner-DM leak is reproduced and stays sanitized", async () => {
	const leaked = [
		"[REPLY:1544933092823928892] 형님이 부르셨으니 하겠습니다. 승패 판정은 일이 아닙니다.",
		"[REPLY:1544925661179936821] 그건 이미 들어가 있습니다 — `/usage`에 provider가 등록돼 있습니다.",
	].join("\n[BREAK]\n");
	const { pushed, delivered } = await monitorNote(leaked);
	expect(delivered).toBe(1);
	expect(pushed).toHaveLength(1);
	const text = pushed[0]?.text as string;
	for (const token of ["[REPLY:", "[BREAK", "[REACT:", "[SILENT"]) expect(text).not.toContain(token);
	// Both parts survive as the separate messages they were meant to be, and the
	// first target threads the one fenced delivery this batch may emit.
	expect(text).toBe(
		"형님이 부르셨으니 하겠습니다. 승패 판정은 일이 아닙니다.\n\n그건 이미 들어가 있습니다 — `/usage`에 provider가 등록돼 있습니다.",
	);
	expect(pushed[0]?.replyToMessageId).toBe("1544933092823928892");
});

test("a broken fragment in a note cannot delete it or repoint where it is sent", async () => {
	// Sweep-then-interpret, the same order the chat path uses. Reading silence or a
	// routing target off the raw note let a malformed fragment smuggle a `[SILENT]`
	// that threw the real note away, or a second `[REPLY:...]` that repointed it.
	const smuggled = await monitorNote("[REPLY:bad\r[SILENT]\rargument] 실제 보고입니다");
	expect(smuggled.delivered).toBe(1);
	expect(smuggled.pushed[0]?.text).toBe("argument] 실제 보고입니다");

	const repointed = await monitorNote("[REPLY:bad\n[REPLY:evil-target]\nargument] 본문");
	expect(repointed.delivered).toBe(1);
	expect(repointed.pushed[0]?.text).toBe("argument] 본문");
	// The inner target lived inside a fragment that is not a token, so it must not
	// decide where this delivery lands.
	expect(repointed.pushed[0]?.replyToMessageId).toBeUndefined();
});

test("a note that is nothing but control syntax produces no delivery at all", async () => {
	const { pushed, delivered } = await monitorNote("[REPLY:1544305635179495434] [BREAK] [REACT:👍]");
	expect(delivered).toBe(0);
	expect(pushed).toHaveLength(0);
});

test("one silent note never suppresses a visible sibling in the same batch", async () => {
	// Silence is a PER-EVENT choice. Joining the batch into one string first and
	// judging the aggregate threw away the note the owner still needed to see.
	const { pushed, delivered } = await monitorNote("[SILENT]", "디스크 85% 찼습니다");
	expect(delivered).toBe(1);
	expect(pushed).toHaveLength(1);
	expect((pushed[0] as { text: string }).text).toBe("디스크 85% 찼습니다");
});
