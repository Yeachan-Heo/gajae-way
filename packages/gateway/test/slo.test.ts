import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	classifySloEvents,
	eligibleSpans,
	evaluateSlo,
	SLO_MAX_EVENTS_PER_DAY,
	SloRecorder,
	type SloWindowRecord,
} from "../src/ops/slo";
import { Store } from "../src/store";

const stores: Store[] = [];
const directories: string[] = [];
const DAY_MS = 24 * 60 * 60_000;
const OCT_9 = Date.UTC(2026, 9, 9);
const OCT_10_NOON = new Date(Date.UTC(2026, 9, 10, 12));

async function fixture() {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-slo-"));
	directories.push(home);
	const store = await Store.open(join(home, "gateway.db"));
	stores.push(store);
	return { home, store };
}

afterEach(async () => {
	for (const store of stores.splice(0)) store.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

function setMeta(store: Store, key: string, value: string): void {
	store.transaction(() => store.core.metaSet(key, value));
}

function setAlive(store: Store, date: string, intervals: readonly (readonly [number, number])[]): void {
	setMeta(store, `slo:${date}:alive`, JSON.stringify(intervals));
}

function receipt(input: {
	id: string;
	requestedAt: string;
	finishedAt?: string;
	trigger?: "update" | "manual";
	state?: "queued" | "running" | "ok" | "stale" | "failed";
	stepResults?: readonly ("pending" | "ok" | "stale" | "failed" | "skipped")[];
}) {
	return {
		id: input.id,
		trigger: input.trigger ?? "update",
		state: input.state ?? "ok",
		requestedAt: input.requestedAt,
		...(input.finishedAt === undefined ? {} : { finishedAt: input.finishedAt }),
		steps: (input.stepResults ?? ["ok"]).map((result, index) => ({ label: `step-${index}`, result })),
	};
}

async function writeReceipt(home: string, value: unknown): Promise<void> {
	await writeFile(join(home, "restart-stack.json"), JSON.stringify(value));
}

describe("durable SLO evidence", () => {
	test("evaluates only fully elapsed UTC days and fails closed on missing or partial coverage", async () => {
		const { store } = await fixture();
		const _oct7 = Date.UTC(2026, 9, 7);
		const oct8 = Date.UTC(2026, 9, 8);
		const oct9 = Date.UTC(2026, 9, 9);
		setAlive(store, "2026-10-08", [[oct8, oct8 + DAY_MS]]);
		setAlive(store, "2026-10-09", [[oct9 + 2 * 60 * 60_000, oct9 + DAY_MS - 2 * 60 * 60_000]]);

		const result = evaluateSlo(store.core, 3, new Date(Date.UTC(2026, 9, 10, 12)));
		expect(result.days.map(({ date, status }) => [date, status])).toEqual([
			["2026-10-07", "insufficient"],
			["2026-10-08", "ok"],
			["2026-10-09", "insufficient"],
		]);
		expect(result.days[0]?.maxUncoveredGapMs).toBe(DAY_MS);
		expect(result.ok).toBe(false);
	});

	test("coverage accepts an 89-minute gap, rejects a 91-minute gap, and measures the final tail", async () => {
		const { store } = await fixture();
		const gapStart = OCT_9 + 10 * 60 * 60_000;
		for (const [gapMinutes, complete] of [
			[89, true],
			[91, false],
		] as const) {
			const gap = gapMinutes * 60_000;
			setAlive(store, "2026-10-09", [
				[OCT_9, gapStart],
				[gapStart + gap, OCT_9 + DAY_MS],
			]);
			const day = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
			expect(day.complete).toBe(complete);
			expect(day.maxUncoveredGapMs).toBe(gap);
		}

		setAlive(store, "2026-10-09", [[OCT_9, OCT_9 + DAY_MS - 60 * 60_000]]);
		const tail = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
		expect(tail.complete).toBe(true);
		expect(tail.maxUncoveredGapMs).toBe(60 * 60_000);
		expect(tail.aliveMs).toBe(DAY_MS - 60 * 60_000);
	});

	test("ticks merge duplicates and process restarts, but preserve gaps after the six-minute extension", async () => {
		const { home, store } = await fixture();
		const first = new SloRecorder(store.core, { home, bootId: "boot-one", log: () => {} });
		const second = new SloRecorder(store.core, { home, bootId: "boot-two", log: () => {} });
		const t0 = Date.parse("2026-10-09T00:05:00.000Z");
		await first.tick(t0);
		await first.tick(t0);
		await second.tick(t0 + 5 * 60_000);
		await second.tick(t0 + 12 * 60_000);
		const intervals = JSON.parse(store.core.metaGet("slo:2026-10-09:alive")!) as number[][];
		expect(intervals).toEqual([
			[Date.parse("2026-10-09T00:00:00.000Z"), t0 + 5 * 60_000],
			[t0 + 7 * 60_000, t0 + 12 * 60_000],
		]);
	});

	test("boot coverage begins after start, splits at UTC midnight, and gets a final shutdown tick", async () => {
		const { home, store } = await fixture();
		let now = Date.parse("2026-10-08T23:58:00.000Z");
		const recorder = new SloRecorder(store.core, { home, now: () => now, bootId: "boot-midnight", log: () => {} });
		expect(store.core.metaGet("slo:2026-10-08:alive")).toBeUndefined();
		await recorder.start();
		now = Date.parse("2026-10-09T00:03:00.000Z");
		await recorder.tick();
		await recorder.stop();
		expect(JSON.parse(store.core.metaGet("slo:2026-10-08:alive")!)).toEqual([
			[Date.parse("2026-10-08T23:58:00.000Z"), Date.parse("2026-10-09T00:00:00.000Z")],
		]);
		expect(JSON.parse(store.core.metaGet("slo:2026-10-09:alive")!)).toEqual([
			[Date.parse("2026-10-09T00:00:00.000Z"), Date.parse("2026-10-09T00:03:00.000Z")],
		]);
	});

	test("corrupt window, event, or coverage evidence never certifies a complete day", async () => {
		const { store } = await fixture();
		setAlive(store, "2026-10-09", [[OCT_9, OCT_9 + DAY_MS]]);
		setMeta(store, "slo:window:broken", "not json");
		let day = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
		expect(day.complete).toBe(true);
		expect(day.status).toBe("insufficient");

		store.transaction((tx) => store.core.metaDeleteRange(tx, "slo:window:", "slo:window;"));
		setMeta(store, "slo:2026-10-09:broker_unavailable:ts", "not json");
		day = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
		expect(day.status).toBe("insufficient");

		setMeta(store, "slo:2026-10-09:alive", JSON.stringify([[OCT_9, OCT_9 + 2 * DAY_MS]]));
		day = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
		expect(day.aliveMs).toBe(0);
		expect(day.status).toBe("insufficient");
	});
	test("notes persist bootId/seq identity, log distinct lines, and cap the daily journal", async () => {
		const { home, store } = await fixture();
		const logs: string[] = [];
		const at = Date.parse("2026-10-09T12:00:00.000Z");
		new SloRecorder(store.core, { home, bootId: "gateway-a", log: (line) => logs.push(line) }).note(
			"broker_unavailable",
			at,
		);
		new SloRecorder(store.core, { home, bootId: "gateway-b", log: (line) => logs.push(line) }).note(
			"broker_unavailable",
			at,
		);
		expect(JSON.parse(store.core.metaGet("slo:2026-10-09:broker_unavailable:ts")!)).toEqual([
			[at, "gateway-a", 1],
			[at, "gateway-b", 1],
		]);
		expect(logs).toEqual([
			`slo_event boot=gateway-a seq=1 code=broker_unavailable at=${new Date(at).toISOString()}`,
			`slo_event boot=gateway-b seq=1 code=broker_unavailable at=${new Date(at).toISOString()}`,
		]);

		const overflow = new SloRecorder(store.core, { home, bootId: "overflow", log: () => {} });
		for (let index = 0; index < SLO_MAX_EVENTS_PER_DAY + 1; index++) overflow.note("persona_bind_failed", at);
		const events = JSON.parse(store.core.metaGet("slo:2026-10-09:persona_bind_failed:ts")!) as unknown[];
		expect(events).toHaveLength(SLO_MAX_EVENTS_PER_DAY - 2);
		expect(JSON.parse(store.core.metaGet("slo:2026-10-09:broker_unavailable:ts")!)).toHaveLength(2);
		expect(events.length + 2).toBe(SLO_MAX_EVENTS_PER_DAY);
		expect(store.core.metaGet("slo:2026-10-09:overflow")).toBe("1");
		expect(evaluateSlo(store.core, 1, OCT_10_NOON).days[0]?.status).toBe("failed");
	});

	test("refreshes restart receipts defensively and upserts the latest state by receipt id", async () => {
		const { home, store } = await fixture();
		const recorder = new SloRecorder(store.core, { home, log: () => {} });
		const running = receipt({ id: "receipt-a", requestedAt: "2026-10-09T10:00:00.000Z", state: "running" });
		await writeReceipt(home, running);
		expect(await recorder.refreshWindow()).toBe(true);
		expect(JSON.parse(store.core.metaGet("slo:window:receipt-a")!)).toEqual({
			trigger: "update",
			state: "running",
			requestedAt: "2026-10-09T10:00:00.000Z",
			finishedAt: null,
			stepResults: ["ok"],
		});

		await writeReceipt(
			home,
			receipt({
				id: "receipt-a",
				requestedAt: "2026-10-09T10:00:00.000Z",
				finishedAt: "2026-10-09T10:31:00.000Z",
			}),
		);
		expect(await recorder.refreshWindow()).toBe(true);
		expect(JSON.parse(store.core.metaGet("slo:window:receipt-a")!).state).toBe("ok");
		await writeFile(join(home, "restart-stack.json"), "not json");
		expect(await recorder.refreshWindow()).toBe(false);
		expect(JSON.parse(store.core.metaGet("slo:window:receipt-a")!).state).toBe("ok");
	});

	test("verified update windows cap event exclusions at 30 minutes for 29/30/31-minute receipts", async () => {
		for (const durationMinutes of [29, 30, 31]) {
			const { home, store } = await fixture();
			const recorder = new SloRecorder(store.core, { home, bootId: `boot-${durationMinutes}`, log: () => {} });
			const requestedAt = Date.parse("2026-10-09T10:00:00.000Z");
			const finishedAt = requestedAt + durationMinutes * 60_000;
			await writeReceipt(
				home,
				receipt({
					id: `update-${durationMinutes}`,
					requestedAt: new Date(requestedAt).toISOString(),
					finishedAt: new Date(finishedAt).toISOString(),
				}),
			);
			await recorder.refreshWindow();
			for (const offset of [28 * 60_000 + 59_000, 29 * 60_000, 30 * 60_000, 30 * 60_000 + 1_000])
				recorder.note("broker_unavailable", requestedAt + offset);
			const result = evaluateSlo(store.core, 1, OCT_10_NOON);
			const day = result.days[0]!;
			expect(day.status).toBe("failed");
			expect(day.broker_unavailable).toBe(durationMinutes === 29 ? 2 : 1);
			expect(day.excluded).toBe(durationMinutes === 29 ? 2 : 3);
			const window = result.windows[0]!;
			expect(window.verified).toBe(true);
			expect(window.eligibleStart).toBe(new Date(requestedAt).toISOString());
			expect(window.eligibleEnd).toBe(new Date(requestedAt + Math.min(durationMinutes, 30) * 60_000).toISOString());
			expect(window.overCapMs).toBe(Math.max(0, durationMinutes - 30) * 60_000);
		}
	});

	test("long updates exclude only their first 30 minutes, and do not excuse a two-hour coverage gap", async () => {
		const { home, store } = await fixture();
		const recorder = new SloRecorder(store.core, { home, bootId: "long-update", log: () => {} });
		const requestedAt = Date.parse("2026-10-09T10:00:00.000Z");
		const finishedAt = requestedAt + 2 * 60 * 60_000;
		await writeReceipt(
			home,
			receipt({
				id: "update-two-hours",
				requestedAt: new Date(requestedAt).toISOString(),
				finishedAt: new Date(finishedAt).toISOString(),
			}),
		);
		await recorder.refreshWindow();
		for (const offset of [0, 30 * 60_000, 30 * 60_000 + 1_000, 2 * 60 * 60_000])
			recorder.note("persona_bind_failed", requestedAt + offset);
		setAlive(store, "2026-10-09", [
			[OCT_9, requestedAt],
			[finishedAt, OCT_9 + DAY_MS],
		]);

		const day = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
		expect(day.persona_bind_failed).toBe(2);
		expect(day.excluded).toBe(2);
		expect(day.maxUncoveredGapMs).toBe(2 * 60 * 60_000);
		expect(day.complete).toBe(false);
	});

	test("manual, failed, stale, and failed-step receipts never exclude events", async () => {
		const { home, store } = await fixture();
		const recorder = new SloRecorder(store.core, { home, bootId: "unverified", log: () => {} });
		const fixtures = [
			{ id: "manual", trigger: "manual" as const, state: "ok" as const, stepResults: ["ok"] as const },
			{ id: "failed", trigger: "update" as const, state: "failed" as const, stepResults: ["ok"] as const },
			{ id: "stale", trigger: "update" as const, state: "stale" as const, stepResults: ["ok"] as const },
			{ id: "step-failed", trigger: "update" as const, state: "ok" as const, stepResults: ["failed"] as const },
			{ id: "empty-steps", trigger: "update" as const, state: "ok" as const, stepResults: [] as const },
		];
		for (let index = 0; index < fixtures.length; index++) {
			const item = fixtures[index]!;
			const requestedAt = Date.parse(`2026-10-09T${String(2 + index * 5).padStart(2, "0")}:00:00.000Z`);
			await writeReceipt(
				home,
				receipt({
					...item,
					requestedAt: new Date(requestedAt).toISOString(),
					finishedAt: new Date(requestedAt + 20 * 60_000).toISOString(),
				}),
			);
			await recorder.refreshWindow();
			recorder.note("broker_unavailable", requestedAt + 5 * 60_000);
		}
		const result = evaluateSlo(store.core, 1, OCT_10_NOON);
		expect(result.days[0]?.broker_unavailable).toBe(5);
		expect(result.days[0]?.excluded).toBe(0);
		expect(result.windows.every((window) => !window.verified)).toBe(true);
	});

	test("no-restart has no receipt, and unfinished updates are pending then insufficient", async () => {
		const { home, store } = await fixture();
		const recorder = new SloRecorder(store.core, { home, bootId: "unfinished", log: () => {} });
		expect(await recorder.refreshWindow()).toBe(false);
		expect(evaluateSlo(store.core, 1, OCT_10_NOON).windows).toEqual([]);

		const requestedAt = Date.parse("2026-10-09T23:50:00.000Z");
		await writeReceipt(
			home,
			receipt({
				id: "still-running",
				requestedAt: new Date(requestedAt).toISOString(),
				state: "running",
			}),
		);
		await recorder.refreshWindow();
		recorder.note("broker_unavailable", requestedAt + 5 * 60_000);
		const pending = evaluateSlo(store.core, 7, new Date("2026-10-10T00:05:00.000Z"));
		expect(pending.days.at(-1)?.status).toBe("failed");
		expect(pending.days.at(-1)?.broker_unavailable).toBe(1);
		expect(pending.ok).toBe(false);

		setMeta(store, "slo:2026-10-09:broker_unavailable:ts", "[]");
		const noEventPending = evaluateSlo(store.core, 1, new Date("2026-10-10T00:05:00.000Z"));
		expect(noEventPending.days[0]?.status).toBe("pending");
		const overdue = evaluateSlo(store.core, 7, new Date("2026-10-10T00:21:00.000Z"));
		expect(overdue.days.at(-1)?.status).toBe("insufficient");
		expect(overdue.ok).toBe(false);
	});

	test("overwritten unfinished receipts remain running and cannot certify a day", async () => {
		const { home, store } = await fixture();
		const recorder = new SloRecorder(store.core, { home, log: () => {} });
		await writeReceipt(
			home,
			receipt({
				id: "old-running",
				requestedAt: "2026-10-09T23:50:00.000Z",
				state: "running",
			}),
		);
		await recorder.refreshWindow();
		await writeReceipt(
			home,
			receipt({
				id: "later-manual",
				requestedAt: "2026-10-10T00:01:00.000Z",
				finishedAt: "2026-10-10T00:02:00.000Z",
				trigger: "manual",
			}),
		);
		await recorder.refreshWindow();
		const result = evaluateSlo(store.core, 1, new Date("2026-10-10T00:05:00.000Z"));
		expect(result.windows.find((window) => window.id === "old-running")?.state).toBe("running");
		expect(result.days[0]?.status).toBe("pending");
	});

	test("the shared eligibility predicate classifies randomized events and coverage consistently", async () => {
		const { store } = await fixture();
		let seed = 91;
		const random = () => {
			seed = (seed * 48271) % 2_147_483_647;
			return seed / 2_147_483_647;
		};
		for (let iteration = 0; iteration < 40; iteration++) {
			const windows: SloWindowRecord[] = Array.from({ length: 5 }, (_, index) => {
				const requestedAt = OCT_9 + Math.floor(random() * 20 * 60) * 60_000;
				const duration = Math.floor(random() * 120) * 60_000;
				return {
					id: `w-${iteration}-${index}`,
					trigger: random() < 0.7 ? "update" : "manual",
					state: random() < 0.75 ? "ok" : random() < 0.5 ? "failed" : "stale",
					requestedAt: new Date(requestedAt).toISOString(),
					finishedAt: new Date(requestedAt + duration).toISOString(),
					stepResults: [random() < 0.85 ? "ok" : "failed"],
				};
			});
			const spans = eligibleSpans(windows);
			const events = Array.from({ length: 20 }, (_, index) => ({
				atMs: OCT_9 + Math.floor(random() * DAY_MS),
				bootId: `b-${iteration}`,
				seq: index + 1,
			}));
			const classification = classifySloEvents(events, windows);
			expect(classification.counted.length + classification.excluded.length).toBe(events.length);
			for (const event of events) {
				expect(classification.excluded.includes(event)).toBe(
					spans.some((span) => event.atMs >= span.startMs && event.atMs <= span.endMs),
				);
			}

			const gapStart = OCT_9 + Math.floor(random() * 20 * 60) * 60_000;
			const gapMs = (random() < 0.5 ? 89 : 91) * 60_000;
			const gapEnd = Math.min(OCT_9 + DAY_MS, gapStart + gapMs);
			const intervals: Array<[number, number]> = [];
			if (gapStart > OCT_9) intervals.push([OCT_9, gapStart]);
			if (gapEnd < OCT_9 + DAY_MS) intervals.push([gapEnd, OCT_9 + DAY_MS]);
			store.transaction((tx) => {
				store.core.metaDeleteRange(tx, "slo:", "slo;");
				for (const window of windows) store.core.metaSet(`slo:window:${window.id}`, JSON.stringify(window));
				store.core.metaSet("slo:2026-10-09:alive", JSON.stringify(intervals));
				store.core.metaSet(
					"slo:2026-10-09:broker_unavailable:ts",
					JSON.stringify(
						events.filter((_, index) => index % 2 === 0).map((event) => [event.atMs, event.bootId, event.seq]),
					),
				);
				store.core.metaSet(
					"slo:2026-10-09:persona_bind_failed:ts",
					JSON.stringify(
						events.filter((_, index) => index % 2 === 1).map((event) => [event.atMs, event.bootId, event.seq]),
					),
				);
			});
			const evaluated = evaluateSlo(store.core, 1, OCT_10_NOON).days[0]!;
			const inDate = events.filter((event) => event.atMs >= OCT_9 && event.atMs < OCT_9 + DAY_MS);
			const expectedCounted = classifySloEvents(inDate, windows).counted.length;
			expect(evaluated.broker_unavailable + evaluated.persona_bind_failed).toBe(expectedCounted);
			const expectedComplete =
				gapEnd - gapStart <= 90 * 60_000 || spans.some((span) => gapStart >= span.startMs && gapEnd <= span.endMs);
			expect(evaluated.complete).toBe(expectedComplete);
		}
	});

	test("separate gateway processes may both emit seq 1 without collapsing event identity", async () => {
		const { home, store } = await fixture();
		const at = Date.parse("2026-10-09T12:00:00.000Z");
		new SloRecorder(store.core, { home, bootId: "process-one", log: () => {} }).note("broker_unavailable", at);
		new SloRecorder(store.core, { home, bootId: "process-two", log: () => {} }).note("broker_unavailable", at);
		const events = JSON.parse(store.core.metaGet("slo:2026-10-09:broker_unavailable:ts")!) as unknown[][];
		expect(events.map((event) => event.slice(1))).toEqual([
			["process-one", 1],
			["process-two", 1],
		]);
	});
});
