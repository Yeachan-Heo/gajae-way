import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { OpsSloResult, OpsSloWindow, SloDayResult, SloDayStatus } from "@gajae-gateway/protocol";
import type { StoreCore } from "../store/core";

export const SLO_TICK_MS = 5 * 60_000;
export const SLO_TICK_EXTENSION_MS = 6 * 60_000;
export const SLO_MAX_EVENTS_PER_DAY = 1_000;
export const SLO_COVERAGE_GAP_MS = 90 * 60_000;
export const SLO_UPDATE_WINDOW_MS = 30 * 60_000;
export const DEFAULT_SLO_DAYS = 7;

export type SloEventCode = "broker_unavailable" | "persona_bind_failed";
export type RestartState = "queued" | "running" | "ok" | "stale" | "failed";
export type RestartStepResult = "pending" | "ok" | "stale" | "failed" | "skipped";

export interface SloWindowRecord {
	readonly id: string;
	readonly trigger: string;
	readonly state: string;
	readonly requestedAt: string;
	readonly finishedAt: string | null;
	readonly stepResults: readonly string[];
}

export interface SloEvent {
	readonly atMs: number;
	readonly bootId: string;
	readonly seq: number;
}

export interface EligibleSpan {
	readonly id: string;
	readonly startMs: number;
	readonly endMs: number;
}

const PROCESS_IDENTITY = { bootId: randomUUID(), seq: 0 };
const STEP_RESULTS = new Set<RestartStepResult>(["pending", "ok", "stale", "failed", "skipped"]);
const RESTART_STATES = new Set<RestartState>(["queued", "running", "ok", "stale", "failed"]);

export interface SloRecorderOptions {
	readonly home: string;
	readonly now?: () => number;
	readonly log?: (line: string) => void;
	/** Test seam; production uses one module-level id and sequence per process. */
	readonly bootId?: string;
}

/** Durable gateway SLO journal, ready-coverage recorder, and restart-window reader. */
export class SloRecorder {
	readonly #core: StoreCore;
	readonly #receiptPath: string;
	readonly #now: () => number;
	readonly #log: (line: string) => void;
	readonly #identity: { bootId: string; seq: number };
	#readySince: number | undefined;
	#timer: ReturnType<typeof setInterval> | undefined;
	#tickPromise: Promise<void> | undefined;
	#stopped = false;

	constructor(core: StoreCore, options: SloRecorderOptions) {
		this.#core = core;
		this.#receiptPath = join(options.home, "restart-stack.json");
		this.#now = options.now ?? Date.now;
		this.#log = options.log ?? ((line) => console.info(line));
		this.#identity = options.bootId === undefined ? PROCESS_IDENTITY : { bootId: options.bootId, seq: 0 };
	}

	/** Single event-counting function for the two canonical failure sites. */
	note(code: SloEventCode, at: number = this.#now()): void {
		if (!Number.isFinite(at)) throw new RangeError("SLO event timestamp must be finite");
		const atMs = Math.trunc(at);
		const date = utcDate(atMs);
		const seq = ++this.#identity.seq;
		const key = `slo:${date}:${code}:ts`;
		try {
			this.#core.transaction(() => {
				const raw = this.#core.metaGet(key);
				const events = raw === undefined ? [] : parseEvents(raw);
				const otherCode: SloEventCode = code === "broker_unavailable" ? "persona_bind_failed" : "broker_unavailable";
				const otherRaw = this.#core.metaGet(`slo:${date}:${otherCode}:ts`);
				const otherEvents = otherRaw === undefined ? [] : parseEvents(otherRaw);
				if (events === undefined || otherEvents === undefined) {
					this.#core.metaSet(`slo:${date}:invalid`, "1");
					return;
				}
				if (events.length + otherEvents.length >= SLO_MAX_EVENTS_PER_DAY) {
					this.#core.metaSet(`slo:${date}:overflow`, "1");
					return;
				}
				events.push([atMs, this.#identity.bootId, seq]);
				this.#core.metaSet(key, JSON.stringify(events));
			});
		} catch (error) {
			try {
				this.#core.transaction(() => this.#core.metaSet(`slo:${date}:invalid`, "1"));
			} catch {}
			this.#log(`slo_write_failed code=${code} at=${new Date(atMs).toISOString()} detail=${diagnostic(error)}`);
		}
		this.#log(`slo_event boot=${this.#identity.bootId} seq=${seq} code=${code} at=${new Date(atMs).toISOString()}`);
	}

	/** Start periodic ready coverage and capture the current restart receipt after the server is listening. */
	async start(): Promise<void> {
		if (this.#readySince !== undefined || this.#stopped) return;
		this.#readySince = this.#now();
		await this.refreshWindow();
		this.#timer = setInterval(() => {
			const active = this.#tickPromise;
			if (active) return;
			this.#tickPromise = this.tick().finally(() => {
				this.#tickPromise = undefined;
			});
		}, SLO_TICK_MS);
	}

	/** Record one tick interval, split precisely at UTC midnight. */
	tick(at: number = this.#now()): Promise<void> {
		if (!Number.isFinite(at)) return Promise.reject(new RangeError("SLO tick timestamp must be finite"));
		const endMs = Math.trunc(at);
		const startMs = Math.max(this.#readySince ?? endMs - SLO_TICK_MS, endMs - SLO_TICK_MS);
		return this.#recordAlive(startMs, endMs).then(async () => {
			await this.refreshWindow();
		});
	}

	/** Final coverage tick on orderly shutdown; timer work is drained before the final observation. */
	async stop(): Promise<void> {
		if (this.#stopped) return;
		this.#stopped = true;
		if (this.#timer !== undefined) clearInterval(this.#timer);
		this.#timer = undefined;
		await this.#tickPromise;
		if (this.#readySince === undefined) return;
		try {
			await this.tick();
		} catch (error) {
			this.#log(`slo_final_tick_failed detail=${diagnostic(error)}`);
		}
	}
	/** Refresh latest receipt state, then evaluate the requested fully elapsed UTC days. */
	async evaluate(days: number = DEFAULT_SLO_DAYS, now: Date = new Date(this.#now())): Promise<OpsSloResult> {
		await this.refreshWindow();
		return evaluateSlo(this.#core, days, now);
	}

	/** Reads restart-stack.json without importing CLI code; malformed receipts are ignored. */
	async refreshWindow(): Promise<boolean> {
		let raw: string;
		try {
			raw = await readFile(this.#receiptPath, "utf8");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT")
				this.#log(`slo_window_read_failed detail=${diagnostic(error)}`);
			return false;
		}
		const receipt = parseRestartReceipt(raw);
		if (!receipt) return false;
		const { id, ...window } = receipt;
		try {
			this.#core.transaction(() => this.#core.metaSet(`slo:window:${id}`, JSON.stringify(window)));
			return true;
		} catch (error) {
			this.#log(`slo_window_write_failed id=${receipt.id} detail=${diagnostic(error)}`);
			return false;
		}
	}

	#recordAlive(startMs: number, endMs: number): Promise<void> {
		if (endMs < startMs) return Promise.resolve();
		const intervals = splitAtUtcMidnight(startMs, endMs);
		try {
			this.#core.transaction(() => {
				for (const interval of intervals) {
					const key = `slo:${interval.date}:alive`;
					const raw = this.#core.metaGet(key);
					const existing = raw === undefined ? [] : parseIntervals(raw);
					if (!existing) {
						this.#core.metaSet(`slo:${interval.date}:invalid`, "1");
						continue;
					}
					mergeTickInterval(existing, interval.startMs, interval.endMs);
					this.#core.metaSet(
						key,
						JSON.stringify(
							existing.map(({ startMs: intervalStart, endMs: intervalEnd }) => [intervalStart, intervalEnd]),
						),
					);
				}
			});
		} catch (error) {
			this.#log(`slo_alive_write_failed detail=${diagnostic(error)}`);
		}
		return Promise.resolve();
	}
}

/** The shared eligibility predicate for event counting, coverage, and journal corroboration. */
export function eligibleSpans(windows: readonly SloWindowRecord[]): EligibleSpan[] {
	const spans: EligibleSpan[] = [];
	for (const window of windows) {
		const requestedAt = Date.parse(window.requestedAt);
		const finishedAt = window.finishedAt === null ? Number.NaN : Date.parse(window.finishedAt);
		if (
			window.trigger !== "update" ||
			window.state !== "ok" ||
			window.stepResults.length === 0 ||
			!window.stepResults.every((result) => result === "ok" || result === "skipped") ||
			!Number.isFinite(requestedAt) ||
			!Number.isFinite(finishedAt) ||
			requestedAt > finishedAt
		)
			continue;
		spans.push({
			id: window.id,
			startMs: requestedAt,
			endMs: Math.min(finishedAt, requestedAt + SLO_UPDATE_WINDOW_MS),
		});
	}
	return spans.sort((left, right) => left.startMs - right.startMs || left.id.localeCompare(right.id));
}

/** Pure durable-evidence evaluation used by ops.slo and ops.cycle. */
export function evaluateSlo(core: StoreCore, days: number = DEFAULT_SLO_DAYS, now: Date = new Date()): OpsSloResult {
	if (!Number.isSafeInteger(days) || days < 1 || days > 3_650)
		throw new RangeError("SLO days must be an integer from 1 to 3650");
	const nowMs = now.getTime();
	if (!Number.isFinite(nowMs)) throw new RangeError("SLO evaluation time must be valid");
	const todayStart = utcDayStart(nowMs);
	const windowRead = readWindows(core);
	const spans = eligibleSpans(windowRead.windows);
	const resultDays: SloDayResult[] = [];
	for (let offset = days; offset >= 1; offset--) {
		const startMs = todayStart - offset * DAY_MS;
		const endMs = startMs + DAY_MS;
		resultDays.push(evaluateDay(core, startMs, endMs, nowMs, spans, windowRead.windows, windowRead.corrupt));
	}
	const windows = windowRead.windows
		.map(projectWindow)
		.sort((left, right) => left.requestedAt.localeCompare(right.requestedAt) || left.id.localeCompare(right.id));
	return {
		days: resultDays,
		windows,
		ok: resultDays.length === days && resultDays.every((day) => day.status === "ok"),
	};
}

export function classifySloEvents<T extends SloEvent>(
	events: readonly T[],
	windows: readonly SloWindowRecord[],
): {
	readonly counted: readonly T[];
	readonly excluded: readonly T[];
} {
	const spans = eligibleSpans(windows);
	const counted: T[] = [];
	const excluded: T[] = [];
	for (const event of events) {
		if (spans.some((span) => event.atMs >= span.startMs && event.atMs <= span.endMs)) excluded.push(event);
		else counted.push(event);
	}
	return { counted, excluded };
}

const DAY_MS = 24 * 60 * 60_000;

function evaluateDay(
	core: StoreCore,
	startMs: number,
	endMs: number,
	nowMs: number,
	spans: readonly EligibleSpan[],
	windows: readonly SloWindowRecord[],
	windowCorrupt: boolean,
): SloDayResult {
	const date = utcDate(startMs);
	const eventRead = readDayEvents(core, date);
	const aliveRead = readDayIntervals(core, date);
	const counted = classifySloEvents(eventRead.events, windows).counted.filter(
		(event) => event.atMs >= startMs && event.atMs < endMs,
	);
	const excluded =
		eventRead.events.length -
		counted.length -
		eventRead.events.filter((event) => event.atMs < startMs || event.atMs >= endMs).length;
	const intervals = clipAndUnion(aliveRead.intervals, startMs, endMs);
	const gaps = uncoveredGaps(intervals, startMs, endMs);
	const maxUncoveredGapMs = gaps.reduce((largest, gap) => Math.max(largest, gap.endMs - gap.startMs), 0);
	const complete = gaps.every(
		(gap) =>
			gap.endMs - gap.startMs <= SLO_COVERAGE_GAP_MS ||
			spans.some((span) => gap.startMs >= span.startMs && gap.endMs <= span.endMs),
	);
	const aliveMs = intervals.reduce((total, interval) => total + interval.endMs - interval.startMs, 0);
	const pendingWindows = windows.filter((window) => {
		if (window.trigger !== "update") return false;
		const requestedAt = Date.parse(window.requestedAt);
		if (!Number.isFinite(requestedAt) || requestedAt >= endMs || nowMs < startMs) return false;
		const unfinished = window.state === "queued" || window.state === "running" || window.finishedAt === null;
		return unfinished && requestedAt < endMs && nowMs > startMs;
	});
	const pending = pendingWindows.some((window) => nowMs < Date.parse(window.requestedAt) + SLO_UPDATE_WINDOW_MS);
	const insufficientWindow = pendingWindows.length > 0 && !pending;
	const overflow = core.metaGet(`slo:${date}:overflow`) === "1" || eventRead.events.length > SLO_MAX_EVENTS_PER_DAY;
	const corruptEvidence =
		windowCorrupt || eventRead.corrupt || aliveRead.corrupt || core.metaGet(`slo:${date}:invalid`) === "1";
	let status: SloDayStatus;
	if (counted.length > 0 || overflow) status = "failed";
	else if (pending) status = "pending";
	else if (insufficientWindow || corruptEvidence || !complete) status = "insufficient";
	else status = "ok";
	return {
		date,
		status,
		complete,
		aliveMs,
		maxUncoveredGapMs,
		broker_unavailable: counted.filter((event) => event.code === "broker_unavailable").length,
		persona_bind_failed: counted.filter((event) => event.code === "persona_bind_failed").length,
		excluded,
	};
}

interface ParsedEvent extends SloEvent {
	readonly code: SloEventCode;
}

function readDayEvents(core: StoreCore, date: string): { events: ParsedEvent[]; corrupt: boolean } {
	const codes: readonly SloEventCode[] = ["broker_unavailable", "persona_bind_failed"];
	const events: ParsedEvent[] = [];
	const startMs = Date.parse(`${date}T00:00:00.000Z`);
	const endMs = startMs + DAY_MS;
	let corrupt = false;
	for (const code of codes) {
		const raw = core.metaGet(`slo:${date}:${code}:ts`);
		if (raw === undefined) continue;
		const parsed = parseEvents(raw);
		if (!parsed) {
			corrupt = true;
			continue;
		}
		for (const [atMs, bootId, seq] of parsed) {
			if (atMs < startMs || atMs >= endMs) corrupt = true;
			else events.push({ atMs, bootId, seq, code });
		}
	}
	return { events, corrupt };
}

function readDayIntervals(
	core: StoreCore,
	date: string,
): { intervals: Array<{ startMs: number; endMs: number }>; corrupt: boolean } {
	const raw = core.metaGet(`slo:${date}:alive`);
	if (raw === undefined) return { intervals: [], corrupt: false };
	const intervals = parseIntervals(raw);
	if (!intervals) return { intervals: [], corrupt: true };
	const startMs = Date.parse(`${date}T00:00:00.000Z`);
	const endMs = startMs + DAY_MS;
	if (!intervals.every((interval) => interval.startMs >= startMs && interval.endMs <= endMs))
		return { intervals: [], corrupt: true };
	return { intervals, corrupt: false };
}

function readWindows(core: StoreCore): { windows: SloWindowRecord[]; corrupt: boolean } {
	const windows: SloWindowRecord[] = [];
	let corrupt = false;
	for (const key of core.metaKeysInRange("slo:window:", "slo:window;")) {
		const raw = core.metaGet(key);
		if (raw === undefined) {
			corrupt = true;
			continue;
		}
		try {
			const window = parseStoredWindow(JSON.parse(raw), key.slice("slo:window:".length));
			if (!window) corrupt = true;
			else windows.push(window);
		} catch {
			corrupt = true;
		}
	}
	return { windows, corrupt };
}

function projectWindow(window: SloWindowRecord): OpsSloWindow {
	const requestedAt = Date.parse(window.requestedAt);
	const finishedAt = window.finishedAt === null ? Number.NaN : Date.parse(window.finishedAt);
	const verified = eligibleSpans([window]).length === 1;
	const eligibleStart = verified ? requestedAt : undefined;
	const eligibleEnd = verified ? Math.min(finishedAt, requestedAt + SLO_UPDATE_WINDOW_MS) : undefined;
	const overCapMs =
		Number.isFinite(finishedAt) && Number.isFinite(requestedAt)
			? Math.max(0, finishedAt - (requestedAt + SLO_UPDATE_WINDOW_MS))
			: 0;
	return {
		id: window.id,
		trigger: window.trigger,
		state: window.state,
		requestedAt: window.requestedAt,
		finishedAt: window.finishedAt,
		verified,
		eligibleStart: eligibleStart === undefined ? null : new Date(eligibleStart).toISOString(),
		eligibleEnd: eligibleEnd === undefined ? null : new Date(eligibleEnd).toISOString(),
		overCapMs,
	};
}

function parseRestartReceipt(raw: string): SloWindowRecord | undefined {
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch {
		return undefined;
	}
	if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
	const receipt = value as Record<string, unknown>;
	if (
		typeof receipt.id !== "string" ||
		receipt.id.length === 0 ||
		receipt.id.length > 128 ||
		(receipt.trigger !== "update" && receipt.trigger !== "manual") ||
		typeof receipt.state !== "string" ||
		!RESTART_STATES.has(receipt.state as RestartState) ||
		typeof receipt.requestedAt !== "string" ||
		!Number.isFinite(Date.parse(receipt.requestedAt)) ||
		!Array.isArray(receipt.steps)
	)
		return undefined;
	const stepResults: string[] = [];
	for (const step of receipt.steps) {
		if (typeof step !== "object" || step === null || Array.isArray(step)) return undefined;
		const result = (step as Record<string, unknown>).result;
		if (typeof result !== "string" || !STEP_RESULTS.has(result as RestartStepResult)) return undefined;
		stepResults.push(result);
	}
	let finishedAt: string | null = null;
	if (receipt.finishedAt !== undefined) {
		if (typeof receipt.finishedAt !== "string" || !Number.isFinite(Date.parse(receipt.finishedAt))) return undefined;
		finishedAt = receipt.finishedAt;
	}
	return {
		id: receipt.id,
		trigger: receipt.trigger,
		state: receipt.state,
		requestedAt: receipt.requestedAt,
		finishedAt,
		stepResults,
	};
}

function parseStoredWindow(value: unknown, id: string): SloWindowRecord | undefined {
	if (!id || id.length > 128 || typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
	const window = value as Record<string, unknown>;
	if (
		(window.trigger !== "update" && window.trigger !== "manual") ||
		typeof window.state !== "string" ||
		!RESTART_STATES.has(window.state as RestartState) ||
		typeof window.requestedAt !== "string" ||
		!Number.isFinite(Date.parse(window.requestedAt)) ||
		(window.finishedAt !== null &&
			(typeof window.finishedAt !== "string" || !Number.isFinite(Date.parse(window.finishedAt)))) ||
		!Array.isArray(window.stepResults) ||
		!window.stepResults.every((result) => typeof result === "string" && STEP_RESULTS.has(result as RestartStepResult))
	)
		return undefined;
	return {
		id,
		trigger: window.trigger,
		state: window.state,
		requestedAt: window.requestedAt,
		finishedAt: window.finishedAt as string | null,
		stepResults: window.stepResults as string[],
	};
}

function parseEvents(raw: string): Array<[number, string, number]> | undefined {
	try {
		const value: unknown = JSON.parse(raw);
		if (!Array.isArray(value)) return undefined;
		const events: Array<[number, string, number]> = [];
		for (const event of value) {
			if (
				!Array.isArray(event) ||
				event.length !== 3 ||
				!Number.isSafeInteger(event[0]) ||
				typeof event[1] !== "string" ||
				event[1].length === 0 ||
				!Number.isSafeInteger(event[2]) ||
				event[2] < 1
			)
				return undefined;
			events.push([event[0] as number, event[1] as string, event[2] as number]);
		}
		return events;
	} catch {
		return undefined;
	}
}

function parseIntervals(raw: string): Array<{ startMs: number; endMs: number }> | undefined {
	try {
		const value: unknown = JSON.parse(raw);
		if (!Array.isArray(value)) return undefined;
		const intervals: Array<{ startMs: number; endMs: number }> = [];
		for (const interval of value) {
			if (
				!Array.isArray(interval) ||
				interval.length !== 2 ||
				!Number.isSafeInteger(interval[0]) ||
				!Number.isSafeInteger(interval[1]) ||
				interval[1] < interval[0]
			)
				return undefined;
			intervals.push({ startMs: interval[0] as number, endMs: interval[1] as number });
		}
		return intervals;
	} catch {
		return undefined;
	}
}

function splitAtUtcMidnight(startMs: number, endMs: number): Array<{ date: string; startMs: number; endMs: number }> {
	if (startMs === endMs) return [{ date: utcDate(startMs), startMs, endMs }];
	const result: Array<{ date: string; startMs: number; endMs: number }> = [];
	for (let cursor = startMs; cursor < endMs; ) {
		const date = utcDate(cursor);
		const dayEnd = utcDayStart(cursor) + DAY_MS;
		const segmentEnd = Math.min(endMs, dayEnd);
		result.push({ date, startMs: cursor, endMs: segmentEnd });
		cursor = segmentEnd;
	}
	return result;
}

function mergeTickInterval(intervals: Array<{ startMs: number; endMs: number }>, startMs: number, endMs: number): void {
	const last = intervals.at(-1);
	if (last && endMs <= last.endMs) return;
	if (last && endMs - last.endMs <= SLO_TICK_EXTENSION_MS) {
		last.startMs = Math.min(last.startMs, startMs);
		last.endMs = endMs;
		return;
	}
	intervals.push({ startMs, endMs });
}

function clipAndUnion(
	intervals: readonly { startMs: number; endMs: number }[],
	startMs: number,
	endMs: number,
): Array<{ startMs: number; endMs: number }> {
	const clipped = intervals
		.map((interval) => ({ startMs: Math.max(startMs, interval.startMs), endMs: Math.min(endMs, interval.endMs) }))
		.filter((interval) => interval.endMs > interval.startMs)
		.sort((left, right) => left.startMs - right.startMs || left.endMs - right.endMs);
	const union: Array<{ startMs: number; endMs: number }> = [];
	for (const interval of clipped) {
		const last = union.at(-1);
		if (last && interval.startMs <= last.endMs) last.endMs = Math.max(last.endMs, interval.endMs);
		else union.push({ ...interval });
	}
	return union;
}

function uncoveredGaps(
	intervals: readonly { startMs: number; endMs: number }[],
	startMs: number,
	endMs: number,
): Array<{ startMs: number; endMs: number }> {
	const gaps: Array<{ startMs: number; endMs: number }> = [];
	let cursor = startMs;
	for (const interval of intervals) {
		if (interval.startMs > cursor) gaps.push({ startMs: cursor, endMs: interval.startMs });
		cursor = Math.max(cursor, interval.endMs);
	}
	if (cursor < endMs) gaps.push({ startMs: cursor, endMs });
	return gaps;
}

function utcDate(atMs: number): string {
	return new Date(atMs).toISOString().slice(0, 10);
}

function utcDayStart(atMs: number): number {
	const date = new Date(atMs);
	return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function diagnostic(error: unknown): string {
	return (error instanceof Error ? error.message : String(error)).replace(/[\r\n\0]/g, " ").slice(0, 300);
}
