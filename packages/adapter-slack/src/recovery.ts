import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import { SlackApiError, type SlackHistoryPage } from "./api";
import { adapterHome } from "./config";
import type { SlackInboundMessage } from "./main";
import { isSlackDmChannel } from "./origin";

export const RECOVERY_BOOTSTRAP_LOOKBACK_MS = 24 * 60 * 60 * 1000;
export const RECOVERY_PAGE_LIMIT = 200;
export const RECOVERY_MAX_PAGES = 10;
export const RECOVERY_KNOWN_DM_CAP = 100;
export const RECOVERY_KNOWN_DM_TTL_MS = 30 * 24 * 60 * 60 * 1000;
export const RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS = 3;
export const RECOVERY_RETRY_BASE_MS = 1_000;
export const RECOVERY_RETRY_MAX_MS = 60_000;

export interface RecoveryCursorState {
	readonly recoveredThrough: Readonly<Record<string, string>>;
	readonly knownDms: Readonly<Record<string, { readonly lastSeenAt: string }>>;
	readonly quarantined: Readonly<
		Record<string, { readonly reason: string; readonly failures: number; readonly since: string }>
	>;
	readonly deadLetters: readonly {
		readonly messageId: string;
		readonly conversationId: string;
		readonly reason: string;
		readonly at: string;
	}[];
}

export function recoveryCursorPath(home: string = adapterHome()): string {
	return join(home, "adapters", "slack", "recovery-cursor.json");
}

export async function loadRecoveryCursors(path: string): Promise<RecoveryCursorState> {
	let text: string;
	try {
		text = await readFile(path, "utf8");
	} catch (error) {
		if ((error as { code?: string }).code === "ENOENT") {
			return { recoveredThrough: {}, knownDms: {}, quarantined: {}, deadLetters: [] };
		}
		throw error;
	}
	try {
		const state = JSON.parse(text);
		const record = (value: unknown): value is Record<string, unknown> =>
			typeof value === "object" && value !== null && !Array.isArray(value);
		if (
			!record(state) ||
			!record(state.recoveredThrough) ||
			!record(state.knownDms) ||
			!record(state.quarantined) ||
			!Array.isArray(state.deadLetters) ||
			!Object.values(state.recoveredThrough).every((ts) => typeof ts === "string" && /^\d+\.\d+$/.test(ts)) ||
			!Object.values(state.knownDms).every(
				(entry) =>
					record(entry) && typeof entry.lastSeenAt === "string" && Number.isFinite(Date.parse(entry.lastSeenAt)),
			) ||
			!Object.values(state.quarantined).every(
				(entry) =>
					record(entry) &&
					typeof entry.reason === "string" &&
					Number.isInteger(entry.failures) &&
					(entry.failures as number) >= 0 &&
					typeof entry.since === "string",
			) ||
			!state.deadLetters.every(
				(entry) =>
					record(entry) &&
					[entry.messageId, entry.conversationId, entry.reason, entry.at].every((value) => typeof value === "string"),
			)
		) {
			throw new Error("Invalid Slack recovery cursor state");
		}
		return state as unknown as RecoveryCursorState;
	} catch (error) {
		throw new Error(`Cannot load Slack recovery cursors at ${path}`, { cause: error });
	}
}

/** Atomic replacement keeps interrupted saves from destroying the last recovery watermark. */
export async function saveRecoveryCursors(path: string, state: RecoveryCursorState): Promise<void> {
	await mkdir(dirname(path), { recursive: true, mode: 0o700 });
	const temporary = `${path}.${randomUUID()}.tmp`;
	try {
		const file = await open(temporary, "wx", 0o600);
		try {
			await file.writeFile(`${JSON.stringify(state, null, "\t")}\n`);
			await file.sync();
		} finally {
			await file.close();
		}
		await rename(temporary, path);
	} finally {
		await unlink(temporary).catch((error: NodeJS.ErrnoException) => {
			if (error.code !== "ENOENT") throw error;
		});
	}
}

export function pruneKnownDms(state: RecoveryCursorState, nowMs: number): RecoveryCursorState {
	const entries = Object.entries(state.knownDms)
		.filter(([, entry]) => Date.parse(entry.lastSeenAt) > nowMs - RECOVERY_KNOWN_DM_TTL_MS)
		.sort((a, b) => Date.parse(b[1].lastSeenAt) - Date.parse(a[1].lastSeenAt))
		.slice(0, RECOVERY_KNOWN_DM_CAP);
	return { ...state, knownDms: Object.fromEntries(entries) };
}

export function rememberKnownDm(state: RecoveryCursorState, channel: string, nowMs: number): RecoveryCursorState {
	return pruneKnownDms(
		isSlackDmChannel(channel)
			? {
					...state,
					knownDms: { ...state.knownDms, [channel]: { lastSeenAt: new Date(nowMs).toISOString() } },
				}
			: state,
		nowMs,
	);
}

export function tsIsAfter(a: string, b: string): boolean {
	const [as = "0", af = ""] = a.split(".");
	const [bs = "0", bf = ""] = b.split(".");
	const secondsA = BigInt(as);
	const secondsB = BigInt(bs);
	return secondsA !== secondsB
		? secondsA > secondsB
		: af.padEnd(Math.max(af.length, bf.length), "0") > bf.padEnd(Math.max(af.length, bf.length), "0");
}

export function tsFromTimestamp(ms: number): string {
	return `${Math.floor(ms / 1000)}.000000`;
}

export type RecoveryDelivery = "acked" | "duplicate" | "unavailable" | "skip";
export interface RecoveryHistoryPort {
	history(channel: string, options: { oldest: string; cursor?: string; limit: number }): Promise<SlackHistoryPage>;
	replies(
		channel: string,
		threadTs: string,
		options: { oldest: string; cursor?: string; limit: number },
	): Promise<SlackHistoryPage>;
}
export interface RecoveryOptions {
	readonly cursor?: string;
	readonly nowMs: number;
	readonly pageLimit?: number;
	readonly maxPages?: number;
	readonly botUserId: string;
	deliver(message: SlackInboundMessage): Promise<RecoveryDelivery>;
}
export interface RecoveryOutcome {
	readonly advancedTo?: string;
	readonly delivered: number;
	readonly duplicates: number;
	readonly skipped: number;
	readonly truncated: boolean;
	readonly failed: boolean;
	readonly fetchError?: string;
	readonly permanent?: boolean;
}

/** Live traffic never advances this watermark: seeing a new message proves nothing about the gap. */
export async function recoverConversation(
	port: RecoveryHistoryPort,
	channel: string,
	options: RecoveryOptions,
): Promise<RecoveryOutcome> {
	const oldest = options.cursor ?? tsFromTimestamp(options.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS);
	const limit = Math.min(RECOVERY_PAGE_LIMIT, Math.max(1, Math.floor(options.pageLimit ?? RECOVERY_PAGE_LIMIT)));
	const maxPages = Math.max(1, Math.floor(options.maxPages ?? RECOVERY_MAX_PAGES));
	const messages = new Map<string, SlackInboundMessage>();
	let truncated = false;
	let advancedTo: string | undefined;
	let delivered = 0;
	let duplicates = 0;
	let skipped = 0;
	const outcome = (failed: boolean): RecoveryOutcome => ({
		advancedTo: truncated ? undefined : advancedTo,
		delivered,
		duplicates,
		skipped,
		truncated,
		failed,
	});
	const collect = async (
		fetchPage: (cursor?: string) => Promise<SlackHistoryPage>,
		threadTs?: string,
	): Promise<void> => {
		let cursor: string | undefined;
		const cursors = new Set<string>();
		for (let page = 0; page < maxPages; page++) {
			const result = await fetchPage(cursor);
			for (const raw of result.messages) {
				if (typeof raw.ts !== "string" || !/^\d+\.\d+$/.test(raw.ts)) throw new SlackApiError(200, "invalid_response");
				if (raw.ts === threadTs || !tsIsAfter(raw.ts, oldest)) continue;
				messages.set(raw.ts, { ...raw, channel, ...(threadTs ? { thread_ts: threadTs } : {}) } as SlackInboundMessage);
			}
			if (!result.has_more && !result.next_cursor) return;
			if (!result.next_cursor || cursors.has(result.next_cursor) || page + 1 === maxPages) {
				truncated = true;
				return;
			}
			cursor = result.next_cursor;
			cursors.add(cursor);
		}
	};
	try {
		await collect((cursor) => port.history(channel, { oldest, cursor, limit }));
		for (const message of [...messages.values()]) {
			const raw = message as SlackInboundMessage & { reply_count?: number };
			if ((raw.reply_count ?? 0) > 0 || message.thread_ts === message.ts) {
				await collect((cursor) => port.replies(channel, message.ts, { oldest, cursor, limit }), message.ts);
			}
		}
	} catch (error) {
		return {
			...outcome(true),
			fetchError: error instanceof SlackApiError ? error.code : String(error),
			permanent:
				error instanceof SlackApiError &&
				["channel_not_found", "not_in_channel", "missing_scope", "is_archived", "invalid_auth"].includes(error.code),
		};
	}
	// Slack pages newest-first. Sort the entire bounded window before acknowledging anything;
	// an incomplete window cannot advance a durable cursor past older, unfetched messages.
	const ordered = [...messages.values()].sort((a, b) => (tsIsAfter(a.ts, b.ts) ? 1 : tsIsAfter(b.ts, a.ts) ? -1 : 0));
	for (const message of ordered) {
		let result: RecoveryDelivery;
		try {
			result = message.user === options.botUserId ? "skip" : await options.deliver(message);
		} catch {
			return outcome(true);
		}
		if (result === "unavailable") return outcome(true);
		if (result === "acked") delivered++;
		else if (result === "duplicate") duplicates++;
		else skipped++;
		advancedTo = message.ts;
	}
	return outcome(false);
}

/** A single retry loop prevents reconnect storms from creating parallel recovery passes. */
export class RecoveryScheduler {
	private running = false;
	private followUp = false;
	private stopped = false;
	private waiting = false;
	private backoff = RECOVERY_RETRY_BASE_MS;
	private timer?: ReturnType<typeof setTimeout>;
	private wake?: () => void;

	constructor(
		private readonly run: () => Promise<boolean>,
		private readonly sleep?: (ms: number) => Promise<void>,
		private readonly random: () => number = Math.random,
	) {}

	get retryPending(): boolean {
		return this.waiting;
	}

	trigger(): void {
		if (this.stopped) return;
		if (this.running) {
			this.followUp = true;
			this.wake?.();
			return;
		}
		this.running = true;
		void this.loop();
	}

	stop(): void {
		this.stopped = true;
		this.followUp = false;
		this.wake?.();
	}

	private async loop(): Promise<void> {
		try {
			while (!this.stopped) {
				this.followUp = false;
				let completed = false;
				try {
					completed = await this.run();
				} catch {
					// A failed pass is incomplete, not a reason to abandon future recovery.
				}
				if (this.stopped) return;
				if (completed) this.backoff = RECOVERY_RETRY_BASE_MS;
				if (this.followUp) continue;
				if (completed) return;
				const delay = Math.min(RECOVERY_RETRY_MAX_MS, this.backoff + Math.floor(this.random() * this.backoff * 0.2));
				this.backoff = Math.min(RECOVERY_RETRY_MAX_MS, this.backoff * 2);
				this.waiting = true;
				try {
					await new Promise<void>((resolve) => {
						this.wake = resolve;
						if (this.sleep) void this.sleep(delay).then(resolve, resolve);
						else this.timer = setTimeout(resolve, delay);
					});
				} finally {
					if (this.timer !== undefined) clearTimeout(this.timer);
					this.timer = undefined;
					this.wake = undefined;
					this.waiting = false;
				}
			}
		} finally {
			this.running = false;
		}
	}
}
