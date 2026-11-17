import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname } from "node:path";
import type { OriginRef } from "@gajae-gateway/protocol";

export type PendingImage = {
	readonly messageId: string;
	readonly channelId: string;
	readonly request?: Readonly<Record<string, unknown>>;
	readonly terminal?: boolean;
	readonly completed?: boolean;
	readonly completedAt?: number;
	readonly createdAt?: number;
	readonly origin?: OriginRef;
	/** Attachment identity captured at admission, before preparation can fail. */
	readonly sourceIdentity?: string;
	/** Message text captured at admission, kept with sourceIdentity. */
	readonly sourceText?: string;
	readonly failureAt?: number;
	readonly audited?: boolean;
	/** The intent started as live ingress, so a retry may still select its destination. */
	readonly live?: boolean;
	/** The gateway explicitly declined this send: no work ran, so no quota-charged tombstone. */
	readonly declined?: boolean;
	/** A restore turned this completed DM into a replay target; it is already queued. */
	readonly restored?: boolean;
	readonly dm?: boolean;
};

const SNOWFLAKE = /^\d{1,20}$/;
/** New unresolved intents are refused past this bound; existing intents keep retrying. */
const MAX_UNRESOLVED_IMAGES = 1_000;
/** Terminal evidence is finite without eviction: admission backs off at these bounds. */
const MAX_TERMINAL_IMAGES_PER_CHANNEL = 500;
const MAX_TERMINAL_IMAGES = 10_000;
const MAX_COMPLETED_IMAGES = 10_000;
/**
 * Declined sends keep dedupe evidence, not delivery evidence: they never gate
 * admission and never keep an authorized image out, so they carry their own
 * bound and are evicted oldest-first. Losing one only costs a re-declined
 * retry; the gateway's inbound ledger still owns exactly-once.
 */
const MAX_DECLINED_IMAGES = 2_000;

export class ImageOutboxCapacityError extends Error {
	constructor() {
		super("Image outbox admission bound reached");
		this.name = "ImageOutboxCapacityError";
	}
}
const DISCORD_EPOCH_MS = 1_420_070_400_000n;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSnowflake(value: unknown): value is string {
	return typeof value === "string" && SNOWFLAKE.test(value);
}

function snowflakeTime(messageId: string): number {
	return Number((BigInt(messageId) >> 22n) + DISCORD_EPOCH_MS);
}

/**
 * Get the age of an image entry in milliseconds from now.
 * Uses createdAt if available, otherwise derives from the message ID snowflake.
 */
function getEntryAge(entry: PendingImage, now: number): number {
	const timestamp = entry.createdAt ?? snowflakeTime(entry.messageId);
	return now - timestamp;
}

function validateEntry(value: unknown): asserts value is PendingImage {
	if (
		!isRecord(value) ||
		!isSnowflake(value.messageId) ||
		typeof value.channelId !== "string" ||
		value.channelId === "" ||
		("request" in value && !isRecord(value.request)) ||
		("terminal" in value && typeof value.terminal !== "boolean") ||
		("completed" in value && typeof value.completed !== "boolean") ||
		("completedAt" in value && typeof value.completedAt !== "number") ||
		("createdAt" in value && typeof value.createdAt !== "number") ||
		("origin" in value && !isRecord(value.origin)) ||
		("sourceIdentity" in value && typeof value.sourceIdentity !== "string") ||
		("sourceText" in value && typeof value.sourceText !== "string") ||
		("failureAt" in value && typeof value.failureAt !== "number") ||
		("audited" in value && typeof value.audited !== "boolean") ||
		("live" in value && typeof value.live !== "boolean") ||
		("declined" in value && typeof value.declined !== "boolean") ||
		("restored" in value && typeof value.restored !== "boolean") ||
		("dm" in value && typeof value.dm !== "boolean") ||
		(value.declined === true && value.completed !== true) ||
		(value.terminal === true && value.completed === true)
	) {
		throw new TypeError("Invalid pending image record");
	}
}

/**
 * Loads every readable entry and drops individual corrupt records. One damaged
 * record must not disable image delivery for the whole adapter, and the dropped
 * record's id is logged (never its payload).
 */
async function readEntries(path: string): Promise<{ entries: PendingImage[]; dropped: number }> {
	let contents: string;
	try {
		contents = await readFile(path, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return { entries: [], dropped: 0 };
		throw error;
	}
	const value: unknown = JSON.parse(contents);
	if (!Array.isArray(value)) throw new TypeError("Invalid image outbox");
	const entries: PendingImage[] = [];
	let dropped = 0;
	for (const entry of value) {
		try {
			validateEntry(entry);
			entries.push(entry);
		} catch {
			dropped++;
			const id = isRecord(entry) && isSnowflake(entry.messageId) ? entry.messageId : "unknown";
			console.error(`Discord image outbox dropped an invalid record for message ${id}.`);
		}
	}
	return { entries, dropped };
}

/**
 * Keeps every readable row; time-based eviction is unsafe because a lagging
 * watermark could resurrect a rejected or already-delivered send. Completed and
 * terminal evidence is removed by watermark compaction in the caller.
 * When bounds are exceeded, evicts oldest unresolved and terminal rows by age
 * to prevent indefinite journal growth.
 */
function pruneEntries(entries: readonly PendingImage[]): PendingImage[] {
	const kept: PendingImage[] = [];
	for (const entry of entries) {
		try {
			snowflakeTime(entry.messageId);
			kept.push(entry);
		} catch {}
	}

	const now = Date.now();
	const evictedIds = new Set<string>();

	// Evict excess declined entries oldest-first
	const declined = kept.filter((entry) => entry.declined === true);
	if (declined.length > MAX_DECLINED_IMAGES) {
		const toEvict = declined
			.sort(
				(a, b) =>
					(a.completedAt ?? 0) - (b.completedAt ?? 0) ||
					(a.messageId < b.messageId ? -1 : a.messageId > b.messageId ? 1 : 0),
			)
			.slice(0, declined.length - MAX_DECLINED_IMAGES);
		for (const entry of toEvict) {
			evictedIds.add(entry.messageId);
		}
	}

	// Evict excess unresolved entries oldest-first (leave room for one more new entry)
	const unresolved = kept.filter((entry) => !entry.declined && !entry.completed && !entry.terminal);
	if (unresolved.length >= MAX_UNRESOLVED_IMAGES) {
		const toEvict = unresolved
			.sort((a, b) => getEntryAge(a, now) - getEntryAge(b, now))
			.slice(0, unresolved.length - MAX_UNRESOLVED_IMAGES + 1);
		for (const entry of toEvict) {
			evictedIds.add(entry.messageId);
		}
	}

	// Evict excess terminal entries oldest-first (leave room for one more new entry)
	const terminal = kept.filter((entry) => !entry.declined && entry.terminal && !entry.completed);
	if (terminal.length >= MAX_TERMINAL_IMAGES) {
		const toEvict = terminal
			.sort((a, b) => getEntryAge(a, now) - getEntryAge(b, now))
			.slice(0, terminal.length - MAX_TERMINAL_IMAGES + 1);
		for (const entry of toEvict) {
			evictedIds.add(entry.messageId);
		}
	}

	// Evict excess terminal entries per channel oldest-first (leave room for one more new entry)
	const terminalByChannel = new Map<string, PendingImage[]>();
	for (const entry of terminal) {
		if (!evictedIds.has(entry.messageId)) {
			const list = terminalByChannel.get(entry.channelId) ?? [];
			list.push(entry);
			terminalByChannel.set(entry.channelId, list);
		}
	}
	for (const [_channelId, entries] of terminalByChannel) {
		if (entries.length >= MAX_TERMINAL_IMAGES_PER_CHANNEL) {
			const toEvict = entries
				.sort((a, b) => getEntryAge(a, now) - getEntryAge(b, now))
				.slice(0, entries.length - MAX_TERMINAL_IMAGES_PER_CHANNEL + 1);
			for (const entry of toEvict) {
				evictedIds.add(entry.messageId);
			}
		}
	}

	return kept.filter((entry) => !evictedIds.has(entry.messageId));
}

async function writeEntries(path: string, entries: readonly PendingImage[]): Promise<void> {
	const contents = `${JSON.stringify(entries)}\n`;
	const directory = dirname(path);
	await mkdir(directory, { recursive: true, mode: 0o700 });
	const temporary = `${path}.${process.pid}.tmp`;
	try {
		const file = await open(temporary, "w", 0o600);
		try {
			await file.writeFile(contents, "utf8");
			await file.sync();
		} finally {
			await file.close();
		}
		await rename(temporary, path);
	} finally {
		await unlink(temporary).catch(() => {});
	}
	await syncOutboxDirectory(directory);
}

/** Fsync the renamed directory entry; only unsupported directory syncs may degrade. */
export async function syncOutboxDirectory(directory: string, openFile: typeof open = open): Promise<void> {
	try {
		const parent = await openFile(directory, "r");
		try {
			await parent.sync();
		} finally {
			await parent.close();
		}
	} catch (error) {
		// The rename is atomic, but its durability depends on the directory fsync.
		// Only platforms that cannot fsync a directory may degrade; real I/O errors
		// must fail admission rather than claim a crash-durable intent.
		const code = (error as { code?: unknown } | null | undefined)?.code;
		if (code !== "EINVAL" && code !== "ENOTSUP" && code !== "EISDIR" && code !== "EPERM" && code !== "EBADF")
			throw error;
	}
}

export class ImageOutbox {
	readonly #path: string;
	#pending: Promise<void> = Promise.resolve();
	#entries: PendingImage[] | undefined;
	#index: Map<string, PendingImage> | undefined;
	#dropped = 0;

	constructor(path: string) {
		this.#path = path;
	}

	#serialize<T>(operation: () => Promise<T>): Promise<T> {
		const result = this.#pending.then(operation).catch((error: unknown) => {
			this.#entries = undefined;
			this.#index = undefined;
			if (error instanceof ImageOutboxCapacityError) throw error;
			// Do not log error messages: callers may have supplied sensitive payloads.
			const code =
				typeof error === "object" &&
				error !== null &&
				"code" in error &&
				typeof error.code === "string" &&
				/^[A-Z0-9_]+$/.test(error.code)
					? ` code=${error.code}`
					: "";
			console.error(`Discord image outbox persistence operation failed${code}; the caller must retry.`);
			throw error;
		});
		this.#pending = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async load(): Promise<readonly PendingImage[]> {
		return this.#serialize(async () => {
			const entries = await this.#loadEntries();
			const pruned = pruneEntries(entries);
			if (this.#dropped > 0 || pruned.length !== entries.length) {
				await writeEntries(this.#path, pruned);
				this.#entries = pruned;
				this.#index = new Map(pruned.map((entry) => [entry.messageId, entry]));
				this.#dropped = 0;
			}
			return [...pruned];
		});
	}

	async #loadEntries(): Promise<PendingImage[]> {
		if (!this.#entries) {
			const loaded = await readEntries(this.#path);
			this.#entries = loaded.entries;
			this.#dropped = loaded.dropped;
			this.#index = new Map(this.#entries.map((entry) => [entry.messageId, entry]));
		}
		return this.#entries;
	}

	/** Retain non-enumerable DM targets as replay intents after a gateway restore. */
	async invalidateCompleted(): Promise<void> {
		return this.#serialize(async () => {
			const entries = await this.#loadEntries();
			if (!entries.some((entry) => entry.completed)) return;
			const kept: PendingImage[] = entries.flatMap((entry) => {
				if (!entry.completed) return [entry];
				return entry.dm ? [{ messageId: entry.messageId, channelId: entry.channelId, dm: true, restored: true }] : [];
			});
			await writeEntries(this.#path, kept);
			this.#entries = kept;
			this.#index = new Map(kept.map((entry) => [entry.messageId, entry]));
		});
	}

	async get(messageId: string): Promise<PendingImage | undefined> {
		return this.#serialize(async () => {
			await this.#loadEntries();
			return this.#index?.get(messageId);
		});
	}

	async put(entry: PendingImage): Promise<void> {
		return this.#serialize(async () => {
			validateEntry(entry);
			const entries = [...(await this.#loadEntries())];
			const index = entries.findIndex((pending) => pending.messageId === entry.messageId);
			const existing = index === -1 ? undefined : entries[index];
			// Final evidence is monotonic: a stale owner that prepared before a delete/edit
			// or before acknowledgement must never turn a terminal or completed row back
			// into a fresh send. Forward evidence writes (terminal -> completed) still land.
			if (
				existing &&
				(existing.terminal === true || existing.completed === true) &&
				!entry.terminal &&
				!entry.completed
			)
				return;
			if (index === -1) entries.push(entry);
			else entries[index] = entry;
			// Check capacity before pruning to ensure rejections happen before eviction
			if (index === -1 && !entry.completed && !entry.terminal) {
				// Declined entries are reclaimable evidence, never delivery quota:
				// one declined image must not be able to block a later authorized one.
				const unresolved = entries.filter((pending) => !pending.declined && !pending.completed && !pending.terminal);
				const terminal = entries.filter((pending) => !pending.declined && pending.terminal && !pending.completed);
				const completed = entries.filter((pending) => pending.completed && pending.declined !== true);
				if (
					unresolved.length >= MAX_UNRESOLVED_IMAGES ||
					terminal.filter((pending) => pending.channelId === entry.channelId).length >=
						MAX_TERMINAL_IMAGES_PER_CHANNEL ||
					terminal.length >= MAX_TERMINAL_IMAGES ||
					completed.length >= MAX_COMPLETED_IMAGES
				)
					throw new ImageOutboxCapacityError();
			}
			// Enforce the journal bounds on the write path too: a long-lived session
			// must not grow until the next recovery load.
			const pruned = pruneEntries(entries);
			await writeEntries(this.#path, pruned);
			this.#entries = pruned;
			this.#dropped = 0;
			this.#index = new Map(pruned.map((pending) => [pending.messageId, pending]));
		});
	}

	/**
	 * Drops records rejected by `keep` in one atomic write. Terminal rows are
	 * removed only after the recovery watermark has passed them, so a replay can
	 * never turn an intentionally rejected image back into a fresh send.
	 */
	async compact(keep: (entry: PendingImage) => boolean): Promise<void> {
		return this.#serialize(async () => {
			const entries = await this.#loadEntries();
			const kept = entries.filter(keep);
			if (kept.length === entries.length) return;
			await writeEntries(this.#path, kept);
			this.#entries = kept;
			this.#dropped = 0;
			this.#index = new Map(kept.map((entry) => [entry.messageId, entry]));
		});
	}

	async remove(messageId: string): Promise<void> {
		return this.#serialize(async () => {
			const entries = (await this.#loadEntries()).filter((entry) => entry.messageId !== messageId);
			await writeEntries(this.#path, entries);
			this.#entries = entries;
			this.#dropped = 0;
			this.#index?.delete(messageId);
		});
	}
}
