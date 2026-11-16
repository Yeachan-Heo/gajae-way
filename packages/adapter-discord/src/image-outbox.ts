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
	readonly failureAt?: number;
	readonly audited?: boolean;
	readonly dm?: boolean;
};

const SNOWFLAKE = /^\d{1,20}$/;
/** New unresolved intents are refused past this bound; existing intents keep retrying. */
const MAX_UNRESOLVED_IMAGES = 1_000;
/** Terminal evidence is finite without eviction: admission backs off at these bounds. */
const MAX_TERMINAL_IMAGES_PER_CHANNEL = 500;
const MAX_TERMINAL_IMAGES = 10_000;
const MAX_COMPLETED_IMAGES = 10_000;

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
		("failureAt" in value && typeof value.failureAt !== "number") ||
		("audited" in value && typeof value.audited !== "boolean") ||
		("dm" in value && typeof value.dm !== "boolean") ||
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
 * terminal evidence is removed by watermark compaction in the caller, and
 * unresolved rows alone use admission backoff.
 */
function pruneEntries(entries: readonly PendingImage[]): PendingImage[] {
	const kept: PendingImage[] = [];
	for (const entry of entries) {
		try {
			snowflakeTime(entry.messageId);
			kept.push(entry);
		} catch {}
	}
	return kept;
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
	try {
		const parent = await open(directory, "r");
		try {
			await parent.sync();
		} finally {
			await parent.close();
		}
	} catch {
		// Best-effort: the rename is already atomic; only the directory entry's
		// durability window widens on platforms that reject a directory fsync.
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
				return entry.dm ? [{ messageId: entry.messageId, channelId: entry.channelId, dm: true }] : [];
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
			if (index === -1) entries.push(entry);
			else entries[index] = entry;
			// Enforce the journal bounds on the write path too: a long-lived session
			// must not grow until the next recovery load.
			const pruned = pruneEntries(entries);
			if (index === -1 && !entry.completed && !entry.terminal) {
				const unresolved = pruned.filter((pending) => !pending.completed && !pending.terminal);
				const terminal = pruned.filter((pending) => pending.terminal);
				const completed = pruned.filter((pending) => pending.completed);
				if (
					unresolved.length >= MAX_UNRESOLVED_IMAGES ||
					terminal.filter((pending) => pending.channelId === entry.channelId).length >=
						MAX_TERMINAL_IMAGES_PER_CHANNEL ||
					terminal.length >= MAX_TERMINAL_IMAGES ||
					completed.length >= MAX_COMPLETED_IMAGES
				)
					throw new ImageOutboxCapacityError();
			}
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
