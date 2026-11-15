import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname } from "node:path";

export type PendingImage = {
	readonly messageId: string;
	readonly channelId: string;
	readonly request?: Readonly<Record<string, unknown>>;
	readonly terminal?: boolean;
	readonly terminalAt?: number;
	readonly completed?: boolean;
	readonly completedAt?: number;
	readonly failureAt?: number;
	readonly audited?: boolean;
	readonly dm?: boolean;
};

const SNOWFLAKE = /^\d{1,20}$/;
/** A completed tombstone only has to outlive the message's own recovery window. */
const TOMBSTONE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** Bound on retained tombstones even when their snowflakes are recent. */
const TOMBSTONE_CAP = 1_000;
/** Terminal evidence is retained long enough to outlive every history rewalk. */
const TERMINAL_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
/** New unresolved intents are refused past this bound; existing intents keep retrying. */
const MAX_UNRESOLVED_IMAGES = 1_000;

export class ImageOutboxCapacityError extends Error {
	constructor() {
		super(`Image outbox holds ${MAX_UNRESOLVED_IMAGES} unresolved intents`);
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
		("terminalAt" in value && typeof value.terminalAt !== "number") ||
		("completed" in value && typeof value.completed !== "boolean") ||
		("completedAt" in value && typeof value.completedAt !== "number") ||
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
 * Bounds the journal on every read and write: completed tombstones by completion
 * age and count, terminal evidence by message age (history is never rewalked this
 * far), and unresolved rows only by admission, never by silent eviction.
 */
function pruneEntries(entries: readonly PendingImage[], now = Date.now()): PendingImage[] {
	const kept: PendingImage[] = [];
	const completed: PendingImage[] = [];
	for (const entry of entries) {
		let at: number;
		try {
			at = entry.completed ? (entry.completedAt ?? snowflakeTime(entry.messageId)) : snowflakeTime(entry.messageId);
		} catch {
			continue;
		}
		if (entry.completed) {
			if (now - at <= TOMBSTONE_RETENTION_MS) completed.push(entry);
		} else if (entry.terminal) {
			// Legacy terminal rows have no timestamp; keep them rather than guess.
			if (entry.terminalAt === undefined || now - entry.terminalAt <= TERMINAL_RETENTION_MS) kept.push(entry);
		} else {
			kept.push(entry);
		}
	}
	completed.sort((a, b) => (BigInt(b.messageId) > BigInt(a.messageId) ? 1 : -1));
	return [...kept, ...completed.slice(0, TOMBSTONE_CAP)];
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
			if (
				index === -1 &&
				!entry.completed &&
				!entry.terminal &&
				pruned.filter((pending) => !pending.completed && !pending.terminal).length > MAX_UNRESOLVED_IMAGES
			)
				throw new ImageOutboxCapacityError();
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
