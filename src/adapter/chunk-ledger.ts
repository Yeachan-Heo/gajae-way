import * as fs from "node:fs";
import * as path from "node:path";

/** Bounded so the ledger cannot grow without limit while being rewritten per send. */
export const MAX_TRACKED_CHUNKS = 5_000;

export class ChunkLedgerError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ChunkLedgerError";
	}
}

/**
 * Durable record of chunks already confirmed on the platform.
 *
 * A multi-chunk reply commits its journal event only after every chunk is sent,
 * so an interruption mid-set replays the whole set. Deterministic nonces let the
 * platform suppress duplicates, but Discord's `enforce_nonce` deduplication is
 * TIME-BOUNDED: an outage longer than that window would re-post chunks that
 * already landed. This ledger makes the skip decision durable instead, so replay
 * is safe regardless of elapsed time.
 */
export interface ChunkLedger {
	/** Message id recorded for an already-confirmed chunk, if any. */
	recorded(key: string): string | undefined;
	/** Records a chunk as confirmed on the platform. */
	record(key: string, platformMessageId: string): void;
}

/** In-memory ledger for tests and for callers without a durable state directory. */
export class MemoryChunkLedger implements ChunkLedger {
	readonly #entries = new Map<string, string>();

	recorded(key: string): string | undefined {
		return this.#entries.get(key);
	}

	record(key: string, platformMessageId: string): void {
		this.#entries.set(key, platformMessageId);
		if (this.#entries.size > MAX_TRACKED_CHUNKS) {
			const oldest = this.#entries.keys().next();
			if (!oldest.done) this.#entries.delete(oldest.value);
		}
	}
}

/**
 * File-backed ledger. An ABSENT file is a legitimate fresh start; a present but
 * unreadable or malformed one fails closed, because silently starting empty would
 * discard exactly the records that prevent duplicate posts.
 */
export class FileChunkLedger implements ChunkLedger {
	readonly #filePath: string;
	#entries: Record<string, string>;

	constructor(stateDir: string, fileName = "chunk-ledger.json") {
		if (!stateDir) throw new ChunkLedgerError("A chunk ledger requires a state directory.");
		this.#filePath = path.join(stateDir, fileName);
		this.#entries = readLedger(this.#filePath);
	}

	recorded(key: string): string | undefined {
		return this.#entries[key];
	}

	record(key: string, platformMessageId: string): void {
		if (!platformMessageId) throw new ChunkLedgerError("A confirmed chunk requires a platform message id.");
		this.#entries[key] = platformMessageId;
		const keys = Object.keys(this.#entries);
		if (keys.length > MAX_TRACKED_CHUNKS) {
			for (const stale of keys.slice(0, keys.length - MAX_TRACKED_CHUNKS)) delete this.#entries[stale];
		}
		fs.mkdirSync(path.dirname(this.#filePath), { recursive: true });
		const temporary = `${this.#filePath}.${process.pid}.tmp`;
		fs.writeFileSync(temporary, JSON.stringify(this.#entries));
		fs.renameSync(temporary, this.#filePath);
	}
}

function readLedger(filePath: string): Record<string, string> {
	let raw: string;
	try {
		raw = fs.readFileSync(filePath, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException)?.code === "ENOENT") return {};
		throw new ChunkLedgerError(`Chunk ledger at ${filePath} could not be read: ${error instanceof Error ? error.message : String(error)}`);
	}
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch (error) {
		throw new ChunkLedgerError(`Chunk ledger at ${filePath} is not valid JSON: ${error instanceof Error ? error.message : String(error)}`);
	}
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ChunkLedgerError(`Chunk ledger at ${filePath} is not an object.`);
	}
	const entries: Record<string, string> = {};
	for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
		if (typeof entry !== "string" || !entry) {
			throw new ChunkLedgerError(`Chunk ledger at ${filePath} has a malformed entry for ${key}.`);
		}
		entries[key] = entry;
	}
	return entries;
}
