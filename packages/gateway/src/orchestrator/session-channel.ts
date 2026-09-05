/**
 * Query multiplexer over the ONE resident `gjc sdk serve --stdio --session <id>`
 * relay a session already owns (I4a, path (ii) from the pinned-runtime
 * recordings). The relay is a byte relay for the host WebSocket: frames written to
 * its stdin as `query_request` are answered on stdout as `query_response` carrying
 * the same `id`, interleaved with the live lifecycle / `turn_stream` frames the
 * tail consumes. This class owns the writer and the `id` correlation; the tail
 * keeps every frame that is not a response to one of its requests.
 *
 * Scope is deliberately minimal: `session.checkpoint`, `transcript.list` with its
 * same-connection continuation, `Q23` oversized-item chunks and `turn.result`.
 * No prompt or control routing lives here (that is I9); nothing is spawned here.
 *
 * Verified protocol (gjc 0.16.3, artifacts/pinned-runtime-pagination-0.16.3.json):
 * - `session.checkpoint` -> `{ok, result:{checkpointToken, revisionId}}`.
 * - `transcript.list` seeded with `input.checkpointToken`, continued with the
 *   top-level `cursor` = previous `page.continuationCursor`, until `page.complete`.
 *   Continuation cursors are bound to THIS connection; using them elsewhere fails.
 * - an item over the page budget arrives as `{id, error:{code:"item_too_large"},
 *   continuations:[{query:"Q23", field, ...}]}`; each field is fetched by `Q23`
 *   whose page items are `{field, itemId, byteOffset, body, complete}` and are
 *   continued with the same `cursor` discipline until `complete`.
 */

export class ChannelQueryError extends Error {
	readonly code: string;
	constructor(message: string, code: string) {
		super(message);
		this.name = "ChannelQueryError";
		this.code = code;
	}
}

export class TranscriptIncompleteError extends Error {
	readonly pages: number;
	constructor(message: string, pages: number) {
		super(message);
		this.name = "TranscriptIncompleteError";
		this.pages = pages;
	}
}

export interface TranscriptRow {
	readonly id: string;
	readonly role: string;
	readonly ts: string | undefined;
	/** Full body text; `body` is preferred over `textSummary`, never truncated. */
	readonly body: string;
}

export interface TranscriptSnapshot {
	readonly revision: string;
	readonly generation: number | undefined;
	readonly rows: readonly TranscriptRow[];
	readonly complete: true;
	readonly source: "channel";
}

export interface ChannelTransport {
	/** Writes one NDJSON frame to the relay's stdin. */
	write(line: string): void | Promise<void>;
	/** Registers the frame sink; the transport calls it for every stdout line. */
	onLine(listener: (line: string) => void): () => void;
}

export interface SessionChannelOptions {
	readonly sessionId: string;
	readonly transport: ChannelTransport;
	readonly requestTimeoutMs?: number;
	readonly maxPages?: number;
	readonly now?: () => number;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	readonly log?: (line: string) => void;
	readonly newId?: () => string;
}

type Pending = { resolve(frame: Record<string, unknown>): void; reject(error: Error): void; timer: unknown };

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_PAGES = 5_000;

export class SessionChannel {
	readonly sessionId: string;
	readonly #transport: ChannelTransport;
	readonly #timeoutMs: number;
	readonly #maxPages: number;
	readonly #setTimeout: (work: () => void, delayMs: number) => unknown;
	readonly #clearTimeout: (timer: unknown) => void;
	readonly #log: (line: string) => void;
	readonly #newId: () => string;
	readonly #pending = new Map<string, Pending>();
	readonly #detach: () => void;
	#orphanFrames = 0;
	#closed = false;

	constructor(options: SessionChannelOptions) {
		this.sessionId = options.sessionId;
		this.#transport = options.transport;
		this.#timeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		this.#maxPages = options.maxPages ?? DEFAULT_MAX_PAGES;
		this.#setTimeout = options.setTimeout ?? ((work, delayMs) => setTimeout(work, delayMs));
		this.#clearTimeout = options.clearTimeout ?? ((timer) => clearTimeout(timer as ReturnType<typeof setTimeout>));
		this.#log = options.log ?? (() => {});
		this.#newId = options.newId ?? (() => crypto.randomUUID());
		this.#detach = this.#transport.onLine((line) => this.#receive(line));
	}

	/** Frames that were responses to nobody; the tail still sees them. */
	get orphanFrames(): number {
		return this.#orphanFrames;
	}

	/**
	 * Returns true when the line was a response to one of this channel's requests
	 * and has been consumed. The tail must skip such lines.
	 */
	consumes(line: string): boolean {
		const frame = parseFrame(line);
		if (!frame) return false;
		return (
			(frame.type === "query_response" || frame.type === "control_response") && this.#pending.has(String(frame.id))
		);
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#detach();
		for (const [id, pending] of this.#pending) {
			this.#clearTimeout(pending.timer);
			pending.reject(new ChannelQueryError(`channel closed before ${id} was answered`, "channel_closed"));
		}
		this.#pending.clear();
	}

	async query(query: string, input: Record<string, unknown> = {}, cursor?: string): Promise<Record<string, unknown>> {
		if (this.#closed) throw new ChannelQueryError("channel is closed", "channel_closed");
		const id = this.#newId();
		const frame: Record<string, unknown> = { type: "query_request", id, query, input };
		if (cursor !== undefined) frame.cursor = cursor;
		const response = await new Promise<Record<string, unknown>>((resolve, reject) => {
			const timer = this.#setTimeout(() => {
				this.#pending.delete(id);
				reject(new ChannelQueryError(`${query} timed out after ${this.#timeoutMs}ms`, "channel_timeout"));
			}, this.#timeoutMs);
			this.#pending.set(id, { resolve, reject, timer });
			Promise.resolve(this.#transport.write(`${JSON.stringify(frame)}\n`)).catch((error: unknown) => {
				this.#pending.delete(id);
				this.#clearTimeout(timer);
				reject(new ChannelQueryError(`${query} could not be written: ${message(error)}`, "channel_write_failed"));
			});
		});
		if (response.ok !== true) {
			const error = recordOf(response.error);
			const code = typeof error?.code === "string" ? error.code : "query_failed";
			throw new ChannelQueryError(`${query} failed: ${code}`, code);
		}
		return response;
	}

	async checkpoint(): Promise<{ checkpointToken: string; revisionId: string | undefined }> {
		const response = await this.query("session.checkpoint");
		const result = recordOf(response.result);
		if (typeof result?.checkpointToken !== "string")
			throw new ChannelQueryError("session.checkpoint returned no checkpointToken", "checkpoint_invalid");
		return {
			checkpointToken: result.checkpointToken,
			revisionId: typeof result.revisionId === "string" ? result.revisionId : undefined,
		};
	}

	async turnResult(clientRef: string): Promise<Record<string, unknown> | undefined> {
		const response = await this.query("turn.result", { kind: "prompt", clientRef });
		return recordOf(response.result);
	}

	/**
	 * One complete transcript snapshot on this connection. Refuses (throws) rather
	 * than returning a partial read: an incomplete snapshot has no usable order.
	 */
	async readTranscript(): Promise<TranscriptSnapshot> {
		const checkpoint = await this.checkpoint();
		const rows: TranscriptRow[] = [];
		let revision: string | undefined;
		let generation: number | undefined;
		let cursor: string | undefined;
		let input: Record<string, unknown> = { checkpointToken: checkpoint.checkpointToken };
		for (let pages = 0; pages < this.#maxPages; pages++) {
			const response = await this.query("transcript.list", input, cursor);
			const page = recordOf(response.page);
			if (!page || !Array.isArray(page.items))
				throw new TranscriptIncompleteError("transcript.list returned no page", pages + 1);
			const pageRevision = typeof page.revision === "string" ? page.revision : checkpoint.revisionId;
			if (revision === undefined) revision = pageRevision;
			else if (pageRevision !== undefined && pageRevision !== revision)
				throw new TranscriptIncompleteError(
					`transcript.list revision changed mid-read (${revision} -> ${pageRevision})`,
					pages + 1,
				);
			const pageGeneration = recordOf(page.highWatermark)?.generation;
			if (typeof pageGeneration === "number") generation = pageGeneration;
			for (const item of page.items) {
				const record = recordOf(item);
				if (!record) continue;
				rows.push(await this.#materialize(record, revision));
			}
			if (page.complete === true) {
				if (revision === undefined)
					throw new TranscriptIncompleteError("transcript.list reported no snapshot identity", pages + 1);
				return { revision, generation, rows, complete: true, source: "channel" };
			}
			const next = page.continuationCursor;
			if (typeof next !== "string" || next.length === 0)
				throw new TranscriptIncompleteError(
					"transcript.list returned an incomplete page without a continuation cursor",
					pages + 1,
				);
			cursor = next;
			input = {};
		}
		throw new TranscriptIncompleteError(`transcript.list exceeded ${this.#maxPages} pages`, this.#maxPages);
	}

	async #materialize(item: Record<string, unknown>, revision: string | undefined): Promise<TranscriptRow> {
		const id = String(item.id ?? item.itemId ?? "");
		const error = recordOf(item.error);
		if (error?.code === "item_too_large") {
			const continuations = Array.isArray(item.continuations) ? item.continuations.map(recordOf) : [];
			const fields = new Map<string, Record<string, unknown>>();
			for (const continuation of continuations) {
				if (continuation && typeof continuation.field === "string") fields.set(continuation.field, continuation);
			}
			const fetch = async (field: string): Promise<string | undefined> => {
				const continuation = fields.get(field);
				if (!continuation) return undefined;
				return await this.#fetchField(continuation, revision);
			};
			const body = (await fetch("body")) ?? (await fetch("textSummary")) ?? "";
			return { id: (await fetch("id")) ?? id, role: (await fetch("role")) ?? "", ts: await fetch("ts"), body };
		}
		return {
			id,
			role: typeof item.role === "string" ? item.role : "",
			ts: typeof item.ts === "string" ? item.ts : undefined,
			body: typeof item.body === "string" ? item.body : typeof item.textSummary === "string" ? item.textSummary : "",
		};
	}

	async #fetchField(continuation: Record<string, unknown>, revision: string | undefined): Promise<string> {
		const { query, ...input } = continuation;
		if (typeof query !== "string")
			throw new ChannelQueryError("oversized item continuation has no query", "q23_invalid");
		const chunks: Buffer[] = [];
		let expectedOffset = 0;
		let cursor: string | undefined;
		for (let pages = 0; pages < this.#maxPages; pages++) {
			const response = await this.query(query, input, cursor);
			const page = recordOf(response.page);
			const chunk = Array.isArray(page?.items) ? recordOf(page.items[0]) : undefined;
			if (!page || !chunk)
				throw new TranscriptIncompleteError(`${query} ${String(input.field)} returned no chunk`, pages + 1);
			if (revision !== undefined && typeof page.revision === "string" && page.revision !== revision)
				throw new TranscriptIncompleteError(`${query} revision changed mid-read`, pages + 1);
			// The runtime's byteOffset is authoritative and chunks may end anywhere in
			// the byte stream (the live recording split a multibyte body at 262143),
			// so reassemble bytes, not decoded strings, and require contiguity.
			const offset = typeof chunk.byteOffset === "number" ? chunk.byteOffset : expectedOffset;
			if (offset !== expectedOffset)
				throw new TranscriptIncompleteError(
					`${query} ${String(input.field)} chunk offset ${offset} != expected ${expectedOffset}`,
					pages + 1,
				);
			const bytes = Buffer.from(typeof chunk.body === "string" ? chunk.body : "", "utf8");
			chunks.push(bytes);
			expectedOffset += bytes.length;
			if (chunk.complete === true) return Buffer.concat(chunks).toString("utf8");
			const next = page.continuationCursor;
			if (typeof next !== "string" || next.length === 0)
				throw new TranscriptIncompleteError(`${query} ${String(input.field)} incomplete without a cursor`, pages + 1);
			cursor = next;
		}
		throw new TranscriptIncompleteError(`${query} exceeded ${this.#maxPages} chunks`, this.#maxPages);
	}

	#receive(line: string): void {
		const frame = parseFrame(line);
		if (!frame) return;
		if (frame.type !== "query_response" && frame.type !== "control_response") return;
		const pending = this.#pending.get(String(frame.id));
		if (!pending) {
			this.#orphanFrames += 1;
			this.#log(`channel_orphan_frame session=${this.sessionId} type=${String(frame.type)}`);
			return;
		}
		this.#pending.delete(String(frame.id));
		this.#clearTimeout(pending.timer);
		pending.resolve(frame);
	}
}

function parseFrame(line: string): Record<string, unknown> | undefined {
	const trimmed = line.trim();
	if (!trimmed.startsWith("{")) return undefined;
	try {
		return recordOf(JSON.parse(trimmed));
	} catch {
		return undefined;
	}
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function message(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
