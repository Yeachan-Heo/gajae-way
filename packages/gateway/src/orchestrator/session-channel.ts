/**
 * Query multiplexer over the ONE resident `gjc sdk serve --stdio --session <id>`
 * relay a session already owns (I4a, path (ii) from the pinned-runtime
 * recordings). The relay is a byte relay for the host WebSocket: frames written to
 * its stdin as `query_request` are answered on stdout as `query_response` carrying
 * the same `id`, interleaved with the live lifecycle / `turn_stream` frames the
 * tail consumes. This class owns the writer and the `id` correlation; the tail
 * keeps every frame that is not a response to one of its requests.
 *
 * Query and control correlation share one bounded writer; nothing is spawned here.
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
	readonly bytesWritten: number | undefined;
	constructor(message: string, code: string, bytesWritten?: number) {
		super(message);
		this.name = "ChannelQueryError";
		this.code = code;
		this.bytesWritten = bytesWritten;
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
	write(line: string): void | number | Promise<void | number>;
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
	readonly restarts?: number;
}

type Pending = {
	resolve(frame: Record<string, unknown>): void;
	reject(error: Error): void;
	timer: unknown;
	responseType: string;
	bytesWritten: number;
};

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
	readonly #now: () => number;
	readonly #restarts: number;
	readonly #faultListeners = new Set<(reason: string) => void>();
	#writer: Promise<void> = Promise.resolve();
	#faults = 0;
	#timeouts = 0;
	#lastFrameAt: number;
	#lastProbeAt = 0;
	#lastActivityAt: number;
	#probeTimer: unknown;
	#probing = false;
	#lastWrite: { id: string; bytesWritten: number } | undefined;

	constructor(options: SessionChannelOptions) {
		this.sessionId = options.sessionId;
		this.#transport = options.transport;
		this.#timeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		this.#maxPages = options.maxPages ?? DEFAULT_MAX_PAGES;
		this.#setTimeout = options.setTimeout ?? ((work, delayMs) => setTimeout(work, delayMs));
		this.#clearTimeout = options.clearTimeout ?? ((timer) => clearTimeout(timer as ReturnType<typeof setTimeout>));
		this.#log = options.log ?? (() => {});
		this.#newId = options.newId ?? (() => crypto.randomUUID());
		this.#now = options.now ?? Date.now;
		this.#restarts = options.restarts ?? 0;
		this.#lastFrameAt = this.#lastActivityAt = this.#now();
		this.#detach = this.#transport.onLine((line) => this.#receive(line));
	}

	/** Frames that were responses to nobody; the tail still sees them. */
	get orphanFrames(): number {
		return this.#orphanFrames;
	}

	get lastWrite(): { id: string; bytesWritten: number } | undefined {
		return this.#lastWrite;
	}

	health(): { healthy: boolean; lastFrameAt: number; lastProbeAt: number } {
		return {
			healthy: !this.#closed && this.#faults === 0,
			lastFrameAt: this.#lastFrameAt,
			lastProbeAt: this.#lastProbeAt,
		};
	}

	stats(): { restarts: number; faults: number; orphanFrames: number; inFlight: number } {
		return {
			restarts: this.#restarts,
			faults: this.#faults,
			orphanFrames: this.#orphanFrames,
			inFlight: this.#pending.size,
		};
	}

	onFault(listener: (reason: string) => void): () => void {
		this.#faultListeners.add(listener);
		return () => this.#faultListeners.delete(listener);
	}

	fault(reason: string): void {
		if (this.#closed || this.#faults > 0) return;
		this.#faults++;
		this.#log(`channel_degraded session=${this.sessionId} reason=${reason}`);
		this.close();
		for (const listener of this.#faultListeners) listener(reason);
	}

	startIdleProbe(): void {
		if (this.#probing || this.#closed) return;
		this.#probing = true;
		this.#scheduleProbe();
	}

	stopIdleProbe(): void {
		this.#probing = false;
		if (this.#probeTimer !== undefined) this.#clearTimeout(this.#probeTimer);
		this.#probeTimer = undefined;
	}

	#scheduleProbe(): void {
		if (!this.#probing || this.#closed) return;
		if (this.#probeTimer !== undefined) this.#clearTimeout(this.#probeTimer);
		this.#probeTimer = this.#setTimeout(
			() => {
				this.#probeTimer = undefined;
				this.#lastProbeAt = this.#now();
				this.#lastActivityAt = this.#now();
				this.#scheduleProbe();
				void this.checkpoint().catch(() => {});
			},
			Math.max(1, 10_000 - (this.#now() - this.#lastActivityAt)),
		);
	}

	#activity(): void {
		this.#lastActivityAt = this.#now();
		if (this.#probing) this.#scheduleProbe();
	}

	/**
	 * Returns true when the line was a response to one of this channel's requests
	 * and has been consumed. The tail must skip such lines.
	 */
	consumes(line: string): boolean {
		const frame = parseFrame(line);
		if (!frame) return false;
		return (
			(frame.type === "query_response" || frame.type === "control_response") &&
			this.#pending.get(String(frame.id))?.responseType === frame.type
		);
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.stopIdleProbe();
		this.#detach();
		for (const [id, pending] of this.#pending) {
			this.#clearTimeout(pending.timer);
			pending.reject(
				new ChannelQueryError(`channel closed before ${id} was answered`, "channel_closed", pending.bytesWritten),
			);
		}
		this.#pending.clear();
	}

	async query(
		query: string,
		input: Record<string, unknown> = {},
		cursor?: string,
		opts?: { timeoutMs?: number },
	): Promise<Record<string, unknown>> {
		return this.#request(
			{ type: "query_request", query, input, ...(cursor === undefined ? {} : { cursor }) },
			query,
			opts?.timeoutMs ?? this.#timeoutMs,
		);
	}

	async control(
		op: string,
		input: Record<string, unknown>,
		opts?: { timeoutMs?: number },
	): Promise<Record<string, unknown>> {
		return this.#request(
			// The pinned 0.16.3 host keys control requests by `operation` (recorded C1/C5);
			// the fixture accepts either. Emit both so one codec fits both peers.
			{ type: "control_request", op, operation: op, input },
			op,
			opts?.timeoutMs ?? (op === "turn.prompt" ? 30_000 : this.#timeoutMs),
		);
	}

	async #request(
		frame: Record<string, unknown>,
		operation: string,
		timeoutMs: number,
	): Promise<Record<string, unknown>> {
		if (this.#closed) throw new ChannelQueryError("channel is closed", "channel_closed", 0);
		if (this.#pending.size >= 8) throw new ChannelQueryError("channel has eight in-flight requests", "channel_busy", 0);
		const id = this.#newId();
		const line = `${JSON.stringify({ ...frame, id })}\n`;
		const size = Buffer.byteLength(line);
		if (size > 256 * 1024) throw new ChannelQueryError("channel frame exceeds 256 KiB", "channel_frame_too_large", 0);
		const response = await new Promise<Record<string, unknown>>((resolve, reject) => {
			const timer = this.#setTimeout(() => {
				const pending = this.#pending.get(id);
				if (!pending) return;
				this.#pending.delete(id);
				reject(
					new ChannelQueryError(`${operation} timed out after ${timeoutMs}ms`, "channel_timeout", pending.bytesWritten),
				);
				this.#log(
					`channel_timeout session=${this.sessionId} op=${operation} timeoutMs=${timeoutMs} inFlight=${this.#pending.size} bytesWritten=${pending.bytesWritten ?? "unknown"} lastFrameAgeMs=${this.#now() - this.#lastFrameAt} streak=${this.#timeouts + 1}`,
				);
				if (++this.#timeouts >= 2) this.fault("timeouts");
			}, timeoutMs);
			const pending: Pending = {
				resolve,
				reject,
				timer,
				responseType: frame.type === "control_request" ? "control_response" : "query_response",
				bytesWritten: 0,
			};
			this.#pending.set(id, pending);
			this.#writer = this.#writer.then(async () => {
				if (!this.#pending.has(id) || this.#closed) return;
				// A void writer cannot prove zero bytes after an exception. Conservatively
				// report the attempted size unless the transport supplies an exact count.
				pending.bytesWritten = size;
				this.#lastWrite = { id, bytesWritten: size };
				this.#activity();
				try {
					const written = await this.#transport.write(line);
					if (typeof written === "number") pending.bytesWritten = written;
					this.#lastWrite = { id, bytesWritten: pending.bytesWritten };
				} catch (error) {
					const written = recordOf(error)?.bytesWritten;
					if (typeof written === "number") pending.bytesWritten = written;
					this.#lastWrite = { id, bytesWritten: pending.bytesWritten };
					this.#pending.delete(id);
					this.#clearTimeout(timer);
					reject(
						new ChannelQueryError(
							`${operation} could not be written: ${message(error)}`,
							"channel_write_failed",
							pending.bytesWritten,
						),
					);
				}
			});
		});
		if (response.ok !== true) {
			const error = recordOf(response.error);
			const code = typeof error?.code === "string" ? error.code : "query_failed";
			throw new ChannelQueryError(`${operation} failed: ${code}`, code);
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
		if (this.#closed) return;
		const frame = parseFrame(line);
		if (frame?.type === "transport_error" || (!frame && /\btransport_error\b/.test(line))) {
			this.fault("transport_error");
			return;
		}
		if (!frame) return;
		this.#lastFrameAt = this.#now();
		this.#activity();
		if (frame.type !== "query_response" && frame.type !== "control_response") return;
		const pending = this.#pending.get(String(frame.id));
		if (!pending || pending.responseType !== frame.type) {
			this.#orphanFrames += 1;
			this.#log(`channel_orphan_frame session=${this.sessionId} type=${String(frame.type)}`);
			return;
		}
		this.#pending.delete(String(frame.id));
		this.#clearTimeout(pending.timer);
		this.#timeouts = 0;
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
