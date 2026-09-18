import type { Frame } from "@gajae-gateway/protocol";
import { encodeFrame } from "@gajae-gateway/protocol";

export interface FrameWriterSink {
	write(bytes: Uint8Array): number;
	close(): void;
}

export interface OrderedFrameWriterOptions {
	readonly maxQueuedFrames?: number;
	readonly maxQueuedBytes?: number;
}

const DEFAULT_MAX_QUEUED_FRAMES = 256;
const DEFAULT_MAX_QUEUED_BYTES = 4 * 1024 * 1024;

/**
 * Serializes complete protocol frames onto a byte-oriented stream.
 *
 * A socket write may accept only a prefix of a frame. The remainder is kept
 * until the transport reports that it is writable again; the next frame is
 * never attempted until the current one is fully accepted.
 */
export class OrderedFrameWriter {
	#closed = false;
	#tail: Promise<void> = Promise.resolve();
	#resume: (() => void) | undefined;
	#queuedFrames = 0;
	#queuedBytes = 0;
	readonly #maxQueuedFrames: number;
	readonly #maxQueuedBytes: number;

	constructor(
		private readonly sink: FrameWriterSink,
		private readonly onFailure: (error: unknown) => void = () => {},
		options: OrderedFrameWriterOptions = {},
	) {
		this.#maxQueuedFrames = options.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES;
		this.#maxQueuedBytes = options.maxQueuedBytes ?? DEFAULT_MAX_QUEUED_BYTES;
	}

	write(frame: Frame): void {
		if (this.#closed) return;
		const bytes = Buffer.from(encodeFrame(frame), "utf8");
		if (
			!Number.isInteger(this.#maxQueuedFrames) ||
			this.#maxQueuedFrames < 1 ||
			!Number.isInteger(this.#maxQueuedBytes) ||
			this.#maxQueuedBytes < 1
		)
			throw new Error("invalid frame writer queue limits");
		if (this.#queuedFrames >= this.#maxQueuedFrames || this.#queuedBytes + bytes.byteLength > this.#maxQueuedBytes) {
			this.fail(new Error("socket frame writer queue limit exceeded"));
			return;
		}
		this.#queuedFrames++;
		this.#queuedBytes += bytes.byteLength;
		const operation = this.#tail.then(async () => {
			try {
				await this.#flush(bytes);
			} finally {
				this.#queuedFrames--;
				this.#queuedBytes -= bytes.byteLength;
			}
		});
		this.#tail = operation.catch((error: unknown) => {
			this.fail(error);
		});
	}

	drain(): void {
		const resume = this.#resume;
		this.#resume = undefined;
		resume?.();
	}

	fail(error: unknown): void {
		if (this.#closed) return;
		this.#closed = true;
		const resume = this.#resume;
		this.#resume = undefined;
		resume?.();
		try {
			this.onFailure(error);
		} catch {
			// A diagnostic callback must never turn a handled write failure into an
			// unhandled rejection.
		}
		try {
			this.sink.close();
		} catch {
			// The transport is already unusable; there is nothing else to do.
		}
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		const resume = this.#resume;
		this.#resume = undefined;
		resume?.();
		try {
			this.sink.close();
		} catch {
			// Closing an already closed transport is harmless.
		}
	}

	get closed(): boolean {
		return this.#closed;
	}

	async settled(): Promise<void> {
		await this.#tail;
	}

	async #flush(bytes: Uint8Array): Promise<void> {
		let offset = 0;
		while (offset < bytes.byteLength && !this.#closed) {
			const written = this.sink.write(bytes.subarray(offset));
			if (!Number.isInteger(written) || written < 0 || written > bytes.byteLength - offset)
				throw new Error(`invalid socket write count: ${written}`);
			if (written > 0) offset += written;
			if (offset < bytes.byteLength) await this.#waitForDrain();
		}
	}

	async #waitForDrain(): Promise<void> {
		if (this.#closed) return;
		await new Promise<void>((resolve) => {
			this.#resume = resolve;
		});
	}
}
