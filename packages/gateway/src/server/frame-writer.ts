import type { Frame } from "@gajaeway/protocol";
import { encodeFrame } from "@gajaeway/protocol";

export interface FrameWriterSink {
	write(bytes: Uint8Array): number;
	close(): void;
}

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

	constructor(
		private readonly sink: FrameWriterSink,
		private readonly onFailure: (error: unknown) => void = () => {},
	) {}

	write(frame: Frame): void {
		if (this.#closed) return;
		const bytes = Buffer.from(encodeFrame(frame), "utf8");
		const operation = this.#tail.then(() => this.#flush(bytes));
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
		this.#resume = undefined;
		this.onFailure(error);
		this.sink.close();
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#resume = undefined;
		this.sink.close();
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
			if (!Number.isFinite(written) || written < 0 || written > bytes.byteLength - offset)
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
