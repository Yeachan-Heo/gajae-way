import { describe, expect, test } from "bun:test";
import type { Frame, FrameWriterSink } from "@gajae-gateway/protocol";
import { encodeFrame, FrameDecoder, OrderedFrameWriter } from "@gajae-gateway/protocol";

describe("SDK socket partial writes (issue #420 acceptance)", () => {
	test("64KB chat.send arrives intact with stub sink accepting 64 bytes per write", async () => {
		const received: Buffer[] = [];
		let writeCount = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Simulate small send buffer: only accept 64 bytes at a time
				const accepted = Math.min(bytes.length, 64);
				received.push(Buffer.from(bytes.subarray(0, accepted)));
				// If not all accepted, signal that drain will be needed
				if (accepted < bytes.length && sinkState.writer) {
					// Schedule drain asynchronously
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		// Create a 64KB+ text payload (Korean characters are 3 bytes in UTF-8)
		const largeText = "안녕하세요 ".repeat(6400); // ~80KB in UTF-8
		const largeFrame: Frame = {
			v: "0.1",
			type: "request",
			id: "large-1",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: largeText },
		};

		writer.write(largeFrame);
		await writer.settled();

		// Verify all bytes were written through multiple write() calls
		const combined = Buffer.concat(received);
		expect(writeCount).toBeGreaterThan(1); // Should require multiple writes

		// Verify the frame decodes correctly from combined bytes
		const decoder = new FrameDecoder();
		const frames = decoder.feed(combined.toString("utf-8"));
		expect(frames.length).toBe(1);
		expect(frames[0].type).toBe("request");
		if (frames[0].type === "request") {
			expect(frames[0].id).toBe("large-1");
			// biome-ignore lint/suspicious/noExplicitAny: test convenience
			const params = frames[0].params as any;
			expect(params.text).toBe(largeText);
		}
	});

	test("burst of 50 4KB frames all arrive intact with stub sink accepting 512 bytes per write", async () => {
		const received: Buffer[] = [];
		let writeCount = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Simulate small send buffer: only accept 512 bytes at a time
				const accepted = Math.min(bytes.length, 512);
				received.push(Buffer.from(bytes.subarray(0, accepted)));
				if (accepted < bytes.length && sinkState.writer) {
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		// Send 50 frames, each with ~4KB of data
		const frameData = "x".repeat(4000);
		for (let i = 0; i < 50; i++) {
			const frame: Frame = {
				v: "0.1",
				type: "request",
				id: String(i),
				verb: "chat.send",
				params: { origin: { platform: "slack", id: "C123" }, text: frameData },
			};
			writer.write(frame);
		}
		await writer.settled();

		// Verify all bytes were written through multiple write() calls
		const combined = Buffer.concat(received);
		expect(writeCount).toBeGreaterThan(50); // Should require many writes due to small buffer

		// Verify all 50 frames decode correctly in order
		const decoder = new FrameDecoder();
		const frames = decoder.feed(combined.toString("utf-8"));
		expect(frames.length).toBe(50);

		for (let i = 0; i < 50; i++) {
			const frame = frames[i];
			expect(frame.type).toBe("request");
			if (frame.type === "request") {
				expect(frame.id).toBe(String(i));
				expect(frame.verb).toBe("chat.send");
			}
		}
	});

	test("partial writes are retried until complete (no data loss)", async () => {
		let writeCount = 0;
		let totalWritten = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Only accept 128 bytes per write
				const accepted = Math.min(bytes.length, 128);
				totalWritten += accepted;
				if (accepted < bytes.length && sinkState.writer) {
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		const frame: Frame = {
			v: "0.1",
			type: "request",
			id: "partial-test",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: "test".repeat(100) },
		};

		writer.write(frame);
		await writer.settled();

		// Verify that write was called multiple times (partial writes)
		expect(writeCount).toBeGreaterThan(1);
		// Verify total bytes written equals frame size
		const frameBytes = Buffer.byteLength(encodeFrame(frame), "utf-8");
		expect(totalWritten).toBe(frameBytes);
	});
});
