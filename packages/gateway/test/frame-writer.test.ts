import { expect, test } from "bun:test";
import { type FrameWriterSink, OrderedFrameWriter } from "../src/server/frame-writer";

const response = (id: string, text: string) => ({ v: "0.1", type: "response", id, result: { text } }) as const;

class FakeSink implements FrameWriterSink {
	readonly chunks: Uint8Array[] = [];
	readonly writes: number[] = [];
	closed = false;
	paused = true;
	constructor(private readonly window = 8) {}
	write(bytes: Uint8Array): number {
		if (this.closed) throw new Error("write after close");
		if (this.paused) {
			this.paused = false;
			this.writes.push(0);
			return 0;
		}
		const count = Math.min(this.window, bytes.byteLength);
		this.chunks.push(bytes.slice(0, count));
		this.writes.push(count);
		this.paused = count === this.window;
		return count;
	}
	close(): void {
		this.closed = true;
	}
	resume(): void {
		this.paused = false;
	}
	text(): string {
		return Buffer.concat(this.chunks.map((chunk) => Buffer.from(chunk))).toString("utf8");
	}
}

test("flushes short writes and queues complete UTF-8 frames in order", async () => {
	const sink = new FakeSink(32);
	const writer = new OrderedFrameWriter(sink);
	writer.write(response("large", "한글".repeat(80)));
	writer.write(response("second", "after"));
	for (let attempt = 0; attempt < 1000 && !writer.closed; attempt++) {
		await Bun.sleep(1);
		sink.resume();
		writer.drain();
		if (sink.text().includes('"id":"second"')) break;
	}
	await writer.settled();
	const lines = sink.text().trimEnd().split("\n");
	expect(lines).toHaveLength(2);
	expect(JSON.parse(lines[0]).id).toBe("large");
	expect(JSON.parse(lines[1]).id).toBe("second");
	expect(sink.writes.some((count) => count === 0)).toBe(true);
});

test("failure closes the writer and does not create an unhandled rejection", async () => {
	const sink: FrameWriterSink = {
		write: () => {
			throw new Error("broken pipe");
		},
		close: () => {},
	};
	const failures: unknown[] = [];
	const writer = new OrderedFrameWriter(sink, (error) => failures.push(error));
	writer.write(response("one", "x"));
	writer.write(response("two", "y"));
	await writer.settled();
	expect(writer.closed).toBe(true);
	expect(failures).toHaveLength(1);
});

test("fails closed at the configured queue bound and wakes a blocked write", async () => {
	const sink = new FakeSink(4);
	const failures: unknown[] = [];
	const writer = new OrderedFrameWriter(sink, (error) => failures.push(error), {
		maxQueuedFrames: 2,
		maxQueuedBytes: 100_000,
	});
	writer.write(response("first", "x".repeat(100)));
	await Bun.sleep(0);
	writer.write(response("second", "x"));
	writer.write(response("third", "x"));
	await writer.settled();
	expect(writer.closed).toBe(true);
	expect(failures).toHaveLength(1);
	expect(sink.text()).not.toContain('"id":"second"');
});

test("settles a blocked frame when the transport closes", async () => {
	const sink = new FakeSink(4);
	const writer = new OrderedFrameWriter(sink);
	writer.write(response("blocked", "x".repeat(100)));
	await Bun.sleep(0);
	writer.close();
	await writer.settled();
	expect(writer.closed).toBe(true);
	expect(sink.closed).toBe(true);
});

test("fails closed when queued bytes exceed the limit independently of frame count", async () => {
	const sink = new FakeSink(4);
	const failures: unknown[] = [];
	const writer = new OrderedFrameWriter(sink, (error) => failures.push(error), {
		maxQueuedFrames: 10,
		maxQueuedBytes: 500,
	});
	writer.write(response("first", "x".repeat(100)));
	await Bun.sleep(0);
	writer.write(response("second", "x".repeat(400)));
	await writer.settled();
	expect(writer.closed).toBe(true);
	expect(failures).toHaveLength(1);
	expect(sink.text()).not.toContain('"id":"second"');
});

test("rejects fractional socket write counts instead of corrupting the offset", async () => {
	const failures: unknown[] = [];
	const writer = new OrderedFrameWriter({ write: () => 1.5, close: () => {} }, (error) => failures.push(error));
	writer.write(response("fractional", "x"));
	await writer.settled();
	expect(writer.closed).toBe(true);
	expect(failures[0]).toBeInstanceOf(Error);
});
