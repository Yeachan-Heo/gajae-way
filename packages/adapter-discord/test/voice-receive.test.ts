import { expect, test } from "bun:test";
import type { SttStream } from "@gajaeway/voice-core";
import {
	createSpeakerReceiver,
	downsamplePcm48kStereoTo16kMono,
	type VoiceAudioSubscriptionLike,
	type VoiceReceiveDecoderLike,
	type VoiceReceiveIngressEvent,
	type VoiceReceiverLike,
} from "../src/voice/receive";

class FakeAudioStream implements VoiceAudioSubscriptionLike {
	#queue: Uint8Array[] = [];
	#waiters: Array<(result: IteratorResult<Uint8Array>) => void> = [];
	#ended = false;

	push(packet: Uint8Array): void {
		if (this.#ended) return;
		const waiter = this.#waiters.shift();
		if (waiter !== undefined) waiter({ done: false, value: packet });
		else this.#queue.push(packet);
	}

	end(): void {
		if (this.#ended) return;
		this.#ended = true;
		for (const waiter of this.#waiters.splice(0)) waiter({ done: true, value: undefined });
	}

	destroy(): void {
		this.end();
	}

	async return(): Promise<IteratorResult<Uint8Array>> {
		this.end();
		return { done: true, value: undefined };
	}

	[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
		return this;
	}

	next(): Promise<IteratorResult<Uint8Array>> {
		const value = this.#queue.shift();
		if (value !== undefined) return Promise.resolve({ done: false, value });
		if (this.#ended) return Promise.resolve({ done: true, value: undefined });
		return new Promise((resolve) => this.#waiters.push(resolve));
	}
}

class FakeReceiver implements VoiceReceiverLike {
	readonly stream = new FakeAudioStream();

	subscribe(_userId: string): FakeAudioStream {
		return this.stream;
	}
}

class FakeDecoder implements VoiceReceiveDecoderLike {
	constructor(readonly pcm: Uint8Array) {}

	decode(_opus: Uint8Array): Uint8Array {
		return this.pcm;
	}
}

class FakeSttStream implements SttStream {
	readonly frames: Uint8Array[] = [];
	closed = false;
	commits: string[] = [];
	droppedFrames = 0;

	constructor(readonly maxQueuedFrames: number) {}

	get queuedFrames(): number {
		return this.frames.length;
	}

	push(frame: Uint8Array): "accepted" | "dropped_oldest" | "rejected_closed" {
		if (this.closed) return "rejected_closed";
		if (this.frames.length >= this.maxQueuedFrames) {
			this.frames.shift();
			this.droppedFrames += 1;
			this.frames.push(frame);
			return "dropped_oldest";
		}
		this.frames.push(frame);
		return "accepted";
	}

	commit(reason: "teardown" | "speaker_left" | "idle"): void {
		this.commits.push(reason);
	}

	async close(): Promise<void> {
		this.closed = true;
	}
}

const energyGate = { rmsThreshold: 0.001, minDurationMs: 300 };

function pcmPacket(samples: readonly number[]): Uint8Array {
	const bytes = new Uint8Array(samples.length * 2);
	const view = new DataView(bytes.buffer);
	for (let index = 0; index < samples.length; index += 1) view.setInt16(index * 2, samples[index] ?? 0, true);
	return bytes;
}

function stereoFrames(frames: readonly [number, number][]): Uint8Array {
	return pcmPacket(frames.flat());
}

async function settle(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
}

function createReceive(
	stream: SttStream,
	receiver = new FakeReceiver(),
	events: VoiceReceiveIngressEvent[] = [],
): { receiver: FakeReceiver; handle: ReturnType<typeof createSpeakerReceiver>; events: VoiceReceiveIngressEvent[] } {
	const decoder = new FakeDecoder(
		stereoFrames([
			[30, 30],
			[30, 30],
			[30, 30],
		]),
	);
	return {
		receiver,
		events,
		handle: createSpeakerReceiver({
			receiver,
			speakerId: "speaker-1",
			decoder,
			stream,
			clock: { now: () => 0 },
			energyGate,
			silenceEndMs: 700,
			maxQueuedFrames: 200,
			onAdmission: (event) => events.push(event),
		}),
	};
}

test("downsamples 48 kHz stereo PCM16 to 16 kHz mono with averaged sample values", () => {
	const input = stereoFrames([
		[1, 3],
		[5, 7],
		[9, 11],
		[13, 15],
		[17, 19],
		[21, 23],
	]);
	const output = downsamplePcm48kStereoTo16kMono(input);
	const view = new DataView(output.buffer, output.byteOffset, output.byteLength);
	expect(output.byteLength).toBe(4);
	expect([view.getInt16(0, true), view.getInt16(2, true)]).toEqual([6, 18]);
});

test("a 20 ms 48 kHz stereo input produces 320 mono 16 kHz samples", () => {
	const input = stereoFrames(Array.from({ length: 960 }, (_, index) => [index, index] as [number, number]));
	const output = downsamplePcm48kStereoTo16kMono(input);
	expect(output.byteLength).toBe(320 * 2);
});

test("records accepted admissions under the configured bound", async () => {
	const stream = new FakeSttStream(2);
	const setup = createReceive(stream);
	setup.receiver.stream.push(new Uint8Array([1]));
	await settle();
	expect(stream.frames).toHaveLength(1);
	expect(setup.handle.counters).toEqual({ accepted: 1, dropped: 0, rejectedClosed: 0 });
	expect(setup.events.map((event) => event.admission)).toEqual(["accepted"]);
	await setup.handle.close();
});

test("drops the oldest frame at the bound and records the admission", async () => {
	const stream = new FakeSttStream(2);
	const receiver = new FakeReceiver();
	const decoder: VoiceReceiveDecoderLike = {
		decode(opus) {
			return stereoFrames([
				[opus[0] ?? 0, opus[0] ?? 0],
				[opus[0] ?? 0, opus[0] ?? 0],
				[opus[0] ?? 0, opus[0] ?? 0],
			]);
		},
	};
	const events: VoiceReceiveIngressEvent[] = [];
	const handle = createSpeakerReceiver({
		receiver,
		speakerId: "speaker-1",
		decoder,
		stream,
		clock: { now: () => 0 },
		energyGate,
		silenceEndMs: 700,
		onAdmission: (event) => events.push(event),
	});
	receiver.stream.push(new Uint8Array([1]));
	receiver.stream.push(new Uint8Array([2]));
	receiver.stream.push(new Uint8Array([3]));
	await settle();
	expect(stream.frames).toHaveLength(2);
	expect(
		stream.frames.map((frame) => new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getInt16(0, true)),
	).toEqual([2, 3]);
	expect(stream.droppedFrames).toBe(1);
	expect(handle.counters).toEqual({ accepted: 2, dropped: 1, rejectedClosed: 0 });
	expect(events.map((event) => event.admission)).toEqual(["accepted", "accepted", "dropped_oldest"]);
	await handle.close();
});

test("records rejected_closed after the STT stream closes", async () => {
	const stream = new FakeSttStream(2);
	const setup = createReceive(stream);
	stream.closed = true;
	setup.receiver.stream.push(new Uint8Array([1]));
	await settle();
	expect(setup.handle.counters).toEqual({ accepted: 0, dropped: 1, rejectedClosed: 1 });
	expect(setup.events).toMatchObject([{ admission: "rejected_closed", ingress: "dropped" }]);
	await setup.handle.close();
});
