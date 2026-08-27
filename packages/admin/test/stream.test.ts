import { describe, expect, test } from "bun:test";
import { RETRY_DEGRADED_MS, RETRY_MS, StreamHub } from "../src/stream";

async function drain(response: Response, reads: number): Promise<string> {
	const reader = response.body?.getReader();
	if (!reader) throw new Error("no body");
	const decoder = new TextDecoder();
	let text = "";
	for (let i = 0; i < reads; i += 1) {
		const chunk = await reader.read();
		if (chunk.done) break;
		text += decoder.decode(chunk.value);
	}
	await reader.cancel();
	return text;
}

function hub(reachable = true, snapshot: unknown = { at: "2026-08-27T14:02:31.000Z" }): StreamHub {
	return new StreamHub({ snapshot: async () => snapshot, gatewayReachable: () => reachable });
}

describe("StreamHub", () => {
	test("a client is greeted with a retry interval and a full snapshot", async () => {
		const instance = hub();
		try {
			const text = await drain(instance.connect(), 2);
			expect(text).toContain(`retry: ${RETRY_MS}`);
			expect(text).toContain("event: snapshot");
			expect(text).toContain('"at":"2026-08-27T14:02:31.000Z"');
		} finally {
			instance.close();
		}
	});

	test("an unreachable gateway backs the reconnect interval off instead of hammering it", async () => {
		const instance = hub(false);
		try {
			expect(await drain(instance.connect(), 1)).toContain(`retry: ${RETRY_DEGRADED_MS}`);
		} finally {
			instance.close();
		}
	});

	test("event ids are monotonic, so a client can tell it missed something", async () => {
		const instance = hub();
		try {
			const text = await drain(instance.connect(), 2);
			expect(text).toContain("id: 1\n");
			instance.broadcast("monitor.event", { eventId: "e1" });
		} finally {
			instance.close();
		}
	});

	test("broadcasting with nobody connected is a no-op, not an error", () => {
		const instance = hub();
		expect(instance.clientCount).toBe(0);
		expect(() => instance.broadcast("monitor.event", {})).not.toThrow();
		instance.close();
	});

	test("reconciling with nobody connected does not read the snapshot", async () => {
		let reads = 0;
		const instance = new StreamHub({
			snapshot: async () => {
				reads += 1;
				return {};
			},
			gatewayReachable: () => true,
		});
		await instance.reconcile();
		expect(reads).toBe(0);
		instance.close();
	});

	test("a broadcast reaches every connected client", async () => {
		const instance = hub();
		const first = instance.connect();
		const second = instance.connect();
		// Both readers must consume the greeting before the broadcast is observable.
		const firstReader = first.body?.getReader();
		const secondReader = second.body?.getReader();
		await firstReader?.read();
		await firstReader?.read();
		await secondReader?.read();
		await secondReader?.read();
		expect(instance.clientCount).toBe(2);

		instance.broadcast("monitor.event", { eventId: "e1" });
		const decoder = new TextDecoder();
		expect(decoder.decode((await firstReader?.read())?.value)).toContain("event: monitor.event");
		expect(decoder.decode((await secondReader?.read())?.value)).toContain('"eventId":"e1"');
		await firstReader?.cancel();
		await secondReader?.cancel();
		instance.close();
	});

	test("the response advertises an unbuffered event stream", () => {
		const instance = hub();
		const response = instance.connect();
		expect(response.headers.get("content-type")).toBe("text/event-stream; charset=utf-8");
		expect(response.headers.get("cache-control")).toBe("no-store");
		expect(response.headers.get("x-accel-buffering")).toBe("no");
		instance.close();
	});

	test("data frames are single-line json, so no payload can break the framing", async () => {
		const instance = hub(true, { note: "line one\nline two" });
		try {
			const text = await drain(instance.connect(), 2);
			const dataLine = text.split("\n").find((line) => line.startsWith("data: "));
			expect(dataLine).toBe('data: {"note":"line one\\nline two"}');
		} finally {
			instance.close();
		}
	});
});
