import { expect, test } from "bun:test";
import { encodeFrame, PROFILE_VERSION } from "@gajaeway/protocol";
import { GajaewayClient } from "../src/client";

for (const count of [2, 1001]) {
	test(`RT-SLACK-32 first SDK subscriber receives newest held events in order (${count})`, async () => {
		let controller!: ReadableStreamDefaultController<Uint8Array>;
		const readable = new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		});
		let helloCount = 0;
		const client = await GajaewayClient.connectStdio({
			readable,
			writable: {
				write(data) {
					expect(JSON.parse(String(data)).type).toBe("hello");
					helloCount++;
					const negotiated = encodeFrame({
						v: PROFILE_VERSION,
						type: "negotiated",
						payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
					});
					const events = Array.from({ length: count }, (_, i) =>
						encodeFrame({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload: { text: String(i) } }),
					).join("");
					const internal = encodeFrame({
						v: PROFILE_VERSION,
						type: "event",
						event: "__internal",
						payload: "must not replay",
					});
					controller.enqueue(new TextEncoder().encode(negotiated + events + internal + negotiated));
				},
			},
		});
		try {
			const first: string[] = [];
			const second: string[] = [];
			const internals: unknown[] = [];
			client.onChatMessage((message) => first.push(message.text));
			client.onChatMessage((message) => second.push(message.text));
			client.on("__internal", (payload) => internals.push(payload));
			client.on("__negotiated", (payload) => internals.push(payload));
			expect(helloCount).toBe(1);
			expect(first).toEqual(
				Array.from({ length: Math.min(count, 1000) }, (_, i) => String(i + Math.max(0, count - 1000))),
			);
			expect(second).toEqual([]);
			expect(internals).toEqual([]);
		} finally {
			controller.close();
			await client.close();
		}
	});
}
