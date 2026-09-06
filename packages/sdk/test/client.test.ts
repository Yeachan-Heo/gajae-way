import { expect, test } from "bun:test";
import { encodeFrame, PROFILE_VERSION, type ProtocolError } from "@gajaeway/protocol";
import { GajaewayClient } from "../src/client";

function transportWithoutResponses(): {
	readable: ReadableStream<Uint8Array>;
	writable: { write(data: Uint8Array | string): void };
} {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	const readable = new ReadableStream<Uint8Array>({
		start(value) {
			controller = value;
		},
	});
	return {
		readable,
		writable: {
			write() {
				controller.enqueue(
					new TextEncoder().encode(
						encodeFrame({
							v: PROFILE_VERSION,
							type: "negotiated",
							payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
						}),
					),
				);
			},
		},
	};
}

test("a request-specific timeout is bounded and reports unknown server completion", async () => {
	const client = await GajaewayClient.connectStdio(transportWithoutResponses(), { requestTimeoutMs: 1_000 });
	try {
		await expect(client.request("memory.autolink", undefined, { timeoutMs: 10 })).rejects.toMatchObject({
			name: "ProtocolError",
			code: "verb_failed",
			message: "memory.autolink timed out after 10ms; server completion is unknown",
		} satisfies Partial<ProtocolError>);
	} finally {
		await client.close();
	}
});

test("invalid request-specific timeouts fail before a frame is sent", async () => {
	const client = await GajaewayClient.connectStdio(transportWithoutResponses());
	try {
		await expect(client.request("memory.autolink", undefined, { timeoutMs: 0 })).rejects.toThrow(
			"request timeout must be a positive number",
		);
	} finally {
		await client.close();
	}
});
