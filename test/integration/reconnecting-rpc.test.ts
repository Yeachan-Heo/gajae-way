import { expect, test } from "bun:test";
import { ReconnectingRpcClient } from "../../src/adapter/reconnecting-rpc";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";

/**
 * The adapter used to become permanently useless after a gateway restart: the
 * plain client owns no reconnect policy, so every later request threw
 * "RPC socket closed", the outbox logged that forever, and the chat surface went
 * silent until an operator restarted the process by hand.
 */
function deadThenLive(): { connects: number; connect: (path: string) => Promise<JsonRpcClient> } {
	const state = { connects: 0, connect: async (_path: string) => ({}) as JsonRpcClient };
	state.connect = async (_path: string): Promise<JsonRpcClient> => {
		state.connects += 1;
		const generation = state.connects;
		return {
			async request(method: string): Promise<JsonRpcResponse> {
				// The first connection dies on use; later ones work.
				if (generation === 1) throw new Error("RPC socket closed");
				return { jsonrpc: "2.0", id: 1, result: { method, generation } };
			},
			close() {},
		};
	};
	return state;
}

test("a lost gateway connection is re-dialled on the next request", async () => {
	const harness = deadThenLive();
	const diagnostics: string[] = [];
	const client = await ReconnectingRpcClient.connect("/tmp/does-not-matter.sock", {
		connect: harness.connect,
		onDiagnostic: message => diagnostics.push(message),
	});
	try {
		// The in-flight request is surfaced, never silently retried: its outcome is
		// unknown and the caller owns idempotency.
		await expect(client.request("main.events.read")).rejects.toThrow(/RPC socket closed/);
		expect(diagnostics.some(message => message.includes("connection lost"))).toBe(true);

		// The next request transparently reconnects and succeeds.
		const response = await client.request("consumer.commit");
		expect(response.result).toEqual({ method: "consumer.commit", generation: 2 });
		expect(diagnostics.some(message => message.includes("re-established"))).toBe(true);
		expect(harness.connects).toBe(2);
	} finally {
		client.close();
	}
});

test("a closed reconnecting client stops reconnecting", async () => {
	const harness = deadThenLive();
	const client = await ReconnectingRpcClient.connect("/tmp/does-not-matter.sock", { connect: harness.connect });
	client.close();
	await expect(client.request("way.health")).rejects.toThrow(/closed/);
});
