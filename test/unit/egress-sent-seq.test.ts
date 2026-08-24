import { expect, test } from "bun:test";
import { AdapterEgress, SENT_SEQ_LIMIT } from "../../src/adapter/runtime/egress";
import type { AdapterPlatform, SendResult } from "../../src/adapter/runtime/protocol";
import type { JsonRpcClient, JsonRpcResponse, RpcRequestOptions } from "../../src/rpc-client";

interface FakeEvent {
	readonly seq: string;
	readonly text: string;
}

/**
 * Minimal gateway stub. `commitsToDrop` lets a test simulate a commit that never
 * lands (the crash window) so the in-process guard is exercised rather than
 * cleared.
 */
class FakeGateway implements JsonRpcClient {
	claims = 0;
	commits: string[][] = [];
	#pending: FakeEvent[];
	readonly #failCommit: boolean;

	constructor(events: FakeEvent[], options: { failCommit?: boolean } = {}) {
		this.#pending = [...events];
		this.#failCommit = options.failCommit ?? false;
	}

	close(): void {}

	async request(method: string, params?: unknown, _options?: RpcRequestOptions): Promise<JsonRpcResponse> {
		if (method === "consumer.claim") {
			this.claims += 1;
			return ok({ claim_id: `claim-${this.claims}`, cursor: "1:0", expires_at: Date.now() + 60_000 });
		}
		if (method === "main.events.read") {
			const events = this.#pending.map((event) => ({
				seq: event.seq,
				kind: "assistant_message",
				payload: { finalized: true, text: event.text },
			}));
			return ok({ events, next_cursor: `1:${this.#pending.length}` });
		}
		if (method === "consumer.commit") {
			const proofs = (params as { proofs?: Array<{ seq: string }> }).proofs ?? [];
			if (this.#failCommit) throw new Error("commit did not land");
			this.commits.push(proofs.map((proof) => proof.seq));
			// Settled: the gateway would not redeliver these.
			this.#pending = [];
			return ok({});
		}
		throw new Error(`unexpected method ${method}`);
	}
}

function ok(result: unknown): JsonRpcResponse {
	return { jsonrpc: "2.0", id: 1, result } as JsonRpcResponse;
}

function platform(dedupe: "platform_nonce" | "at_least_once", sends: string[]): AdapterPlatform {
	return {
		dedupe,
		async start() {},
		async stop() {},
		async send(_chatId, text): Promise<SendResult> {
			sends.push(text);
			return dedupe === "platform_nonce" ? { platformMsgId: `msg-${sends.length}` } : { platformMsgId: undefined };
		},
		async ack() {},
		async typing() {},
		onDisconnect() {},
	};
}

function egress(gateway: FakeGateway, plat: AdapterPlatform): AdapterEgress {
	return new AdapterEgress({
		rpc: gateway,
		platform: plat,
		consumerId: "test-adapter",
		surfaceId: "surface-a",
		chatId: "chat-1",
		idleDelayMs: 0,
		retryDelayMs: 0,
	});
}

/**
 * D3: within one process lifetime a re-claim of the same events must not
 * re-send. This is the case that actually happens - a reconnect or a claim-lease
 * renewal - as opposed to a process crash.
 */
test("a re-claim of an uncommitted seq does not re-send it", async () => {
	const sends: string[] = [];
	const gateway = new FakeGateway([{ seq: "7", text: "hello" }], { failCommit: true });
	const stream = egress(gateway, platform("at_least_once", sends));

	// First pass sends, then the commit fails so the guard stays populated.
	await expect(stream.runOnce()).rejects.toThrow();
	expect(sends).toEqual(["hello"]);
	expect(stream.pendingSentSeqCount).toBe(1);

	// The gateway redelivers the same event; it must not be sent twice.
	await expect(stream.runOnce()).rejects.toThrow();
	expect(sends).toEqual(["hello"]);
});

test("a committed seq is dropped from the in-process guard", async () => {
	const sends: string[] = [];
	const gateway = new FakeGateway([{ seq: "9", text: "commit me" }]);
	const stream = egress(gateway, platform("platform_nonce", sends));

	expect(await stream.runOnce()).toBe("sent");
	expect(sends).toEqual(["commit me"]);
	expect(gateway.commits).toEqual([["9"]]);
	// Durably settled, so the guard is no longer needed.
	expect(stream.pendingSentSeqCount).toBe(0);
});

/**
 * The residual is documented rather than hidden: a fresh process starts with an
 * empty guard, so an `at_least_once` platform can duplicate across a crash or
 * with two overlapping adapter processes.
 */
test("a second egress instance re-sends an uncommitted seq, documenting the residual", async () => {
	const sends: string[] = [];
	const plat = platform("at_least_once", sends);
	const gateway = new FakeGateway([{ seq: "11", text: "duplicate risk" }], { failCommit: true });

	const first = egress(gateway, plat);
	await expect(first.runOnce()).rejects.toThrow();
	expect(sends).toEqual(["duplicate risk"]);

	// A replacement process has no memory of the in-flight send.
	const second = egress(gateway, plat);
	await expect(second.runOnce()).rejects.toThrow();
	expect(sends).toEqual(["duplicate risk", "duplicate risk"]);
});

test("the guard is bounded and evicts oldest-first", async () => {
	const sends: string[] = [];
	const events = Array.from({ length: SENT_SEQ_LIMIT + 5 }, (_, index) => ({
		seq: String(index + 1),
		text: `event-${index + 1}`,
	}));
	const gateway = new FakeGateway(events, { failCommit: true });
	const stream = egress(gateway, platform("at_least_once", sends));

	await expect(stream.runOnce()).rejects.toThrow();

	// Bounded: memory cannot grow without limit while commits stall.
	expect(stream.pendingSentSeqCount).toBe(SENT_SEQ_LIMIT);
	expect(sends).toHaveLength(SENT_SEQ_LIMIT + 5);
});

/**
 * An adapter must settle an event it cannot render, or its checkpoint stalls
 * forever. Alerts are the concrete case: a non-owner adapter commits a proof
 * carrying a dedupe key and no platform message id.
 */
test("an unrenderable event is settled without a platform send", async () => {
	const sends: string[] = [];
	const gateway = new FakeGateway([]);
	const plat = platform("platform_nonce", sends);
	const stream = new AdapterEgress({
		rpc: {
			close(): void {},
			async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
				if (method === "consumer.claim") return ok({ claim_id: "c1", cursor: "1:0", expires_at: Date.now() + 60_000 });
				if (method === "main.events.read") {
					return ok({
						events: [{ seq: "21", kind: "alert_raised", payload: { condition: "failed_closed" } }],
						next_cursor: "1:21",
					});
				}
				if (method === "consumer.commit") {
					const proofs = (params as { proofs: Array<{ seq: string; platform_msg_id?: string; dedupe_key: string }> })
						.proofs;
					expect(proofs).toHaveLength(1);
					expect(proofs[0]?.platform_msg_id).toBeUndefined();
					expect(proofs[0]?.dedupe_key).toContain("21");
					return ok({});
				}
				throw new Error(`unexpected ${method}`);
			},
		} as JsonRpcClient,
		platform: plat,
		consumerId: "test-adapter",
		surfaceId: "surface-a",
		chatId: "chat-1",
		// Alerts must be opted into explicitly; the gateway rejects unknown kinds.
		eventKinds: ["assistant_message", "alert_raised"],
		idleDelayMs: 0,
		retryDelayMs: 0,
	});

	expect(await stream.runOnce()).toBe("sent");
	// Settled, never sent.
	expect(sends).toEqual([]);
	expect(gateway.commits).toEqual([]);
});
