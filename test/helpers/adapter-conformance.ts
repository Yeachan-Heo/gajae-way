import { expect, test } from "bun:test";
import { AdapterEgress } from "../../src/adapter/runtime/egress";
import { AdapterIngress } from "../../src/adapter/runtime/ingress";
import type { AdapterPlatform, InboundMessage } from "../../src/adapter/runtime/protocol";
import { AdapterProtocolUnsupportedError } from "../../src/adapter/runtime/protocol";
import {
	AdapterSessionError,
	assertGatewayReady,
	assertSurfaceUsable,
	SurfaceWatch,
} from "../../src/adapter/runtime/session";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";

export interface ConformancePlatform {
	readonly platform: AdapterPlatform;
	/** Every text handed to `send`, in order. */
	readonly sends: string[];
	/** Nonces observed by `send`, for `platform_nonce` drivers. */
	readonly nonces: Array<string | undefined>;
	/** Chats that received a typing call. */
	readonly typings: string[];
}

export interface ConformanceAdapterFactory {
	readonly name: string;
	readonly consumerId: string;
	readonly chatId: string;
	readonly surfaceId: string;
	create(): ConformancePlatform;
}

interface GatewayScript {
	health?: unknown;
	surface?: unknown;
	events?: Array<{ seq: string; kind: string; payload?: unknown }>;
	failCommit?: boolean;
	submitAccepted?: boolean;
}

class ScriptedGateway implements JsonRpcClient {
	claims = 0;
	commits: Array<Array<{ seq: string; platform_msg_id?: string; dedupe_key: string }>> = [];
	submits = 0;
	readonly #script: GatewayScript;
	#pending: Array<{ seq: string; kind: string; payload?: unknown }>;

	constructor(script: GatewayScript = {}) {
		this.#script = script;
		this.#pending = [...(script.events ?? [])];
	}

	close(): void {}

	async request(method: string, params?: unknown): Promise<JsonRpcResponse> {
		switch (method) {
			case "way.health":
				return ok(this.#script.health ?? { status: "healthy", state: "running" });
			case "surface.resolve":
				if (this.#script.surface === "error") throw new Error("surface.resolve unavailable");
				return ok(this.#script.surface ?? { surfaceId: "surface-a", quarantined: false });
			case "main.submit":
				this.submits += 1;
				return ok(
					this.#script.submitAccepted === false
						? { accepted: false }
						: { accepted: true, op_ref: "op-1", delivered_as: "prompt", journal_head_cursor: "1:0" },
				);
			case "consumer.claim":
				this.claims += 1;
				return ok({ claim_id: `claim-${this.claims}`, cursor: "1:0", expires_at: Date.now() + 60_000 });
			case "main.events.read":
				return ok({ events: this.#pending, next_cursor: `1:${this.#pending.length}` });
			case "consumer.commit": {
				const proofs =
					(params as { proofs?: Array<{ seq: string; platform_msg_id?: string; dedupe_key: string }> }).proofs ?? [];
				if (this.#script.failCommit) throw new Error("commit did not land");
				this.commits.push(proofs);
				this.#pending = [];
				return ok({});
			}
			default:
				throw new Error(`unexpected method ${method}`);
		}
	}
}

function ok(result: unknown): JsonRpcResponse {
	return { jsonrpc: "2.0", id: 1, result } as JsonRpcResponse;
}

function message(factory: ConformanceAdapterFactory, id: string, text: string): InboundMessage {
	return { platformMsgId: id, chatId: factory.chatId, text, senderId: "operator" };
}

/**
 * The out-of-process adapter protocol contract.
 *
 * Every adapter runs this identical suite. One conforming implementation plus a
 * grandfathered exception is exactly the parallel-convention outcome the
 * extraction exists to prevent, so a driver that cannot pass this is not a
 * supported adapter.
 */
export function describeAdapterConformance(factory: ConformanceAdapterFactory): void {
	const label = `[${factory.name}]`;

	test(`${label} refuses to start when the gateway is not healthy and running`, async () => {
		const gateway = new ScriptedGateway({
			health: { status: "unhealthy", state: "failed_closed", reason: "profile_drift" },
		});
		await expect(assertGatewayReady(gateway)).rejects.toBeInstanceOf(AdapterSessionError);
		// Nothing was claimed, so no delivery could be settled by a fenced adapter.
		expect(gateway.claims).toBe(0);
	});

	test(`${label} refuses to start on a protocol version mismatch, before any claim`, async () => {
		const gateway = new ScriptedGateway({ health: { status: "healthy", state: "running", adapter_protocol: 99 } });
		await expect(assertGatewayReady(gateway)).rejects.toBeInstanceOf(AdapterProtocolUnsupportedError);
		expect(gateway.claims).toBe(0);
	});

	test(`${label} refuses to start against a quarantined surface and issues zero claims`, async () => {
		const gateway = new ScriptedGateway({ surface: { surfaceId: factory.surfaceId, quarantined: true } });
		await expect(assertSurfaceUsable(gateway, factory.surfaceId)).rejects.toBeInstanceOf(AdapterSessionError);
		expect(gateway.claims).toBe(0);
	});

	test(`${label} treats an unresolvable surface as a hard refusal rather than a blind send`, async () => {
		const gateway = new ScriptedGateway({ surface: "error" });
		await expect(assertSurfaceUsable(gateway, factory.surfaceId)).rejects.toBeInstanceOf(AdapterSessionError);
	});

	test(`${label} acknowledges only after a durably accepted submit`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway({ submitAccepted: false });
		const order: string[] = [];
		const ingress = new AdapterIngress({
			rpc: gateway,
			platform: created.platform,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			onAccepted: () => order.push("accepted"),
			onAcknowledged: () => order.push("acknowledged"),
		});

		// A refused submit must leave the message unacknowledged.
		await expect(ingress.handle(message(factory, "m1", "refused"))).rejects.toThrow();
		expect(order).toEqual([]);
		expect(created.typings).toEqual([]);

		const accepting = new ScriptedGateway();
		const ok2: string[] = [];
		const ingress2 = new AdapterIngress({
			rpc: accepting,
			platform: created.platform,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			onAccepted: () => ok2.push("accepted"),
			onAcknowledged: () => ok2.push("acknowledged"),
		});
		expect(await ingress2.handle(message(factory, "m2", "accepted"))).toBe(true);
		// Ordering invariant: acceptance strictly precedes acknowledgement.
		expect(ok2).toEqual(["accepted", "acknowledged"]);
	});

	test(`${label} ignores traffic from another chat and its own bot echo`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway();
		const ingress = new AdapterIngress({
			rpc: gateway,
			platform: created.platform,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
		});

		expect(await ingress.handle({ ...message(factory, "m3", "elsewhere"), chatId: "other-chat" })).toBe(false);
		expect(await ingress.handle({ ...message(factory, "m4", "echo"), authorBot: true })).toBe(false);
		expect(await ingress.handle(message(factory, "m5", "   "))).toBe(false);
		expect(gateway.submits).toBe(0);
	});

	test(`${label} never commits before the platform send returns`, async () => {
		const created = factory.create();
		const order: string[] = [];
		const gateway = new ScriptedGateway({
			events: [{ seq: "5", kind: "assistant_message", payload: { finalized: true, text: "reply" } }],
		});
		const egress = new AdapterEgress({
			rpc: {
				close: () => {},
				request: async (method, params) => {
					if (method === "consumer.commit") order.push("commit");
					return await gateway.request(method, params);
				},
			} as JsonRpcClient,
			platform: {
				...created.platform,
				send: async (chatId, text, options) => {
					order.push("send");
					return await created.platform.send(chatId, text, options);
				},
			},
			consumerId: factory.consumerId,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			idleDelayMs: 0,
			retryDelayMs: 0,
		});

		expect(await egress.runOnce()).toBe("sent");
		expect(order).toEqual(["send", "commit"]);
		expect(created.sends).toEqual(["reply"]);
	});

	test(`${label} settles an alert it must not render with a dedupe key and no platform id`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway({
			events: [{ seq: "8", kind: "alert_raised", payload: { condition: "lock_quarantined" } }],
		});
		const egress = new AdapterEgress({
			rpc: gateway,
			platform: created.platform,
			consumerId: factory.consumerId,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			// Opt in explicitly: an adapter may only request kinds the gateway emits.
			eventKinds: ["assistant_message", "alert_raised"],
			idleDelayMs: 0,
			retryDelayMs: 0,
		});

		expect(await egress.runOnce()).toBe("sent");
		// Settled but not rendered: otherwise this consumer's checkpoint stalls.
		expect(created.sends).toEqual([]);
		expect(gateway.commits).toHaveLength(1);
		expect(gateway.commits[0]?.[0]?.platform_msg_id).toBeUndefined();
		expect(gateway.commits[0]?.[0]?.dedupe_key).toContain("8");
	});

	test(`${label} delivers an assistant_message that carries no surface_id`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway({
			// A scheduler-originated turn has no surface attribution.
			events: [{ seq: "12", kind: "assistant_message", payload: { finalized: true, text: "scheduled reply" } }],
		});
		const egress = new AdapterEgress({
			rpc: gateway,
			platform: created.platform,
			consumerId: factory.consumerId,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			idleDelayMs: 0,
			retryDelayMs: 0,
		});

		expect(await egress.runOnce()).toBe("sent");
		expect(created.sends).toEqual(["scheduled reply"]);
	});

	test(`${label} does not re-send an already-sent seq after a reconnect mid-claim`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway({
			events: [{ seq: "17", kind: "assistant_message", payload: { finalized: true, text: "once only" } }],
			failCommit: true,
		});
		const egress = new AdapterEgress({
			rpc: gateway,
			platform: created.platform,
			consumerId: factory.consumerId,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			idleDelayMs: 0,
			retryDelayMs: 0,
		});

		await expect(egress.runOnce()).rejects.toThrow();
		expect(created.sends).toEqual(["once only"]);

		// The gateway redelivers after the lost commit; within one process
		// lifetime this must not produce a second send.
		await expect(egress.runOnce()).rejects.toThrow();
		expect(created.sends).toEqual(["once only"]);
	});

	test(`${label} stops settling and re-resolves when its surface changes`, async () => {
		const watch = new SurfaceWatch(factory.surfaceId);
		expect(watch.stale).toBe(false);

		watch.observe({ kind: "assistant_message", payload: {} });
		expect(watch.stale).toBe(false);

		watch.observe({ kind: "registry_change", payload: { surface_id: "someone-else" } });
		expect(watch.stale).toBe(false);

		watch.observe({ kind: "registry_change", payload: { surface_id: factory.surfaceId } });
		expect(watch.stale).toBe(true);

		const gateway = new ScriptedGateway();
		await watch.reresolve(gateway);
		expect(watch.stale).toBe(false);
	});

	test(`${label} declares a dedupe mode the runtime honours`, async () => {
		const created = factory.create();
		const gateway = new ScriptedGateway({
			events: [{ seq: "21", kind: "assistant_message", payload: { finalized: true, text: "nonce check" } }],
		});
		const egress = new AdapterEgress({
			rpc: gateway,
			platform: created.platform,
			consumerId: factory.consumerId,
			surfaceId: factory.surfaceId,
			chatId: factory.chatId,
			idleDelayMs: 0,
			retryDelayMs: 0,
		});

		await egress.runOnce();

		if (created.platform.dedupe === "platform_nonce") {
			// A deterministic nonce is what makes retries idempotent server-side.
			expect(created.nonces[0]).toMatch(/^[0-9a-f]{24}$/u);
			expect(gateway.commits[0]?.[0]?.platform_msg_id).toBeDefined();
		} else {
			expect(created.nonces[0]).toBeUndefined();
		}
	});
}
