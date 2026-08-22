import { AsyncLocalStorage } from "node:async_hooks";
import { expect, test } from "bun:test";
import { DiscordOutbox, discordDedupeKey, discordWireNonce } from "../../src/adapter/discord/outbox";
import { startDiscordAdapter } from "../../src/adapter/discord/main";
import { DiscordRouteHandler } from "../../src/adapter/discord/route";
import type { JsonRpcClient, JsonRpcResponse } from "../../src/rpc-client";
import { DiscordFixture, DiscordFixtureClock } from "../fixtures/discord-fixture";
import { createExternalGateway, eventually, type ExternalGateway } from "../helpers/external-gateway";

const gatewayScope = new AsyncLocalStorage<ExternalGateway[]>();

type ExternalTestBody = () => void | Promise<void>;

let externalTestTail: Promise<void> = Promise.resolve();

function externalTest(name: string, body: ExternalTestBody, timeoutMs?: number): void {
	test(name, async () => {
		let release: (() => void) | undefined;
		const previous = externalTestTail;
		externalTestTail = new Promise<void>(resolve => {
			release = resolve;
		});
		await previous;
		const gateways: ExternalGateway[] = [];
		try {
			await gatewayScope.run(gateways, body);
		} finally {
			for (const active of gateways.splice(0)) await active.stop();
			release?.();
		}
	}, Math.max(timeoutMs ?? 0, 60_000));
}

async function gateway(): Promise<ExternalGateway> {
	const active = await createExternalGateway({
		ownerSurface: { id: "discord:owner-dm", platform: "discord", kind: "dm" },
		knownSurfaces: [{ id: "discord:guest-channel", platform: "discord", kind: "channel" }],
	});
	const gateways = gatewayScope.getStore();
	if (!gateways) throw new Error("gateway() must run inside externalTest().");
	gateways.push(active);
	return active;
}

function outbox(
	gatewayUnderTest: ExternalGateway,
	fixture: DiscordFixture,
	hooks: ConstructorParameters<typeof DiscordOutbox>[0]["hooks"] = undefined,
): DiscordOutbox {
	return new DiscordOutbox({
		rpc: gatewayUnderTest.client,
		platform: fixture,
		route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		claimTtlMs: 5_000,
		readWaitMs: 0,
		hooks,
	});
}

const TYPING_CHANNEL_ID = "123456789012345678";
const TYPING_ROUTE = { channelId: TYPING_CHANNEL_ID, surfaceId: "discord:owner-dm" };

function typingAdapterRpc(
	accepted: boolean,
	acceptedResponse: Record<string, unknown> = accepted ? { accepted: true, journal_head_cursor: "1:0" } : { accepted: false },
): JsonRpcClient {

	let claimCount = 0;
	return {
		async request(method): Promise<JsonRpcResponse> {
			switch (method) {
				case "way.health":
					return typingRpcResult({ status: "healthy", state: "running" });
				case "main.submit":
					return typingRpcResult(acceptedResponse);
				case "consumer.claim":
					claimCount += 1;
					return typingRpcResult({ claim_id: `typing-claim-${claimCount}`, cursor: "1:0", expires_at: Date.now() + 5_000 });
				case "main.events.read":
					return typingRpcResult({ events: [], next_cursor: "1:0" });
				case "consumer.commit":
					return typingRpcResult({});
				default:
					throw new Error(`Unexpected typing fixture RPC method: ${method}`);
			}
		},
		close() {},
	};
}

function typingRpcResult(result: unknown): JsonRpcResponse {
	return { jsonrpc: "2.0", id: 1, result };
}

async function startTypingFixtureAdapter(
	fixture: DiscordFixture,
	clock: DiscordFixtureClock,
	rpc: JsonRpcClient,
	onError: (error: Error) => void = () => undefined,
) {
	return await startDiscordAdapter(
		{
			rpcSocketPath: "/tmp/gajaeway-discord-typing-fixture.sock",
			token: "fixture-token",
			route: TYPING_ROUTE,
			ackBudgetMs: 2_000,
			claimTtlMs: 5_000,
			readWaitMs: 0,
		},
		{
			rpcConnect: async () => rpc,
			platformFactory: () => fixture,
			typingKeepaliveClock: clock,
			onError,
		},
	);
}

async function advanceTypingClock(clock: DiscordFixtureClock, milliseconds: number): Promise<void> {
	clock.advanceTimersBy(milliseconds);
	await clock.flushAsync();
}

async function advanceTypingIntervals(clock: DiscordFixtureClock, count: number): Promise<void> {
	for (let index = 0; index < count; index += 1) await advanceTypingClock(clock, 8_000);
}


test("Discord wire nonce is deterministic, distinct, and exactly 96 bits", () => {
	const firstKey = discordDedupeKey("discord:owner-dm", "1");
	const secondKey = discordDedupeKey("discord:owner-dm", "2");
	const firstNonce = discordWireNonce(firstKey);
	expect(firstNonce).toMatch(/^[a-f0-9]{24}$/);
	expect(discordWireNonce(firstKey)).toBe(firstNonce);
	expect(discordWireNonce(secondKey)).not.toBe(firstNonce);
});

externalTest("Discord inbound deduplicates a message id before external broker admission and sends finalized output", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		});
		const unsubscribe = fixture.onMessage(async message => {
			await route.handle(message);
		});
		const inbound = { id: "message-id-once", channelId: "123456789012345678", text: "external broker round trip" };
		await Promise.all([fixture.emitMessage(inbound), fixture.emitMessage(inbound)]);
		unsubscribe();

		expect(fixture.acknowledgements).toHaveLength(1);
		expect(gatewayUnderTest.fixture.commands()).toEqual([
			expect.objectContaining({ operation: "turn.prompt", text: "external broker round trip" }),
		]);
		await eventually(
			() => (gatewayUnderTest.core.journalRead("1:0", 20).events.some(event => event.kind === "assistant_message") ? true : undefined),
			"external assistant output was not journaled",
		);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toEqual([expect.objectContaining({ channelId: "123456789012345678", text: "ack" })]);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord typing starts after delayed durable main.submit acceptance", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const ordering: string[] = [];
	let durableAcceptedAt: number | undefined;
	const delayedRpc: JsonRpcClient = {
		async request(method, params, options) {
			const request = gatewayUnderTest.client.request(method, params, options);
			if (method === "main.submit") clock.advance(2_001);
			const response = await request;
			if (method === "main.submit") {
				if (response.error || (response.result as { accepted?: unknown } | undefined)?.accepted !== true) {
					throw new Error("delayed main.submit did not return durable acceptance");
				}
				durableAcceptedAt = clock.now();
				ordering.push("durably accepted");
			}
			return response;
		},
		close() {},
	};
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: delayedRpc,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
			acknowledgement: { now: clock.now, budgetMs: 2_000 },
			onAcknowledged: () => ordering.push("typing acknowledged"),
		});
		const inbound = {
			id: "delayed-admission",
			channelId: "123456789012345678",
			text: "acknowledge after delayed admission",
			acceptedAt: clock.now(),
		};

		expect(await route.handle(inbound)).toBe(true);
		const acceptedAt = durableAcceptedAt;
		if (acceptedAt === undefined) throw new Error("delayed main.submit acceptance was not observed");
		expect(acceptedAt - inbound.acceptedAt).toBeGreaterThan(2_000);
		expect(fixture.acknowledgements).toEqual([{ channelId: inbound.channelId, at: acceptedAt }]);
		expect(ordering).toEqual(["durably accepted", "typing acknowledged"]);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord typing keepalive refreshes a slow accepted turn and stops after reply delivery", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, gatewayUnderTest.client);
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-slow-turn", channelId: TYPING_CHANNEL_ID, text: "keep typing until the reply arrives" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"slow Discord turn was not admitted",
		);

		await advanceTypingIntervals(clock, 3);

		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 17_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 25_000 },
		]);

		gatewayUnderTest.fixture.complete(opRef, { text: "typing keepalive delivered reply" });
		await eventually(
			() => (fixture.sends.some(send => send.text === "typing keepalive delivered reply") ? true : undefined),
			"Discord outbox did not deliver the slow turn reply",
		);
		const acknowledgementsAtDelivery = fixture.acknowledgements.length;
		await advanceTypingClock(clock, 30_000);

		expect(fixture.acknowledgements).toHaveLength(acknowledgementsAtDelivery);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive ignores an older delayed delivery but stops at a later reply", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, gatewayUnderTest.client);

	let releasedOlderSend = false;
	try {
		const olderText = "older assistant delivery must not clear newer typing";
		fixture.deferNextSend();
		const older = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: olderText }));
		await eventually(() => (fixture.pendingSendCount === 1 ? true : undefined), "old Discord outbox send was not paused");

		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-causal-new", channelId: TYPING_CHANNEL_ID, text: "new accepted command survives old delivery" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"new Discord turn was not admitted",
		);

		fixture.releaseNextSend();
		releasedOlderSend = true;
		await eventually(
			() => (gatewayUnderTest.core.consumerCursor("gajaeway-discord") === older.cursor ? true : undefined),
			"old Discord outbox delivery did not settle",
		);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);

		const replyText = "newer assistant delivery stops typing";
		gatewayUnderTest.fixture.complete(opRef, { text: replyText });
		await eventually(() => (fixture.sends.some(send => send.text === replyText) ? true : undefined), "new assistant reply was not delivered");
		await eventually(() => (clock.scheduledTimerCount === 0 ? true : undefined), "newer delivery did not stop the typing keepalive");
		const acknowledgementsAfterReply = fixture.acknowledgementAttempts.length;
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(acknowledgementsAfterReply);
	} finally {
		if (!releasedOlderSend && fixture.pendingSendCount > 0) {
			fixture.releaseNextSend();
			await clock.flushAsync();
		}
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive stops for a reply journaled before the accepted response reaches the adapter", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const replyText = "fast reply predates client receipt of acceptance";
	let completed = false;
	let serverBoundary: unknown;
	const adapterRpc: JsonRpcClient = {
		async request(method, params, options) {
			const response = await gatewayUnderTest.client.request(method, params, options);
			if (method !== "main.submit" || completed) return response;
			const result = response.result as { accepted?: unknown; op_ref?: unknown; journal_head_cursor?: unknown } | undefined;
			if (result?.accepted !== true || typeof result.op_ref !== "string") return response;
			completed = true;
			serverBoundary = result.journal_head_cursor;
			gatewayUnderTest.fixture.complete(result.op_ref, { text: replyText });
			await eventually(
				() =>
					gatewayUnderTest.core
						.journalRead("1:0", 100)
						.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes(replyText))
						? true
						: undefined,
				"fast assistant reply was not journaled before main.submit returned to the adapter",
			);
			return response;
		},
		close() {
			gatewayUnderTest.client.close();
		},
	};
	const adapter = await startTypingFixtureAdapter(fixture, clock, adapterRpc);
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		fixture.deferNextSend();
		await fixture.emitMessage({ id: "typing-fast-reply", channelId: TYPING_CHANNEL_ID, text: "respond before adapter observes acceptance" });
		expect(serverBoundary).toMatch(/^\d+:\d+$/);
		await eventually(() => (fixture.pendingSendCount === 1 ? true : undefined), "fast journaled reply was not held before Discord delivery");
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
		expect(clock.scheduledTimerCount).toBe(2);

		fixture.releaseNextSend();
		await eventually(() => (fixture.sends.some(send => send.text === replyText) ? true : undefined), "fast reply was not delivered to Discord");
		await eventually(() => (clock.scheduledTimerCount === 0 ? true : undefined), "fast reply did not stop the typing keepalive");
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
	} finally {
		if (fixture.pendingSendCount > 0) {
			fixture.releaseNextSend();
			await clock.flushAsync();
		}
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive shares one channel interval and extends its cap for a later admission", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	try {
		await fixture.emitMessage({ id: "typing-first", channelId: TYPING_CHANNEL_ID, text: "first accepted admission" });
		clock.advance(4_000);
		await fixture.emitMessage({ id: "typing-second", channelId: TYPING_CHANNEL_ID, text: "second accepted admission" });

		await advanceTypingIntervals(clock, 75);

		const refreshes = fixture.acknowledgements.slice(2);
		expect(fixture.acknowledgements.slice(0, 2)).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 5_000 },
		]);
		expect(refreshes).toHaveLength(74);
		for (const [index, acknowledgement] of refreshes.entries()) {
			expect(acknowledgement).toEqual({ channelId: TYPING_CHANNEL_ID, at: 13_000 + index * 8_000 });
		}

		expect(clock.scheduledTimerCount).toBe(0);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive never starts for a rejected admission", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(false));
	try {
		await fixture.emitMessage({ id: "typing-rejected", channelId: TYPING_CHANNEL_ID, text: "must not type" });
		await advanceTypingClock(clock, 30_000);

		expect(fixture.acknowledgements).toEqual([]);
		expect(fixture.acknowledgementAttempts).toEqual([]);
		expect(clock.scheduledTimerCount).toBe(0);
		expect(fixture.connected).toBe(true);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive diagnoses missing and malformed admission boundaries while retaining typing", async () => {
	for (const scenario of [
		{ name: "missing", response: { accepted: true }, detail: "omitted journal_head_cursor" },
		{ name: "malformed", response: { accepted: true, journal_head_cursor: "not-a-cursor" }, detail: "invalid journal_head_cursor" },
	] as const) {
		const clock = new DiscordFixtureClock(1_000);
		const fixture = new DiscordFixture({ now: clock.now });
		const errors: Error[] = [];
		const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true, scenario.response), error => errors.push(error));
		try {
			await fixture.emitMessage({
				id: `typing-boundary-${scenario.name}`,
				channelId: TYPING_CHANNEL_ID,
				text: `${scenario.name} accepted boundary retains typing`,
			});
			expect(errors).toEqual([expect.objectContaining({ message: expect.stringContaining(scenario.detail) })]);
			expect(clock.scheduledTimerCount).toBe(2);
			await advanceTypingClock(clock, 8_000);
			expect(fixture.acknowledgementAttempts).toEqual([
				{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
				{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
			]);
			expect(clock.scheduledTimerCount).toBe(2);
		} finally {
			await adapter.stop();
		}
	}
});

externalTest("Discord typing keepalive diagnoses a stale-generation admission boundary while retaining typing", async () => {
	const gatewayUnderTest = await gateway();
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const staleBoundaryRpc: JsonRpcClient = {
		async request(method, params, options) {
			const response = await gatewayUnderTest.client.request(method, params, options);
			if (method !== "main.submit" || !response.result || typeof response.result !== "object") return response;
			return {
				...response,
				result: { ...(response.result as Record<string, unknown>), journal_head_cursor: "0:0" },
			};
		},
		close() {
			gatewayUnderTest.client.close();
		},
	};
	const adapter = await startTypingFixtureAdapter(fixture, clock, staleBoundaryRpc, error => errors.push(error));
	try {
		gatewayUnderTest.fixture.holdNextTurn();
		const inbound = { id: "typing-stale-generation", channelId: TYPING_CHANNEL_ID, text: "stale generation must retain typing" };
		await fixture.emitMessage(inbound);
		const opRef = await eventually(
			() => {
				const command = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt" && command.text === inbound.text);
				return typeof command?.opRef === "string" ? command.opRef : undefined;
			},
			"stale-boundary Discord turn was not admitted",
		);
		gatewayUnderTest.fixture.complete(opRef, { text: "stale generation reply" });
		await eventually(() => (fixture.sends.some(send => send.text === "stale generation reply") ? true : undefined), "stale-boundary reply was not delivered");
		await eventually(
			() => (errors.some(error => error.message.includes("does not match accepted generation")) ? true : undefined),
			"stale-generation boundary diagnostic was not reported",
		);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive cancels its timer when the adapter stops", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	await fixture.emitMessage({ id: "typing-teardown", channelId: TYPING_CHANNEL_ID, text: "stop before refresh" });
	expect(clock.scheduledTimerCount).toBe(2);

	await adapter.stop();
	expect(clock.scheduledTimerCount).toBe(0);
	await advanceTypingClock(clock, 30_000);

	expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
});

externalTest("Discord typing keepalive retries after a non-fatal typing failure", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true), error => errors.push(error));
	try {
		await fixture.emitMessage({ id: "typing-retry", channelId: TYPING_CHANNEL_ID, text: "retry typing after failure" });
		fixture.failNextTyping();
		await advanceTypingClock(clock, 8_000);

		expect(fixture.acknowledgementAttempts).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 9_000 },
		]);
		expect(fixture.acknowledgements).toEqual([{ channelId: TYPING_CHANNEL_ID, at: 1_000 }]);
		expect(errors).toEqual([expect.objectContaining({ message: `Discord typing keepalive failed for channel ${TYPING_CHANNEL_ID}: fixture typing acknowledgement failed` })]);

		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgements).toEqual([
			{ channelId: TYPING_CHANNEL_ID, at: 1_000 },
			{ channelId: TYPING_CHANNEL_ID, at: 17_000 },
		]);
		expect(fixture.connected).toBe(true);
	} finally {
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive allows only one deferred refresh per channel", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true));
	try {
		await fixture.emitMessage({ id: "typing-deferred-refresh", channelId: TYPING_CHANNEL_ID, text: "hold one refresh request" });
		await clock.flushAsync();
		fixture.deferNextTyping();
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(clock.scheduledTimerCount).toBe(1);

		await advanceTypingIntervals(clock, 3);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(clock.scheduledTimerCount).toBe(1);

		fixture.resolveNextTyping();
		await clock.flushAsync();
		expect(fixture.pendingTypingCount).toBe(0);
		expect(clock.scheduledTimerCount).toBe(2);
		await advanceTypingClock(clock, 8_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(3);
	} finally {
		while (fixture.pendingTypingCount > 0) fixture.resolveNextTyping();
		await clock.flushAsync();
		await adapter.stop();
	}
});

externalTest("Discord typing keepalive does not reschedule or report after stop during a refresh", async () => {
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	const errors: Error[] = [];
	const adapter = await startTypingFixtureAdapter(fixture, clock, typingAdapterRpc(true), error => errors.push(error));
	let stopped = false;
	try {
		await fixture.emitMessage({ id: "typing-stop-pending", channelId: TYPING_CHANNEL_ID, text: "stop pending refresh" });
		await clock.flushAsync();
		fixture.deferNextTyping();
		await advanceTypingClock(clock, 8_000);
		expect(fixture.pendingTypingCount).toBe(1);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);

		await adapter.stop();
		stopped = true;
		expect(clock.scheduledTimerCount).toBe(0);
		fixture.rejectNextTyping(new Error("typing request rejected after adapter stop"));
		await clock.flushAsync();
		await advanceTypingClock(clock, 30_000);
		expect(fixture.acknowledgementAttempts).toHaveLength(2);
		expect(clock.scheduledTimerCount).toBe(0);
		expect(errors).toEqual([]);
	} finally {
		if (fixture.pendingTypingCount > 0) fixture.resolveNextTyping();
		await clock.flushAsync();
		if (!stopped) await adapter.stop();
	}
});

externalTest("Discord typing follows accepted admission, not a stale healthy-to-fenced connection", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		});
		gatewayUnderTest.fixture.holdNextTurn();
		gatewayUnderTest.fixture.suppressNextAdmissionReceipt();
		const ambiguous = await gatewayUnderTest.client.request("main.submit", {
			text: "establish an admission recovery fence",
			surface_id: "discord:owner-dm",
			idempotency_key: "discord-health-flap-fence",
		});
		expect(ambiguous.error).toMatchObject({ code: -32603 });
		const [pending] = gatewayUnderTest.core.mainAdmissionOperationsPending();
		const intent = JSON.parse(pending?.intentJson ?? "{}") as { op_ref?: unknown };
		if (typeof intent.op_ref !== "string") throw new Error("fenced Discord setup did not retain its operation reference");
		await eventually(
			() => (gatewayUnderTest.host.mutationReadinessReason === "admission_recovery_pending" ? true : undefined),
			"gateway did not enter the admission recovery fence",
		);

		const blocked = { id: "discord-fenced-message", channelId: "123456789012345678", text: "must remain unacknowledged" };
		await expect(route.handle({ ...blocked, acceptedAt: Date.now() })).rejects.toMatchObject({ name: "RpcResponseError", code: 1003 });
		expect(fixture.connected).toBe(true);
		expect(fixture.acknowledgements).toEqual([]);
		expect(gatewayUnderTest.fixture.commands().filter(command => command.text === blocked.text)).toEqual([]);

		gatewayUnderTest.fixture.complete(intent.op_ref, { text: "recovery fence terminal reply" });
		await eventually(
			() => (gatewayUnderTest.host.mutationReadinessReason === undefined ? true : undefined),
			"gateway did not promote after the fenced admission's terminal evidence",
		);
		const fresh = { id: "discord-after-promotion", channelId: "123456789012345678", text: "fresh ingress after promotion", acceptedAt: Date.now() };
		expect(await route.handle(fresh)).toBe(true);
		expect(fixture.acknowledgements).toHaveLength(1);
		expect(gatewayUnderTest.fixture.commands()).toEqual(
			expect.arrayContaining([expect.objectContaining({ operation: "turn.prompt", text: fresh.text })]),
		);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord startup reports rate-limited verifying and transport readiness diagnostics without opening ingress", async () => {
	const config = {
		rpcSocketPath: "/tmp/discord-gateway-readiness.sock",
		token: "fixture-token",
		route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		ackBudgetMs: 2_000,
		claimTtlMs: 5_000,
		readWaitMs: 0,
	};
	const verifyingPlatform = new DiscordFixture();
	const verifyingAbort = new AbortController();
	const verifyingDiagnostics: string[] = [];
	let healthRequests = 0;
	const verifyingRpc: JsonRpcClient = {
		async request() {
			healthRequests += 1;
			return { jsonrpc: "2.0", id: healthRequests, result: { status: "booting", state: "verifying" } };
		},
		close() {},
	};
	const verifyingStart = startDiscordAdapter(config, {
		rpcConnect: async () => verifyingRpc,
		platformFactory: () => verifyingPlatform,
		startupSignal: verifyingAbort.signal,
		onDiagnostic: message => verifyingDiagnostics.push(message),
	});
	await eventually(() => (healthRequests >= 3 ? true : undefined), "adapter did not poll the verifying gateway");
	verifyingAbort.abort();
	await expect(verifyingStart).rejects.toMatchObject({ name: "AbortError" });
	expect(verifyingPlatform.connectCount).toBe(0);
	expect(verifyingDiagnostics).toEqual(["gateway verifying (expected wait); delaying Discord connection until healthy/running."]);

	const transportAbort = new AbortController();
	const transportDiagnostics: string[] = [];
	let connectionAttempts = 0;
	const transportStart = startDiscordAdapter(config, {
		rpcConnect: async () => {
			connectionAttempts += 1;
			throw new Error("fixture UDS unavailable");
		},
		startupSignal: transportAbort.signal,
		onDiagnostic: message => transportDiagnostics.push(message),
	});
	await eventually(() => (connectionAttempts >= 3 ? true : undefined), "adapter did not retry the unavailable gateway transport");
	transportAbort.abort();
	await expect(transportStart).rejects.toMatchObject({ name: "AbortError" });
	expect(transportDiagnostics).toEqual(["gateway transport/protocol error while waiting: could not connect to the local gateway: fixture UDS unavailable"]);
});

externalTest("Discord outbox settles server-side delivery before a restart can repost", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "settle this" }));
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
		expect(gatewayUnderTest.core.consumerOutbox("gajaeway-discord")).toEqual([
			expect.objectContaining({ seq: appended.seq, state: "sent", dedupeKey: `gajaeway-discord:discord:owner-dm:${appended.seq}` }),
		]);

		const restarted = outbox(gatewayUnderTest, fixture);
		expect(await restarted.runOnce()).toBe("idle");
		expect(fixture.sends).toHaveLength(1);
	} finally {
		await fixture.disconnect();
	}
});

externalTest("Discord crash before send leaves the durable checkpoint for restart delivery", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "retry before send" }));
		const abort = new AbortController();
		const interrupted = outbox(gatewayUnderTest, fixture, { beforeSend: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toEqual([]);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
	}
}, 15_000);

externalTest("Discord crash after send before settlement retries with a nonce and does not double-post", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "retry after send" }));
		const abort = new AbortController();
		const interrupted = outbox(gatewayUnderTest, fixture, { afterSendBeforeCommit: () => abort.abort() });
		await expect(interrupted.runOnce(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
		expect(fixture.sends).toHaveLength(1);
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe("1:0");

		await Bun.sleep(5_100);
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(fixture.sends).toHaveLength(1);
		const wireNonce = discordWireNonce(discordDedupeKey("discord:owner-dm", String(appended.seq)));
		expect(wireNonce.length).toBeLessThanOrEqual(25);
		expect(fixture.sendAttempts).toEqual([
			expect.objectContaining({ duplicate: false, nonce: wireNonce }),
			expect.objectContaining({ duplicate: true, nonce: wireNonce }),
		]);
		for (const attempt of fixture.sendAttempts) {
			// Live Discord rejects nonces longer than 25 characters (50035 NONCE_TYPE_TOO_LONG).
			expect(attempt.nonce.length).toBeLessThanOrEqual(25);
		}
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
	} finally {
		await fixture.disconnect();
	}
}, 15_000);

externalTest("Discord non-owner engagement is admitted as follow_up whether the external turn is idle or busy", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const guestRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "222222222222222222", surfaceId: "discord:guest-channel" },
		});
		await guestRoute.handle({ id: "guest-idle", channelId: "222222222222222222", text: "idle guest message", acceptedAt: Date.now() });

		gatewayUnderTest.fixture.holdNextTurn();
		const ownerRoute = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
		});
		await ownerRoute.handle({ id: "owner-held", channelId: "123456789012345678", text: "held owner message", acceptedAt: Date.now() });
		await eventually(() => (gatewayUnderTest.host.turnState === "busy" ? true : undefined), "owner turn was not admitted as busy");
		await guestRoute.handle({ id: "guest-busy", channelId: "222222222222222222", text: "busy guest message", acceptedAt: Date.now() });

		expect(gatewayUnderTest.fixture.commands().map(command => ({ operation: command.operation, text: command.text }))).toEqual([
			{ operation: "turn.follow_up", text: "idle guest message" },
			{ operation: "turn.prompt", text: "held owner message" },
			{ operation: "turn.follow_up", text: "busy guest message" },
		]);
		expect(fixture.acknowledgements).toHaveLength(3);
		const heldPrompt = gatewayUnderTest.fixture.commands().find(command => command.operation === "turn.prompt");
		if (typeof heldPrompt?.opRef !== "string") throw new Error("held Discord owner prompt was not recorded with an operation reference");
		gatewayUnderTest.fixture.complete(heldPrompt.opRef, { text: "settled after Discord follow-up assertion" });
		await eventually(
			() =>
				gatewayUnderTest.core
					.journalRead("1:0", 100)
					.events.some(event => event.kind === "assistant_message" && JSON.parse(event.payloadJson).text === "settled after Discord follow-up assertion")
					? true
					: undefined,
			"held Discord owner prompt did not settle before teardown",
			15_000,
		);
	} finally {
		await fixture.disconnect();
	}
});
