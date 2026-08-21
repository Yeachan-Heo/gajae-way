import { AsyncLocalStorage } from "node:async_hooks";
import { expect, test } from "bun:test";
import { DiscordOutbox } from "../../src/adapter/discord/outbox";
import { startDiscordAdapter } from "../../src/adapter/discord/main";
import { DiscordRouteHandler } from "../../src/adapter/discord/route";
import type { JsonRpcClient } from "../../src/rpc-client";
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
		expect(fixture.sendAttempts).toEqual([
			expect.objectContaining({ duplicate: false, nonce: `gajaeway-discord:discord:owner-dm:${appended.seq}` }),
			expect.objectContaining({ duplicate: true, nonce: `gajaeway-discord:discord:owner-dm:${appended.seq}` }),
		]);
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
