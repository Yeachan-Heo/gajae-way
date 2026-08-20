import { AsyncLocalStorage } from "node:async_hooks";
import { expect, test } from "bun:test";
import { DiscordOutbox } from "../../src/adapter/discord/outbox";
import { DiscordRouteHandler } from "../../src/adapter/discord/route";
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
	const clock = new DiscordFixtureClock(1_000);
	const fixture = new DiscordFixture({ now: clock.now });
	await fixture.connect();
	try {
		const route = new DiscordRouteHandler({
			rpc: gatewayUnderTest.client,
			platform: fixture,
			route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
			acknowledgement: { now: clock.now, budgetMs: 2_000 },
		});
		const unsubscribe = fixture.onMessage(async message => {
			await route.handle(message);
		});
		const inbound = { id: "message-id-once", channelId: "123456789012345678", text: "external broker round trip" };
		await Promise.all([fixture.emitMessage(inbound), fixture.emitMessage(inbound)]);
		unsubscribe();

		expect(fixture.acknowledgements).toHaveLength(1);
		expect(fixture.acknowledgements[0]?.at).toBeLessThanOrEqual(clock.now() + 2_000);
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

externalTest("Discord outbox settles server-side delivery before a restart can repost", async () => {
	const gatewayUnderTest = await gateway();
	const fixture = new DiscordFixture();
	await fixture.connect();
	try {
		const appended = gatewayUnderTest.core.journalAppend("assistant_message", JSON.stringify({ finalized: true, text: "settle this" }));
		expect(await outbox(gatewayUnderTest, fixture).runOnce()).toBe("sent");
		expect(gatewayUnderTest.core.consumerCursor("gajaeway-discord")).toBe(appended.cursor);
		expect(gatewayUnderTest.core.consumerOutbox("gajaeway-discord")).toEqual([
			expect.objectContaining({ seq: appended.seq, state: "sent", dedupeKey: "gajaeway-discord:discord:owner-dm:1" }),
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
			expect.objectContaining({ duplicate: false, nonce: "gajaeway-discord:discord:owner-dm:1" }),
			expect.objectContaining({ duplicate: true, nonce: "gajaeway-discord:discord:owner-dm:1" }),
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
