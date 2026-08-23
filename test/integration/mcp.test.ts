import { expect, test } from "bun:test";
import { DiscordOutbox } from "../../src/adapter/discord/outbox";
import { createExternalGateway, eventually } from "../helpers/external-gateway";
import { DiscordFixture } from "../fixtures/discord-fixture";

const OWNER_ROUTE = { channelId: "123456789012345678", surfaceId: "discord:owner-dm", kind: "dm" } as const;
const CONSULTANT_ROUTE = { channelId: "222222222222222222", surfaceId: "discord:consultant", kind: "channel", engagement: "always" } as const;

test("main.say reaches a non-owner Discord surface through the real journal outbox without creating a turn", async () => {
	const gateway = await createExternalGateway({
		ownerSurface: { id: OWNER_ROUTE.surfaceId, platform: "discord", kind: "dm" },
		knownSurfaces: [{ id: CONSULTANT_ROUTE.surfaceId, platform: "discord", kind: "channel" }],
	});
	const discord = new DiscordFixture();
	try {
	await discord.connect();
		const request = { text: "persona initiated consultant delivery", surface_id: CONSULTANT_ROUTE.surfaceId, idempotency_key: "mcp-consultant-one" };
		const first = await gateway.client.request("main.say", request);
		expect(first.error).toBeUndefined();
		expect(first.result).toMatchObject({ accepted: true, origin: "persona", surface_id: CONSULTANT_ROUTE.surfaceId });
		const retry = await gateway.client.request("main.say", request);
		expect(retry.result).toEqual(first.result);
		const rows = await eventually(
			() => {
				const events = gateway.core.journalRead("1:0", 100).events;
				return events.some(event => event.kind === "assistant_message" && event.payloadJson.includes("persona initiated consultant delivery")) ? events : undefined;
			},
			"main.say event was not journaled",
		);
		expect(rows.filter(event => event.kind === "assistant_message" && event.payloadJson.includes("mcp-consultant-one"))).toHaveLength(1);
		expect(rows.filter(event => event.kind === "turn_start" || event.kind === "turn_end")).toEqual([]);
		expect(gateway.fixture.admissionAttempts()).toEqual([]);

		const outbox = new DiscordOutbox({
			rpc: gateway.client,
			platform: discord,
			routes: [OWNER_ROUTE, CONSULTANT_ROUTE],
			unattributedDelivery: "owner-dm",
			unattributedRoute: OWNER_ROUTE,
			claimTtlMs: 5_000,
			readWaitMs: 0,
		});
		expect(await outbox.runOnce()).toBe("sent");
		expect(discord.sends).toHaveLength(1);
		expect(discord.sends[0]).toMatchObject({ channelId: CONSULTANT_ROUTE.channelId, text: request.text });
	} finally {
		await gateway.stop();
	}
});

test("main.turn.origin returns an admitted surface and refuses autonomous context honestly", async () => {
	const gateway = await createExternalGateway({ ownerSurface: { id: OWNER_ROUTE.surfaceId, platform: "discord", kind: "dm" } });
	try {
		gateway.fixture.holdNextTurn();
		const accepted = await gateway.client.request("main.submit", { text: "origin probe", surface_id: OWNER_ROUTE.surfaceId, idempotency_key: "origin-probe" });
		const opRef = (accepted.result as { op_ref: string }).op_ref;
		expect(await gateway.client.request("main.turn.origin", {})).toMatchObject({ result: { surface_id: OWNER_ROUTE.surfaceId } });
		gateway.fixture.complete(opRef, { text: "origin probe complete" });
		await eventually(() => (gateway.host.turnState === "idle" ? true : undefined), "origin probe turn did not settle");
		const autonomous = await gateway.client.request("main.turn.origin", {});
		expect(autonomous.error).toMatchObject({ code: 1403, message: "turn_origin_unavailable" });
	} finally {
		await gateway.stop();
	}
});

test("way.surfaces exposes owner and known non-owner metadata without widening the RPC surface", async () => {
	const gateway = await createExternalGateway({
		ownerSurface: { id: OWNER_ROUTE.surfaceId, platform: "discord", kind: "dm" },
		knownSurfaces: [{ id: CONSULTANT_ROUTE.surfaceId, platform: "discord", kind: "channel" }],
	});
	try {
		expect(await gateway.client.request("main.surfaces", {})).toMatchObject({
			result: expect.arrayContaining([
				{ id: OWNER_ROUTE.surfaceId, platform: "discord", kind: "dm", is_owner: true },
				{ id: CONSULTANT_ROUTE.surfaceId, platform: "discord", kind: "channel", is_owner: false },
			]),
		});
	} finally {
		await gateway.stop();
	}
});

test("main.say rejects an unknown surface with existing refusal code", async () => {
	const gateway = await createExternalGateway({ ownerSurface: { id: OWNER_ROUTE.surfaceId, platform: "discord", kind: "dm" } });
	try {
		const response = await gateway.client.request("main.say", { text: "should refuse", surface_id: "discord:unknown", idempotency_key: "unknown-surface" });
		expect(response.error).toMatchObject({ code: 1300, message: "unknown_surface" });
	} finally {
		await gateway.stop();
	}
});