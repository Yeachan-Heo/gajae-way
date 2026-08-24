import { expect, test } from "bun:test";
import type { OwnerSurface } from "../../src/profile";
import { parseThreadSurfaceId, resolveSurface, threadSurfaceId } from "../../src/surface-routing";
import { resolveDiscordEgressRoute, validateDiscordRoutes } from "../../src/adapter/discord/route";

function catalog(...surfaces: OwnerSurface[]): ReadonlyMap<string, OwnerSurface> {
	return new Map(surfaces.map(surface => [surface.id, surface]));
}

const discordChannel: OwnerSurface = { id: "discord:consultant-hermes", platform: "discord", kind: "channel", sessionKind: "conversation" };
const telegramGroup: OwnerSurface = { id: "telegram:ops", platform: "telegram", kind: "channel", sessionKind: "conversation" };
const ownerDm: OwnerSurface = { id: "discord:owner-dm", platform: "discord", kind: "dm", sessionKind: "main" };

test("a derived child surface round-trips through one shared format", () => {
	const id = threadSurfaceId(discordChannel.id, "1493635653441945762");
	expect(id).toBe("discord:consultant-hermes/thread:1493635653441945762");
	expect(parseThreadSurfaceId(id)).toEqual({ parentSurfaceId: discordChannel.id, threadId: "1493635653441945762" });
	// A non-numeric or separator-bearing thread id is refused so the prefix rule
	// stays unambiguous for the gateway's admission check.
	expect(() => threadSurfaceId(discordChannel.id, "not-numeric")).toThrow();
	expect(parseThreadSurfaceId("discord:consultant-hermes")).toBeUndefined();
});

test("a Telegram forum topic is admissible, not only a Discord thread", () => {
	// This is the drift the split sources of truth caused: the gateway's admission
	// resolver hardcoded platform === "discord", so topic surfaces the Telegram
	// adapter derives were refused as unknown_surface and the feature was dead.
	const surfaces = catalog(discordChannel, telegramGroup, ownerDm);
	const telegramTopic = threadSurfaceId(telegramGroup.id, "777");
	const resolvedTelegram = resolveSurface(telegramTopic, surfaces);
	expect(resolvedTelegram).toMatchObject({
		// The derived child inherits the parent's declared redaction class.
		surface: { id: telegramTopic, platform: "telegram", kind: "thread", sessionKind: "conversation" },
		quarantineSurface: telegramGroup,
		derived: true,
	});

	const discordThread = threadSurfaceId(discordChannel.id, "999");
	expect(resolveSurface(discordThread, surfaces)).toMatchObject({
		surface: { platform: "discord", kind: "thread" },
		quarantineSurface: discordChannel,
		derived: true,
	});
});

test("only a configured channel surface can parent a derived child", () => {
	const surfaces = catalog(discordChannel, ownerDm);
	// A DM cannot parent a thread, and an unconfigured parent stays refused.
	expect(resolveSurface(threadSurfaceId(ownerDm.id, "12"), surfaces)).toBeUndefined();
	expect(resolveSurface("discord:not-configured/thread:12", surfaces)).toBeUndefined();
	// An exact configured surface always wins and is never marked derived.
	expect(resolveSurface(discordChannel.id, surfaces)).toMatchObject({ surface: discordChannel, derived: false });
});

test("route validation refuses a table where a surface is ingressable but not deliverable", () => {
	// Ingress and egress are separate lookups over the same table, so an ambiguous
	// or broken egress mapping must be caught at startup rather than at reply time.
	const duplicateSurface = [
		{ channelId: "111111111111111111", surfaceId: "discord:a", kind: "channel" as const, groupPolicy: "mention" as const },
		{ channelId: "222222222222222222", surfaceId: "discord:a", kind: "channel" as const, groupPolicy: "mention" as const },
	];
	expect(() => validateDiscordRoutes(duplicateSurface)).toThrow();

	const healthy = [
		{ channelId: "111111111111111111", surfaceId: "discord:a", kind: "channel" as const, groupPolicy: "mention" as const },
		{ channelId: "333333333333333333", surfaceId: "discord:owner", kind: "dm" as const },
	];
	expect(() => validateDiscordRoutes(healthy)).not.toThrow();
	// And the derived thread child of a channel route is deliverable.
	expect(resolveDiscordEgressRoute(healthy, "discord:a/thread:987654321")).toMatchObject({ channelId: "987654321" });
});
