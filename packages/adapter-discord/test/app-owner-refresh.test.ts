import { expect, test } from "bun:test";
import { ReconnectingGateway } from "../src/main";

/**
 * Issue #862 finding 3 at the adapter boundary: `refreshAppOwnerIds` must
 * fail CLOSED. A Discord lookup that fails (or resolves nobody) clears the
 * owner set — it never keeps stale grants — and a successful refresh pushes
 * the replacement to the live gateway via `gateway.adapterOwners`.
 */

function gatewayFor(application: unknown, initialOwners?: readonly string[]): ReconnectingGateway {
	const gateway = new ReconnectingGateway(
		"/tmp/never-used.sock",
		{ channels: { fetch: async () => undefined }, application } as never,
		{ tokenFile: "token", token: "redacted", configPath: "config", channels: {} } as never,
	);
	gateway.setAppOwnerIds(initialOwners);
	return gateway;
}

test("a refresh failure clears the owners (fail closed), never keeps the previous set", async () => {
	const gateway = gatewayFor({ fetch: () => Promise.reject(new Error("DiscordAPIError: 500")) }, [
		"2000000000000000001",
	]);
	expect(gateway.ownerResolved).toBe(true);
	await gateway.refreshAppOwnerIds();
	expect(gateway.ownerResolved).toBe(false);
});

test("a refresh that resolves nobody clears the owners too", async () => {
	const gateway = gatewayFor({ fetch: () => Promise.resolve({ owner: null }) }, ["2000000000000000001"]);
	await gateway.refreshAppOwnerIds();
	expect(gateway.ownerResolved).toBe(false);
});

test("a refresh with an unchanged set reports no change and keeps authority", async () => {
	const team = {
		fetch: () =>
			Promise.resolve({ owner: { ownerId: "2000000000000000001", members: { values: () => [][Symbol.iterator]() } } }),
	};
	const gateway = gatewayFor(team, ["2000000000000000001"]);
	await gateway.refreshAppOwnerIds();
	expect(gateway.ownerResolved).toBe(true);
});
