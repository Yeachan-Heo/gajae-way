import { expect, test } from "bun:test";
import { resolveApplicationOwnerIds } from "../src/main";

/**
 * Issue #862: the adapter resolves its own application's owners at login —
 * the `owner` user, or every ACCEPTED team member when the owner is a Team.
 * Accepted semantics come from discord.js (`TeamMemberMembershipState.Accepted
 * === 2`); guild ownership is never consulted; failure resolves to undefined
 * so every gate stays closed.
 */

const APPLICATION = {
	fetch: () =>
		Promise.resolve({
			owner: { id: "1000000000000000001" },
		}),
};

/** Create a discord.js Collection-like object that iterates values. */
function createMockCollection(members: Array<{ id: string; user: { id: string }; membershipState: number }>) {
	return {
		values() {
			return members[Symbol.iterator]();
		},
		[Symbol.iterator]() {
			return members[Symbol.iterator]();
		},
	};
}

const TEAM_APPLICATION = {
	fetch: () =>
		Promise.resolve({
			owner: {
				members: createMockCollection([
					{ id: "2000000000000000001", user: { id: "2000000000000000001" }, membershipState: 2 },
					{ id: "2000000000000000002", user: { id: "2000000000000000002" }, membershipState: 2 },
					{ id: "2000000000000000003", user: { id: "2000000000000000003" }, membershipState: 1 },
				]),
			},
		}),
};

test("a user-owned application resolves to the owner id", async () => {
	expect(await resolveApplicationOwnerIds(APPLICATION)).toEqual(["1000000000000000001"]);
});

test("a team-owned application resolves to accepted members only, invited excluded", async () => {
	const ids = await resolveApplicationOwnerIds(TEAM_APPLICATION);
	expect(ids).toHaveLength(2);
	expect(ids).toContain("2000000000000000001");
	expect(ids).toContain("2000000000000000002");
	expect(ids).not.toContain("2000000000000000003");
});

test("a failed lookup resolves to undefined (fail closed), not an empty authority", async () => {
	const errors: string[] = [];
	const original = console.error;
	console.error = (...values: unknown[]) => errors.push(values.map(String).join(" "));
	try {
		expect(
			await resolveApplicationOwnerIds({
				fetch: () => Promise.reject(new Error("DiscordAPIError: 500")),
			}),
		).toBeUndefined();
	} finally {
		console.error = original;
	}
	expect(errors.join("\n")).toContain("application owner lookup failed");
});

test("no application (not ready yet) resolves to undefined", async () => {
	expect(await resolveApplicationOwnerIds(undefined)).toBeUndefined();
	expect(await resolveApplicationOwnerIds(null)).toBeUndefined();
});

test("an application that resolves neither owner nor members keeps the gate closed", async () => {
	expect(await resolveApplicationOwnerIds({ fetch: () => Promise.resolve({ owner: null }) })).toBeUndefined();
	expect(
		await resolveApplicationOwnerIds({
			fetch: () => Promise.resolve({ owner: { id: "not-a-snowflake" } }),
		}),
	).toBeUndefined();
	expect(
		await resolveApplicationOwnerIds({
			fetch: () =>
				Promise.resolve({
					owner: {
						members: createMockCollection([{ id: "1", user: { id: "1" }, membershipState: 2 }]),
					},
				}),
		}),
	).toBeUndefined();
});
