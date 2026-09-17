import { expect, test } from "bun:test";
import { resolveSlackDisplayName, SlackDirectory, type SlackUserLike } from "../src/author";

test("Slack display names skip blanks at each precedence level", () => {
	const user = {
		id: "U1",
		name: "handle",
		real_name: "real",
		profile: { real_name: "profile real", display_name: "display" },
	};
	expect(resolveSlackDisplayName(user)).toBe("display");
	expect(resolveSlackDisplayName({ ...user, profile: { ...user.profile, display_name: " " } })).toBe("profile real");
	expect(resolveSlackDisplayName({ ...user, profile: null })).toBe("real");
	expect(resolveSlackDisplayName({ ...user, profile: null, real_name: "" })).toBe("handle");
	expect(resolveSlackDisplayName(undefined)).toBeUndefined();
});

test("Slack directory caches, updates LRU on reads, primes payloads and hides DM names", async () => {
	const calls: string[] = [];
	const directory = new SlackDirectory(
		{
			usersInfo: async (id) => {
				calls.push(id);
				return { id, name: id };
			},
			conversationsInfo: async (id) => ({ id, name: "general", is_im: id === "IM" }),
		},
		2,
	);
	await directory.user("U1");
	await directory.user("U2");
	expect(directory.userName("U1")).toBe("U1");
	directory.prime({ id: "U3", profile: { display_name: "three" } });
	expect(directory.userName("U2")).toBeUndefined();
	await directory.user("U1");
	expect(calls).toEqual(["U1", "U2"]);
	expect(directory.userName("U3")).toBe("three");
	await directory.user("U2");
	expect(calls).toEqual(["U1", "U2", "U2"]);
	for (const id of ["C1", "D1", "IM"]) {
		await directory.conversation(id);
		expect(directory.channelName(id)).toBe(id === "C1" ? "general" : undefined);
	}
});

test("Slack negative lookups expire after 60 seconds and concurrent requests coalesce", async () => {
	let now = 0;
	let calls = 0;
	const directory = new SlackDirectory(
		{
			usersInfo: async (id) => {
				calls++;
				if (calls === 1) throw new Error("Slack unavailable");
				return { id, name: "recovered" };
			},
			conversationsInfo: async () => {
				throw new Error("Slack unavailable");
			},
		},
		2,
		() => now,
	);
	expect(await Promise.all([directory.user("U1"), directory.user("U1")])).toEqual([undefined, undefined]);
	now = 59_999;
	expect(await directory.user("U1")).toBeUndefined();
	expect(calls).toBe(1);
	now = 60_000;
	expect((await directory.user("U1"))?.name).toBe("recovered");
	expect(calls).toBe(2);
	expect(await directory.conversation("C1")).toBeUndefined();
});

test("Slack priming during a lookup preserves the fresher payload", async () => {
	let resolve!: (user: SlackUserLike) => void;
	const directory = new SlackDirectory({
		usersInfo: () =>
			new Promise((done) => {
				resolve = done;
			}),
		conversationsInfo: async (id) => ({ id }),
	});
	const request = directory.user("U1");
	await Promise.resolve();
	directory.prime({ id: "U1", name: "fresh" });
	resolve({ id: "U1", name: "stale" });
	expect((await request)?.name).toBe("fresh");
	expect(directory.userName("U1")).toBe("fresh");
});
