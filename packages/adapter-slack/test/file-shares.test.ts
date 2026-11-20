import { expect, test } from "bun:test";
import { FILE_SHARE_PENDING_TTL_MS, SlackFileShares } from "../src/file-shares";

const channel = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const thread = { platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" } as const;

test("a share is claimed once, by its place and name, in upload order", () => {
	const shares = new SlackFileShares();
	shares.expect(thread, "C1", "1.0", "a.txt");
	shares.expect(channel, "C1", "1.0", "a.txt");
	expect(shares.claim("C1", "1.0", "b.txt")).toBeUndefined();
	expect(shares.claim("C1", undefined, "a.txt")).toBeUndefined();
	expect(shares.claim("C1", "1.0", "a.txt")).toEqual(thread);
	expect(shares.claim("C1", "1.0", "a.txt")).toEqual(channel);
	expect(shares.claim("C1", "1.0", "a.txt")).toBeUndefined();
});

test("a failed upload is withdrawn and a stale one expires", () => {
	let clock = 0;
	const shares = new SlackFileShares(() => clock);
	const withdraw = shares.expect(thread, "C1", "1.0", "a.txt");
	withdraw();
	expect(shares.claim("C1", "1.0", "a.txt")).toBeUndefined();

	shares.expect(thread, "C1", "1.0", "a.txt");
	clock += FILE_SHARE_PENDING_TTL_MS;
	expect(shares.claim("C1", "1.0", "a.txt")).toBeUndefined();
});
