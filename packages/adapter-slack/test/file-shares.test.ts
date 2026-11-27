import { expect, test } from "bun:test";
import { FILE_SHARE_PENDING_LIMIT, FILE_SHARE_PENDING_TTL_MS, SlackFileShares } from "../src/file-shares";

const channel = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const thread = { platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" } as const;

test("a share is claimed once, by its own file id, whatever order the events arrive in", () => {
	const shares = new SlackFileShares();
	shares.expect("F1", thread);
	shares.expect("F2", channel);
	expect(shares.claim("F3")).toBeUndefined();
	expect(shares.claim("F2")).toEqual(channel);
	expect(shares.claim("F1")).toEqual(thread);
	expect(shares.claim("F1")).toBeUndefined();
});

test("a failed upload is withdrawn, a stale one expires, and the pending set stays bounded", () => {
	let clock = 0;
	const shares = new SlackFileShares(() => clock);
	shares.expect("F1", thread)();
	expect(shares.claim("F1")).toBeUndefined();

	shares.expect("F2", thread);
	clock += FILE_SHARE_PENDING_TTL_MS;
	expect(shares.claim("F2")).toBeUndefined();

	for (let index = 0; index <= FILE_SHARE_PENDING_LIMIT; index++) shares.expect(`G${index}`, channel);
	expect(shares.claim("G0")).toBeUndefined();
	expect(shares.claim(`G${FILE_SHARE_PENDING_LIMIT}`)).toEqual(channel);
});
