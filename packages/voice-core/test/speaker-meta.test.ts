import { describe, expect, test } from "bun:test";
import { assembleEngagementContext, resolveSpeakerDisplayName } from "../src/speaker-meta";

describe("speaker metadata", () => {
	const base = {
		authorId: "user-1",
		guildNickname: "Guild Nick",
		memberDisplayName: "Member Display",
		globalName: "Global Name",
		handle: "handle",
		channelLabel: "lounge",
		serverLabel: "Community",
	} as const;

	test("uses guild nickname before all other names", () => {
		expect(resolveSpeakerDisplayName(base)).toBe("Guild Nick");
	});

	test("falls through member, global, and handle precedence", () => {
		expect(resolveSpeakerDisplayName({ ...base, guildNickname: "" })).toBe("Member Display");
		expect(resolveSpeakerDisplayName({ ...base, guildNickname: " ", memberDisplayName: "" })).toBe("Global Name");
		expect(resolveSpeakerDisplayName({ ...base, guildNickname: null, memberDisplayName: "\t", globalName: null })).toBe(
			"handle",
		);
	});

	test("skips whitespace-only values and preserves meaningful values", () => {
		expect(
			assembleEngagementContext({
				authorId: "user-1",
				guildNickname: "  ",
				memberDisplayName: "\n",
				globalName: "Global",
				handle: "  handle  ",
				channelLabel: " ",
				serverLabel: "Server",
			}),
		).toEqual({
			authorId: "user-1",
			authorName: "Global",
			authorHandle: "  handle  ",
			serverLabel: "Server",
		});
	});

	test("returns only the required id when no optional metadata exists", () => {
		expect(assembleEngagementContext({ authorId: "user-1" })).toEqual({ authorId: "user-1" });
	});
});
