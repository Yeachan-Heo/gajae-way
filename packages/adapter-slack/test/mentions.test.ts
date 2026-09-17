import { expect, test } from "bun:test";
import type { SlackUserLike } from "../src/author";
import { buildReverseIndex, repairMentions } from "../src/mentions";

const users: SlackUserLike[] = [
	{ id: "U0C2GSKTA6M", name: "bellman", real_name: "Yeachan Heo", profile: { display_name: "신규허예찬 (Bellman)" } },
	{ id: "U0C29R46FC3", name: "gajaeway", real_name: "sionic-gajae", profile: { display_name: "sionic-gajae" } },
	{ id: "U0BT1S5UGS1", name: "jane.doe", real_name: "Jane Doe", profile: { display_name: "" } },
	// Two users sharing a display name: neither may be repaired by that name.
	{ id: "U0AAAAAAAA1", name: "minsu.a", real_name: "Minsu", profile: { display_name: "Minsu" } },
	{ id: "U0AAAAAAAA2", name: "minsu.b", real_name: "Minsu", profile: { display_name: "Minsu" } },
];
const directory = { knownUsers: () => users };

// Every shape below was observed verbatim in the live workspace on 2026-09-17.
test("a backticked <@U…> becomes a real mention", () => {
	expect(repairMentions("`<@U0C2GSKTA6M>` 확인 부탁드립니다", directory)).toBe("<@U0C2GSKTA6M> 확인 부탁드립니다");
	expect(repairMentions("cc `<@U0BT1S5UGS1|Jane>`", directory)).toBe("cc <@U0BT1S5UGS1|Jane>");
});

test("a bare @U… id gets its angle brackets back", () => {
	expect(repairMentions("@U0BT1S5UGS1 링크 뚫었습니다", directory)).toBe("<@U0BT1S5UGS1> 링크 뚫었습니다");
	// Already-correct mentions are untouched (no double wrapping).
	expect(repairMentions("<@U0BT1S5UGS1> 링크", directory)).toBe("<@U0BT1S5UGS1> 링크");
});

test("@handle resolves through the directory, by handle or display name, case- and separator-insensitively", () => {
	expect(repairMentions("@bellman 형님 보고드립니다", directory)).toBe("<@U0C2GSKTA6M> 형님 보고드립니다");
	expect(repairMentions("@sionic-gajae 가 처리", directory)).toBe("<@U0C29R46FC3> 가 처리");
	expect(repairMentions("@Jane.Doe 께", directory)).toBe("<@U0BT1S5UGS1> 께");
	expect(repairMentions("@jane_doe 께", directory)).toBe("<@U0BT1S5UGS1> 께");
	expect(repairMentions("@JaneDoe 께", directory)).toBe("<@U0BT1S5UGS1> 께");
});

test("unknown, ambiguous, and non-person @tokens are left exactly as written", () => {
	// Not a user the adapter knows.
	expect(repairMentions("@RequestMapping 어노테이션", directory)).toBe("@RequestMapping 어노테이션");
	expect(repairMentions("@OG 팀", directory)).toBe("@OG 팀");
	// Two users share the name: a wrong ping is worse than none.
	expect(repairMentions("@Minsu 확인", directory)).toBe("@Minsu 확인");
	// Handles are still unique, so those resolve.
	expect(repairMentions("@minsu.a 확인", directory)).toBe("<@U0AAAAAAAA1> 확인");
});

test("emails, paths, and scoped package names are not mentions", () => {
	for (const text of [
		"mail me at bellman@sionic.ai",
		"`@sionic-ai/fe-harness` 설치 403",
		"user@host:/path",
		"https://x.test/@bellman",
	])
		expect(repairMentions(text, directory)).toBe(text);
});

test("fenced code is never repaired; prose around it is", () => {
	const text = "@bellman 봐주세요\n```\nconst who = '@bellman'; // <@U0C2GSKTA6M>\n```\n그리고 `<@U0BT1S5UGS1>` 도";
	expect(repairMentions(text, directory)).toBe(
		"<@U0C2GSKTA6M> 봐주세요\n```\nconst who = '@bellman'; // <@U0C2GSKTA6M>\n```\n그리고 <@U0BT1S5UGS1> 도",
	);
});

test("text without @ is returned untouched without building the index", () => {
	let built = 0;
	const lazy = {
		*knownUsers() {
			built++;
			yield* users;
		},
	};
	expect(repairMentions("멘션 없음", lazy)).toBe("멘션 없음");
	expect(repairMentions("<@U0C2GSKTA6M> 이미 정상", lazy)).toBe("<@U0C2GSKTA6M> 이미 정상");
	expect(built).toBe(0);
});

test("the reverse index drops names two users share and ignores blank profile fields", () => {
	const index = buildReverseIndex(users);
	expect(index.get("minsu")).toBeUndefined();
	expect(index.get("minsua")).toBe("U0AAAAAAAA1");
	expect(index.get("bellman")).toBe("U0C2GSKTA6M");
	expect(index.get("yeachanheo")).toBe("U0C2GSKTA6M");
	expect(index.has("")).toBe(false);
});
