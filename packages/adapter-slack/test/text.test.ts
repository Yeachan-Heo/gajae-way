import { expect, test } from "bun:test";
import { mentionedUserIds, normalizeSlackText } from "../src/text";

test("Slack mention extraction preserves order and deduplicates", () => {
	expect(mentionedUserIds("<@U2|two> <@U1> <@U2> @U3 &lt;@U4&gt;")).toEqual(["U2", "U1"]);
});

test("Slack token normalization honors cached names then labels then identifiers", () => {
	const names = { userName: (id: string) => (id === "U1" ? "Alice" : undefined) };
	expect(normalizeSlackText("<@U1|handle> <@U2|Bob> <@U3>", names)).toBe("@Alice @Bob @U3");
	for (const [input, output] of [
		["<#C1|general> <#C2>", "#general #C2"],
		["<https://x|label> <https://x>", "label (https://x) https://x"],
		["<mailto:a@b|a@b> <mailto:a@b>", "a@b a@b"],
		["<!here> <!channel> <!everyone>", "@here @channel @everyone"],
		["<!subteam^S1|@group>", "@group"],
		["<!date^123^{date_short}|yesterday>", "yesterday"],
		["<!unknown|fallback> <!unknown>", "fallback <!unknown>"],
		["&lt;@U1&gt; &amp; &amp;lt; <https://x?a=1&amp;b=2|A &lt; B>", "<@U1> & &lt; A < B (https://x?a=1&b=2)"],
	])
		expect(normalizeSlackText(input as string)).toBe(output);
});

test("Slack Enterprise Grid mentions normalize and prime the directory", () => {
	expect(mentionedUserIds("<@W123> <@W123|name>")).toEqual(["W123"]);
	expect(normalizeSlackText("<@W123>", { userName: () => "Enterprise" })).toBe("@Enterprise");
});
