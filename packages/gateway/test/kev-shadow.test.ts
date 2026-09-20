import { expect, test } from "bun:test";
import { kevShadowEnabled, recordKevShadow, renderShadowState, shadowScore } from "../src/engagement/kev-shadow";

test("disabled unless KEV_SHADOW_URL is set, so an unconfigured deploy is a no-op", async () => {
	const saved = process.env.KEV_SHADOW_URL;
	delete process.env.KEV_SHADOW_URL;
	expect(kevShadowEnabled()).toBe(false);
	// Must resolve without reaching the network or throwing.
	await recordKevShadow({ originKey: "discord:c1", text: "anything" });
	if (saved !== undefined) process.env.KEV_SHADOW_URL = saved;
});

test("an unreachable gate never throws and never blocks", async () => {
	const saved = process.env.KEV_SHADOW_URL;
	// Port 9 discards; the probe must swallow the failure.
	process.env.KEV_SHADOW_URL = "http://127.0.0.1:9";
	process.env.KEV_SHADOW_TIMEOUT_MS = "300";
	const started = Date.now();
	await recordKevShadow({ originKey: "discord:c1", text: "gjc worktree 만들면 심링크가 깨집니다" });
	expect(Date.now() - started).toBeLessThan(3000);
	if (saved === undefined) {
		delete process.env.KEV_SHADOW_URL;
	} else process.env.KEV_SHADOW_URL = saved;
});

test("the judged message is isolated under its own header", () => {
	const state = renderShadowState({
		originKey: "discord:c1",
		text: "does ultragoal keep the ledger across sessions?",
		earlier: ["alice: 릴리즈 나갔나요?", "bob: 네 나갔어요"],
		authorLabel: "carol",
	});
	expect(state).toContain("EARLIER CONTEXT:");
	expect(state).toContain("NEW MESSAGE:");
	// The new message must come last so the questions can point at it.
	expect(state.indexOf("NEW MESSAGE:")).toBeGreaterThan(state.indexOf("EARLIER CONTEXT:"));
	expect(state).toContain("carol: does ultragoal keep the ledger across sessions?");
	expect(state).toContain("alice: 릴리즈 나갔나요?");
});

test("with no earlier turns the context section says so rather than being blank", () => {
	const state = renderShadowState({ originKey: "discord:c1", text: "ㅎㅇ" });
	expect(state).toContain("(no earlier messages)");
	expect(state).toContain("user: ㅎㅇ");
});

test("with no earlier turns, context-dependent vetoes cannot fire", () => {
	// isAnswer=0.73 on a standalone question is exactly the observed failure.
	const withoutContext = shadowScore([0.85, 0.6, 0.73, 0.1], false);
	const withContext = shadowScore([0.85, 0.6, 0.73, 0.1], true);
	expect(withoutContext.score).toBeGreaterThan(withContext.score);
	// chatter is low here, so without context nothing vetoes at all.
	expect(withoutContext.score).toBeCloseTo(0.85, 5);
	expect(withoutContext.verdict).toBe("would-engage");
});

test("a veto only bites above 0.5 rather than multiplying every factor down", () => {
	// Three mid-confidence reads must not compound a real question into a skip.
	const mid = shadowScore([0.8, 0.5, 0.5, 0.5], true);
	expect(mid.score).toBeCloseTo(0.8, 5);
	expect(mid.verdict).toBe("would-engage");
	// A confident veto still suppresses.
	const vetoed = shadowScore([0.8, 0.95, 0.2, 0.2], true);
	expect(vetoed.score).toBeLessThan(0.25);
	expect(vetoed.verdict).toBe("would-skip");
});

test("verdict bands are ordered and exhaustive", () => {
	expect(shadowScore([0.05, 0, 0, 0], false).verdict).toBe("would-skip");
	expect(shadowScore([0.4, 0, 0, 0], false).verdict).toBe("would-defer");
	expect(shadowScore([0.9, 0, 0, 0], false).verdict).toBe("would-engage");
});
