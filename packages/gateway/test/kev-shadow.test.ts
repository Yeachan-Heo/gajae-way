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

test("the other three probes are diagnostics and cannot veto a real question", () => {
	// With real history the ack/isAnswer probes fire on the context they were
	// given; applying them as vetoes crushed measured true positives to 0.083.
	const vetoed = shadowScore([0.73, 0.95, 0.88, 0.62]);
	expect(vetoed.score).toBeCloseTo(0.73, 5);
	expect(vetoed.verdict).toBe("would-engage");
	// Still reported, so a verdict stays auditable against the raw probes.
	expect(vetoed.ack).toBeCloseTo(0.95, 5);
	expect(vetoed.isAnswer).toBeCloseTo(0.88, 5);
	expect(vetoed.chatter).toBeCloseTo(0.62, 5);
});

test("verdict bands sit between the two measured populations", () => {
	// Ambient community traffic measured 0.024..0.391, answer-me traffic 0.662..0.820.
	expect(shadowScore([0.391, 0.1, 0.1, 0.9]).verdict).toBe("would-skip");
	expect(shadowScore([0.5, 0, 0, 0]).verdict).toBe("would-defer");
	expect(shadowScore([0.662, 0.9, 0.9, 0.9]).verdict).toBe("would-engage");
});

test("the log line separates addressed traffic from ambient traffic", async () => {
	const server = Bun.serve({
		port: 0,
		fetch: () =>
			Response.json({
				probs: [
					[0.8, 0.2],
					[0.5, 0.5],
					[0.5, 0.5],
					[0.6, 0.4],
				],
			}),
	});
	const savedUrl = process.env.KEV_SHADOW_URL;
	process.env.KEV_SHADOW_URL = `http://127.0.0.1:${server.port}`;
	const lines: string[] = [];
	const error = console.error;
	console.error = (line: unknown) => {
		lines.push(String(line));
	};
	try {
		await recordKevShadow({ originKey: "discord:c1", text: "잘되냐 이제", addressed: true });
		await recordKevShadow({ originKey: "discord:c1", text: "ㅋㅋㅋ" });
	} finally {
		console.error = error;
		if (savedUrl === undefined) {
			delete process.env.KEV_SHADOW_URL;
		} else process.env.KEV_SHADOW_URL = savedUrl;
		server.stop(true);
	}
	expect(lines).toHaveLength(2);
	// A direct question scoring would-skip is exactly the case that must stay visible.
	expect(lines[0]).toContain("addressed=1");
	expect(lines[0]).toContain("verdict=would-skip");
	expect(lines[1]).toContain("addressed=0");
});

test("earlier turns are rendered oldest-first under the context header and counted", async () => {
	const seen: string[] = [];
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			seen.push(((await request.json()) as { state: string }).state);
			return Response.json({
				probs: [
					[0.27, 0.73],
					[0.5, 0.5],
					[0.5, 0.5],
					[0.6, 0.4],
				],
			});
		},
	});
	const savedUrl = process.env.KEV_SHADOW_URL;
	process.env.KEV_SHADOW_URL = `http://127.0.0.1:${server.port}`;
	const lines: string[] = [];
	const error = console.error;
	console.error = (line: unknown) => {
		lines.push(String(line));
	};
	try {
		await recordKevShadow({
			originKey: "discord:c1",
			text: "잘되냐 이제",
			addressed: true,
			earlier: ["Bellman: 유닛으로 올려라", "you: 올렸습니다. /health 200입니다."],
		});
	} finally {
		console.error = error;
		if (savedUrl === undefined) {
			delete process.env.KEV_SHADOW_URL;
		} else process.env.KEV_SHADOW_URL = savedUrl;
		server.stop(true);
	}
	const state = seen[0] as string;
	expect(state.indexOf("유닛으로 올려라")).toBeLessThan(state.indexOf("/health 200"));
	expect(state).not.toContain("(no earlier messages)");
	// The context depth is on the line: a shadow read with no history is not
	// evidence about the gate, it is evidence the caller starved it.
	expect(lines[0]).toContain("ctx=2");
	expect(lines[0]).toContain("verdict=would-engage");
});
