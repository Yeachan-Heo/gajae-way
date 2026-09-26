import { expect, test } from "bun:test";
import {
	ASSISTANT_LABEL,
	kevShadowEnabled,
	recordKevShadow,
	renderShadowState,
	shadowClass,
	shadowScore,
} from "../src/engagement/kev-shadow";

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

test("the judged message is isolated under its own header, after the thread it lands in", () => {
	const state = renderShadowState({
		originKey: "discord:c1",
		text: "does ultragoal keep the ledger across sessions?",
		earlier: [
			{ author: "alice", body: "릴리즈 나갔나요?" },
			{ author: ASSISTANT_LABEL, body: "네, 방금 나갔습니다" },
		],
		authorLabel: "carol",
	});
	expect(state).toContain("CONVERSATION SO FAR");
	expect(state).toContain("NEW MESSAGE (judge only this one):");
	// The new message must come last so the questions can point at it.
	expect(state.indexOf("NEW MESSAGE")).toBeGreaterThan(state.indexOf("CONVERSATION SO FAR"));
	expect(state).toContain("carol: does ultragoal keep the ledger across sessions?");
	expect(state).toContain("alice: 릴리즈 나갔나요?");
	// The assistant's own turn is labelled, not anonymised: `directed` needs a referent.
	expect(state).toContain(`${ASSISTANT_LABEL}: 네, 방금 나갔습니다`);
	expect(state).toContain(`LAST SPEAKER BEFORE THIS: ${ASSISTANT_LABEL}`);
});

test("the state carries place and why the message counts as addressed", () => {
	const dm = renderShadowState({
		originKey: "discord:dm",
		text: "야",
		place: "direct message",
		addressed: true,
		addressedBy: "dm",
	});
	expect(dm).toContain("PLACE: direct message");
	expect(dm).toContain("ADDRESSED: yes");
	expect(dm).toContain("a direct message");
	const ambient = renderShadowState({ originKey: "discord:c1", text: "ㅋㅋㅋ", place: "group channel #dev-main" });
	expect(ambient).toContain("PLACE: group channel #dev-main");
	expect(ambient).toContain("ADDRESSED: no");
	expect(ambient).toContain("(no earlier messages)");
});

test("turn ages are rendered relative to now so recency is legible", () => {
	const now = Date.parse("2026-09-20T12:00:00.000Z");
	const state = renderShadowState(
		{
			originKey: "discord:c1",
			text: "그래서 됐냐",
			earlier: [
				{ author: "owner", body: "확인해봐", at: "2026-09-20T09:00:00.000Z" },
				{ author: ASSISTANT_LABEL, body: "확인했습니다", at: "2026-09-20T11:58:00.000Z" },
			],
		},
		6000,
		now,
	);
	expect(state).toContain("[3h ago] owner: 확인해봐");
	expect(state).toContain(`[2m ago] ${ASSISTANT_LABEL}: 확인했습니다`);
});

test("the oldest turns are dropped first and the drop is announced", () => {
	const earlier = Array.from({ length: 40 }, (_, i) => ({
		author: "alice",
		body: `message number ${i} ${"x".repeat(80)}`,
	}));
	const state = renderShadowState({ originKey: "discord:c1", text: "ping", earlier }, 1200);
	expect(state.length).toBeLessThanOrEqual(1200);
	expect(state).toContain("(older turns omitted)");
	// The newest turn survives, the oldest does not.
	expect(state).toContain("message number 39");
	expect(state).not.toContain("message number 0 ");
	expect(state).toContain("NEW MESSAGE (judge only this one):\nuser: ping");
});

test("a bare summons engages on `directed` even with no help signal", () => {
	// The live 11:48Z failure: 야 / 살아있는거맞냐 scored help≈0.32..0.36 and were skipped.
	// The running gate scores both at directed≈0.98 once it can see the thread.
	expect(shadowScore([0.36, 0.98, 0.32, 0.2, 0.24]).verdict).toBe("would-engage");
	expect(shadowScore([0.32, 0.98, 0.35, 0.3, 0.33]).verdict).toBe("would-engage");
	// help alone — the pre-change design — would have skipped both.
	expect(shadowScore([0.36, 0, 0.32, 0.2, 0.24]).verdict).toBe("would-skip");
});

test("a weakly-flagged closing thanks does not reach engage", () => {
	// Live gate reading of "ㅇㅇ 수고" after an answer: directed 0.98, ack only 0.53.
	// A veto biting from 0.5 with a shallow slope let this engage; it must not.
	const weak = shadowScore([0.16, 0.98, 0.53, 0.18, 0.28]);
	expect(weak.verdict).toBe("would-defer");
	expect(weak.score).toBeLessThan(0.6);
});

test("`isAnswer` and `chatter` are diagnostics and cannot veto a real question", () => {
	// With real history those probes fire on the context they were given; applying
	// them as vetoes crushed measured true positives to 0.083, losing 4 of 24.
	const notVetoed = shadowScore([0.73, 0.1, 0.1, 0.88, 0.62]);
	expect(notVetoed.score).toBeCloseTo(0.73, 5);
	expect(notVetoed.verdict).toBe("would-engage");
	// Still reported, so a verdict stays auditable against the raw probes.
	expect(notVetoed.isAnswer).toBeCloseTo(0.88, 5);
	expect(notVetoed.chatter).toBeCloseTo(0.62, 5);
});

test("a confident closing ack is the one veto that survives", () => {
	const thanks = shadowScore([0.05, 0.45, 0.96, 0.2, 0.6]);
	expect(thanks.score).toBe(0);
	expect(thanks.verdict).toBe("would-skip");
	// Below the bite point it costs nothing.
	expect(shadowScore([0.7, 0.2, 0.34, 0.2, 0.2]).score).toBeCloseTo(0.7, 5);
});

test("verdict bands sit between the two measured populations", () => {
	// Ambient community traffic measured 0.024..0.391, answer-me traffic 0.662..0.820.
	expect(shadowScore([0.391, 0.1, 0.1, 0.1, 0.9]).verdict).toBe("would-skip");
	expect(shadowScore([0.5, 0.1, 0, 0, 0]).verdict).toBe("would-defer");
	expect(shadowScore([0.662, 0.1, 0.1, 0.9, 0.9]).verdict).toBe("would-engage");
});

test("the log line reports every factor, why it was addressed, and how much thread it saw", async () => {
	const server = Bun.serve({
		port: 0,
		fetch: () =>
			Response.json({
				probs: [
					[0.8, 0.2],
					[0.1, 0.9],
					[0.9, 0.1],
					[0.5, 0.5],
					[0.4, 0.6],
				],
			}),
	});
	const savedUrl = process.env.KEV_SHADOW_URL;
	process.env.KEV_SHADOW_URL = `http://127.0.0.1:${server.port}`;
	const lines: string[] = [];
	const error = console.error;
	console.error = (line: unknown) => {
		// console.error is process-wide: timers left by other test files in this
		// run can log while the probe awaits. Only the shadow's own lines count.
		if (String(line).startsWith("kev-shadow ")) lines.push(String(line));
	};
	try {
		await recordKevShadow({
			originKey: "discord:c1",
			text: "잘되냐 이제",
			addressed: true,
			addressedBy: "dm",
			earlier: [{ author: ASSISTANT_LABEL, body: "배포했습니다" }],
		});
		await recordKevShadow({ originKey: "discord:c1", text: "ㅋㅋㅋ" });
	} finally {
		console.error = error;
		if (savedUrl === undefined) {
			delete process.env.KEV_SHADOW_URL;
		} else process.env.KEV_SHADOW_URL = savedUrl;
		server.stop(true);
	}
	expect(lines).toHaveLength(2);
	// directed=0.9 on addressed traffic now carries the decision that help=0.2 lost.
	expect(lines[0]).toContain("addressed=dm");
	expect(lines[0]).toContain("directed=0.9000");
	expect(lines[0]).toContain("ctx=1");
	expect(lines[0]).toContain("verdict=would-engage");
	// Ambient, no thread: the same probabilities must not be read as addressed.
	expect(lines[1]).toContain("addressed=0");
	expect(lines[1]).toContain("ctx=0");
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
					[0.2, 0.8],
					[0.8, 0.2],
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
		// console.error is process-wide: timers left by other test files in this
		// run can log while the probe awaits. Only the shadow's own lines count.
		if (String(line).startsWith("kev-shadow ")) lines.push(String(line));
	};
	try {
		await recordKevShadow({
			originKey: "discord:c1",
			text: "잘되냐 이제",
			addressed: true,
			earlier: [
				{ author: "Bellman", body: "유닛으로 올려라" },
				{ author: ASSISTANT_LABEL, body: "올렸습니다. /health 200입니다." },
			],
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

test("a cron self-prompt is its own class, not addressed traffic", () => {
	// The follow-up posts mention this bot, so `addressed` alone would file them
	// with the owner's questions and a promoted gate would skip every sweep.
	expect(
		shadowClass({ originKey: "d:c", text: "🔄 [clawhip] Follow-up <@bot>", addressed: true, authorIsBot: true }),
	).toBe("machine");
	expect(shadowClass({ originKey: "d:c", text: "잘되냐이제", addressed: true })).toBe("addressed");
	expect(shadowClass({ originKey: "d:c", text: "ㅋㅋㅋ" })).toBe("ambient");
});
