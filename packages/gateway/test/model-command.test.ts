import { describe, expect, test } from "bun:test";
import { applyModelCommand, parseModelArgument } from "../src/server/model-command";
import type { GjcModelSelection } from "../src/store/db";

function store(initial?: GjcModelSelection) {
	let current = initial;
	const writes: GjcModelSelection[] = [];
	return {
		conversationModelGet: () => (current === undefined ? undefined : { selection: current }),
		conversationModelSet: (_key: string, selection: GjcModelSelection) => {
			current = selection;
			writes.push(selection);
		},
		conversationModelClear: () => {
			const had = current !== undefined;
			current = undefined;
			return had;
		},
		writes,
		get current() {
			return current;
		},
	};
}

const CHANNEL = { platform: "discord" };

describe("parseModelArgument", () => {
	test("a bare name is a preset, a slashed name is an explicit selector", () => {
		expect(parseModelArgument("gpt-heavy")).toEqual({ preset: "gpt-heavy" });
		expect(parseModelArgument("z-ai/glm-5.3")).toBe("z-ai/glm-5.3");
	});

	test("prefixes force the interpretation", () => {
		expect(parseModelArgument("preset:frontier-heavy")).toEqual({ preset: "frontier-heavy" });
		// A provider selector with no slash would otherwise be read as a preset.
		expect(parseModelArgument("model:some-local-model")).toBe("some-local-model");
	});

	test("rejects empty and multi-token input", () => {
		expect(parseModelArgument("")).toEqual({ error: "empty selection" });
		expect(parseModelArgument("   ")).toEqual({ error: "empty selection" });
		expect(parseModelArgument("gpt heavy")).toEqual({ error: "a selection cannot contain spaces" });
	});
});

describe("/model show", () => {
	test("reports the conversation override and names it as such", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model", "k", CHANNEL, s, "config-default");
		expect(out.text).toContain("preset gpt-heavy");
		expect(out.text).toContain("this conversation");
		expect(out.resetSession).toBe(false);
	});

	test("falls back to the gateway default and says where it came from", () => {
		const out = applyModelCommand("/model show", "k", CHANNEL, store(), { preset: "frontier-default" });
		expect(out.text).toContain("preset frontier-default");
		expect(out.text).toContain("gateway default");
	});

	test("reports gjc's own default when nothing is configured", () => {
		const out = applyModelCommand("/model", "k", CHANNEL, store(), undefined);
		expect(out.text).toContain("gjc default");
	});
});

describe("/model set", () => {
	test("stores the selection and demands a session reset", () => {
		const s = store();
		const out = applyModelCommand("/model set gpt-heavy", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "gpt-heavy" });
		// The reset must be stated: a live gjc process cannot change its own argv,
		// so a silent "set" would do nothing until an unrelated restart.
		expect(out.resetSession).toBe(true);
		expect(out.text).toContain("session reset");
	});

	test("accepts the bare form without the set keyword", () => {
		const s = store();
		const out = applyModelCommand("/model glm-gpt", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "glm-gpt" });
		expect(out.resetSession).toBe(true);
	});

	test("a bad selection changes nothing and explains the usage", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model set two words", "k", CHANNEL, s, undefined);
		expect(s.writes).toHaveLength(0);
		expect(s.current).toEqual({ preset: "gpt-heavy" });
		expect(out.resetSession).toBe(false);
		expect(out.text).toContain("/model set");
	});

	test("replaces an existing override rather than stacking", () => {
		const s = store({ preset: "glm-gpt" });
		applyModelCommand("/model set frontier-heavy", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "frontier-heavy" });
	});
});

describe("/model clear", () => {
	test("removes an override and resets", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model clear", "k", CHANNEL, s, "config-default");
		expect(s.current).toBeUndefined();
		expect(out.resetSession).toBe(true);
		expect(out.text).toContain("config-default");
	});

	test("clearing nothing does not claim a reset that did not happen", () => {
		const out = applyModelCommand("/model clear", "k", CHANNEL, store(), undefined);
		expect(out.resetSession).toBe(false);
		expect(out.text).toContain("no conversation override");
	});
});
