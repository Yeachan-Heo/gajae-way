import { describe, expect, test } from "bun:test";
import {
	ATTACH_PREAMBLE_MAX_BYTES,
	assembleGjcAttachArgv,
	classifyGjcWrapperFlags,
} from "../src/orchestrator/gjc-client";

/**
 * Closed-allowlist flag contract + argv assembly (plan step 2, AC-7/AC-8/AC-13/AC-15).
 *
 * The load-bearing property is that a REFUSED value-taking flag consumes its
 * value: `--resume ID` must not leave `ID` behind as a positional that reaches
 * the child.
 */

describe("classifyGjcWrapperFlags", () => {
	test("drops wrapper-owned flags without forwarding or refusing them", () => {
		const result = classifyGjcWrapperFlags(["--new", "--socket", "/tmp/x.sock"]);
		expect(result.wrapperOwned).toContain("--new");
		expect(result.wrapperOwned).toContain("--socket");
		expect(result.refused).toEqual([]);
		// `--socket`'s value is not a forwarded token either.
		expect(result.forwarded).toEqual([]);
	});

	test.each([
		["--resume", "session-abc"],
		["--session-dir", "/tmp/elsewhere"],
		["--append-system-prompt", "evil override"],
		["--mode", "json"],
		["--system-prompt", "you are something else"],
	])("refuses %s and consumes its value", (flag, value) => {
		const result = classifyGjcWrapperFlags([flag, value]);
		expect(result.refused).toContain(flag);
		// The value must not survive anywhere the child could see it.
		expect(result.forwarded).not.toContain(value);
		expect(result.forwarded).toEqual([]);
	});

	test.each([
		["--resume=session-abc", "--resume"],
		["--session-dir=/tmp/elsewhere", "--session-dir"],
		["--mode=json", "--mode"],
	])("refuses the inline-value form %s", (token, name) => {
		const result = classifyGjcWrapperFlags([token]);
		expect(result.refused).toContain(name);
		expect(result.forwarded).toEqual([]);
	});

	test.each([
		"-p",
		"--print",
		"--continue",
		"-c",
		"--fork",
		"--no-session",
		"-r",
	])("refuses presence-only flag %s", (flag) => {
		const result = classifyGjcWrapperFlags([flag]);
		expect(result.refused).toContain(flag);
		expect(result.forwarded).toEqual([]);
	});

	test("refuses unknown flags fail-closed", () => {
		const result = classifyGjcWrapperFlags(["--totally-unknown", "--yolo"]);
		expect(result.refused).toContain("--totally-unknown");
		expect(result.refused).toContain("--yolo");
		expect(result.forwarded).toEqual([]);
	});

	test("treats --worktree and -w as WRAPPER-OWNED, never forwarding them", () => {
		// Forwarding the flag would let native gjc enter a worktree it names itself
		// and then offer to FORK the gateway-bound session (reproduced against real
		// gjc). The gateway prepares the worktree and binds there instead, so the
		// flag is consumed here and acted on by the gateway.
		for (const [flag, value] of [
			["--worktree", "feature-x"],
			["-w", "feature-x"],
		] as const) {
			const result = classifyGjcWrapperFlags([flag, value]);
			expect(result.worktreeBranch).toBe("feature-x");
			expect(result.wrapperOwned).toContain(flag);
			expect(result.refused).toEqual([]);
			// Neither the flag nor its value may reach the child.
			expect(result.forwarded).toEqual([]);
			expect(result.forwarded).not.toContain(value);
		}

		// Inline form carries the branch too.
		expect(classifyGjcWrapperFlags(["--worktree=feature-x"]).worktreeBranch).toBe("feature-x");
		// Bare form means "a managed worktree, gateway picks the name".
		expect(classifyGjcWrapperFlags(["--worktree"]).worktreeBranch).toBe("");
		// Absent means no worktree at all, which must stay distinguishable from bare.
		expect(classifyGjcWrapperFlags([]).worktreeBranch).toBeUndefined();
		expect(classifyGjcWrapperFlags(["--thinking"]).worktreeBranch).toBeUndefined();
	});

	test("forwards --thinking as presence-only, consuming no extra token", () => {
		const result = classifyGjcWrapperFlags(["--thinking", "--model", "opus"]);
		expect(result.forwarded).toEqual(["--thinking", "--model", "opus"]);
		expect(result.refused).toEqual([]);
	});

	test("forwards model selectors and reports operatorModel", () => {
		const model = classifyGjcWrapperFlags(["--model", "opus"]);
		expect(model.forwarded).toEqual(["--model", "opus"]);
		expect(model.operatorModel).toBe(true);

		const preset = classifyGjcWrapperFlags(["--mpreset", "codex-medium"]);
		expect(preset.forwarded).toEqual(["--mpreset", "codex-medium"]);
		expect(preset.operatorModel).toBe(true);

		expect(classifyGjcWrapperFlags(["--thinking"]).operatorModel).toBe(false);
	});

	test("stops classifying at `--`", () => {
		const result = classifyGjcWrapperFlags(["--thinking", "--", "--resume", "x"]);
		expect(result.forwarded).toEqual(["--thinking"]);
		// Tokens after `--` are positionals; the binder uses none of them, and they
		// must not be reported as refusals either.
		expect(result.refused).toEqual([]);
	});

	test("a refused flag mixed with forwarded ones still fails closed", () => {
		const result = classifyGjcWrapperFlags(["--model", "opus", "--resume", "abc", "--thinking"]);
		expect(result.refused).toEqual(["--resume"]);
		expect(result.forwarded).toEqual(["--model", "opus", "--thinking"]);
		expect(result.forwarded).not.toContain("abc");
	});

	test("refuses a valueless model selector instead of letting it swallow the next flag", () => {
		// `--model --thinking` would make gjc read `--thinking` as the model value.
		const result = classifyGjcWrapperFlags(["--model", "--thinking"]);
		expect(result.refused).toContain("--model");
		expect(result.forwarded).not.toContain("--model");

		expect(classifyGjcWrapperFlags(["--model"]).refused).toContain("--model");
		expect(classifyGjcWrapperFlags(["--mpreset"]).refused).toContain("--mpreset");
	});

	test("refuses a repeated model selector so one argv can never carry two", () => {
		const repeated = classifyGjcWrapperFlags(["--model", "opus", "--model", "sonnet"]);
		expect(repeated.refused).toContain("--model");
		expect(repeated.forwarded).toEqual(["--model", "opus"]);
		expect(repeated.forwarded).not.toContain("sonnet");

		const mixed = classifyGjcWrapperFlags(["--model", "opus", "--mpreset", "codex-medium"]);
		expect(mixed.refused).toContain("--mpreset");
		expect(mixed.forwarded).toEqual(["--model", "opus"]);
	});

	test("a wrapper-owned --worktree does not swallow a following flag", () => {
		const result = classifyGjcWrapperFlags(["--worktree", "--thinking"]);
		// `--thinking` is a flag, not the worktree's value, so it still forwards and
		// the worktree falls back to the bare form.
		expect(result.worktreeBranch).toBe("");
		expect(result.forwarded).toEqual(["--thinking"]);
		expect(result.refused).toEqual([]);
	});

	test("refuses a blank inline value in every whitespace form", () => {
		// `--model=` parses as "has a value" while carrying nothing usable, so it
		// must fail closed exactly like a missing value. The separate-token and
		// inline forms must agree on what counts as blank — they once did not, and a
		// whitespace-only inline value slipped through.
		for (const blank of ["", " ", "   ", "\t", "\n", " \t\n "]) {
			expect(classifyGjcWrapperFlags([`--model=${blank}`]).refused).toContain("--model");
			expect(classifyGjcWrapperFlags([`--model=${blank}`]).forwarded).toEqual([]);
			expect(classifyGjcWrapperFlags([`--mpreset=${blank}`]).refused).toContain("--mpreset");
			// `--worktree`/`-w` are wrapper-owned: a blank value degrades to the bare
			// form rather than reaching the child.
			expect(classifyGjcWrapperFlags([`--worktree=${blank}`]).worktreeBranch).toBe("");
			expect(classifyGjcWrapperFlags([`--worktree=${blank}`]).forwarded).toEqual([]);
			expect(classifyGjcWrapperFlags([`-w=${blank}`]).forwarded).toEqual([]);
		}

		// A non-blank inline value is still fine, including one that merely contains
		// whitespace.
		expect(classifyGjcWrapperFlags(["--model=opus"]).forwarded).toEqual(["--model=opus"]);
		expect(classifyGjcWrapperFlags(["--model=opus"]).operatorModel).toBe(true);
		// `--worktree` is wrapper-owned; its value is captured, never forwarded.
		expect(classifyGjcWrapperFlags(["--worktree=my branch"]).worktreeBranch).toBe("my branch");
		expect(classifyGjcWrapperFlags(["--worktree=my branch"]).forwarded).toEqual([]);
	});

	test("never treats `--` as a flag's value", () => {
		// `--` is a separator. Swallowing it as a value would both lose the separator
		// and hand gjc an empty model selector.
		const model = classifyGjcWrapperFlags(["--model", "--"]);
		expect(model.refused).toContain("--model");
		expect(model.forwarded).toEqual([]);

		// Same for a refused value-taking flag: `--` must not be consumed as its
		// value. Putting a REAL FLAG after `--` proves classification actually
		// stopped there — the old separator-consuming behavior produced the same
		// refusal list, so asserting only `--resume` would be ceremonial.
		const refused = classifyGjcWrapperFlags(["--resume", "--", "--print", "positional"]);
		expect(refused.refused).toEqual(["--resume"]);
		expect(refused.refused).not.toContain("--print");
		expect(refused.forwarded).toEqual([]);
	});

	test("refuses a standalone empty value token", () => {
		// `--model ""` is a separate token that is present but blank; forwarding it
		// hands gjc a model selector it cannot resolve.
		for (const empty of ["", "   "]) {
			const model = classifyGjcWrapperFlags(["--model", empty]);
			expect(model.refused).toContain("--model");
			expect(model.forwarded).toEqual([]);

			const preset = classifyGjcWrapperFlags(["--mpreset", empty]);
			expect(preset.refused).toContain("--mpreset");
			expect(preset.forwarded).toEqual([]);

			// `--worktree` is wrapper-owned, so a blank value degrades to the bare
			// form and nothing reaches the child.
			const worktree = classifyGjcWrapperFlags(["--worktree", empty]);
			expect(worktree.worktreeBranch).toBe("");
			expect(worktree.forwarded).toEqual([]);
			expect(worktree.refused).toEqual([]);
		}
	});

	test("an empty value cannot leak a model selector alongside another flag", () => {
		const result = classifyGjcWrapperFlags(["--model", "", "--thinking"]);
		expect(result.refused).toContain("--model");
		expect(result.forwarded).toEqual(["--thinking"]);
	});
});

describe("assembleGjcAttachArgv", () => {
	const base = {
		sessionId: "bound-session-1",
		personaPreamble: "PERSONA",
		configModel: undefined,
	};

	test("injects the binder flags and nothing forbidden", () => {
		const argv = assembleGjcAttachArgv({ ...base, forwarded: [] });
		expect(argv[0]).toBe("gjc");
		expect(argv).toContain("--resume");
		expect(argv[argv.indexOf("--resume") + 1]).toBe("bound-session-1");
		expect(argv[argv.indexOf("--append-system-prompt") + 1]).toBe("PERSONA");
		// The native coding register is preserved by omission, and the child must
		// stay interactive.
		expect(argv).not.toContain("--system-prompt");
		expect(argv).not.toContain("-p");
		expect(argv).not.toContain("--print");
		expect(argv).not.toContain("--mode");
	});

	test("never injects --session-dir, because create and resume must share a scope", () => {
		// `gjc sdk session raw` (the create path) rejects `--session-dir`, so the
		// bound session lives in gjc's default managed scope. Injecting the epoch
		// directory here made the real TUI exit with `Session "<id>" not found`.
		const argv = assembleGjcAttachArgv({ ...base, forwarded: [] });
		expect(argv).not.toContain("--session-dir");

		// It must nevertheless stay REFUSED for operators: redirecting the store
		// would unbind the managed session.
		expect(classifyGjcWrapperFlags(["--session-dir", "/tmp/elsewhere"]).refused).toContain("--session-dir");
	});

	test("injects config.model exactly once when the operator gave none", () => {
		const argv = assembleGjcAttachArgv({ ...base, forwarded: [], configModel: "sonnet" });
		expect(argv.filter((token) => token === "--model")).toEqual(["--model"]);
		expect(argv[argv.indexOf("--model") + 1]).toBe("sonnet");
	});

	test("operator model suppresses config.model so argv never carries two", () => {
		const argv = assembleGjcAttachArgv({
			...base,
			forwarded: ["--model", "opus"],
			configModel: "sonnet",
		});
		expect(argv.filter((token) => token === "--model")).toEqual(["--model"]);
		expect(argv[argv.indexOf("--model") + 1]).toBe("opus");
		expect(argv).not.toContain("sonnet");
	});

	test("operator mpreset also suppresses a config model", () => {
		const argv = assembleGjcAttachArgv({
			...base,
			forwarded: ["--mpreset", "codex-medium"],
			configModel: "sonnet",
		});
		expect(argv).not.toContain("--model");
		expect(argv).toContain("--mpreset");
	});

	test("config mpreset selection is injected as --mpreset", () => {
		const argv = assembleGjcAttachArgv({ ...base, forwarded: [], configModel: { preset: "opencodego" } });
		expect(argv[argv.indexOf("--mpreset") + 1]).toBe("opencodego");
	});

	test("forwarded operator flags land after the binder flags", () => {
		const argv = assembleGjcAttachArgv({ ...base, forwarded: ["--mpreset", "codex-medium", "--thinking"] });
		expect(argv.slice(-3)).toEqual(["--mpreset", "codex-medium", "--thinking"]);
	});

	test("the preamble bound is a real byte ceiling", () => {
		expect(ATTACH_PREAMBLE_MAX_BYTES).toBe(32_768);
	});
});
