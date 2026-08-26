import { describe, expect, test } from "bun:test";
import { isRuntimeSessionId, LaneRegistry, LaneScopeError, laneSessionKey, laneSessionName } from "../src/lane";

describe("laneSessionName", () => {
	test("builds the issue and pr forms", () => {
		expect(laneSessionName({ kind: "issue", number: 123, slug: "runtime-cycle" })).toBe(
			"project-issue-123-runtime-cycle",
		);
		expect(laneSessionName({ kind: "pr", number: 7, slug: "admin-ui" })).toBe("project-pr-7-admin-ui");
	});

	test.each([["Runtime_Cycle"], ["-leading"], ["trailing-"], [""]])("rejects a non-kebab slug %p", (slug) => {
		expect(() => laneSessionName({ kind: "issue", number: 1, slug })).toThrow(LaneScopeError);
	});

	test("rejects a non-positive lane number", () => {
		expect(() => laneSessionName({ kind: "issue", number: 0, slug: "x" })).toThrow(LaneScopeError);
	});
});

describe("isRuntimeSessionId", () => {
	test("a lane name is not a runtime session id", () => {
		expect(isRuntimeSessionId("project-issue-1-x")).toBe(false);
		expect(isRuntimeSessionId("1e392759-c034-4f11-b5d2-a51b1e4a87ef")).toBe(true);
	});
});

describe("LaneRegistry", () => {
	const laneA = {
		sessionName: "project-issue-1-a",
		branch: "feat/a",
		worktreePath: "/wt/a",
	};

	test("re-claiming the same lane is idempotent across a supervisor restart", () => {
		const registry = new LaneRegistry();
		registry.claim(laneA);
		registry.claim(laneA);
		expect(registry.size).toBe(1);
		expect(registry.owner("feat/a")?.sessionName).toBe(laneA.sessionName);
	});

	test("refuses a second mutation owner on the same branch", () => {
		const registry = new LaneRegistry();
		registry.claim(laneA);
		expect(() => registry.claim({ sessionName: "other", branch: "feat/a", worktreePath: "/wt/other" })).toThrow(
			/already owned by project-issue-1-a/,
		);
	});

	test("refuses a second owner on the same worktree even under a different branch", () => {
		const registry = new LaneRegistry();
		registry.claim(laneA);
		expect(() => registry.claim({ sessionName: "other", branch: "feat/b", worktreePath: "/wt/a" })).toThrow(
			LaneScopeError,
		);
	});

	test("independent lanes run in parallel without a limit", () => {
		const registry = new LaneRegistry();
		for (let index = 1; index <= 25; index += 1) {
			registry.claim({
				sessionName: `project-issue-${index}-x`,
				branch: `feat/${index}`,
				worktreePath: `/wt/${index}`,
			});
		}
		expect(registry.size).toBe(25);
	});

	test("release lets a replacement session take over the lane", () => {
		const registry = new LaneRegistry();
		registry.claim(laneA);
		registry.release(laneA.sessionName);
		expect(registry.owner("feat/a")).toBeUndefined();
		const next = registry.claim({
			sessionName: "project-issue-1-a-retry",
			branch: "feat/a",
			worktreePath: "/wt/a",
		});
		expect(next.sessionName).toBe("project-issue-1-a-retry");
	});
});

describe("laneSessionKey", () => {
	test("is tmux-safe and decoupled from the branch name", () => {
		const key = laneSessionKey({ kind: "feature", label: "subsessions runtime" });
		expect(key).toBe("project-feature-subsessions-runtime");
		expect(key).not.toContain("/");
	});

	test("sanitises a branch-shaped label instead of emitting slashes", () => {
		expect(laneSessionKey({ kind: "feature", label: "feat/subsession-runtime" })).toBe(
			"project-feature-feat-subsession-runtime",
		);
	});

	test("binds a number once the issue or PR exists", () => {
		expect(laneSessionKey({ kind: "pr", number: 12, label: "admin ui" })).toBe("project-pr-12-admin-ui");
	});

	test("rejects a feature key carrying a number", () => {
		expect(() => laneSessionKey({ kind: "feature", label: "x", number: 3 })).toThrow(LaneScopeError);
	});

	test("rejects a numbered kind without a number", () => {
		expect(() => laneSessionKey({ kind: "issue", label: "x" })).toThrow(LaneScopeError);
	});

	test("rejects a label with no usable characters", () => {
		expect(() => laneSessionKey({ kind: "feature", label: "///" })).toThrow(LaneScopeError);
	});
});
