import { describe, expect, test } from "bun:test";
import {
	ALLOWED_OPERATIONS,
	assertControlAllowed,
	classifyOperation,
	DENIED_OPERATIONS,
	OPERATOR_GATED_OPERATIONS,
	PolicyError,
	PROMPT_POLICY,
	TERMINAL_DELIVERABLE,
} from "../src/policy";
import { DEFAULT_SUBSESSION_SKILLS, resolveSkills, SKILL_UNAVAILABLE, SkillUnavailableError } from "../src/skills";

const trusted = (available: readonly string[]) => ({ available, trusted: true });

describe("skill resolution", () => {
	test("returns /skill: directives for allowlisted, exposed skills", () => {
		const resolved = resolveSkills({
			requested: ["ultragoal", "gjc-sdk-operate"],
			surface: trusted(["ultragoal", "gjc-sdk-operate", "ralplan"]),
			mission: "implementation",
		});
		expect(resolved.names).toEqual(["ultragoal", "gjc-sdk-operate"]);
		expect(resolved.directives).toEqual(["/skill:ultragoal", "/skill:gjc-sdk-operate"]);
	});

	test("rejects an unknown skill instead of falling back", () => {
		expect(() =>
			resolveSkills({
				requested: ["make-it-work"],
				surface: trusted(["ultragoal"]),
				mission: "implementation",
			}),
		).toThrow(SkillUnavailableError);
	});

	test("fails closed when the surface is untrusted, without copying anything in", () => {
		try {
			resolveSkills({
				requested: ["ultragoal"],
				surface: { available: ["ultragoal"], trusted: false },
				mission: "implementation",
			});
			throw new Error("should have thrown");
		} catch (error) {
			expect((error as SkillUnavailableError).code).toBe(SKILL_UNAVAILABLE);
			expect((error as Error).message).toMatch(/do not copy skills in/);
		}
	});

	test("an empty surface holds rather than installing defaults", () => {
		expect(() => resolveSkills({ requested: ["ultragoal"], surface: trusted([]), mission: "implementation" })).toThrow(
			/no discoverable skills/,
		);
	});

	test("an allowlisted skill the session does not expose is still refused", () => {
		expect(() =>
			resolveSkills({
				requested: ["ralplan"],
				surface: trusted(["ultragoal"]),
				mission: "planning",
			}),
		).toThrow(/does not expose it/);
	});

	test("autoresearch cannot drive an implementation mission", () => {
		expect(() =>
			resolveSkills({
				requested: ["autoresearch"],
				surface: trusted(["autoresearch"]),
				mission: "implementation",
			}),
		).toThrow(/use ultragoal for implementation/);
	});

	test("autoresearch is allowed for a real research mission", () => {
		const resolved = resolveSkills({
			requested: ["autoresearch"],
			surface: trusted(["autoresearch"]),
			mission: "research",
		});
		expect(resolved.directives).toEqual(["/skill:autoresearch"]);
	});

	test("duplicate requests collapse", () => {
		const resolved = resolveSkills({
			requested: ["ultragoal", "ultragoal"],
			surface: trusted(["ultragoal"]),
			mission: "implementation",
		});
		expect(resolved.names).toEqual(["ultragoal"]);
	});

	test("the default allowlist is the agreed set and excludes autoresearch", () => {
		expect([...DEFAULT_SUBSESSION_SKILLS]).toEqual([
			"gjc-sdk-discover",
			"gjc-sdk-operate",
			"ultragoal",
			"ralplan",
			"deep-interview",
		]);
	});
});

describe("control policy", () => {
	test.each([...ALLOWED_OPERATIONS])("%s is allowed", (operation) => {
		expect(classifyOperation(operation).class).toBe("allowed");
		expect(() => assertControlAllowed(operation)).not.toThrow();
	});

	test.each([...OPERATOR_GATED_OPERATIONS])("%s needs operator approval", (operation) => {
		expect(classifyOperation(operation).class).toBe("operator_gated");
		expect(() => assertControlAllowed(operation)).toThrow(/explicit operator approval/);
		expect(() => assertControlAllowed(operation, { operatorApproval: true })).not.toThrow();
	});

	test.each([...DENIED_OPERATIONS])("%s is denied even with approval", (operation) => {
		expect(classifyOperation(operation).class).toBe("denied");
		expect(() => assertControlAllowed(operation, { operatorApproval: true })).toThrow(PolicyError);
	});

	test("an unlisted operation is denied, not forwarded", () => {
		const classified = classifyOperation("raw.do_whatever");
		expect(classified.class).toBe("denied");
		expect(classified.reason).toMatch(/arbitrary raw operation forwarding is refused/);
	});

	test("a frozen lane refuses even allowed operations", () => {
		expect(() => assertControlAllowed("turn.prompt", { frozen: true })).toThrow(/frozen/);
	});

	test("merge, release, tag and deploy are all denied", () => {
		for (const operation of ["repo.merge", "release.publish", "release.tag", "deploy"]) {
			expect(classifyOperation(operation).class).toBe("denied");
		}
	});

	test("session.delete is operator-gated: refused without approval, never from a worker", () => {
		expect(classifyOperation("session.delete").class).toBe("operator_gated");
		expect(() => assertControlAllowed("session.delete")).toThrow(/operator approval/);
		expect(() => assertControlAllowed("session.delete", { operatorApproval: true })).not.toThrow();
	});

	test("credentials and permissions are denied", () => {
		for (const operation of ["credentials.read", "credentials.write", "config.write", "permissions.write"]) {
			expect(classifyOperation(operation).class).toBe("denied");
		}
	});
});

describe("prompt policy", () => {
	test("states the out-of-scope actions in text", () => {
		expect(PROMPT_POLICY).toMatch(/merging into main/);
		expect(PROMPT_POLICY).toMatch(/release or tag, publishing, or deploying/);
		expect(PROMPT_POLICY).toMatch(/credentials/);
	});

	test("names the terminal deliverable so a worker knows where to stop", () => {
		expect([...TERMINAL_DELIVERABLE]).toEqual([
			"worktree edits",
			"tests",
			"commit",
			"branch push",
			"PR open/update",
			"exact-head CI/review evidence",
		]);
		expect(PROMPT_POLICY).toMatch(/exact-head CI and review evidence/);
	});

	test("requires evidence rather than a claim", () => {
		expect(PROMPT_POLICY).toMatch(/commit SHAs, test output, CI conclusions/);
	});
});
