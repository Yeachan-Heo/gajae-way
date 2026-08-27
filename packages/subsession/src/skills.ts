/**
 * Skill selection for subsessions.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 5/5):
 *   - copying or injecting `.gjc/skills` from outside is NOT canonical. The
 *     external controller only speaks broker-bound `gjc sdk session`; discovery
 *     inside the session is GJC's own (user scope `~/.gjc/agent/skills`, project
 *     scope `<project>/.gjc/skills`, gated by `skills.trustUserSkills` /
 *     `skills.trustProjectSkills`).
 *   - a skill is a procedural surface selected in the prompt as `/skill:<name>`.
 *     It is NOT a permission boundary; enforcement lives in the runtime, broker
 *     and host policy.
 *   - an unknown or untrusted skill surface must fail closed as
 *     `skill_unavailable`, never silently fall back to a default or a copy.
 */

export const SKILL_UNAVAILABLE = "skill_unavailable";

/**
 * Skills a work subsession may select, kept separate from the prompt text so the
 * allowlist is auditable on its own.
 */
export const DEFAULT_SUBSESSION_SKILLS: readonly string[] = [
	"gjc-sdk-discover",
	"gjc-sdk-operate",
	"ultragoal",
	"ralplan",
	"deep-interview",
];

/** Allowed only for a genuine research mission. */
export const RESEARCH_ONLY_SKILLS: readonly string[] = ["autoresearch"];

export type MissionKind = "implementation" | "research" | "planning";

export class SkillUnavailableError extends Error {
	readonly code = SKILL_UNAVAILABLE;
	readonly requested: readonly string[];
	readonly detail: string;

	constructor(requested: readonly string[], detail: string) {
		super(`${SKILL_UNAVAILABLE}: ${detail}`);
		this.name = "SkillUnavailableError";
		this.requested = requested;
		this.detail = detail;
	}
}

export type SkillSurface = {
	/**
	 * Skill names the session itself reports as discoverable. An empty or absent
	 * surface is a hold condition, not an invitation to install anything.
	 */
	readonly available: readonly string[];
	readonly trusted: boolean;
};

export type ResolveSkillsInput = {
	readonly requested: readonly string[];
	readonly surface: SkillSurface;
	readonly mission: MissionKind;
	readonly allowlist?: readonly string[];
	readonly researchOnly?: readonly string[];
};

export type ResolvedSkills = {
	readonly names: readonly string[];
	/** `/skill:<name>` tokens to place in the prompt. */
	readonly directives: readonly string[];
};

/**
 * Resolves the skills a prompt may select.
 *
 * Every rejection is loud: an unknown name, an untrusted surface, a skill the
 * session does not actually expose, or `autoresearch` on an implementation
 * mission. Silent degradation here would hand a worker a different procedure
 * than the operator authorised.
 */
export function resolveSkills(input: ResolveSkillsInput): ResolvedSkills {
	const allowlist = new Set(input.allowlist ?? DEFAULT_SUBSESSION_SKILLS);
	const researchOnly = new Set(input.researchOnly ?? RESEARCH_ONLY_SKILLS);

	if (!input.surface.trusted) {
		throw new SkillUnavailableError(
			input.requested,
			"the session skill surface is not trusted; recover through the runtime's own discovery/config, do not copy skills in",
		);
	}
	if (input.surface.available.length === 0) {
		throw new SkillUnavailableError(
			input.requested,
			"the session reports no discoverable skills; hold for an operator instead of installing defaults",
		);
	}

	const available = new Set(input.surface.available);
	const names: string[] = [];

	for (const requested of input.requested) {
		if (researchOnly.has(requested)) {
			if (input.mission !== "research") {
				throw new SkillUnavailableError(
					input.requested,
					`${requested} is a research workflow and must not drive an ${input.mission} mission; use ultragoal for implementation`,
				);
			}
		} else if (!allowlist.has(requested)) {
			throw new SkillUnavailableError(input.requested, `${requested} is not on the skill allowlist`);
		}
		if (!available.has(requested)) {
			throw new SkillUnavailableError(
				input.requested,
				`${requested} is allowlisted but the session does not expose it`,
			);
		}
		if (!names.includes(requested)) {
			names.push(requested);
		}
	}

	return { names, directives: names.map((name) => `/skill:${name}`) };
}
