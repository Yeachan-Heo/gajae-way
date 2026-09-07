/**
 * Authority policy for the subsession controller.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 5/5). Three layers, and the
 * weakest one is never trusted alone:
 *
 *   1. prompt policy - the subsession is told, in text, that merge/release/
 *      publish are out of scope and what its terminal deliverable is;
 *   2. SDK control allowlist - this module. The controller can only issue
 *      statically allowed operations; arbitrary raw forwarding is refused;
 *   3. repository/hosting protection - branch protection, required review/CI and
 *      owner-only merge rights do the real enforcement.
 *
 * A skill name or a sentence in a prompt is not a permission. Layer 2 exists so
 * that a compromised or confused prompt still cannot reach a destructive verb.
 */

export type OperationClass = "allowed" | "operator_gated" | "denied";

export const ALLOWED_OPERATIONS: readonly string[] = [
	"session.create",
	"session.fork",
	"session.resume",
	"session.close",
	"session.list",
	"session.inspect",
	"turn.prompt",
	"turn.status",
	"turn.tail",
	"transcript.list",
	"transcript.body",
	"resource.body",
	"artifact.read",
	"session.last_assistant",
	"session.metadata",
];

export const OPERATOR_GATED_OPERATIONS: readonly string[] = [
	"turn.steer",
	"turn.abort",
	"turn.replace",
	"workflow.gate_answer",
	// Destructive on the broker index, so never reachable from a worker turn
	// (no worker path passes approval). The gateway's own session-index GC is
	// runtime code acting on sessions it provably no longer references and
	// passes explicit approval; without a way to delete, a rotating persona
	// origin grows the index without bound and the broker's session.list
	// cursor budget drains until every id-resolving call fails (jip, 2026-09-06).
	"session.delete",
];

export const DENIED_OPERATIONS: readonly string[] = [
	"repo.merge",
	"release.publish",
	"release.tag",
	"deploy",
	"credentials.read",
	"credentials.write",
	"config.write",
	"permissions.write",
];

export type ClassifyResult = {
	readonly operation: string;
	readonly class: OperationClass;
	readonly reason: string;
};

/**
 * Classifies an operation.
 *
 * Anything unlisted is denied rather than passed through: an allowlist that
 * defaults to forwarding is not an allowlist.
 */
export function classifyOperation(operation: string): ClassifyResult {
	if (ALLOWED_OPERATIONS.includes(operation)) {
		return { operation, class: "allowed", reason: "statically allowed controller operation" };
	}
	if (OPERATOR_GATED_OPERATIONS.includes(operation)) {
		return {
			operation,
			class: "operator_gated",
			reason: "mutates an active turn or answers a workflow gate: requires explicit operator approval",
		};
	}
	if (DENIED_OPERATIONS.includes(operation)) {
		return { operation, class: "denied", reason: "denied by default: owner-gated or destructive" };
	}
	return {
		operation,
		class: "denied",
		reason: "not on the controller allowlist; arbitrary raw operation forwarding is refused",
	};
}

export class PolicyError extends Error {
	readonly operation: string;
	readonly operationClass: OperationClass;

	constructor(operation: string, operationClass: OperationClass, message: string) {
		super(message);
		this.name = "PolicyError";
		this.operation = operation;
		this.operationClass = operationClass;
	}
}

export type ControlContext = {
	/** An operator explicitly approved this specific gated operation. */
	readonly operatorApproval?: boolean;
	/** Mutation freeze from a contamination signal or an uncertain operation. */
	readonly frozen?: boolean;
};

/** Throws unless the operation may be issued in this context. */
export function assertControlAllowed(operation: string, context: ControlContext = {}): void {
	const classified = classifyOperation(operation);

	if (classified.class === "denied") {
		throw new PolicyError(operation, "denied", `${operation} is denied: ${classified.reason}`);
	}
	if (context.frozen === true) {
		throw new PolicyError(
			operation,
			classified.class,
			`mutations are frozen for this lane; ${operation} is refused until the hold is resolved`,
		);
	}
	if (classified.class === "operator_gated" && context.operatorApproval !== true) {
		throw new PolicyError(operation, "operator_gated", `${operation} requires explicit operator approval`);
	}
}

/**
 * Prompt-side policy text (layer 1).
 *
 * Stated as the subsession's terminal deliverable so a worker knows where to
 * stop, rather than being left to infer it and drift into a merge.
 */
export const PROMPT_POLICY = `Scope of this session:
- work only inside the assigned worktree and branch
- run the project's tests for what you changed
- commit, push the branch, and open or update its pull request
- collect exact-head CI and review evidence, and report it

Out of scope, never do these:
- merging into main or any integration branch
- creating a release or tag, publishing, or deploying
- deleting sessions, reading or writing credentials, changing config or permissions
- mutating anything outside this pull request's branch

If a task appears to require an out-of-scope action, stop and report the blocker
with the evidence you have. Report a deliverable only with verifiable evidence:
commit SHAs, test output, CI conclusions.`;

/** The subsession's terminal deliverable, in order. */
export const TERMINAL_DELIVERABLE: readonly string[] = [
	"worktree edits",
	"tests",
	"commit",
	"branch push",
	"PR open/update",
	"exact-head CI/review evidence",
];
