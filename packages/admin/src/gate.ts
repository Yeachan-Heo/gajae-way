/**
 * Mutation gate for the admin surface.
 *
 * The admin UI is read-only by default. Anything that changes gateway state has
 * to pass a double gate - an operation allowlist AND an explicit per-request
 * approval - and every attempt is written to an audit trail. The failure this
 * prevents is concrete: an operator clicking around a dashboard must not be able
 * to make the bot speak in a channel or rewrite monitors by accident.
 */

export type MutationOperation = {
	readonly id: string;
	/** Underlying gateway method this operation is allowed to call. */
	readonly method: string;
	readonly summary: string;
};

/**
 * Allowlisted mutations. `chat.send` is deliberately absent: sending messages
 * from an admin console is the one mistake with an irreversible, public effect.
 */
export const DEFAULT_ALLOWLIST: readonly MutationOperation[] = [
	{ id: "monitor.add", method: "monitor.add", summary: "Create a monitor" },
	{ id: "monitor.remove", method: "monitor.remove", summary: "Remove a monitor" },
	{ id: "monitor.test", method: "monitor.test", summary: "Fire a monitor test event" },
	{ id: "ops.backup", method: "ops.backup", summary: "Write a gateway backup" },
	{ id: "ops.integrity", method: "ops.integrity", summary: "Run an integrity check" },
];

export type AuditEntry = {
	readonly at: string;
	readonly operationId: string;
	readonly actor: string;
	readonly decision: "allowed" | "rejected";
	readonly reason?: string;
	readonly params?: unknown;
};

export type AuditSink = (entry: AuditEntry) => void | Promise<void>;

export type MutationRequest = {
	readonly operationId: string;
	readonly actor?: string;
	/** Must equal the operation id: a deliberate, non-guessable-by-accident echo. */
	readonly confirm?: string;
	readonly params?: Record<string, unknown>;
};

export type GateDecision =
	| { readonly allowed: true; readonly operation: MutationOperation }
	| { readonly allowed: false; readonly status: number; readonly reason: string };

export type GateOptions = {
	readonly allowlist?: readonly MutationOperation[];
	readonly audit?: AuditSink;
	readonly now?: () => Date;
	/** When false, every mutation is refused regardless of confirmation. */
	readonly mutationsEnabled?: boolean;
};

export class MutationGate {
	readonly #allowlist: Map<string, MutationOperation>;
	readonly #audit: AuditSink | undefined;
	readonly #now: () => Date;
	readonly #enabled: boolean;

	constructor(options: GateOptions = {}) {
		this.#allowlist = new Map((options.allowlist ?? DEFAULT_ALLOWLIST).map((operation) => [operation.id, operation]));
		this.#audit = options.audit;
		this.#now = options.now ?? (() => new Date());
		this.#enabled = options.mutationsEnabled ?? true;
	}

	get operations(): readonly MutationOperation[] {
		return [...this.#allowlist.values()];
	}

	async evaluate(request: MutationRequest): Promise<GateDecision> {
		const actor = request.actor?.trim() || "anonymous";

		if (!this.#enabled) {
			return await this.#reject(request, actor, 403, "mutations are disabled for this deployment");
		}
		const operation = this.#allowlist.get(request.operationId);
		if (!operation) {
			return await this.#reject(request, actor, 404, `operation ${request.operationId} is not allowlisted`);
		}
		if (actor === "anonymous") {
			return await this.#reject(request, actor, 401, "an actor is required for a mutation");
		}
		if (request.confirm !== request.operationId) {
			return await this.#reject(
				request,
				actor,
				428,
				"explicit confirmation is required: echo the operation id in `confirm`",
			);
		}

		await this.#audit?.({
			at: this.#now().toISOString(),
			operationId: operation.id,
			actor,
			decision: "allowed",
			...(request.params === undefined ? {} : { params: request.params }),
		});
		return { allowed: true, operation };
	}

	async #reject(request: MutationRequest, actor: string, status: number, reason: string): Promise<GateDecision> {
		await this.#audit?.({
			at: this.#now().toISOString(),
			operationId: request.operationId,
			actor,
			decision: "rejected",
			reason,
			...(request.params === undefined ? {} : { params: request.params }),
		});
		return { allowed: false, status, reason };
	}
}
