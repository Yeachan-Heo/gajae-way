/**
 * Mutation gate for the admin surface.
 *
 * The admin UI is read-only by default. Anything that changes gateway state has
 * to pass a double gate - an operation allowlist AND an explicit per-request
 * approval - and every attempt is written to an audit trail. The failure this
 * prevents is concrete: an operator clicking around a dashboard must not be able
 * to make the bot speak in a channel or rewrite monitors by accident.
 *
 * `evaluate()` is deliberately unchanged: allowlist, actor, `confirm ===
 * operationId`, audit-before-dispatch. The console layers a *stronger*
 * requirement on top for destructive operations (typing the target object's
 * name, per the allowlist's `confirmToken`); it never relaxes what is here.
 */

/** How much blast radius an operation has. Drives the console's confirmation ceremony. */
export type MutationSeverity = "low" | "medium" | "high";

/**
 * One typed input of an operation. `path` is the dotted location the value
 * takes in the outgoing params object, so a single generic builder serves every
 * operation and no per-operation JSON is ever typed by a human.
 */
export type MutationField = {
	readonly name: string;
	readonly label: string;
	/** `monitor-ref` renders a picker populated from `monitor.list`, never a UUID box. */
	readonly kind: "text" | "monitor-ref" | "path" | "json";
	readonly required: boolean;
	/** Dotted params path; defaults to `name` when absent. */
	readonly path?: string;
	/** Split a comma-separated value into an array before sending. */
	readonly list?: boolean;
	readonly placeholder?: string;
	readonly hint?: string;
};

export type MutationOperation = {
	readonly id: string;
	/** Underlying gateway method this operation is allowed to call. */
	readonly method: string;
	readonly summary: string;
	readonly severity: MutationSeverity;
	readonly fields: readonly MutationField[];
	/**
	 * What the operator must type back before the console will submit. Absent for
	 * additive, low-severity operations - ceremony that cannot fail teaches
	 * click-through. `target-name` is enforced server-side against a live read of
	 * the target, so the token is never the string sitting next to the input.
	 */
	readonly confirmToken?: "target-name";
	/** Fixed params merged under the typed fields (e.g. a trigger discriminant). */
	readonly paramsTemplate?: Readonly<Record<string, unknown>>;
	/** Sentence shown above the confirm field, naming what cannot be undone. */
	readonly consequence?: string;
	/**
	 * Imperative verb for the action button, so it can restate its target:
	 * `Remove weekday-review` rather than `Remove a monitor weekday-review`.
	 */
	readonly action?: string;
};

/**
 * Allowlisted mutations. `chat.send` is deliberately absent: sending messages
 * from an admin console is the one mistake with an irreversible, public effect.
 */
export const DEFAULT_ALLOWLIST: readonly MutationOperation[] = [
	{
		id: "monitor.add",
		method: "monitor.add",
		summary: "Create a monitor",
		severity: "medium",
		paramsTemplate: { trigger: { kind: "cron" } },
		fields: [
			{
				name: "name",
				label: "Monitor name",
				kind: "text",
				required: true,
				placeholder: "weekday-review",
				hint: "How you will refer to it later. Removal asks for this exact string.",
			},
			{
				name: "schedule",
				label: "Cron schedule",
				kind: "text",
				required: true,
				path: "trigger.schedule",
				placeholder: "30 8 * * 1-5",
				hint: "Five fields: minute hour day-of-month month day-of-week.",
			},
			{
				name: "eventTypes",
				label: "Event types",
				kind: "text",
				required: true,
				list: true,
				placeholder: "review.due",
				hint: "Declared up front, never inferred. Comma-separated for more than one.",
			},
		],
		consequence: "A new schedule starts firing as soon as it is created.",
	},
	{
		id: "monitor.remove",
		method: "monitor.remove",
		summary: "Remove a monitor",
		action: "Remove",
		severity: "high",
		confirmToken: "target-name",
		fields: [{ name: "monitorId", label: "Monitor", kind: "monitor-ref", required: true }],
		consequence:
			"This cannot be undone. The monitor and its schedule are deleted. Its past events and authored notes are kept.",
	},
	{
		id: "monitor.test",
		method: "monitor.test",
		summary: "Fire a monitor test event",
		severity: "medium",
		fields: [
			{ name: "monitorId", label: "Monitor", kind: "monitor-ref", required: true },
			{
				name: "eventType",
				label: "Event type",
				kind: "text",
				required: false,
				hint: "Leave empty to use the monitor's first declared type.",
			},
		],
		consequence: "A real event is injected. If the monitor has a channel target, the agent may post to it.",
	},
	{
		id: "ops.backup",
		method: "ops.backup",
		summary: "Write a gateway backup",
		severity: "low",
		fields: [
			{
				name: "path",
				label: "Backup path",
				kind: "path",
				required: true,
				placeholder: "~/.gajaeway/backups/manual.sqlite",
			},
		],
	},
	{
		id: "ops.integrity",
		method: "ops.integrity",
		summary: "Run an integrity check",
		severity: "low",
		fields: [],
	},
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

	get mutationsEnabled(): boolean {
		return this.#enabled;
	}

	/**
	 * Record an attempt the surrounding route refused before the gate could see
	 * it. The audit trail must not have holes just because a stricter check ran
	 * first.
	 */
	async record(entry: Omit<AuditEntry, "at">): Promise<void> {
		await this.#audit?.({ at: this.#now().toISOString(), ...entry });
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
