/**
 * Rebindable-failure classification and failure surfacing for the gjc runtime
 * (issues #13 / #14).
 *
 * Measured problem: the gateway derives ONE idempotency key per origin+epoch
 * (`gajaeway-<instanceId>-<originKey>-e<epoch>`). When the runtime condemns that
 * key, the session.create op fails identically forever and the host goes mute;
 * recovery used to require hand-editing `gateway.db` to bump `epoch`. A poisoned
 * key is not a broken conversation, so the four codes below are classified as
 * REBINDABLE: the remedy is a new key (epoch bump), not a retry.
 *
 * Classification is strictly code-based. Human-readable message text is never
 * matched, because message wording is not a contract.
 */

/**
 * Codes that are permanent for the poisoned idempotency key and absent for a
 * fresh one. Each was observed in the live incident:
 * - `resource_gone`: the session endpoint record is gone.
 * - `spawn_failed`: SDK startup did not complete before the readiness cutoff.
 * - `terminal_uncertain`: startup cleanup could not be proven.
 * - `managed_append_identity_mismatch`: turn-level; the bound session's identity
 *   no longer matches the cwd, so it can never be resumed from here again.
 */
export const REBINDABLE_ERROR_CODES: ReadonlySet<string> = new Set([
	"resource_gone",
	"spawn_failed",
	"terminal_uncertain",
	"managed_append_identity_mismatch",
]);

/**
 * Reviewer-mandated ceiling (gaebal-gajae): silent unbounded epoch growth is
 * worse than being mute, because nobody notices it. Past the cap the gateway
 * fails loudly instead of minting another key.
 */
export const DEFAULT_REBIND_CAP = 3;

/** Message budget for a delivered failure notice: truncated, never erased. */
const MESSAGE_LIMIT = 240;

/** Structured error as reported by the gjc runtime. */
export interface RuntimeErrorDetail {
	readonly code?: string;
	readonly message?: string;
}

/** A gjc failure that carries the runtime's own structured code. */
export class GjcRuntimeError extends Error {
	/** The runtime's error code, when the runtime reported one. */
	readonly code: string | undefined;
	/** The runtime's own message, unwrapped from any gateway framing. */
	readonly runtimeMessage: string | undefined;

	constructor(message: string, detail: RuntimeErrorDetail = {}) {
		super(message);
		this.name = "GjcRuntimeError";
		this.code = detail.code;
		this.runtimeMessage = detail.message;
	}
}

/** Raised when a session/origin has exhausted its rebind budget. */
export class RebindCapExceededError extends GjcRuntimeError {
	constructor(originKey: string, cap: number, cause: string, epoch: number) {
		super(
			`gjc session rebind cap ${cap} reached for ${originKey} (last cause ${cause}, epoch ${epoch}); refusing to mint another session key`,
			{ code: "rebind_cap_exceeded", message: `rebind cap ${cap} reached after ${cause} at epoch ${epoch}` },
		);
		this.name = "RebindCapExceededError";
	}
}

/** True when the remedy for this code is a fresh session key, not a retry. */
export function isRebindableCode(code: string | undefined): boolean {
	return code !== undefined && REBINDABLE_ERROR_CODES.has(code);
}

/** The rebindable code carried by an error, or undefined for a genuine failure. */
export function rebindableCodeOf(error: unknown): string | undefined {
	const code = error instanceof GjcRuntimeError ? error.code : undefined;
	return isRebindableCode(code) ? code : undefined;
}

const CODE_SHAPE = /^[a-z][a-z0-9_]*$/;

/**
 * Pulls the runtime's structured error out of a gjc stdout/stderr blob. The gjc
 * SDK envelope is `{"ok":false,"error":{"code":…,"message":…}}` and the ndjson
 * turn stream carries `{"type":"error","error":{…}}`; both are handled, plus a
 * bare `{"code":…,"message":…}`. Non-JSON noise lines are skipped.
 */
export function extractRuntimeError(text: string): RuntimeErrorDetail | undefined {
	for (const line of text.split("\n")) {
		const trimmed = line.trim();
		if (!trimmed.startsWith("{")) continue;
		let parsed: unknown;
		try {
			parsed = JSON.parse(trimmed);
		} catch {
			continue;
		}
		const detail = runtimeErrorOfEnvelope(parsed);
		if (detail) return detail;
	}
	return undefined;
}

/** Reads the structured error out of one already-parsed gjc envelope object. */
export function runtimeErrorOfEnvelope(envelope: unknown): RuntimeErrorDetail | undefined {
	if (typeof envelope !== "object" || envelope === null) return undefined;
	const record = envelope as { ok?: unknown; error?: unknown; code?: unknown; message?: unknown };
	const nested =
		typeof record.error === "object" && record.error !== null
			? (record.error as { code?: unknown; message?: unknown })
			: undefined;
	const candidate = nested ?? record;
	// A bare object is only an error report when the envelope says so; otherwise a
	// success frame carrying an unrelated `message` field would be misread.
	if (!nested && record.ok !== false) return undefined;
	const code = typeof candidate.code === "string" && CODE_SHAPE.test(candidate.code) ? candidate.code : undefined;
	const message = typeof candidate.message === "string" && candidate.message.length > 0 ? candidate.message : undefined;
	if (!code && !message) return undefined;
	return { ...(code ? { code } : {}), ...(message ? { message } : {}) };
}

/**
 * Secret scrubbing for text that reaches a chat surface. This redacts SECRETS
 * ONLY: a runtime code such as `unsupported_state_version` is the whole
 * diagnosis and is never sensitive, so nothing else is erased.
 */
export function redactSecrets(text: string): string {
	return text
		.replace(
			/((?:token|secret|password|passwd|api[_-]?key|apikey|authorization|bearer)["']?\s*[:=]\s*["']?)[^\s"',}]+/gi,
			"$1[redacted]",
		)
		.replace(/\b(?:sk|rk|pk)-[A-Za-z0-9_-]{8,}/g, "[redacted]")
		.replace(/\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{8,}/g, "[redacted]")
		.replace(/\bxox[abposr]-[A-Za-z0-9-]{8,}/g, "[redacted]")
		.replace(/\bey[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g, "[redacted]");
}

/** The runtime code and message an error should be reported with. */
export function describeFailure(error: unknown): RuntimeErrorDetail & { readonly text: string } {
	const code = error instanceof GjcRuntimeError ? error.code : undefined;
	const raw =
		(error instanceof GjcRuntimeError ? error.runtimeMessage : undefined) ??
		(error instanceof Error ? error.message : String(error));
	const message = redactSecrets(raw).trim();
	const truncated = message.length > MESSAGE_LIMIT ? `${message.slice(0, MESSAGE_LIMIT).trimEnd()}…` : message;
	return {
		...(code ? { code } : {}),
		message: truncated,
		text: code ? `${code}: ${truncated}` : truncated,
	};
}

/**
 * The single string used for BOTH the operator log and the delivered chat
 * notice, so a channel transcript is enough to triage without shell access
 * (#14). The `/new` hint appears only when a rebind is plausibly the remedy.
 */
export function formatFailureNotice(error: unknown): string {
	const described = describeFailure(error);
	const hint = isRebindableCode(described.code) ? " Send /new to rebind this conversation." : "";
	return `[turn failed] ${described.text}${hint}`;
}

/** The subset of the gateway store a rebind needs. */
export interface RebindStore {
	withTransaction<T>(run: () => T): T;
	rebindEpoch(originKey: string): number;
}

/**
 * Owns the epoch-bump budget for rebindable failures. One instance per
 * GjcClient (so one per gateway process), which means the counter spans every
 * attempt for a session/origin rather than resetting per call. A successful
 * bind clears the budget: the origin is demonstrably healthy again.
 */
export class SessionRebinder {
	readonly #store: RebindStore;
	readonly #cap: number;
	readonly #log: (line: string) => void;
	readonly #used = new Map<string, number>();

	constructor(store: RebindStore, cap = DEFAULT_REBIND_CAP, log: (line: string) => void = console.warn) {
		this.#store = store;
		this.#cap = cap;
		this.#log = log;
	}

	get cap(): number {
		return this.#cap;
	}

	/** Rebinds allocated to this origin so far. */
	used(originKey: string): number {
		return this.#used.get(originKey) ?? 0;
	}

	/**
	 * Bumps and persists the epoch for one rebindable failure, logging the
	 * causing code with both epochs so nobody has to reverse engineer "why is
	 * this e7?" later. Throws once the budget is spent.
	 */
	rebind(originKey: string, causeCode: string, fromEpoch: number): number {
		const used = this.used(originKey);
		if (used >= this.#cap) throw new RebindCapExceededError(originKey, this.#cap, causeCode, fromEpoch);
		const toEpoch = this.#store.withTransaction(() => this.#store.rebindEpoch(originKey));
		this.#used.set(originKey, used + 1);
		this.#log(
			`gateway session rebind ${used + 1}/${this.#cap} origin=${originKey} cause=${causeCode} epoch ${fromEpoch} -> ${toEpoch}`,
		);
		return toEpoch;
	}

	/** Clears the budget after a proven-good bind. */
	clear(originKey: string): void {
		this.#used.delete(originKey);
	}
}
