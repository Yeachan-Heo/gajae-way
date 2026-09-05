/**
 * Rebindable-failure classification and failure surfacing for the gjc runtime
 * (issues #13 / #14).
 *
 * Measured problem: the gateway derives ONE idempotency key per origin+epoch
 * (`gajaeway-<instanceId>-<originKey>-e<epoch>`). When the runtime condemns that
 * key, the session.create op fails identically forever and the host goes mute;
 * recovery used to require hand-editing `gateway.db` to bump `epoch`. A poisoned
 * key is not a broken conversation, so the codes below are classified as
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
 * - `managed_append_identity_mismatch`: the bound session identity
 *   no longer matches the cwd, so it can never be resumed from here again.
 * - `resume_unusable`: the runtime no longer holds the bound session id, so the
 *   session cannot be re-attached from here (measured on the former per-turn
 *   process path, where the runtime raised an uncaught exception with no
 *   structured code and every monitor tick failed identically until `gateway.db`
 *   was hand-edited). Kept as a bind-time classification; it is never inferred
 *   from message text.
 */
export const REBINDABLE_ERROR_CODES: ReadonlySet<string> = new Set([
	"resource_gone",
	"spawn_failed",
	"terminal_uncertain",
	"managed_append_identity_mismatch",
	"resume_unusable",
	// Measured 2026-09-02 (집가재): the runtime remembers the key with an earlier
	// request body (readinessTimeoutMs added on stable) and rejects every
	// session.create for that origin+epoch forever. The key is dead; mint a new one.
	"idempotency_conflict",
]);

/** The code the gateway synthesizes for a child that died before the turn began. */
export const RESUME_UNUSABLE_CODE = "resume_unusable";

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

/**
 * True when the remedy for this code is a fresh session key, not a retry.
 * Membership is exact after the same narrow normalization the reporting path
 * uses, so classification and the delivered notice can never disagree.
 */
export function isRebindableCode(code: string | undefined): boolean {
	const normalized = normalizeCode(code);
	return normalized !== undefined && REBINDABLE_ERROR_CODES.has(normalized);
}

/** The rebindable code carried by an error, or undefined for a genuine failure. */
export function rebindableCodeOf(error: unknown): string | undefined {
	const code = error instanceof GjcRuntimeError ? normalizeCode(error.code) : undefined;
	return isRebindableCode(code) ? code : undefined;
}

/**
 * Codes are reported, not filtered. A code is only ever CLASSIFIED by exact
 * membership of REBINDABLE_ERROR_CODES, so an unfamiliar shape can never widen
 * classification — but it must still reach the operator, because the code is the
 * whole diagnosis (#14). Control characters are stripped and the length is
 * bounded so a hostile or runaway code cannot flood a chat surface.
 *
 * Normalization is deliberately narrow: surrounding whitespace and control
 * characters are removed, because `" resource_gone"` is the same code with
 * transport noise. Case is NOT folded — `RESOURCE_GONE` is a different code and
 * is treated as a genuine failure rather than assumed to be a rebindable one.
 */
const CODE_LIMIT = 64;

function normalizeCode(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const cleaned = stripControl(value).trim();
	if (!cleaned) return undefined;
	return cleaned.length > CODE_LIMIT ? `${cleaned.slice(0, CODE_LIMIT)}…` : cleaned;
}

/**
 * Removes transport control characters so they cannot be used to smuggle a
 * secret past the redactor: `sk_live_\u0000deadbeef…` matches no key pattern
 * while the NUL is present, and stripping it afterwards would reassemble the
 * key in the clear. Normalization therefore always runs BEFORE redaction.
 * Newlines and tabs are kept for messages, which carry real structure; a code is
 * a single token and keeps nothing.
 */
function stripControl(text: string, keepWhitespace = false): string {
	return text.replace(/\p{C}/gu, (character) =>
		keepWhitespace && (character === "\n" || character === "\t") ? character : "",
	);
}

/**
 * Pulls the runtime's structured error out of a gjc stdout/stderr blob. Only a
 * declared error frame counts: the gjc SDK envelope `{"ok":false,…}` or an
 * ndjson `{"type":"error",…}`. Non-JSON noise lines are skipped.
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

/**
 * Reads the structured error out of one already-parsed gjc envelope.
 *
 * The frame must DECLARE itself an error (`ok:false` or `type:"error"`). A
 * nested `error` object is not sufficient: a per-tool failure frame
 * (`{"type":"tool_execution_end","error":{…}}`) reports a tool that failed, not
 * a condemned session key, and reading it as one caused spurious epoch bumps and
 * turn replays.
 */
export function runtimeErrorOfEnvelope(envelope: unknown): RuntimeErrorDetail | undefined {
	if (typeof envelope !== "object" || envelope === null) return undefined;
	const record = envelope as { ok?: unknown; type?: unknown; error?: unknown; code?: unknown; message?: unknown };
	if (record.ok !== false && record.type !== "error") return undefined;
	const nested =
		typeof record.error === "object" && record.error !== null
			? (record.error as { code?: unknown; message?: unknown })
			: undefined;
	const candidate = nested ?? record;
	const code = normalizeCode(candidate.code);
	const message = typeof candidate.message === "string" && candidate.message.length > 0 ? candidate.message : undefined;
	if (!code && !message) return undefined;
	return { ...(code ? { code } : {}), ...(message ? { message } : {}) };
}

/**
 * Secret scrubbing for text that reaches a chat surface.
 *
 * Two earlier designs failed in opposite directions, so this one is deliberately
 * boring and explicit. Guessing from the VALUE's shape both erased
 * `max_tokens=200000` and delivered `secret=deadbeefcafe`; guessing from an
 * affixed key word erased paths, UUIDs, SHAs and compound codes. A credential is
 * recognised by exactly two things:
 *
 * 1. an EXACT credential key name (`secret=`, `authorization:`,
 *    `AWS_SECRET_ACCESS_KEY=`), delimiter-bounded so `max_tokens`,
 *    `token_count` and `auth_expired` are not credential keys at all; and
 * 2. a vendor SHAPE with its own discriminator (`sk_live_…`, `ghp_…`, `xox…`,
 *    a JWT triplet, a `Bearer`/`Basic` header value).
 *
 * Everything else survives, because a runtime code or a file path is the whole
 * diagnosis and #14 exists to deliver it.
 */
export function redactSecrets(text: string): string {
	// A BRACKETED value is handled first via the bounded scanner (a runtime that
	// dumps `{"secrets":[…]}` must not deliver every entry, and an over-long or
	// unterminated array must redact WHOLE rather than leak past the bound).
	const bracketed = redactBracketedCredentials(text);
	return (
		bracketed
			// Header form FIRST: the key=value rule would otherwise consume only the
			// scheme word and leave the token itself in the clear.
			.replace(/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}/gi, "$1 [redacted]")
			// An exact credential key, in three more value shapes. QUOTED may contain
			// whitespace and falls back to end of line, because an unbalanced quote
			// must never mean "redact nothing" — then the two unquoted forms.
			.replace(CREDENTIAL_QUOTED, (match, key: string, quote: string, value: string, close: string | undefined) =>
				isDiagnosticWord(value) ? match : `${key}${quote}[redacted]${close ?? ""}`,
			)
			// A human passphrase can contain spaces unquoted, and leaving every word
			// after the first is partial disclosure of exactly the shape that needs
			// protecting most, so these keys consume the rest of the line.
			.replace(PASSPHRASE_TO_END_OF_LINE, (match, key: string, value: string) =>
				isDiagnosticWord(value.trim()) ? match : `${key}[redacted]`,
			)
			.replace(CREDENTIAL_BARE, (match, key: string, value: string) =>
				isDiagnosticWord(value) ? match : `${key}[redacted]`,
			)
			// Vendor shapes carrying their own discriminator: matched anywhere, since
			// a leading word character used to defeat them and stripping a zero-width
			// space could manufacture that adjacency.
			.replace(/(?:sk|pk|rk)[-_](?:live|test|proj|ant|api)[-_][A-Za-z0-9_-]{8,}/gi, "[redacted]")
			.replace(/(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{8,}/g, "[redacted]")
			.replace(/xox[abposr]-[A-Za-z0-9-]{8,}/g, "[redacted]")
			.replace(/\bey[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g, "[redacted]")
			// An undiscriminated two-letter prefix additionally requires a DIGIT in
			// the token. Real keys in these families always carry one; ordinary
			// identifiers such as `sk_migration_runner_failed`, `pk_index_rebuild`
			// and `rk_queue_drain_timeout` never do, and erasing them destroyed the
			// whole diagnosis.
			.replace(/\b(?:sk|pk|rk)[-_](?=[A-Za-z0-9_-]*[0-9])[A-Za-z0-9_-]{12,}/gi, "[redacted]")
	);
}

/**
 * Key names whose VALUE is a credential by definition. Bounded by a delimiter or
 * string edge on both sides, so `max_tokens`, `token_count`, `auth_expired` and
 * `oauth` are not matches: their values are limits, counts and states.
 */
const CREDENTIAL_KEYS = [
	"secret",
	"secrets",
	"password",
	"passwd",
	"passphrase",
	"token",
	"api_key",
	"apikey",
	"api-key",
	"access_key",
	"secret_key",
	"secret_access_key",
	"private_key",
	"client_secret",
	"access_token",
	"refresh_token",
	"id_token",
	"session_token",
	"auth_token",
	"bearer_token",
	"credential",
	"credentials",
	"authorization",
];
/**
 * `access_token` -> `accessToken`. JSON and JS runtimes spell these keys in
 * camelCase at least as often as snake_case, and a case-insensitive match cannot
 * see a word boundary expressed only by capitalization.
 *
 * Expanded into the fixed alternation rather than matched with a generic
 * `(?:[a-z0-9]+(?=[A-Z]))*?` prefix: that shape nests two quantifiers and took
 * 6 seconds on a 56-character input, which is a denial-of-service hazard in a
 * function that parses untrusted runtime output.
 */
function camelCaseKey(key: string): string {
	return key.replace(/[_-](.)/g, (_match, character: string) => character.toUpperCase());
}

/**
 * `[vendor_prefix_]<credential key>[_suffix][:=] value`. Both affixes are
 * separator-delimited and the suffix vocabulary is credential-only, which is
 * what makes `AWS_SECRET_ACCESS_KEY`, `GITHUB_TOKEN` and `my_api_key_value`
 * match while `max_tokens`, `token_count` and `auth_expired` do not.
 *
 * The left boundary includes URL punctuation (`?`, `&`, `#`, `/`, `;`): a
 * query-string credential is the most common way a real HTTP failure line
 * carries one, and `?api_key=…` was not matching at all.
 */
const CREDENTIAL_KEY = `(?:^|[\\s,{(\\["'?&#/;])(?:[A-Za-z0-9]+[_-])*?(?:${[...CREDENTIAL_KEYS, ...CREDENTIAL_KEYS.map(camelCaseKey)].join("|")})(?:[_-](?:value|key|token|secret|string|data|b64|base64))*["']?`;
/**
 * `key: [ … ]` — a list of credentials must not be delivered entry by entry.
 * Handled by `redactBracketedCredentials`: a linear, quote/escape/depth-aware
 * scan (no work cap that could become an egress boundary), redacting through
 * the matching outer close — or end-of-diagnostic if none exists.
 */
const CREDENTIAL_BRACKET_OPEN = new RegExp(`(${CREDENTIAL_KEY}\\s*[:=]\\s*)\\[`, "gi");

/**
 * Redacts a credential-keyed bracketed array with an UNBOUNDED linear,
 * quote/escape/bracket-depth-aware scan. Redaction ends only at the matching
 * outer close; unterminated or malformed nesting redacts through end-of-
 * diagnostic. Linear with no backtracking, so there is no DoS concern.
 */
function redactBracketedCredentials(text: string): string {
	const open = new RegExp(CREDENTIAL_BRACKET_OPEN.source, "gi");
	let result = "";
	let copied = 0;
	for (let match = open.exec(text); match !== null; match = open.exec(text)) {
		const start = match.index + match[0].length;
		let index = start;
		let quote: string | null = null;
		// Bracket DEPTH outside quotes: a nested array's `]` must not end the
		// redaction; only the matching outer close does. Unterminated or
		// malformed nesting redacts to the end of the diagnostic (fail closed).
		let depth = 1;
		while (index < text.length && depth > 0) {
			const character = text[index];
			if (quote !== null) {
				if (character === "\\") index++;
				else if (character === quote) quote = null;
			} else if (character === '"' || character === "'") quote = character;
			else if (character === "[") depth++;
			else if (character === "]") depth--;
			index++;
		}
		const consumed = (index >= text.length ? text.length : index) - start;
		result += text.slice(copied, match.index) + `${match[1]}[redacted]`;
		copied = consumed >= text.length ? text.length : start + consumed;
		open.lastIndex = copied;
	}
	return result + text.slice(copied);
}

/** `key: "value with spaces"`; the closing quote is optional so it fails closed. */
const CREDENTIAL_QUOTED = new RegExp(`(${CREDENTIAL_KEY}\\s*[:=]\\s*)(["'])([^"'\\n]*)(["'])?`, "gi");
/**
 * `password=correct horse battery staple` — a human passphrase is written with
 * spaces and unquoted, so for these keys only the value runs to end of line.
 */
const PASSPHRASE_TO_END_OF_LINE =
	/((?:^|[\s,{(["'?&#/;])(?:[A-Za-z0-9]+[_-])*?(?:password|passwd|passphrase)\s*[:=]\s*)([^\n]+)/gi;
/** `key=value` — one token, stopped by whitespace or URL/JSON punctuation. */
const CREDENTIAL_BARE = new RegExp(`(${CREDENTIAL_KEY}\\s*[:=]\\s*)([^\\s"',}&#]+)`, "gi");

/**
 * State words a runtime legitimately puts where a credential would go, so
 * `token=expired` and `credential: unrecoverable` stay readable. Named
 * explicitly rather than inferred from length or character classes, because a
 * short lowercase value is otherwise indistinguishable from a weak key:
 * `secret=short` must be redacted, `secret=missing` must not.
 */
const CREDENTIAL_STATE_WORDS = new Set([
	"absent",
	"denied",
	"failed",
	"forbidden",
	"insufficient",
	"mismatch",
	"timeout",
	"unavailable",
	"empty",
	"expired",
	"invalid",
	"malformed",
	"missing",
	"none",
	"null",
	"redacted",
	"rejected",
	"required",
	"revoked",
	"unauthenticated",
	"unauthorized",
	"undefined",
	"unknown",
	"unrecoverable",
	"unset",
	"unsupported",
]);

/**
 * The one exemption for an exact credential key: a value that is plainly a CODE
 * or a named state rather than a credential. Runtimes do use `authorization` and
 * `credential` as field names for a reason string, and erasing
 * `authorization: insufficient_scope_for_this_operation` destroys the whole
 * diagnosis #14 exists to deliver.
 */
function isDiagnosticWord(value: string): boolean {
	// A credential can be dressed as snake_case (`secret=missing_but_actually_
	// deadbeefcafe`), so a long hex run inside the value disqualifies it: real
	// codes do not carry twelve hex characters in a row.
	if (/[0-9a-f]{12,}/i.test(value)) return false;
	if (value.length <= 64 && /^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$/.test(value)) return true;
	return CREDENTIAL_STATE_WORDS.has(value.toLowerCase());
}

/**
 * The first candidate that survives control-character stripping, redaction and
 * trimming as non-empty. A child killed mid-write can emit a buffer of control
 * characters, which must not pass for a diagnosis.
 */
function firstDiagnosis(...candidates: (string | undefined)[]): string | undefined {
	for (const candidate of candidates) {
		if (candidate === undefined) continue;
		const cleaned = sanitizeDiagnostic(candidate);
		if (cleaned) return cleaned;
	}
	return undefined;
}

/**
 * The one sanitizer for error text that is persisted or logged, not just
 * delivered: control-character stripping FIRST (so a split secret cannot be
 * reassembled around NULs), then redaction, then trim. Generic catches log
 * `error.message` verbatim, so anything embedded in a throwable must pass here.
 */
export function sanitizeDiagnostic(text: string): string {
	return redactSecrets(stripControl(text, true)).trim();
}

/**
 * The runtime code and message an error should be reported with. Truncated, and
 * never erased: a killed or crashed child produces an empty runtime message, and
 * reporting that as a bare `[turn failed]` is precisely the zero-diagnosis line
 * #14 exists to remove, so the gateway's own framing is used as the fallback.
 */
function describeFailure(error: unknown): RuntimeErrorDetail & { readonly text: string } {
	// The code reaches a chat surface too, so it is normalized, then redacted, then
	// bounded — a secret pasted into a code field must not ride along, and control
	// characters must not be able to smuggle it past the redactor.
	const normalizedCode = error instanceof GjcRuntimeError ? normalizeCode(error.code) : undefined;
	const code = normalizedCode ? redactSecrets(normalizedCode) : undefined;
	const message =
		firstDiagnosis(
			error instanceof GjcRuntimeError ? error.runtimeMessage : undefined,
			error instanceof Error ? error.message : String(error),
		) ?? "the runtime produced no diagnosis";
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
	// The cap being spent is the one failure where a rebind IS the remedy but the
	// gateway will no longer perform it, so the manual hint is what is left.
	const remediable = isRebindableCode(described.code) || described.code === "rebind_cap_exceeded";
	// Separated by a period, so the hint cannot read as part of a truncated
	// runtime message on the branch's headline surface.
	const hint = remediable ? `${/[.!?]$/.test(described.text) ? "" : "."} Send /new to rebind this conversation.` : "";
	return `[turn failed] ${described.text}${hint}`;
}

/**
 * The subset of the gateway store a rebind needs. `metaGet`/`metaSet` are
 * optional durable counters: when the store provides them the budget survives
 * a gateway restart; when absent the rebinder degrades to process-local
 * counting (correct, just less conservative).
 */
export interface RebindStore {
	withTransaction<T>(run: () => T): T;
	mutateEpoch(
		originKey: string,
		input: import("../store/epoch-mutation").EpochMutationInput,
	): { fromEpoch: number; toEpoch: number };
	metaGet?(key: string): string | undefined;
	metaSet?(key: string, value: string): void;
}

/**
 * Owns the epoch-bump budget for rebindable failures. One instance per
 * GjcClient (so one per gateway process), which means the counter spans every
 * attempt for a session/origin rather than resetting per call.
 *
 * The budget is cleared ONLY by a proven-good TURN, never by a successful
 * create. A fresh idempotency key always creates successfully, so clearing on
 * create made the cap unreachable for a recurring turn-level failure and let the
 * epoch grow one bump per message — the silent growth the cap exists to stop.
 *
 * The cap bounds CONSECUTIVE rebinds: a turn that completes unaided is genuine
 * evidence of health and clears it, because a lifetime quota recoverable only by
 * `/new` would be worse. That leaves one shape the cap cannot bound — a failure
 * that alternates with clean turns grows the epoch at half rate while every
 * consecutive count reads `1/3`. The lifetime total is therefore logged on every
 * rebind, so that pattern is visible instead of silent; "nobody notices" is the
 * actual harm the guard exists to prevent.
 *
 * The consecutive budget is DURABLE when the store exposes meta counters: it
 * survives a gateway restart, so a restart cannot act as an unauthorized cap
 * reset; only a proven-good turn or `/new` restores it. Lifetime totals are
 * durable for the same reason. Without meta support the counters degrade to
 * process-local (documented degradation, never silently claimed as durable).
 */
export class SessionRebinder {
	readonly #store: RebindStore;
	readonly #cap: number;
	readonly #log: (line: string) => void;
	readonly #used = new Map<string, number>();
	/** Never cleared by health: the total this process has ever spent per origin. */
	readonly #lifetime = new Map<string, number>();
	/** Origins hydrated from durable counters so far (unused when the store has no meta). */
	readonly #hydrated = new Set<string>();
	/**
	 * Origins whose durable counter is corrupted: rebinds are refused (the
	 * operator sees the cap-exceeded notice) until /new rewrites the counter.
	 */
	readonly #corrupt = new Set<string>();

	constructor(store: RebindStore, cap = DEFAULT_REBIND_CAP, log: (line: string) => void = console.warn) {
		this.#store = store;
		this.#cap = cap;
		this.#log = log;
	}
	#durable(originKey: string, used: number, lifetime: number): void {
		if (!this.#store.metaSet) return;
		this.#store.metaSet(`rebind_budget:${originKey}`, JSON.stringify({ used, lifetime }));
	}

	#countersFor(originKey: string): { used: number; lifetime: number } {
		if (this.#store.metaGet && !this.#hydrated.has(originKey)) {
			this.#hydrated.add(originKey);
			try {
				// Absence (`undefined`) is the only clean "no counter yet" state: an
				// EMPTY or malformed stored value is corruption, not absence, and must
				// fail closed rather than reset the budget to zero.
				const raw = this.#store.metaGet(`rebind_budget:${originKey}`);
				if (raw !== undefined) {
					const parsed = JSON.parse(raw) as { used?: unknown; lifetime?: unknown };
					if (
						parsed &&
						Number.isInteger(parsed.used) &&
						(parsed.used as number) >= 0 &&
						Number.isInteger(parsed.lifetime) &&
						(parsed.lifetime as number) >= 0
					) {
						// Seed the in-memory maps so a later clear()/rebind() preserves
						// the durable audit total instead of restarting it at 1.
						const used = parsed.used as number;
						const lifetime = Math.max(parsed.used as number, parsed.lifetime as number);
						this.#used.set(originKey, used);
						this.#lifetime.set(originKey, lifetime);
						return { used, lifetime };
					}
					this.#corrupt.add(originKey);
					this.#log(
						`gateway rebind budget for ${originKey} is CORRUPT; rebinds are blocked until /new rewrites the counter`,
					);
				}
			} catch {
				// Malformed JSON counts as corruption, not absence: fail closed, as above.
				this.#corrupt.add(originKey);
				this.#log(
					`gateway rebind budget for ${originKey} is CORRUPT (unreadable); rebinds are blocked until /new rewrites the counter`,
				);
			}
		}
		return {
			used: this.#corrupt.has(originKey) ? this.#cap : (this.#used.get(originKey) ?? 0),
			lifetime: this.#lifetime.get(originKey) ?? 0,
		};
	}

	/**
	 * Bumps and persists the epoch for one rebindable failure, logging the
	 * causing code with both epochs so nobody has to reverse engineer "why is
	 * this e7?" later. Throws once the budget is spent.
	 */
	rebind(originKey: string, causeCode: string, fromEpoch: number): number {
		const counters = this.#countersFor(originKey);
		if (counters.used >= this.#cap) throw new RebindCapExceededError(originKey, this.#cap, causeCode, fromEpoch);
		const used = counters.used + 1;
		// Lifetime is its own monotonic counter and must NOT be tied to the
		// consecutive count: a proven-good turn clears `used` but never history.
		const lifetime = counters.lifetime + 1;
		// ONE durable transition: the epoch bump and its counter commit together,
		// so a crash can never spend an epoch whose counter was not incremented
		// (that split would let a restart allocate another rebind past the cap).
		const toEpoch = this.#store.withTransaction(() => {
			const epoch = this.#store.mutateEpoch(originKey, {
				scope: originKey.startsWith("monitor/") ? "monitor" : originKey.startsWith("work/") ? "work" : "persona",
				reason: "create_key_poisoned",
				cause: { kind: "retirement" },
			}).toEpoch;
			this.#durable(originKey, used, lifetime);
			return epoch;
		});
		this.#used.set(originKey, used);
		this.#lifetime.set(originKey, lifetime);
		this.#log(
			`gateway session rebind ${used}/${this.#cap} origin=${originKey} cause=${causeCode} epoch ${fromEpoch} -> ${toEpoch} lifetime=${lifetime}`,
		);
		return toEpoch;
	}

	/** Clears the budget after a proven-good turn, or an explicit operator reset. */
	clear(originKey: string): void {
		// Hydrate FIRST (so a restart inherits the durable lifetime), then lift the
		// corruption hold — /new is the explicit operator rewrite of a corrupt
		// counter, and must not re-read the value it is about to replace.
		const { lifetime } = this.#countersFor(originKey);
		this.#used.delete(originKey);
		this.#corrupt.delete(originKey);
		this.#durable(originKey, 0, lifetime);
	}
}
