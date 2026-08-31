import type { GjcModelSelection } from "../config";
import type { GatewayDatabase } from "../store/db";
import {
	DEFAULT_REBIND_CAP,
	extractRuntimeError,
	GjcRuntimeError,
	type RuntimeErrorDetail,
	rebindableCodeOf,
	runtimeErrorOfEnvelope,
	SessionRebinder,
	sanitizeDiagnostic,
} from "./rebind";

export interface TurnProgress {
	/** Tool executions the turn has started so far. */
	readonly toolCalls: number;
	/** Output tokens so far: exact per completed message, estimated between. */
	readonly outputTokens: number;
}

/**
 * Replaces gjc's default coding-assistant identity (owner directive: gajaeway is
 * a GENERIC personal agent, and the default made every reply read like a coding
 * CLI report). Persona identity itself arrives via the appended workspace files
 * (SOUL.md / AGENTS.md / USER.md); this base only sets the register and keeps
 * full tool capability.
 */
export const GENERIC_AGENT_SYSTEM_PROMPT = [
	"You are a general-purpose personal agent living in chat surfaces (Discord, Telegram, a local console).",
	"You are NOT a coding CLI assistant. Do not produce engineering status reports, verification ceremony, commit/file-path narration, or headed markdown documents unless the conversation genuinely calls for them. Answer like a person in a chat: direct, natural, sized to the message.",
	"You still have full tool access and may do real work (files, shell, code, web) whenever the conversation needs it — capability stays, the coding-assistant register goes.",
	"Keep replies chat-sized: short messages, one thought at a time, like a person typing. When several inbound messages are folded into your turn, do NOT answer them as one consolidated report — address what matters, briefly. To send multiple separate messages, put a line containing exactly [BREAK] between the parts (at most 5 parts); each part is delivered as its own chat message.",
	"In rooms with several people, address people the way humans do — mix the mechanisms: start a part with [REPLY:<msg id>] (ids appear in the message headers) to reply-thread to that specific message, and use <@author id> to mention someone. Reply-thread when answering something said a while ago or when several threads are running; mention when calling someone into the conversation; plain text when the flow is obvious.",
	"Sometimes the honest reply is an emoji, not a sentence: open your reply with [REACT:<emoji>] (optionally [REACT:<emoji>@<msg id>]) to react to a message on the platform. With nothing after the token you acknowledge without speaking; text after it is still sent. Only the allowlisted emoji listed in the conversation notice work.",
	"Your actual identity, voice, and standing instructions are defined by the appended persona documents (SOUL.md, AGENTS.md, USER.md) and always take precedence over this base note.",
].join("\n");

/** Per-call session/turn options for worker sessions (delegated coding work). */
export interface TurnOptions {
	/** Working directory override (worker sessions run in repo checkouts, not the persona workspace). */
	readonly cwd?: string;
	/** Keep gjc's own coding-assistant system prompt instead of the generic-agent override. */
	readonly codingRegister?: boolean;
	/**
	 * Environment overrides for the spawned gjc process.
	 *
	 * Used by `session.attach` to pin the native state root, so the session is
	 * created in exactly the store the interactive child will later resume from.
	 * Without it the daemon's own environment decides, and a child that resolved a
	 * different root dies with `Session "<id>" not found`.
	 */
	readonly env?: Readonly<Record<string, string>>;
	/**
	 * Called once per completed assistant message while the turn is still
	 * running, so callers can deliver intermediate replies of a long agentic
	 * turn instead of staying silent until the process exits.
	 */
	readonly onAssistantText?: (text: string) => void;
	/** Rebuilds epoch-bound trusted preamble after an automatic session rebind. */
	readonly systemPreambleForEpoch?: (epoch: number) => string | Promise<string>;
}

export interface GjcPort {
	ensureSession(originKey: string, epoch?: number, options?: TurnOptions): Promise<{ sessionId: string }>;
	sendTurn(
		sessionId: string,
		text: string,
		systemPreamble?: string,
		onProgress?: (progress: TurnProgress) => void,
		options?: TurnOptions,
	): Promise<string>;
	/**
	 * Forgets an origin's rebind budget after an explicit operator reset (`/new`),
	 * which is the manual form of the same remedy. Required, not optional: the
	 * server wiring that makes the `/new` remedy real must not be able to become a
	 * silent no-op behind an absent method.
	 */
	forgetRebinds(originKey: string): void;
}

export function gjcModelArgs(model: GjcModelSelection | undefined): readonly string[] {
	if (!model) return [];
	return typeof model === "string" ? ["--model", model] : ["--mpreset", model.preset];
}

/**
 * Upper bound on the assembled `--append-system-prompt` payload.
 *
 * argv has an OS length ceiling (`E2BIG`). Failing closed here keeps the lease
 * from being handed out alongside an argv that `spawn` cannot exec.
 * `SESSION_BOOTSTRAP_MAX_BYTES` already bounds only the bootstrap portion.
 */
export const ATTACH_PREAMBLE_MAX_BYTES = 32_768;

/** Wrapper-owned: consumed by the CLI, never present on child argv. */
const WRAPPER_OWNED_FLAGS = new Set(["--socket", "--new"]);
/** Forwarded, presence-only (consumes no following token). */
const FORWARDED_PRESENCE_FLAGS = new Set(["--thinking"]);
/**
 * Forwarded, value-taking (flag AND value reach the child).
 *
 * `--worktree`/`-w` are deliberately ABSENT because they are WRAPPER-OWNED, not
 * refused and not forwarded. Native gjc chdirs into a worktree it names itself
 * (observed suffix `hazard-branch-f6ddf077`) BEFORE resolving the session, so a
 * forwarded flag makes the gateway-bound session read as a different project and
 * the TUI offers to fork it. The gateway instead prepares the worktree with plain
 * `git worktree add`, binds the session in that directory, and spawns the child
 * there with no worktree flag — measured to resume cleanly with no fork prompt.
 */
const FORWARDED_VALUE_FLAGS = new Set(["--model", "--mpreset"]);

/** Wrapper-owned and value-taking: consumed here, acted on by the gateway. */
const WRAPPER_OWNED_VALUE_FLAGS = new Set(["--worktree", "-w"]);
/**
 * Refused, value-taking. The value is CONSUMED as part of the refusal so it
 * cannot leak through as a positional (e.g. `--resume ID` must not put `ID` on
 * child argv).
 */
const REFUSED_VALUE_FLAGS = new Set([
	"--resume",
	"-r",
	"--session-dir",
	"--append-system-prompt",
	"--mode",
	"--system-prompt",
]);
/** Refused, presence-only. */
const REFUSED_PRESENCE_FLAGS = new Set(["-p", "--print", "--continue", "-c", "--fork", "--no-session"]);

function looksLikeFlag(token: string): boolean {
	return token.startsWith("-") && token !== "-" && token !== "--";
}

/**
 * The ONE definition of a blank value: absent, empty, or whitespace-only.
 *
 * Both value forms must agree on this. They previously diverged — the separate
 * token was trimmed while the inline `--model=   ` form was only compared to
 * `""` — which let a whitespace-only selector through.
 */
function isBlankValue(value: string | undefined): boolean {
	return value === undefined || value.trim() === "";
}

/**
 * True when `token` can serve as a flag's value in the SEPARATE-token form.
 *
 * Rejects a blank token plus the `--` separator and any other flag, none of
 * which are values: `--model ""` or `--model --` would otherwise hand gjc a
 * selector it cannot resolve.
 */
function isUsableValue(token: string | undefined): token is string {
	if (token === undefined) return false;
	if (token === "--" || looksLikeFlag(token)) return false;
	return !isBlankValue(token);
}

export interface GjcWrapperFlagClassification {
	/** Tokens (flag and value) that go to the child. */
	readonly forwarded: string[];
	/** Refused tokens, named back to the operator. Non-empty means fail-closed. */
	readonly refused: string[];
	/** Wrapper-owned tokens seen and dropped. */
	readonly wrapperOwned: string[];
	/** True when the operator supplied a model selector, which suppresses `config.model`. */
	readonly operatorModel: boolean;
	/** Reasons for refusals that are unsupported capabilities rather than policy. */
	readonly unsupported: string[];
	/**
	 * Worktree the operator asked for, when `--worktree`/`-w` was given.
	 *
	 * `undefined` means no worktree flag. A present-but-empty branch (bare
	 * `--worktree`) means "a managed worktree, name it for me", which the gateway
	 * resolves to a default branch name.
	 */
	readonly worktreeBranch?: string;
}

/**
 * THE closed allowlist for operator flags on `gajaeway gjc`.
 *
 * Fail-closed: anything not explicitly forwarded is refused, including unknown
 * flags. Session-selection and system-prompt flags are refused because the
 * binder owns `--resume`, `--session-dir`, and `--append-system-prompt`; letting
 * an operator set them would unbind the managed session or overwrite the persona.
 */
export function classifyGjcWrapperFlags(args: readonly string[]): GjcWrapperFlagClassification {
	const forwarded: string[] = [];
	const refused: string[] = [];
	const unsupported: string[] = [];
	const wrapperOwned: string[] = [];
	let operatorModel = false;
	let worktreeBranch: string | undefined;

	for (let i = 0; i < args.length; i++) {
		const token = args[i];
		if (token === undefined) continue;
		// `--` ends flag classification; the binder uses no positionals.
		if (token === "--") break;

		const eq = token.indexOf("=");
		const name = eq > 0 ? token.slice(0, eq) : token;
		const hasInlineValue = eq > 0;
		// `--model=` and `--model=   ` both carry a syntactically present but blank
		// value; they fail closed exactly like a missing one.
		const inlineValue = hasInlineValue ? token.slice(eq + 1) : undefined;
		const hasUsableInlineValue = hasInlineValue && !isBlankValue(inlineValue);

		if (WRAPPER_OWNED_FLAGS.has(name)) {
			wrapperOwned.push(token);
			continue;
		}
		if (WRAPPER_OWNED_VALUE_FLAGS.has(name)) {
			// Consumed here and acted on by the gateway; never forwarded.
			wrapperOwned.push(token);
			if (hasUsableInlineValue) {
				worktreeBranch = inlineValue;
			} else if (!hasInlineValue && isUsableValue(args[i + 1])) {
				worktreeBranch = args[i + 1];
				i += 1;
			} else {
				// Bare `--worktree`: a managed worktree with a gateway-chosen name.
				worktreeBranch = "";
			}
			continue;
		}
		if (REFUSED_VALUE_FLAGS.has(name)) {
			refused.push(name);
			// Consume the following token so a refused flag's value cannot survive as
			// a positional. An empty token is consumed too (it is this flag's value,
			// not payload), but `--` and another flag are left alone.
			if (!hasInlineValue) {
				const next = args[i + 1];
				if (next !== undefined && next !== "--" && !looksLikeFlag(next)) i += 1;
			}
			continue;
		}
		if (REFUSED_PRESENCE_FLAGS.has(name)) {
			refused.push(name);
			continue;
		}
		if (FORWARDED_PRESENCE_FLAGS.has(name)) {
			forwarded.push(token);
			continue;
		}
		if (FORWARDED_VALUE_FLAGS.has(name)) {
			const isModelSelector = name === "--model" || name === "--mpreset";
			// A repeated model selector would put two model flags on one argv, which
			// is exactly what operator precedence exists to prevent.
			if (isModelSelector && operatorModel) {
				refused.push(name);
				if (!hasInlineValue && isUsableValue(args[i + 1])) i += 1;
				continue;
			}
			if (hasInlineValue) {
				if (!hasUsableInlineValue) {
					// `--model=` / `--mpreset=` / `--worktree=` with nothing after the `=`.
					refused.push(name);
					continue;
				}
				if (isModelSelector) operatorModel = true;
				forwarded.push(token);
				continue;
			}
			const next = args[i + 1];
			// `--worktree` takes an OPTIONAL value; the model selectors do not. An
			// unusable value (missing, `--`, another flag, or empty) must never be
			// forwarded as a model selector, or gjc resolves a blank model.
			if (!isUsableValue(next)) {
				if (isModelSelector) {
					refused.push(name);
					continue;
				}
				forwarded.push(token);
				continue;
			}
			if (isModelSelector) operatorModel = true;
			forwarded.push(token);
			forwarded.push(next);
			i += 1;
			continue;
		}
		// Unknown flag: fail closed rather than widening the vendor surface.
		if (looksLikeFlag(name)) {
			refused.push(name);
		}
		// A bare positional is not a flag; the binder ignores it.
	}

	return { forwarded, refused, wrapperOwned, operatorModel, unsupported, worktreeBranch };
}

/**
 * Every environment variable that can decide WHERE native gjc keeps session
 * state, including the legacy `PI_*` aliases and the XDG data root.
 *
 * `session.create` runs in the daemon's environment; the TUI runs in the
 * operator's. If any of these disagree the child resolves a different store and
 * dies with `Session "<id>" not found` — reproduced with a daemon and CLI given
 * different `GJC_CODING_AGENT_DIR` values.
 *
 * The list must stay COMPLETE rather than convenient: a selector that is missing
 * here is a selector the operator's shell can silently win.
 */
const NATIVE_STATE_ENV_KEYS = [
	"GJC_CODING_AGENT_DIR",
	"PI_CODING_AGENT_DIR",
	"GJC_CONFIG_DIR",
	"PI_CONFIG_DIR",
	"XDG_DATA_HOME",
	"HOME",
] as const;

/**
 * The daemon's native-state decision, expressed TOTALLY.
 *
 * `set` are the values the daemon has; `unset` are the selectors it does not.
 * Returning only `set` would be a patch, not a decision: a selector the
 * operator exports but the daemon does not would survive and strand the child.
 */
export function nativeStateEnv(source: Record<string, string | undefined> = process.env): {
	set: Record<string, string>;
	unset: string[];
} {
	const set: Record<string, string> = {};
	const unset: string[] = [];
	for (const key of NATIVE_STATE_ENV_KEYS) {
		const value = source[key];
		if (typeof value === "string" && value !== "") set[key] = value;
		else unset.push(key);
	}
	return { set, unset };
}

/**
 * Assemble the child argv for an attached terminal session.
 *
 * The binder injects `--resume <bound sessionId>` and the persona
 * `--append-system-prompt`. It never emits `--system-prompt`, `-p`, `--print`,
 * or `--mode`: the native coding register is preserved by OMISSION of the
 * generic-agent prompt, and the child must stay interactive.
 *
 * It deliberately does NOT inject `--session-dir`, even though the gateway owns
 * an epoch-scoped directory. `session.create` is issued through
 * `gjc sdk session raw`, which REJECTS `--session-dir`, so the bound session
 * always lives in gjc's default managed scope; telling the TUI to look in the
 * epoch directory instead made it exit with `Session "<id>" not found`. That was
 * found by the live drill, which is precisely what the drill exists to catch.
 * Resume stays stable because the spawn cwd is always the persona workspace, so
 * the create and the resume resolve the same scope. `--session-dir` remains
 * REFUSED for operators: redirecting the store would unbind the managed session.
 */
export function assembleGjcAttachArgv(input: {
	readonly sessionId: string;
	readonly personaPreamble: string;
	readonly forwarded: readonly string[];
	readonly configModel: GjcModelSelection | undefined;
}): string[] {
	const operatorModel = input.forwarded.some((token) => {
		const name = token.includes("=") ? token.slice(0, token.indexOf("=")) : token;
		return name === "--model" || name === "--mpreset";
	});
	// Operator precedence: an operator model selector suppresses config.model so
	// a single argv never carries two model flags.
	const modelArgs = operatorModel ? [] : gjcModelArgs(input.configModel);
	return [
		"gjc",
		"--resume",
		input.sessionId,
		"--append-system-prompt",
		input.personaPreamble,
		...modelArgs,
		...input.forwarded,
	];
}

/**
 * Incremental parser for the `gjc -p --mode json` ndjson event stream.
 * Tracks tool executions, captures the final assistant text, and (optionally)
 * reports each completed assistant message as it streams so long agentic turns
 * can deliver intermediate replies instead of going silent until process exit.
 */
export class GjcTurnStream {
	#buffer = "";
	#exactOutputTokens = 0;
	#deltaChars = 0;
	readonly #onAssistantText: ((text: string) => void) | undefined;
	toolCalls = 0;
	finalText: string | undefined;
	/** Structured error frame seen in the stream, if the runtime emitted one. */
	runtimeError: RuntimeErrorDetail | undefined;

	constructor(onAssistantText?: (text: string) => void) {
		this.#onAssistantText = onAssistantText;
	}

	/** Exact usage from completed messages plus a ~4-chars/token estimate of the in-flight one. */
	get outputTokens(): number {
		return this.#exactOutputTokens + Math.ceil(this.#deltaChars / 4);
	}

	/**
	 * True when a partial, unterminated line is still buffered. A child killed
	 * mid-write leaves its last frame here, so the frame is never parsed: the
	 * no-replay guard treats a pending buffer as work that may already have
	 * happened, because re-running a side effect is worse than refusing a replay.
	 */
	get pending(): boolean {
		// Only an unterminated JSON frame is possible work. Real gjc renders some
		// failures as plain text, and refusing a replay over a trailing prose
		// fragment would disable the recovery this exists to protect.
		return this.#buffer.trim().startsWith("{");
	}

	feed(chunk: string): void {
		this.#buffer += chunk;
		let index = this.#buffer.indexOf("\n");
		while (index !== -1) {
			const line = this.#buffer.slice(0, index).trim();
			this.#buffer = this.#buffer.slice(index + 1);
			if (line) this.#line(line);
			index = this.#buffer.indexOf("\n");
		}
	}

	#line(line: string): void {
		let event: {
			type?: string;
			assistantMessageEvent?: { delta?: string };
			message?: { role?: string; content?: Array<{ type?: string; text?: string }>; usage?: { output?: number } };
			error?: { code?: string; message?: string };
			ok?: boolean;
		};
		try {
			event = JSON.parse(line) as typeof event;
		} catch {
			return; // non-JSON noise line
		}
		// Structured turn failures (e.g. managed_append_identity_mismatch) arrive as
		// their own frame; the code is kept so the caller can classify without ever
		// matching on human message text.
		// First-wins: the FIRST declared error frame is the failure. A later
		// incidental error frame must not overwrite the genuine cause.
		this.runtimeError ??= runtimeErrorOfEnvelope(event);
		if (event.type === "message_update" && typeof event.assistantMessageEvent?.delta === "string")
			this.#deltaChars += event.assistantMessageEvent.delta.length;
		if (event.type === "tool_execution_start") this.toolCalls++;
		if (event.type === "message_end" && event.message?.role === "assistant") {
			if (typeof event.message.usage?.output === "number") {
				this.#exactOutputTokens += event.message.usage.output;
				this.#deltaChars = 0;
			}
			const text = (event.message.content ?? [])
				.filter((part) => part.type === "text" && typeof part.text === "string")
				.map((part) => part.text)
				.join("\n")
				.trim();
			if (text) {
				this.finalText = text;
				this.#onAssistantText?.(text);
			}
		}
	}
}

/**
 * ARCH-007 (reworded for the `gajaeway gjc` terminal entrypath): this module is
 * the sole assembler of gjc argv and the sole binder of managed sessions. The
 * spawn site is no longer unique — `gajaeway gjc` spawns an interactive TTY
 * child in the operator's terminal, because a child spawned inside the daemon
 * would inherit daemon stdio and could never be an interactive TUI. That CLI
 * spawn is an OPAQUE CONSUMER of the preassembled `SessionAttachResult.argv`:
 * flag classification lives only in `classifyGjcWrapperFlags`, argv assembly
 * only in `assembleGjcAttachArgv`, and the CLI must not classify, extend, or
 * reorder what it receives. Under `GAJAEWAY_TEST_STUB_GJC=1` the only argv
 * mutation the CLI may make is rewriting `argv[0]` to the test stub child.
 *
 * P0 strategy per the spike verdict (artifacts/p0-gjc-spike-report.md):
 * - Session identity: atomic idempotent create-or-resume via
 *   `gjc sdk session raw global --op session.create --idempotency-key <key>`.
 *   The same origin key always resolves to the same gjc session (proven under
 *   5-way concurrency), so a crash between create and binding commit is
 *   harmless: re-running create returns the same session (§4 row 3, branch A).
 * - Turns: spawn-per-turn `gjc --resume <id> -p --append-system-prompt <text>`.
 *   The gateway intentionally does not pass `--no-tools`; gjc owns its tool
 *   policy, informed by the unoverridable ActionGuard floor notice.
 *
 * The child inherits the owner's environment: the gateway is the owner's own
 * process and gjc needs the owner's provider credentials. Secrets are never
 * logged or persisted by this module.
 */
/**
 * Process seam. Bound at construction rather than stored as a bare
 * `Bun.spawn` reference, so the production path never depends on the method
 * being callable detached from its receiver.
 */
type SpawnFn = typeof Bun.spawn;

export class GjcClient implements GjcPort {
	/** GAJAEWAY_TEST_STUB_GJC is a test-only deterministic process seam; never set it in production. */
	readonly #sessions = new Map<string, string>();
	/** Reverse binding, so a turn failure can be traced back to the origin it must rebind. */
	readonly #origins = new Map<string, { originKey: string; epoch: number }>();
	/** In-flight session binds, so concurrent callers share one create-or-rebind instead of racing two epoch bumps. */
	readonly #inflight = new Map<string, Promise<{ sessionId: string }>>();
	readonly #database: GatewayDatabase;
	readonly #timeoutMs: number;
	readonly #cwd: string;
	readonly #model: GjcModelSelection | undefined;
	readonly #rebinder: SessionRebinder;
	readonly #spawn: SpawnFn;

	// 300s ceiling: the persona is an action-capable agent that runs real tools per
	// turn; 120s killed live owner turns mid-investigation (P1 drill finding).
	constructor(
		database: GatewayDatabase,
		timeoutMs = 300_000,
		cwd = process.cwd(),
		model?: GjcModelSelection,
		deps: {
			/** Rebinds allowed per session/origin before the gateway fails loudly. */
			readonly rebindCap?: number;
			/** Process seam; production always uses Bun.spawn. */
			readonly spawn?: SpawnFn;
			readonly log?: (line: string) => void;
		} = {},
	) {
		this.#database = database;
		this.#timeoutMs = timeoutMs;
		this.#cwd = cwd;
		this.#model = model;
		this.#spawn = deps.spawn ?? Bun.spawn.bind(Bun);
		this.#rebinder = new SessionRebinder(
			database,
			deps.rebindCap ?? DEFAULT_REBIND_CAP,
			deps.log ?? ((line) => console.warn(line)),
		);
	}

	/**
	 * Deterministic test-seam session id, row-first.
	 *
	 * Row-first is load-bearing, not cosmetic. The old recipe was a pure function
	 * of origin+epoch, so a test could "prove" sequential resume by recomputing
	 * the formula and comparing it to itself — while the persist seam was in fact
	 * missing. Returning the PERSISTED id when the row already holds one at this
	 * epoch means a second attach can only match if `putSession` really ran; and
	 * the freshly minted id is deliberately NOT the formula, so nothing can be
	 * recomputed. A per-call random id would be wrong in the other direction: the
	 * second attach would mint a new id and overwrite the row.
	 */
	#stubSession(originKey: string, epoch: number): { sessionId: string } {
		const row = this.#database.getSessionRecord(originKey);
		if (row && row.sessionId !== "" && row.epoch === epoch) return { sessionId: row.sessionId };
		return { sessionId: `stub-session-${crypto.randomUUID()}` };
	}

	/**
	 * Binds an origin to a gjc session, rebinding once when the runtime condemns
	 * the derived idempotency key (#13). A rebindable code means the key is dead
	 * forever, so retrying it is guaranteed silence; the epoch is bumped through
	 * the same reset semantics as `/new`, persisted, and `session.create` is
	 * retried exactly once per attempt.
	 */
	async ensureSession(originKey: string, epoch = 0, options?: TurnOptions): Promise<{ sessionId: string }> {
		if (process.env.GAJAEWAY_TEST_STUB_GJC === "1") return this.#stubSession(originKey, epoch);
		const cached = this.#cachedSession(originKey, epoch);
		if (cached) return { sessionId: cached };
		// One bind in flight per origin+epoch: without this, two concurrent callers
		// whose create fails rebindably EACH bump the epoch and mint a session where
		// the contract is exactly one rebind per condemned key. Settled entries are
		// reaped in finally, so the map stays bounded to actually-concurrent binds.
		return this.#bindAt(originKey, epoch, options);
	}

	/**
	 * The ONE bind coordinator for every origin+epoch. Chat turns are serialized
	 * per origin upstream by KeyedQueue, but monitor propagation, future direct
	 * callers, and sendTurn's own turn-level rebind path are not — so every bind,
	 * whatever its caller, goes through here: first caller performs
	 * create-or-rebind, joiners await the same settled promise and receive the
	 * identical binding instead of racing a second epoch bump.
	 */
	#bindAt(originKey: string, epoch: number, options?: TurnOptions): Promise<{ sessionId: string }> {
		return this.#coordinatedBind(originKey, epoch, options, true);
	}

	/**
	 * The coordinator entry: join an in-flight bind for this origin+epoch or
	 * start one. `mayRebind` is true only for the TOP-LEVEL bind a caller asked
	 * for; the post-rebind create starts with false, so one caller's
	 * ensureSession spends at most ONE rebind instead of chaining bumps while
	 * every consecutive epoch fails — the cap only resets on good turns.
	 */
	#coordinatedBind(
		originKey: string,
		epoch: number,
		options: TurnOptions | undefined,
		mayRebind: boolean,
	): Promise<{ sessionId: string }> {
		const cacheKey = `${originKey}#${epoch}`;
		const existing = this.#inflight.get(cacheKey);
		if (existing) return existing;
		const bind = this.#ensureSessionUncached(originKey, epoch, options, mayRebind).finally(() => {
			this.#inflight.delete(cacheKey);
		});
		this.#inflight.set(cacheKey, bind);
		return bind;
	}

	async #ensureSessionUncached(
		originKey: string,
		epoch: number,
		options: TurnOptions | undefined,
		mayRebind: boolean,
	): Promise<{ sessionId: string }> {
		try {
			return await this.#createSession(originKey, epoch, options);
		} catch (error) {
			const code = rebindableCodeOf(error);
			if (!code || !mayRebind) throw error;
			const nextEpoch = this.#rebinder.rebind(originKey, code, epoch);
			// The post-rebind bind ALSO enters the coordinator (as a non-rebinding
			// step): a concurrent caller reading the newly durable e1 joins this
			// exact create instead of starting a second one that could race to e2.
			return await this.#coordinatedBind(originKey, nextEpoch, options, false);
		}
	}

	/** An explicit `/new` is the manual rebind, so it restores the budget too. */
	forgetRebinds(originKey: string): void {
		this.#rebinder.clear(originKey);
	}

	#cachedSession(originKey: string, epoch: number): string | undefined {
		const cacheKey = `${originKey}#${epoch}`;
		const record = this.#database.getSessionRecord(originKey);
		const cached =
			this.#sessions.get(cacheKey) ??
			(record && record.epoch === epoch && record.sessionId ? record.sessionId : undefined);
		if (!cached) return undefined;
		this.#sessions.set(cacheKey, cached);
		this.#origins.set(cached, { originKey, epoch });
		return cached;
	}

	async #createSession(originKey: string, epoch: number, options?: TurnOptions): Promise<{ sessionId: string }> {
		const cacheKey = `${originKey}#${epoch}`;
		// Instance-scoped key: two gateway installs (or two homes on one machine)
		// must never collide on the same gjc session (cross-instance replay bug
		// found in P2 integration). Epoch is always included so /new provably
		// binds a fresh transcript.
		const idempotencyKey = `gajaeway-${this.#database.instanceId}-${originKey.replace(/[^A-Za-z0-9._-]/g, "-")}-e${epoch}`;
		const child = this.#spawn({
			cmd: [
				"gjc",
				"sdk",
				"session",
				"raw",
				"global",
				"--op",
				"session.create",
				"--idempotency-key",
				idempotencyKey,
				"--json-input-stdin",
			],
			cwd: options?.cwd ?? this.#cwd,
			stdin: new Response(JSON.stringify({ cwd: options?.cwd ?? this.#cwd })).body ?? "ignore",
			stdout: "pipe",
			stderr: "pipe",
			// Caller overrides last: the attach path pins the native state root here
			// so create and the later interactive resume share one store.
			env: { ...(process.env as Record<string, string>), ...(options?.env ?? {}) },
		});
		const [stdout, stderr, exitCode] = await this.#bounded(child, "session.create");
		if (exitCode !== 0) {
			// The runtime's structured envelope can land on either stream; the code is
			// what decides rebindability, so both are inspected before giving up.
			const detail = extractRuntimeError(stdout) ?? extractRuntimeError(stderr);
			throw runtimeFailure(`gjc session.create exited ${exitCode}`, detail, stderr.trim());
		}
		const sessionId = parseCreateResult(stdout);
		// Publication validation BEFORE any cache mutation: if another path
		// rebound this origin PAST our epoch while our create was in flight, this
		// binding is obsolete. It must not be cached, must not evict the live
		// mapping, and must never be handed to the caller as if it were current —
		// the caller chases the live epoch's coordinated bind instead.
		const persistedEpoch = this.#database.getSessionRecord(originKey)?.epoch ?? -1;
		if (persistedEpoch > epoch) {
			console.warn(`gateway discarding superseded bind ${originKey}#e${epoch}; persisted epoch is e${persistedEpoch}`);
			return await this.#bindAt(originKey, persistedEpoch);
		}
		// One live binding per origin: older epochs are unreachable once a new one
		// is bound, so dropping them keeps both maps bounded and prevents a stale
		// epoch from being reported as a rebind's from-epoch.
		this.#forgetOrigin(originKey);
		this.#sessions.set(cacheKey, sessionId);
		this.#origins.set(sessionId, { originKey, epoch });
		this.#database.withTransaction(() => this.#database.putSession(originKey, sessionId));
		// The budget is deliberately NOT cleared here: a fresh key always creates,
		// so clearing on create would make the cap unreachable for a recurring
		// turn-level failure. Only a proven-good turn clears it.
		return { sessionId };
	}

	#forgetOrigin(originKey: string): void {
		for (const [cacheKey, sessionId] of this.#sessions)
			if (cacheKey.startsWith(`${originKey}#`)) {
				this.#origins.delete(sessionId);
				this.#sessions.delete(cacheKey);
			}
	}

	/**
	 * Runs one turn, rebinding once when the runtime reports that the bound
	 * session can no longer be resumed from this cwd (#13): a turn-level
	 * `managed_append_identity_mismatch` condemns the binding itself, so retrying
	 * the same session id is guaranteed silence. The rebind bumps and persists
	 * the epoch and binds a fresh session.
	 *
	 * The turn is replayed only when the failed attempt provably did nothing. The
	 * persona has full tool access by design, so replaying a turn that already ran
	 * tools would re-run real side effects; in that case the gateway still rebinds
	 * (so the next turn works) but surfaces the failure instead of re-running.
	 */
	async sendTurn(
		sessionId: string,
		text: string,
		systemPreamble?: string,
		onProgress?: (progress: TurnProgress) => void,
		options?: TurnOptions,
	): Promise<string> {
		if (typeof text !== "string" || text.length === 0) {
			throw new Error("turn text must be non-empty");
		}
		if (process.env.GAJAEWAY_TEST_STUB_GJC === "1") {
			await Bun.sleep(50);
			if (process.env.GAJAEWAY_TEST_STUB_CAPTURE)
				await Bun.write(process.env.GAJAEWAY_TEST_STUB_CAPTURE, systemPreamble ?? "");
			onProgress?.({ toolCalls: 0, outputTokens: 0 });
			return process.env.GAJAEWAY_TEST_STUB_REPLY ?? "stub reply";
		}
		const observed = { toolCalls: 0, outputTokens: 0, hadText: false, pending: false };
		try {
			const reply = await this.#runTurn(sessionId, text, systemPreamble, onProgress, options, observed);
			// A turn that completed WITHOUT needing a rebind is the only proof this
			// origin is healthy, so it is the only thing that restores the budget.
			// Clearing after a REPLAYED turn would reopen unbounded epoch growth: for
			// a code that recurs every message and clears on the replay, each message
			// would cost one bump and reset the counter, so the cap would never fire.
			const bound = this.#origins.get(sessionId);
			if (bound) this.#rebinder.clear(bound.originKey);
			return reply;
		} catch (error) {
			const code = rebindableCodeOf(error);
			const binding = this.#origins.get(sessionId);
			if (!code || !binding) throw error;
			// The persisted epoch is the truth; the remembered binding can be stale,
			// and the rebind log line must not misstate the epoch transition.
			const fromEpoch = this.#database.getSessionRecord(binding.originKey)?.epoch ?? binding.epoch;
			const nextEpoch = this.#rebinder.rebind(binding.originKey, code, fromEpoch);
			this.#sessions.delete(`${binding.originKey}#${binding.epoch}`);
			this.#origins.delete(sessionId);
			// The turn-level rebind binds through the SAME coordinator as
			// ensureSession, so a concurrent ensureSession for this origin joins the
			// fresh binding instead of racing its own epoch bump.
			const rebound = await this.#bindAt(binding.originKey, nextEpoch, options);
			if (observed.toolCalls > 0 || observed.hadText || observed.outputTokens > 0 || observed.pending) {
				// Report the evidence that actually blocked the replay, and give it its
				// own code: the automatic rebind already happened, so telling the user
				// to /new here would bump a second epoch and discard the fresh binding.
				const evidence = [
					observed.toolCalls > 0 ? `${observed.toolCalls} tool call(s)` : undefined,
					observed.hadText ? "assistant text" : undefined,
					observed.outputTokens > 0 ? `${observed.outputTokens} output token(s)` : undefined,
					observed.pending ? "an unterminated frame from a child that died mid-write" : undefined,
				]
					.filter((part) => part !== undefined)
					.join(", ");
				throw new GjcRuntimeError(
					`${error instanceof Error ? error.message : String(error)} (rebound to e${nextEpoch}; not replayed because the turn had already produced ${evidence})`,
					{
						code: "turn_not_replayed",
						message: `${code}: the turn had already produced ${evidence}, so it was rebound to e${nextEpoch} but not replayed`,
					},
				);
			}
			const reboundPreamble = options?.systemPreambleForEpoch
				? await options.systemPreambleForEpoch(nextEpoch)
				: systemPreamble;
			return await this.#runTurn(rebound.sessionId, text, reboundPreamble, onProgress, options);
		}
	}

	async #runTurn(
		sessionId: string,
		text: string,
		systemPreamble?: string,
		onProgress?: (progress: TurnProgress) => void,
		options?: TurnOptions,
		/** Work the attempt was seen to perform; a replay is only safe when it is empty. */
		observed?: { toolCalls: number; outputTokens: number; hadText: boolean; pending: boolean },
	): Promise<string> {
		const child = this.#spawn({
			cmd: [
				"gjc",
				"--resume",
				sessionId,
				"-p",
				"--mode",
				"json",
				...(options?.codingRegister ? [] : ["--system-prompt", GENERIC_AGENT_SYSTEM_PROMPT]),
				...gjcModelArgs(this.#model),
				...(systemPreamble ? ["--append-system-prompt", systemPreamble] : []),
				text,
			],
			cwd: options?.cwd ?? this.#cwd,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
			env: process.env as Record<string, string>,
		});
		// The ceiling is an INACTIVITY ceiling, not a wall-clock one: a turn that is
		// visibly working (streaming events, running tools) is never killed, while a
		// hung child that goes silent for turnTimeoutMs is reaped (owner directive:
		// long agentic work must not die mid-flight; liveness is reported instead).
		const stream = new GjcTurnStream(options?.onAssistantText);
		let lastActivity = Date.now();
		let killedForInactivity = false;
		const watchdog = setInterval(() => {
			if (Date.now() - lastActivity > this.#timeoutMs) {
				killedForInactivity = true;
				child.kill();
			}
		}, 1_000);
		try {
			const decoder = new TextDecoder();
			const stderrPromise = new Response(child.stderr as ReadableStream).text();
			// Async iteration instead of an explicit reader: killing the child while a
			// getReader() read was pending crashed the compiled Bun binary in production
			// (panic at sendTurn); iteration ends or throws cleanly on teardown instead.
			try {
				for await (const value of child.stdout as unknown as AsyncIterable<Uint8Array>) {
					lastActivity = Date.now();
					stream.feed(decoder.decode(value, { stream: true }));
					if (observed) {
						observed.toolCalls = stream.toolCalls;
						observed.outputTokens = stream.outputTokens;
						observed.hadText = stream.finalText !== undefined;
						observed.pending = stream.pending;
					}
					if (onProgress) onProgress({ toolCalls: stream.toolCalls, outputTokens: stream.outputTokens });
				}
			} catch {
				// Stream teardown after kill or child death; the exit code decides the outcome.
			}
			const [stderr, exitCode] = await Promise.all([stderrPromise, child.exited]);
			if (killedForInactivity) throw new Error(`gjc turn made no progress for ${this.#timeoutMs}ms and was reaped`);
			if (exitCode !== 0) {
				// Classification is code-only: a turn-level structured error may arrive
				// in the ndjson stream or on stderr, and its code (never its wording)
				// decides whether the binding is condemned.
				const detail = stream.runtimeError ?? extractRuntimeError(stderr);
				throw runtimeFailure(`gjc turn exited ${exitCode}`, detail, stderr.trim());
			}
			if (stream.finalText === undefined) throw new Error("gjc turn stream produced no assistant text");
			return stream.finalText;
		} finally {
			clearInterval(watchdog);
		}
	}

	async #bounded(child: ReturnType<typeof Bun.spawn>, label: string): Promise<[string, string, number]> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				Promise.all([
					new Response(child.stdout as ReadableStream).text(),
					new Response(child.stderr as ReadableStream).text(),
					child.exited,
				]),
				new Promise<never>((_, reject) => {
					timer = setTimeout(() => {
						child.kill();
						reject(new Error(`gjc ${label} timed out after ${this.#timeoutMs}ms`));
					}, this.#timeoutMs);
				}),
			]);
		} finally {
			if (timer) clearTimeout(timer);
		}
	}
}

/**
 * One framing for every gjc failure: the gateway's own context, then the
 * runtime's code and message when it reported them, with the structured detail
 * attached so the caller classifies on the CODE rather than on this string.
 */
function runtimeFailure(context: string, detail: RuntimeErrorDetail | undefined, fallback: string): GjcRuntimeError {
	// Sanitize at construction: this error's message is what every downstream
	// catch logs verbatim (daemon log is owner-visible), so control-character
	// stripping and the same secret-only redaction the delivered notice applies
	// must hold here — an envelope-less stderr echo of a credential, including a
	// code-only envelope where stderr becomes the message, must not survive into
	// any durable record. A fully-sanitized-empty result keeps the gateway's own
	// framing so the error is never blanked into meaninglessness.
	const resolved: RuntimeErrorDetail = detail ?? { message: fallback };
	const code = resolved.code ? sanitizeDiagnostic(resolved.code) || undefined : undefined;
	const message = sanitizeDiagnostic(resolved.message ?? fallback) || context;
	return new GjcRuntimeError(`${context}: ${code ? `${code}: ` : ""}${message}`, {
		...(code ? { code } : {}),
		message,
	});
}

/**
 * Reads the sessionId out of the SDK envelope. An `ok:false` envelope keeps the
 * runtime's own code, because that code — not the message wording — is what
 * decides whether the idempotency key is condemned and must be rebound (#13).
 */
function parseCreateResult(stdout: string): string {
	for (const line of stdout.trim().split("\n")) {
		let parsed: { ok?: boolean; result?: { sessionId?: string } } | undefined;
		try {
			parsed = JSON.parse(line) as typeof parsed;
		} catch {
			continue; // non-JSON noise line; keep scanning
		}
		if (parsed?.ok && typeof parsed.result?.sessionId === "string") return parsed.result.sessionId;
		if (parsed?.ok === false) {
			throw runtimeFailure("gjc session.create failed", runtimeErrorOfEnvelope(parsed), line);
		}
	}
	throw new Error("gjc session.create produced no parseable sessionId");
}
