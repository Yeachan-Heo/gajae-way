/**
 * Prompt submission and the durable send receipt.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 2/5):
 *   - `--op-ref` is `turn.prompt`'s `clientRef`: a caller-chosen correlation key,
 *     non-empty, at most 128 chars, unique per prompt kind inside one live
 *     session runtime, and freshly minted for every logical prompt.
 *   - `send --wait --timeout-ms` is only the controller's bounded wait. A
 *     `wait_timeout` is NON-terminal and must never be reported as failure or
 *     retried as a new prompt; the turn keeps running under the runtime's own
 *     inactivity lease and hard-runtime ceilings.
 *   - `send` success is an *accepted receipt*, not completion. The receipt tuple
 *     must survive a controller restart so progress is reconciled via `status`.
 */

import type { ControllerOptions } from "./cli";
import { GjcCliError, parseEnvelope } from "./cli";
import {
	fetchOpState,
	isTerminalStatus,
	projectOpState,
	requiresOperatorHold,
	type StatusReport,
	type SupervisorOpState,
} from "./status";

export const MAX_OP_REF_LENGTH = 128;

export class OpRefError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "OpRefError";
	}
}

const OP_REF_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;

export function assertValidOpRef(opRef: string): void {
	if (opRef.length === 0) {
		throw new OpRefError("op-ref must not be empty");
	}
	if (opRef.length > MAX_OP_REF_LENGTH) {
		throw new OpRefError(`op-ref must be at most ${MAX_OP_REF_LENGTH} chars, got ${opRef.length}`);
	}
	if (!OP_REF_PATTERN.test(opRef)) {
		throw new OpRefError(`op-ref "${opRef}" must be lowercase [a-z0-9._-] starting alphanumeric`);
	}
}

/**
 * Mints `gw-<taskKey>-<ulid>`.
 *
 * The random suffix is what keeps a retry from colliding with the prompt it is
 * replacing; an attempt counter would silently reuse a ref after a restart that
 * lost the counter.
 */
export function newOpRef(taskKey: string, ulid: () => string = lowercaseUlid): string {
	const opRef = `gw-${taskKey}-${ulid()}`.toLowerCase();
	assertValidOpRef(opRef);
	return opRef;
}

function lowercaseUlid(): string {
	const alphabet = "0123456789abcdefghjkmnpqrstvwxyz";
	const time = Date.now();
	let timePart = "";
	let remaining = time;
	for (let index = 0; index < 10; index += 1) {
		timePart = `${alphabet[remaining % 32]}${timePart}`;
		remaining = Math.floor(remaining / 32);
	}
	const random = crypto.getRandomValues(new Uint8Array(16));
	let randomPart = "";
	for (const byte of random) {
		randomPart += alphabet[byte % 32];
	}
	return `${timePart}${randomPart}`;
}

export type SendReceipt = {
	readonly sessionId: string;
	readonly operationRef: string;
	readonly commandId?: string;
	readonly turnId?: string;
	readonly acceptedAt: string;
	readonly taskKey: string;
};

/**
 * Tracks issued op-refs for one session runtime.
 *
 * Reuse is rejected because two logical prompts sharing a correlation key make
 * `status` ambiguous - the exact failure mode that loses a worker's result.
 */
export class OpRefLedger {
	readonly #issued = new Map<string, Set<string>>();

	issue(sessionId: string, opRef: string): void {
		assertValidOpRef(opRef);
		const perSession = this.#issued.get(sessionId) ?? new Set<string>();
		if (perSession.has(opRef)) {
			throw new OpRefError(`op-ref ${opRef} was already used in session ${sessionId}`);
		}
		perSession.add(opRef);
		this.#issued.set(sessionId, perSession);
	}

	/** The same value in a different session is not a collision. */
	has(sessionId: string, opRef: string): boolean {
		return this.#issued.get(sessionId)?.has(opRef) === true;
	}
}

export type SendPromptInput = {
	readonly sessionId: string;
	readonly text: string;
	readonly taskKey: string;
	readonly opRef?: string;
	/** Controller-side bounded wait only; omitted means fire-and-reconcile. */
	readonly waitTimeoutMs?: number;
	readonly ledger?: OpRefLedger;
	readonly now?: () => Date;
};

/**
 * Submits one logical prompt and returns the accepted receipt.
 *
 * The receipt is returned even when the bounded wait elapses, because a
 * `wait_timeout` means "still running", not "failed".
 */
export async function sendPrompt(options: ControllerOptions, input: SendPromptInput): Promise<SendReceipt> {
	if (input.text.trim().length === 0) {
		throw new OpRefError("prompt text must not be empty");
	}
	const opRef = input.opRef ?? newOpRef(input.taskKey);
	assertValidOpRef(opRef);
	input.ledger?.issue(input.sessionId, opRef);

	const args = [
		"sdk",
		"session",
		...(options.agentDir ? ["--agent-dir", options.agentDir] : []),
		"send",
		input.sessionId,
		"--repo",
		options.repo,
		"--text",
		input.text,
		"--op-ref",
		opRef,
		...(input.waitTimeoutMs === undefined ? [] : ["--wait", "--timeout-ms", String(input.waitTimeoutMs)]),
	];

	const raw = await options.run(args, {
		...(input.waitTimeoutMs === undefined ? {} : { timeoutMs: input.waitTimeoutMs + 5_000 }),
	});
	let payload: { commandId?: unknown; turnId?: unknown; sessionId?: unknown };
	try {
		payload = parseEnvelope(raw, "session send");
	} catch (error) {
		if (isOpRefRejection(error)) {
			throw new OpRefRejectedError(
				opRef,
				CLIENT_REF_CONFLICT_CODE,
				error instanceof GjcCliError ? error.details : undefined,
			);
		}
		throw error;
	}

	const now = input.now ?? (() => new Date());
	return {
		sessionId: typeof payload.sessionId === "string" ? payload.sessionId : input.sessionId,
		operationRef: opRef,
		...(typeof payload.commandId === "string" ? { commandId: payload.commandId } : {}),
		...(typeof payload.turnId === "string" ? { turnId: payload.turnId } : {}),
		acceptedAt: now().toISOString(),
		taskKey: input.taskKey,
	};
}

/**
 * A resend of an already-used op-ref is REJECTED, not replayed.
 *
 * `turn.prompt` is ordered and non-idempotent, so the runtime refuses a
 * duplicate `clientRef` instead of returning the earlier result. The correct
 * recovery is to reconcile the existing record via `status` - never to mint a
 * fresh ref for the same logical prompt, which would run the work twice.
 *
 * Classification is by the structured envelope code ONLY. Human messages get
 * reworded, translated and wrapped between releases, so a text heuristic
 * misclassifies both ways: it would call an unrelated failure a ref conflict, or
 * miss a real one behind a wrapping error.
 */
export const CLIENT_REF_CONFLICT_CODE = "client_ref_conflict";

export class OpRefRejectedError extends Error {
	readonly opRef: string;
	/** Exact envelope code that produced this classification. */
	readonly code: string;
	/** Original error payload, kept verbatim for the operator. */
	readonly details: unknown;

	constructor(opRef: string, code: string, details: unknown) {
		super(`op-ref ${opRef} was rejected with ${code}; reconcile it with status instead of resending or reminting`);
		this.name = "OpRefRejectedError";
		this.opRef = opRef;
		this.code = code;
		this.details = details;
	}
}

/** Reads `error.code` from a parsed envelope payload, if it has one. */
export function envelopeErrorCode(details: unknown): string | undefined {
	if (typeof details !== "object" || details === null) {
		return undefined;
	}
	const code = (details as { code?: unknown }).code;
	return typeof code === "string" ? code : undefined;
}

/**
 * True only for an exact `client_ref_conflict` envelope code.
 *
 * Anything else - including an envelope with no code at all - stays a generic
 * operational failure and fails closed.
 */
export function isOpRefRejection(error: unknown): boolean {
	if (!(error instanceof GjcCliError)) {
		return false;
	}
	return envelopeErrorCode(error.details) === CLIENT_REF_CONFLICT_CODE;
}

export type PromptOutcome = {
	/** Only `terminal_ok | failed` are terminal; `unknown` is neither. */
	readonly terminal: boolean;
	/** True when automation must stop and wait for operator judgement. */
	readonly hold: boolean;
	readonly state: SupervisorOpState;
	readonly report: StatusReport;
	readonly receipt: SendReceipt;
};

/**
 * Reconciles a stored receipt against the runtime, e.g. after a restart.
 *
 * The canonical five statuses are projected onto the supervisor state, so a
 * cancel (`terminal_ok` + `outcome.reason = "cancelled"`) is never reported as a
 * completed deliverable and an `unknown` record is held rather than guessed.
 */
export async function pollStatus(
	options: ControllerOptions,
	receipt: SendReceipt,
	timeoutMs?: number,
): Promise<PromptOutcome> {
	const report = await fetchOpState(options, receipt.sessionId, receipt.operationRef, timeoutMs);
	const state = projectOpState(report.status);
	return {
		terminal: isTerminalStatus(report.status.status),
		hold: requiresOperatorHold(state),
		state,
		report,
		receipt,
	};
}

export type ReconcileResult =
	| { readonly kind: "resumed"; readonly outcome: PromptOutcome }
	| { readonly kind: "sent"; readonly receipt: SendReceipt };

/**
 * Restart-safe entry point for one logical prompt.
 *
 * With a stored receipt the runtime is asked first: a live turn is resumed, a
 * terminal turn is reported, and neither path resends. Sending only happens when
 * there is no prior record, which is what keeps an ordered non-idempotent
 * `turn.prompt` from being executed twice after a controller restart.
 */
export async function reconcileOrSend(
	options: ControllerOptions,
	input: SendPromptInput & { readonly existing?: SendReceipt },
): Promise<ReconcileResult> {
	if (input.existing) {
		return { kind: "resumed", outcome: await pollStatus(options, input.existing) };
	}
	return { kind: "sent", receipt: await sendPrompt(options, input) };
}
