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
import { parseEnvelope } from "./cli";

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
	const payload = parseEnvelope<{ commandId?: unknown; turnId?: unknown; sessionId?: unknown }>(raw, "session send");

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

export type PromptOutcome = {
	/** Terminal states end reconciliation; `wait_timeout`/`running` do not. */
	readonly terminal: boolean;
	readonly state: string;
	readonly receipt: SendReceipt;
	readonly raw: unknown;
};

const TERMINAL_STATES = new Set(["completed", "failed", "cancelled", "canceled", "error"]);
const NON_TERMINAL_STATES = new Set(["wait_timeout", "running", "active", "pending", "accepted"]);

export function classifyState(state: string): { terminal: boolean } {
	if (TERMINAL_STATES.has(state)) {
		return { terminal: true };
	}
	if (NON_TERMINAL_STATES.has(state)) {
		return { terminal: false };
	}
	// Unknown states are treated as non-terminal: closing a live turn early is
	// the more expensive mistake.
	return { terminal: false };
}

/** Reconciles a stored receipt against the runtime, e.g. after a restart. */
export async function pollStatus(
	options: ControllerOptions,
	receipt: SendReceipt,
	timeoutMs?: number,
): Promise<PromptOutcome> {
	const args = [
		"sdk",
		"session",
		...(options.agentDir ? ["--agent-dir", options.agentDir] : []),
		"status",
		receipt.sessionId,
		receipt.operationRef,
		"--repo",
		options.repo,
		...(timeoutMs === undefined ? [] : ["--timeout-ms", String(timeoutMs)]),
	];
	const raw = await options.run(args);
	const payload = parseEnvelope<{ state?: unknown; status?: unknown }>(raw, "session status");
	const state =
		(typeof payload.state === "string" && payload.state) ||
		(typeof payload.status === "string" && payload.status) ||
		"unknown";
	return { ...classifyState(state), state, receipt, raw: payload };
}
