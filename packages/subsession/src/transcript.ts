/**
 * Result body retrieval.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 3/5):
 *   - `status(sessionId, opRef)` is the terminal authority for an operation, but
 *     it is not the store for the assistant body.
 *   - the body comes from `gjc sdk session raw query --query session.last_assistant`,
 *     following the returned continuation cursor until `page.complete === true`.
 *   - inferring paths or reading `.gjc` directly is forbidden.
 *   - `tail --until-idle` observes retained transcript and lifecycle; it is not
 *     lossless terminal authority. A `retention_gap` under `tail --strict` means
 *     events are missing, so the operation must be reconciled through `status`
 *     and the body re-fetched instead of guessing a terminal state.
 */

import type { ControllerOptions } from "./cli";
import { parseEnvelope } from "./cli";

export type TranscriptPage = {
	readonly text: string;
	readonly cursor?: string;
	readonly complete: boolean;
};

export type LastAssistantResult = {
	readonly text: string;
	readonly pages: number;
	readonly complete: boolean;
};

export class TranscriptIncompleteError extends Error {
	readonly pages: number;

	constructor(message: string, pages: number) {
		super(message);
		this.name = "TranscriptIncompleteError";
		this.pages = pages;
	}
}

export class RetentionGapError extends Error {
	constructor(sessionId: string) {
		super(
			`tail --strict reported a retention gap for ${sessionId}; reconcile the operation with status and re-fetch the body instead of inferring a terminal state`,
		);
		this.name = "RetentionGapError";
	}
}

function parsePage(payload: Record<string, unknown>): TranscriptPage {
	const page = (payload.page ?? {}) as Record<string, unknown>;
	const text = typeof payload.text === "string" ? payload.text : typeof payload.body === "string" ? payload.body : "";
	const cursor =
		typeof page.cursor === "string" ? page.cursor : typeof payload.cursor === "string" ? payload.cursor : undefined;
	return {
		text,
		...(cursor ? { cursor } : {}),
		complete: page.complete === true,
	};
}

/**
 * Reads the last assistant message of a session transcript, following pagination
 * to the end.
 *
 * A run that stops before `page.complete === true` throws instead of returning a
 * partial body: a truncated deliverable that looks whole is worse than a loud
 * failure. `maxPages` only bounds a runaway cursor loop.
 */
export async function fetchLastAssistant(
	options: ControllerOptions,
	sessionId: string,
	limits: { readonly maxPages?: number } = {},
): Promise<LastAssistantResult> {
	const maxPages = limits.maxPages ?? 50;
	const chunks: string[] = [];
	let cursor: string | undefined;
	let pages = 0;

	for (;;) {
		const raw = await options.run([
			"sdk",
			"session",
			...(options.agentDir ? ["--agent-dir", options.agentDir] : []),
			"raw",
			"query",
			sessionId,
			"--query",
			"session.last_assistant",
			"--repo",
			options.repo,
			...(cursor ? ["--cursor", cursor] : []),
		]);
		const payload = parseEnvelope<Record<string, unknown>>(raw, "session raw query");
		const page = parsePage(payload);
		pages += 1;
		chunks.push(page.text);

		if (page.complete) {
			return { text: chunks.join(""), pages, complete: true };
		}
		if (!page.cursor) {
			throw new TranscriptIncompleteError(
				`session.last_assistant page ${pages} is incomplete but returned no continuation cursor`,
				pages,
			);
		}
		if (pages >= maxPages) {
			throw new TranscriptIncompleteError(`session.last_assistant did not complete within ${maxPages} pages`, pages);
		}
		cursor = page.cursor;
	}
}

export type TailEvent = { readonly kind?: string; readonly reason?: string };

/**
 * Detects the retention gap signal in `tail --strict` output.
 *
 * Exposed as a check rather than a recovery: the caller must go back to `status`,
 * because tail can never be promoted to terminal authority for an operation.
 */
export function assertNoRetentionGap(sessionId: string, events: readonly TailEvent[]): void {
	const gap = events.some((event) => event.kind === "retention_gap" || event.reason === "retention_gap");
	if (gap) {
		throw new RetentionGapError(sessionId);
	}
}
