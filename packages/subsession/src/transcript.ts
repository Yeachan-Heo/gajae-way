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

/**
 * Full-body expansion.
 *
 * `session.last_assistant` recovers the final message; the complete record needs
 * the documented ladder `transcript.list -> transcript.body | resource.body ->
 * artifact.read`. Each rung is a named raw query, so no step guesses a path or
 * reads `.gjc` state.
 */

export type TranscriptEntry = {
	readonly id: string;
	readonly kind?: string;
	/** Set when the body was externalised as an artifact. */
	readonly artifactId?: string;
	/** Set when the body lives behind a resource handle. */
	readonly resourceId?: string;
};

export type ExpandedEntry = {
	readonly entry: TranscriptEntry;
	readonly text: string;
	/** Which rung of the ladder actually produced the text. */
	readonly source: "transcript.body" | "resource.body" | "artifact.read";
};

async function rawQuery(
	options: ControllerOptions,
	sessionId: string,
	query: string,
	extra: readonly string[] = [],
): Promise<Record<string, unknown>> {
	const raw = await options.run([
		"sdk",
		"session",
		...(options.agentDir ? ["--agent-dir", options.agentDir] : []),
		"raw",
		"query",
		sessionId,
		"--query",
		query,
		"--repo",
		options.repo,
		...extra,
	]);
	return parseEnvelope<Record<string, unknown>>(raw, `session raw query ${query}`);
}

/** Lists transcript entries, following the continuation cursor to the end. */
export async function listTranscript(
	options: ControllerOptions,
	sessionId: string,
	limits: { readonly maxPages?: number } = {},
): Promise<readonly TranscriptEntry[]> {
	const maxPages = limits.maxPages ?? 100;
	const entries: TranscriptEntry[] = [];
	let cursor: string | undefined;
	let pages = 0;

	for (;;) {
		const payload = await rawQuery(options, sessionId, "transcript.list", cursor ? ["--cursor", cursor] : []);
		const page = (payload.page ?? {}) as Record<string, unknown>;
		for (const raw of (payload.entries ?? []) as Record<string, unknown>[]) {
			if (typeof raw.id === "string") {
				entries.push({
					id: raw.id,
					...(typeof raw.kind === "string" ? { kind: raw.kind } : {}),
					...(typeof raw.artifactId === "string" ? { artifactId: raw.artifactId } : {}),
					...(typeof raw.resourceId === "string" ? { resourceId: raw.resourceId } : {}),
				});
			}
		}
		pages += 1;
		if (page.complete === true) {
			return entries;
		}
		const next = typeof page.cursor === "string" ? page.cursor : undefined;
		if (!next) {
			throw new TranscriptIncompleteError(
				`transcript.list page ${pages} is incomplete but returned no continuation cursor`,
				pages,
			);
		}
		if (pages >= maxPages) {
			throw new TranscriptIncompleteError(`transcript.list did not complete within ${maxPages} pages`, pages);
		}
		cursor = next;
	}
}

function readText(payload: Record<string, unknown>): string | undefined {
	for (const key of ["text", "body", "content"]) {
		const value = payload[key];
		if (typeof value === "string") {
			return value;
		}
	}
	return undefined;
}

/**
 * Expands one entry down the ladder.
 *
 * The rungs are tried in the documented order and the first one that actually
 * returns text wins, with the winning rung reported so a caller can tell an
 * inline body apart from an artifact it may need to cite differently.
 */
export async function expandEntry(
	options: ControllerOptions,
	sessionId: string,
	entry: TranscriptEntry,
): Promise<ExpandedEntry> {
	const inline = readText(
		await rawQuery(options, sessionId, "transcript.body", ["--json-input", JSON.stringify({ id: entry.id })]),
	);
	if (inline !== undefined) {
		return { entry, text: inline, source: "transcript.body" };
	}

	if (entry.resourceId) {
		const resource = readText(
			await rawQuery(options, sessionId, "resource.body", [
				"--json-input",
				JSON.stringify({ resourceId: entry.resourceId }),
			]),
		);
		if (resource !== undefined) {
			return { entry, text: resource, source: "resource.body" };
		}
	}

	if (entry.artifactId) {
		const artifact = readText(
			await rawQuery(options, sessionId, "artifact.read", [
				"--json-input",
				JSON.stringify({ artifactId: entry.artifactId }),
			]),
		);
		if (artifact !== undefined) {
			return { entry, text: artifact, source: "artifact.read" };
		}
	}

	throw new TranscriptIncompleteError(
		`entry ${entry.id} has no readable body via transcript.body, resource.body or artifact.read`,
		0,
	);
}
