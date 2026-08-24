import { createHash } from "node:crypto";
import { canonicalJson } from "./gates";
import type { MainSessionHost } from "./host";
import type { OwnerSurface, WayProfile } from "../profile";
import { resolveSurface } from "../surface-routing";

export const MAIN_SAY_REPLAY_SCAN_EVENTS = 5_000;
const PERSONA_SAY_RATE_SCAN_EVENTS = 2_000;
const MAIN_SAY_SCOPE = "main.say";
export const PERSONA_SAY_MAX_TEXT_LENGTH = 4_000;
export const PERSONA_SAY_WINDOW_MS = 60_000;
export const PERSONA_SAY_MAX_PER_WINDOW = 20;

export class GatewayToolError extends Error {
	readonly code: number;
	readonly data?: unknown;

	constructor(code: number, message: string, data?: unknown) {
		super(message);
		this.name = "GatewayToolError";
		this.code = code;
		this.data = data;
	}
}

interface JournalFrame {
	readonly seq: string;
	readonly ts: number;
	readonly kind: string;
	readonly payloadJson: string;
}

interface ToolCore {
	idempotencyReplay(input: { scope: string; key: string; requestJson: string }): { replayed: boolean; responseJson?: string };
	idempotencyStore(input: { scope: string; key: string; requestJson: string; responseJson: string }): void;
	journalAppend(kind: string, payloadJson: string): { cursor: string; seq: string };
	journalRead(cursor?: string, limit?: number): { events: JournalFrame[]; nextCursor: string; gap?: unknown };
	journalHeadCursor(): string;
}

export interface GatewayToolControllerOptions {
	readonly core: ToolCore;
	readonly profile: Pick<WayProfile, "knownSurfaces" | "ownerSurfaces">;
	readonly host: Pick<MainSessionHost, "turnOriginSurfaceId">;
	readonly now?: () => number;
}

export interface MainSayRequest {
	readonly text: string;
	readonly surfaceId: string;
	readonly idempotencyKey: string;
}

export interface MainSayResponse {
	readonly accepted: true;
	readonly origin: "persona";
	readonly event_cursor: string;
	readonly event_seq: string;
	readonly idempotency_key: string;
	readonly surface_id: string;
}

interface StoredSayResponse extends MainSayResponse {
	readonly state: "committed";
}

interface ClaimedSayResponse {
	readonly state: "claimed";
	readonly request_hash: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, field: string, maxLength: number): string {
	if (typeof value !== "string") throw new GatewayToolError(-32602, `${field} must be a string.`);
	const normalized = value.trim();
	if (!normalized) throw new GatewayToolError(-32602, `${field} must be non-empty.`);
	if (normalized.length > maxLength) throw new GatewayToolError(-32602, `${field} exceeds the ${maxLength}-character limit.`);
	return normalized;
}

function parseMainSayRequest(value: unknown): MainSayRequest {
	if (!isRecord(value)) throw new GatewayToolError(-32602, "params must be an object.");
	const allowed = new Set(["text", "surface_id", "idempotency_key"]);
	for (const key of Object.keys(value)) if (!allowed.has(key)) throw new GatewayToolError(-32602, `unknown parameter: ${key}`);
	return {
		text: requiredString(value.text, "text", PERSONA_SAY_MAX_TEXT_LENGTH),
		surfaceId: requiredString(value.surface_id, "surface_id", 512),
		idempotencyKey: requiredString(value.idempotency_key, "idempotency_key", 256),
	};
}

function parseCursor(cursor: string): { generation: string; seq: bigint } {
	const match = /^(\d+):(\d+)$/.exec(cursor);
	if (!match) throw new GatewayToolError(1503, "main_say_journal_cursor_invalid");
	return { generation: match[1] as string, seq: BigInt(match[2] as string) };
}

function parseStoredResponse(value: string | undefined): StoredSayResponse | undefined {
	if (!value) return undefined;
	try {
		const parsed = JSON.parse(value) as unknown;
		if (!isRecord(parsed) || parsed.state !== "committed" || parsed.accepted !== true || parsed.origin !== "persona") return undefined;
		if (
			typeof parsed.event_cursor !== "string" ||
			typeof parsed.event_seq !== "string" ||
			typeof parsed.idempotency_key !== "string" ||
			typeof parsed.surface_id !== "string"
		) return undefined;
		return parsed as unknown as StoredSayResponse;
	} catch {
		return undefined;
	}
}

function parseClaimedResponse(value: string | undefined): ClaimedSayResponse | undefined {
	if (!value) return undefined;
	try {
		const parsed = JSON.parse(value) as unknown;
		if (!isRecord(parsed) || parsed.state !== "claimed" || typeof parsed.request_hash !== "string") return undefined;
		return parsed as unknown as ClaimedSayResponse;
	} catch {
		return undefined;
	}
}

function payloadForFrame(frame: JournalFrame): Record<string, unknown> | undefined {
	try {
		const parsed = JSON.parse(frame.payloadJson) as unknown;
		return isRecord(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

function idempotencyConflict(error: unknown): never {
	if (error instanceof GatewayToolError) throw error;
	const message = error instanceof Error ? error.message : String(error);
	if (/idempotency|conflict/i.test(message)) throw new GatewayToolError(1500, "idempotency_conflict");
	throw error;
}

/**
 * Reads a BOUNDED recent window of the durable journal without trusting a local
 * cursor or memory cache.
 *
 * Starting at sequence 0 would make every call cost O(journal size) and, worse,
 * would always cross the retention floor once the journal has rotated past
 * MAX_RETAINED_EVENTS - permanently breaking persona output. The window is
 * therefore anchored to the head cursor. A gap encountered while seeking into
 * that window is expected after rotation and simply moves the window start; the
 * caller's authority for exactly-once remains the durable idempotency store.
 */
function readRecentJournal(core: ToolCore, maxEvents: number): JournalFrame[] {
	const head = parseCursor(core.journalHeadCursor());
	const window = BigInt(maxEvents);
	const start = head.seq > window ? head.seq - window : 0n;
	const events: JournalFrame[] = [];
	let cursor = `${head.generation}:${start}`;
	for (;;) {
		let page: { events: JournalFrame[]; nextCursor: string; gap?: unknown };
		try {
			page = core.journalRead(cursor, 500);
		} catch (error) {
			throw new GatewayToolError(1503, "main_say_journal_unavailable", { detail: error instanceof Error ? error.message : String(error) });
		}
		if (page.gap !== undefined) {
			// The requested start fell below the retention floor. Resync forward to
			// whatever the journal still retains instead of failing the call.
			if (page.nextCursor === cursor) return events;
			cursor = page.nextCursor;
			continue;
		}
		events.push(...page.events);
		if (page.events.length === 0 || page.nextCursor === cursor) return events;
		cursor = page.nextCursor;
	}
}

function findCommittedSay(core: ToolCore, key: string, requestHash: string): StoredSayResponse | undefined {
	// The durable idempotency store is the authority for replay; this bounded scan
	// only recovers the committed frame's cursor for the response.
	for (const frame of readRecentJournal(core, MAIN_SAY_REPLAY_SCAN_EVENTS)) {
		const payload = payloadForFrame(frame);
		if (frame.kind !== "assistant_message" || payload?.origin !== "persona" || payload.idempotency_key !== key) continue;
		if (payload.request_hash !== requestHash || payload.finalized !== true || typeof payload.surface_id !== "string") {
			throw new GatewayToolError(1500, "idempotency_conflict");
		}
		return {
			state: "committed",
			accepted: true,
			origin: "persona",
			event_cursor: `${parseCursor(core.journalHeadCursor()).generation}:${frame.seq}`,
			event_seq: frame.seq,
			idempotency_key: key,
			surface_id: payload.surface_id,
		};
	}
	return undefined;
}

function countRecentPersonaSays(core: ToolCore, now: number): number {
	const cutoff = now - PERSONA_SAY_WINDOW_MS;
	// Only the rate-limit window matters, so the scan stays bounded regardless of
	// how large the journal has grown.
	return readRecentJournal(core, PERSONA_SAY_RATE_SCAN_EVENTS).filter(frame => {
		if (frame.ts < cutoff || frame.kind !== "assistant_message") return false;
		return payloadForFrame(frame)?.origin === "persona";
	}).length;
}

/**
 * Resolved through the shared routing SSOT so persona output and gateway
 * admission cannot disagree about what a surface is. An exact-match-only lookup
 * meant `way_say` refused derived thread/topic surfaces that `main.submit`
 * accepts, so the persona could be spoken TO in a thread but could not answer
 * proactively in one.
 */
function validateSurface(profile: Pick<WayProfile, "knownSurfaces">, surfaceId: string): OwnerSurface {
	const resolved = resolveSurface(surfaceId, new Map(profile.knownSurfaces.map(surface => [surface.id, surface])));
	if (!resolved) throw new GatewayToolError(1300, "unknown_surface");
	return resolved.surface;
}

/** Gateway-mediated tools. This class is deliberately not a generic RPC proxy. */
export class GatewayToolController {
	readonly #core: ToolCore;
	readonly #profile: Pick<WayProfile, "knownSurfaces" | "ownerSurfaces">;
	readonly #host: Pick<MainSessionHost, "turnOriginSurfaceId">;
	readonly #now: () => number;
	#sayQueue: Promise<unknown> = Promise.resolve();

	constructor(options: GatewayToolControllerOptions) {
		this.#core = options.core;
		this.#profile = options.profile;
		this.#host = options.host;
		this.#now = options.now ?? Date.now;
	}

	listSurfaces(): readonly (OwnerSurface & { readonly is_owner: boolean })[] {
		const ownerIds = new Set(this.#profile.ownerSurfaces.map(surface => surface.id));
		return this.#profile.knownSurfaces.map(surface => ({ ...surface, is_owner: ownerIds.has(surface.id) }));
	}

	turnOrigin(): { readonly surface_id: string } {
		const surfaceId = this.#host.turnOriginSurfaceId;
		if (!surfaceId) throw new GatewayToolError(1403, "turn_origin_unavailable");
		return { surface_id: surfaceId };
	}

	say(value: unknown): Promise<MainSayResponse> {
		const request = parseMainSayRequest(value);
		const run = this.#sayQueue.then(() => this.#say(request), () => this.#say(request));
		this.#sayQueue = run.then(() => undefined, () => undefined);
		return run;
	}

	async #say(request: MainSayRequest): Promise<MainSayResponse> {
		const surface = validateSurface(this.#profile, request.surfaceId);
		const requestJson = canonicalJson({ idempotency_key: request.idempotencyKey, surface_id: surface.id, text: request.text });
		const requestHash = createHash("sha256").update(requestJson).digest("hex");
		let existing: ReturnType<ToolCore["idempotencyReplay"]>;
		try {
			existing = this.#core.idempotencyReplay({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson });
		} catch (error) {
			return idempotencyConflict(error);
		}
		if (existing.replayed) {
			const committed = parseStoredResponse(existing.responseJson);
			if (committed) return committed;
			if (!parseClaimedResponse(existing.responseJson)) throw new GatewayToolError(1503, "main_say_idempotency_state_invalid");
			const recovered = findCommittedSay(this.#core, request.idempotencyKey, requestHash);
			if (recovered) {
				this.#core.idempotencyStore({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson, responseJson: canonicalJson(recovered) });
				return recovered;
			}
		} else if (countRecentPersonaSays(this.#core, this.#now()) >= PERSONA_SAY_MAX_PER_WINDOW) {
			throw new GatewayToolError(1404, "persona_output_rate_limited", { window_ms: PERSONA_SAY_WINDOW_MS, max: PERSONA_SAY_MAX_PER_WINDOW });
		}

		const claimed: ClaimedSayResponse = { state: "claimed", request_hash: requestHash };
		try {
			this.#core.idempotencyStore({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson, responseJson: canonicalJson(claimed) });
		} catch (error) {
			let retry: ReturnType<ToolCore["idempotencyReplay"]>;
			try {
				retry = this.#core.idempotencyReplay({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson });
			} catch (retryError) {
				return idempotencyConflict(retryError);
			}
			const committed = parseStoredResponse(retry.responseJson);
			if (committed) return committed;
			if (!retry.replayed) throw error;
		}
		const recovered = findCommittedSay(this.#core, request.idempotencyKey, requestHash);
		if (recovered) {
			this.#core.idempotencyStore({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson, responseJson: canonicalJson(recovered) });
			return recovered;
		}
		if (countRecentPersonaSays(this.#core, this.#now()) >= PERSONA_SAY_MAX_PER_WINDOW) {
			throw new GatewayToolError(1404, "persona_output_rate_limited", { window_ms: PERSONA_SAY_WINDOW_MS, max: PERSONA_SAY_MAX_PER_WINDOW });
		}

		const payload = canonicalJson({
			finalized: true,
			text: request.text,
			surface_id: surface.id,
			origin: "persona",
			persona_initiated: true,
			idempotency_key: request.idempotencyKey,
			request_hash: requestHash,
		});
		const appended = this.#core.journalAppend("assistant_message", payload);
		const response: StoredSayResponse = {
			state: "committed",
			accepted: true,
			origin: "persona",
			event_cursor: appended.cursor,
			event_seq: appended.seq,
			idempotency_key: request.idempotencyKey,
			surface_id: surface.id,
		};
		this.#core.idempotencyStore({ scope: MAIN_SAY_SCOPE, key: request.idempotencyKey, requestJson, responseJson: canonicalJson(response) });
		return response;
	}
}

export function toolSurfaceSchema(): { readonly type: "object"; readonly properties: Record<string, unknown>; readonly required: readonly string[]; readonly additionalProperties: false } {
	return {
		type: "object",
		properties: { surface_id: { type: "string", minLength: 1 }, text: { type: "string", minLength: 1, maxLength: PERSONA_SAY_MAX_TEXT_LENGTH }, idempotency_key: { type: "string", minLength: 1, maxLength: 256 } },
		required: ["surface_id", "text", "idempotency_key"],
		additionalProperties: false,
	};
}