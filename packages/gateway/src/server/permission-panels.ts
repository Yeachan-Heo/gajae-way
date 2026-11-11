import { createHash, randomUUID } from "node:crypto";
import { type ChatMessagePayload, type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { TailHandle } from "../orchestrator/tail-runner";

/** How long a permission panel stays answerable before gjc is told the request was cancelled. */
export const PERMISSION_PANEL_TTL_MS = 300_000;

/** Chat platforms whose adapters render interactive approval panels. */
const PANEL_PLATFORMS = new Set(["slack", "discord"]);

export interface PermissionReverseRequest {
	readonly id: string;
	readonly connectionId: string;
	readonly capability: string;
	readonly leaseId: string;
	readonly payload: { method: string; payload: unknown };
}

interface OfferedOption {
	readonly optionId: string;
	readonly name: string;
	readonly kind: string;
}

interface PendingPermissionPanel {
	readonly request: PermissionReverseRequest;
	readonly tail: Pick<TailHandle, "sendReverseResponse">;
	readonly authorId: string;
	readonly originKey: string;
	readonly options: readonly OfferedOption[];
	readonly expiresAt: number;
	readonly digest: string;
}

export type PanelResponseKind = "option_selected" | "approved" | "denied";

export interface PermissionPanelsOptions {
	/** Persists and broadcasts the panel message to the adapters. */
	readonly deliver: (payload: ChatMessagePayload) => void;
	readonly now?: () => number;
	readonly ttlMs?: number;
}

/**
 * Bridges gjc's `permission.request` reverse requests to chat approval panels.
 * gjc accepts only `{ outcome: "cancelled" }` or `{ outcome: "selected", optionId }`
 * naming one of the options it offered, so every answer is picked from those.
 */
export class PermissionPanels {
	readonly #pending = new Map<string, PendingPermissionPanel>();
	readonly #deliver: PermissionPanelsOptions["deliver"];
	readonly #now: () => number;
	readonly #ttlMs: number;

	constructor(options: PermissionPanelsOptions) {
		this.#deliver = options.deliver;
		this.#now = options.now ?? Date.now;
		this.#ttlMs = options.ttlMs ?? PERMISSION_PANEL_TTL_MS;
	}

	get size(): number {
		return this.#pending.size;
	}

	/** Posts a panel for a reverse request, or declines it when no panel can be answered. */
	async open(input: {
		origin: OriginRef | undefined;
		triggerAuthorId: string | undefined;
		tail: Pick<TailHandle, "sendReverseResponse">;
		request: PermissionReverseRequest;
	}): Promise<void> {
		const { origin, triggerAuthorId, tail, request } = input;
		const decline = (message: string) =>
			tail
				.sendReverseResponse({
					id: request.id,
					connectionId: request.connectionId,
					leaseId: request.leaseId,
					error: { code: "unavailable", message },
				})
				.catch(() => {});
		if (request.capability !== "permission" || request.payload.method !== "request")
			return await decline(`${request.capability}.${request.payload.method} not available`);
		if (!origin) return await decline("session origin not found");
		if (!PANEL_PLATFORMS.has(origin.platform)) return await decline("panels not available for this platform");
		if (!triggerAuthorId) return await decline("no trigger author to answer the panel");
		const payload = (request.payload.payload ?? {}) as { toolCall?: unknown; options?: unknown };
		const options = offeredOptions(payload.options);
		if (options.length === 0) return await decline("permission request offered no options");

		const panelId = randomUUID();
		const expiresAt = this.#now() + this.#ttlMs;
		const digest = panelDigest(request, payload.toolCall, options);
		this.#pending.set(panelId, {
			request,
			tail,
			authorId: triggerAuthorId,
			originKey: originKey(origin),
			options,
			expiresAt,
			digest,
		});
		const message = describeToolCall(payload.toolCall);
		this.#deliver({
			turnId: randomUUID(),
			origin,
			role: "assistant",
			text: message,
			final: true,
			deliveryId: randomUUID(),
			approvalPanel: { panelId, message, expiresAt: new Date(expiresAt).toISOString(), digest, options },
		});
	}

	/** Answers gjc for a clicked panel. Returns an error code when nothing was sent. */
	async respond(input: {
		panelId: string;
		responderId: string;
		responseKind: PanelResponseKind;
		selectedOptionId?: string;
		/** Echo of the delivered panel's binding digest; a mismatch is refused. */
		digest?: string;
		/** Origin the response arrived on; it must be the origin the panel was delivered to. */
		originKey?: string;
	}): Promise<string | undefined> {
		const panel = this.#pending.get(input.panelId);
		if (!panel || panel.expiresAt <= this.#now()) return "panel_not_found";
		if (input.responderId !== panel.authorId) return "unauthorized_responder";
		if (input.digest !== panel.digest) return "digest_mismatch";
		if (input.originKey !== undefined && input.originKey !== panel.originKey) return "origin_mismatch";
		const chosen = chooseOption(panel.options, input.responseKind, input.selectedOptionId);
		if (!chosen) return "option_not_offered";
		this.#pending.delete(input.panelId);
		await panel.tail.sendReverseResponse({
			id: panel.request.id,
			connectionId: panel.request.connectionId,
			leaseId: panel.request.leaseId,
			result: { outcome: "selected", optionId: chosen.optionId, kind: chosen.kind },
		});
		return undefined;
	}

	/** Tells gjc every expired request was cancelled so its turn does not hang. */
	async sweep(): Promise<number> {
		const now = this.#now();
		const expired = [...this.#pending].filter(([, panel]) => panel.expiresAt <= now);
		for (const [panelId, panel] of expired) {
			this.#pending.delete(panelId);
			await panel.tail
				.sendReverseResponse({
					id: panel.request.id,
					connectionId: panel.request.connectionId,
					leaseId: panel.request.leaseId,
					result: { outcome: "cancelled" },
				})
				.catch(() => {});
		}
		return expired.length;
	}
}

function offeredOptions(value: unknown): OfferedOption[] {
	if (!Array.isArray(value)) return [];
	return value.flatMap((option) => {
		if (typeof option !== "object" || option === null) return [];
		const { optionId, name, kind } = option as Record<string, unknown>;
		if (typeof optionId !== "string" || typeof kind !== "string") return [];
		return [{ optionId, name: typeof name === "string" ? name : optionId, kind }];
	});
}

/**
 * Binds a panel to the exact reverse request it answers: identity, offered
 * options, and the tool call under review. Hex, 32 chars (128 bits) because a
 * Discord custom_id is capped at 100 chars and must carry panel + option too.
 */
function panelDigest(request: PermissionReverseRequest, toolCall: unknown, options: readonly OfferedOption[]): string {
	const canonical = canonicalJson({
		id: request.id,
		connectionId: request.connectionId,
		leaseId: request.leaseId,
		capability: request.capability,
		method: request.payload.method,
		toolCall: toolCall ?? null,
		options: options.map((option) => [option.optionId, option.name, option.kind]),
	});
	return createHash("sha256").update(canonical).digest("hex").slice(0, 32);
}

/** Stable JSON for hashing: object keys are sorted at every depth. */
function canonicalJson(value: unknown): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	const entries = Object.entries(value as Record<string, unknown>)
		.filter(([, item]) => item !== undefined)
		.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
	return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
}

function chooseOption(
	options: readonly OfferedOption[],
	responseKind: PanelResponseKind,
	selectedOptionId: string | undefined,
): OfferedOption | undefined {
	if (responseKind === "option_selected") return options.find((option) => option.optionId === selectedOptionId);
	// Legacy allow/deny answers still name an EXACT offered kind: never widen an
	// approval to a different policy option (allow_always) when allow_once is
	// absent. No matching exact option means the panel refuses the answer.
	const once = responseKind === "approved" ? "allow_once" : "reject_once";
	return options.find((option) => option.kind === once);
}

function describeToolCall(toolCall: unknown): string {
	if (typeof toolCall === "object" && toolCall !== null) {
		const { title, kind } = toolCall as Record<string, unknown>;
		if (typeof title === "string" && title.trim()) return title;
		if (typeof kind === "string" && kind.trim()) return `Allow ${kind}?`;
	}
	return "Allow this tool call?";
}
