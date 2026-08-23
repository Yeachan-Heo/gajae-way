import { acknowledgeDiscordMessage, type DiscordAcknowledgement, type DiscordAcknowledgementOptions } from "./ack";
import type { DiscordMessage, DiscordPlatform } from "./platform";
import { rpcResult, type JsonRpcClient } from "../../rpc-client";

export type DiscordRouteKind = "channel" | "dm";

/** A configured Discord ingress and attributed egress route. */
export interface DiscordRoute {
	readonly surfaceId: string;
	readonly channelId: string;
	/** `channel` permits derived Discord thread routes; `dm` does not. */
	readonly kind?: DiscordRouteKind;
}

export interface DiscordRouteHandlerOptions {
	readonly routes: readonly DiscordRoute[];
	readonly rpc: JsonRpcClient;
	readonly platform: DiscordPlatform;
	readonly acknowledgement?: DiscordAcknowledgementOptions;
	onAccepted?(message: DiscordMessage, journalHeadCursor: unknown): void;
	onAcknowledged?(acknowledgement: DiscordAcknowledgement): void;
	onAcknowledgementFailed?(message: DiscordMessage): void;
}

export class DiscordRouteError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordRouteError";
	}
}

/**
 * Converts MESSAGE_CREATE to the gateway's durable idempotent submit call.
 * A platform message id is the idempotency key. The tiny in-flight map only
 * coalesces concurrent gateway replays; it is discarded on restart so the
 * server-side idempotency record remains the recovery authority.
 */
export class DiscordRouteHandler {
	readonly #routesByChannel: ReadonlyMap<string, DiscordRoute>;
	readonly #rpc: JsonRpcClient;
	readonly #platform: DiscordPlatform;
	readonly #acknowledgement: DiscordAcknowledgementOptions;
	readonly #onAccepted: ((message: DiscordMessage, journalHeadCursor: unknown) => void) | undefined;
	readonly #onAcknowledged: ((acknowledgement: DiscordAcknowledgement) => void) | undefined;
	readonly #onAcknowledgementFailed: ((message: DiscordMessage) => void) | undefined;
	readonly #inFlight = new Map<string, Promise<boolean>>();

	constructor(options: DiscordRouteHandlerOptions) {
		validateDiscordRoutes(options.routes);
		this.#routesByChannel = new Map(options.routes.map(route => [route.channelId, route]));
		this.#rpc = options.rpc;
		this.#platform = options.platform;
		this.#acknowledgement = options.acknowledgement ?? {};
		this.#onAccepted = options.onAccepted;
		this.#onAcknowledged = options.onAcknowledged;
		this.#onAcknowledgementFailed = options.onAcknowledgementFailed;
	}

	async handle(message: DiscordMessage): Promise<boolean> {
		if (message.authorBot || !message.text.trim()) return false;
		const existing = this.#inFlight.get(message.id);
		if (existing) return await existing;
		const handling = this.resolveAndSubmit(message);
		this.#inFlight.set(message.id, handling);
		try {
			return await handling;
		} finally {
			if (this.#inFlight.get(message.id) === handling) this.#inFlight.delete(message.id);
		}
	}

	private async resolveAndSubmit(message: DiscordMessage): Promise<boolean> {
		const surfaceId = await this.surfaceIdForMessage(message);
		if (!surfaceId) return false;
		return await this.submitAndAcknowledge(message, surfaceId);
	}

	private async surfaceIdForMessage(message: DiscordMessage): Promise<string | undefined> {
		const direct = this.#routesByChannel.get(message.channelId);
		if (direct) return direct.surfaceId;

		// Discord MESSAGE_CREATE carries only a thread channel id. Resolve its
		// parent through the platform's cached read-only GET /channels/{id}; no
		// unknown channel is admitted unless that parent is explicitly routed.
		const parentChannelId = await this.#platform.resolveThreadParent(message.channelId);
		if (!parentChannelId) return undefined;
		const parent = this.#routesByChannel.get(parentChannelId);
		if (!parent || parent.kind !== "channel") return undefined;
		return discordThreadSurfaceId(parent.surfaceId, message.channelId);
	}

	private async submitAndAcknowledge(message: DiscordMessage, surfaceId: string): Promise<boolean> {
		// `main.submit` exposes no response at its earlier durable-claim boundary.
		// Its accepted response is the adapter's first observable durable ingress
		// boundary, so the acknowledgement budget starts only after that response.
		// A fenced or failed gateway submission remains unacknowledged, preventing
		// a consumed inbound message from receiving typing without a durable turn.
		const response = await this.#rpc.request("main.submit", {
			text: message.text,
			surface_id: surfaceId,
			idempotency_key: message.id,
		});
		const result = rpcResult<unknown>(response, "main.submit");
		if (!isRecord(result) || result.accepted !== true) {
			throw new DiscordRouteError("Gateway main.submit returned an invalid acceptance response.");
		}
		this.#onAccepted?.(message, result.journal_head_cursor);
		let acknowledgement: DiscordAcknowledgement;
		try {
			acknowledgement = await acknowledgeDiscordMessage(this.#platform, message, this.#acknowledgement);
		} catch (error) {
			this.#onAcknowledgementFailed?.(message);
			throw error;
		}
		this.#onAcknowledged?.(acknowledgement);
		return true;
	}
}

export function validateDiscordRoutes(routes: readonly DiscordRoute[]): void {
	if (!Array.isArray(routes) || routes.length === 0) throw new DiscordRouteError("Discord routing requires at least one route.");
	const channelIds = new Set<string>();
	const surfaceIds = new Set<string>();
	for (const route of routes) {
		validateDiscordRoute(route);
		if (channelIds.has(route.channelId)) throw new DiscordRouteError(`Discord routes duplicate channel ${route.channelId}.`);
		if (surfaceIds.has(route.surfaceId)) throw new DiscordRouteError(`Discord routes duplicate surface ${route.surfaceId}.`);
		channelIds.add(route.channelId);
		surfaceIds.add(route.surfaceId);
	}
}

export function discordThreadSurfaceId(parentSurfaceId: string, threadChannelId: string): string {
	if (!parentSurfaceId.trim()) throw new DiscordRouteError("Discord thread parent surface id must not be empty.");
	if (!isDiscordSnowflake(threadChannelId)) throw new DiscordRouteError("Discord thread channel id must be a snowflake.");
	return `${parentSurfaceId}/thread:${threadChannelId}`;
}

/** Resolves an attributed surface to a configured channel, including a derived thread channel. */
export function resolveDiscordEgressRoute(routes: readonly DiscordRoute[], surfaceId: string): DiscordRoute | undefined {
	const direct = routes.find(route => route.surfaceId === surfaceId);
	if (direct) return direct;
	const threadRoutes: DiscordRoute[] = [];
	for (const route of routes) {
		if (route.kind !== "channel") continue;
		const prefix = `${route.surfaceId}/thread:`;
		if (!surfaceId.startsWith(prefix)) continue;
		const threadChannelId = surfaceId.slice(prefix.length);
		if (isDiscordSnowflake(threadChannelId)) threadRoutes.push({ surfaceId, channelId: threadChannelId, kind: "channel" });
	}
	return threadRoutes.length === 1 ? threadRoutes[0] : undefined;
}

function validateDiscordRoute(route: DiscordRoute): void {
	if (!route.surfaceId.trim()) throw new DiscordRouteError("Discord route surfaceId must not be empty.");
	if (!isDiscordSnowflake(route.channelId)) throw new DiscordRouteError("Discord route channelId must be a Discord snowflake.");
	if (route.kind !== undefined && route.kind !== "channel" && route.kind !== "dm") {
		throw new DiscordRouteError("Discord route kind must be channel or dm.");
	}
}

function isDiscordSnowflake(value: string): boolean {
	return /^\d+$/.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
