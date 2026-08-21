import { acknowledgeDiscordMessage, type DiscordAcknowledgement, type DiscordAcknowledgementOptions } from "./ack";
import type { DiscordMessage, DiscordPlatform } from "./platform";
import { rpcResult, type JsonRpcClient } from "../../rpc-client";

/** One static v1 owner-DM route from Discord channel to configured surface. */
export interface DiscordRoute {
	readonly surfaceId: string;
	readonly channelId: string;
}

export interface DiscordRouteHandlerOptions {
	readonly route: DiscordRoute;
	readonly rpc: JsonRpcClient;
	readonly platform: DiscordPlatform;
	readonly acknowledgement?: DiscordAcknowledgementOptions;
	onAcknowledged?(acknowledgement: DiscordAcknowledgement): void;
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
	readonly #route: DiscordRoute;
	readonly #rpc: JsonRpcClient;
	readonly #platform: DiscordPlatform;
	readonly #acknowledgement: DiscordAcknowledgementOptions;
	readonly #onAcknowledged: ((acknowledgement: DiscordAcknowledgement) => void) | undefined;
	readonly #inFlight = new Map<string, Promise<boolean>>();

	constructor(options: DiscordRouteHandlerOptions) {
		validateRoute(options.route);
		this.#route = options.route;
		this.#rpc = options.rpc;
		this.#platform = options.platform;
		this.#acknowledgement = options.acknowledgement ?? {};
		this.#onAcknowledged = options.onAcknowledged;
	}

	async handle(message: DiscordMessage): Promise<boolean> {
		if (message.channelId !== this.#route.channelId || message.authorBot || !message.text.trim()) return false;
		const existing = this.#inFlight.get(message.id);
		if (existing) return await existing;
		const handling = this.submitAndAcknowledge(message);
		this.#inFlight.set(message.id, handling);
		try {
			return await handling;
		} finally {
			if (this.#inFlight.get(message.id) === handling) this.#inFlight.delete(message.id);
		}
	}

	private async submitAndAcknowledge(message: DiscordMessage): Promise<boolean> {
		// Admission is the sole acknowledgement authority. A fenced or failed
		// gateway submission leaves the Discord event unacknowledged, preventing a
		// consumed inbound message from receiving typing without a durable turn.
		const response = await this.#rpc.request("main.submit", {
			text: message.text,
			surface_id: this.#route.surfaceId,
			idempotency_key: message.id,
		});
		const result = rpcResult<unknown>(response, "main.submit");
		if (!isRecord(result) || result.accepted !== true) {
			throw new DiscordRouteError("Gateway main.submit returned an invalid acceptance response.");
		}
		const acknowledgement = await acknowledgeDiscordMessage(this.#platform, message, this.#acknowledgement);
		this.#onAcknowledged?.(acknowledgement);
		return true;
	}
}

function validateRoute(route: DiscordRoute): void {
	if (!route.surfaceId.trim()) throw new DiscordRouteError("Discord route surfaceId must not be empty.");
	if (!/^\d+$/.test(route.channelId)) throw new DiscordRouteError("Discord route channelId must be a Discord snowflake.");
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
