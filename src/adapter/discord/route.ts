import { parseThreadSurfaceId, threadSurfaceId } from "../../surface-routing";
import { acknowledgeDiscordMessage, type DiscordAcknowledgement, type DiscordAcknowledgementOptions } from "./ack";
import type { DiscordMessage, DiscordPlatform } from "./platform";
import type { AdapterPlatformName } from "./platform";
import { rpcResult, type JsonRpcClient } from "../../rpc-client";

export type DiscordRouteKind = "channel" | "dm";
export type DiscordGroupPolicy = "open" | "mention";
/** Legacy engagement vocabulary accepted at the configuration boundary. */
export type DiscordEngagementMode = "mention" | "always";

/** A configured Discord ingress and attributed egress route. */
export interface DiscordRoute {
	readonly surfaceId: string;
	readonly channelId: string;
	/** `channel` permits derived Discord thread routes; `dm` does not. */
	readonly kind?: DiscordRouteKind;
	/** OpenClaw policy: channels default to mention, open responds to all traffic. */
	readonly groupPolicy?: DiscordGroupPolicy;
	/** Legacy alias: `mention` maps to mention and `always` maps to open. */
	readonly engagement?: DiscordEngagementMode;
}

export interface DiscordRouteHandlerOptions {
	readonly routes: readonly DiscordRoute[];
	readonly rpc: JsonRpcClient;
	readonly platform: DiscordPlatform;
	readonly botUserId: string;
	readonly blockedAuthorIds?: readonly string[];
	/** OpenClaw's permissive default; self-authored messages remain refused. */
	readonly allowBots?: boolean;
	readonly acknowledgement?: DiscordAcknowledgementOptions;
	onAccepted?(message: DiscordMessage, journalHeadCursor: unknown, surfaceId?: string): void;
	onAcknowledged?(acknowledgement: DiscordAcknowledgement): void;
	onAcknowledgementFailed?(message: DiscordMessage): void;
	onDiagnostic?(message: string): void;
	/** Test-only wall clock injection for bounded ingress diagnostics. */
	diagnosticNow?: () => number;
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
	readonly #botUserId: string;
	readonly #blockedAuthorIds: ReadonlySet<string>;
	readonly #allowBots: boolean;
	readonly #acknowledgement: DiscordAcknowledgementOptions;
	readonly #onAccepted: ((message: DiscordMessage, journalHeadCursor: unknown, surfaceId?: string) => void) | undefined;
	readonly #onAcknowledged: ((acknowledgement: DiscordAcknowledgement) => void) | undefined;
	readonly #onAcknowledgementFailed: ((message: DiscordMessage) => void) | undefined;
	readonly #diagnostics: DiscordIngressDiagnostics;
	readonly #inFlight = new Map<string, Promise<boolean>>();


	constructor(options: DiscordRouteHandlerOptions) {
		validateDiscordRoutes(options.routes);
		if (!isDiscordSnowflake(options.botUserId)) throw new DiscordRouteError("Discord bot user id must be a snowflake.");
		for (const authorId of options.blockedAuthorIds ?? []) {
			if (!isDiscordSnowflake(authorId)) throw new DiscordRouteError("Discord blocked author ids must be snowflakes.");
		}
		this.#routesByChannel = new Map(options.routes.map(route => [route.channelId, route]));
		this.#rpc = options.rpc;
		this.#platform = options.platform;
		this.#botUserId = options.botUserId;
		this.#blockedAuthorIds = new Set(options.blockedAuthorIds ?? []);
		this.#allowBots = options.allowBots ?? true;
		this.#acknowledgement = options.acknowledgement ?? {};
		this.#onAccepted = options.onAccepted;
		this.#onAcknowledged = options.onAcknowledged;
		this.#onAcknowledgementFailed = options.onAcknowledgementFailed;
		this.#diagnostics = new DiscordIngressDiagnostics(options.onDiagnostic, options.diagnosticNow);
	}

	async handle(message: DiscordMessage): Promise<boolean> {
		if (message.authorId === this.#botUserId || (message.authorBot && !this.#allowBots)) return false;
		if (!message.text.trim()) {
			// Never silent: an empty body on a guild message is the signature of a
			// missing MESSAGE_CONTENT privileged intent, which otherwise looks
			// identical to the bot ignoring its owner.
			this.#diagnostics.drop("empty_text", message.channelId);
			return false;
		}
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
		const ingress = await this.resolveIngressRoute(message);
		if (!ingress) {
			this.#diagnostics.drop("unrouted", message.channelId);
			return false;
		}
		if (message.authorId && this.#blockedAuthorIds.has(message.authorId)) {
			this.#diagnostics.drop("blacklisted", message.channelId);
			return false;
		}
		const text = await this.engagedText(message, ingress);
		if (text === undefined) return false;
		return await this.submitAndAcknowledge(message, ingress.surfaceId, text);
	}

	private async resolveIngressRoute(message: DiscordMessage): Promise<DiscordIngressRoute | undefined> {
		const direct = this.#routesByChannel.get(message.channelId);
		if (direct) return { route: direct, surfaceId: direct.surfaceId, thread: false };

		// Discord MESSAGE_CREATE carries only a thread channel id. Resolve its
		// parent through the platform's cached read-only GET /channels/{id}; no
		// unknown channel is admitted unless that parent is explicitly routed.
		let parentChannelId: string | undefined;
		try {
			parentChannelId = await this.#platform.resolveThreadParent(message.channelId);
		} catch {
			return undefined;
		}
		if (!parentChannelId) return undefined;
		const parent = this.#routesByChannel.get(parentChannelId);
		if (!parent || parent.kind !== "channel") return undefined;
		return { route: parent, surfaceId: discordThreadSurfaceId(parent.surfaceId, message.channelId), thread: true };
	}

	private async engagedText(message: DiscordMessage, ingress: DiscordIngressRoute): Promise<string | undefined> {
		if (ingress.thread || engagementMode(ingress.route) === "always") return message.text;

		if (hasDirectBotMention(message, this.#botUserId)) {
			const text = stripLeadingBotMention(message.text, this.#botUserId);
			if (text.trim()) return text;
			// A bare mention is a deliberate ping and must engage: dropping it made the
			// bot look dead to an owner who simply mentioned it with no words. The
			// original text is forwarded verbatim rather than substituting invented
			// wording, so the persona sees exactly what was sent and nothing is
			// fabricated on the owner's behalf.
			if (message.text.trim()) return message.text;
			this.#diagnostics.drop("empty_after_mention", message.channelId);
			return undefined;
		}

		const reply = await this.replyTargetsBot(message);
		if (reply === "unresolved") {
			this.#diagnostics.drop("unresolved_reference", message.channelId);
			return undefined;
		}
		if (reply) return message.text;
		this.#diagnostics.drop("unengaged", message.channelId);
		return undefined;
	}

	private async replyTargetsBot(message: DiscordMessage): Promise<boolean | "unresolved"> {
		if (message.referencedMessageAuthorId !== undefined) return message.referencedMessageAuthorId === this.#botUserId;
		const reference = message.messageReference;
		if (!reference) return false;
		try {
			const authorId = await this.#platform.resolveMessageAuthor(reference.channelId, reference.messageId);
			return authorId === undefined ? "unresolved" : authorId === this.#botUserId;
		} catch {
			return "unresolved";
		}
	}

	private async submitAndAcknowledge(message: DiscordMessage, surfaceId: string, text: string): Promise<boolean> {
		// `main.submit` exposes no response at its earlier durable-claim boundary.
		// Its accepted response is the adapter's first observable durable ingress
		// boundary, so the acknowledgement budget starts only after that response.
		// A fenced or failed gateway submission remains unacknowledged, preventing
		// a consumed inbound message from receiving typing without a durable turn.
		const response = await this.#rpc.request("main.submit", {
			text,
			surface_id: surfaceId,
			idempotency_key: message.id,
		});
		const result = rpcResult<unknown>(response, "main.submit");
		if (!isRecord(result) || result.accepted !== true) {
			throw new DiscordRouteError("Gateway main.submit returned an invalid acceptance response.");
		}
		this.#onAccepted?.(message, result.journal_head_cursor, surfaceId);
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
	// Delegates to the shared routing SSOT so the derived-surface format cannot
	// drift from what the gateway admits.
	try {
		return threadSurfaceId(parentSurfaceId, threadChannelId);
	} catch (error) {
		throw new DiscordRouteError(error instanceof Error ? error.message : String(error));
	}
}

/** Resolves an attributed surface to a configured channel, including a derived thread channel. */
export function resolveDiscordEgressRoute(routes: readonly DiscordRoute[], surfaceId: string): DiscordRoute | undefined {
	const direct = routes.find(route => route.surfaceId === surfaceId);
	if (direct) return direct;
	// Parsed through the shared SSOT rather than re-deriving the prefix here, so
	// ingress, egress, and gateway admission agree by construction.
	const parsed = parseThreadSurfaceId(surfaceId);
	if (!parsed) return undefined;
	const parent = routes.find(route => route.surfaceId === parsed.parentSurfaceId && route.kind === "channel");
	if (!parent) return undefined;
	return { surfaceId, channelId: parsed.threadId, kind: "channel" };
}

function validateDiscordRoute(route: DiscordRoute): void {
	if (!route.surfaceId.trim()) throw new DiscordRouteError("Discord route surfaceId must not be empty.");
	if (!isDiscordSnowflake(route.channelId)) throw new DiscordRouteError("Discord route channelId must be a Discord snowflake.");
	if (route.kind !== undefined && route.kind !== "channel" && route.kind !== "dm") {
		throw new DiscordRouteError("Discord route kind must be channel or dm.");
	}
	if (route.groupPolicy !== undefined && route.groupPolicy !== "mention" && route.groupPolicy !== "open") {
		throw new DiscordRouteError("Discord route groupPolicy must be mention or open.");
	}
	if (route.engagement !== undefined && route.engagement !== "mention" && route.engagement !== "always") {
		throw new DiscordRouteError("Discord route engagement must be mention or always.");
	}
	if (route.groupPolicy !== undefined && route.engagement !== undefined && route.groupPolicy !== engagementToGroupPolicy(route.engagement)) {
		throw new DiscordRouteError("Discord route groupPolicy conflicts with legacy engagement.");
	}
	if (route.kind === "dm" && (route.groupPolicy !== undefined || route.engagement !== undefined)) {
		throw new DiscordRouteError("Discord dm routes are always engaged and must not configure groupPolicy or engagement.");
	}
}

interface DiscordIngressRoute {
	readonly route: DiscordRoute;
	readonly surfaceId: string;
	readonly thread: boolean;
}

type DiscordIngressDrop = "unrouted" | "blacklisted" | "unengaged" | "unresolved_reference" | "empty_after_mention" | "empty_text";

class DiscordIngressDiagnostics {
	readonly #onDiagnostic: ((message: string) => void) | undefined;
	readonly #now: () => number;
	readonly #lastAt = new Map<DiscordIngressDrop, number>();

	constructor(onDiagnostic: ((message: string) => void) | undefined, now: (() => number) | undefined) {
		this.#onDiagnostic = onDiagnostic;
		this.#now = now ?? Date.now;
	}

	drop(kind: DiscordIngressDrop, channelId: string): void {
		const now = this.#now();
		const previous = this.#lastAt.get(kind);
		if (previous !== undefined && now - previous < DISCORD_INGRESS_DIAGNOSTIC_INTERVAL_MS) return;
		this.#lastAt.set(kind, now);
		const detail = ingressDropDescription(kind);
		try {
			this.#onDiagnostic?.(`discord ingress dropped ${detail} in channel ${boundedDiagnosticValue(channelId)}.`);
		} catch {
			// Diagnostics must not interfere with fail-closed inbound filtering.
		}
	}
}

/** Matches gateway-startup diagnostic throttling so untrusted ingress cannot flood logs. */
const DISCORD_INGRESS_DIAGNOSTIC_INTERVAL_MS = 30_000;

function ingressDropDescription(kind: DiscordIngressDrop): string {
	switch (kind) {
		case "unrouted":
			return "an unrouted message";
		case "blacklisted":
			return "a blacklisted-author message";
		case "unengaged":
			return "an unengaged mention-mode message";
		case "unresolved_reference":
			return "an unengaged message with an unresolved reply reference";
		case "empty_after_mention":
			return "an empty message after leading bot mention stripping";
		case "empty_text":
			return "a message with no readable text (check the MESSAGE_CONTENT privileged intent for guild channels)";
	}
}

function engagementMode(route: DiscordRoute): DiscordEngagementMode {
	if (route.kind === "dm") return "always";
	const policy = route.groupPolicy ?? (route.engagement === undefined ? "mention" : engagementToGroupPolicy(route.engagement));
	return policy === "open" ? "always" : "mention";
}

function engagementToGroupPolicy(engagement: DiscordEngagementMode): DiscordGroupPolicy {
	return engagement === "always" ? "open" : "mention";
}

function hasDirectBotMention(message: DiscordMessage, botUserId: string): boolean {
	return (
		message.mentionedUserIds?.includes(botUserId) === true ||
		message.text.includes(`<@${botUserId}>`) ||
		message.text.includes(`<@!${botUserId}>`)
	);
}

/** Removes only a conventional leading bot mention and its single separator. */
function stripLeadingBotMention(text: string, botUserId: string): string {
	for (const token of [`<@${botUserId}>`, `<@!${botUserId}>`]) {
		if (!text.startsWith(token)) continue;
		const remainder = text.slice(token.length);
		return remainder.startsWith(" ") ? remainder.slice(1) : remainder;
	}
	return text;
}

function boundedDiagnosticValue(value: string): string {
	return value.slice(0, 128);
}

function isDiscordSnowflake(value: string): boolean {
	return /^\d+$/.test(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
