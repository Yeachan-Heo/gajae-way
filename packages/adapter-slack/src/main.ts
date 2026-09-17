import { join } from "node:path";
import type {
	ChannelEngagementPolicy,
	ChatMessagePayload,
	ChatProgressPayload,
	EngagementContext,
	OriginRef,
} from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import pkg from "../package.json";
import { deliveryFailureIsAmbiguous, OutboundLimiter, SlackApiError, SlackWebApi } from "./api";
import { describeInboundBody, type SlackFileCarrier } from "./attachments";
import { SlackDirectory } from "./author";
import { adapterHome, type LoadedSlackAdapterConfig, loadSlackAdapterConfig } from "./config";
import { AdapterAlreadyRunningError, AdapterLock } from "./lock";
import { chunkSlackMessage, markdownToMrkdwn } from "./mrkdwn";
import {
	isSlackDmChannel,
	parseSlackMessageId,
	type SlackMessageOriginShape,
	slackMessageId,
	slackMessageOrigin,
} from "./origin";
import {
	describeSlackReaction,
	type SlackReactionDescription,
	type SlackReactionEvent,
	slackReactionFor,
} from "./reactions";
import {
	classifyRecoveryFailure,
	clearAttempt,
	loadRecoveryCursors,
	pruneKnownDms,
	pruneParticipatedThreads,
	RECOVERY_MAX_ATTEMPTS,
	RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS,
	type RecoveryCursorState,
	type RecoveryDelivery,
	RecoveryScheduler,
	recordAttempt,
	recordDeadLetter,
	recoverConversation,
	recoverThread,
	recoveryCursorPath,
	rememberKnownDm,
	rememberParticipatedThread,
	saveRecoveryCursors,
} from "./recovery";
import { type SlackSlashCommand, SlackSocketMode, type SocketModeOptions } from "./socket";
import { isPresenceReaction, WorkingStatus } from "./status";
import { mentionedUserIds, normalizeSlackText } from "./text";

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	onChatProgress?(handler: (progress: ChatProgressPayload) => void): () => void;
	close?(): Promise<void>;
}

export interface SlackInboundMessage extends SlackMessageOriginShape, SlackFileCarrier {
	readonly type?: string;
	readonly subtype?: string;
	readonly ts: string;
	readonly text?: string | null;
	readonly user?: string;
	readonly bot_id?: string;
	readonly username?: string;
	readonly thread_ts?: string;
	readonly parent_user_id?: string;
	readonly edited?: { readonly user?: string; readonly ts?: string };
	readonly message?: SlackInboundMessage;
	readonly previous_message?: SlackInboundMessage;
	readonly hidden?: boolean;
}

export interface SlackIdentity {
	readonly botUserId: string;
	readonly botId?: string;
	readonly teamName?: string;
}

export interface SlackNames {
	userName(id: string): string | undefined;
	userHandle(id: string): string | undefined;
	channelName(id: string): string | undefined;
}

export const SKIPPED_SUBTYPES: ReadonlySet<string> = new Set([
	"channel_join",
	"channel_leave",
	"channel_topic",
	"channel_purpose",
	"channel_name",
	"channel_archive",
	"channel_unarchive",
	"group_join",
	"group_leave",
	"group_topic",
	"group_purpose",
	"group_name",
	"message_deleted",
	"message_replied",
	"tombstone",
	"ekm_access_denied",
	"pinned_item",
	"unpinned_item",
]);

type Channels = Readonly<Record<string, ChannelEngagementPolicy>> | undefined;

export function engagementForMessage(
	message: SlackInboundMessage,
	origin: OriginRef,
	identity: SlackIdentity,
	names: SlackNames,
	channels: Channels,
): EngagementContext {
	const replyTo =
		message.thread_ts && message.thread_ts !== message.ts
			? {
					messageId: slackMessageId(message.channel, message.thread_ts),
					...(message.parent_user_id
						? { authorId: message.parent_user_id, fromSelf: message.parent_user_id === identity.botUserId }
						: {}),
				}
			: undefined;
	// A reply to our own message addresses us just like an explicit mention.
	const mentioned =
		[...(message.text ?? "").matchAll(/<@([^>|]+)(?:\|[^>]*)?>/g)].some((match) => match[1] === identity.botUserId) ||
		replyTo?.fromSelf === true ||
		(origin.kind !== "dm" && channels?.[origin.parentId ?? origin.conversationId]?.engagement === "open");
	const authorName = (message.user ? names.userName(message.user) : undefined) ?? message.username;
	const authorHandle = message.user ? names.userHandle(message.user) : undefined;
	const channelName = origin.kind !== "dm" ? names.channelName(message.channel) : undefined;
	return {
		mentioned,
		group: origin.kind !== "dm",
		authorId: message.user ?? message.bot_id ?? "",
		...(message.bot_id || message.subtype === "bot_message" ? { authorIsBot: true } : {}),
		...(authorName ? { authorName } : {}),
		...(authorHandle ? { authorHandle } : {}),
		...(channelName ? { channelLabel: `#${channelName}` } : {}),
		...(identity.teamName ? { serverLabel: identity.teamName } : {}),
		...(replyTo ? { replyTo } : {}),
	};
}

/** Only transport admission belongs here; authorization remains the gateway's decision. */
export function decideInbound(
	message: SlackInboundMessage,
	identity: SlackIdentity,
	names: SlackNames,
	channels: Channels,
): { readonly origin: OriginRef; readonly engagement: EngagementContext } | undefined {
	if (
		!message.ts ||
		!message.channel ||
		message.user === identity.botUserId ||
		(identity.botId && message.bot_id === identity.botId) ||
		message.hidden === true ||
		message.subtype === "message_changed" ||
		SKIPPED_SUBTYPES.has(message.subtype ?? "") ||
		!(message.user ?? message.bot_id)
	)
		return undefined;
	const origin = slackMessageOrigin(message);
	return { origin, engagement: engagementForMessage(message, origin, identity, names, channels) };
}

export function renderInboundText(message: SlackInboundMessage, names: SlackNames): string {
	return describeInboundBody({ text: normalizeSlackText(message.text ?? "", names), files: message.files });
}

export interface PendingEdit {
	readonly messageId: string;
	readonly origin: OriginRef;
	readonly text: string;
	readonly engagement: EngagementContext;
	readonly receivedAt?: string;
}
export interface DescribedMessageEdit extends PendingEdit {
	readonly receivedAt: string;
}

export function describeMessageEdit(
	event: SlackInboundMessage,
	identity: SlackIdentity,
	names: SlackNames,
	channels: Channels,
): DescribedMessageEdit | undefined {
	if (event.subtype !== "message_changed" || !event.message) return undefined;
	const message = { ...event.message, channel: event.channel };
	const admitted = decideInbound(message, identity, names, channels);
	if (!admitted) return undefined;
	const text = renderInboundText(message, names);
	// Link previews and other metadata changes are not edits of what the user said.
	if (text === "" || (event.previous_message && renderInboundText(event.previous_message, names) === text))
		return undefined;
	return {
		...admitted,
		messageId: slackMessageId(message.channel, message.ts),
		text,
		receivedAt: timestamp(message.edited?.ts ?? event.ts),
	};
}

export function addressedTurn(engagement: Pick<EngagementContext, "group" | "mentioned">): boolean {
	return !engagement.group || engagement.mentioned;
}

/** Bounded inbound identity memory prevents replay/reconnect duplicate turns. */
export class LruSet {
	readonly #values = new Map<string, undefined>();
	constructor(readonly limit = 10_000) {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("Slack LRU limit must be a positive integer");
	}
	addIfAbsent(value: string): boolean {
		const present = this.#values.has(value);
		this.#values.delete(value);
		this.#values.set(value, undefined);
		if (this.#values.size > this.limit) this.#values.delete(this.#values.keys().next().value as string);
		return !present;
	}

	has(value: string): boolean {
		return this.#values.has(value);
	}
}

/** Reserve arrival order before asynchronous directory work; unrelated conversations stay parallel. */
export class OrderedIngress {
	private readonly chains = new Map<string, Promise<void>>();
	run(key: string, task: () => Promise<void>): void {
		const previous = this.chains.get(key) ?? Promise.resolve();
		// A rejected task must not poison the chain for later messages.
		const next = previous
			.then(task)
			.catch((error: unknown) => console.error(`Slack ingress failed: ${errorText(error)}`));
		this.chains.set(key, next);
		void next.then(() => {
			if (this.chains.get(key) === next) this.chains.delete(key);
		});
	}
	async drain(): Promise<void> {
		while (this.chains.size > 0) await Promise.all([...this.chains.values()]);
	}
}

/** The Slack channel a delivery for this origin is posted in. */
export function deliveryChannel(origin: OriginRef): string {
	return origin.kind === "thread" ? (origin.parentId ?? origin.conversationId) : origin.conversationId;
}

/**
 * Resolves where a reply is posted: inside its thread, or at the top level.
 *
 * Throws a definitive `SlackApiError` when the routing intent cannot be honoured
 * - a thread origin whose id is not a `channel:ts` pair, or an explicit reply
 * target that is malformed or lives in another channel. Posting at the top level
 * instead would silently answer in the wrong place and confirm success for it.
 */
export function replyThreadTs(message: Pick<ChatMessagePayload, "origin" | "replyToMessageId">): string | undefined {
	const channel = deliveryChannel(message.origin);
	if (message.origin.kind === "thread") {
		const root = parseSlackMessageId(message.origin.conversationId);
		if (!root || root.channel !== channel)
			throw new SlackApiError(0, "invalid_target", `Slack thread origin ${message.origin.conversationId} is malformed`);
		return root.ts;
	}
	if (message.replyToMessageId === undefined) return undefined;
	const target = parseSlackMessageId(message.replyToMessageId);
	if (!target) throw new SlackApiError(0, "invalid_target", "Slack reply target has a malformed message id");
	if (target.channel !== channel)
		throw new SlackApiError(0, "invalid_target", "Slack reply target belongs to a foreign channel");
	return target.ts;
}

export async function settleSlackDelivery(
	gateway: Pick<GatewayClientLike, "request">,
	api: Pick<SlackWebApi, "postMessage" | "addReaction">,
	message: ChatMessagePayload,
	_log: Pick<Console, "error"> = console,
	status?: Pick<WorkingStatus, "clear">,
): Promise<void> {
	if (message.origin.platform !== "slack" || !message.deliveryId) return;
	if (message.reaction) {
		try {
			await settleSlackReaction(gateway, api, message);
		} finally {
			await status?.clear(message.origin.conversationId).catch(() => {});
		}
		return;
	}
	const deliveryId = message.deliveryId;
	try {
		const channel = deliveryChannel(message.origin);
		// Routing is decided before any write, so a bad target never half-posts.
		const threadTs = replyThreadTs(message);
		const text = markdownToMrkdwn(
			message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text,
		);
		// Every chunk must stay in the same Slack thread, not just the first chunk.
		for (const chunk of chunkSlackMessage(text)) await api.postMessage(channel, chunk, threadTs);
		// voiceText is intentionally ignored: Slack has no bot voice messages.
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: errorText(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	} finally {
		// Cosmetic cleanup must never turn a confirmed Slack delivery into a failure.
		await status?.clear(message.origin.conversationId).catch(() => {});
	}
}

export async function settleSlackReaction(
	gateway: Pick<GatewayClientLike, "request">,
	api: Pick<SlackWebApi, "addReaction">,
	message: ChatMessagePayload,
): Promise<void> {
	if (message.origin.platform !== "slack" || !message.deliveryId || !message.reaction) return;
	const deliveryId = message.deliveryId;
	try {
		const target = parseSlackMessageId(message.reaction.targetMessageId);
		if (!target) throw new SlackApiError(0, "invalid_target", "Slack reaction target has a malformed message id");
		if (target.channel !== deliveryChannel(message.origin))
			throw new SlackApiError(0, "invalid_target", "Slack reaction target belongs to a foreign channel");
		await api.addReaction(target.channel, target.ts, slackReactionFor(message.reaction));
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: errorText(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	}
}

export function subscribeSlackDeliveries(
	gateway: GatewayClientLike,
	api: Pick<SlackWebApi, "postMessage" | "addReaction">,
	log: Pick<Console, "error"> = console,
	status?: Pick<WorkingStatus, "clear">,
): () => void {
	return gateway.onChatMessage((message) => {
		void settleSlackDelivery(gateway, api, message, log, status).catch((error) =>
			log.error(`Slack delivery settlement request failed: ${errorText(error)}`),
		);
	});
}

export function subscribeSlackProgress(
	gateway: GatewayClientLike,
	status: Pick<WorkingStatus, "update" | "clear">,
	log: Pick<Console, "error"> = console,
): () => void {
	if (!gateway.onChatProgress) return () => {};
	return gateway.onChatProgress((progress) => {
		if (progress.origin.platform !== "slack") return;
		// Final arrives even when a turn delivers nothing; delivery-only cleanup leaves silent turns orphaned.
		const action = progress.final ? status.clear(progress.origin.conversationId) : status.update(progress);
		void action.catch((error) =>
			log.error(`Slack working status ${progress.final ? "clear" : "update"} failed: ${errorText(error)}`),
		);
	});
}

/** A single slow status probe must not tear down a healthy delivery subscription. */
export function monitorFailureDecision(
	strikes: number,
): { action: "retry"; strikes: number } | { action: "reconnect" } {
	return strikes + 1 >= 3 ? { action: "reconnect" } : { action: "retry", strikes: strikes + 1 };
}

export class ReconnectingGateway implements GatewayClientLike {
	#client: GatewayClientLike | undefined;
	#reconnecting = false;
	#attempt = 0;
	#deliveryOff: (() => void) | undefined;
	#handlers = new Set<(message: ChatMessagePayload) => void>();
	/** Ids the gateway has acknowledged (or reported as already known). */
	readonly #inbound = new LruSet();
	/**
	 * Sends in flight, keyed by message id. A recovery pass that meets a live send
	 * for the same id must wait for THAT outcome rather than assume it succeeded:
	 * calling it a duplicate and advancing the watermark past it would lose the
	 * message if the live send then fails before the gateway accepts it.
	 */
	readonly #pendingSends = new Map<string, Promise<RecoveredSend>>();
	readonly #editOutbox = new Map<string, PendingEdit>();
	#editFlush: Promise<void> | undefined;
	#reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	#monitorTimer: ReturnType<typeof setTimeout> | undefined;
	#connectionGeneration = 0;

	/** Runs after every successful (re)connect: recovery re-walks the gap the outage left. */
	onConnected: (() => void) | undefined;
	/** Runs when the link is lost, so the outage length can gate the next recovery pass. */
	onDisconnected: (() => void) | undefined;

	constructor(
		readonly socketPath: string,
		readonly api: Pick<SlackWebApi, "postMessage" | "addReaction">,
		initialClient?: GatewayClientLike,
		readonly status?: WorkingStatus,
	) {
		if (initialClient) this.adoptClient(initialClient);
	}

	/** True while a gateway client is attached; recovery sends are pointless without one. */
	get connected(): boolean {
		return this.#client !== undefined;
	}

	adoptClient(client: GatewayClientLike): void {
		++this.#connectionGeneration;
		clearTimeout(this.#reconnectTimer);
		clearTimeout(this.#monitorTimer);
		this.#reconnecting = false;
		this.#client = client;
		this.#attempt = 0;
		this.#deliveryOff?.();
		const off = subscribeSlackDeliveries(client, this.api, console, this.status);
		const progressOff = this.status ? subscribeSlackProgress(client, this.status) : undefined;
		const handlersOff = client.onChatMessage((message) => {
			for (const handler of this.#handlers) handler(message);
		});
		this.#deliveryOff = () => {
			off();
			handlersOff();
			progressOff?.();
		};
		this.monitor(client);
		// Queued edits first: a backfilled message must not overtake an edit the
		// user made before the link came back. Recovery starts once they drained.
		const generation = this.#connectionGeneration;
		void this.#drainEdits().then(() => {
			if (this.#client === client && generation === this.#connectionGeneration) this.onConnected?.();
		});
	}

	/**
	 * Flushes until the outbox is empty or the link is gone. A flush that was
	 * already running against the previous (dead) client returns as soon as it
	 * notices, so one round is not enough right after a reconnect.
	 */
	async #drainEdits(): Promise<void> {
		for (;;) {
			await this.#flushEdits();
			if (this.#editOutbox.size === 0 || !this.#client || this.#editFlush) return;
			if (this.#reconnecting) return;
			// Something was queued or a stale flush just ended: go again.
			const before = this.#editOutbox.size;
			await this.#flushEdits();
			if (this.#editOutbox.size >= before) return;
		}
	}

	async connect(): Promise<void> {
		const generation = ++this.#connectionGeneration;
		try {
			const client = await GajaewayClient.connectSocket(this.socketPath);
			if (generation !== this.#connectionGeneration) {
				await client.close();
				return;
			}
			this.adoptClient(client);
			console.log("Slack adapter connected to gateway.");
		} catch {
			if (generation === this.#connectionGeneration) this.scheduleReconnect();
		}
	}

	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		const client = this.#client;
		if (!client) throw new Error("Slack gateway is not connected");
		try {
			return await client.request<T>(verb, params);
		} catch (error) {
			if (this.#client === client) this.scheduleReconnect();
			throw error;
		}
	}

	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void {
		this.#handlers.add(handler);
		return () => {
			this.#handlers.delete(handler);
		};
	}

	async requestInbound(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
	): Promise<{ engaged?: boolean } | undefined> {
		const sent = await this.requestRecovered(messageId, origin, text, engagement, receivedAt);
		return sent.verdict === "acked" ? sent.result : undefined;
	}

	/**
	 * The recovery-facing send: same LRU dedupe, same chat.send verb, but the
	 * outcome is classified so a watermark can only advance past a message the
	 * gateway actually acknowledged (or one it already knew). An `unavailable`
	 * verdict leaves the id out of the LRU so the next pass retries it.
	 */
	async requestRecovered(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
	): Promise<RecoveredSend> {
		const pending = this.#pendingSends.get(messageId);
		// Joining an in-flight send shares its verdict; a second caller never claims
		// its own "acked" for a send it did not make.
		if (pending) {
			const shared = await pending;
			// A failed send is returned as-is so the joiner can classify it; only a
			// successful send becomes "duplicate" for the second caller.
			return shared.verdict === "unavailable" ? shared : { verdict: "duplicate" };
		}
		if (this.#inbound.has(messageId)) return { verdict: "duplicate" };
		const attempt = (async (): Promise<RecoveredSend> => {
			try {
				const result = await this.request<{ engaged?: boolean } | undefined>("chat.send", {
					origin,
					text,
					messageId,
					engagement,
					...(receivedAt ? { receivedAt } : {}),
				});
				this.#inbound.addIfAbsent(messageId);
				if (result?.engaged && addressedTurn(engagement)) this.status?.arm(origin, messageId);
				return { verdict: "acked", ...(result ? { result } : {}) };
			} catch (error) {
				console.error(`Slack chat.send failed: ${errorText(error)}`);
				if (!this.#client) this.scheduleReconnect();
				return { verdict: "unavailable", failure: error };
			}
		})();
		this.#pendingSends.set(messageId, attempt);
		try {
			return await attempt;
		} finally {
			if (this.#pendingSends.get(messageId) === attempt) this.#pendingSends.delete(messageId);
		}
	}

	sendInbound(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
	): void {
		void this.requestInbound(messageId, origin, text, engagement, receivedAt);
	}

	/** Edits bypass inbound dedupe; the gateway owns idempotency by message + content. */
	sendEdit(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
	): void {
		this.#editOutbox.set(messageId, { messageId, origin, text, engagement, ...(receivedAt ? { receivedAt } : {}) });
		if (this.#editOutbox.size > 256) {
			const oldest = this.#editOutbox.keys().next().value as string;
			this.#editOutbox.delete(oldest);
			console.error(`Slack edit outbox full; dropped the oldest queued edit (message ${oldest}).`);
		}
		void this.#flushEdits();
	}
	get pendingEdits(): readonly PendingEdit[] {
		return [...this.#editOutbox.values()];
	}

	async #flushEdits(): Promise<void> {
		if (this.#editFlush) return this.#editFlush;
		// Defer the drain so synchronous exit cannot leave a completed promise latched.
		this.#editFlush = Promise.resolve()
			.then(async () => {
				for (;;) {
					const edit = this.#editOutbox.values().next().value as PendingEdit | undefined;
					if (!edit) return;
					const client = this.#client;
					if (!client) {
						this.scheduleReconnect();
						return;
					}
					try {
						const result = await client.request<{ engaged?: boolean } | undefined>("chat.edit", edit);
						if (result?.engaged && addressedTurn(edit.engagement)) this.status?.arm(edit.origin, edit.messageId);
						// A superseding edit queued during the request must drain in this pass too.
						if (this.#editOutbox.get(edit.messageId) === edit) this.#editOutbox.delete(edit.messageId);
					} catch (error) {
						console.error(`Slack chat.edit failed; edit of ${edit.messageId} kept for replay: ${errorText(error)}`);
						if (this.#client === client) {
							this.scheduleReconnect();
							return;
						}
					}
				}
			})
			.finally(() => {
				this.#editFlush = undefined;
			});
		return this.#editFlush;
	}

	sendReaction(description: SlackReactionDescription): void {
		// Rejected engagement metadata is not a link failure; the monitor owns reconnects.
		void this.#client
			?.request("engagement.reaction", description)
			.catch((error) => console.error(`Slack engagement.reaction failed: ${errorText(error)}`));
	}

	private monitor(client: GatewayClientLike, strikes = 0): void {
		this.#monitorTimer = setTimeout(
			() => {
				if (this.#client !== client) return;
				void client.request("gateway.status").then(
					() => {
						if (this.#client === client) this.monitor(client);
					},
					() => {
						if (this.#client !== client) return;
						const next = monitorFailureDecision(strikes);
						if (next.action === "reconnect") this.scheduleReconnect();
						else this.monitor(client, next.strikes);
					},
				);
			},
			strikes === 0 ? 30_000 : 5_000,
		);
		this.#monitorTimer.unref?.();
	}

	private scheduleReconnect(): void {
		if (this.#reconnecting) return;
		this.#reconnecting = true;
		this.onDisconnected?.();
		this.#client = undefined;
		this.#deliveryOff?.();
		clearTimeout(this.#monitorTimer);
		const delay = Math.min(30_000, 500 * 2 ** Math.min(this.#attempt++, 6));
		const jitter = Math.floor(Math.random() * Math.max(1, delay / 4));
		console.log(`Slack adapter gateway reconnecting in ${delay + jitter}ms.`);
		this.#reconnectTimer = setTimeout(() => {
			this.#reconnecting = false;
			void this.connect();
		}, delay + jitter);
		this.#reconnectTimer.unref?.();
	}
}

export async function startSlackAdapter(
	config: LoadedSlackAdapterConfig,
	ports: {
		api?: SlackWebApi;
		socketFactory?: SocketModeOptions["factory"];
		log?: Pick<Console, "log" | "error">;
		recoveryCursorPath?: string;
		now?: () => number;
	} = {},
): Promise<{
	readonly socket: SlackSocketMode;
	readonly gateway: ReconnectingGateway;
	readonly identity: SlackIdentity;
	readonly directory: SlackDirectory;
	readonly ingress: OrderedIngress;
	readonly handleEvent: (event: Record<string, unknown>) => Promise<void>;
	readonly handleSlashCommand: (command: SlackSlashCommand) => Promise<void>;
	/** One bounded catch-up pass over configured channels and known DMs; true when it finished cleanly. */
	readonly recoverMissedMessages: () => Promise<boolean>;
	/** Resets quarantine strike counts so the next pass probes unreadable channels again. */
	readonly reprobeQuarantined: () => Promise<void>;
	readonly recovery: RecoveryScheduler;
	/** Outage bookkeeping for the recovery gate; the socket and gateway handlers call these. */
	readonly links: {
		readonly disconnected: (link: "socket" | "gateway") => void;
		readonly reconnected: (link: "socket" | "gateway") => void;
	};
}> {
	const api = ports.api ?? new SlackWebApi(config.botToken, { limiter: new OutboundLimiter() });
	const log = ports.log ?? console;
	const auth = await api.authTest();
	const identity: SlackIdentity = {
		botUserId: auth.user_id,
		...(auth.bot_id ? { botId: auth.bot_id } : {}),
		...(auth.team ? { teamName: auth.team } : {}),
	};
	const directory = new SlackDirectory(api);
	const status = new WorkingStatus(api, log);
	const gateway = new ReconnectingGateway(
		config.gatewaySocket ?? join(adapterHome(), "gateway.sock"),
		api,
		undefined,
		status,
	);
	const ingress = new OrderedIngress();
	const now = ports.now ?? Date.now;
	const cursorPath = ports.recoveryCursorPath ?? recoveryCursorPath();
	let cursors: RecoveryCursorState | undefined;
	// Single-flight load: two first-contact DMs arriving together must not each
	// load an empty store and then overwrite one another's registration.
	let cursorLoad: Promise<RecoveryCursorState | undefined> | undefined;
	// The persist chain keeps cursor writes ordered. `dirty` marks state that is in
	// memory but not yet on disk: a pass may not report completion while it is
	// set, otherwise a disk fault silently strands a watermark or a known DM.
	let cursorSaves: Promise<void> = Promise.resolve();
	let dirty = false;
	const ensureCursors = (): Promise<RecoveryCursorState | undefined> => {
		if (cursors) return Promise.resolve(cursors);
		cursorLoad ??= loadRecoveryCursors(cursorPath)
			.then((loaded) => {
				cursors = loaded;
				return loaded;
			})
			.catch((error: unknown) => {
				log.error(`Slack recovery refused: cursor store ${cursorPath} is unusable (${errorText(error)}).`);
				return undefined;
			})
			.finally(() => {
				cursorLoad = undefined;
			});
		return cursorLoad;
	};
	const persist = (next: RecoveryCursorState): void => {
		cursors = next;
		dirty = true;
		cursorSaves = cursorSaves
			.then(async () => {
				await saveRecoveryCursors(cursorPath, next);
				if (cursors === next) dirty = false;
			})
			.catch((error: unknown) => log.error(`Slack recovery cursor persist failed: ${errorText(error)}`));
	};
	/** Applies a mutation to the CURRENT state after the load settled, never to a stale snapshot. */
	const mutate = async (change: (state: RecoveryCursorState) => RecoveryCursorState): Promise<void> => {
		const state = await ensureCursors();
		if (!state) return;
		const next = change(cursors ?? state);
		if (next !== (cursors ?? state)) persist(next);
	};
	const rememberDm = (message: SlackInboundMessage): Promise<void> => {
		if (!isSlackDmChannel(message.channel, message.channel_type)) return Promise.resolve();
		return mutate((state) => rememberKnownDm(state, message.channel, now()));
	};
	/** A thread the persona is answering in stays recoverable after its parent falls behind the watermark. */
	const rememberThread = (origin: OriginRef): Promise<void> => {
		if (origin.kind !== "thread") return Promise.resolve();
		return mutate((state) => rememberParticipatedThread(state, origin.conversationId, now()));
	};
	const prime = async (message: SlackInboundMessage): Promise<void> => {
		await Promise.all([
			...(message.user ? [directory.user(message.user)] : []),
			...(!isSlackDmChannel(message.channel, message.channel_type) ? [directory.conversation(message.channel)] : []),
			...mentionedUserIds(message.text ?? "")
				.slice(0, 10)
				.map((id) => directory.user(id)),
		]);
	};
	const handleEvent = async (event: Record<string, unknown>): Promise<void> => {
		// app_mention also arrives as message; forwarding both would double-turn.
		if (event.type === "message") {
			const envelope = event as unknown as SlackInboundMessage;
			const message =
				envelope.subtype === "message_changed" && envelope.message
					? { ...envelope.message, channel: envelope.channel }
					: envelope;
			const admitted = decideInbound(message, identity, directory, config.channels);
			if (!admitted) return;
			ingress.run(admitted.origin.conversationId, async () => {
				await prime(message);
				// DMs cannot be enumerated from Slack's history API without a channel
				// id, so live traffic records the bounded set recovery will revisit.
				await rememberDm(message);
				if (envelope.subtype === "message_changed") {
					const edit = describeMessageEdit(envelope, identity, directory, config.channels);
					if (edit) gateway.sendEdit(edit.messageId, edit.origin, edit.text, edit.engagement, edit.receivedAt);
					return;
				}
				const text = renderInboundText(message, directory);
				if (text === "") return;
				const engagement = engagementForMessage(message, admitted.origin, identity, directory, config.channels);
				const result = await gateway.requestInbound(
					slackMessageId(message.channel, message.ts),
					admitted.origin,
					text,
					engagement,
					timestamp(message.ts),
				);
				if (result?.engaged) await rememberThread(admitted.origin);
			});
		} else if (event.type === "reaction_added" || event.type === "reaction_removed") {
			const reaction = event as unknown as SlackReactionEvent;
			// Our own presence markers are not engagement, even if the identity check
			// ever misses (e.g. a legacy bot user id): never report them inbound.
			if (reaction.user === identity.botUserId && isPresenceReaction(reaction.reaction)) return;
			const description = describeSlackReaction(reaction, identity.botUserId, directory);
			if (description) gateway.sendReaction(description);
		}
	};
	const handleSlashCommand = async (command: SlackSlashCommand): Promise<void> => {
		try {
			if (!SLASH_COMMANDS.has(command.command)) {
				await api.respond(command.response_url, { response_type: "ephemeral", text: "unknown command" });
				return;
			}
			const origin = slackMessageOrigin({ channel: command.channel_id, user: command.user_id });
			const sent = await gateway.requestRecovered(`slash-${command.trigger_id}`, origin, command.command, {
				mentioned: true,
				group: origin.kind !== "dm",
				authorId: command.user_id,
				...(command.user_name ? { authorHandle: command.user_name } : {}),
			});
			// Honest ack: the gateway owns command authorization, not the adapter, and
			// "we could not ask" is a different answer from "it said no".
			await api.respond(command.response_url, {
				response_type: "ephemeral",
				text: slashCommandAck(command.command, sent),
			});
		} catch (error) {
			log.error(`Slack slash command failed: ${errorText(error)}`);
		}
	};
	/**
	 * Bounded catch-up for messages missed while the socket or the gateway link was
	 * down. Replays through the same decideInbound -> chat.send path as live events;
	 * the gateway dedupes durably on `channel:ts`, so overlap with live traffic is
	 * safe. The watermark moves only past messages the gateway acked or already knew.
	 */
	// Recovery is a burst of history reads per configured channel. A socket
	// blip that reconnects within seconds cannot have created a gap the gateway
	// does not already dedupe, so a pass is skipped when the previous clean pass
	// is recent and no outage longer than RECOVERY_OUTAGE_GATE_MS was observed.
	let lastCleanPassAt: number | undefined;
	// Socket and gateway links drop independently; each outage is timed on its own
	// so a long socket outage is not erased by a quick gateway reconnect.
	const disconnectedAt: { socket?: number; gateway?: number } = {};
	let longOutageSeen = false;
	const recoverMissedMessages = async (): Promise<boolean> => {
		if (!gateway.connected) return false;
		const loaded = await ensureCursors();
		if (!loaded) return false;
		const nowMs = now();
		const current = (): RecoveryCursorState => cursors ?? loaded;
		await mutate((state) => pruneParticipatedThreads(pruneKnownDms(state, nowMs), nowMs));
		const channels = [...new Set([...Object.keys(config.channels ?? {}), ...Object.keys(current().knownDms)])];
		let completed = true;
		const port = {
			history: (channel: string, options: { oldest: string; latest?: string; cursor?: string; limit: number }) =>
				api.conversationsHistory(channel, options),
			replies: (channel: string, threadTs: string, options: { oldest: string; cursor?: string; limit: number }) =>
				api.conversationsReplies(channel, threadTs, options),
		};
		const deliver = async (message: SlackInboundMessage): Promise<RecoveryDelivery> => {
			const admitted = decideInbound(message, identity, directory, config.channels);
			if (!admitted) return "skip";
			await prime(message);
			const text = renderInboundText(message, directory);
			if (text === "") return "skip";
			const messageId = slackMessageId(message.channel, message.ts);
			const sent = await gateway.requestRecovered(
				messageId,
				admitted.origin,
				text,
				engagementForMessage(message, admitted.origin, identity, directory, config.channels),
				timestamp(message.ts),
			);
			if (sent.verdict !== "unavailable") {
				await mutate((state) => clearAttempt(state, messageId));
				if (sent.verdict === "acked" && sent.result?.engaged) await rememberThread(admitted.origin);
				return sent.verdict;
			}
			// Only a payload-specific refusal burns this message's budget; a link outage
			// says nothing about the message and leaves the cursor exactly where it is.
			const classification = classifyRecoveryFailure(sent.failure);
			const reason = errorText(sent.failure);
			let exhausted = false;
			await mutate((state) => {
				const recorded = recordAttempt(state, messageId, message.channel, classification, reason, nowMs);
				exhausted = recorded.exhausted;
				return recorded.state;
			});
			if (!exhausted) return "unavailable";
			log.error(`Slack recovery discarded ${messageId} after repeated ${classification} failures: ${reason}`);
			await mutate((state) =>
				recordDeadLetter(state, {
					messageId,
					conversationId: message.channel,
					classification,
					attempts: RECOVERY_MAX_ATTEMPTS,
					reason,
					at: new Date(nowMs).toISOString(),
				}),
			);
			return "discard";
		};
		for (const channel of channels) {
			if ((current().quarantined[channel]?.failures ?? 0) >= RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS) continue;
			const resume = current().continuation[channel];
			const outcome = await recoverConversation(port, channel, {
				cursor: current().recoveredThrough[channel],
				...(resume ? { latest: resume.olderThan } : {}),
				nowMs,
				botUserId: identity.botUserId,
				deliver,
			});
			if (outcome.failed) {
				completed = false;
				if (outcome.permanent) {
					// A channel the bot cannot read stops driving the retry loop after a few
					// strikes; the next socket/gateway connect resets the count and probes again.
					await mutate((state) => ({
						...state,
						quarantined: {
							...state.quarantined,
							[channel]: {
								reason: outcome.fetchError ?? "unreadable",
								failures: (state.quarantined[channel]?.failures ?? 0) + 1,
								since: state.quarantined[channel]?.since ?? new Date(nowMs).toISOString(),
							},
						},
					}));
					log.error(`Slack recovery cannot read ${channel}: ${outcome.fetchError ?? "unreadable"}`);
				} else log.error(`Slack recovery incomplete for ${channel}: ${outcome.fetchError ?? "gateway unavailable"}`);
				continue;
			}
			await mutate((state) => {
				const { [channel]: _cleared, ...quarantined } = state.quarantined;
				const { [channel]: _done, ...continuation } = state.continuation;
				if (outcome.truncated && outcome.continuation) {
					// The newest suffix of the gap is delivered; resume BELOW it next pass so
					// an oversized gap drains in bounded slices instead of refetching forever.
					// The watermark waits until the gap closes.
					return {
						...state,
						quarantined,
						continuation: {
							...continuation,
							[channel]: { olderThan: outcome.continuation.olderThan, through: outcome.continuation.through },
						},
					};
				}
				// Gap closed: the watermark becomes the newest ts of the whole gap, which is
				// the continuation's `through` when this was the last slice.
				const through = resume?.through ?? outcome.advancedTo;
				return {
					...state,
					quarantined,
					continuation,
					...(through ? { recoveredThrough: { ...state.recoveredThrough, [channel]: through } } : {}),
				};
			});
			if (outcome.truncated) completed = false;
			// A reply walk that hit its bound is drained per root on later passes,
			// whether or not the persona engaged in anything fetched so far: the
			// unread reply that mentions it may be exactly the one still unfetched.
			for (const threadTs of outcome.truncatedThreads ?? []) {
				completed = false;
				await mutate((state) => ({
					...state,
					pendingThreads: {
						...state.pendingThreads,
						[slackMessageId(channel, threadTs)]: { since: new Date(nowMs).toISOString() },
					},
				}));
			}
		}
		// Threads walked on their own bounded cursor: ones the persona took part in
		// (their parents may be below the channel watermark by now) and ones whose
		// reply walk was cut short above.
		const threadKeys = new Set([
			...Object.keys(current().participatedThreads),
			...Object.keys(current().pendingThreads),
		]);
		for (const threadKey of threadKeys) {
			const root = parseSlackMessageId(threadKey);
			if (!root) continue;
			if ((current().quarantined[root.channel]?.failures ?? 0) >= RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS) continue;
			// The root's cursor lives on whichever record knows it; an unengaged pending
			// root advances its own so repeated bounded walks drain toward the tail.
			const cursor = current().participatedThreads[threadKey]?.through ?? current().pendingThreads[threadKey]?.through;
			const outcome = await recoverThread(port, root.channel, root.ts, {
				cursor,
				nowMs,
				botUserId: identity.botUserId,
				deliver,
			});
			if (outcome.failed) {
				completed = false;
				if (outcome.permanent) {
					// The thread is gone or unreadable: forget it rather than strike the channel.
					await mutate((state) => {
						const { [threadKey]: _gone, ...participatedThreads } = state.participatedThreads;
						const { [threadKey]: _pending, ...pendingThreads } = state.pendingThreads;
						return { ...state, participatedThreads, pendingThreads };
					});
				}
				continue;
			}
			await mutate((state) => {
				// Only `through` moves here, and only on an entry that still exists: a
				// live message may have refreshed lastSeenAt (or the prune may have
				// removed the entry) while this walk was in flight, and the captured
				// snapshot must neither clobber that nor resurrect the entry.
				const live = state.participatedThreads[threadKey];
				const participatedThreads =
					live && outcome.advancedTo
						? { ...state.participatedThreads, [threadKey]: { ...live, through: outcome.advancedTo } }
						: state.participatedThreads;
				const { [threadKey]: pending, ...pendingThreads } = state.pendingThreads;
				return {
					...state,
					participatedThreads,
					// A truncated walk keeps its pending entry, advanced to what it walked past.
					pendingThreads:
						outcome.truncated && pending
							? {
									...pendingThreads,
									[threadKey]: { ...pending, ...(outcome.advancedTo ? { through: outcome.advancedTo } : {}) },
								}
							: pendingThreads,
				};
			});
			if (outcome.truncated) completed = false;
		}
		await cursorSaves;
		// Progress that is not on disk is not progress: keep retrying until it is.
		const clean = completed && !dirty;
		if (clean) {
			lastCleanPassAt = now();
			longOutageSeen = false;
		}
		return clean;
	};
	const noteDisconnected = (link: "socket" | "gateway"): void => {
		disconnectedAt[link] ??= now();
	};
	const noteConnected = (link: "socket" | "gateway"): void => {
		const since = disconnectedAt[link];
		if (since !== undefined && now() - since >= RECOVERY_OUTAGE_GATE_MS) longOutageSeen = true;
		disconnectedAt[link] = undefined;
	};
	/**
	 * A fresh link is the moment to re-check channels that were unreadable: the
	 * operator may have fixed scopes or membership since. Their strike count is
	 * reset so the next pass actually probes them again.
	 */
	const reprobeQuarantined = (): Promise<void> =>
		mutate((state) => {
			if (Object.keys(state.quarantined).length === 0) return state;
			const quarantined = Object.fromEntries(
				Object.entries(state.quarantined).map(([channel, entry]) => [channel, { ...entry, failures: 0 }]),
			);
			return { ...state, quarantined };
		});
	// A reconnect requests a reprobe; the flag is consumed by the next scheduled
	// pass (so a coalesced trigger cannot skip it) and NOT re-armed by the
	// scheduler's own retries, otherwise strikes would reset on every retry and a
	// channel could never actually reach quarantine.
	let reprobeRequested = false;
	const recovery = new RecoveryScheduler(async () => {
		// A short blip after a recent clean pass is not worth a burst of history
		// reads - unless there are quarantined channels to re-probe, which is the
		// one thing only a reconnect can do for them. An explicit call to
		// recoverMissedMessages is never gated.
		const hasQuarantine = Object.keys(cursors?.quarantined ?? {}).length > 0;
		if (
			!longOutageSeen &&
			!(reprobeRequested && hasQuarantine) &&
			lastCleanPassAt !== undefined &&
			now() - lastCleanPassAt < RECOVERY_RECENT_PASS_MS
		)
			return true;
		if (reprobeRequested) {
			reprobeRequested = false;
			await reprobeQuarantined();
		}
		return recoverMissedMessages();
	});
	const reconnected = (link: "socket" | "gateway"): void => {
		noteConnected(link);
		reprobeRequested = true;
		recovery.trigger();
	};
	gateway.onConnected = () => reconnected("gateway");
	gateway.onDisconnected = () => noteDisconnected("gateway");
	await gateway.connect();
	const socket = new SlackSocketMode(
		() => api.connectionsOpen(config.appToken),
		{
			onEvent: handleEvent,
			onSlashCommand: handleSlashCommand,
			onConnected: () => {
				log.log("Slack adapter connected.");
				reconnected("socket");
			},
			onDisconnected: (reason) => {
				log.log(`Slack socket disconnected: ${reason}`);
				noteDisconnected("socket");
			},
		},
		{ factory: ports.socketFactory, log },
	);
	log.log("Slack adapter starting.");
	await socket.start();
	return {
		socket,
		gateway,
		identity,
		directory,
		ingress,
		handleEvent,
		handleSlashCommand,
		recoverMissedMessages,
		reprobeQuarantined,
		recovery,
		links: { disconnected: noteDisconnected, reconnected },
	};
}

function timestamp(ts: string): string {
	return new Date(Number(ts) * 1000).toISOString();
}
function errorText(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

export const SLACK_USAGE = [
	"usage: gajaeway-slack [--help] [--version]",
	"",
	"Runs the Slack adapter in the foreground. Configuration is read from",
	"$GAJAEWAY_HOME/adapter-slack.json; one instance at a time per home.",
].join("\n");
export const USAGE_EXIT_CODE = 2;
const SLASH_COMMANDS: ReadonlySet<string> = new Set(["/new", "/reset", "/restart"]);
/** A clean recovery pass younger than this is not repeated for a short blip. */
export const RECOVERY_RECENT_PASS_MS = 60_000;
/** An outage at least this long always earns a fresh recovery pass. */
export const RECOVERY_OUTAGE_GATE_MS = 5_000;

/**
 * What the invoking user is told. Wording follows what the gateway actually did:
 * `/restart` restarts the process and keeps sessions, so it never claims a reset;
 * a link that could not carry the command is reported as such, not as a denial.
 */
export function slashCommandAck(command: string, sent: Pick<RecoveredSend, "verdict" | "result">): string {
	if (sent.verdict === "unavailable") return "the gateway is unreachable right now; try again shortly";
	if (sent.verdict === "duplicate") return "already handled";
	if (!sent.result?.engaged) return "not authorized for session commands here";
	return command === "/restart" ? "🦞 restarting the gateway" : "🦞 session reset";
}

/** Outcome of one gateway send, classified for recovery; `failure` carries the raw error for classification. */
export interface RecoveredSend {
	readonly verdict: RecoveryDelivery;
	readonly result?: { engaged?: boolean };
	readonly failure?: unknown;
}

export type SlackArgv =
	| { readonly kind: "run" }
	| { readonly kind: "help" }
	| { readonly kind: "version" }
	| { readonly kind: "usage"; readonly message: string };

/** Resolve before taking a lock or opening either connection. */
export function parseSlackArgs(args: readonly string[]): SlackArgv {
	if (args.length === 0) return { kind: "run" };
	if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) return { kind: "help" };
	if (args.length === 1 && (args[0] === "--version" || args[0] === "-v")) return { kind: "version" };
	return { kind: "usage", message: `gajaeway-slack: unexpected argument ${args[0]}\n${SLACK_USAGE}` };
}

if (import.meta.main) {
	const argv = parseSlackArgs(process.argv.slice(2));
	if (argv.kind === "help") console.log(SLACK_USAGE);
	else if (argv.kind === "version") console.log(pkg.version);
	else if (argv.kind === "usage") {
		console.error(argv.message);
		process.exit(USAGE_EXIT_CODE);
	} else {
		// Refuse a second instance before config or connections can affect the resident one.
		AdapterLock.acquire(adapterHome())
			.then(async (lock) => {
				// Signal handlers suppress default termination, so release and exit explicitly.
				const release = (): void => void lock.release().finally(() => process.exit(0));
				process.once("SIGINT", release);
				process.once("SIGTERM", release);
				await startSlackAdapter(await loadSlackAdapterConfig());
			})
			.catch((error) => {
				console.error(`Slack adapter startup failed: ${errorText(error)}`);
				process.exitCode = error instanceof AdapterAlreadyRunningError ? USAGE_EXIT_CODE : 1;
			});
	}
}
