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
import { deliveryFailureIsAmbiguous, SlackApiError, SlackWebApi } from "./api";
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
	loadRecoveryCursors,
	pruneKnownDms,
	RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS,
	type RecoveryCursorState,
	type RecoveryDelivery,
	RecoveryScheduler,
	recoverConversation,
	recoveryCursorPath,
	rememberKnownDm,
	saveRecoveryCursors,
} from "./recovery";
import { type SlackSlashCommand, SlackSocketMode, type SocketModeOptions } from "./socket";
import { WorkingStatus } from "./status";
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

	/** A send that never reached the gateway must not be remembered as seen. */
	forget(value: string): void {
		this.#values.delete(value);
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

export function replyThreadTs(message: Pick<ChatMessagePayload, "origin" | "replyToMessageId">): string | undefined {
	if (message.origin.kind === "thread") return parseSlackMessageId(message.origin.conversationId)?.ts;
	const target = message.replyToMessageId ? parseSlackMessageId(message.replyToMessageId) : undefined;
	return target?.channel === (message.origin.parentId ?? message.origin.conversationId) ? target.ts : undefined;
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
		const channel =
			message.origin.kind === "thread"
				? (message.origin.parentId ?? message.origin.conversationId)
				: message.origin.conversationId;
		const text = markdownToMrkdwn(
			message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text,
		);
		// Every chunk must stay in the same Slack thread, not just the first chunk.
		for (const chunk of chunkSlackMessage(text)) await api.postMessage(channel, chunk, replyThreadTs(message));
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
		if (target.channel !== (message.origin.parentId ?? message.origin.conversationId))
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
	readonly #inbound = new LruSet();
	readonly #editOutbox = new Map<string, PendingEdit>();
	#editFlush: Promise<void> | undefined;
	#reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	#monitorTimer: ReturnType<typeof setTimeout> | undefined;
	#connectionGeneration = 0;

	/** Runs after every successful (re)connect: recovery re-walks the gap the outage left. */
	onConnected: (() => void) | undefined;

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
		void this.#flushEdits();
		this.onConnected?.();
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
	): Promise<{ readonly verdict: RecoveryDelivery; readonly result?: { engaged?: boolean } }> {
		if (!this.#inbound.addIfAbsent(messageId)) return { verdict: "duplicate" };
		try {
			const result = await this.request<{ engaged?: boolean } | undefined>("chat.send", {
				origin,
				text,
				messageId,
				engagement,
				...(receivedAt ? { receivedAt } : {}),
			});
			if (result?.engaged && addressedTurn(engagement)) this.status?.arm(origin.conversationId);
			return { verdict: "acked", ...(result ? { result } : {}) };
		} catch (error) {
			console.error(`Slack chat.send failed: ${errorText(error)}`);
			this.#inbound.forget(messageId);
			if (!this.#client) this.scheduleReconnect();
			return { verdict: "unavailable" };
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
						if (result?.engaged && addressedTurn(edit.engagement)) this.status?.arm(edit.origin.conversationId);
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
	readonly recovery: RecoveryScheduler;
}> {
	const api = ports.api ?? new SlackWebApi(config.botToken);
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
	// The persist chain keeps cursor writes ordered; a pass joins it before reporting done.
	let cursorSaves: Promise<void> = Promise.resolve();
	const persist = (next: RecoveryCursorState): void => {
		cursors = next;
		cursorSaves = cursorSaves
			.then(() => saveRecoveryCursors(cursorPath, next))
			.catch((error: unknown) => log.error(`Slack recovery cursor persist failed: ${errorText(error)}`));
	};
	const ensureCursors = async (): Promise<RecoveryCursorState | undefined> => {
		if (cursors) return cursors;
		try {
			cursors = await loadRecoveryCursors(cursorPath);
		} catch (error) {
			log.error(`Slack recovery refused: cursor store ${cursorPath} is unusable (${errorText(error)}).`);
		}
		return cursors;
	};
	const rememberDm = async (message: SlackInboundMessage): Promise<void> => {
		if (!isSlackDmChannel(message.channel, message.channel_type)) return;
		const state = await ensureCursors();
		if (state) persist(rememberKnownDm(state, message.channel, now()));
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
				await gateway.requestInbound(
					slackMessageId(message.channel, message.ts),
					admitted.origin,
					text,
					engagementForMessage(message, admitted.origin, identity, directory, config.channels),
					timestamp(message.ts),
				);
			});
		} else if (event.type === "reaction_added" || event.type === "reaction_removed") {
			const description = describeSlackReaction(event as unknown as SlackReactionEvent, identity.botUserId, directory);
			if (description) gateway.sendReaction(description);
		}
	};
	const handleSlashCommand = async (command: SlackSlashCommand): Promise<void> => {
		try {
			if (!["/new", "/reset", "/restart"].includes(command.command)) {
				await api.respond(command.response_url, { response_type: "ephemeral", text: "unknown command" });
				return;
			}
			const origin = slackMessageOrigin({ channel: command.channel_id, user: command.user_id });
			const result = await gateway.requestInbound(`slash-${command.trigger_id}`, origin, command.command, {
				mentioned: true,
				group: origin.kind !== "dm",
				authorId: command.user_id,
				...(command.user_name ? { authorHandle: command.user_name } : {}),
			});
			// Honest ack: the gateway owns command authorization, not the adapter.
			await api.respond(command.response_url, {
				response_type: "ephemeral",
				text: result?.engaged ? "🦞 session reset" : "not authorized for session commands here",
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
	const recoverMissedMessages = async (): Promise<boolean> => {
		if (!gateway.connected) return false;
		const state = await ensureCursors();
		if (!state) return false;
		const nowMs = now();
		const pruned = pruneKnownDms(state, nowMs);
		if (pruned !== state) persist(pruned);
		const targets = [...new Set([...Object.keys(config.channels ?? {}), ...Object.keys(cursors?.knownDms ?? {})])];
		let completed = true;
		const port = {
			history: (channel: string, options: { oldest: string; cursor?: string; limit: number }) =>
				api.conversationsHistory(channel, options),
			replies: (channel: string, threadTs: string, options: { oldest: string; cursor?: string; limit: number }) =>
				api.conversationsReplies(channel, threadTs, options),
		};
		const current = (): RecoveryCursorState => cursors ?? state;
		for (const channel of targets) {
			if ((current().quarantined[channel]?.failures ?? 0) >= RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS) continue;
			const outcome = await recoverConversation(port, channel, {
				cursor: current().recoveredThrough[channel],
				nowMs,
				botUserId: identity.botUserId,
				deliver: async (message) => {
					const admitted = decideInbound(message, identity, directory, config.channels);
					if (!admitted) return "skip";
					await prime(message);
					const text = renderInboundText(message, directory);
					if (text === "") return "skip";
					const sent = await gateway.requestRecovered(
						slackMessageId(message.channel, message.ts),
						admitted.origin,
						text,
						engagementForMessage(message, admitted.origin, identity, directory, config.channels),
						timestamp(message.ts),
					);
					return sent.verdict;
				},
			});
			if (outcome.advancedTo)
				persist({ ...current(), recoveredThrough: { ...current().recoveredThrough, [channel]: outcome.advancedTo } });
			if (outcome.failed) {
				completed = false;
				if (outcome.permanent) {
					// A channel the bot cannot read stops driving the retry loop after a few
					// strikes, but is probed again on the next connect in case access returned.
					const prior = current().quarantined[channel];
					persist({
						...current(),
						quarantined: {
							...current().quarantined,
							[channel]: {
								reason: outcome.fetchError ?? "unreadable",
								failures: (prior?.failures ?? 0) + 1,
								since: prior?.since ?? new Date(nowMs).toISOString(),
							},
						},
					});
					log.error(`Slack recovery cannot read ${channel}: ${outcome.fetchError ?? "unreadable"}`);
				} else log.error(`Slack recovery incomplete for ${channel}: ${outcome.fetchError ?? "gateway unavailable"}`);
			} else if (current().quarantined[channel]) {
				const { [channel]: _cleared, ...quarantined } = current().quarantined;
				persist({ ...current(), quarantined });
			}
			if (outcome.truncated) completed = false;
		}
		await cursorSaves;
		return completed;
	};
	const recovery = new RecoveryScheduler(recoverMissedMessages);
	gateway.onConnected = () => recovery.trigger();
	await gateway.connect();
	const socket = new SlackSocketMode(
		() => api.connectionsOpen(config.appToken),
		{
			onEvent: handleEvent,
			onSlashCommand: handleSlashCommand,
			onConnected: () => {
				log.log("Slack adapter connected.");
				recovery.trigger();
			},
			onDisconnected: (reason) => log.log(`Slack socket disconnected: ${reason}`),
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
		recovery,
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
