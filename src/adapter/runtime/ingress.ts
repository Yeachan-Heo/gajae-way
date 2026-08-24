import { type JsonRpcClient, rpcResult } from "../../rpc-client";
import type { AdapterPlatform, InboundMessage } from "./protocol";

export const DEFAULT_ACK_BUDGET_MS = 2_000;

export class AdapterIngressError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AdapterIngressError";
	}
}

export interface AdapterAcknowledgement {
	readonly platformMsgId: string;
	readonly accepted: boolean;
	readonly elapsedMs: number;
	/** Present when the typing indicator could not be delivered. */
	readonly failure?: string;
}

export interface IngressOptions {
	readonly rpc: JsonRpcClient;
	readonly platform: AdapterPlatform;
	readonly surfaceId: string;
	readonly chatId: string;
	readonly ackBudgetMs?: number;
	readonly now?: () => number;
	onAccepted?(message: InboundMessage, journalHeadCursor: unknown): void;
	onAcknowledged?(acknowledgement: AdapterAcknowledgement): void;
	/** Reports a non-fatal typing failure after a durable acceptance. */
	onAcknowledgementDiagnostic?(message: InboundMessage, reason: string): void;
}

/**
 * Converts one inbound platform message into the gateway's durable idempotent
 * submit call, using the platform message id as the idempotency key.
 *
 * The tiny in-flight map only coalesces concurrent redeliveries from the
 * platform; it is discarded on restart because the server-side idempotency
 * record is the recovery authority.
 */
export class AdapterIngress {
	readonly #options: IngressOptions;
	readonly #inFlight = new Map<string, Promise<boolean>>();

	constructor(options: IngressOptions) {
		if (!options.surfaceId.trim()) throw new AdapterIngressError("Adapter ingress requires a surface id.");
		this.#options = options;
	}

	async handle(message: InboundMessage): Promise<boolean> {
		if (message.chatId !== this.#options.chatId || message.authorBot === true || !message.text.trim()) return false;
		const existing = this.#inFlight.get(message.platformMsgId);
		if (existing) return await existing;
		const handling = this.submitAndAcknowledge(message);
		this.#inFlight.set(message.platformMsgId, handling);
		try {
			return await handling;
		} finally {
			if (this.#inFlight.get(message.platformMsgId) === handling) this.#inFlight.delete(message.platformMsgId);
		}
	}

	private async submitAndAcknowledge(message: InboundMessage): Promise<boolean> {
		// `main.submit` exposes no response at its earlier durable-claim boundary,
		// so its accepted response is the adapter's first observable durable
		// ingress boundary. Acknowledgement therefore begins only after that
		// response: a fenced or rejected submission stays unacknowledged, so a
		// consumed inbound message never receives typing without a durable turn.
		const response = await this.#options.rpc.request("main.submit", {
			text: message.text,
			surface_id: this.#options.surfaceId,
			idempotency_key: message.platformMsgId,
		});
		const result = rpcResult<unknown>(response, "main.submit");
		if (!isRecord(result) || result.accepted !== true) {
			throw new AdapterIngressError("Gateway main.submit returned an invalid acceptance response.");
		}
		this.#options.onAccepted?.(message, result.journal_head_cursor);

		// The gateway has durably accepted, so a typing failure is a diagnostic
		// rather than a message-handling error.
		const acknowledgement = await this.acknowledge(message);
		if (acknowledgement.failure !== undefined) {
			this.#options.onAcknowledgementDiagnostic?.(message, acknowledgement.failure);
		}
		this.#options.onAcknowledged?.(acknowledgement);
		return true;
	}

	private async acknowledge(message: InboundMessage): Promise<AdapterAcknowledgement> {
		const now = this.#options.now ?? Date.now;
		const budgetMs = this.#options.ackBudgetMs ?? DEFAULT_ACK_BUDGET_MS;
		const startedAt = now();
		let failure: string | undefined;
		try {
			await withinBudget(this.#options.platform.typing(this.#options.chatId), budgetMs, message.platformMsgId);
		} catch (error) {
			failure = error instanceof Error ? error.message : String(error);
		}
		const elapsedMs = now() - startedAt;
		if (failure === undefined && elapsedMs > budgetMs) {
			failure = `Adapter acknowledgement exceeded the ${budgetMs}ms diagnostic budget for ${message.platformMsgId}.`;
		}
		return {
			platformMsgId: message.platformMsgId,
			accepted: true,
			elapsedMs,
			...(failure === undefined ? {} : { failure }),
		};
	}
}

function withinBudget(promise: Promise<void>, remainingMs: number, id: string): Promise<void> {
	return new Promise((resolve, reject) => {
		const timeout = setTimeout(
			() => reject(new AdapterIngressError(`Adapter acknowledgement exceeded its deadline for ${id}.`)),
			remainingMs,
		);
		void promise.then(
			() => {
				clearTimeout(timeout);
				resolve();
			},
			(error) => {
				clearTimeout(timeout);
				reject(error);
			},
		);
	});
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
