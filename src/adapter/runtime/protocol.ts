/**
 * The out-of-process adapter protocol.
 *
 * Adapters are separate processes that hold NO durable delivery state: the
 * gateway's consumer checkpoint is the sole recovery authority. This module
 * declares the contract every adapter implements so the shape is enforced by
 * the compiler and by one shared conformance suite, rather than existing only
 * as prose plus a single hand-rolled implementation.
 */

/**
 * Protocol version negotiated at startup.
 *
 * An adapter whose version does not match the gateway's must exit before
 * issuing any `consumer.claim`, so a mismatched build can never settle
 * deliveries it may misinterpret.
 */
export const ADAPTER_PROTOCOL_VERSION = 1;

export class AdapterProtocolUnsupportedError extends Error {
	readonly expected: number;
	readonly actual: number;

	constructor(expected: number, actual: number) {
		super(`Adapter protocol version ${actual} is unsupported; this adapter speaks version ${expected}.`);
		this.name = "AdapterProtocolUnsupportedError";
		this.expected = expected;
		this.actual = actual;
	}
}

export function negotiateProtocolVersion(gatewayVersion: unknown): number {
	// An absent version means a gateway that predates negotiation; treat it as
	// version 1 rather than failing closed on a field that was never sent.
	const actual =
		gatewayVersion === undefined || gatewayVersion === null ? ADAPTER_PROTOCOL_VERSION : Number(gatewayVersion);
	if (!Number.isSafeInteger(actual) || actual !== ADAPTER_PROTOCOL_VERSION) {
		throw new AdapterProtocolUnsupportedError(ADAPTER_PROTOCOL_VERSION, Number(gatewayVersion));
	}
	return actual;
}

/**
 * How a platform suppresses duplicate sends.
 *
 * `platform_nonce` platforms accept a caller-supplied nonce and deduplicate
 * server-side, giving effectively exactly-once delivery. `at_least_once`
 * platforms offer no such key, so a duplicate is possible across a process
 * crash or two overlapping adapter processes. The protocol guarantees the
 * send-before-commit ordering invariant for both; it does not pretend the
 * weaker platforms are exactly-once.
 */
export type AdapterDedupeMode = "platform_nonce" | "at_least_once";

export interface InboundMessage {
	readonly platformMsgId: string;
	readonly chatId: string;
	readonly text: string;
	readonly senderId: string;
	readonly authorBot?: boolean;
}

export interface SendResult {
	/** Absent on `at_least_once` platforms that return no usable id. */
	readonly platformMsgId?: string;
}

/**
 * The port between the shared adapter runtime and one platform driver.
 *
 * Everything the runtime needs from a platform is here, so a new adapter is a
 * driver plus configuration rather than a re-implementation of the delivery
 * protocol.
 */
export interface AdapterPlatform {
	readonly dedupe: AdapterDedupeMode;
	start(onMessage: (message: InboundMessage) => Promise<void>): Promise<void>;
	stop(): Promise<void>;
	send(chatId: string, text: string, options: { readonly nonce?: string }): Promise<SendResult>;
	/** Post-acceptance acknowledgement; a no-op on platforms without one. */
	ack(chatId: string, platformMsgId: string): Promise<void>;
	typing(chatId: string): Promise<void>;
	onDisconnect(handler: (reason: string) => void): void;
}

/** Static description used by the conformance suite and by startup diagnostics. */
export interface AdapterDescriptor {
	readonly name: string;
	readonly consumerId: string;
	readonly dedupe: AdapterDedupeMode;
}
