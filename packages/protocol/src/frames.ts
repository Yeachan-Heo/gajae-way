import { ProtocolError, type ProtocolErrorPayload } from "./errors";

/**
 * Generic action/event envelope (plan §1 driver 1, ARCH-006): fixed in P0 so
 * later phases add typed catalogs additively without changing the frame shape.
 *
 * Wire format: NDJSON — one JSON object per line over stdio or a Unix socket.
 */

/** Hard cap on a single encoded frame (bytes). Oversized frames are rejected typed. */
export const MAX_FRAME_BYTES = 1_048_576;

export type FrameType = "hello" | "negotiated" | "request" | "response" | "event" | "error";

interface FrameBase {
	/** Wire profile version of the sender ("0.1"). */
	readonly v: string;
	readonly type: FrameType;
}

export interface HelloFrame extends FrameBase {
	readonly type: "hello";
	readonly payload: unknown; // HelloPayload, validated at negotiation
}

export interface NegotiatedFrame extends FrameBase {
	readonly type: "negotiated";
	readonly payload: { readonly profileVersion: string; readonly capabilities: readonly string[] };
}

export interface RequestFrame extends FrameBase {
	readonly type: "request";
	/** Client-chosen correlation id, unique per connection. */
	readonly id: string;
	/** Namespaced verb, e.g. "gateway.status", "chat.send". */
	readonly verb: string;
	readonly params?: unknown;
}

export interface ResponseFrame extends FrameBase {
	readonly type: "response";
	readonly id: string;
	readonly result: unknown;
}

export interface EventFrame extends FrameBase {
	readonly type: "event";
	/** Namespaced event name, e.g. "chat.message", "gateway.stopping". */
	readonly event: string;
	/** Present when the event belongs to a request-scoped stream. */
	readonly id?: string;
	readonly payload: unknown;
}

export interface ErrorFrame extends FrameBase {
	readonly type: "error";
	/** Correlates to a request when applicable. */
	readonly id?: string;
	readonly error: ProtocolErrorPayload;
}

export type Frame = HelloFrame | NegotiatedFrame | RequestFrame | ResponseFrame | EventFrame | ErrorFrame;

const FRAME_TYPES: readonly string[] = ["hello", "negotiated", "request", "response", "event", "error"];

export function encodeFrame(frame: Frame): string {
	const line = JSON.stringify(frame);
	if (Buffer.byteLength(line, "utf8") > MAX_FRAME_BYTES) {
		throw new ProtocolError("payload_too_large", `frame exceeds ${MAX_FRAME_BYTES} bytes`);
	}
	return `${line}\n`;
}

/** Decode one NDJSON line into a Frame; throws typed ProtocolError on garbage. */
export function decodeFrame(line: string): Frame {
	if (Buffer.byteLength(line, "utf8") > MAX_FRAME_BYTES) {
		throw new ProtocolError("payload_too_large", `frame exceeds ${MAX_FRAME_BYTES} bytes`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(line);
	} catch {
		throw new ProtocolError("malformed_frame", "frame is not valid JSON");
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new ProtocolError("malformed_frame", "frame is not a JSON object");
	}
	const obj = parsed as Record<string, unknown>;
	if (typeof obj.v !== "string") {
		throw new ProtocolError("malformed_frame", "frame missing profile version field 'v'");
	}
	if (typeof obj.type !== "string" || !FRAME_TYPES.includes(obj.type)) {
		throw new ProtocolError("unsupported_frame_type", `unknown frame type: ${String(obj.type)}`);
	}
	switch (obj.type) {
		case "request":
			if (typeof obj.id !== "string" || typeof obj.verb !== "string") {
				throw new ProtocolError("malformed_frame", "request frame requires string id and verb");
			}
			break;
		case "response":
			if (typeof obj.id !== "string") {
				throw new ProtocolError("malformed_frame", "response frame requires string id");
			}
			break;
		case "event":
			if (typeof obj.event !== "string") {
				throw new ProtocolError("malformed_frame", "event frame requires string event name");
			}
			break;
		case "error":
			if (typeof obj.error !== "object" || obj.error === null) {
				throw new ProtocolError("malformed_frame", "error frame requires error payload");
			}
			break;
		default:
			break;
	}
	return obj as unknown as Frame;
}

/**
 * Incremental NDJSON splitter for stream transports. Feed chunks, get frames.
 * Unknown trailing partial lines are buffered until the next feed.
 */
export class FrameDecoder {
	#buffer = "";

	feed(chunk: string): Frame[] {
		this.#buffer += chunk;
		if (Buffer.byteLength(this.#buffer, "utf8") > MAX_FRAME_BYTES * 2) {
			this.#buffer = "";
			throw new ProtocolError("payload_too_large", "stream buffer exceeded frame budget");
		}
		const frames: Frame[] = [];
		let idx: number;
		// biome-ignore lint/suspicious/noAssignInExpressions: standard splitter loop
		while ((idx = this.#buffer.indexOf("\n")) >= 0) {
			const line = this.#buffer.slice(0, idx).trim();
			this.#buffer = this.#buffer.slice(idx + 1);
			if (line.length === 0) continue;
			frames.push(decodeFrame(line));
		}
		return frames;
	}
}
