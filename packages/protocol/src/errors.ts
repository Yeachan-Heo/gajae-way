/**
 * Typed protocol error codes. Meanings are stable within a profile major
 * (ARCH-006); additions are allowed, repurposing is not.
 */
export const ERROR_CODES = [
	// transport / framing
	"malformed_frame",
	"unsupported_frame_type",
	"payload_too_large",
	// negotiation
	"negotiation_required",
	"incompatible_profile_version",
	"missing_required_capability",
	// verbs
	"unknown_verb",
	"invalid_params",
	"verb_failed",
	// lifecycle / policy
	"gateway_shutting_down",
	"unauthorized",
	"action_execution_disabled",
	// worker lanes
	"lane_capacity",
] as const;

export type ErrorCode = (typeof ERROR_CODES)[number];

export interface ProtocolErrorPayload {
	readonly code: ErrorCode;
	readonly message: string;
	/** Optional structured detail; never contains secret values. */
	readonly detail?: unknown;
}

export class ProtocolError extends Error {
	readonly code: ErrorCode;
	readonly detail?: unknown;
	constructor(code: ErrorCode, message: string, detail?: unknown) {
		super(message);
		this.name = "ProtocolError";
		this.code = code;
		this.detail = detail;
	}
	toPayload(): ProtocolErrorPayload {
		return { code: this.code, message: this.message, detail: this.detail };
	}
}

export function isErrorCode(value: unknown): value is ErrorCode {
	return typeof value === "string" && (ERROR_CODES as readonly string[]).includes(value);
}
