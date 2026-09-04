/**
 * Shared error boundary for ElevenLabs HTTP responses.
 *
 * Provider bodies are untrusted input. Only documented error fields are inspected,
 * and only a flattened, bounded, redacted diagnostic reaches logs.
 */

export type ProviderErrorCategory = "quota_exceeded" | "authentication" | "rate_limited" | "provider_error";

export interface ProviderError {
	readonly category: ProviderErrorCategory;
	readonly code: string;
	readonly status: number;
	readonly diagnostic: string;
}

const MAX_BODY_LENGTH = 32 * 1024;
const MAX_FIELD_LENGTH = 96;
const MAX_MESSAGE_LENGTH = 420;
const MAX_DIAGNOSTIC_LENGTH = 640;

type ProviderFields = {
	readonly code?: string;
	readonly status?: string;
	readonly type?: string;
	readonly message?: string;
};

/** Parses one failed provider response without ever exposing its raw body. */
export async function classifyProviderError(response: Response): Promise<ProviderError> {
	const fields = await readProviderFields(response);
	const category = classify(response.status, fields);
	const code =
		category === "provider_error" ? (safeCode(fields.code ?? fields.status ?? fields.type) ?? category) : category;
	const parts = [`status=${response.status}`, `category=${category}`, `code=${code}`];
	const type = safeField(fields.type, MAX_FIELD_LENGTH);
	const message = safeField(fields.message, MAX_MESSAGE_LENGTH);
	if (type && type !== code) parts.push(`type=${type}`);
	if (message) parts.push(`message=${message}`);
	return {
		category,
		code,
		status: response.status,
		diagnostic: bound(parts.join(" "), MAX_DIAGNOSTIC_LENGTH),
	};
}

async function readProviderFields(response: Response): Promise<ProviderFields> {
	let payload: unknown;
	try {
		const body = await response.text();
		if (body.length > MAX_BODY_LENGTH) return {};
		payload = JSON.parse(body);
	} catch {
		return {};
	}
	if (!isRecord(payload)) return {};
	const detail = isRecord(payload.detail) ? payload.detail : undefined;
	return {
		code: firstText(detail?.code, payload.code),
		status: firstText(detail?.status, payload.status),
		type: firstText(detail?.type, payload.type),
		message: firstText(detail?.message, payload.message),
	};
}

function classify(httpStatus: number, fields: ProviderFields): ProviderErrorCategory {
	const signals = [fields.code, fields.status, fields.type, fields.message]
		.filter((value): value is string => value !== undefined)
		.join(" ")
		.toLowerCase()
		.replace(/[-\s]+/g, "_");
	if (signals.includes("quota_exceeded") || signals.includes("insufficient_credits")) return "quota_exceeded";
	if (
		httpStatus === 401 ||
		httpStatus === 403 ||
		/(?:auth_error|authentication|unauthori[sz]ed|invalid_api_key|expired_api_key|missing_api_key)/.test(signals)
	)
		return "authentication";
	if (httpStatus === 429 || /(?:rate_limit|rate_limited|too_many_requests|throttl|retry_after)/.test(signals))
		return "rate_limited";
	return "provider_error";
}

function firstText(...values: unknown[]): string | undefined {
	for (const value of values) {
		if (typeof value === "string" || typeof value === "number") return String(value);
	}
	return undefined;
}

function safeCode(value: string | undefined): string | undefined {
	if (value === undefined) return undefined;
	const trimmed = value.trim();
	if (redactSecrets(trimmed) !== trimmed) return undefined;
	const normalized = trimmed
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "_")
		.replace(/^_+|_+$/g, "");
	return normalized === "" ? undefined : normalized.slice(0, MAX_FIELD_LENGTH);
}

function safeField(value: string | undefined, limit: number): string | undefined {
	if (value === undefined) return undefined;
	const singleLine = [...value]
		.map((character) => {
			const code = character.codePointAt(0) ?? 0;
			return code < 0x20 || code === 0x7f ? " " : character;
		})
		.join("")
		.replace(/\s+/g, " ")
		.trim();
	const flattened = redactSecrets(singleLine);
	return flattened === "" ? undefined : bound(flattened, limit);
}

function redactSecrets(value: string): string {
	return value
		.replace(/\bBearer\s+[^\s,;]+/gi, "Bearer [redacted]")
		.replace(
			/\b(?:x[-_ ]?api[-_ ]?key|api[-_ ]?key|access[-_ ]?token|auth(?:entication)?[-_ ]?token|secret|password|credential)\s*[:=]\s*["']?[^\s,"'}]+["']?/gi,
			(match) => `${match.slice(0, match.search(/[:=]/) + 1)}[redacted]`,
		)
		.replace(/\b(?:sk|pk|rk|secret|token|key)[-_][A-Za-z0-9][A-Za-z0-9._~-]{5,}\b/gi, "[redacted]");
}

function bound(value: string, limit: number): string {
	return value.length <= limit ? value : `${value.slice(0, Math.max(0, limit - 1))}…`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
