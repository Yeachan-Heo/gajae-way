/**
 * Grok provider HTTP stream handler.
 *
 * Manages streaming requests to xAI's Grok API with:
 * - Dynamic Grok CLI version header from runtime resolution
 * - HTTP 426 (Upgrade Required) handling with version extraction and single retry
 */

import { getLatestGrokCliVersion, updateVersionFrom426 } from "./version-resolver.js";

/**
 * HTTP client configuration for Grok API requests.
 */
export interface GrokHttpClientOptions {
	baseUrl?: string;
	apiKey: string;
	/** Timeout in milliseconds for version resolution */
	versionResolutionTimeoutMs?: number;
}

/**
 * Response from a Grok API request.
 */
export interface GrokApiResponse {
	status: number;
	headers: Record<string, string>;
	body: ReadableStream<Uint8Array>;
}

/**
 * Create a Grok HTTP client that automatically manages version headers.
 */
export function createGrokHttpClient(options: GrokHttpClientOptions) {
	const baseUrl = options.baseUrl || "https://api.x.ai";
	const apiKey = options.apiKey;
	const versionResolutionTimeoutMs = options.versionResolutionTimeoutMs || 5000;

	/**
	 * Make a streaming request to Grok API with version header and 426 retry.
	 */
	async function streamRequest(
		method: string,
		path: string,
		body?: BodyInit,
		customHeaders?: Record<string, string>
	): Promise<GrokApiResponse> {
		// Resolve the latest version
		const version = await getLatestGrokCliVersion(versionResolutionTimeoutMs);

		// First attempt with the resolved version
		const response = await makeRequest(method, path, body, customHeaders, version);

		// If we get a 426, parse the required version and retry once
		if (response.status === 426) {
			const responseText = await response.bodyText;
			const requiredVersion = await updateVersionFrom426(responseText);

			if (requiredVersion && requiredVersion !== version) {
				// Retry with the required version
				return makeRequest(method, path, body, customHeaders, requiredVersion);
			}
		}

		return response;
	}

	/**
	 * Internal: make the actual HTTP request with the specified version.
	 */
	async function makeRequest(
		method: string,
		path: string,
		body?: BodyInit,
		customHeaders?: Record<string, string>,
		version?: string
	): Promise<GrokApiResponse> {
		const headersRecord: Record<string, string> = {
			Authorization: `Bearer ${apiKey}`,
			"x-grok-client-version": version || (await getLatestGrokCliVersion(versionResolutionTimeoutMs)),
		};
		if (customHeaders) {
			Object.assign(headersRecord, customHeaders);
		}
		const headers = new Headers(headersRecord);

		const url = `${baseUrl}${path}`;

		const response = await fetch(url, {
			method,
			headers,
			body,
		});

		const bodyText = await response.text();
		const responseHeadersRecord: Record<string, string> = {};
		response.headers.forEach((value, key) => {
			responseHeadersRecord[key] = value;
		});

		// Create a ReadableStream from the body text
		const bodyStream = new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(new TextEncoder().encode(bodyText));
				controller.close();
			},
		});

		return {
			status: response.status,
			headers: responseHeadersRecord,
			body: bodyStream,
			bodyText,
		};
	}

	return {
		streamRequest,
		makeRequest,
	};
}

/**
 * Streaming request to Grok v1 with dynamic version header.
 * Handles HTTP 426 responses automatically.
 */
export async function streamToGrokV1(
	options: GrokHttpClientOptions & {
		model: string;
		messages: Array<{ role: string; content: string }>;
	}
): Promise<Response> {
	const { model, messages, ...clientOptions } = options;
	const client = createGrokHttpClient(clientOptions);

	const body = JSON.stringify({
		model,
		messages,
		stream: true,
	});

	const response = await client.streamRequest("POST", "/v1/chat/completions", body, {
		"Content-Type": "application/json",
	});

	// Convert our response format to standard Response
	return new Response(response.body, {
		status: response.status,
		headers: response.headers,
	});
}

/**
 * Extend Response interface to include bodyText for easier testing.
 */
declare global {
	interface Response {
		bodyText?: string;
	}
}
