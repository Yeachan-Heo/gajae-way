/**
 * Tests for Grok API stream handler with 426 retry logic.
 *
 * Covers:
 * - Dynamic version header injection
 * - HTTP 426 error handling with single retry
 * - Version update from 426 error body
 * - Successful request after 426 retry
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { createGrokHttpClient, streamToGrokV1 } from "../../src/providers/grok/stream.js";
import { clearVersionCache } from "../../src/providers/grok/version-resolver.js";

// Mock fetch globally
global.fetch = vi.fn();

describe("Grok API Stream Handler", () => {
	beforeEach(() => {
		clearVersionCache();
		vi.clearAllMocks();
	});

	afterEach(() => {
		clearVersionCache();
		vi.clearAllMocks();
	});

	describe("createGrokHttpClient", () => {
		it("should inject version header in requests", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			// Mock npm registry for version resolution
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.13" } }),
			});

			// Mock successful response
			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers({ "content-type": "application/json" }),
				text: async () => JSON.stringify({ data: "test" }),
			});

			await client.streamRequest("GET", "/v1/test");

			// Check that version header was sent
			const calls = (global.fetch as any).mock.calls;
			const apiCall = calls.find((c: any[]) => c[0].includes("/v1/test"));
			expect(apiCall).toBeDefined();

			if (apiCall) {
				const headers = apiCall[1]?.headers as any;
				const versionHeader = headers?.get?.("x-grok-client-version") || headers?.["x-grok-client-version"];
				expect(versionHeader).toBe("1.0.13");
			}
		});

		it("should retry on HTTP 426 with updated version", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			// Mock version resolution
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.0" } }),
			});

			// First call: 426 response
			(global.fetch as any).mockResolvedValueOnce({
				status: 426,
				ok: false,
				headers: new Headers(),
				text: async () => "Please update to version 1.0.13",
			});

			// Second call: successful response with new version
			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers({ "content-type": "application/json" }),
				text: async () => JSON.stringify({ success: true }),
			});

			const response = await client.streamRequest("GET", "/v1/models");

			expect(response.status).toBe(200);
		});

		it("should not retry 426 if version parsing fails", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			// Mock version resolution
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.0" } }),
			});

			// 426 response with no parseable version
			(global.fetch as any).mockResolvedValueOnce({
				status: 426,
				ok: false,
				headers: new Headers(),
				text: async () => "Something went wrong",
			});

			const response = await client.streamRequest("GET", "/v1/models");

			// Should return the 426 response without retry
			expect(response.status).toBe(426);

			// Should only have made 2 calls (version + one API call)
			const fetchCalls = (global.fetch as any).mock.calls.length;
			expect(fetchCalls).toBe(2);
		});

		it("should handle authorization headers", async () => {
			const client = createGrokHttpClient({
				apiKey: "secret-key-123",
			});

			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.13" } }),
			});

			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers(),
				text: async () => "{}",
			});

			await client.streamRequest("GET", "/v1/test");

			const calls = (global.fetch as any).mock.calls;
			const apiCall = calls.find((c: any[]) => c[0].includes("/v1/test"));

			if (apiCall) {
				const headers = apiCall[1]?.headers as any;
				const authHeader = headers?.get?.("authorization") || headers?.["authorization"];
				expect(authHeader).toBe("Bearer secret-key-123");
			}
		});

		it("should merge custom headers with version header", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.13" } }),
			});

			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers(),
				text: async () => "{}",
			});

			const customHeaders = { "X-Custom": "value", "Content-Type": "application/json" };
			await client.streamRequest("POST", "/v1/test", undefined, customHeaders);

			const calls = (global.fetch as any).mock.calls;
			const apiCall = calls.find((c: any[]) => c[0].includes("/v1/test"));

			if (apiCall) {
				const headers = apiCall[1]?.headers as any;
				const getHeader = (name: string) => headers?.get?.(name) || headers?.[name];
				expect(getHeader("x-custom")).toBe("value");
				expect(getHeader("content-type")).toBe("application/json");
				expect(getHeader("x-grok-client-version")).toBe("1.0.13");
			}
		});
	});

	describe("streamToGrokV1", () => {
		it("should make streaming request to /v1/chat/completions", async () => {
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.13" } }),
			});

			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers(),
				text: async () => "streaming data",
			});

			const response = await streamToGrokV1({
				apiKey: "test-key",
				model: "grok-4.7",
				messages: [{ role: "user", content: "Hello" }],
			});

			expect(response.status).toBe(200);

			const calls = (global.fetch as any).mock.calls;
			const apiCall = calls.find((c: any[]) => c[0].includes("/v1/chat/completions"));
			expect(apiCall).toBeDefined();

			if (apiCall) {
				const body = apiCall[1]?.body;
				if (typeof body === "string") {
					const parsedBody = JSON.parse(body);
					expect(parsedBody.model).toBe("grok-4.7");
					expect(parsedBody.stream).toBe(true);
					expect(parsedBody.messages).toEqual([{ role: "user", content: "Hello" }]);
				}
			}
		});

		it("should handle 426 response with retry in streaming request", async () => {
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.0" } }),
			});

			// First call: 426
			(global.fetch as any).mockResolvedValueOnce({
				status: 426,
				ok: false,
				headers: new Headers(),
				text: async () => "Update to version 1.0.13",
			});

			// Second call: success
			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers({ "content-type": "text/event-stream" }),
				text: async () => "data: {}\n\n",
			});

			const response = await streamToGrokV1({
				apiKey: "test-key",
				model: "grok-4.7",
				messages: [{ role: "user", content: "Test" }],
			});

			expect(response.status).toBe(200);
		});
	});

	describe("HTTP 426 Retry Edge Cases", () => {
		it("should not retry if new version equals old version", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			// Mock version resolution - returns 1.0.13
			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.13" } }),
			});

			// First call: 426 with same version in message
			(global.fetch as any).mockResolvedValueOnce({
				status: 426,
				ok: false,
				headers: new Headers(),
				text: async () => "Update to version 1.0.13",
			});

			const response = await client.streamRequest("GET", "/v1/test");

			// Should return 426 without retrying
			expect(response.status).toBe(426);

			// Should only have made 2 calls (version + one API call)
			const fetchCalls = (global.fetch as any).mock.calls.length;
			expect(fetchCalls).toBe(2);
		});

		it("should include version header in retry request", async () => {
			const client = createGrokHttpClient({ apiKey: "test-key" });

			(global.fetch as any).mockResolvedValueOnce({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.0.0" } }),
			});

			// First: 426
			(global.fetch as any).mockResolvedValueOnce({
				status: 426,
				ok: false,
				headers: new Headers(),
				text: async () => "Update to version 1.0.13",
			});

			// Second: success
			(global.fetch as any).mockResolvedValueOnce({
				status: 200,
				ok: true,
				headers: new Headers(),
				text: async () => "{}",
			});

			await client.streamRequest("GET", "/v1/test");

			const calls = (global.fetch as any).mock.calls;
			const apiCalls = calls.filter((c: any[]) => c[0].includes("/v1/test"));

			// Should have 2 API calls (initial + retry)
			expect(apiCalls.length).toBe(2);

			// Both should have version header
			if (apiCalls.length === 2) {
				const firstHeaders = apiCalls[0][1]?.headers as any;
				const secondHeaders = apiCalls[1][1]?.headers as any;
				const getHeader = (headers: any, name: string) => headers?.get?.(name) || headers?.[name];
				expect(getHeader(firstHeaders, "x-grok-client-version")).toBe("1.0.0");
				expect(getHeader(secondHeaders, "x-grok-client-version")).toBe("1.0.13");
			}
		});
	});
});
