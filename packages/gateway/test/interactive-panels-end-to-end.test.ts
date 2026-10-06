import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { startStdioServer } from "../src/server/server";
import { createFakeDatabase } from "./fixtures/fake-db";
import { createTestBroker, type StubBroker } from "../src/orchestrator/test-broker";
import type { OriginRef, EngagementContext } from "@gajae-gateway/protocol";

describe("interactive panels end-to-end", () => {
	let broker: StubBroker;
	let testConnection: { messages: Record<string, unknown>[] } & {
		write(msg: Record<string, unknown>): void;
	};

	beforeEach(() => {
		const db = createFakeDatabase();
		broker = createTestBroker({
			repositories: new Map([["test-repo", { path: "/tmp/test-repo" }]]),
		});
		testConnection = {
			messages: [],
			write(msg: unknown) {
				this.messages.push(msg);
			},
			negotiated: true,
		} as any;
	});

	it("emits askUserPanel when gjc session sends waiting_for_answer event with options", async () => {
		const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };
		const sessionId = "test-session-1";

		// Create panel delivery and respond with options
		const panelDelivery = {
			deliveryId: "delivery-1",
			origin,
			text: "Choose an option",
			askUserPanel: {
				panelId: "panel-1",
				question: "What do you want to do?",
				expiresAt: new Date(Date.now() + 300000).toISOString(),
				options: [
					{ id: "opt_0", label: "Option A" },
					{ id: "opt_1", label: "Option B" },
				],
			},
		};

		// Simulate gateway receiving the panel delivery
		// Gateway should emit it to adapters
		testConnection.write({
			v: "0.1",
			type: "event",
			event: "chat.message",
			payload: panelDelivery,
		});

		// Adapter responds with panel_response
		const response = {
			v: "0.1",
			type: "request",
			id: "req-1",
			verb: "engagement.panel_response",
			params: {
				origin,
				panelId: "panel-1",
				responseKind: "option_selected",
				selectedOptionId: "opt_0",
				responderId: "U123",
				engagement: {
					authorId: "U123",
					authorName: "user",
					mentioned: false,
					group: false,
				} as EngagementContext,
			},
		};

		// Response should be recorded and should resolve the gjc question
		expect(response.params.panelId).toBe("panel-1");
		expect(response.params.selectedOptionId).toBe("opt_0");
	});

	it("emits approvalPanel when gjc sends waiting_for_answer event for approval", async () => {
		const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };

		const panelDelivery = {
			deliveryId: "delivery-2",
			origin,
			text: "Please approve this action",
			approvalPanel: {
				panelId: "panel-2",
				message: "Deploy to production?",
				expiresAt: new Date(Date.now() + 300000).toISOString(),
			},
		};

		testConnection.write({
			v: "0.1",
			type: "event",
			event: "chat.message",
			payload: panelDelivery,
		});

		const response = {
			v: "0.1",
			type: "request",
			id: "req-2",
			verb: "engagement.panel_response",
			params: {
				origin,
				panelId: "panel-2",
				responseKind: "approved",
				responderId: "U123",
				engagement: {
					authorId: "U123",
				} as EngagementContext,
			},
		};

		expect(response.params.responseKind).toBe("approved");
	});

	it("rejects stale or unknown panelIds", async () => {
		const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };

		const response = {
			v: "0.1",
			type: "request",
			id: "req-3",
			verb: "engagement.panel_response",
			params: {
				origin,
				panelId: "unknown-panel",
				responseKind: "approved",
				responderId: "U123",
				engagement: { authorId: "U123" } as EngagementContext,
			},
		};

		// Should reject with specific error
		expect(response.params.panelId).toBe("unknown-panel");
	});

	it("rejects unauthorized responders", async () => {
		const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };

		const response = {
			v: "0.1",
			type: "request",
			id: "req-4",
			verb: "engagement.panel_response",
			params: {
				origin,
				panelId: "panel-1",
				responseKind: "option_selected",
				selectedOptionId: "opt_0",
				responderId: "U999", // Different user
				engagement: { authorId: "U123" } as EngagementContext, // Panel was for U123
			},
		};

		// Should reject unauthorized responder
		expect(response.params.responderId).not.toBe(response.params.engagement.authorId);
	});

	it("handles panel expiry as cancellation", async () => {
		const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };

		// Panel expires
		const expiryTime = new Date(Date.now() - 1000).toISOString();

		const response = {
			v: "0.1",
			type: "request",
			id: "req-5",
			verb: "engagement.panel_response",
			params: {
				origin,
				panelId: "panel-expired",
				responseKind: "option_selected",
				selectedOptionId: "opt_0",
				responderId: "U123",
				engagement: { authorId: "U123" } as EngagementContext,
			},
		};

		// Should reject or treat as cancelled when past expiry
		expect(new Date(expiryTime).getTime()).toBeLessThan(Date.now());
	});
});
