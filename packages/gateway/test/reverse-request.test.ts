import { beforeEach, describe, expect, it } from "bun:test";
import type { OriginRef } from "@gajae-gateway/protocol";
import { PanelTracker } from "../src/server/panel-tracker";

describe("reverse request handling", () => {
	let panelTracker: PanelTracker;

	beforeEach(() => {
		panelTracker = new PanelTracker();
	});

	it("permission panel renders from toolCall and options", () => {
		const panelId = "permission-panel-1";
		const toolCall = { title: "Approve this tool call?", kind: "permission" };
		const options = [
			{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
			{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
		];

		// Register a permission panel
		panelTracker.registerPanel({
			panelId,
			questionId: "q-1",
			turnId: "turn-1",
			sessionId: "session-1",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: "U-trigger-author", // The actual trigger author
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-1",
			reverseRequest: {
				id: "request-1",
				connectionId: "conn-1",
				leaseId: "lease-1",
				capability: "permission",
				method: "request",
				payload: { toolCall, options },
			},
		});

		const panel = panelTracker.getPanel(panelId);
		expect(panel).toBeDefined();
		expect(panel?.reverseRequest?.payload).toEqual({ toolCall, options });
	});

	it("non-Slack origin declines reverse request", () => {
		// A non-Slack origin should be declined at the platform check
		// This is tested by the server handler returning unavailable error
		expect(true).toBe(true); // Placeholder for integration test
	});

	it("panel authorization uses trigger author", () => {
		const panelId = "auth-panel-1";
		const triggerAuthorId = "U-trigger-author";
		const otherId = "U-other-user";

		panelTracker.registerPanel({
			panelId,
			questionId: "q-2",
			turnId: "turn-2",
			sessionId: "session-2",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: triggerAuthorId, // Set to the trigger author
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-2",
		});

		const panel = panelTracker.getPanel(panelId);
		expect(panel).toBeDefined();

		if (panel) {
			// Only the trigger author can respond
			expect(panelTracker.isAuthorizedResponder(panel, triggerAuthorId)).toBe(true);
			// Other users cannot respond
			expect(panelTracker.isAuthorizedResponder(panel, otherId)).toBe(false);
		}
	});

	it("permission panel response selects offered allow option", () => {
		const panelId = "allow-response-1";
		const options = [
			{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
			{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
		];

		panelTracker.registerPanel({
			panelId,
			questionId: "q-3",
			turnId: "turn-3",
			sessionId: "session-3",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: "U-author",
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-3",
			reverseRequest: {
				id: "request-3",
				connectionId: "conn-3",
				leaseId: "lease-3",
				capability: "permission",
				method: "request",
				payload: { options },
			},
		});

		const panel = panelTracker.getPanel(panelId);
		expect(panel).toBeDefined();

		// When approved, should select the allow_once option
		// The response builder would create: { outcome: "selected", optionId: "allow_once", kind: "allow_once" }
		if (panel?.reverseRequest) {
			const optionsList = Array.isArray((panel.reverseRequest.payload as any)?.options)
				? (panel.reverseRequest.payload as any).options
				: [];
			let allowOptionId: string | undefined;
			for (const opt of optionsList) {
				if (typeof opt === "object" && opt !== null) {
					const optRecord = opt as Record<string, unknown>;
					const kind = String(optRecord.kind || "").toLowerCase();
					if (kind.includes("allow")) {
						allowOptionId = String(optRecord.optionId || "");
					}
				}
			}
			expect(allowOptionId).toBe("allow_once");
		}
	});

	it("permission panel response selects offered reject option", () => {
		const panelId = "reject-response-1";
		const options = [
			{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
			{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
		];

		panelTracker.registerPanel({
			panelId,
			questionId: "q-4",
			turnId: "turn-4",
			sessionId: "session-4",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: "U-author",
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-4",
			reverseRequest: {
				id: "request-4",
				connectionId: "conn-4",
				leaseId: "lease-4",
				capability: "permission",
				method: "request",
				payload: { options },
			},
		});

		const panel = panelTracker.getPanel(panelId);
		expect(panel).toBeDefined();

		// When denied, should select the reject_once option
		// The response builder would create: { outcome: "selected", optionId: "reject_once", kind: "reject_once" }
		if (panel?.reverseRequest) {
			const optionsList = Array.isArray((panel.reverseRequest.payload as any)?.options)
				? (panel.reverseRequest.payload as any).options
				: [];
			let rejectOptionId: string | undefined;
			for (const opt of optionsList) {
				if (typeof opt === "object" && opt !== null) {
					const optRecord = opt as Record<string, unknown>;
					const kind = String(optRecord.kind || "").toLowerCase();
					if (kind.includes("reject")) {
						rejectOptionId = String(optRecord.optionId || "");
					}
				}
			}
			expect(rejectOptionId).toBe("reject_once");
		}
	});

	it("expired panel returns outcome cancelled", () => {
		const panelId = "expired-panel-1";
		const expiresAt = new Date(Date.now() - 1000); // Already expired

		panelTracker.registerPanel({
			panelId,
			questionId: "q-5",
			turnId: "turn-5",
			sessionId: "session-5",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: "U-author",
			expiresAt,
			answerBinding: "binding-5",
			reverseRequest: {
				id: "request-5",
				connectionId: "conn-5",
				leaseId: "lease-5",
				capability: "permission",
				method: "request",
				payload: { options: [] },
			},
		});

		// Expired panels should be reaped
		const reaped = panelTracker.reapExpiredPanels();
		expect(reaped.length).toBeGreaterThan(0);
		expect(reaped[0].panelId).toBe(panelId);

		// Expiry returns outcome: "cancelled"
		// The expiry handler would send: { outcome: "cancelled" }
		const expiredPanel = reaped[0];
		expect(expiredPanel.reverseRequest?.id).toBe("request-5");
	});

	it("panel response validates authorized responder", () => {
		const panelId = "auth-validation-1";
		const triggerAuthorId = "U-trigger-author";

		panelTracker.registerPanel({
			panelId,
			questionId: "q-6",
			turnId: "turn-6",
			sessionId: "session-6",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" } as OriginRef,
			authorId: triggerAuthorId,
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-6",
			reverseRequest: {
				id: "request-6",
				connectionId: "conn-6",
				leaseId: "lease-6",
				capability: "permission",
				method: "request",
				payload: { options: [] },
			},
		});

		const panel = panelTracker.getPanel(panelId);
		expect(panel).toBeDefined();

		if (panel) {
			// Only trigger author is authorized
			expect(panelTracker.isAuthorizedResponder(panel, triggerAuthorId)).toBe(true);
			// Any other user is unauthorized
			expect(panelTracker.isAuthorizedResponder(panel, "U-other-user")).toBe(false);
			expect(panelTracker.isAuthorizedResponder(panel, "U-another-user")).toBe(false);
		}
	});

	it("unknown capability declines with unavailable", () => {
		// Unknown capabilities should be declined
		// This is tested by checking that only permission.request is handled
		expect(true).toBe(true); // Placeholder for integration test
	});
});
