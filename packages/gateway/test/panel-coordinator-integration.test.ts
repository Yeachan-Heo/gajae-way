import { describe, it, expect, beforeEach } from "bun:test";
import type { OriginRef, EngagementContext } from "@gajae-gateway/protocol";
import { PanelTracker } from "../src/server/panel-tracker";

describe("panel coordinator integration", () => {
	let panelTracker: PanelTracker;

	beforeEach(() => {
		panelTracker = new PanelTracker();
	});

	it("registers a pending panel for a question", () => {
		const origin: OriginRef = { platform: "slack", id: "C123:456.789" };
		const panelId = panelTracker.registerPanel({
			panelId: "test-panel-1",
			questionId: "q-12345",
			turnId: "turn-abcde",
			sessionId: "session-xyz",
			origin,
			authorId: "U123",
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-sha256-hash",
		});

		expect(panelId).toBe("test-panel-1");

		const retrieved = panelTracker.getPanel("test-panel-1");
		expect(retrieved).toBeDefined();
		expect(retrieved?.questionId).toBe("q-12345");
		expect(retrieved?.turnId).toBe("turn-abcde");
		expect(retrieved?.sessionId).toBe("session-xyz");
		expect(retrieved?.answerBinding).toBe("binding-sha256-hash");
	});

	it("validates authorized responders", () => {
		const origin: OriginRef = { platform: "slack", id: "C123:456.789" };
		panelTracker.registerPanel({
			panelId: "test-panel-2",
			questionId: "q-67890",
			turnId: "turn-xyz",
			sessionId: "session-abc",
			origin,
			authorId: "U456",
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-sha256-hash-2",
		});

		const panel = panelTracker.getPanel("test-panel-2");
		expect(panel).toBeDefined();

		if (panel) {
			expect(panelTracker.isAuthorizedResponder(panel, "U456")).toBe(true);
			expect(panelTracker.isAuthorizedResponder(panel, "U999")).toBe(false);
		}
	});

	it("marks panels as resolved", () => {
		const origin: OriginRef = { platform: "slack", id: "C123:456.789" };
		panelTracker.registerPanel({
			panelId: "test-panel-3",
			questionId: "q-11111",
			turnId: "turn-111",
			sessionId: "session-111",
			origin,
			authorId: "U789",
			expiresAt: new Date(Date.now() + 300000),
			answerBinding: "binding-sha256-hash-3",
		});

		const resolved = panelTracker.resolvePanel("test-panel-3");
		expect(resolved).toBeDefined();
		expect(resolved?.panelId).toBe("test-panel-3");

		// Should be gone after resolution
		const retrieved = panelTracker.getPanel("test-panel-3");
		expect(retrieved).toBeUndefined();
	});

	it("handles expired panels", () => {
		const origin: OriginRef = { platform: "slack", id: "C123:456.789" };
		panelTracker.registerPanel({
			panelId: "test-panel-expired",
			questionId: "q-expired",
			turnId: "turn-expired",
			sessionId: "session-expired",
			origin,
			authorId: "U-expired",
			expiresAt: new Date(Date.now() - 1000), // Already expired
			answerBinding: "binding-expired",
		});

		// Should return undefined for expired panels
		const retrieved = panelTracker.getPanel("test-panel-expired");
		expect(retrieved).toBeUndefined();
	});

	it("reaps expired panels", () => {
		const origin: OriginRef = { platform: "slack", id: "C123:456.789" };
		panelTracker.registerPanel({
			panelId: "panel-reap-1",
			questionId: "q-reap-1",
			turnId: "turn-reap-1",
			sessionId: "session-reap",
			origin,
			authorId: "U-reap",
			expiresAt: new Date(Date.now() - 1000),
			answerBinding: "binding-reap-1",
		});

		panelTracker.registerPanel({
			panelId: "panel-reap-2",
			questionId: "q-reap-2",
			turnId: "turn-reap-2",
			sessionId: "session-reap",
			origin,
			authorId: "U-reap",
			expiresAt: new Date(Date.now() + 300000), // Not expired
			answerBinding: "binding-reap-2",
		});

		const reaped = panelTracker.reapExpiredPanels();
		expect(reaped.length).toBe(1);
		expect(reaped[0].panelId).toBe("panel-reap-1");

		// Live panel should still be there
		expect(panelTracker.getPanel("panel-reap-2")).toBeDefined();
		// Expired panel should be gone
		expect(panelTracker.getPanel("panel-reap-1")).toBeUndefined();
	});
});
