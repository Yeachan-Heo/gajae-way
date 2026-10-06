import { describe, expect, it } from "bun:test";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { approvalPanelBlocks, approvalPanelFallback, askUserPanelBlocks, askUserPanelFallback } from "../src/panels";

describe("askUserPanelBlocks", () => {
	it("renders ask-user panel with interactive buttons", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "What would you like to do?",
			final: true,
			askUserPanel: {
				panelId: "panel123",
				question: "Choose an option:",
				expiresAt: futureTime,
				options: [
					{ id: "opt1", label: "Option 1" },
					{ id: "opt2", label: "Option 2" },
				],
			},
		};

		const blocks = askUserPanelBlocks(message);
		expect(blocks.length).toBeGreaterThan(0);
		expect(blocks[0].type).toBe("section");
		// Should have buttons in actions blocks
		const actionBlocks = blocks.filter((b: any) => b.type === "actions");
		expect(actionBlocks.length).toBeGreaterThan(0);
	});

	it("renders expired ask-user panel without buttons", () => {
		const pastTime = new Date(Date.now() - 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "What would you like to do?",
			final: true,
			askUserPanel: {
				panelId: "panel123",
				question: "Choose an option:",
				expiresAt: pastTime,
				options: [
					{ id: "opt1", label: "Option 1" },
					{ id: "opt2", label: "Option 2" },
				],
			},
		};

		const blocks = askUserPanelBlocks(message);
		const actionBlocks = blocks.filter((b: any) => b.type === "actions");
		expect(actionBlocks.length).toBe(0); // No action blocks for expired panels
		// Should have a section indicating expiration
		const expiredText = blocks.some((b: any) => b.text?.text?.includes("expired"));
		expect(expiredText).toBe(true);
	});

	it("returns empty array for message without ask-user panel", () => {
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Regular message",
			final: true,
		};

		const blocks = askUserPanelBlocks(message);
		expect(blocks.length).toBe(0);
	});
});

describe("approvalPanelBlocks", () => {
	it("renders approval panel with allow/deny buttons", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Do you approve?",
			final: true,
			approvalPanel: {
				panelId: "approval123",
				message: "This action requires approval",
				expiresAt: futureTime,
			},
		};

		const blocks = approvalPanelBlocks(message);
		expect(blocks.length).toBeGreaterThan(0);
		expect(blocks[0].type).toBe("section");
		// Should have allow/deny buttons
		const actionBlocks = blocks.filter((b: any) => b.type === "actions");
		expect(actionBlocks.length).toBe(1);
		const buttons = actionBlocks[0].elements as any[];
		expect(buttons.length).toBe(2);
		expect(buttons[0].text.text).toBe("Allow");
		expect(buttons[1].text.text).toBe("Deny");
	});

	it("renders approval panel with offered permission options", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Permission request",
			final: true,
			approvalPanel: {
				panelId: "perm-panel-456",
				message: "Grant permission?",
				expiresAt: futureTime,
				options: [
					{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
					{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
				],
			},
		};

		const blocks = approvalPanelBlocks(message);
		expect(blocks.length).toBeGreaterThan(0);
		const actionBlocks = blocks.filter((b: any) => b.type === "actions");
		expect(actionBlocks.length).toBeGreaterThan(0);
		const buttons = actionBlocks[0].elements as any[];
		expect(buttons.length).toBe(2);
		expect(buttons.map((b: any) => b.text.text)).toContain("Allow once");
		expect(buttons.map((b: any) => b.text.text)).toContain("Reject once");
	});

	it("renders expired approval panel without buttons", () => {
		const pastTime = new Date(Date.now() - 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Do you approve?",
			final: true,
			approvalPanel: {
				panelId: "approval123",
				message: "This action requires approval",
				expiresAt: pastTime,
			},
		};

		const blocks = approvalPanelBlocks(message);
		const actionBlocks = blocks.filter((b: any) => b.type === "actions");
		expect(actionBlocks.length).toBe(0); // No action blocks for expired panels
	});

	it("returns empty array for message without approval panel", () => {
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Regular message",
			final: true,
		};

		const blocks = approvalPanelBlocks(message);
		expect(blocks.length).toBe(0);
	});
});

describe("askUserPanelFallback", () => {
	it("returns fallback text for active panel", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Choose",
			final: true,
			askUserPanel: {
				panelId: "panel123",
				question: "Pick one:",
				expiresAt: futureTime,
				options: [
					{ id: "opt1", label: "Yes" },
					{ id: "opt2", label: "No" },
				],
			},
		};

		const fallback = askUserPanelFallback(message);
		expect(fallback).toBeTruthy();
		expect(fallback).toContain("Pick one:");
		expect(fallback).toContain("Yes");
		expect(fallback).toContain("No");
	});

	it("includes expiration notice for expired panel", () => {
		const pastTime = new Date(Date.now() - 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Choose",
			final: true,
			askUserPanel: {
				panelId: "panel123",
				question: "Pick one:",
				expiresAt: pastTime,
				options: [{ id: "opt1", label: "Yes" }],
			},
		};

		const fallback = askUserPanelFallback(message);
		expect(fallback).toContain("expired");
	});

	it("returns null for message without panel", () => {
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Regular message",
			final: true,
		};

		const fallback = askUserPanelFallback(message);
		expect(fallback).toBeNull();
	});
});

describe("approvalPanelFallback", () => {
	it("returns fallback text for active panel", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Approve?",
			final: true,
			approvalPanel: {
				panelId: "approval123",
				message: "Do you agree?",
				expiresAt: futureTime,
			},
		};

		const fallback = approvalPanelFallback(message);
		expect(fallback).toBeTruthy();
		expect(fallback).toContain("Do you agree?");
		expect(fallback).toContain("check_mark");
	});

	it("returns fallback with offered permission options", () => {
		const futureTime = new Date(Date.now() + 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Permission?",
			final: true,
			approvalPanel: {
				panelId: "perm-fallback",
				message: "Grant this permission?",
				expiresAt: futureTime,
				options: [
					{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
					{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
				],
			},
		};

		const fallback = approvalPanelFallback(message);
		expect(fallback).toBeTruthy();
		expect(fallback).toContain("Grant this permission?");
		expect(fallback).toContain("Allow once");
		expect(fallback).toContain("Reject once");
	});

	it("includes expiration notice for expired panel", () => {
		const pastTime = new Date(Date.now() - 60 * 1000).toISOString();
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Approve?",
			final: true,
			approvalPanel: {
				panelId: "approval123",
				message: "Do you agree?",
				expiresAt: pastTime,
			},
		};

		const fallback = approvalPanelFallback(message);
		expect(fallback).toContain("expired");
	});

	it("returns null for message without panel", () => {
		const message: ChatMessagePayload = {
			turnId: "turn123",
			origin: { platform: "slack", kind: "channel", conversationId: "C123" },
			role: "assistant",
			text: "Regular message",
			final: true,
		};

		const fallback = approvalPanelFallback(message);
		expect(fallback).toBeNull();
	});
});
