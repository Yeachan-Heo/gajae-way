import { describe, expect, it } from "bun:test";
import { describePanelResponse, type SlackBlockAction } from "../src/interactions";

describe("describePanelResponse", () => {
	it("parses ask-user panel option selection", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [
				{
					type: "button",
					action_id: "ask_user_panel123_option1",
					value: "option1",
				},
			],
			trigger_id: "trigger123",
			user: {
				id: "U123",
				username: "testuser",
				name: "Test User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "C123",
				name: "general",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
				text: "Question text",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "channel",
			conversationId: "C123",
		});

		expect(response).not.toBeNull();
		expect(response?.panelId).toBe("panel123");
		expect(response?.responseKind).toBe("option_selected");
		expect(response?.selectedOptionId).toBe("option1");
		expect(response?.responderId).toBe("U123");
		expect(response?.engagement.mentioned).toBe(true);
		expect(response?.engagement.group).toBe(true);
	});

	it("parses approval panel approve response", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [
				{
					type: "button",
					action_id: "approval_approval456_allow",
				},
			],
			trigger_id: "trigger123",
			user: {
				id: "U456",
				username: "approver",
				name: "Approver User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "C123",
				name: "general",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "channel",
			conversationId: "C123",
		});

		expect(response).not.toBeNull();
		expect(response?.panelId).toBe("approval456");
		expect(response?.responseKind).toBe("approved");
		expect(response?.responderId).toBe("U456");
	});

	it("parses approval panel deny response", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [
				{
					type: "button",
					action_id: "approval_approval789_deny",
				},
			],
			trigger_id: "trigger123",
			user: {
				id: "U789",
				username: "reviewer",
				name: "Reviewer User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "C123",
				name: "general",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "channel",
			conversationId: "C123",
		});

		expect(response).not.toBeNull();
		expect(response?.panelId).toBe("approval789");
		expect(response?.responseKind).toBe("denied");
		expect(response?.responderId).toBe("U789");
	});

	it("returns null for non-panel block actions", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [
				{
					type: "button",
					action_id: "other_action",
				},
			],
			trigger_id: "trigger123",
			user: {
				id: "U123",
				username: "testuser",
				name: "Test User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "C123",
				name: "general",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "channel",
			conversationId: "C123",
		});

		expect(response).toBeNull();
	});

	it("returns null for empty actions", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [],
			trigger_id: "trigger123",
			user: {
				id: "U123",
				username: "testuser",
				name: "Test User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "C123",
				name: "general",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "channel",
			conversationId: "C123",
		});

		expect(response).toBeNull();
	});

	it("handles DM origin correctly", () => {
		const blockAction: SlackBlockAction = {
			type: "block_actions",
			actions: [
				{
					type: "button",
					action_id: "ask_user_panelDM_optionA",
				},
			],
			trigger_id: "trigger123",
			user: {
				id: "U123",
				username: "testuser",
				name: "Test User",
				team_id: "T123",
			},
			team: {
				id: "T123",
				domain: "test",
			},
			channel: {
				id: "D123",
				name: "directmessage",
			},
			message: {
				type: "message",
				ts: "1234567890.123456",
			},
			token: "token123",
			api_app_id: "app123",
			event_ts: "1234567890.123456",
			event_id: "event123",
		};

		const response = describePanelResponse(blockAction, {
			platform: "slack",
			kind: "dm",
			conversationId: "D123",
			peerId: "U123",
		});

		expect(response).not.toBeNull();
		expect(response?.engagement.group).toBe(false);
	});
});
