import { expect, test } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { describePanelResponse, type SlackBlockAction } from "../src/interactions";
import { approvalPanelBlocks, askUserPanelBlocks } from "../src/panels";

function click(message: ChatMessagePayload, approval = false): SlackBlockAction {
	const blocks = approval ? approvalPanelBlocks(message) : askUserPanelBlocks(message);
	const actions = blocks.find((block) => block.type === "actions");
	const button = (actions?.elements as Array<{ action_id: string; value: string }>)[0];
	return {
		type: "block_actions",
		actions: [{ type: "button", action_id: button.action_id, value: button.value }],
		trigger_id: "trigger",
		user: { id: "U999", username: "alice", name: "Alice", team_id: "T1" },
		team: { id: "T1", domain: "workspace" },
		channel: { id: message.origin.parentId ?? message.origin.conversationId, name: "general" },
		message: { type: "message", ts: "1700000001.000001", thread_ts: "1700000000.000001" },
		token: "token",
		api_app_id: "A1",
		event_ts: "1700000002.000001",
		event_id: "Ev1",
	};
}

for (const origin of [
	{ platform: "slack", kind: "channel", conversationId: "C1" },
	{ platform: "slack", kind: "thread", conversationId: "C1:1700000000.000001", parentId: "C1" },
	{ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" },
] satisfies OriginRef[]) {
	test(`rendered ask-user button preserves underscore ids and original ${origin.kind} origin`, () => {
		const message: ChatMessagePayload = {
			turnId: "turn",
			origin,
			role: "assistant",
			text: "Choose",
			final: true,
			askUserPanel: {
				panelId: "question_001",
				question: "Tool?",
				expiresAt: "2099-01-01T00:00:00Z",
				options: [{ id: "option_a", label: "Tool A" }],
			},
		};
		const response = describePanelResponse(click(message));
		expect(response).toMatchObject({
			origin,
			panelId: "question_001",
			selectedOptionId: "option_a",
			responseKind: "option_selected",
			responderId: "U999",
			engagement: { group: origin.kind !== "dm", authorId: "U999" },
		});
	});
}

test("rendered approval buttons retain gateway permission option ids", () => {
	const message: ChatMessagePayload = {
		turnId: "turn",
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		role: "assistant",
		text: "Permission",
		final: true,
		approvalPanel: {
			panelId: "permission_001",
			message: "Allow?",
			expiresAt: "2099-01-01T00:00:00Z",
			options: [{ optionId: "allow_once", name: "Allow once", kind: "allow_once" }],
		},
	};
	expect(describePanelResponse(click(message, true))).toMatchObject({
		panelId: "permission_001",
		selectedOptionId: "allow_once",
		responseKind: "option_selected",
		origin: message.origin,
	});
});

test("malformed rendered action metadata is rejected instead of guessing origin or option", () => {
	const message: ChatMessagePayload = {
		turnId: "turn",
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		role: "assistant",
		text: "Choose",
		final: true,
		askUserPanel: {
			panelId: "q_1",
			question: "Tool?",
			expiresAt: "2099-01-01T00:00:00Z",
			options: [{ id: "opt_a", label: "A" }],
		},
	};
	const event = click(message);
	event.actions[0]!.value = "opt_a";
	expect(describePanelResponse(event)).toBeNull();
});
