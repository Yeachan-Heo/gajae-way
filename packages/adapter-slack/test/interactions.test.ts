import { describe, expect, it } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { describePanelResponse, type SlackBlockAction } from "../src/interactions";
import { approvalPanelBlocks, approvalPanelFallback, askUserPanelBlocks, askUserPanelFallback } from "../src/panels";

const channel: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };
const thread: OriginRef = {
	platform: "slack",
	kind: "thread",
	conversationId: "C123:1234567890.123456",
	parentId: "C123",
};
const dm: OriginRef = { platform: "slack", kind: "dm", conversationId: "D123", peerId: "U123" };

function message(origin: OriginRef = channel): ChatMessagePayload {
	return {
		turnId: "t1",
		origin,
		role: "assistant",
		text: "Choose",
		final: true,
		askUserPanel: {
			panelId: "question_001",
			question: "Choose an option",
			expiresAt: new Date(Date.now() + 60_000).toISOString(),
			options: [{ id: "option_a", label: "Option A" }],
		},
	};
}

type Button = { type: string; action_id: string; value: string };
function buttons(blocks: Record<string, unknown>[]): Button[] {
	return blocks.flatMap((block) => (block.elements as Button[] | undefined) ?? []);
}

function click(button: Button, origin: OriginRef, threadTs?: string): SlackBlockAction {
	return {
		type: "block_actions",
		actions: [button],
		user: { id: "U123", username: "user", name: "User", team_id: "T123" },
		channel: { id: origin.kind === "thread" ? origin.parentId : origin.conversationId, name: "channel" },
		message: { type: "message", ts: "1234567891.123456", thread_ts: threadTs },
	} as SlackBlockAction;
}

describe("panel actions", () => {
	it("round trips underscored panel and option IDs and the original thread origin", () => {
		const button = buttons(askUserPanelBlocks(message(thread)))[0] as Button;
		const response = describePanelResponse(click(button, thread, "1234567890.123456"));
		expect(response).toMatchObject({
			origin: thread,
			panelId: "question_001",
			responseKind: "option_selected",
			selectedOptionId: "option_a",
			responderId: "U123",
		});
	});

	it("preserves channel sessions under bot-parent threads and DM origins", () => {
		for (const origin of [channel, dm]) {
			const button = buttons(askUserPanelBlocks(message(origin)))[0] as Button;
			const response = describePanelResponse(click(button, origin, "1234567890.123456"));
			expect(response?.origin).toEqual(origin);
			expect(response?.engagement.group).toBe(origin.kind !== "dm");
		}
	});

	it("preserves approval decisions and exact offered option IDs", () => {
		const base = message(thread);
		const approval = {
			...base,
			askUserPanel: undefined,
			approvalPanel: {
				panelId: "approval_001",
				message: "Approve?",
				expiresAt: new Date(Date.now() + 60_000).toISOString(),
			},
		};
		const decisions = buttons(approvalPanelBlocks(approval)).map((button) =>
			describePanelResponse(click(button, thread)),
		);
		expect(decisions.map((response) => [response?.origin, response?.panelId, response?.responseKind])).toEqual([
			[thread, "approval_001", "approved"],
			[thread, "approval_001", "denied"],
		]);
		const offered = {
			...approval,
			approvalPanel: {
				...approval.approvalPanel,
				options: [
					{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
					{ optionId: "allow", name: "Allow", kind: "allow" },
				],
			},
		};
		const responses = buttons(approvalPanelBlocks(offered)).map((button) =>
			describePanelResponse(click(button, thread)),
		);
		expect(responses.map((response) => [response?.responseKind, response?.selectedOptionId])).toEqual([
			["option_selected", "allow_once"],
			["option_selected", "allow"],
		]);
	});

	it("rejects malformed, mismatched and non-panel actions", () => {
		const button = buttons(askUserPanelBlocks(message()))[0] as Button;
		const data = JSON.parse(button.value);
		for (const invalid of [
			{ ...button, value: "option_a" },
			{ ...button, value: "{" },
			{ ...button, action_id: "ask_user_question_001_other" },
			{ ...button, type: "static_select" },
			{ ...button, value: JSON.stringify({ ...data, panelId: "" }) },
			{
				...button,
				value: JSON.stringify({ ...data, origin: { platform: "slack", kind: "thread", conversationId: "C123:1.0" } }),
			},
			{
				...button,
				value: JSON.stringify({
					...data,
					origin: { platform: "slack", kind: "thread", conversationId: "C999:1.0", parentId: "C123" },
				}),
			},
			{ ...button, value: JSON.stringify({ ...data, origin: { ...channel, conversationId: "C999" } }) },
			{ ...button, value: JSON.stringify({ ...data, responseKind: "approved" }) },
		]) {
			expect(describePanelResponse(click(invalid, channel))).toBeNull();
		}
		expect(describePanelResponse({ ...click(button, channel), actions: [] })).toBeNull();
		expect(describePanelResponse({ ...click(button, channel), actions: [button, button] })).toBeNull();
		const threadButton = buttons(askUserPanelBlocks(message(thread)))[0] as Button;
		expect(describePanelResponse(click(threadButton, thread, "999.000"))).toBeNull();
	});

	it("shows duplicate warnings in blocks and fallback text for active and expired panels", () => {
		for (const expiresAt of [
			new Date(Date.now() + 60_000).toISOString(),
			new Date(Date.now() - 60_000).toISOString(),
		]) {
			const base = message();
			const ask = { ...base, duplicateWarning: true, askUserPanel: { ...base.askUserPanel!, expiresAt } };
			const approval = {
				...ask,
				askUserPanel: undefined,
				approvalPanel: { panelId: "approval", message: "Approve?", expiresAt },
			};
			for (const [blocks, fallback] of [
				[askUserPanelBlocks(ask), askUserPanelFallback(ask)],
				[approvalPanelBlocks(approval), approvalPanelFallback(approval)],
			] as const) {
				expect(JSON.stringify(blocks)).toContain("[recovered - may be a duplicate]");
				expect(fallback).toContain("[recovered - may be a duplicate]");
			}
		}
	});
});
