import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import type { Block } from "@slack/web-api";

/**
 * Slack Block Kit blocks for an ask-user interactive panel with buttons.
 * Falls back to plain text if buttons are not supported.
 */
export function askUserPanelBlocks(message: ChatMessagePayload): Block[] {
	const panel = message.askUserPanel;
	if (!panel) return [];

	const now = Date.now();
	const expiresAt = new Date(panel.expiresAt).getTime();
	const isExpired = now >= expiresAt;

	const blocks: Block[] = [
		{
			type: "section",
			text: {
				type: "mrkdwn",
				text: panel.question,
			},
		},
	];

	if (isExpired) {
		// Expired: show plain text fallback without interactive buttons
		const optionsList = panel.options.map((opt) => `• ${opt.label}`).join("\n");
		blocks.push({
			type: "section",
			text: {
				type: "mrkdwn",
				text: `_(This question has expired. Options were:)\n${optionsList}_`,
			},
		});
	} else {
		// Not expired: show interactive buttons
		const buttonElements: Array<{
			type: "button";
			text: { type: "plain_text"; text: string; emoji: boolean };
			action_id: string;
			value: string;
		}> = [];
		for (const option of panel.options) {
			buttonElements.push({
				type: "button",
				text: {
					type: "plain_text",
					text: option.label,
					emoji: true,
				},
				action_id: `ask_user_${panel.panelId}_${option.id}`,
				value: option.id,
			});
		}

		// Slack button layouts have a max of 5 buttons per row, and max 50 blocks total
		// Group buttons into rows of 5
		for (let i = 0; i < buttonElements.length; i += 5) {
			const rowActions = buttonElements.slice(i, i + 5);
			blocks.push({
				type: "actions",
				elements: rowActions as unknown[],
			});
		}
	}

	return blocks;
}

/**
 * Slack Block Kit blocks for an approval (allow/deny) interactive panel.
 * Falls back to plain text if buttons are not supported.
 */
export function approvalPanelBlocks(message: ChatMessagePayload): Block[] {
	const panel = message.approvalPanel;
	if (!panel) return [];

	const now = Date.now();
	const expiresAt = new Date(panel.expiresAt).getTime();
	const isExpired = now >= expiresAt;

	const blocks: Block[] = [
		{
			type: "section",
			text: {
				type: "mrkdwn",
				text: panel.message,
			},
		},
	];

	if (isExpired) {
		// Expired: show plain text without interactive buttons
		blocks.push({
			type: "section",
			text: {
				type: "mrkdwn",
				text: "_(This approval request has expired.)_",
			},
		});
	} else {
		// Not expired: show interactive allow/deny buttons
		blocks.push({
			type: "actions",
			elements: [
				{
					type: "button",
					text: {
						type: "plain_text",
						text: "Allow",
						emoji: true,
					},
					style: "primary",
					action_id: `approval_${panel.panelId}_allow`,
					value: "allow",
					confirm: {
						title: {
							type: "plain_text",
							text: "Confirm",
						},
						text: {
							type: "mrkdwn",
							text: "Are you sure you want to allow this?",
						},
						confirm: {
							type: "plain_text",
							text: "Allow",
						},
						deny: {
							type: "plain_text",
							text: "Cancel",
						},
					},
				},
				{
					type: "button",
					text: {
						type: "plain_text",
						text: "Deny",
						emoji: true,
					},
					style: "danger",
					action_id: `approval_${panel.panelId}_deny`,
					value: "deny",
					confirm: {
						title: {
							type: "plain_text",
							text: "Confirm",
						},
						text: {
							type: "mrkdwn",
							text: "Are you sure you want to deny this?",
						},
						confirm: {
							type: "plain_text",
							text: "Deny",
						},
						deny: {
							type: "plain_text",
							text: "Cancel",
						},
					},
				},
			],
		});
	}

	return blocks;
}

/**
 * Plain text fallback for ask-user panel when Block Kit is unavailable.
 */
export function askUserPanelFallback(message: ChatMessagePayload): string | null {
	const panel = message.askUserPanel;
	if (!panel) return null;

	const now = Date.now();
	const expiresAt = new Date(panel.expiresAt).getTime();
	const isExpired = now >= expiresAt;

	let text = `${panel.question}\n`;
	text += panel.options.map((opt) => `• ${opt.label}`).join("\n");

	if (isExpired) {
		text += "\n_(This question has expired.)_";
	}

	return text;
}

/**
 * Plain text fallback for approval panel when Block Kit is unavailable.
 */
export function approvalPanelFallback(message: ChatMessagePayload): string | null {
	const panel = message.approvalPanel;
	if (!panel) return null;

	const now = Date.now();
	const expiresAt = new Date(panel.expiresAt).getTime();
	const isExpired = now >= expiresAt;

	let text = `${panel.message}\n`;
	if (isExpired) {
		text += "_(This approval request has expired.)_";
	} else {
		text += "React with :white_check_mark: to allow or :x: to deny.";
	}

	return text;
}
