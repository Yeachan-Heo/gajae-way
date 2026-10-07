import type { ChatMessagePayload } from "@gajae-gateway/protocol";

/** Slack Block Kit block definition */
type Block = Record<string, unknown>;

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
		const optionsList = panel.options ? panel.options.map((opt) => `• ${opt.name}`).join("\n") : "";
		blocks.push({
			type: "section",
			text: {
				type: "mrkdwn",
				text: `_(This approval request has expired.${optionsList ? ` Options were:\n${optionsList}` : ""})_`,
			},
		});
	} else {
		// Not expired: show interactive buttons for offered options
		const buttonElements: Array<{
			type: "button";
			text: { type: "plain_text"; text: string; emoji: boolean };
			action_id: string;
			value: string;
			style?: string;
		}> = [];

		// If options are provided, render them; otherwise fall back to allow/deny
		if (panel.options && panel.options.length > 0) {
			for (const option of panel.options) {
				// Style reject-like options as danger
				const style = option.kind.toLowerCase().includes("reject") ? "danger" : "primary";
				buttonElements.push({
					type: "button",
					text: {
						type: "plain_text",
						text: option.name,
						emoji: true,
					},
					style,
					action_id: `approval_${panel.panelId}_${option.optionId}`,
					value: option.optionId,
				});
			}
		} else {
			// Fallback to allow/deny buttons if no options provided
			buttonElements.push(
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
				},
			);
		}

		// Slack button layouts have a max of 5 buttons per row
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
	} else if (panel.options && panel.options.length > 0) {
		text += "Options:\n";
		for (const option of panel.options) {
			text += `• ${option.name}\n`;
		}
	} else {
		text += "React with :white_check_mark: to allow or :x: to deny.";
	}

	return text;
}
