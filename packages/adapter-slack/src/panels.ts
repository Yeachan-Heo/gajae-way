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
		// Not expired: show interactive buttons for offered options.
		if (!panel.digest) {
			// A legacy or malformed panel has no request binding; buttons would only
			// produce an answer the gateway must refuse.
			blocks.push({
				type: "section",
				text: { type: "mrkdwn", text: "_(This approval request cannot be answered.)_" },
			});
			return blocks;
		}
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
					// The digest binds the click to this exact request; panelId alone is
					// not an approval. Slack caps action_id at 255 chars.
					action_id: `approval_${panel.panelId}_${panel.digest}_${option.optionId}`,
					value: option.optionId,
				});
			}
		} else {
			// No offered options: render no buttons. An optionless allow/deny
			// fallback would approve a request without binding an exact option.
		}

		// Slack rejects an overlong action_id. Never truncate the binding: a
		// read-only panel is the safe fallback when an offered option id does not
		// fit alongside the panel id and digest.
		if (buttonElements.some((button) => button.action_id.length > 255)) {
			blocks.push({
				type: "section",
				text: {
					type: "mrkdwn",
					text: "_(This approval request cannot be answered with buttons; wait for it to expire.)_",
				},
			});
			return blocks;
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
		if (panel.options?.length) {
			text += "\nOptions were:\n";
			for (const option of panel.options) text += `• ${option.name}\n`;
		}
	} else if (panel.options && panel.options.length > 0) {
		text += "Options:\n";
		for (const option of panel.options) {
			text += `• ${option.name}\n`;
		}
	} else {
		text += "_No options were offered for this request._";
	}

	return text;
}
