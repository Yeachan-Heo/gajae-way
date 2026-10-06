import type { EngagementPanelResponseParams, OriginRef } from "@gajae-gateway/protocol";

/**
 * Slack block_actions event from Socket Mode.
 * Used for interactive components like buttons and select menus.
 */
export interface SlackBlockAction {
	type: "block_actions";
	actions: Array<{
		type: string;
		action_id: string;
		value?: string;
		selected_option?: { value: string };
	}>;
	trigger_id: string;
	user: {
		id: string;
		username: string;
		name: string;
		team_id: string;
	};
	team: {
		id: string;
		domain: string;
	};
	channel: {
		id: string;
		name: string;
	};
	message: {
		type: string;
		user?: string;
		ts: string;
		bot_id?: string;
		text?: string;
	};
	view?: {
		id: string;
		team_id: string;
		type: string;
		blocks: Array<Record<string, unknown>>;
		private_metadata: string;
		callback_id: string;
		state: Record<string, Record<string, unknown>>;
		hash: string;
		title: {
			type: string;
			text: string;
			emoji: boolean;
		};
		clear_on_close: boolean;
		notify_on_close: boolean;
		previous_view_id: string | null;
		root_view_id: string;
		app_id: string;
		external_trigger_id?: string;
		app_installed_team_id: string;
		created_at: number;
		updated_at: number;
	};
	response_url?: string;
	response_url_verified?: boolean;
	ack_url?: string;
	token: string;
	api_app_id: string;
	event_ts: string;
	event_id: string;
}

/**
 * Parses a block_actions event and extracts panel response information.
 * Returns null if the action is not a panel response.
 */
export function describePanelResponse(
	event: SlackBlockAction,
	origin: OriginRef,
): EngagementPanelResponseParams | null {
	if (!event.actions || event.actions.length === 0) return null;

	const action = event.actions[0];
	if (!action.action_id) return null;

	// Parse action_id format: ask_user_<panelId>_<optionId> or approval_<panelId>_allow/deny
	// Use greedy matching for panelId to handle IDs with underscores, stop at last underscore
	const askUserMatch = action.action_id.match(/^ask_user_(.+)_([^_]+)$/);
	if (askUserMatch) {
		const [, panelId, optionId] = askUserMatch;
		return {
			origin,
			panelId,
			responseKind: "option_selected",
			selectedOptionId: optionId,
			responderId: event.user.id,
			engagement: {
				mentioned: true,
				group: origin.kind !== "dm",
				authorId: event.user.id,
				authorName: event.user.name,
			},
		};
	}

	const approvalMatch = action.action_id.match(/^approval_(.+?)_(allow|deny)$/);
	if (approvalMatch) {
		const [, panelId, decision] = approvalMatch;
		return {
			origin,
			panelId,
			responseKind: decision === "allow" ? "approved" : "denied",
			responderId: event.user.id,
			engagement: {
				mentioned: true,
				group: origin.kind !== "dm",
				authorId: event.user.id,
				authorName: event.user.name,
			},
		};
	}

	return null;
}
