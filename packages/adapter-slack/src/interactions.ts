import { type EngagementPanelResponseParams, type OriginRef, validateOriginRef } from "@gajae-gateway/protocol";
import { parseSlackMessageId } from "./origin";

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
		thread_ts?: string;
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

type PanelResponseKind = "option_selected" | "approved" | "denied";

/** Encode the original routing origin and the selected option in the button value. */
export function panelAction(
	type: "ask_user" | "approval",
	panelId: string,
	optionId: string,
	origin: OriginRef,
	responseKind: PanelResponseKind,
): { action_id: string; value: string } {
	return {
		action_id: `${type}_${panelId}_${optionId}`,
		value: JSON.stringify({ panelId, optionId, origin, responseKind }),
	};
}

/**
 * Parses a block_actions event and extracts panel response information.
 * Returns null if the action is not a panel response.
 */
export function describePanelResponse(event: SlackBlockAction): EngagementPanelResponseParams | null {
	if (event.type !== "block_actions" || !Array.isArray(event.actions) || event.actions.length !== 1) return null;
	const action = event.actions[0];
	if (
		!action ||
		action.type !== "button" ||
		typeof action.value !== "string" ||
		typeof event.user?.id !== "string" ||
		!event.user.id
	)
		return null;
	let data: { panelId?: unknown; optionId?: unknown; origin?: unknown; responseKind?: unknown };
	try {
		data = JSON.parse(action.value);
		if (!data || typeof data !== "object" || Array.isArray(data)) return null;
	} catch {
		return null;
	}
	const { panelId, optionId, responseKind } = data;
	if (typeof panelId !== "string" || !panelId || typeof optionId !== "string" || !optionId) return null;
	const ask = action.action_id === `ask_user_${panelId}_${optionId}` && responseKind === "option_selected";
	const approval =
		action.action_id === `approval_${panelId}_${optionId}` &&
		(responseKind === "option_selected" ||
			(responseKind === "approved" && optionId === "allow") ||
			(responseKind === "denied" && optionId === "deny"));
	if (!ask && !approval) return null;
	if (!data.origin || typeof data.origin !== "object" || Array.isArray(data.origin)) return null;
	const origin = data.origin as OriginRef;
	try {
		validateOriginRef(origin);
	} catch {
		return null;
	}
	if (origin.platform !== "slack" || !["dm", "channel", "thread"].includes(origin.kind)) return null;
	if (typeof event.channel?.id !== "string" || !event.channel.id) return null;
	if ((origin.kind === "thread" ? origin.parentId : origin.conversationId) !== event.channel.id) return null;
	if (origin.kind === "thread") {
		const thread = parseSlackMessageId(origin.conversationId);
		if (
			!thread ||
			thread.channel !== origin.parentId ||
			(event.message?.thread_ts && event.message.thread_ts !== thread.ts)
		)
			return null;
	}
	return {
		origin,
		panelId,
		responseKind: responseKind as PanelResponseKind,
		...(responseKind === "option_selected" ? { selectedOptionId: optionId } : {}),
		responderId: event.user.id,
		engagement: {
			mentioned: true,
			group: origin.kind !== "dm",
			authorId: event.user.id,
			authorName: event.user.name,
		},
	};
}
