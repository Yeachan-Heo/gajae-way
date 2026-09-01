import type { GjcModelSelection } from "../store/db";

/** The store surface `/model` needs; narrowed so tests need no real database. */
export interface ModelOverrideStore {
	conversationModelGet(originKey: string): { readonly selection: GjcModelSelection } | undefined;
	conversationModelSet(originKey: string, selection: GjcModelSelection, setBy?: string): void;
	conversationModelClear(originKey: string): boolean;
}

export interface ModelCommandOutcome {
	readonly text: string;
	/**
	 * Whether the caller must reset the conversation's session. The selector is
	 * argv on an already-running gjc process, so a change cannot reach a live
	 * session: either we reset, or the command is a lie until an unrelated
	 * restart happens.
	 */
	readonly resetSession: boolean;
}

/** Renders a selection the way the owner typed it. */
export function describeSelection(selection: GjcModelSelection): string {
	return typeof selection === "string" ? selection : `preset ${selection.preset}`;
}

/**
 * Parses a `/model` argument into a selection. A bare token containing `/` is
 * treated as an explicit provider selector; anything else is a preset name,
 * which is what a slash-command choice supplies. `preset:` and `model:`
 * prefixes force the interpretation when a name is ambiguous.
 */
export function parseModelArgument(argument: string): GjcModelSelection | { readonly error: string } {
	const raw = argument.trim();
	if (raw === "") return { error: "empty selection" };
	if (/\s/.test(raw)) return { error: "a selection cannot contain spaces" };
	const forcedPreset = /^preset:(.+)$/.exec(raw);
	if (forcedPreset?.[1]) return { preset: forcedPreset[1] };
	const forcedModel = /^model:(.+)$/.exec(raw);
	if (forcedModel?.[1]) return forcedModel[1];
	return raw.includes("/") ? raw : { preset: raw };
}

/**
 * Executes `/model`, `/model <choice>`, `/model set <choice>` or
 * `/model clear`, returning the reply text and whether a session reset is
 * required. Reporting where the effective selection came from is deliberate:
 * "which model am I talking to" is unanswerable otherwise, and a silent
 * config default is the thing people get wrong.
 */
export function applyModelCommand(
	text: string,
	originKey: string,
	origin: { readonly platform: string },
	store: ModelOverrideStore,
	configModel: GjcModelSelection | undefined,
	setBy?: string,
): ModelCommandOutcome {
	const rest = text.slice("/model".length).trim();
	const [head = "", ...tail] = rest.split(/\s+/).filter((part) => part !== "");
	const showEffective = (): string => {
		const override = store.conversationModelGet(originKey)?.selection;
		if (override) return `🦞 model: **${describeSelection(override)}** (this conversation)`;
		if (configModel) return `🦞 model: **${describeSelection(configModel)}** (gateway default)`;
		return "🦞 model: **gjc default** (no gateway setting, no conversation override)";
	};

	if (rest === "" || head === "show") return { text: showEffective(), resetSession: false };

	if (head === "clear" || head === "reset" || head === "default") {
		const removed = store.conversationModelClear(originKey);
		if (!removed) return { text: `no conversation override to clear. ${showEffective()}`, resetSession: false };
		return {
			text: `🦞 cleared this conversation's override, session reset. ${showEffective()}`,
			resetSession: true,
		};
	}

	const argument = head === "set" ? tail.join(" ") : rest;
	const parsed = parseModelArgument(argument);
	if (typeof parsed !== "string" && "error" in parsed)
		return {
			text: `could not read that selection (${parsed.error}). usage: \`/model\`, \`/model set <preset-or-selector>\`, \`/model clear\``,
			resetSession: false,
		};
	store.conversationModelSet(originKey, parsed, setBy);
	// Say the reset out loud. A model change that silently applied "sometime
	// later" would be indistinguishable from one that did nothing.
	const scope = origin.platform === "loopback" ? "this session" : "this conversation";
	return {
		text: `🦞 model set to **${describeSelection(parsed)}** for ${scope}. session reset so it takes effect now.`,
		resetSession: true,
	};
}
