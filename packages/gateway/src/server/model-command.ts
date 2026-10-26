import { join } from "node:path";
import type { GjcModelSelection } from "../store/groups/bindings";
import type { WriteTx } from "../store/tx";

/** The store surface `/model` needs; narrowed so tests need no real database. */
export interface ModelOverrideStore {
	readonly bindings: {
		modelGet(originKey: string): { readonly selection: GjcModelSelection } | undefined;
		modelSet(tx: WriteTx, originKey: string, selection: GjcModelSelection, setBy?: string): void;
		modelClear(tx: WriteTx, originKey: string): boolean;
	};
	transaction<T>(operation: (tx: WriteTx) => T): T;
}

export type ModelRebindIntent =
	| { readonly kind: "set"; readonly selection: GjcModelSelection }
	| { readonly kind: "clear" };

export interface ModelCommandOutcome {
	readonly text: string;
	/** A same-session control transition the per-origin actor must serialize. */
	readonly rebind?: ModelRebindIntent;
}

/** A provider model listing is served this long before a background refresh. */
export const PROVIDER_MODELS_TTL_MS = 5 * 60_000;
/** A failed listing is retried no sooner than this, so a broken gjc is not respawned per keystroke. */
export const PROVIDER_MODELS_RETRY_MS = 30_000;
/**
 * Discord drops an autocomplete answer after 3s. A cold listing (gjc start plus
 * catalog resolution) is waited for at most this long; past it the choices go
 * out without provider models and the listing lands for the next keystroke.
 */
export const PROVIDER_MODELS_WAIT_MS = 1_500;

export interface ProviderModelCatalogOptions {
	readonly ttlMs?: number;
	readonly retryMs?: number;
	readonly waitMs?: number;
	readonly now?: () => number;
	readonly log?: (line: string) => void;
}

/**
 * Cached `provider/model` ids from gjc's own model listing for `/model set`
 * autocomplete. One listing runs at a time; a fresh listing is served from
 * memory, a stale one is served while it refreshes, and a failed one keeps the
 * last good listing (or none). It never throws: suggestions are a convenience
 * and a typed choice is accepted either way.
 */
export class ProviderModelCatalog {
	readonly #load: () => Promise<readonly string[]>;
	readonly #ttlMs: number;
	readonly #retryMs: number;
	readonly #waitMs: number;
	readonly #now: () => number;
	readonly #log: (line: string) => void;
	#models: readonly string[] = [];
	#loaded = false;
	#refreshAt = 0;
	#inflight: Promise<void> | undefined;

	constructor(load: () => Promise<readonly string[]>, options: ProviderModelCatalogOptions = {}) {
		this.#load = load;
		this.#ttlMs = options.ttlMs ?? PROVIDER_MODELS_TTL_MS;
		this.#retryMs = options.retryMs ?? PROVIDER_MODELS_RETRY_MS;
		this.#waitMs = options.waitMs ?? PROVIDER_MODELS_WAIT_MS;
		this.#now = options.now ?? Date.now;
		this.#log = options.log ?? ((line) => console.error(line));
	}

	async list(): Promise<readonly string[]> {
		if (this.#now() >= this.#refreshAt) this.#refresh();
		const inflight = this.#inflight;
		// Only a cold catalog waits; a stale one answers now and refreshes behind.
		if (inflight && !this.#loaded) {
			let timer: ReturnType<typeof setTimeout> | undefined;
			await Promise.race([inflight, new Promise<void>((resolve) => (timer = setTimeout(resolve, this.#waitMs)))]);
			if (timer !== undefined) clearTimeout(timer);
		}
		return this.#models;
	}

	#refresh(): void {
		if (this.#inflight) return;
		this.#inflight = Promise.resolve()
			.then(() => this.#load())
			.then(
				(models) => {
					this.#models = [...models];
					this.#loaded = true;
					this.#refreshAt = this.#now() + this.#ttlMs;
				},
				(error: unknown) => {
					this.#refreshAt = this.#now() + this.#retryMs;
					this.#log(
						`model_choices_provider_list_failed reason=${error instanceof Error ? error.message : String(error)}`,
					);
				},
			)
			.finally(() => {
				this.#inflight = undefined;
			});
	}
}

/**
 * `/model set` autocomplete choices: the preset names in the gjc profile's
 * `models.yml` `profiles:` map plus the configured gateway selector, sorted,
 * then the `provider/model` ids gjc lists, sorted. Named choices lead so an
 * empty query still shows them inside Discord's 25-choice window. Fails soft to
 * whatever could be read: a missing or unreadable models file must not break
 * the command, because a typed choice is still accepted.
 */
export async function listModelChoices(
	agentDir: string | undefined,
	configModel: GjcModelSelection | undefined,
	providerModels: readonly string[] = [],
): Promise<string[]> {
	const choices = new Set<string>();
	if (configModel) choices.add(typeof configModel === "string" ? configModel : configModel.preset);
	if (agentDir) {
		try {
			const parsed = Bun.YAML.parse(await Bun.file(join(agentDir, "models.yml")).text()) as unknown;
			const profiles = (parsed as { profiles?: unknown } | null)?.profiles;
			if (profiles && typeof profiles === "object" && !Array.isArray(profiles))
				for (const name of Object.keys(profiles)) choices.add(name);
		} catch {
			// Fail soft: no catalog means no suggestions, never a broken command.
		}
	}
	// A name with whitespace could never round-trip through `/model set <choice>`.
	const usable = (choice: string): boolean => choice !== "" && !/\s/.test(choice);
	const named = [...choices].filter(usable).sort();
	const provided = [...new Set(providerModels)].filter((choice) => usable(choice) && !choices.has(choice)).sort();
	return [...named, ...provided];
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
 * `/model clear`. A change returns a same-session rebind intent; the actor
 * applies it through `model.set` without changing the conversation epoch.
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
		const override = store.bindings.modelGet(originKey)?.selection;
		if (override) return `🦞 model: **${describeSelection(override)}** (this conversation)`;
		if (configModel) return `🦞 model: **${describeSelection(configModel)}** (gateway default)`;
		return "🦞 model: **gjc default** (no gateway setting, no conversation override)";
	};

	if (rest === "" || head === "show") return { text: showEffective() };

	if (head === "clear" || head === "reset" || head === "default") {
		const existing = store.bindings.modelGet(originKey)?.selection;
		if (!existing) return { text: `no conversation override to clear. ${showEffective()}` };
		if (!configModel)
			return {
				text: "cannot live-clear this override because no gateway default model is configured. Set a gateway model or choose an explicit model instead.",
			};
		store.transaction((tx) => store.bindings.modelClear(tx, originKey));
		return {
			text: `🦞 cleared this conversation's override. ${showEffective()} The model applies from the next turn on this same session.`,
			rebind: { kind: "clear" },
		};
	}

	const argument = head === "set" ? tail.join(" ") : rest;
	const parsed = parseModelArgument(argument);
	if (typeof parsed !== "string" && "error" in parsed)
		return {
			text: `could not read that selection (${parsed.error}). usage: \`/model\`, \`/model set <preset-or-selector>\`, \`/model clear\``,
		};
	store.transaction((tx) => store.bindings.modelSet(tx, originKey, parsed, setBy));
	const scope = origin.platform === "loopback" ? "this session" : "this conversation";
	return {
		text: `🦞 model set to **${describeSelection(parsed)}** for ${scope}. It applies from the next turn on this same session.`,
		rebind: { kind: "set", selection: parsed },
	};
}
