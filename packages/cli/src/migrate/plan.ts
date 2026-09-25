import type { MonitorSpec, OriginRef } from "@gajae-gateway/protocol";
import { eventTypeOrigin, validateOriginRef } from "@gajae-gateway/protocol";

export const MIGRATION_SOURCES = ["hermes", "openclaw"] as const;
export type MigrationSource = (typeof MIGRATION_SOURCES)[number];

export type MigrationCategory = "persona" | "memory" | "channels" | "credentials" | "monitors" | "skills" | "other";

/** One file the migration produces, relative to the gajae-way home. */
export interface SourceFile {
	readonly category: MigrationCategory;
	/** Source path(s) this output derives from, for the report. */
	readonly sources: readonly string[];
	/** Target path relative to the gajae-way home. */
	readonly target: string;
	readonly content: string | Uint8Array;
	/** Credential material: written 0600 under secrets/, never printed. */
	readonly secret?: boolean;
	/** Modification time to carry over from the source (timestamps are preserved). */
	readonly mtime?: Date;
}

/**
 * A JSON document merged into whatever the target home already holds, so a
 * re-run over its own output converges instead of stacking duplicates.
 */
export interface MergedJson {
	readonly category: MigrationCategory;
	readonly sources: readonly string[];
	readonly target: string;
	merge(existing: Record<string, unknown> | undefined): Record<string, unknown>;
}

export interface Unmapped {
	readonly category: MigrationCategory;
	readonly source: string;
	readonly reason: string;
}

export interface Inventory {
	readonly persona: string[];
	readonly memory: string[];
	readonly channels: string[];
	readonly schedules: string[];
	readonly skills: string[];
}

/** Everything a source reader extracts; the planner turns it into concrete writes. */
export interface SourceExtract {
	readonly source: MigrationSource;
	readonly root: string;
	readonly inventory: Inventory;
	readonly files: SourceFile[];
	readonly merges: MergedJson[];
	readonly monitors: { readonly source: string; readonly spec: MonitorSpec }[];
	readonly unmapped: Unmapped[];
	readonly warnings: string[];
}

export function emptyExtract(source: MigrationSource, root: string): SourceExtract {
	return {
		source,
		root,
		inventory: { persona: [], memory: [], channels: [], schedules: [], skills: [] },
		files: [],
		merges: [],
		monitors: [],
		unmapped: [],
		warnings: [],
	};
}

/** The subset of five-field cron syntax the gateway's cron trigger evaluates (no names, no `?`/`L`/`#`). */
const CRON_FIELD = /^(\*|\d+(-\d+)?)(\/\d+)?(,(\*|\d+(-\d+)?)(\/\d+)?)*$/;
const CRON_BOUNDS: readonly (readonly [number, number])[] = [
	[0, 59],
	[0, 23],
	[1, 31],
	[1, 12],
	[0, 7],
];

export function gatewayCron(expr: string): string | undefined {
	const fields = expr.trim().split(/\s+/);
	if (fields.length !== 5) return undefined;
	for (const [index, field] of fields.entries()) {
		if (!CRON_FIELD.test(field)) return undefined;
		const [min, max] = CRON_BOUNDS[index] as readonly [number, number];
		for (const number of field.match(/\d+/g) ?? []) {
			const value = Number(number);
			// Step values are divisors, not field values; only bound the field values.
			if (field.includes(`/${number}`)) continue;
			if (value < min || value > max) return undefined;
		}
	}
	return fields.join(" ");
}

/** A fixed interval as a cron schedule, when it divides the hour or the day evenly. */
export function intervalCron(minutes: number): string | undefined {
	if (!Number.isInteger(minutes) || minutes < 1) return undefined;
	if (minutes === 1) return "* * * * *";
	if (minutes < 60 && 60 % minutes === 0) return `*/${minutes} * * * *`;
	if (minutes === 60) return "0 * * * *";
	if (minutes % 60 === 0 && minutes < 1440 && 24 % (minutes / 60) === 0) return `0 */${minutes / 60} * * *`;
	if (minutes === 1440) return "0 0 * * *";
	return undefined;
}

export function slug(value: string): string {
	return (
		value
			.toLowerCase()
			.replace(/[^a-z0-9]+/g, "-")
			.replace(/^-+|-+$/g, "")
			.slice(0, 48)
			.replace(/-+$/, "") || "job"
	);
}

export const MONITOR_INSTRUCTION_MAX = 4000;

/**
 * Builds a validated cron MonitorSpec, or the reason it cannot be one. The
 * checks mirror the gateway registry's own validation so a spec accepted here
 * is never rejected at import.
 */
export function cronMonitor(input: {
	readonly source: MigrationSource;
	readonly name: string;
	readonly schedule: string;
	readonly instruction: string;
	readonly enabled: boolean;
	readonly target?: OriginRef;
	readonly taken: Set<string>;
}): MonitorSpec | string {
	const instruction = input.instruction.trim();
	if (!instruction) return "job has no prompt text to use as the monitor instruction";
	if (instruction.length > MONITOR_INSTRUCTION_MAX)
		return `prompt is ${instruction.length} characters; monitor instructions are limited to ${MONITOR_INSTRUCTION_MAX}`;
	const base = `${input.source}-${slug(input.name)}`;
	let name = base;
	for (let n = 2; input.taken.has(name); n++) name = `${base}-${n}`;
	const eventType = `migrated.${name}`;
	validateOriginRef(eventTypeOrigin(eventType));
	if (input.target) validateOriginRef(input.target);
	input.taken.add(name);
	return {
		name,
		trigger: { kind: "cron", schedule: input.schedule },
		eventTypes: [eventType],
		burstPolicy: "dedupe",
		instruction,
		...(input.target ? { channelTarget: { origin: input.target } } : {}),
		enabled: input.enabled,
	};
}

/**
 * A delivery target as an origin. Telegram chat ids are negative for groups and
 * positive for the private chat with that user, where the chat id is the peer.
 * Discord and Slack DMs need a conversation id the source does not record.
 */
export function deliveryOrigin(platform: string, rawTarget: string): OriginRef | string {
	const target = rawTarget.trim();
	if (platform === "telegram") {
		const chat = target.replace(/^(channel|group|chat):/, "").split(":")[0] ?? "";
		if (!/^-?\d+$/.test(chat)) return `telegram target ${JSON.stringify(target)} is not a numeric chat id`;
		return chat.startsWith("-")
			? { platform: "telegram", kind: "channel", conversationId: chat }
			: { platform: "telegram", kind: "dm", conversationId: chat, peerId: chat };
	}
	if (platform === "discord" || platform === "slack") {
		if (target.startsWith("user:"))
			return `${platform} user target ${JSON.stringify(target)} needs a DM conversation id the source does not record`;
		const id = target.replace(/^channel:/, "");
		const valid = platform === "discord" ? /^\d+$/.test(id) : /^[CG][A-Z0-9]+$/.test(id);
		if (!valid) return `${platform} target ${JSON.stringify(target)} is not a channel id`;
		return { platform, kind: "channel", conversationId: id };
	}
	return `delivery platform ${JSON.stringify(platform)} has no gajae-way adapter`;
}

/** Channel credential names this migration can carry. */
export type ChannelCredential = "discord" | "telegram" | "slack-bot" | "slack-app";

export const SECRET_FILES: Record<ChannelCredential, string> = {
	discord: "secrets/discord-token",
	telegram: "secrets/telegram-token",
	"slack-bot": "secrets/slack-bot-token",
	"slack-app": "secrets/slack-app-token",
};

export interface ChannelPlan {
	readonly credentials: Partial<Record<ChannelCredential, { readonly value: string; readonly source: string }>>;
	/** Gateway `config.json` channel policies keyed `<platform>:<id>`. */
	readonly channels: Record<string, { engagement: "open" | "mention-open" }>;
	readonly allowlist: Set<string>;
	dmPolicy?: "open";
}

export function emptyChannelPlan(): ChannelPlan {
	return { credentials: {}, channels: {}, allowlist: new Set() };
}

/**
 * Turns a channel plan into secret files plus merged gateway/adapter config.
 * Merges only add: an existing policy, allowlist entry or owner target the
 * operator already set is never replaced by an imported one.
 */
export function emitChannels(extract: SourceExtract, plan: ChannelPlan, home: string, configSource: string): void {
	const credentialSources = Object.values(plan.credentials).map((entry) => entry.source);
	for (const [name, entry] of Object.entries(plan.credentials) as [
		ChannelCredential,
		{ value: string; source: string },
	][]) {
		extract.files.push({
			category: "credentials",
			sources: [entry.source],
			target: SECRET_FILES[name],
			content: `${entry.value}\n`,
			secret: true,
		});
	}
	const has = (name: ChannelCredential) => plan.credentials[name] !== undefined;
	const byPlatform = (platform: string) =>
		Object.fromEntries(
			Object.entries(plan.channels)
				.filter(([key]) => key.startsWith(`${platform}:`))
				.map(([key, policy]) => [key.slice(platform.length + 1), policy]),
		);
	const credentialRefs: Record<string, { credentialFile: string }> = {};
	if (has("discord")) credentialRefs.discord = { credentialFile: `${home}/${SECRET_FILES.discord}` };
	if (has("telegram")) credentialRefs.telegram = { credentialFile: `${home}/${SECRET_FILES.telegram}` };
	if (has("slack-bot")) credentialRefs.slackBot = { credentialFile: `${home}/${SECRET_FILES["slack-bot"]}` };
	if (has("slack-app")) credentialRefs.slackApp = { credentialFile: `${home}/${SECRET_FILES["slack-app"]}` };
	const allowlist = [...plan.allowlist].sort();
	extract.merges.push({
		category: "channels",
		sources: [configSource, ...credentialSources],
		target: "config.json",
		merge(existing) {
			const base = existing ?? { schemaVersion: 1 };
			const credentials = { ...(base.credentials as Record<string, unknown> | undefined) };
			for (const [name, reference] of Object.entries(credentialRefs)) credentials[name] ??= reference;
			const channels = { ...(base.channels as Record<string, unknown> | undefined) };
			for (const [key, policy] of Object.entries(plan.channels)) channels[key] ??= policy;
			const existingAllow = Array.isArray(base.mentionAllowlist) ? (base.mentionAllowlist as string[]) : [];
			const mergedAllow = [...new Set([...existingAllow, ...allowlist])];
			return {
				...base,
				...(Object.keys(credentials).length ? { credentials } : {}),
				...(Object.keys(channels).length ? { channels } : {}),
				...(mergedAllow.length ? { mentionAllowlist: mergedAllow } : {}),
				...(base.dmPolicy === undefined && plan.dmPolicy ? { dmPolicy: plan.dmPolicy } : {}),
			};
		},
	});
	const adapter = (target: string, required: Record<string, string>, channelKey: string, platform: string) => {
		const channels = byPlatform(platform);
		// The Telegram adapter's chat map only accepts `open`; mention gating stays in config.json.
		const adapterChannels =
			platform === "telegram"
				? Object.fromEntries(Object.entries(channels).filter(([, policy]) => policy.engagement === "open"))
				: channels;
		extract.merges.push({
			category: "channels",
			sources: [configSource],
			target,
			merge(existing) {
				const base = { ...existing };
				for (const [field, value] of Object.entries(required)) base[field] ??= value;
				const merged = { ...(base[channelKey] as Record<string, unknown> | undefined) };
				for (const [id, policy] of Object.entries(adapterChannels)) merged[id] ??= policy;
				return { ...base, ...(Object.keys(merged).length ? { [channelKey]: merged } : {}) };
			},
		});
	};
	if (has("discord")) adapter("adapter-discord.json", { tokenFile: SECRET_FILES.discord }, "channels", "discord");
	if (has("telegram")) adapter("adapter-telegram.json", { tokenFile: SECRET_FILES.telegram }, "chats", "telegram");
	if (has("slack-bot") && has("slack-app"))
		adapter(
			"adapter-slack.json",
			{ botTokenFile: SECRET_FILES["slack-bot"], appTokenFile: SECRET_FILES["slack-app"] },
			"channels",
			"slack",
		);
	else if (has("slack-bot") !== has("slack-app"))
		extract.warnings.push(
			"Slack needs both a bot token (xoxb-) and a Socket Mode app token (xapp-); only one was found, so adapter-slack.json was not written",
		);
	for (const [name, prefix] of [
		["slack-bot", "xoxb-"],
		["slack-app", "xapp-"],
	] as const) {
		const value = plan.credentials[name]?.value;
		if (value !== undefined && !value.startsWith(prefix))
			extract.warnings.push(`${SECRET_FILES[name]} does not start with ${prefix}; the Slack adapter will refuse it`);
	}
}

/** Adds imported monitor specs to `migration/monitors.json`, which the gateway imports once at boot. */
export function emitMonitors(extract: SourceExtract): void {
	if (extract.monitors.length === 0) return;
	const specs = extract.monitors.map((entry) => entry.spec);
	extract.merges.push({
		category: "monitors",
		sources: [...new Set(extract.monitors.map((entry) => entry.source))],
		target: MIGRATED_MONITORS_FILE,
		merge(existing) {
			const current = Array.isArray(existing?.monitors) ? (existing.monitors as MonitorSpec[]) : [];
			const names = new Set(current.map((spec) => spec.name));
			return { version: 1, monitors: [...current, ...specs.filter((spec) => !names.has(spec.name))] };
		},
	});
}

/** Relative to the gajae-way home; the gateway reads the same path (packages/gateway/src/monitors/migrated.ts). */
export const MIGRATED_MONITORS_FILE = "migration/monitors.json";

/** Custom memory axis that holds imported curated memory, so the audit does not report it as orphaned. */
export const IMPORTED_AXIS = {
	id: "imported",
	displayName: "Imported memory",
	root: "imported",
	nesting: "nested",
	index: "tree",
	layout: "free",
	retrievalPriority: 50,
} as const;

export function emitImportedAxis(extract: SourceExtract): void {
	extract.merges.push({
		category: "memory",
		sources: [],
		target: "memory/axes.json",
		merge(existing) {
			const axes = Array.isArray(existing?.axes) ? (existing.axes as { id?: unknown }[]) : [];
			if (axes.some((axis) => axis.id === IMPORTED_AXIS.id)) return { version: 1, ...existing, axes };
			return { version: 1, ...existing, axes: [...axes, IMPORTED_AXIS] };
		},
	});
}
