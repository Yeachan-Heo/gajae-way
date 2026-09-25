import { join } from "node:path";
import type { OriginRef } from "@gajae-gateway/protocol";
import {
	type ChannelPlan,
	cronMonitor,
	deliveryOrigin,
	emitChannels,
	emitImportedAxis,
	emitMonitors,
	emptyChannelPlan,
	emptyExtract,
	gatewayCron,
	intervalCron,
	type SourceExtract,
} from "./plan";
import { entries, isDirectory, isRecord, parseDotenv, regularFile, type SourceTree, stringList, walkFiles } from "./source-fs";

/**
 * Hermes Agent (NousResearch/hermes-agent) keeps everything under one home,
 * `~/.hermes` (HERMES_HOME):
 *
 *   SOUL.md, AGENTS.md            persona / instructions
 *   memories/MEMORY.md, USER.md   bounded entry stores, entries joined by "\n§\n"
 *   .env                          credentials (DISCORD_BOT_TOKEN, SLACK_BOT_TOKEN, ...)
 *   config.yaml                   settings (discord.free_response_channels, ...)
 *   cron/jobs.json                {"jobs": [...], "updated_at": ...}
 *   skills/<category>/<name>/SKILL.md, skills/.bundled_manifest
 *
 * See docs/migration.md for the upstream references.
 */
export const HERMES_ENTRY_DELIMITER = "\n§\n";

export async function detectHermes(root: string): Promise<boolean> {
	return (
		(await regularFile(join(root, "config.yaml"))) !== undefined ||
		(await isDirectory(join(root, "memories"))) ||
		(await regularFile(join(root, "cron", "jobs.json"))) !== undefined
	);
}

/** Channel tokens Hermes reads from `.env`, keyed to the gajae-way credential they become. */
const TOKEN_KEYS = {
	DISCORD_BOT_TOKEN: "discord",
	TELEGRAM_BOT_TOKEN: "telegram",
	SLACK_BOT_TOKEN: "slack-bot",
	SLACK_APP_TOKEN: "slack-app",
} as const;

const ALLOWLIST_KEYS = ["DISCORD_ALLOWED_USERS", "TELEGRAM_ALLOWED_USERS", "SLACK_ALLOWED_USERS"] as const;
const HOME_CHANNEL_KEYS: Record<string, string> = {
	discord: "DISCORD_HOME_CHANNEL",
	telegram: "TELEGRAM_HOME_CHANNEL",
	slack: "SLACK_HOME_CHANNEL",
};

export async function readHermes(tree: SourceTree, home: string): Promise<SourceExtract> {
	const root = tree.root;
	const extract = emptyExtract("hermes", root);

	for (const name of ["SOUL.md", "AGENTS.md"] as const) {
		const path = join(root, name);
		const text = await tree.text(path);
		if (text === undefined) continue;
		const info = await regularFile(path);
		extract.inventory.persona.push(tree.rel(path));
		extract.files.push({ category: "persona", sources: [tree.rel(path)], target: `workspace/${name}`, content: text, mtime: info?.mtime });
	}

	const memoryDir = join(root, "memories");
	const userPath = join(memoryDir, "USER.md");
	const userText = await tree.text(userPath);
	if (userText !== undefined) {
		const info = await regularFile(userPath);
		extract.inventory.persona.push(tree.rel(userPath));
		extract.files.push({
			category: "persona",
			sources: [tree.rel(userPath)],
			target: "workspace/USER.md",
			content: entriesToMarkdown("User profile (imported from Hermes)", userText),
			mtime: info?.mtime,
		});
	}
	const memoryPath = join(memoryDir, "MEMORY.md");
	const memoryText = await tree.text(memoryPath);
	if (memoryText !== undefined) {
		const info = await regularFile(memoryPath);
		extract.inventory.memory.push(tree.rel(memoryPath));
		extract.files.push({
			category: "memory",
			sources: [tree.rel(memoryPath)],
			target: "memory/imported/hermes/MEMORY.md",
			content: entriesToMarkdown("Long-term memory (imported from Hermes)", memoryText),
			mtime: info?.mtime,
		});
		emitImportedAxis(extract);
	}

	const envPath = join(root, ".env");
	const envText = await tree.text(envPath);
	const env = envText === undefined ? new Map<string, string>() : parseDotenv(envText);
	const consumedEnv = new Set<string>();
	const configPath = join(root, "config.yaml");
	const configText = await tree.text(configPath);
	let config: Record<string, unknown> = {};
	if (configText !== undefined) {
		try {
			const parsed = Bun.YAML.parse(configText);
			if (isRecord(parsed)) config = parsed;
		} catch (error) {
			extract.warnings.push(`config.yaml could not be parsed (${error instanceof Error ? error.message : String(error)}); channel policies were not read`);
		}
	}
	const consumedConfig = new Set<string>();

	const channels = emptyChannelPlan();
	for (const [key, credential] of Object.entries(TOKEN_KEYS)) {
		const value = env.get(key);
		if (!value) continue;
		consumedEnv.add(key);
		extract.inventory.channels.push(`.env ${key}`);
		if (value.includes(",")) {
			extract.unmapped.push({
				category: "credentials",
				source: `.env ${key}`,
				reason: "holds several comma-separated tokens (multi-workspace); gajae-way runs one bot per adapter, so none was imported",
			});
			continue;
		}
		channels.credentials[credential] = { value, source: `.env ${key}` };
	}
	for (const key of ALLOWLIST_KEYS) {
		const value = env.get(key);
		if (value === undefined) continue;
		consumedEnv.add(key);
		for (const id of stringList(value)) channels.allowlist.add(id);
	}
	readHermesChannelPolicy(config, env, channels, consumedConfig, consumedEnv, extract);
	if (Object.keys(channels.credentials).length > 0 || Object.keys(channels.channels).length > 0)
		emitChannels(extract, channels, home, ".env / config.yaml");

	await readHermesCron(tree, env, consumedEnv, extract);
	emitMonitors(extract);
	await readHermesSkills(tree, extract);

	for (const key of env.keys())
		if (!consumedEnv.has(key))
			extract.unmapped.push({
				category: "credentials",
				source: `.env ${key}`,
				reason: /(_KEY|_TOKEN|_SECRET|PASSWORD)$/.test(key)
					? "provider/tool credential; gajae-way sessions use the shared gjc agent profile, so set it there if still needed"
					: "setting has no gajae-way equivalent",
			});
	for (const key of Object.keys(config))
		if (!consumedConfig.has(key))
			extract.unmapped.push({ category: "other", source: `config.yaml ${key}`, reason: "setting has no gajae-way equivalent" });
	return extract;
}

/** Hermes stores memory as delimited entries; gajae-way memory is Markdown, one bullet block per entry. */
export function entriesToMarkdown(title: string, raw: string): string {
	const items = raw
		.split(HERMES_ENTRY_DELIMITER)
		.map((entry) => entry.trim())
		.filter(Boolean);
	return `# ${title}\n\n${items.map((entry) => `- ${entry.replace(/\n/g, "\n  ")}`).join("\n")}\n`;
}

function readHermesChannelPolicy(
	config: Record<string, unknown>,
	env: Map<string, string>,
	channels: ChannelPlan,
	consumedConfig: Set<string>,
	consumedEnv: Set<string>,
	extract: SourceExtract,
): void {
	const discord = isRecord(config.discord) ? config.discord : undefined;
	if (discord) {
		consumedConfig.add("discord");
		const free = stringList(discord.free_response_channels);
		for (const id of free) {
			if (id === "*") {
				extract.unmapped.push({ category: "channels", source: "config.yaml discord.free_response_channels", reason: '"*" (every channel) has no per-channel equivalent; list channels explicitly' });
				continue;
			}
			channels.channels[`discord:${id}`] = { engagement: "open" };
			extract.inventory.channels.push(`config.yaml discord.free_response_channels ${id}`);
		}
		for (const key of Object.keys(discord))
			if (key !== "free_response_channels" && key !== "require_mention")
				extract.unmapped.push({ category: "channels", source: `config.yaml discord.${key}`, reason: "Discord adapter setting has no gajae-way equivalent" });
	}
	const slackChannels = env.get("SLACK_ALLOWED_CHANNELS");
	if (slackChannels !== undefined) {
		consumedEnv.add("SLACK_ALLOWED_CHANNELS");
		for (const id of stringList(slackChannels)) channels.channels[`slack:${id}`] = { engagement: "mention-open" };
	}
	const telegram = isRecord(config.telegram) ? config.telegram : undefined;
	if (telegram) consumedConfig.add("telegram");
	const telegramGroups = env.get("TELEGRAM_GROUP_ALLOWED_CHATS");
	if (telegramGroups !== undefined) {
		consumedEnv.add("TELEGRAM_GROUP_ALLOWED_CHATS");
		const engagement = telegram?.require_mention === true ? "mention-open" : "open";
		for (const id of stringList(telegramGroups)) channels.channels[`telegram:${id}`] = { engagement };
	}
	for (const key of ["DISCORD_ALLOW_ALL_USERS", "TELEGRAM_ALLOW_ALL_USERS", "SLACK_ALLOW_ALL_USERS", "GATEWAY_ALLOW_ALL_USERS"]) {
		if (!env.has(key)) continue;
		consumedEnv.add(key);
		extract.unmapped.push({
			category: "channels",
			source: `.env ${key}`,
			reason: "allow-everyone is not carried over; gajae-way DMs default to the allowlist. Set dmPolicy \"open\" in config.json deliberately if wanted",
		});
	}
}

async function readHermesCron(tree: SourceTree, env: Map<string, string>, consumedEnv: Set<string>, extract: SourceExtract): Promise<void> {
	const path = join(tree.root, "cron", "jobs.json");
	const text = await tree.text(path);
	if (text === undefined) return;
	let jobs: unknown[] = [];
	try {
		const parsed: unknown = JSON.parse(text);
		jobs = Array.isArray(parsed) ? parsed : isRecord(parsed) && Array.isArray(parsed.jobs) ? parsed.jobs : [];
	} catch {
		extract.unmapped.push({ category: "monitors", source: tree.rel(path), reason: "is not valid JSON" });
		return;
	}
	const taken = new Set<string>();
	for (const job of jobs) {
		if (!isRecord(job)) continue;
		const label = String(job.name ?? job.id ?? "job");
		const source = `${tree.rel(path)} ${JSON.stringify(label)}`;
		extract.inventory.schedules.push(source);
		const skip = (reason: string) => extract.unmapped.push({ category: "monitors", source, reason });
		if (job.no_agent === true || (typeof job.script === "string" && job.script))
			{ skip("script job: gajae-way script monitors run a command under scriptRoot, recreate it with a script trigger"); continue; }
		if (typeof job.monitor_script === "string" || typeof job.monitor_url === "string")
			{ skip("change-detection pre-check (monitor_script/monitor_url) has no equivalent"); continue; }
		const schedule = isRecord(job.schedule) ? job.schedule : {};
		let cron: string | undefined;
		if (schedule.kind === "cron" && typeof schedule.expr === "string") {
			cron = gatewayCron(schedule.expr);
			if (!cron) { skip(`cron expression ${JSON.stringify(schedule.expr)} uses syntax the gateway cron trigger does not evaluate`); continue; }
		} else if (schedule.kind === "interval" && typeof schedule.minutes === "number") {
			cron = intervalCron(schedule.minutes);
			if (!cron) { skip(`every ${schedule.minutes}m does not divide an hour or a day, so it has no cron form`); continue; }
		} else {
			skip(`schedule kind ${JSON.stringify(schedule.kind)} (one-shot) has no recurring monitor equivalent`);
			continue;
		}
		const skills = stringList(job.skills);
		const prompt = typeof job.prompt === "string" ? job.prompt : "";
		const instruction = skills.length ? `Use the skill(s): ${skills.join(", ")}.\n\n${prompt}` : prompt;
		const target = hermesTarget(job, env, consumedEnv, extract, source);
		const repeat = isRecord(job.repeat) ? job.repeat.times : null;
		if (typeof repeat === "number")
			extract.warnings.push(`${source}: repeat limit ${repeat} dropped; the monitor fires until removed`);
		const spec = cronMonitor({
			source: "hermes",
			name: label,
			schedule: cron,
			instruction,
			enabled: job.enabled !== false && job.state !== "paused",
			target,
			taken,
		});
		if (typeof spec === "string") skip(spec);
		else extract.monitors.push({ source, spec });
	}
}

function hermesTarget(
	job: Record<string, unknown>,
	env: Map<string, string>,
	consumedEnv: Set<string>,
	extract: SourceExtract,
	source: string,
): OriginRef | undefined {
	const targets = stringList(job.deliver ?? "local").filter((target) => target !== "local");
	if (targets.length === 0) return undefined;
	const resolved: OriginRef[] = [];
	for (const target of targets) {
		let result: OriginRef | string;
		if (target === "origin" || target === "origin_fallback") {
			const origin = isRecord(job.origin) ? job.origin : undefined;
			result = origin && typeof origin.platform === "string" && origin.chat_id !== undefined
				? deliveryOrigin(origin.platform, String(origin.chat_id))
				: "origin delivery recorded no origin chat";
		} else {
			const [platform = "", ...rest] = target.split(":");
			let chat = rest.join(":");
			if (!chat && HOME_CHANNEL_KEYS[platform]) {
				chat = env.get(HOME_CHANNEL_KEYS[platform] as string) ?? "";
				consumedEnv.add(HOME_CHANNEL_KEYS[platform] as string);
				consumedEnv.add(`${HOME_CHANNEL_KEYS[platform]}_NAME`);
			}
			result = chat ? deliveryOrigin(platform, chat) : `delivery target ${JSON.stringify(target)} has no chat id or home channel`;
		}
		if (typeof result === "string") extract.warnings.push(`${source}: ${result}; delivered to the owner target instead`);
		else resolved.push(result);
	}
	if (resolved.length > 1)
		extract.warnings.push(`${source}: a monitor has one channel target; kept the first of ${resolved.length}`);
	return resolved[0];
}

async function readHermesSkills(tree: SourceTree, extract: SourceExtract): Promise<void> {
	const skillsDir = join(tree.root, "skills");
	if (!(await isDirectory(skillsDir))) return;
	tree.consume(skillsDir);
	const bundledText = await tree.text(join(skillsDir, ".bundled_manifest"));
	const bundled = new Set(
		(bundledText ?? "")
			.split(/\r?\n/)
			.map((line) => line.split(":")[0]?.trim() ?? "")
			.filter(Boolean),
	);
	const files = (await walkFiles(skillsDir)).filter((file) => !file.split("/").some((part) => part.startsWith(".")));
	const skillRoots = files.filter((file) => file.endsWith("/SKILL.md") || file === "SKILL.md").map((file) => file.slice(0, -"/SKILL.md".length));
	const seen = new Set<string>();
	for (const skillRoot of skillRoots) {
		const name = skillRoot.split("/").at(-1) ?? skillRoot;
		if (bundled.has(name)) continue;
		await copySkill(tree, join(skillsDir, skillRoot), name, extract, seen);
	}
	if (bundled.size) extract.warnings.push(`${bundled.size} Hermes-bundled skill(s) skipped; they ship with Hermes, not with your data`);
}

export async function copySkill(
	tree: SourceTree,
	dir: string,
	name: string,
	extract: SourceExtract,
	seen: Set<string>,
): Promise<void> {
	const source = tree.rel(dir);
	extract.inventory.skills.push(source);
	if (name === "self-ops" || seen.has(name)) {
		extract.unmapped.push({ category: "skills", source, reason: `a skill named ${name} is already imported or built in` });
		return;
	}
	seen.add(name);
	for (const file of await walkFiles(dir)) {
		const path = join(dir, file);
		const content = await tree.bytes(path);
		if (content === undefined) continue;
		extract.files.push({
			category: "skills",
			sources: [tree.rel(path)],
			target: `workspace/.gjc/skills/${name}/${file}`,
			content,
			mtime: (await regularFile(path))?.mtime,
		});
	}
}

/** Top-level entries no reader consumed, reported so nothing is dropped silently. */
export async function reportLeftovers(tree: SourceTree, dir: string, extract: SourceExtract, reason: (name: string) => string): Promise<void> {
	for (const name of await entries(dir)) {
		const path = join(dir, name);
		if (tree.isFullyConsumed(path)) continue;
		if (tree.isConsumed(path)) {
			if (await isDirectory(path)) await reportLeftovers(tree, path, extract, reason);
			continue;
		}
		extract.unmapped.push({ category: "other", source: tree.rel(path), reason: reason(name) });
	}
}
