import { join } from "node:path";
import type { MonitorSpec, OriginRef } from "@gajae-gateway/protocol";
import {
	type ChannelPlan,
	cronMonitor,
	deliveryOrigin,
	emptyChannelPlan,
	emptyExtract,
	intervalCron,
	MONITOR_INSTRUCTION_MAX,
	type SourceExtract,
	slug,
} from "./plan";
import { isDirectory, isRecord, parseDotenv, regularFile, type SourceTree, stringList, walkFiles } from "./source-fs";

/**
 * OpenClaw is a gajae-way-compatible agent framework. Migration reads its
 * config.json, .env, and skill/memory structures from an OpenClaw home.
 */
export const OPENCLAW_CONFIG = "config.json";

export async function detectOpenClaw(root: string): Promise<boolean> {
	const configPath = join(root, OPENCLAW_CONFIG);
	const stat = await regularFile(configPath);
	return stat !== undefined;
}

interface OpenClawConfig {
	persona?: { name?: string };
	skills?: Record<string, { enabled?: boolean }>;
	memory?: Record<string, unknown>;
	channels?: Record<string, unknown>;
	monitors?: Record<string, unknown>;
}

export async function readOpenClaw(tree: SourceTree, _home: string): Promise<SourceExtract> {
	const extract = emptyExtract("openclaw", tree.root);
	const configPath = join(tree.root, OPENCLAW_CONFIG);
	const envPath = join(tree.root, ".env");

	// Read configuration
	let config: OpenClawConfig = {};
	const configText = await tree.text(configPath);
	if (configText) {
		try {
			config = JSON.parse(configText) as OpenClawConfig;
		} catch (e) {
			extract.warnings.push(`Invalid config.json: ${String(e)}`);
		}
	}

	// Read environment variables
	const env = new Map<string, string>();
	const envText = await tree.text(envPath);
	if (envText) {
		const parsed = parseDotenv(envText);
		for (const [key, val] of parsed.entries()) {
			env.set(key, val);
		}
	}

	// Establish consumed sets for tracking unmapped entries
	const consumedConfig = new Set<string>();
	const consumedEnv = new Set<string>();

	// Extract persona
	if (config.persona?.name) {
		extract.files.push({
			path: "persona.json",
			category: "persona",
			content: JSON.stringify({ name: config.persona.name }),
		});
		consumedConfig.add("persona");
	}

	// Extract channel policies and credentials
	const channels: ChannelPlan = emptyChannelPlan();
	if (isRecord(config.channels)) {
		readOpenClawChannels(config.channels, env, channels, consumedConfig, consumedEnv);
	}

	// Extract monitors
	if (isRecord(config.monitors)) {
		for (const [name, spec] of Object.entries(config.monitors)) {
			if (isRecord(spec)) {
				const monitorResult = buildOpenClawMonitor(name, spec);
				if (typeof monitorResult === "object") {
					extract.monitors.push({ source: `config.monitors.${name}`, spec: monitorResult });
				} else {
					extract.unmapped.push({
						path: `config.monitors.${name}`,
						reason: monitorResult,
						category: "monitors",
					});
				}
			}
		}
		consumedConfig.add("monitors");
	}

	// Extract skills
	if (isRecord(config.skills)) {
		await readOpenClawSkills(tree, config.skills, extract);
		consumedConfig.add("skills");
	}

	// Extract memory
	if (isRecord(config.memory)) {
		await readOpenClawMemory(tree, config.memory, extract);
		consumedConfig.add("memory");
	}

	// Report unmapped
	for (const [key] of Object.entries(config)) {
		if (!consumedConfig.has(key)) {
			extract.unmapped.push({
				path: `config.${key}`,
				reason: "not mapped by migration",
				category: "other",
			});
		}
	}

	for (const [key] of env.entries()) {
		if (!consumedEnv.has(key)) {
			extract.unmapped.push({
				path: `.env.${key}`,
				reason: "not mapped by migration",
				category: "credentials",
			});
		}
	}

	return extract;
}

function readOpenClawChannels(
	config: Record<string, unknown>,
	env: Map<string, string>,
	channels: ChannelPlan,
	consumedConfig: Set<string>,
	consumedEnv: Set<string>,
): void {
	for (const [platform, platformConfig] of Object.entries(config)) {
		if (!isRecord(platformConfig)) continue;

		if (platform === "discord" || platform === "telegram" || platform === "slack") {
			// Handle credentials
			const credKey = `${platform.toUpperCase()}_TOKEN`;
			const token = env.get(credKey);
			if (token) {
				consumedEnv.add(credKey);
				channels.credentials[platform === "slack" ? "slack-bot" : platform] = {
					name: `${platform}-bot`,
					secret: token,
				};
			}

			// Handle channel policies
			if (isRecord(platformConfig.channels)) {
				for (const [channelId, channelPolicy] of Object.entries(platformConfig.channels)) {
					if (isRecord(channelPolicy)) {
						const engagement = typeof channelPolicy.engagement === "string" ? channelPolicy.engagement : "closed";
						const audience = typeof channelPolicy.audience === "string" ? channelPolicy.audience : "human-only";
						const owner = typeof channelPolicy.owner === "string" ? channelPolicy.owner : undefined;

						channels.channels[`${platform}:${channelId}`] = {
							engagement: engagement as "open" | "mention-open" | "closed",
							audience: audience as "all" | "human-only" | "bot-only",
							owner,
						};
					}
				}
			}

			// Handle allowlist
			const allowlistKey = `${platform.toUpperCase()}_ALLOWED_USERS`;
			const allowlist = env.get(allowlistKey);
			if (allowlist) {
				consumedEnv.add(allowlistKey);
				for (const user of stringList(allowlist)) {
					channels.allowlist.add(`${platform}:${user}`);
				}
			}
		}
	}
	consumedConfig.add("channels");
}

function buildOpenClawMonitor(name: string, spec: Record<string, unknown>): MonitorSpec | string {
	const schedule = typeof spec.schedule === "string" ? spec.schedule : undefined;
	const enabled = typeof spec.enabled === "boolean" ? spec.enabled : true;
	const instruction = typeof spec.instruction === "string" ? spec.instruction : "";
	const target = typeof spec.target === "string" ? spec.target : undefined;

	if (!schedule || instruction.length === 0) {
		return "Missing schedule or instruction";
	}

	if (instruction.length > MONITOR_INSTRUCTION_MAX) {
		return `Instruction exceeds ${MONITOR_INSTRUCTION_MAX} characters`;
	}

	// Validate and convert schedule
	const cronExpr = schedule.includes("/") ? schedule : intervalCron(parseInt(schedule, 10));
	if (!cronExpr) {
		return "Invalid schedule";
	}

	// Parse target
	let deliveryTarget: OriginRef | undefined;
	if (target) {
		const [platform, ...rest] = target.split(":");
		const rawTarget = rest.join(":");
		const origin = deliveryOrigin(platform, rawTarget);
		if (typeof origin === "object") {
			deliveryTarget = origin;
		}
	}

	return cronMonitor({
		name: slug(name),
		instruction,
		schedule: cronExpr,
		enabled,
		target: deliveryTarget,
	});
}

async function readOpenClawSkills(
	tree: SourceTree,
	skillsConfig: Record<string, unknown>,
	extract: SourceExtract,
): Promise<void> {
	const skillsDir = join(tree.root, "skills");
	const hasSkillsDir = await isDirectory(skillsDir);

	for (const [skillName] of Object.entries(skillsConfig)) {
		if (!hasSkillsDir) continue;

		const skillPath = join(skillsDir, `${skillName}.md`);
		const skillContent = await tree.text(skillPath);

		if (skillContent) {
			extract.files.push({
				path: join(".gjc", "skills", `${slug(skillName)}.md`),
				category: "skills",
				content: skillContent,
			});
		}
	}
}

async function readOpenClawMemory(
	tree: SourceTree,
	_memoryConfig: Record<string, unknown>,
	extract: SourceExtract,
): Promise<void> {
	const memoryDir = join(tree.root, "memory");
	const hasMemoryDir = await isDirectory(memoryDir);

	if (!hasMemoryDir) {
		return;
	}

	const files = await walkFiles(memoryDir);

	for (const file of files) {
		if (file.endsWith(".md")) {
			const fullPath = join(memoryDir, file);
			const content = await tree.text(fullPath);

			if (content) {
				extract.files.push({
					path: join("memory", "imported.md"),
					category: "memory",
					content,
				});
			}
		}
	}
}
