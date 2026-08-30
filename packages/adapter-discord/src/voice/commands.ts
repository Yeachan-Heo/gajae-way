import type { VoiceConfig } from "../config";
import type { VoiceJoinRequest, VoiceSessionState } from "./session";

export interface VoiceCommandUser {
	readonly id: string;
	readonly username?: string;
	readonly globalName?: string | null;
}

export interface VoiceCommandChannel {
	readonly id: string;
	readonly guildId?: string;
}

export interface VoiceCommandMember {
	readonly voice?: {
		readonly channel?: VoiceCommandChannel | null;
		readonly channelId?: string | null;
	} | null;
}

export interface VoiceCommandInteractionLike {
	isChatInputCommand?(): boolean;
	readonly commandName?: string;
	readonly subcommand?: string;
	readonly id: string;
	readonly user?: VoiceCommandUser;
	readonly guildId?: string | null;
	readonly guild?: { readonly id: string } | null;
	readonly member?: VoiceCommandMember | null;
	readonly options?: { getSubcommand?(): string };
	reply(options: { readonly content: string; readonly ephemeral?: boolean }): Promise<unknown>;
}

export interface VoiceCommandSessionLike {
	readonly state: VoiceSessionState;
}

export interface VoiceCommandSessionManagerLike {
	readonly config?: VoiceConfig | { readonly voice?: VoiceConfig };
	readonly voiceConfig?: VoiceConfig;
	get(originKey: string): VoiceCommandSessionLike | undefined;
	join(request: VoiceJoinRequest): Promise<VoiceCommandSessionLike>;
	leave(originKey: string): Promise<void>;
}

export function voiceOriginKey(channelId: string): string {
	return `discord/channel/${channelId}`;
}

/** Handles only the Discord /voice join and /voice leave command surface. */
export async function handleVoiceCommand(
	interaction: VoiceCommandInteractionLike,
	sessions: VoiceCommandSessionManagerLike,
	log: Pick<Console, "error"> = console,
): Promise<void> {
	if (!interaction.isChatInputCommand?.()) return;
	if (interaction.commandName !== "voice") return;

	const subcommand = resolveSubcommand(interaction);
	if (subcommand !== "join" && subcommand !== "leave") return;

	const config = resolveVoiceConfig(sessions);
	if (config === undefined || !config.enabled) {
		await reply(interaction, "voice is disabled");
		return;
	}
	const user = interaction.user;
	if (user === undefined || !config.joinCommandAllowlist.includes(user.id)) {
		await reply(interaction, "you are not allowed to use voice commands");
		return;
	}

	const target = resolveVoiceTarget(interaction);
	if (target === undefined) {
		await reply(interaction, "join a voice channel first");
		return;
	}

	const originKey = voiceOriginKey(target.channelId);
	if (subcommand === "leave") {
		const current = sessions.get(originKey);
		if (current === undefined || current.state === "closed") {
			await reply(interaction, "no active voice session");
			return;
		}
		try {
			await sessions.leave(originKey);
			await reply(interaction, "left the voice channel");
		} catch (error) {
			log.error(`Discord voice leave failed: ${error instanceof Error ? error.message : String(error)}`);
			await reply(interaction, "unable to leave the voice channel");
		}
		return;
	}

	const request: VoiceJoinRequest = {
		originKey,
		channelId: target.channelId,
		...(target.guildId === undefined ? {} : { guildId: target.guildId }),
	};
	try {
		await sessions.join(request);
		await reply(interaction, "joined the voice channel");
	} catch (error) {
		log.error(`Discord voice join failed: ${error instanceof Error ? error.message : String(error)}`);
		await reply(interaction, "unable to join the voice channel");
	}
}

function resolveSubcommand(interaction: VoiceCommandInteractionLike): string | undefined {
	if (interaction.subcommand !== undefined) return interaction.subcommand;
	const option = interaction.options?.getSubcommand?.();
	if (option !== undefined) return option;
	const commandName = interaction.commandName;
	if (commandName?.startsWith("voice ")) return commandName.slice("voice ".length);
	return undefined;
}

function resolveVoiceConfig(manager: VoiceCommandSessionManagerLike): VoiceConfig | undefined {
	if (manager.voiceConfig !== undefined) return manager.voiceConfig;
	const config = manager.config;
	if (config === undefined) return undefined;
	if (isVoiceConfig(config)) return config;
	return config.voice;
}

function isVoiceConfig(value: VoiceConfig | { readonly voice?: VoiceConfig }): value is VoiceConfig {
	return "enabled" in value && typeof value.enabled === "boolean";
}

function resolveVoiceTarget(
	interaction: VoiceCommandInteractionLike,
): { readonly channelId: string; readonly guildId: string | undefined } | undefined {
	const voice = interaction.member?.voice;
	const channel = voice?.channel;
	const channelId = channel?.id ?? voice?.channelId ?? undefined;
	if (channelId === undefined || channelId.length === 0) return undefined;
	const guildId = interaction.guildId ?? interaction.guild?.id ?? channel?.guildId ?? undefined;
	return { channelId, guildId: guildId ?? undefined };
}

async function reply(interaction: VoiceCommandInteractionLike, content: string): Promise<void> {
	await interaction.reply({ content, ephemeral: true });
}
