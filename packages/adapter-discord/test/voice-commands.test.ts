import { describe, expect, test } from "bun:test";
import type { VoiceConfig } from "../src/config";
import {
	handleVoiceCommand,
	type VoiceCommandInteractionLike,
	type VoiceCommandSessionManagerLike,
} from "../src/voice/commands";
import type { VoiceJoinRequest } from "../src/voice/session";

const config: VoiceConfig = {
	enabled: true,
	silenceEndMs: 700,
	energyGate: { minDurationMs: 300, rmsThreshold: 0.02 },
	idleLeaveMs: 300_000,
	unread: { maxItems: 20, maxCharsPerItem: 200 },
	mergeWindowMs: 1_500,
	transcriptWaitMs: 1_500,
	sessionMaxMs: 3_600_000,
	turnMapTtlMs: 900_000,
	outstanding: { ttlMs: 900_000, maxEntries: 32 },
	bargeIn: { minTranscriptChars: 3, cooldownMs: 1_500, echoSimilarity: 0.6, frameCorrectionMs: 20 },
	ingress: { maxQueuedFramesPerSpeaker: 200, overflow: "drop_oldest" },
	reconnect: { initialBackoffMs: 500, maxBackoffMs: 30_000, maxAttempts: 6 },
	joinCommandAllowlist: ["owner"],
	announceOnTurnStart: true,
	drillLog: { enabled: false, path: "artifacts" },
	elevenlabs: {
		apiKeyFile: "elevenlabs-key",
		stt: {
			model: "scribe_v2_realtime",
			audioFormat: "pcm_16000",
			commitStrategy: "vad",
			includeLanguageDetection: true,
			filterBackgroundAudio: true,
			keyterms: [],
		},
		tts: {
			model: "eleven_flash_v2_5",
			voiceId: "voice-1",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 20,
			applyTextNormalization: "auto",
			chunkLengthSchedule: [120, 160, 250, 300],
		},
	},
};

type FakeSession = { readonly state: "active" | "closed" };

class FakeManager implements VoiceCommandSessionManagerLike {
	config: VoiceConfig | { readonly voice?: VoiceConfig } = config;
	readonly joins: VoiceJoinRequest[] = [];
	readonly leaves: string[] = [];
	readonly sessions = new Map<string, FakeSession>();

	get(originKey: string): FakeSession | undefined {
		return this.sessions.get(originKey);
	}

	async join(request: VoiceJoinRequest): Promise<FakeSession> {
		this.joins.push(request);
		const session: FakeSession = { state: "active" };
		this.sessions.set(request.originKey, session);
		return session;
	}

	async leave(originKey: string): Promise<void> {
		this.leaves.push(originKey);
		this.sessions.set(originKey, { state: "closed" });
	}
}

function interaction(
	subcommand: "join" | "leave",
	options: {
		readonly userId?: string;
		readonly channelId?: string;
		readonly guildId?: string;
	} = {},
): VoiceCommandInteractionLike & {
	readonly replies: Array<{ readonly content: string; readonly ephemeral?: boolean }>;
} {
	const replies: Array<{ readonly content: string; readonly ephemeral?: boolean }> = [];
	return {
		isChatInputCommand: () => true,
		commandName: "voice",
		subcommand,
		id: `interaction-${subcommand}`,
		user: { id: options.userId ?? "owner" },
		guildId: options.guildId ?? "guild-1",
		member:
			options.channelId === undefined
				? { voice: { channel: null, channelId: null } }
				: { voice: { channel: { id: options.channelId, guildId: options.guildId ?? "guild-1" } } },
		replies,
		reply: async (value) => {
			replies.push(value);
		},
	};
}

describe("/voice commands", () => {
	test.each([
		["disabled", (manager: FakeManager) => (manager.config = { ...config, enabled: false })],
		["not allowlisted", (manager: FakeManager) => (manager.config = { ...config, joinCommandAllowlist: ["other"] })],
		["not in a voice channel", (_manager: FakeManager) => {}],
	] as const)("rejects %s honestly and creates no session", async (caseName, configure) => {
		const manager = new FakeManager();
		configure(manager);
		const noVoice = caseName === "not in a voice channel";
		const command = interaction("join", {
			userId: caseName === "not allowlisted" ? "owner" : "owner",
			channelId: noVoice ? undefined : "room-1",
		});
		await handleVoiceCommand(command, manager);
		expect(manager.joins).toHaveLength(0);
		expect(command.replies).toHaveLength(1);
		expect(command.replies[0]?.ephemeral).toBe(true);
		expect(command.replies[0]?.content).not.toContain("joined");
	});

	test("successfully joins exactly one session without admitting a turn", async () => {
		const manager = new FakeManager();
		const command = interaction("join", { channelId: "room-1" });
		await handleVoiceCommand(command, manager);
		expect(manager.joins).toEqual([{ originKey: "discord/channel/room-1", channelId: "room-1", guildId: "guild-1" }]);
		expect(command.replies).toEqual([{ content: "joined the voice channel", ephemeral: true }]);
	});

	test("leave closes the active session with a command operation and is honest when none exists", async () => {
		const manager = new FakeManager();
		manager.sessions.set("discord/channel/room-1", { state: "active" });
		const leave = interaction("leave", { channelId: "room-1" });
		await handleVoiceCommand(leave, manager);
		expect(manager.leaves).toEqual(["discord/channel/room-1"]);
		expect(leave.replies).toEqual([{ content: "left the voice channel", ephemeral: true }]);

		const noSession = interaction("leave", { channelId: "room-2" });
		await handleVoiceCommand(noSession, manager);
		expect(manager.leaves).toEqual(["discord/channel/room-1"]);
		expect(noSession.replies).toEqual([{ content: "no active voice session", ephemeral: true }]);
	});

	test("ignores non-chat and non-voice commands", async () => {
		const manager = new FakeManager();
		const command = interaction("join", { channelId: "room-1" });
		const nonChat = { ...command, isChatInputCommand: () => false };
		await handleVoiceCommand(nonChat, manager);
		const other = { ...command, commandName: "reset" };
		await handleVoiceCommand(other, manager);
		expect(manager.joins).toHaveLength(0);
		expect(command.replies).toHaveLength(0);
	});
});
