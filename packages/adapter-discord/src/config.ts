/**
 * Inbound voice-message transcription.
 *
 * The key lives in a file, never inline, for the same reason the Discord token
 * does: a config is readable, quotable, and gets pasted into issues.
 *
 * Absent `voice` means transcription is off and voice messages arrive with
 * their url alone — the pre-existing behaviour — so omitting this section can
 * never break an existing deployment.
 */
export interface DiscordVoiceConfig {
	readonly apiKeyFile: string;
	/**
	 * Pinned language for transcription; omitted means auto-detect.
	 *
	 * Auto-detect measured p=1.0 on long Korean speech but failed *confidently*
	 * on short clips — a 1.9s Korean message came back as Spanish at p=0.999 —
	 * so a single-language deployment should pin this. A confidence floor cannot
	 * substitute: the wrong answer arrived at maximum confidence.
	 */
	readonly languageCode?: string;
	readonly endpoint?: string;
	readonly model?: string;
	readonly timeoutMs?: number;
	/** Outbound speech; omitted fields fall back to the tested ElevenLabs defaults. */
	readonly voiceId?: string;
	readonly speechModel?: string;
	readonly speechEndpoint?: string;
	readonly outputFormat?: string;
	/**
	 * Optional spoken length cap. Unset means the whole reply is spoken.
	 *
	 * Capping was tried and reverted: a listener cannot read the remainder out of
	 * the text, so a truncated utterance is a truncated answer for the only
	 * person the audio exists for.
	 */
	readonly maxSpokenChars?: number;
	readonly speechTimeoutMs?: number;
	/**
	 * Playback speed, 0.7-1.2 (the provider's range). Defaults to the ceiling:
	 * the owner asked for faster delivery and billing is per character, so speed
	 * costs nothing. Out-of-range values are clamped, not rejected, because a
	 * typo here should not silence the voice reply entirely.
	 */
	readonly speechSpeed?: number;
}

export interface LoadedDiscordVoiceConfig extends DiscordVoiceConfig {
	readonly apiKey: string;
}
