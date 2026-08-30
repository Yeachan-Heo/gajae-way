/**
 * `@gajaeway/voice-core` — provider-agnostic voice logic.
 *
 * Everything here is pure: no I/O, no timers, no ambient clock, and no dependency on
 * `discord.js` or any provider SDK. Time and configuration are always supplied by the
 * caller, which is what makes the audio behavior unit-testable without audio.
 */

export * from "./barge-in";
export * from "./drill-log";
export * from "./energy-gate";
export * from "./merge-window";
export * from "./modality";
export * from "./pcm";
export * from "./ports";
export * from "./speaker-meta";
export * from "./spoken-prefix";
export * from "./stt-machine";
export * from "./turn-gate";
export * from "./utterance";
export * from "./voice-unread";
