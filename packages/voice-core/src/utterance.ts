/**
 * Authoritative per-speaker utterance boundary state machine.
 *
 * The adapter reports activity samples with injected timestamps. A silence sample
 * only ends an utterance after the configured uninterrupted silence duration; brief
 * gaps are represented in the state and are cleared by the next speech sample.
 */

export type UtteranceBoundary = "silence_end";

export interface UtteranceRecord {
	readonly speakerId: string;
	readonly startedAtMs: number;
	readonly endedAtMs: number;
	readonly boundary: UtteranceBoundary;
}

export interface UtteranceState {
	readonly speakerId: string;
	readonly silenceEndMs: number;
	readonly phase: "idle" | "speaking" | "silence";
	readonly startedAtMs: number | null;
	readonly lastSpeechAtMs: number | null;
	readonly silenceStartedAtMs: number | null;
}

export interface UtteranceAdvance {
	readonly state: UtteranceState;
	readonly utterance: UtteranceRecord | null;
}

export type UtteranceActivity = "speech" | "silence";

function assertTimestamp(atMs: number, previousAtMs: number | null): void {
	if (!Number.isFinite(atMs) || atMs < 0) {
		throw new RangeError("atMs must be a finite non-negative number");
	}
	if (previousAtMs !== null && atMs < previousAtMs) {
		throw new RangeError("atMs must not move backwards");
	}
}

/** Creates an idle tracker for one speaker. */
export function createUtteranceState(speakerId: string, silenceEndMs: number): UtteranceState {
	if (speakerId.trim() === "") {
		throw new RangeError("speakerId must not be blank");
	}
	if (!Number.isFinite(silenceEndMs) || silenceEndMs < 0) {
		throw new RangeError("silenceEndMs must be a finite non-negative number");
	}
	return {
		speakerId,
		silenceEndMs,
		phase: "idle",
		startedAtMs: null,
		lastSpeechAtMs: null,
		silenceStartedAtMs: null,
	};
}

function idleState(state: UtteranceState): UtteranceState {
	return {
		...state,
		phase: "idle",
		startedAtMs: null,
		lastSpeechAtMs: null,
		silenceStartedAtMs: null,
	};
}

/**
 * Applies one speech/silence activity sample. A boundary is emitted at the exact
 * sample timestamp that reaches the configured silence duration.
 */
export function advanceUtterance(state: UtteranceState, activity: UtteranceActivity, atMs: number): UtteranceAdvance {
	const previousAtMs = state.lastSpeechAtMs ?? state.silenceStartedAtMs;
	assertTimestamp(atMs, previousAtMs);

	if (activity === "speech") {
		return {
			state: {
				...state,
				phase: "speaking",
				startedAtMs: state.startedAtMs ?? atMs,
				lastSpeechAtMs: atMs,
				silenceStartedAtMs: null,
			},
			utterance: null,
		};
	}

	if (activity !== "silence") {
		throw new TypeError(`Unsupported utterance activity: ${String(activity)}`);
	}
	if (state.startedAtMs === null || state.lastSpeechAtMs === null) {
		return {
			state: { ...state, phase: "idle", silenceStartedAtMs: null },
			utterance: null,
		};
	}

	const silenceStartedAtMs = state.silenceStartedAtMs ?? state.lastSpeechAtMs;
	const silenceDurationMs = atMs - state.lastSpeechAtMs;
	if (silenceDurationMs < state.silenceEndMs) {
		return {
			state: { ...state, phase: "silence", silenceStartedAtMs },
			utterance: null,
		};
	}

	const utterance: UtteranceRecord = {
		speakerId: state.speakerId,
		startedAtMs: state.startedAtMs,
		endedAtMs: atMs,
		boundary: "silence_end",
	};
	return { state: idleState(state), utterance };
}
