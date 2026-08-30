/**
 * Provider-agnostic STT lifecycle state machine.
 *
 * This module only returns state and effects. Opening sockets, applying backoff,
 * committing streams, and closing sessions remain responsibilities of the adapter.
 */

import type { SttCommitReason, SttEvent, SttOpenOptions } from "./ports";

export type SttMachineStatus =
	| "idle"
	| "opening"
	| "listening"
	| "speaking"
	| "awaiting_text"
	| "reconnecting"
	| "closed";

export interface SttMachineConfig {
	readonly speakerId: string;
	readonly sampleRateHz: number;
	readonly maxQueuedFrames: number;
	readonly transcriptWaitMs: number;
	readonly reconnectInitialBackoffMs: number;
	readonly reconnectMaxBackoffMs: number;
	readonly reconnectMaxAttempts: number;
}

export interface SttMachineState {
	readonly status: SttMachineStatus;
	readonly epoch: number;
	readonly reconnectAttempts: number;
	readonly lastCommittedText: string | null;
	readonly lastPartial: string | null;
	readonly lastPartialAtMs: number | null;
	readonly boundaryAtMs: number | null;
	readonly pendingPreviousText: string | null;
	readonly config: SttMachineConfig;
}

export type SttMachineInput =
	| SttEvent
	| { readonly kind: "open" }
	| {
			readonly kind: "energy_gate_passed";
			readonly atMs: number;
			readonly energyRms: number;
			readonly energyThreshold: number;
			readonly energyHoldMs: number;
	  }
	| { readonly kind: "silence_boundary"; readonly atMs: number }
	| { readonly kind: "transcript_wait_expired"; readonly atMs: number }
	| { readonly kind: "teardown" };

export type SttMachineAction =
	| { readonly kind: "open"; readonly options: SttOpenOptions; readonly reconnect: boolean }
	| {
			readonly kind: "close";
			readonly reason: string;
			readonly beforeOpen?: boolean;
			readonly session?: boolean;
			readonly code?: string;
	  }
	| { readonly kind: "backoff"; readonly attempt: number; readonly delayMs: number }
	| { readonly kind: "commit"; readonly reason: SttCommitReason }
	| { readonly kind: "epoch_increment"; readonly epoch: number }
	| {
			readonly kind: "utterance_started";
			readonly atMs: number;
			readonly energyRms: number;
			readonly energyThreshold: number;
			readonly energyHoldMs: number;
	  }
	| { readonly kind: "partial"; readonly text: string; readonly atMs: number }
	| { readonly kind: "arm_transcript_wait"; readonly waitMs: number }
	| {
			readonly kind: "finalize";
			readonly text: string;
			readonly textSource: "committed" | "partial_fallback";
			readonly boundary: "silence_end" | "dropped_noise";
			readonly boundaryAtMs: number;
			readonly atMs: number;
			readonly languageCode?: string;
	  }
	| { readonly kind: "warning"; readonly code: string; readonly message: string };

export interface SttMachineTransition {
	readonly state: SttMachineState;
	readonly actions: readonly SttMachineAction[];
}

function assertNonBlank(value: string, name: string): void {
	if (value.trim() === "") throw new RangeError(`${name} must not be blank`);
}

function assertPositive(value: number, name: string): void {
	if (!Number.isFinite(value) || value <= 0) throw new RangeError(`${name} must be positive`);
}

function assertNonNegative(value: number, name: string): void {
	if (!Number.isFinite(value) || value < 0) throw new RangeError(`${name} must be non-negative`);
}

function assertTimestamp(value: number, name: string): void {
	assertNonNegative(value, name);
}

function validateConfig(config: SttMachineConfig): void {
	assertNonBlank(config.speakerId, "speakerId");
	assertPositive(config.sampleRateHz, "sampleRateHz");
	if (!Number.isInteger(config.maxQueuedFrames) || config.maxQueuedFrames < 0) {
		throw new RangeError("maxQueuedFrames must be a non-negative integer");
	}
	assertNonNegative(config.transcriptWaitMs, "transcriptWaitMs");
	assertNonNegative(config.reconnectInitialBackoffMs, "reconnectInitialBackoffMs");
	assertNonNegative(config.reconnectMaxBackoffMs, "reconnectMaxBackoffMs");
	if (config.reconnectMaxBackoffMs < config.reconnectInitialBackoffMs) {
		throw new RangeError("reconnectMaxBackoffMs must not be less than reconnectInitialBackoffMs");
	}
	if (!Number.isInteger(config.reconnectMaxAttempts) || config.reconnectMaxAttempts < 1) {
		throw new RangeError("reconnectMaxAttempts must be a positive integer");
	}
}

/** Creates an idle machine with no provider effects queued. */
export function createSttMachine(config: SttMachineConfig): SttMachineState {
	validateConfig(config);
	return {
		status: "idle",
		epoch: 0,
		reconnectAttempts: 0,
		lastCommittedText: null,
		lastPartial: null,
		lastPartialAtMs: null,
		boundaryAtMs: null,
		pendingPreviousText: null,
		config,
	};
}

function baseOpenOptions(config: SttMachineConfig): SttOpenOptions {
	return {
		speakerId: config.speakerId,
		sampleRateHz: config.sampleRateHz,
		maxQueuedFrames: config.maxQueuedFrames,
	};
}

function transition(state: SttMachineState, changes: Partial<Omit<SttMachineState, "config">>): SttMachineState {
	return { ...state, ...changes };
}

function noEffect(state: SttMachineState): SttMachineTransition {
	return { state, actions: [] };
}

function retryableTransition(
	state: SttMachineState,
	event: Extract<SttEvent, { readonly kind: "retryable" }>,
): SttMachineTransition {
	if (state.status === "closed") return noEffect(state);
	if (state.reconnectAttempts >= state.config.reconnectMaxAttempts) {
		return {
			state: transition(state, {
				status: "closed",
				boundaryAtMs: null,
				lastPartial: null,
				lastPartialAtMs: null,
			}),
			actions: [
				{
					kind: "close",
					reason: "provider_fatal",
					session: true,
					code: event.code,
				},
			],
		};
	}

	const attempt = state.reconnectAttempts + 1;
	const delayMs = Math.min(
		state.config.reconnectMaxBackoffMs,
		state.config.reconnectInitialBackoffMs * 2 ** (attempt - 1),
	);
	return {
		state: transition(state, {
			status: "reconnecting",
			reconnectAttempts: attempt,
			pendingPreviousText: state.lastCommittedText,
			boundaryAtMs: null,
			lastPartial: null,
			lastPartialAtMs: null,
		}),
		actions: [
			{
				kind: "close",
				reason: "retryable",
				beforeOpen: true,
				code: event.code,
			},
			{ kind: "backoff", attempt, delayMs },
		],
	};
}

function fatalTransition(
	state: SttMachineState,
	event: Extract<SttEvent, { readonly kind: "fatal" }>,
): SttMachineTransition {
	if (state.status === "closed") return noEffect(state);
	return {
		state: transition(state, {
			status: "closed",
			boundaryAtMs: null,
			lastPartial: null,
			lastPartialAtMs: null,
		}),
		actions: [
			{
				kind: "close",
				reason: "provider_fatal",
				session: true,
				code: event.code,
			},
		],
	};
}

function teardownTransition(state: SttMachineState): SttMachineTransition {
	if (state.status === "closed") return noEffect(state);
	return {
		state: transition(state, {
			status: "closed",
			boundaryAtMs: null,
			lastPartial: null,
			lastPartialAtMs: null,
		}),
		actions: [
			{ kind: "commit", reason: "teardown" },
			{ kind: "close", reason: "teardown", session: true },
		],
	};
}

function openTransition(state: SttMachineState): SttMachineTransition {
	if (state.status !== "idle" && state.status !== "reconnecting") return noEffect(state);
	const reconnect = state.status === "reconnecting";
	const options = baseOpenOptions(state.config);
	const optionsWithPrevious =
		reconnect && state.pendingPreviousText !== null ? { ...options, previousText: state.pendingPreviousText } : options;
	return {
		state: transition(state, { status: "opening", pendingPreviousText: null }),
		actions: [{ kind: "open", options: optionsWithPrevious, reconnect }],
	};
}

function readyTransition(state: SttMachineState): SttMachineTransition {
	if (state.status !== "opening") return noEffect(state);
	const epoch = state.epoch + 1;
	return {
		state: transition(state, { status: "listening", epoch, reconnectAttempts: 0 }),
		actions: [{ kind: "epoch_increment", epoch }],
	};
}

function partialTransition(
	state: SttMachineState,
	event: Extract<SttEvent, { readonly kind: "partial" }>,
): SttMachineTransition {
	if (state.status !== "speaking" && state.status !== "awaiting_text") return noEffect(state);
	return {
		state: transition(state, { lastPartial: event.text, lastPartialAtMs: event.atMs }),
		actions: [{ kind: "partial", text: event.text, atMs: event.atMs }],
	};
}

function committedTransition(
	state: SttMachineState,
	event: Extract<SttEvent, { readonly kind: "committed" }>,
): SttMachineTransition {
	if (state.status === "closed") return noEffect(state);
	if (state.status !== "awaiting_text") {
		return {
			state: transition(state, { lastCommittedText: event.text }),
			actions: [],
		};
	}

	const boundaryAtMs = state.boundaryAtMs ?? event.atMs;
	const action: SttMachineAction = {
		kind: "finalize",
		text: event.text,
		textSource: "committed",
		boundary: "silence_end",
		boundaryAtMs,
		atMs: event.atMs,
		...(event.languageCode === undefined ? {} : { languageCode: event.languageCode }),
	};
	return {
		state: transition(state, {
			status: "listening",
			lastCommittedText: event.text,
			lastPartial: null,
			lastPartialAtMs: null,
			boundaryAtMs: null,
		}),
		actions: [action],
	};
}

function energyGateTransition(
	state: SttMachineState,
	input: Extract<SttMachineInput, { readonly kind: "energy_gate_passed" }>,
): SttMachineTransition {
	if (state.status !== "listening") return noEffect(state);
	assertTimestamp(input.atMs, "atMs");
	assertNonNegative(input.energyRms, "energyRms");
	assertNonNegative(input.energyThreshold, "energyThreshold");
	assertNonNegative(input.energyHoldMs, "energyHoldMs");
	return {
		state: transition(state, {
			status: "speaking",
			lastPartial: null,
			lastPartialAtMs: null,
			boundaryAtMs: null,
		}),
		actions: [
			{
				kind: "utterance_started",
				atMs: input.atMs,
				energyRms: input.energyRms,
				energyThreshold: input.energyThreshold,
				energyHoldMs: input.energyHoldMs,
			},
		],
	};
}

function silenceBoundaryTransition(
	state: SttMachineState,
	input: Extract<SttMachineInput, { readonly kind: "silence_boundary" }>,
): SttMachineTransition {
	if (state.status !== "speaking") return noEffect(state);
	assertTimestamp(input.atMs, "atMs");
	return {
		state: transition(state, { status: "awaiting_text", boundaryAtMs: input.atMs }),
		actions: [{ kind: "arm_transcript_wait", waitMs: state.config.transcriptWaitMs }],
	};
}

function transcriptWaitTransition(
	state: SttMachineState,
	input: Extract<SttMachineInput, { readonly kind: "transcript_wait_expired" }>,
): SttMachineTransition {
	if (state.status !== "awaiting_text") return noEffect(state);
	assertTimestamp(input.atMs, "atMs");
	const hasPartial = state.lastPartial !== null;
	const action: SttMachineAction = {
		kind: "finalize",
		text: state.lastPartial ?? "",
		textSource: "partial_fallback",
		boundary: hasPartial ? "silence_end" : "dropped_noise",
		boundaryAtMs: state.boundaryAtMs ?? input.atMs,
		atMs: input.atMs,
	};
	return {
		state: transition(state, {
			status: "listening",
			lastPartial: null,
			lastPartialAtMs: null,
			boundaryAtMs: null,
		}),
		actions: [action],
	};
}

/** Applies one provider event or local lifecycle signal. */
export function transitionSttMachine(state: SttMachineState, input: SttMachineInput): SttMachineTransition {
	if (input.kind === "teardown") return teardownTransition(state);
	if (input.kind === "retryable") return retryableTransition(state, input);
	if (input.kind === "fatal") return fatalTransition(state, input);
	if (input.kind === "open") return openTransition(state);
	if (input.kind === "ready") return readyTransition(state);
	if (input.kind === "energy_gate_passed") return energyGateTransition(state, input);
	if (input.kind === "silence_boundary") return silenceBoundaryTransition(state, input);
	if (input.kind === "transcript_wait_expired") return transcriptWaitTransition(state, input);
	if (input.kind === "partial") return partialTransition(state, input);
	if (input.kind === "committed") return committedTransition(state, input);
	if (input.kind === "warning") {
		if (state.status === "closed") return noEffect(state);
		return { state, actions: [{ kind: "warning", code: input.code, message: input.message }] };
	}
	return noEffect(state);
}
