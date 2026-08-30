import { describe, expect, test } from "bun:test";
import { createSttMachine, type SttMachineState, transitionSttMachine } from "../src/stt-machine";

const config = {
	speakerId: "speaker-1",
	sampleRateHz: 16_000,
	maxQueuedFrames: 200,
	transcriptWaitMs: 1_500,
	reconnectInitialBackoffMs: 100,
	reconnectMaxBackoffMs: 500,
	reconnectMaxAttempts: 3,
} as const;

function step(state: SttMachineState, input: Parameters<typeof transitionSttMachine>[1]) {
	return transitionSttMachine(state, input);
}

function listeningState(): SttMachineState {
	let state = createSttMachine(config);
	state = step(state, { kind: "open" }).state;
	state = step(state, { kind: "ready", sessionId: "session-1" }).state;
	return state;
}

function speakingState(): SttMachineState {
	return step(listeningState(), {
		kind: "energy_gate_passed",
		atMs: 100,
		energyRms: 0.1,
		energyThreshold: 0.02,
		energyHoldMs: 300,
	}).state;
}

describe("STT transition table", () => {
	test("opens from idle, then ready increments epoch and enters listening", () => {
		const opened = step(createSttMachine(config), { kind: "open" });
		expect(opened.state.status).toBe("opening");
		expect(opened.actions).toEqual([
			{
				kind: "open",
				options: { speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 200 },
				reconnect: false,
			},
		]);
		const ready = step(opened.state, { kind: "ready", sessionId: "session-1" });
		expect(ready.state.status).toBe("listening");
		expect(ready.state.epoch).toBe(1);
		expect(ready.actions).toEqual([{ kind: "epoch_increment", epoch: 1 }]);
	});

	test("energy gate starts speaking and partials are retained", () => {
		const started = step(listeningState(), {
			kind: "energy_gate_passed",
			atMs: 10,
			energyRms: 0.2,
			energyThreshold: 0.02,
			energyHoldMs: 300,
		});
		expect(started.state.status).toBe("speaking");
		expect(started.actions[0]).toEqual({
			kind: "utterance_started",
			atMs: 10,
			energyRms: 0.2,
			energyThreshold: 0.02,
			energyHoldMs: 300,
		});
		const partial = step(started.state, { kind: "partial", text: "hel", atMs: 20 });
		expect(partial.state.lastPartial).toBe("hel");
		expect(partial.actions).toEqual([{ kind: "partial", text: "hel", atMs: 20 }]);
	});

	test("silence boundary arms the transcript wait", () => {
		const boundary = step(speakingState(), { kind: "silence_boundary", atMs: 800 });
		expect(boundary.state.status).toBe("awaiting_text");
		expect(boundary.state.boundaryAtMs).toBe(800);
		expect(boundary.actions).toEqual([{ kind: "arm_transcript_wait", waitMs: 1_500 }]);
	});

	test("awaiting_text committed finalizes with committed source", () => {
		const state = step(speakingState(), { kind: "silence_boundary", atMs: 800 }).state;
		const finalized = step(state, { kind: "committed", text: "hello", atMs: 900, languageCode: "en" });
		expect(finalized.state.status).toBe("listening");
		expect(finalized.actions).toEqual([
			{
				kind: "finalize",
				text: "hello",
				textSource: "committed",
				boundary: "silence_end",
				boundaryAtMs: 800,
				atMs: 900,
				languageCode: "en",
			},
		]);
	});

	test("timer expiry finalizes the last partial with fallback source", () => {
		let state = step(speakingState(), { kind: "partial", text: "hello wor", atMs: 500 }).state;
		state = step(state, { kind: "silence_boundary", atMs: 800 }).state;
		const finalized = step(state, { kind: "transcript_wait_expired", atMs: 2_300 });
		expect(finalized.state.status).toBe("listening");
		expect(finalized.actions).toEqual([
			{
				kind: "finalize",
				text: "hello wor",
				textSource: "partial_fallback",
				boundary: "silence_end",
				boundaryAtMs: 800,
				atMs: 2_300,
			},
		]);
	});

	test("timer expiry without a partial drops noise", () => {
		const state = step(speakingState(), { kind: "silence_boundary", atMs: 800 }).state;
		const finalized = step(state, { kind: "transcript_wait_expired", atMs: 2_300 });
		expect(finalized.actions).toEqual([
			{
				kind: "finalize",
				text: "",
				textSource: "partial_fallback",
				boundary: "dropped_noise",
				boundaryAtMs: 800,
				atMs: 2_300,
			},
		]);
	});

	test("retryable closes before backoff and reconnects with previous text once", () => {
		let state = listeningState();
		state = step(state, { kind: "committed", text: "last answer", atMs: 50 }).state;
		const retry = step(state, { kind: "retryable", code: "network", message: "lost" });
		expect(retry.state.status).toBe("reconnecting");
		expect(retry.actions).toEqual([
			{ kind: "close", reason: "retryable", beforeOpen: true, code: "network" },
			{ kind: "backoff", attempt: 1, delayMs: 100 },
		]);
		const reopened = step(retry.state, { kind: "open" });
		expect(reopened.actions).toEqual([
			{
				kind: "open",
				options: {
					speakerId: "speaker-1",
					sampleRateHz: 16_000,
					maxQueuedFrames: 200,
					previousText: "last answer",
				},
				reconnect: true,
			},
		]);
		expect(step(reopened.state, { kind: "open" }).actions).toEqual([]);
		const ready = step(reopened.state, { kind: "ready", sessionId: "session-2" });
		expect(ready.state.epoch).toBe(2);
	});

	test("retryable backoff is exponential and capped", () => {
		let state = listeningState();
		const first = step(state, { kind: "retryable", code: "network", message: "lost" });
		expect(first.actions[1]).toEqual({ kind: "backoff", attempt: 1, delayMs: 100 });
		state = first.state;
		const second = step(state, { kind: "retryable", code: "network", message: "still lost" });
		expect(second.actions[1]).toEqual({ kind: "backoff", attempt: 2, delayMs: 200 });
		state = second.state;
		const third = step(state, { kind: "retryable", code: "network", message: "still lost" });
		expect(third.actions[1]).toEqual({ kind: "backoff", attempt: 3, delayMs: 400 });
	});

	test("fatal closes the provider session", () => {
		const result = step(speakingState(), { kind: "fatal", code: "auth", message: "bad key" });
		expect(result.state.status).toBe("closed");
		expect(result.actions).toEqual([{ kind: "close", reason: "provider_fatal", session: true, code: "auth" }]);
	});

	test("teardown commits before closing and is idempotent", () => {
		const result = step(listeningState(), { kind: "teardown" });
		expect(result.state.status).toBe("closed");
		expect(result.actions).toEqual([
			{ kind: "commit", reason: "teardown" },
			{ kind: "close", reason: "teardown", session: true },
		]);
		expect(step(result.state, { kind: "teardown" }).actions).toEqual([]);
	});

	test("promotes retry exhaustion to a fatal session close", () => {
		let state = listeningState();
		state = step(state, { kind: "retryable", code: "network", message: "one" }).state;
		state = step(state, { kind: "retryable", code: "network", message: "two" }).state;
		state = step(state, { kind: "retryable", code: "network", message: "three" }).state;
		const exhausted = step(state, { kind: "retryable", code: "network", message: "four" });
		expect(exhausted.state.status).toBe("closed");
		expect(exhausted.actions).toEqual([{ kind: "close", reason: "provider_fatal", session: true, code: "network" }]);
	});

	test("warnings are exposed without changing lifecycle state", () => {
		const result = step(listeningState(), { kind: "warning", code: "slow", message: "provider lag" });
		expect(result.state.status).toBe("listening");
		expect(result.actions).toEqual([{ kind: "warning", code: "slow", message: "provider lag" }]);
	});
});
