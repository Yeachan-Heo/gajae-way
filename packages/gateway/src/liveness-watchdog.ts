import { renderExitReport } from "./exit-report";

/** A main thread that serviced no timer for this long is wedged, not busy (#615). */
export const LIVENESS_STALL_MS = 120_000;
const BEAT_MS = 1_000;

export interface LivenessWatchdogOptions {
	readonly stallMs?: number;
	readonly beatMs?: number;
}

interface WatchdogState {
	seenBeat: number;
	checkedAt: number;
	stalledMs: number;
}

/** Advances the worker's awake-time budget using a supplied clock sample. */
export function advanceWatchdogState(state: WatchdogState, currentBeat: number, now: number, checkMs: number): void {
	state.stalledMs += Math.min(Math.max(0, now - state.checkedAt), 2 * checkMs);
	state.checkedAt = now;
	if (currentBeat !== state.seenBeat) {
		state.seenBeat = currentBeat;
		state.stalledMs = 0;
	}
}

// Inline source keeps the watchdog working in a `bun build --compile` binary,
// which embeds no separate worker entrypoint. Serialize the state transition so
// the worker and deterministic clock test exercise the same implementation.
const WORKER_SOURCE = `
const { writeSync } = require("node:fs");
const advanceWatchdogState = (${advanceWatchdogState.toString()});
self.onmessage = ({ data }) => {
	const beat = new Int32Array(data.buffer);
	const state = { seenBeat: Atomics.load(beat, 0), checkedAt: Date.now(), stalledMs: 0 };
	setInterval(() => {
		const now = Date.now();
		advanceWatchdogState(state, Atomics.load(beat, 0), now, data.checkMs);
		if (state.stalledMs < data.stallMs) return;
		try {
			writeSync(2, data.line);
		} catch {}
		process.kill(data.pid, "SIGKILL");
	}, data.checkMs);
};
`;

/**
 * Off-thread liveness watchdog. The main thread bumps a shared counter on a
 * timer; a worker thread SIGKILLs the process once the counter stops moving
 * for `stallMs`. A wedged main thread (a synchronous child wait, a stuck
 * native call) runs no JavaScript at all, so no in-loop timer can notice it,
 * and a process that never exits is never restarted by `Restart=always`.
 * SIGKILL, because the SIGTERM handler is JavaScript on the wedged thread.
 */
export function installLivenessWatchdog(options: LivenessWatchdogOptions = {}): () => void {
	const stallMs = options.stallMs ?? LIVENESS_STALL_MS;
	const beatMs = options.beatMs ?? BEAT_MS;
	const buffer = new SharedArrayBuffer(4);
	const beat = new Int32Array(buffer);
	const worker = new Worker(URL.createObjectURL(new Blob([WORKER_SOURCE], { type: "application/javascript" })));
	worker.unref();
	const timer = setInterval(() => Atomics.add(beat, 0, 1), beatMs);
	timer.unref();
	worker.postMessage({
		buffer,
		pid: process.pid,
		stallMs,
		checkMs: Math.max(1, Math.min(beatMs, Math.floor(stallMs / 4))),
		line: renderExitReport(
			"event_loop_stalled",
			`main thread serviced no timer for ${stallMs}ms; SIGKILL for supervisor restart`,
		),
	});
	let disposed = false;
	return () => {
		if (disposed) return;
		disposed = true;
		clearInterval(timer);
		worker.terminate();
	};
}
