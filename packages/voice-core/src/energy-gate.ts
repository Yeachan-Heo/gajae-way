/**
 * Pure speech-energy gating for decoded PCM16 frames.
 *
 * The adapter supplies the threshold and hold duration from configuration. Keeping
 * those values on the state makes every decision reproducible in a drill log.
 */

export interface EnergyGateConfig {
	readonly rmsThreshold: number;
	readonly minDurationMs: number;
}

export interface EnergyGateState {
	readonly energyThreshold: number;
	readonly minDurationMs: number;
	readonly energyRms: number;
	readonly energyHoldMs: number;
	readonly energyGatePassed: boolean;
	readonly aboveThreshold: boolean;
	readonly aboveSinceMs: number | null;
	readonly lastAtMs: number | null;
}

/** The result is state itself so callers can persist it without hidden mutable state. */

export type Pcm16Frame = Int16Array | Uint8Array;

function assertFiniteNonNegative(value: number, name: string): void {
	if (!Number.isFinite(value) || value < 0) {
		throw new RangeError(`${name} must be a finite non-negative number`);
	}
}

function assertAtMs(atMs: number, previousAtMs: number | null): void {
	if (!Number.isFinite(atMs) || atMs < 0) {
		throw new RangeError("atMs must be a finite non-negative number");
	}
	if (previousAtMs !== null && atMs < previousAtMs) {
		throw new RangeError("atMs must not move backwards");
	}
}

function frameSamples(frame: Pcm16Frame): readonly number[] | Int16Array {
	if (frame instanceof Int16Array) return frame;
	if (frame.byteLength % 2 !== 0) {
		throw new RangeError("PCM16LE frames must contain an even number of bytes");
	}
	const samples = new Int16Array(frame.byteLength / 2);
	const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
	for (let index = 0; index < samples.length; index += 1) {
		samples[index] = view.getInt16(index * 2, true);
	}
	return samples;
}

/**
 * Computes normalized RMS (`1` is the magnitude of signed PCM16's -32768 value).
 * Empty frames are silence and therefore have an RMS of zero.
 */
export function computePcm16Rms(frame: Pcm16Frame): number {
	const samples = frameSamples(frame);
	if (samples.length === 0) return 0;
	let sumSquares = 0;
	for (const sample of samples) {
		const normalized = sample / 32_768;
		sumSquares += normalized * normalized;
	}
	return Math.sqrt(sumSquares / samples.length);
}

/** Creates an idle gate; the first above-threshold sample starts a new hold. */
export function createEnergyGate(config: EnergyGateConfig): EnergyGateState {
	assertFiniteNonNegative(config.rmsThreshold, "rmsThreshold");
	assertFiniteNonNegative(config.minDurationMs, "minDurationMs");
	if (config.rmsThreshold > 1) {
		throw new RangeError("rmsThreshold must not exceed one");
	}
	return {
		energyThreshold: config.rmsThreshold,
		minDurationMs: config.minDurationMs,
		energyRms: 0,
		energyHoldMs: 0,
		energyGatePassed: false,
		aboveThreshold: false,
		aboveSinceMs: null,
		lastAtMs: null,
	};
}

/**
 * Advances the gate with an already-computed RMS measurement.
 *
 * A below-threshold measurement is a gap and resets the sustained hold. While
 * above threshold, elapsed time is measured between injected timestamps, so an
 * exact `minDurationMs` reaches the pass condition.
 */
export function updateEnergyGate(state: EnergyGateState, energyRms: number, atMs: number): EnergyGateState {
	assertFiniteNonNegative(energyRms, "energyRms");
	assertAtMs(atMs, state.lastAtMs);

	if (energyRms < state.energyThreshold) {
		return {
			...state,
			energyRms,
			energyHoldMs: 0,
			energyGatePassed: false,
			aboveThreshold: false,
			aboveSinceMs: null,
			lastAtMs: atMs,
		};
	}

	const energyHoldMs =
		state.aboveThreshold && state.lastAtMs !== null ? state.energyHoldMs + (atMs - state.lastAtMs) : 0;
	const aboveSinceMs = state.aboveThreshold && state.aboveSinceMs !== null ? state.aboveSinceMs : atMs;
	return {
		...state,
		energyRms,
		energyHoldMs,
		energyGatePassed: energyHoldMs >= state.minDurationMs,
		aboveThreshold: true,
		aboveSinceMs,
		lastAtMs: atMs,
	};
}

/** Advances the gate by decoding a PCM16 little-endian frame first. */
export function updateEnergyGateFromFrame(state: EnergyGateState, frame: Pcm16Frame, atMs: number): EnergyGateState {
	return updateEnergyGate(state, computePcm16Rms(frame), atMs);
}
