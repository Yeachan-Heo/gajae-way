import { describe, expect, test } from "bun:test";
import { computePcm16Rms, createEnergyGate, updateEnergyGate, updateEnergyGateFromFrame } from "../src/energy-gate";

describe("computePcm16Rms", () => {
	test("computes normalized RMS for signed samples and little-endian bytes", () => {
		expect(computePcm16Rms(new Int16Array([16_384, -16_384]))).toBeCloseTo(0.5, 12);
		expect(computePcm16Rms(new Uint8Array([0, 64]))).toBeCloseTo(0.5, 12);
		expect(computePcm16Rms(new Uint8Array())).toBe(0);
	});
});

describe("energy gate", () => {
	const config = { rmsThreshold: 0.02, minDurationMs: 300 } as const;

	test("does not pass at 299ms and passes at exactly 300ms", () => {
		let state = createEnergyGate(config);
		state = updateEnergyGate(state, 0.1, 0);
		state = updateEnergyGate(state, 0.1, 299);
		expect(state.energyGatePassed).toBe(false);
		expect(state.energyHoldMs).toBe(299);
		state = updateEnergyGate(state, 0.1, 300);
		expect(state.energyGatePassed).toBe(true);
		expect(state.energyHoldMs).toBe(300);
	});

	test("passes at 301ms and resets after a sub-threshold gap", () => {
		let state = createEnergyGate(config);
		state = updateEnergyGate(state, 0.1, 0);
		state = updateEnergyGate(state, 0.1, 301);
		expect(state.energyGatePassed).toBe(true);
		state = updateEnergyGate(state, 0, 302);
		expect(state.energyGatePassed).toBe(false);
		expect(state.energyHoldMs).toBe(0);
		expect(state.energyRms).toBe(0);
	});

	test("a long sub-threshold signal never passes", () => {
		let state = createEnergyGate(config);
		for (const atMs of [0, 100, 200, 299, 300, 500, 1_000]) {
			state = updateEnergyGate(state, 0.019, atMs);
		}
		expect(state.energyGatePassed).toBe(false);
		expect(state.energyHoldMs).toBe(0);
	});

	test("records frame RMS and applied metrics", () => {
		const state = updateEnergyGateFromFrame(createEnergyGate(config), new Int16Array([16_384, 16_384]), 0);
		expect(state.energyRms).toBeCloseTo(0.5, 12);
		expect(state.energyThreshold).toBe(config.rmsThreshold);
		expect(state.energyHoldMs).toBe(0);
	});
});
