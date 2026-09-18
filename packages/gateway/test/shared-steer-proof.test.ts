import { expect, test } from "bun:test";
import { GjcCliError } from "@gajae-gateway/subsession";
import { GjcCliUnavailableError } from "../src/orchestrator/broker";
import type { WorkerOutputResult } from "../src/orchestrator/session-port";
import { observeStatus, provesSharedSteer } from "./work-lane-live.e2e.test";

const original: Extract<WorkerOutputResult, { status: "proven" }> = {
	status: "proven",
	text: "original answer",
	observedAtMs: 200,
	provenance: {
		source: "turn.result",
		fullness: "original",
		sessionId: "owned-session",
		repo: "/workspace",
		opRef: "original-op",
		clientRef: "original-op",
		terminalAt: 100,
		contentVersion: 1,
		byteLength: 15,
	},
};
const proof = {
	sessionId: "owned-session",
	repo: "/workspace",
	opRef: "original-op",
	marker: "UNIQUE_STEER_MARKER",
	clientRef: "steer-ref",
	receipt: { clientRef: "steer-ref", status: "accepted" },
	terminal: { operationRef: "original-op", status: { status: "terminal_ok" } },
};

test("shared steer proof rejects latest-row marker when original operation lacks it", () => {
	const later = { ...original, text: proof.marker, provenance: { ...original.provenance, opRef: "later-op" } };
	expect(later.text.includes(proof.marker)).toBe(true);
	expect(provesSharedSteer({ ...proof, output: original })).toBe(false);
	expect(provesSharedSteer({ ...proof, output: later })).toBe(false);
});

test("shared steer proof accepts only correlated original proven output", () => {
	const output = { ...original, text: proof.marker };
	expect(provesSharedSteer({ ...proof, output })).toBe(true);
	expect(provesSharedSteer({ ...proof, output, receipt: { accepted: true, clientRef: "steer-ref" } })).toBe(true);
	for (const receipt of [
		{ accepted: false, status: "accepted", clientRef: "steer-ref" },
		{ accepted: true, status: "rejected", clientRef: "steer-ref" },
		{ status: "accepted", ok: false, clientRef: "steer-ref" },
		{ accepted: "true", status: "accepted", clientRef: "steer-ref" },
		{ clientRef: "steer-ref" },
		{ status: "accepted", clientRef: "other-steer" },
		{ status: "accepted" },
	]) {
		expect(provesSharedSteer({ ...proof, output, receipt })).toBe(false);
	}
	expect(provesSharedSteer({ ...proof, output, receipt: { accepted: true, clientRef: "other-steer" } })).toBe(false);
	expect(
		provesSharedSteer({ ...proof, output, terminal: { operationRef: "later-op", status: { status: "terminal_ok" } } }),
	).toBe(false);
	expect(
		provesSharedSteer({
			...proof,
			output: { ...output, provenance: { ...output.provenance, sessionId: "other-session" } },
		}),
	).toBe(false);
	expect(provesSharedSteer({ ...proof, output: { status: "absent", code: "output_pending" } })).toBe(false);
});

test("status observation retries only typed router transients then returns the actual terminal", async () => {
	const retries: unknown[] = [];
	let calls = 0;
	const terminal = proof.terminal;
	const errors = [
		new GjcCliError("unavailable", 0, "", { code: "session_unavailable" }),
		new GjcCliError("unavailable", 0, "", { code: "session_unavailable" }),
		new GjcCliError("stale", 0, "", { code: "endpoint_stale" }),
		new GjcCliUnavailableError("request timed out after 10ms"),
	];
	const result = await observeStatus({
		query: async () => {
			const error = errors[calls++];
			if (error) throw error;
			return terminal;
		},
		deadline: Date.now() + 5_000,
		method: "synthetic.status",
		onRetry: (retry) => retries.push(retry),
	});
	expect(result).toBe(terminal);
	expect(calls).toBe(5);
	expect(retries).toEqual(
		errors.map((_, index) => ({
			method: "synthetic.status",
			retryCount: index + 1,
			lastCode: ["session_unavailable", "session_unavailable", "endpoint_stale", "transport_timeout"][index],
		})),
	);
});

test("permanently unavailable and blocked status reads hard fail at the observation deadline", async () => {
	for (const query of [
		async () => {
			throw new GjcCliError("unavailable", 0, "", { code: "session_unavailable" });
		},
		() => new Promise<never>(() => {}),
	]) {
		await expect(
			observeStatus({ query, deadline: Date.now() + 20, method: "synthetic.status", onRetry: () => {} }),
		).rejects.toThrow("status observation deadline: synthetic.status");
	}
});

test("observation never masks arbitrary, authority, identity or terminal failures", async () => {
	for (const error of [
		new Error("session_unavailable"),
		{ code: "session_unavailable" },
		new Error("invalid status identity"),
		new GjcCliUnavailableError("SDK unavailable"),
		new GjcCliError("failed", 0, "", { code: "provider_failed" }),
	]) {
		let calls = 0;
		await expect(
			observeStatus({
				query: async () => {
					calls++;
					throw error;
				},
				deadline: Date.now() + 1_000,
				method: "synthetic.status",
				onRetry: () => {
					throw new Error("unexpected retry");
				},
			}),
		).rejects.toBe(error);
		expect(calls).toBe(1);
	}
	for (const terminal of [
		{ operationRef: "original-op", status: { status: "failed" } },
		{ operationRef: "wrong-op", status: { status: "terminal_ok" } },
	]) {
		const observed = await observeStatus({
			query: async () => terminal,
			deadline: Date.now() + 1_000,
			method: "synthetic.status",
			onRetry: () => {
				throw new Error("unexpected retry");
			},
		});
		expect(observed).toBe(terminal);
		expect(provesSharedSteer({ ...proof, terminal: observed, output: { ...original, text: proof.marker } })).toBe(
			false,
		);
	}
});
