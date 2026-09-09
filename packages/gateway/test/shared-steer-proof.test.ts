import { expect, test } from "bun:test";
import type { WorkerOutputResult } from "../src/orchestrator/session-port";
import { provesSharedSteer } from "./work-lane-live.e2e.test";

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
