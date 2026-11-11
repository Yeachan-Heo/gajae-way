import { describe, expect, test } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { PermissionPanels, type PermissionReverseRequest } from "../src/server/permission-panels";

// gjc's default permission options (coding-agent session/agent-session.ts PERMISSION_OPTIONS).
const GJC_OPTIONS = [
	{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
	{ optionId: "allow_always", name: "Always allow", kind: "allow_always" },
	{ optionId: "reject_once", name: "Reject", kind: "reject_once" },
	{ optionId: "reject_always", name: "Always reject", kind: "reject_always" },
];
const SLACK: OriginRef = { platform: "slack", kind: "channel", conversationId: "C1" };
const DISCORD: OriginRef = { platform: "discord", kind: "channel", conversationId: "D1" };

type Sent = Parameters<PermissionTail["sendReverseResponse"]>[0];
interface PermissionTail {
	sendReverseResponse(input: {
		id: string;
		connectionId: string;
		leaseId: string;
		result?: unknown;
		error?: { code: string; message: string };
	}): Promise<void>;
}

function harness(start = 1_000) {
	let now = start;
	const delivered: ChatMessagePayload[] = [];
	const sent: Sent[] = [];
	const tail: PermissionTail = {
		sendReverseResponse: async (input) => {
			sent.push(input);
		},
	};
	const panels = new PermissionPanels({ deliver: (payload) => delivered.push(payload), now: () => now, ttlMs: 60_000 });
	const request = (overrides: Partial<PermissionReverseRequest> = {}): PermissionReverseRequest => ({
		id: "rev-1",
		connectionId: "conn-1",
		leaseId: "lease-1",
		capability: "permission",
		payload: { method: "request", payload: { toolCall: { title: "Run rm -rf build" }, options: GJC_OPTIONS } },
		...overrides,
	});
	const open = async (overrides: Partial<Parameters<PermissionPanels["open"]>[0]> = {}) => {
		await panels.open({ origin: SLACK, triggerAuthorId: "U-author", tail, request: request(), ...overrides });
		return delivered.at(-1)?.approvalPanel?.panelId ?? "";
	};
	const digest = () => delivered.at(-1)?.approvalPanel?.digest ?? "";
	return {
		panels,
		delivered,
		sent,
		tail,
		request,
		open,
		digest,
		advance: (ms: number) => (now += ms),
	};
}

describe("permission panels", () => {
	test("a Slack permission request posts a panel with gjc's offered options and a digest", async () => {
		const h = harness();
		await h.open();
		expect(h.sent).toEqual([]);
		expect(h.delivered).toHaveLength(1);
		expect(h.delivered[0]).toMatchObject({
			origin: SLACK,
			role: "assistant",
			text: "Run rm -rf build",
			final: true,
			approvalPanel: {
				message: "Run rm -rf build",
				options: GJC_OPTIONS,
				digest: expect.stringMatching(/^[0-9a-f]{32}$/),
			},
		});
		expect(h.delivered[0]?.deliveryId).toEqual(expect.any(String));
	});

	test("a Discord permission request posts a panel too", async () => {
		const h = harness();
		await h.open({ origin: DISCORD });
		expect(h.sent).toEqual([]);
		expect(h.delivered).toHaveLength(1);
		expect(h.delivered[0]).toMatchObject({
			origin: DISCORD,
			text: "Run rm -rf build",
			approvalPanel: {
				message: "Run rm -rf build",
				options: GJC_OPTIONS,
				digest: expect.stringMatching(/^[0-9a-f]{32}$/),
			},
		});
	});

	test.each([
		["telegram origin", { origin: { platform: "telegram", kind: "channel", conversationId: "T1" } as OriginRef }],
		["loopback origin", { origin: { platform: "loopback", kind: "loopback", conversationId: "L1" } as OriginRef }],
		["unknown origin", { origin: undefined }],
		["no trigger author", { triggerAuthorId: undefined }],
	])("%s is declined without posting a panel", async (_name, overrides) => {
		const h = harness();
		await h.open(overrides);
		expect(h.delivered).toEqual([]);
		expect(h.sent).toEqual([
			{
				id: "rev-1",
				connectionId: "conn-1",
				leaseId: "lease-1",
				error: { code: "unavailable", message: expect.any(String) },
			},
		]);
		expect(h.panels.size).toBe(0);
	});

	test("non-permission requests and empty option lists are declined", async () => {
		const h = harness();
		await h.open({ request: h.request({ capability: "ui", payload: { method: "select", payload: {} } }) });
		await h.open({ request: h.request({ payload: { method: "request", payload: { options: [] } } }) });
		expect(h.delivered).toEqual([]);
		expect(h.sent.map((frame) => frame.error?.code)).toEqual(["unavailable", "unavailable"]);
	});

	test("exact option selection answers with exactly that offered option", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "option_selected",
				selectedOptionId: "allow_once",
				digest: h.digest(),
			}),
		).toBeUndefined();
		expect(h.sent).toEqual([
			{
				id: "rev-1",
				connectionId: "conn-1",
				leaseId: "lease-1",
				result: { outcome: "selected", optionId: "allow_once", kind: "allow_once" },
			},
		]);
	});

	test("approve answers allow_once, never allow_always", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: h.digest(),
			}),
		).toBeUndefined();
		expect(h.sent).toEqual([
			{
				id: "rev-1",
				connectionId: "conn-1",
				leaseId: "lease-1",
				result: { outcome: "selected", optionId: "allow_once", kind: "allow_once" },
			},
		]);
	});

	test("approval never widens to a different allow policy option", async () => {
		const h = harness();
		const panelId = await h.open({
			request: h.request({
				payload: {
					method: "request",
					payload: {
						toolCall: { title: "Run rm -rf build" },
						options: [{ optionId: "allow_always", name: "Always allow", kind: "allow_always" }],
					},
				},
			}),
		});
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: h.digest(),
			}),
		).toBe("option_not_offered");
		expect(h.sent).toEqual([]);
		expect(h.panels.size).toBe(1);
	});

	test("deny selects reject_once instead of cancelling the turn", async () => {
		const h = harness();
		const panelId = await h.open();
		await h.panels.respond({ panelId, responderId: "U-author", responseKind: "denied", digest: h.digest() });
		expect(h.sent[0]?.result).toEqual({ outcome: "selected", optionId: "reject_once", kind: "reject_once" });
	});

	test("a clicked option button answers with exactly that offered option", async () => {
		const h = harness();
		const panelId = await h.open();
		await h.panels.respond({
			panelId,
			responderId: "U-author",
			responseKind: "option_selected",
			selectedOptionId: "allow_always",
			digest: h.digest(),
		});
		expect(h.sent[0]?.result).toEqual({ outcome: "selected", optionId: "allow_always", kind: "allow_always" });
	});

	test("an option gjc did not offer is refused and the panel stays open", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "option_selected",
				selectedOptionId: "yolo",
				digest: h.digest(),
			}),
		).toBe("option_not_offered");
		expect(h.sent).toEqual([]);
		expect(h.panels.size).toBe(1);
	});

	test("only the trigger author can answer", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(
			await h.panels.respond({ panelId, responderId: "U-other", responseKind: "approved", digest: h.digest() }),
		).toBe("unauthorized_responder");
		expect(h.sent).toEqual([]);
		expect(h.panels.size).toBe(1);
		expect(
			await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved", digest: h.digest() }),
		).toBeUndefined();
		expect(h.sent).toHaveLength(1);
	});

	test("a missing, truncated, or foreign digest is refused and the panel stays open", async () => {
		const h = harness();
		const panelId = await h.open();
		const digest = h.digest();
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved" })).toBe(
			"digest_mismatch",
		);
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: digest.slice(0, 31),
			}),
		).toBe("digest_mismatch");
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: "0".repeat(32),
			}),
		).toBe("digest_mismatch");
		expect(h.sent).toEqual([]);
		expect(h.panels.size).toBe(1);
		expect(
			await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved", digest }),
		).toBeUndefined();
	});

	test("a digest from a different request is refused", async () => {
		const h = harness();
		const first = await h.open();
		const firstDigest = h.digest();
		const second = await h.open({ request: h.request({ id: "rev-2", leaseId: "lease-2" }) });
		const secondDigest = h.digest();
		expect(firstDigest).not.toBe(secondDigest);
		expect(
			await h.panels.respond({
				panelId: second,
				responderId: "U-author",
				responseKind: "approved",
				digest: firstDigest,
			}),
		).toBe("digest_mismatch");
		expect(
			await h.panels.respond({
				panelId: second,
				responderId: "U-author",
				responseKind: "approved",
				digest: secondDigest,
			}),
		).toBeUndefined();
		expect(h.sent).toEqual([
			{
				id: "rev-2",
				connectionId: "conn-1",
				leaseId: "lease-2",
				result: { outcome: "selected", optionId: "allow_once", kind: "allow_once" },
			},
		]);
		expect(first).not.toBe(second);
	});

	test("a response from a different origin is refused", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: h.digest(),
				originKey: "discord/channel/D1",
			}),
		).toBe("origin_mismatch");
		expect(h.sent).toEqual([]);
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: h.digest(),
				originKey: "slack/channel/C1",
			}),
		).toBeUndefined();
	});

	test("a panel is answered at most once", async () => {
		const h = harness();
		const panelId = await h.open();
		const digest = h.digest();
		await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved", digest });
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "denied", digest })).toBe(
			"panel_not_found",
		);
		expect(h.sent).toHaveLength(1);
	});

	test("an expired panel is answered as cancelled by the sweep and can no longer be clicked", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(await h.panels.sweep()).toBe(0);
		h.advance(60_000);
		expect(
			await h.panels.respond({
				panelId,
				responderId: "U-author",
				responseKind: "approved",
				digest: h.digest(),
			}),
		).toBe("panel_not_found");
		expect(await h.panels.sweep()).toBe(1);
		expect(h.sent).toEqual([
			{ id: "rev-1", connectionId: "conn-1", leaseId: "lease-1", result: { outcome: "cancelled" } },
		]);
		expect(h.panels.size).toBe(0);
	});
});
