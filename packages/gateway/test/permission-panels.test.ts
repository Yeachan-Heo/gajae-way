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
	return { panels, delivered, sent, tail, request, open, advance: (ms: number) => (now += ms) };
}

describe("permission panels", () => {
	test("a Slack permission request posts a panel with gjc's offered options", async () => {
		const h = harness();
		await h.open();
		expect(h.sent).toEqual([]);
		expect(h.delivered).toHaveLength(1);
		expect(h.delivered[0]).toMatchObject({
			origin: SLACK,
			role: "assistant",
			text: "Run rm -rf build",
			final: true,
			approvalPanel: { message: "Run rm -rf build", options: GJC_OPTIONS },
		});
		expect(h.delivered[0]?.deliveryId).toEqual(expect.any(String));
	});

	test.each([
		["non-Slack origin", { origin: { platform: "discord", kind: "channel", conversationId: "D1" } as OriginRef }],
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

	test("approve answers allow_once, never allow_always", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved" })).toBeUndefined();
		expect(h.sent).toEqual([
			{
				id: "rev-1",
				connectionId: "conn-1",
				leaseId: "lease-1",
				result: { outcome: "selected", optionId: "allow_once", kind: "allow_once" },
			},
		]);
	});

	test("deny selects reject_once instead of cancelling the turn", async () => {
		const h = harness();
		const panelId = await h.open();
		await h.panels.respond({ panelId, responderId: "U-author", responseKind: "denied" });
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
			}),
		).toBe("option_not_offered");
		expect(h.sent).toEqual([]);
		expect(h.panels.size).toBe(1);
	});

	test("only the trigger author can answer", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(await h.panels.respond({ panelId, responderId: "U-other", responseKind: "approved" })).toBe(
			"unauthorized_responder",
		);
		expect(h.sent).toEqual([]);
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved" })).toBeUndefined();
		expect(h.sent).toHaveLength(1);
	});

	test("a panel is answered at most once", async () => {
		const h = harness();
		const panelId = await h.open();
		await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved" });
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "denied" })).toBe(
			"panel_not_found",
		);
		expect(h.sent).toHaveLength(1);
	});

	test("an expired panel is answered as cancelled by the sweep and can no longer be clicked", async () => {
		const h = harness();
		const panelId = await h.open();
		expect(await h.panels.sweep()).toBe(0);
		h.advance(60_000);
		expect(await h.panels.respond({ panelId, responderId: "U-author", responseKind: "approved" })).toBe(
			"panel_not_found",
		);
		expect(await h.panels.sweep()).toBe(1);
		expect(h.sent).toEqual([
			{ id: "rev-1", connectionId: "conn-1", leaseId: "lease-1", result: { outcome: "cancelled" } },
		]);
		expect(h.panels.size).toBe(0);
	});
});
