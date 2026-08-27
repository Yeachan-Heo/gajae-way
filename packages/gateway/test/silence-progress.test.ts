import { describe, expect, test } from "bun:test";
import type { ChatProgressPayload } from "@gajaeway/protocol";
import { subscribeDiscordProgress } from "../../adapter-discord/src/main";

/**
 * The bug this pins: a turn that ends with a silence token delivers nothing, so the
 * adapter's delivery path - the only place that used to call `clear()` - never ran and
 * the temporary "working" message stayed in the channel forever.
 */
function harness() {
	const calls: string[] = [];
	let handler: ((progress: ChatProgressPayload) => void) | undefined;
	const gateway = {
		onChatProgress(next: (progress: ChatProgressPayload) => void) {
			handler = next;
			return () => {
				handler = undefined;
			};
		},
		// The production type also carries request/send; this suite only drives the
		// progress path, so the rest is deliberately absent.
	} as unknown as Parameters<typeof subscribeDiscordProgress>[0];
	const status = {
		async update(progress: ChatProgressPayload) {
			calls.push(`update:${progress.origin.conversationId}`);
		},
		async clear(conversationId: string) {
			calls.push(`clear:${conversationId}`);
		},
	};
	const off = subscribeDiscordProgress(gateway, status, { error: () => {} });
	const emit = (extra: Partial<ChatProgressPayload> = {}) =>
		handler?.({
			turnId: "t1",
			origin: { platform: "discord", kind: "channel", conversationId: "c1" },
			elapsedMs: 12_000,
			toolCalls: 0,
			outputTokens: 0,
			...extra,
		} as ChatProgressPayload);
	return { calls, emit, off };
}

describe("progress subscription", () => {
	test("a non-final event updates the status message", () => {
		const h = harness();
		h.emit();
		expect(h.calls).toEqual(["update:c1"]);
	});

	test("a final event clears it, which is what a silenced turn relies on", () => {
		const h = harness();
		h.emit();
		h.emit({ final: true });
		expect(h.calls).toEqual(["update:c1", "clear:c1"]);
	});

	test("clear happens even when the turn never delivered a message", () => {
		const h = harness();
		h.emit();
		h.emit({ final: true, outputTokens: 0 });
		expect(h.calls.filter((call) => call.startsWith("clear:"))).toEqual(["clear:c1"]);
	});

	test("unsubscribing stops both paths", () => {
		const h = harness();
		h.off();
		h.emit();
		h.emit({ final: true });
		expect(h.calls).toEqual([]);
	});
});
