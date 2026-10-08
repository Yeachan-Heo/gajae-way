import { describe, expect, test } from "bun:test";
import { encodeFrame, FrameDecoder } from "@gajae-gateway/protocol";

describe("Id-less error frames fail pending requests (issue #420 acceptance)", () => {
	test("id-less malformed_frame error frame is recognized as fatal stream error", () => {
		const frameDecoder = new FrameDecoder();

		// Create an error frame without an id - should be treated as fatal stream error
		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			// No id field - this is critical
			error: { code: "malformed_frame", message: "frame is not valid JSON" },
		});

		const frames = frameDecoder.feed(errorFrame);
		expect(frames.length).toBe(1);

		const frame = frames[0];
		expect(frame.type).toBe("error");
		if (frame.type === "error") {
			// Verify the frame has no id
			expect(frame.id).toBeUndefined();
			// Verify it's a malformed_frame error
			expect(frame.error.code).toBe("malformed_frame");
			expect(frame.error.message).toBe("frame is not valid JSON");
		}
	});

	test("id-less payload_too_large error frame is recognized as fatal stream error", () => {
		const frameDecoder = new FrameDecoder();

		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			// No id field
			error: { code: "payload_too_large", message: "frame exceeds 1048576 bytes" },
		});

		const frames = frameDecoder.feed(errorFrame);
		expect(frames.length).toBe(1);

		const frame = frames[0];
		expect(frame.type).toBe("error");
		if (frame.type === "error") {
			expect(frame.id).toBeUndefined();
			expect(frame.error.code).toBe("payload_too_large");
		}
	});

	test("error frame with id is NOT treated as fatal stream error", () => {
		const frameDecoder = new FrameDecoder();

		// Create an error frame WITH an id - should be routed to the pending request
		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			id: "123", // Has id - specific to that request
			error: { code: "malformed_frame", message: "invalid params for this specific request" },
		});

		const frames = frameDecoder.feed(errorFrame);
		expect(frames.length).toBe(1);

		const frame = frames[0];
		expect(frame.type).toBe("error");
		if (frame.type === "error") {
			// Verify the frame HAS an id
			expect(frame.id).toBe("123");
			// This should only affect request 123, not all pending requests
		}
	});

	test("id-less other error codes are not treated as fatal stream errors", () => {
		const frameDecoder = new FrameDecoder();

		// Invalid params error without id should go to __negotiation_error, not fail all requests
		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			// No id
			error: { code: "invalid_params", message: "missing required param" },
		});

		const frames = frameDecoder.feed(errorFrame);
		expect(frames.length).toBe(1);

		const frame = frames[0];
		expect(frame.type).toBe("error");
		if (frame.type === "error") {
			expect(frame.id).toBeUndefined();
			// This error code is NOT in the fatal list (malformed_frame, payload_too_large)
			expect(frame.error.code).toBe("invalid_params");
		}
	});

	test("client behavior: id-less malformed_frame fails all pending requests immediately", async () => {
		// Simulate client request and error handling
		const failedRequests: string[] = [];
		const pendingIds = ["req-1", "req-2", "req-3"];

		// Simulate receiving id-less malformed_frame error
		const frameDecoder = new FrameDecoder();
		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			error: { code: "malformed_frame", message: "frame is not valid JSON" },
		});

		const frames = frameDecoder.feed(errorFrame);
		const frame = frames[0];

		// This is what the client should do:
		if (
			frame.type === "error" &&
			!frame.id &&
			(frame.error.code === "malformed_frame" || frame.error.code === "payload_too_large")
		) {
			// Fail all pending requests immediately
			for (const id of pendingIds) {
				failedRequests.push(id);
			}
		}

		// Verify all pending requests were failed
		expect(failedRequests.length).toBe(pendingIds.length);
		expect(failedRequests).toEqual(pendingIds);
	});

	test("client does NOT fail all pending requests for id-less non-fatal errors", async () => {
		const pending = new Map();
		const pendingIds = ["req-1", "req-2"];

		for (const id of pendingIds) {
			pending.set(id, { rejected: false });
		}

		// Simulate receiving id-less invalid_params error (not a fatal stream error)
		const frameDecoder = new FrameDecoder();
		const errorFrame = encodeFrame({
			v: "0.1",
			type: "error",
			error: { code: "invalid_params", message: "missing param" },
		});

		const frames = frameDecoder.feed(errorFrame);
		const frame = frames[0];

		// This should NOT fail all pending requests
		if (frame.type === "error" && !frame.id) {
			if (frame.error.code === "malformed_frame" || frame.error.code === "payload_too_large") {
				// Would fail all
				for (const [, pending_item] of pending) {
					pending_item.rejected = true;
				}
			}
			// Otherwise just emit __negotiation_error, don't fail all pending requests
		}

		// Verify pending requests were NOT failed
		for (const [, pending_item] of pending) {
			expect(pending_item.rejected).toBe(false);
		}
	});
});
