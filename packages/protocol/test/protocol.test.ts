import { describe, expect, test } from "bun:test";
import {
	decodeFrame,
	encodeFrame,
	FrameDecoder,
	LOOPBACK_ORIGIN,
	MAX_FRAME_BYTES,
	negotiate,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	validateOriginRef,
} from "../src/index";

describe("negotiation", () => {
	test("picks highest mutual version", () => {
		const result = negotiate({ supportedVersions: ["0.1"] });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.negotiated.profileVersion).toBe(PROFILE_VERSION);
	});

	test("rejects disjoint versions with typed code", () => {
		const result = negotiate({ supportedVersions: ["9.9"] });
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.code).toBe("incompatible_profile_version");
	});

	test("rejects missing required capability", () => {
		const result = negotiate({
			supportedVersions: ["0.1"],
			requiredCapabilities: ["timetravel"],
		});
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.code).toBe("missing_required_capability");
	});

	test("unknown optional fields are ignored", () => {
		const hello = {
			supportedVersions: ["0.1"],
			futureOptionalField: { anything: true },
		} as never;
		expect(negotiate(hello).ok).toBe(true);
	});
});

describe("frames", () => {
	test("round-trips a request frame", () => {
		const line = encodeFrame({
			v: PROFILE_VERSION,
			type: "request",
			id: "r1",
			verb: "gateway.status",
		});
		const frame = decodeFrame(line.trim());
		expect(frame.type).toBe("request");
	});

	test("rejects non-JSON with malformed_frame", () => {
		expect(() => decodeFrame("not json")).toThrow(ProtocolError);
		try {
			decodeFrame("not json");
		} catch (error) {
			expect((error as ProtocolError).code).toBe("malformed_frame");
		}
	});

	test("rejects unknown frame type typed", () => {
		try {
			decodeFrame(JSON.stringify({ v: "0.1", type: "teleport" }));
			expect.unreachable();
		} catch (error) {
			expect((error as ProtocolError).code).toBe("unsupported_frame_type");
		}
	});

	test("rejects oversized frames", () => {
		const big = "x".repeat(MAX_FRAME_BYTES + 1);
		expect(() => decodeFrame(big)).toThrow(ProtocolError);
	});

	test("decoder splits chunked NDJSON and buffers partials", () => {
		const decoder = new FrameDecoder();
		const line = encodeFrame({ v: "0.1", type: "event", event: "chat.message", payload: {} });
		const half = Math.floor(line.length / 2);
		expect(decoder.feed(line.slice(0, half))).toHaveLength(0);
		const frames = decoder.feed(line.slice(half));
		expect(frames).toHaveLength(1);
		expect(frames[0]?.type).toBe("event");
	});
});

describe("origin normalization", () => {
	test("originKey is deterministic and validated", () => {
		const key = originKey({
			platform: "discord",
			kind: "thread",
			conversationId: "111",
			parentId: "222",
		});
		expect(key).toBe("discord/thread/111/parent=222");
	});

	test("dm requires peerId; channel forbids it", () => {
		expect(() => validateOriginRef({ platform: "discord", kind: "dm", conversationId: "c1" })).toThrow();
		expect(() =>
			validateOriginRef({
				platform: "discord",
				kind: "channel",
				conversationId: "c1",
				peerId: "p1",
			}),
		).toThrow();
	});

	test("thread requires parentId", () => {
		expect(() => validateOriginRef({ platform: "telegram", kind: "topic", conversationId: "c1" })).toThrow();
	});

	test("loopback origin is valid and stable", () => {
		expect(originKey(LOOPBACK_ORIGIN)).toBe("loopback/loopback/loopback");
	});

	test("two distinct origins never share a key", () => {
		const a = originKey({
			platform: "discord",
			kind: "dm",
			conversationId: "c1",
			peerId: "alice",
		});
		const b = originKey({
			platform: "discord",
			kind: "dm",
			conversationId: "c1",
			peerId: "bob",
		});
		expect(a).not.toBe(b);
	});

	test("rejects segment injection attempts", () => {
		expect(() =>
			validateOriginRef({
				platform: "discord",
				kind: "channel",
				conversationId: "c1/parent=evil",
			}),
		).toThrow();
	});
});
