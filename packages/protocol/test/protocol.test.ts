import { describe, expect, test } from "bun:test";
import {
	CHAT_PLATFORMS,
	containsSilenceToken,
	decodeFrame,
	describeChatPlatforms,
	encodeFrame,
	FrameDecoder,
	isChatPlatform,
	isSilenceToken,
	LOOPBACK_ORIGIN,
	MAX_FRAME_BYTES,
	monitorSessionOrigin,
	negotiate,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	parseOriginKey,
	validateOriginRef,
} from "../src/index";

describe("negotiation", () => {
	test("picks highest mutual version", () => {
		const result = negotiate({ supportedVersions: [PROFILE_VERSION] });
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

	test("monitor session origins are scoped by monitor id and round-trip", () => {
		const key = originKey(monitorSessionOrigin("m-1", "backlog.watch"));
		expect(key).toBe("monitor/eventtype/backlog.watch/parent=m-1");
		expect(key).not.toBe(originKey(monitorSessionOrigin("m-2", "backlog.watch")));
		expect(parseOriginKey(key)).toEqual(monitorSessionOrigin("m-1", "backlog.watch"));
		expect(() => originKey(monitorSessionOrigin("bad/id", "backlog.watch"))).toThrow();
	});

	test("thread requires parentId", () => {
		expect(() => validateOriginRef({ platform: "telegram", kind: "topic", conversationId: "c1" })).toThrow();
	});

	test("slack supports dm, channel and thread, and its thread ids round-trip through originKey", () => {
		expect(originKey({ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" })).toBe(
			"slack/dm/D1/peer=U1",
		);
		expect(originKey({ platform: "slack", kind: "channel", conversationId: "C1" })).toBe("slack/channel/C1");
		// A Slack thread is identified by its channel:ts pair; the colon and dot are
		// legal segment characters, so the key parses back to the same origin.
		const thread = {
			platform: "slack",
			kind: "thread",
			conversationId: "C123:1726543210.123456",
			parentId: "C123",
		} as const;
		const key = originKey(thread);
		expect(key).toBe("slack/thread/C123:1726543210.123456/parent=C123");
		expect(parseOriginKey(key)).toEqual(thread);
	});

	test("slack rejects the telegram-only topic kind", () => {
		expect(() =>
			validateOriginRef({ platform: "slack", kind: "topic", conversationId: "C1:1.2", parentId: "C1" }),
		).toThrow(/topic/);
		expect(() =>
			validateOriginRef({ platform: "telegram", kind: "topic", conversationId: "t1", parentId: "-100" }),
		).not.toThrow();
	});

	test("chat platforms are exactly the ones an adapter can deliver to", () => {
		expect(CHAT_PLATFORMS).toEqual(["discord", "telegram", "slack"]);
		for (const platform of CHAT_PLATFORMS) expect(isChatPlatform(platform)).toBe(true);
		expect(isChatPlatform("loopback")).toBe(false);
		expect(isChatPlatform("monitor")).toBe(false);
		expect(describeChatPlatforms()).toBe("discord, telegram or slack");
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

describe("silence tokens", () => {
	for (const text of [
		"preamble [SILENT]",
		"preamble\n[SILENT]\npostscript",
		"preamble [silent]",
		"preamble\n[silent]\npostscript",
	]) {
		test(`recognizes embedded marker ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(true);
			expect(isSilenceToken(text)).toBe(false);
		});
	}

	test("recognizes an original-body marker beyond a 2 KiB excerpt", () => {
		const text = `${"x".repeat(2049)}\n[SILENT]`;
		expect(containsSilenceToken(text)).toBe(true);
		expect(containsSilenceToken(text.slice(0, 2048))).toBe(false);
	});

	for (const text of [
		"ordinary text",
		"preamble SILENT",
		"preamble [Silent]",
		"preamble [NO_REPLY]",
		"preamble [ SILENT ]",
	]) {
		test(`does not broaden embedded grammar for ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(false);
		});
	}

	for (const text of ["SILENT", "[SILENT]", "silent", "NO_REPLY", "NO REPLY", "[NO_REPLY]", "[NO REPLY]"]) {
		test(`preserves exact-body alias ${text}`, () => {
			expect(isSilenceToken(text)).toBe(true);
			expect(isSilenceToken(`  ${text}\n`)).toBe(true);
		});
	}
});
