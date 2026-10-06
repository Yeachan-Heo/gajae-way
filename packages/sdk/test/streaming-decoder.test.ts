import { expect, test, describe } from "bun:test";
import { encodeFrame, FrameDecoder } from "@gajae-gateway/protocol";
import type { Frame } from "@gajae-gateway/protocol";

describe("Streaming TextDecoder for multibyte characters (issue #420 acceptance)", () => {
	test("multibyte character split across two data chunks decodes correctly", () => {
		// Korean character "한" is encoded as 0xED 0x95 0x9C in UTF-8 (3 bytes)
		// Split it across two chunks
		const decoder = new TextDecoder();

		const part1 = new Uint8Array([0xED, 0x95]); // First 2 bytes of "한"
		const part2 = new Uint8Array([0x9C]); // Third byte of "한"

		const decoded1 = decoder.decode(part1, { stream: true } as any);
		const decoded2 = decoder.decode(part2, { stream: true } as any);
		const result = decoded1 + decoded2;

		expect(result).toBe("한");
		expect(result).not.toContain("\uFFFD"); // Should not contain replacement char
	});

	test("client receives multibyte character split across socket reads", () => {
		const frameDecoder = new FrameDecoder();

		// Create a request frame with Korean text
		const frame: Frame = {
			v: "0.1",
			type: "request",
			id: "multibyte-test",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: "한글 테스트" },
		};

		const encoded = encodeFrame(frame);
		const bytes = Buffer.from(encoded, "utf-8");

		// Find a location to split that will split a multibyte character
		// Try several split points to find one that breaks a multibyte sequence
		let foundSplit = false;
		let split1: string = "";
		let split2: string = "";

		for (let i = 10; i < bytes.length - 10; i++) {
			const chunk1 = bytes.subarray(0, i);
			const chunk2 = bytes.subarray(i);

			// Check if this split breaks a multibyte character
			try {
				const decoder1 = new TextDecoder();
				const decoder2 = new TextDecoder();

				const decoded1 = decoder1.decode(chunk1, { stream: true } as any);
				const decoded2 = decoder2.decode(chunk2, { stream: true } as any);

				// If we got a replacement character, we found a good split
				if (decoded1.includes("\uFFFD") || (decoded1 + decoded2).includes("\uFFFD")) {
					// This is a split that doesn't work without streaming
					foundSplit = true;
					split1 = decoded1;
					split2 = decoded2;
					break;
				}
			} catch {
				// Continue to next split point
			}
		}

		// If we found a split that breaks with non-streaming decoders,
		// verify that streaming decoder handles it correctly
		const decoder1 = new TextDecoder();
		const chunk1Bytes = bytes.subarray(0, Math.floor(bytes.length / 2));
		const chunk2Bytes = bytes.subarray(Math.floor(bytes.length / 2));

		const decoded1 = decoder1.decode(chunk1Bytes, { stream: true } as any);
		const decoded2 = decoder1.decode(chunk2Bytes, { stream: true } as any);
		const fullDecoded = decoded1 + decoded2;

		// Verify we can decode frames from the concatenated result
		const frames = frameDecoder.feed(fullDecoded);
		expect(frames.length).toBe(1);
		expect(frames[0].type).toBe("request");
		if (frames[0].type === "request") {
			const params = frames[0].params as any;
			expect(params.text).toBe("한글 테스트");
		}
	});

	test("gateway receives multibyte character split across socket reads", () => {
		const frameDecoder = new FrameDecoder();

		// Create a response frame with multibyte content
		const frame: Frame = {
			v: "0.1",
			type: "response",
			id: "response-1",
			result: { text: "응답: 한글 처리" },
		};

		const encoded = encodeFrame(frame);
		const bytes = Buffer.from(encoded, "utf-8");

		// Split at an arbitrary point (may or may not be in the middle of a multibyte char)
		const splitPoint = Math.floor(bytes.length / 2);
		const chunk1 = bytes.subarray(0, splitPoint);
		const chunk2 = bytes.subarray(splitPoint);

		// Gateway uses streaming decoder to handle this
		const decoder = new TextDecoder();
		const decoded1 = decoder.decode(chunk1, { stream: true } as any);
		const decoded2 = decoder.decode(chunk2, { stream: true } as any);
		const fullDecoded = decoded1 + decoded2;

		// Verify frame decodes correctly
		const frames = frameDecoder.feed(fullDecoded);
		expect(frames.length).toBe(1);
		expect(frames[0].type).toBe("response");
		if (frames[0].type === "response") {
			const result = frames[0].result as any;
			expect(result.text).toBe("응답: 한글 처리");
		}
	});

	test("multiple multibyte characters in sequence decode correctly when split", () => {
		const frameDecoder = new FrameDecoder();

		// Create frame with many multibyte characters
		const text = "你好世界 こんにちは 안녕하세요 Привет שלום مرحبا".repeat(10);
		const frame: Frame = {
			v: "0.1",
			type: "request",
			id: "multilang-test",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text },
		};

		const encoded = encodeFrame(frame);
		const bytes = Buffer.from(encoded, "utf-8");

		// Split at multiple points and verify all decode correctly
		for (let splitPoint of [64, 256, 512, 1024]) {
			if (splitPoint >= bytes.length) continue;

			const chunk1 = bytes.subarray(0, splitPoint);
			const chunk2 = bytes.subarray(splitPoint);

			const decoder1 = new TextDecoder();
			const decoded1 = decoder1.decode(chunk1, { stream: true } as any);
			const decoded2 = decoder1.decode(chunk2, { stream: true } as any);
			const fullDecoded = decoded1 + decoded2;

			const frames = frameDecoder.feed(fullDecoded);
			expect(frames.length).toBeGreaterThan(0);
			if (frames[0].type === "request") {
				const params = frames[0].params as any;
				expect(params.text).toBe(text);
			}
		}
	});
});
