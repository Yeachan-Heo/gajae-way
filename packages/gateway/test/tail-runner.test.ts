import { expect, test } from "bun:test";
import {
	isUnwrittenRelayRequest,
	MAX_RELAY_REQUEST_FRAME_BYTES,
	RelayClosedError,
	RelayPayloadTooLargeError,
	TailRunner,
} from "../src/orchestrator/tail-runner";
import { scriptedRelay } from "./session-port.fake";

test("request frames enforce the UTF-8 byte limit before writing and keep the relay usable", async () => {
	expect(MAX_RELAY_REQUEST_FRAME_BYTES).toBe(262_144);
	const relay = scriptedRelay(async (request) => ({ ok: true, result: { operation: request.operation } }));
	const runner = new TailRunner({ stream: relay.spawn, repo: "/tmp" });
	const handle = await runner.attach({ sessionId: "sdk-frame-limit", brokerGeneration: 0, repo: "/tmp" });
	const connectionId = relay.streams[0]!.connectionId;
	const images = [{ data: "aW1hZ2U=", mimeType: "image/jpeg" }];
	const emptyTextFrame = {
		type: "control_request",
		id: "gw-0-1",
		connectionId,
		operation: "boundary",
		input: { text: "", images },
		confirm: false,
	};
	const exactTextBytes = 262_144 - Buffer.byteLength(JSON.stringify(emptyTextFrame), "utf8");
	const exactText = "a".repeat(exactTextBytes);
	const exactFrame = { ...emptyTextFrame, input: { text: exactText, images } };
	expect(Buffer.byteLength(JSON.stringify(exactFrame), "utf8")).toBe(262_144);
	await expect(handle.control("boundary", { text: exactText, images })).resolves.toEqual({
		ok: true,
		result: { operation: "boundary" },
	});

	const overflowText = `${"a".repeat(exactTextBytes)}漢`;
	const overflow = await handle.control("boundary", { text: overflowText, images }).catch((error: unknown) => error);
	expect(overflow).toBeInstanceOf(RelayPayloadTooLargeError);
	expect(overflow).not.toBeInstanceOf(RelayClosedError);
	expect(isUnwrittenRelayRequest(overflow)).toBe(true);
	expect((overflow as Error).message).not.toContain(overflowText);
	expect(relay.streams[0]!.written.filter((frame) => frame.type === "control_request")).toHaveLength(1);

	await expect(handle.control("small", { text: "still usable" })).resolves.toEqual({
		ok: true,
		result: { operation: "small" },
	});
	expect(relay.requests.map((request) => request.operation)).toEqual(["boundary", "small"]);
	await handle.close();
});
