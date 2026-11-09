import { expect, test } from "bun:test";
import {
	isUnwrittenRelayRequest,
	MAX_RELAY_REQUEST_FRAME_BYTES,
	RelayClosedError,
	TailRunner,
} from "../src/orchestrator/tail-runner";
import { scriptedRelay } from "./session-port.fake";

test("request frames enforce the UTF-8 byte limit before writing and keep the relay usable", async () => {
	const relay = scriptedRelay(async (request) => ({ ok: true, result: { operation: request.operation } }));
	const runner = new TailRunner({ stream: relay.spawn, repo: "/tmp" });
	const handle = await runner.attach({ sessionId: "sdk-frame-limit", brokerGeneration: 0, repo: "/tmp" });
	const connectionId = relay.streams[0]!.connectionId;
	const emptyTextFrame = {
		type: "control_request",
		id: "gw-0-1",
		connectionId,
		operation: "boundary",
		input: { text: "" },
		confirm: false,
	};
	const exactTextBytes = MAX_RELAY_REQUEST_FRAME_BYTES - Buffer.byteLength(JSON.stringify(emptyTextFrame), "utf8");
	const exactText = "a".repeat(exactTextBytes);
	const exactFrame = { ...emptyTextFrame, input: { text: exactText } };
	expect(Buffer.byteLength(JSON.stringify(exactFrame), "utf8")).toBe(MAX_RELAY_REQUEST_FRAME_BYTES);
	await expect(handle.control("boundary", { text: exactText })).resolves.toEqual({
		ok: true,
		result: { operation: "boundary" },
	});

	const overflowText = `${"a".repeat(exactTextBytes)}漢`;
	const overflow = await handle.control("boundary", { text: overflowText }).catch((error: unknown) => error);
	expect(overflow).toBeInstanceOf(RelayClosedError);
	expect((overflow as RelayClosedError).requestWritten).toBe(false);
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
