import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

let home = "";
let child: ReturnType<typeof Bun.spawn> | undefined;
afterEach(async () => {
	child?.kill();
	if (child) await child.exited;
	child = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});
async function start(): Promise<string> {
	const socket = join(home, "gateway.sock");
	child = Bun.spawn({
		cmd: ["bun", "packages/gateway/src/main.ts", "daemon"],
		cwd: join(import.meta.dir, "../../.."),
		env: { ...process.env, GAJAEWAY_HOME: home, GAJAEWAY_TEST_STUB_GJC: "1" },
		stdout: "ignore",
		stderr: "inherit",
	});
	await Bun.sleep(100);
	return socket;
}
async function client(socketPath: string) {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
	send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	return { frames, send, close: () => socket.end() };
}
async function waitFor(frames: any[], predicate: (frame: any) => boolean) {
	for (let i = 0; i < 200; i++) {
		const frame = frames.find(predicate);
		if (frame) return frame;
		await Bun.sleep(10);
	}
	throw new Error("timed out waiting for frame");
}
test("inflight platform delivery is duplicate-labeled after a process crash", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-crash-"));
	const first = await client(await start());
	first.send({
		v: "0.1",
		type: "request",
		id: "send",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "channel" },
			text: "hello",
			engagement: { mentioned: true, group: true, authorId: "user" },
		},
	});
	const event = await waitFor(first.frames, (frame) => frame.type === "event" && frame.event === "chat.message");
	const deliveryId = event.payload.deliveryId;
	child?.kill("SIGKILL");
	if (child) await child.exited;
	child = undefined;
	first.close();
	const second = await client(await start());
	const redelivery = await waitFor(second.frames, (frame) => frame.type === "event" && frame.event === "chat.message");
	expect(redelivery.payload).toMatchObject({ deliveryId, redelivered: true, duplicateWarning: true });
	second.send({ v: "0.1", type: "request", id: "confirm", verb: "delivery.confirm", params: { deliveryId } });
	await waitFor(second.frames, (frame) => frame.id === "confirm");
	second.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	const status = await waitFor(second.frames, (frame) => frame.id === "status");
	expect(status.result.delivery.pending).toBe(0);
	second.close();
});
