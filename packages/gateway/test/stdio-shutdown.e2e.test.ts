import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * A `gateway.shutdown` request over stdio must let `stop()` resolve: the request
 * task awaits stop(), and stop() awaits in-flight requests, so tracking the
 * shutdown task itself would self-await forever and the daemon would never exit.
 */
test("a stdio gateway.shutdown request completes ordered shutdown and exits the daemon", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-stdio-shutdown-"));
	await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const child = Bun.spawn({
		cmd: ["bun", "packages/gateway/test/daemon-entry.ts", "--stdio"],
		cwd: join(import.meta.dir, "../../.."),
		env: { ...process.env, GAJAEWAY_HOME: home },
		stdin: "pipe",
		stdout: "pipe",
		stderr: "ignore",
	});
	try {
		const output = new Response(child.stdout as ReadableStream).text();
		child.stdin.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		child.stdin.write(`${JSON.stringify({ v: "0.1", type: "request", id: "bye", verb: "gateway.shutdown", params: {} })}\n`);
		child.stdin.flush();
		const exit = await Promise.race([
			child.exited,
			Bun.sleep(15_000).then(() => "timeout" as const),
		]);
		expect(exit).not.toBe("timeout");
		const frames = (await output)
			.split("\n")
			.filter(Boolean)
			.map((line) => JSON.parse(line) as { type: string; event?: string; id?: string });
		expect(frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(true);
	} finally {
		child.kill();
		await rm(home, { recursive: true, force: true });
	}
});
