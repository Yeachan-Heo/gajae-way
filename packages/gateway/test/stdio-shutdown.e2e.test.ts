import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
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
		child.stdin.write(
			`${JSON.stringify({ v: "0.1", type: "request", id: "bye", verb: "gateway.shutdown", params: {} })}\n`,
		);
		child.stdin.flush();
		const exit = await Promise.race([child.exited, Bun.sleep(15_000).then(() => "timeout" as const)]);
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

test("stdio retention logs successful deletions as info, failures as errors, and keeps stdout clean", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-stdio-retention-"));
	await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const script = `
		import { join } from "node:path";
		import { installStructuredLogging } from "./packages/log/src/index.ts";
		import { bootGateway, prepareStdioConsole } from "./packages/gateway/src/boot.ts";
		import { Store } from "./packages/gateway/src/store/index.ts";
		import { testOnlyBrokerDependencies } from "./packages/gateway/src/orchestrator/test-broker.ts";
		const home = process.env.GAJAEWAY_HOME;
		let runRetention;
		const originalTimeout = globalThis.setTimeout;
		globalThis.setTimeout = (fn, ms, ...args) => {
			if (ms === 30000) runRetention = fn;
			return originalTimeout(fn, ms, ...args);
		};
		let calls = 0;
		Store.prototype.sweep = function () {
			calls++;
			if (calls === 3) throw new Error("injected retention failure");
			return { deleted: { deliveries: calls === 1 ? 2 : 0 }, more: false };
		};
		prepareStdioConsole();
		const dispose = installStructuredLogging({ path: join(home, "gateway.log"), heartbeatIntervalMs: 0 });
		const gateway = await bootGateway({ stdio: true, stdioConsolePrepared: true, home, broker: testOnlyBrokerDependencies() });
		globalThis.setTimeout = originalTimeout;
		if (!runRetention) throw new Error("retention boot timer missing");
		for (let i = 0; i < 3; i++) { runRetention(); await Bun.sleep(10); }
		await gateway.stop("test");
		dispose();
	`;
	const child = Bun.spawn([process.execPath, "-e", script], {
		cwd: join(import.meta.dir, "../../.."),
		env: { ...process.env, GAJAEWAY_HOME: home },
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	try {
		const stdout = new Response(child.stdout).text();
		const stderr = new Response(child.stderr).text();
		const exit = await Promise.race([child.exited, Bun.sleep(15_000).then(() => "timeout" as const)]);
		if (exit === "timeout") throw new Error("stdio retention test child timed out");
		expect(exit, await stderr).toBe(0);
		const frames = (await stdout)
			.trim()
			.split("\n")
			.map((line) => JSON.parse(line) as { event?: string });
		expect(frames).toEqual([expect.objectContaining({ event: "gateway.stopping" })]);
		const sink = await readFile(join(home, "gateway.log"), "utf8");
		expect(sink).toContain(' info {"retention":{"deliveries":2}}');
		expect(sink).not.toContain(' error {"retention"');
		expect(sink).toContain(" error gateway retention sweep failed: injected retention failure");
		expect(sink.match(/\{"retention":/g) ?? []).toHaveLength(1);
		expect(await stderr).toContain(' info {"retention":{"deliveries":2}}');
	} finally {
		child.kill();
		await rm(home, { recursive: true, force: true });
	}
});
