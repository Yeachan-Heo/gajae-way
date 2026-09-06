import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, parseConfigFile, reloadConfig, resolveTransport, transportFlag } from "../src/config";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromResponder } from "./session-port.fake";
import { eventually } from "./red-first-harness";

const homes: string[] = [];
afterEach(async () => {
	for (const home of homes.splice(0)) await rm(home, { recursive: true, force: true });
});
async function home() {
	const path = await mkdtemp(join(tmpdir(), "transport-selector-"));
	homes.push(path);
	return path;
}

test("transport precedence is flag > environment > config > CLI default", async () => {
	const directory = await home();
	expect((await loadConfig({ home: directory, env: {} })).transport).toBe("cli");
	await Bun.write(join(directory, "config.json"), JSON.stringify({ schemaVersion: 1, transport: "channel" }));
	expect((await loadConfig({ home: directory, env: {} })).transport).toBe("channel");
	expect((await loadConfig({ home: directory, env: { GAJAEWAY_TRANSPORT: "cli" } })).transport).toBe("cli");
	expect(
		(
			await loadConfig({
				home: directory,
				env: { GAJAEWAY_TRANSPORT: "cli" },
				overrides: { transport: transportFlag(["--transport", "channel"]) },
			})
		).transport,
	).toBe("channel");
	expect(resolveTransport("cli", "channel", "channel")).toBe("cli");
});

test("invalid transport values and a missing flag argument are refused", async () => {
	for (const value of ["", "stdio", "CHANNEL", 1, null]) {
		expect(() => parseConfigFile({ schemaVersion: 1, transport: value })).toThrow("transport must be");
	}
	expect(() => transportFlag(["--transport"])).toThrow("transport must be");
	expect(() => transportFlag(["--transport", "bogus"])).toThrow("transport must be");
	await expect(loadConfig({ home: await home(), env: { GAJAEWAY_TRANSPORT: "bogus" } })).rejects.toThrow(
		"transport must be",
	);
});

test("transport edits require restart and status reports the boot-selected transport", async () => {
	const directory = await home();
	await Bun.write(join(directory, "config.json"), JSON.stringify({ schemaVersion: 1, transport: "cli" }));
	const config = await loadConfig({ home: directory, env: {} });
	await Bun.write(config.configPath, JSON.stringify({ schemaVersion: 1, transport: "channel" }));
	const reload = await reloadConfig(config);
	expect(reload.ok && reload.restartRequired).toContain("transport");
	expect(reload.config.transport).toBe("cli");
	const database = await GatewayDatabase.open(config.dbPath);
	const server = await startUnixServer({
		config,
		transport: "channel",
		database,
		sessionPort: sessionPortFromResponder({ respond: async () => "reply" }),
	});
	const frames: Array<{ id?: string; result?: { transport?: string } }> = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	try {
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await eventually(() => frames.length > 0, "hello response");
		socket.write(`${JSON.stringify({ v: "0.1", type: "request", id: "status", verb: "gateway.status" })}\n`);
		await eventually(() => frames.some((frame) => frame.id === "status"), "status response");
		expect(frames.find((frame) => frame.id === "status")?.result?.transport).toBe("channel");
	} finally {
		socket.end();
		await server.stop();
		database.close();
	}
});

test("daemon launcher prints the selected rollback flag and gateway argv rejects invalid transport before boot", async () => {
	const directory = await home();
	await Bun.write(join(directory, "config.json"), JSON.stringify({ schemaVersion: 1, transport: "channel" }));
	const env = { ...process.env, GAJAEWAY_HOME: directory, GAJAEWAY_TRANSPORT: "channel" };
	const launcher = Bun.spawn(
		[process.execPath, join(import.meta.dir, "../../cli/src/main.ts"), "daemon", "run", "--transport", "cli"],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	const stdout = await new Response(launcher.stdout).text();
	const stderr = await new Response(launcher.stderr).text();
	expect(await launcher.exited).toBe(0);
	expect(stderr).toBe("");
	expect(stdout).toContain("daemon --transport cli");
	const invalid = Bun.spawn(
		[process.execPath, join(import.meta.dir, "../src/main.ts"), "daemon", "--transport", "invalid"],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	const diagnostic = await new Response(invalid.stderr).text();
	expect(await invalid.exited).toBe(1);
	expect(diagnostic).toContain('transport must be "cli" or "channel"');
	expect(await Bun.file(join(directory, "gateway.db")).exists()).toBe(false);
});
