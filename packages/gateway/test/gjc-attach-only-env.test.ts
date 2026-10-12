import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	GJC_ATTACH_ONLY_ENV,
	GjcCliUnavailableError,
	GlobalGjcClient,
	readPinnedGjcVersion,
	type BrokerDiscovery,
	type SpawnFn,
} from "../src/orchestrator/broker";

const directories: string[] = [];
const clients: GlobalGjcClient[] = [];

afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop()));
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

async function tempDir(): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), "gjc-attach-only-env-"));
	directories.push(directory);
	return directory;
}

const discovery = (): BrokerDiscovery => ({
	pid: 12345,
	url: "ws://127.0.0.1:12345",
	token: "test-only",
	heartbeatAt: Date.now(),
});

const SESSIONS = JSON.stringify({ ok: true, result: { sessions: [] } });

test("all spawned GJC commands and relays pin the configured agent directory and attach-only mode", async () => {
	const home = await tempDir();
	const agentDir = join(home, "configured-agent");
	const envNames = ["GJC_EXECUTABLE", "GJC_CODING_AGENT_DIR", "PI_CODING_AGENT_DIR", "GJC_AGENT_DIR", GJC_ATTACH_ONLY_ENV] as const;
	const previous = Object.fromEntries(envNames.map((name) => [name, process.env[name]]));
	process.env.GJC_EXECUTABLE = "/inherited/gjc";
	process.env.GJC_CODING_AGENT_DIR = "/inherited/agent";
	process.env.PI_CODING_AGENT_DIR = "/legacy/pi-agent";
	process.env.GJC_AGENT_DIR = "/legacy/agent-alias";
	process.env[GJC_ATTACH_ONLY_ENV] = "1";

	try {
		const invocations: Array<{ cmd: readonly string[]; env: Record<string, string> }> = [];
		const spawn = ((options: { cmd: readonly string[]; env: Record<string, string> }) => {
			invocations.push(options);
			const stdout = options.cmd.includes("--version")
				? `gjc/${readPinnedGjcVersion()}\n`
				: options.cmd.includes("serve")
					? "relay\n"
					: SESSIONS;
			return {
				exited: Promise.resolve(0),
				stdout: new Blob([stdout]).stream(),
				stderr: new Blob([]).stream(),
				kill() {},
			};
		}) as unknown as SpawnFn;
		const client = new GlobalGjcClient({
			executable: "/configured/gjc",
			agentDir,
			cwd: home,
			spawn,
			discovery: async () => discovery(),
			healthProbe: async () => true,
			healthIntervalMs: 60_000,
		});
		clients.push(client);

		await client.preflight();
		await client.start();
		await client.cli(["sdk", "session", "list", "--scope", "all"]);
		const relay = client.openStream("test-session");
		for await (const _line of relay.lines) {
			// Drain the finite fake relay.
		}
		await client.stop();

		expect(invocations.some(({ cmd }) => cmd.includes("--version"))).toBe(true);
		expect(invocations.some(({ cmd }) => cmd.includes("session"))).toBe(true);
		expect(invocations.some(({ cmd }) => cmd.includes("serve"))).toBe(true);
		for (const invocation of invocations) {
			expect(invocation.cmd[0]).toBe("/configured/gjc");
			expect(invocation.env.GJC_CODING_AGENT_DIR).toBe(client.agentDir);
			expect(invocation.env[GJC_ATTACH_ONLY_ENV]).toBe("0");
			expect(invocation.env.GJC_EXECUTABLE).toBeUndefined();
			expect(invocation.env.PI_CODING_AGENT_DIR).toBeUndefined();
			expect(invocation.env.GJC_AGENT_DIR).toBeUndefined();
		}
	} finally {
		for (const name of envNames) {
			const value = previous[name];
			if (value === undefined) delete process.env[name];
			else process.env[name] = value;
		}
	}
});

test("a structured broker_unavailable result becomes GjcCliUnavailableError", async () => {
	const home = await tempDir();
	const client = new GlobalGjcClient({
		executable: "/configured/gjc",
		agentDir: join(home, "configured-agent"),
		command: async () => ({
			exitCode: 1,
			stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "broker is starting" } }),
			stderr: "",
		}),
	});
	clients.push(client);

	await expect(client.cli(["sdk", "session", "list"])).rejects.toBeInstanceOf(GjcCliUnavailableError);
});
