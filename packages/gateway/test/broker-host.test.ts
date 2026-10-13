import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { credentialFingerprint } from "../src/broker-agent/agent-db";
import { resolveBrokerAgentSource } from "../src/broker-agent/allowlist";
import { provisionBrokerAgent } from "../src/broker-agent/provision";
import { runBrokerHost } from "../src/broker-host";
import { loadConfig } from "../src/config";
import { readPinnedGjcVersion } from "../src/orchestrator/broker";

interface Fixture {
	readonly root: string;
	readonly home: string;
	readonly source: string;
	readonly agentDir: string;
}

const roots: string[] = [];
const processes: Array<ReturnType<typeof Bun.spawn>> = [];
afterEach(async () => {
	for (const process of processes.splice(0)) {
		try {
			if (process.exitCode === null && process.signalCode === null) process.kill("SIGKILL");
		} catch {
			// The test may already have reaped the child.
		}
		await process.exited;
	}
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture(): Promise<Fixture> {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-broker-host-")));
	roots.push(root);
	const home = join(root, "home");
	const source = join(root, "operator-agent");
	const agentDir = homeLayout(home).brokerAgentDir;
	await mkdir(home, { recursive: true, mode: 0o700 });
	await mkdir(source, { recursive: true });
	const database = new Database(join(source, "agent.db"));
	database.exec(
		"CREATE TABLE auth_credentials (provider TEXT NOT NULL, token TEXT NOT NULL); INSERT INTO auth_credentials VALUES ('p', 'secret-fixture');",
	);
	database.close();
	return { root, home, source, agentDir };
}

async function provision(f: Fixture): Promise<void> {
	const lease = await acquireLease(f.home, "broker");
	try {
		await provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source: f.source, mode: "force", lease });
	} finally {
		await lease.release();
	}
}

async function installFakeGjc(
	f: Fixture,
	options: { readonly version?: string; readonly waitForTerm?: boolean } = {},
): Promise<{ readonly argsPath: string; readonly envPath: string; readonly termPath: string }> {
	const layout = homeLayout(f.home);
	const bin = join(layout.gjcCurrent, "bin");
	await mkdir(bin, { recursive: true });
	const argsPath = join(f.root, "gjc-args");
	const envPath = join(f.root, "gjc-env");
	const termPath = join(f.root, "gjc-term");
	const scriptPath = layout.gjcExecutable();
	const version = options.version ?? readPinnedGjcVersion();
	const wait = options.waitForTerm ? "while :; do sleep 0.05; done" : "exit 0";
	const script = [
		"#!/bin/sh",
		'if [ "$1" = "--version" ]; then printf \'gjc %s\\n\' ' + shellQuote(version) + "; exit 0; fi",
		`printf '%s\\n' "$@" > ${shellQuote(argsPath)}`,
		`printf '%s\\n%s\\n%s\\n' "$GJC_CODING_AGENT_DIR" "$GJC_SDK_BROKER_AUTOSTART" "$MODEL_PROVIDER_KEY" > ${shellQuote(envPath)}`,
		`trap 'printf forwarded > ${shellQuote(termPath)}; exit 23' TERM`,
		wait,
	].join("\n");
	await writeFile(scriptPath, `${script}\n`, { mode: 0o700 });
	await chmod(scriptPath, 0o700);
	return { argsPath, envPath, termPath };
}

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", "'\\''")}'`;
}

async function configFor(f: Fixture) {
	return loadConfig({ home: f.home });
}

async function runHostChild(f: Fixture): Promise<ReturnType<typeof Bun.spawn>> {
	const hostUrl = new URL("../src/broker-host.ts", import.meta.url).href;
	const configUrl = new URL("../src/config.ts", import.meta.url).href;
	const script = `
		import { loadConfig } from ${JSON.stringify(configUrl)};
		import { runBrokerHost } from ${JSON.stringify(hostUrl)};
		const config = await loadConfig({ home: ${JSON.stringify(f.home)} });
		process.exitCode = await runBrokerHost(config);
	`;
	const child = Bun.spawn([process.execPath, "--eval", script], {
		cwd: join(import.meta.dir, "../../.."),
		stdout: "pipe",
		stderr: "pipe",
		stdin: "ignore",
	});
	processes.push(child);
	return child;
}

async function waitForFile(path: string): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		if (await Bun.file(path).exists()) return;
		await Bun.sleep(10);
	}
	throw new Error(`timed out waiting for ${path}`);
}

async function crashProvision(f: Fixture, marker: string): Promise<void> {
	const provisionUrl = new URL("../src/broker-agent/provision.ts", import.meta.url).href;
	const homeUrl = new URL("../../home/src/index.ts", import.meta.url).href;
	const script = `
		import { writeFileSync } from "node:fs";
		import { provisionBrokerAgent } from ${JSON.stringify(provisionUrl)};
		import { acquireLease } from ${JSON.stringify(homeUrl)};
		const lease = await acquireLease(${JSON.stringify(f.home)}, "broker");
		await provisionBrokerAgent({
			home: ${JSON.stringify(f.home)},
			agentDir: ${JSON.stringify(f.agentDir)},
			source: ${JSON.stringify(f.source)},
			mode: "force",
			lease,
			fault: (point) => {
				if (point === "after_old_agent.db") {
					writeFileSync(${JSON.stringify(marker)}, point);
					Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
				}
			},
		});
	`;
	const child = Bun.spawn([process.execPath, "--eval", script], {
		cwd: join(import.meta.dir, "../../.."),
		stdout: "pipe",
		stderr: "pipe",
		stdin: "ignore",
	});
	processes.push(child);
	await waitForFile(marker);
	child.kill("SIGKILL");
	await child.exited;
	expect(child.signalCode).toBe("SIGKILL");
}

describe("foreground broker host", () => {
	test("holds broker lease, verifies pin and spawns exact foreground argv with attach-only env", async () => {
		const f = await fixture();
		await provision(f);
		const fake = await installFakeGjc(f);
		const exit = await runBrokerHost(await configFor(f));
		expect(exit).toBe(0);
		expect(await readFile(fake.argsPath, "utf8")).toBe(`sdk\nbroker\nrun\n--agent-dir\n${f.agentDir}\n`);
		expect(await readFile(fake.envPath, "utf8")).toBe(`${f.agentDir}\n0\n\n`);
	});

	test("lease contention, invalid target, and missing manifest refuse without spawning", async () => {
		const f = await fixture();
		await mkdir(f.agentDir, { recursive: true, mode: 0o700 });
		const fake = await installFakeGjc(f);
		const config = await configFor(f);
		const held = await acquireLease(f.home, "broker");
		try {
			expect(await runBrokerHost(config)).toBe(1);
		} finally {
			await held.release();
		}
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		expect(await runBrokerHost(config)).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
	});

	test("version mismatch and broker.env policy refuse before foreground spawn", async () => {
		const f = await fixture();
		await provision(f);
		const fake = await installFakeGjc(f, { version: "0.18.7" });
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);

		await installFakeGjc(f);
		const brokerEnv = join(f.home, "broker.env");
		await writeFile(brokerEnv, "GJC_AUTH_BROKER_URL=ws://127.0.0.1:9000\n", { mode: 0o600 });
		await chmod(brokerEnv, 0o600);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		await rm(brokerEnv);
		await writeFile(brokerEnv, "GJC_AUTH_BROKER_TOKEN=literal-token\n", { mode: 0o600 });
		await chmod(brokerEnv, 0o600);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		await writeFile(brokerEnv, "GJC_CODING_AGENT_DIR=/tmp/operator-agent\n", { mode: 0o600 });
		await chmod(brokerEnv, 0o600);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		await writeFile(brokerEnv, "GJC_SDK_BROKER_AUTOSTART=1\n", { mode: 0o600 });
		await chmod(brokerEnv, 0o600);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		await writeFile(brokerEnv, "MODEL_PROVIDER_KEY=literal $HOME value\n", { mode: 0o644 });
		await chmod(brokerEnv, 0o644);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
	});

	test("broker.env symlinks and inherited live broker URL are refused", async () => {
		const f = await fixture();
		await provision(f);
		const fake = await installFakeGjc(f);
		const brokerEnv = join(f.home, "broker.env");
		const target = join(f.root, "broker-env-target");
		await writeFile(target, "MODEL_PROVIDER_KEY=value\n", { mode: 0o600 });
		await symlink(target, brokerEnv);
		expect(await runBrokerHost(await configFor(f))).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		await rm(brokerEnv);
		const previous = process.env.GJC_AUTH_BROKER_URL;
		process.env.GJC_AUTH_BROKER_URL = "ws://127.0.0.1:9000";
		try {
			expect(await runBrokerHost(await configFor(f))).toBe(1);
			expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		} finally {
			if (previous === undefined) delete process.env.GJC_AUTH_BROKER_URL;
			else process.env.GJC_AUTH_BROKER_URL = previous;
		}
		const previousToken = process.env.GJC_AUTH_BROKER_TOKEN;
		process.env.GJC_AUTH_BROKER_TOKEN = "literal-token";
		try {
			expect(await runBrokerHost(await configFor(f))).toBe(1);
			expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		} finally {
			if (previousToken === undefined) delete process.env.GJC_AUTH_BROKER_TOKEN;
			else process.env.GJC_AUTH_BROKER_TOKEN = previousToken;
		}
	});

	test("symlinked or shared agent directories and unrecoverable journals refuse without spawning", async () => {
		const f = await fixture();
		const config = await configFor(f);
		const fake = await installFakeGjc(f);
		const realAgent = join(f.root, "real-agent");
		await mkdir(realAgent, { mode: 0o700 });
		const symlinkedAgent = join(f.root, "symlinked-agent");
		await symlink(realAgent, symlinkedAgent);
		expect(await runBrokerHost({ ...config, gjc: { agentDir: symlinkedAgent } })).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);

		const previousHome = process.env.HOME;
		process.env.HOME = f.root;
		const sharedAgent = resolveBrokerAgentSource();
		try {
			await mkdir(sharedAgent, { recursive: true, mode: 0o700 });
			expect(await runBrokerHost({ ...config, gjc: { agentDir: sharedAgent } })).toBe(1);
			expect(await Bun.file(fake.argsPath).exists()).toBe(false);
		} finally {
			if (previousHome === undefined) delete process.env.HOME;
			else process.env.HOME = previousHome;
		}

		await provision(f);
		const provisionDir = homeLayout(f.home).brokerProvisionDir;
		await writeFile(join(provisionDir, "journal.json"), "{}\n", { mode: 0o600 });
		expect(await runBrokerHost(config)).toBe(1);
		expect(await Bun.file(fake.argsPath).exists()).toBe(false);
	});

	test("broker.env values remain literal and reach the child unchanged", async () => {
		const f = await fixture();
		await provision(f);
		const fake = await installFakeGjc(f);
		const brokerEnv = join(f.home, "broker.env");
		await writeFile(brokerEnv, "MODEL_PROVIDER_KEY=literal $HOME value\n", { mode: 0o600 });
		await chmod(brokerEnv, 0o600);
		expect(await runBrokerHost(await configFor(f))).toBe(0);
		expect(await readFile(fake.envPath, "utf8")).toBe(`${f.agentDir}\n0\nliteral $HOME value\n`);
	});
	test("an interrupted swap is recovered before the host launches the broker", async () => {
		const f = await fixture();
		await provision(f);
		const manifestPath = homeLayout(f.home).brokerProvisionManifest;
		const originalManifest = JSON.parse(await readFile(manifestPath, "utf8")) as { genId: string };
		const originalFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
		const database = new Database(join(f.source, "agent.db"));
		database.query("UPDATE auth_credentials SET token = 'new-source-token'").run();
		database.close();
		const marker = join(f.root, "crash.marker");
		await crashProvision(f, marker);
		expect(await Bun.file(join(f.agentDir, "agent.db")).exists()).toBe(false);
		const fake = await installFakeGjc(f);
		expect(await runBrokerHost(await configFor(f))).toBe(0);
		expect(await readFile(fake.argsPath, "utf8")).toContain("sdk\nbroker\nrun\n");
		expect(await Bun.file(join(f.agentDir, "agent.db")).exists()).toBe(true);
		expect(JSON.parse(await readFile(manifestPath, "utf8")).genId).toBe(originalManifest.genId);
		expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(originalFingerprint);
		expect(await Bun.file(join(f.home, "broker", "provision", "journal.json")).exists()).toBe(false);
	});

	test("forwards SIGTERM to the foreground broker, reaps it, and propagates its exit code", async () => {
		const f = await fixture();
		await provision(f);
		const fake = await installFakeGjc(f, { waitForTerm: true });
		const child = await runHostChild(f);
		await waitForFile(fake.argsPath);
		const args = await readFile(fake.argsPath, "utf8");
		expect(args).toBe(`sdk\nbroker\nrun\n--agent-dir\n${f.agentDir}\n`);
		child.kill("SIGTERM");
		expect(await child.exited).toBe(23);
		expect(await readFile(fake.termPath, "utf8")).toBe("forwarded");
	});
});
