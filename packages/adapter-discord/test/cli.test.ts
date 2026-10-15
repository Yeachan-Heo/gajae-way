import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:net";

import { tmpdir } from "node:os";
import { join } from "node:path";
import { leaseHeld } from "@gajae-gateway/home";
import { encodeFrame, PROFILE_VERSION } from "@gajae-gateway/protocol";

import { DISCORD_USAGE, parseDiscordArgs, USAGE_EXIT_CODE } from "../src/main";

async function startGateway(path: string): Promise<Server> {
	const server = createServer((socket) => {
		let negotiated = false;
		socket.on("data", () => {
			if (negotiated) return;
			negotiated = true;
			socket.write(
				encodeFrame({
					v: PROFILE_VERSION,
					type: "negotiated",
					payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
				}),
			);
		});
	});
	await new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(path, resolve);
	});
	return server;
}

async function closeGateway(server: Server): Promise<void> {
	await new Promise<void>((resolve) => server.close(() => resolve()));
}

describe("argv is resolved before any connection work", () => {
	test("no arguments still runs the adapter", () => {
		expect(parseDiscordArgs([])).toEqual({ kind: "run" });
	});

	test("--help and --version short-circuit", () => {
		expect(parseDiscordArgs(["--help"]).kind).toBe("help");
		expect(parseDiscordArgs(["-h"]).kind).toBe("help");
		expect(parseDiscordArgs(["--version"]).kind).toBe("version");
		expect(parseDiscordArgs(["-v"]).kind).toBe("version");
	});

	test("anything else is a usage error naming the offending argument", () => {
		const argv = parseDiscordArgs(["--boot-now"]);
		expect(argv.kind).toBe("usage");
		expect(argv.kind === "usage" && argv.message).toContain("--boot-now");
		expect(parseDiscordArgs(["--help", "extra"]).kind).toBe("usage");
	});
});

describe("the binary answers --help without booting an adapter", () => {
	async function run(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-cli-"));
		try {
			const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_HOME: home },
			});
			const [stdout, stderr, code] = await Promise.all([
				new Response(child.stdout).text(),
				new Response(child.stderr).text(),
				child.exited,
			]);
			// Usage-only invocation must not create an adapter lease.

			expect(await Bun.file(join(home, "leases", "adapter-discord")).exists()).toBe(false);

			return { code, stdout, stderr };
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}

	test("--help prints usage on stdout and exits 0", async () => {
		const result = await run(["--help"]);
		expect(result.code).toBe(0);
		expect(result.stdout).toContain(DISCORD_USAGE);
	}, 30_000);

	test("--version prints the package version and exits 0", async () => {
		const result = await run(["--version"]);
		expect(result.code).toBe(0);
		expect(result.stdout.trim()).toMatch(/^\d+\.\d+\.\d+/);
	}, 30_000);

	test("an unknown flag exits non-zero with usage on stderr", async () => {
		const result = await run(["--boot-now"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(DISCORD_USAGE);
	}, 30_000);
});

describe("startup failures terminate even with live recovery timers", () => {
	async function runLoginFixture(fail: boolean): Promise<{ code: number; stderr: string; timedOut: boolean }> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-login-"));
		const main = join(import.meta.dir, "../src/main.ts");
		const gatewaySocket = join(home, "gateway.sock");
		let gateway: Server | undefined;
		let killChild: (() => void) | undefined;
		try {
			gateway = await startGateway(gatewaySocket);
			await Bun.write(join(home, "token"), "fixture-not-a-real-token");
			await Bun.write(
				join(home, "adapter-discord.json"),
				JSON.stringify({ tokenFile: join(home, "token"), gatewaySocket }),
			);
			const preload = join(home, "login-fixture.ts");
			// Patch only this child process. No real Discord login or gateway exists in the fixture.
			await Bun.write(
				preload,
				`import { createRequire } from "node:module";
const { Client } = createRequire(${JSON.stringify(main)})("discord.js");
Client.prototype.login = async function () {
	setInterval(() => {}, 10000);
	${fail ? 'throw new Error("getaddrinfo ENOTFOUND discord.com (fixture)");' : 'console.log("fixture login succeeded"); return "fixture";'}
};`,
			);
			const child = Bun.spawn([process.execPath, "--preload", preload, main], {
				stdout: "pipe",
				stderr: "pipe",
				env: { PATH: process.env.PATH, HOME: home, GAJAEWAY_HOME: home },
			});
			killChild = () => child.kill();
			const stderr = new Response(child.stderr).text();
			const stdout = new Response(child.stdout).text();
			let timedOut = false;
			if (fail) {
				const outcome = await Promise.race([child.exited, Bun.sleep(3000).then(() => "timeout" as const)]);
				timedOut = outcome === "timeout";
				if (timedOut) child.kill();
			} else {
				// Successful startup must stay resident, then release its lock on SIGTERM.
				let loginObserved = false;
				for (let attempt = 0; attempt < 100; attempt++) {
					const log = Bun.file(join(home, "adapter-discord.log"));
					if ((await log.exists()) && (await log.text()).includes("fixture login succeeded")) {
						loginObserved = true;
						break;
					}
					await Bun.sleep(20);
				}
				expect(loginObserved).toBe(true);
				expect(child.exitCode).toBeNull();
				child.kill("SIGTERM");
			}
			const [code, errorText] = await Promise.all([child.exited, stderr]);
			await stdout;
			if (!fail) {
				expect(await leaseHeld(home, "adapter-discord")).toBe(false);
			}
			return { code, stderr: errorText, timedOut };
		} finally {
			killChild?.();
			if (gateway) await closeGateway(gateway);
			await rm(home, { recursive: true, force: true });
		}
	}

	test("DNS login failure exits nonzero instead of keeping recovery timers alive", async () => {
		const result = await runLoginFixture(true);
		expect(result.stderr).toContain("getaddrinfo ENOTFOUND discord.com (fixture)");
		expect(result.timedOut).toBe(false);
		expect(result.code).toBe(1);
	}, 10000);

	test("successful login remains supervised and SIGTERM releases the lock", async () => {
		const result = await runLoginFixture(false);
		expect(result.code).toBe(0);
	}, 10000);
});
