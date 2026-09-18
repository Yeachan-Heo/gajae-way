import { describe, expect, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AdapterAlreadyRunningError, AdapterLock, processIsAlive } from "../src/lock";
import { DISCORD_USAGE, parseDiscordArgs, USAGE_EXIT_CODE } from "../src/main";

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
			// Booting would have written the lock and then failed on the missing config.
			expect(await Bun.file(join(home, "adapter-discord.pid")).exists()).toBe(false);
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

describe("a second adapter instance refuses to boot", () => {
	async function withHome(body: (home: string) => Promise<void>): Promise<void> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-lock-"));
		try {
			await body(home);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}

	test("the first instance records its pid in the home", async () => {
		await withHome(async (home) => {
			const lock = await AdapterLock.acquire(home, { pid: 4242, alive: () => true });
			expect(lock.path).toBe(join(home, "adapter-discord.pid"));
			expect((await readFile(lock.path, "utf8")).trim()).toBe("4242");
		});
	});

	test("a second instance is refused while the holder is alive", async () => {
		await withHome(async (home) => {
			await AdapterLock.acquire(home, { pid: 4242, alive: () => true });
			const attempt = AdapterLock.acquire(home, { pid: 5353, alive: () => true });
			await expect(attempt).rejects.toBeInstanceOf(AdapterAlreadyRunningError);
			await expect(attempt).rejects.toThrow(/pid 4242/);
			// The refusal must not have stolen the lock from the live holder.
			expect((await readFile(join(home, "adapter-discord.pid"), "utf8")).trim()).toBe("4242");
		});
	});

	test("a pidfile from a dead process is reclaimed, not a permanent block", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "adapter-discord.pid"), "4242\n");
			const lock = await AdapterLock.acquire(home, { pid: 5353, alive: () => false });
			expect((await readFile(lock.path, "utf8")).trim()).toBe("5353");
		});
	});

	test("a winner slower than the reclaim window keeps its election", async () => {
		await withHome(async (home) => {
			const path = join(home, "adapter-discord.pid");
			await writeFile(path, "99999\n");
			let reachedPidfileWrite = 0;
			const results = await Promise.allSettled(
				Array.from({ length: 20 }, (_, i) =>
					AdapterLock.acquire(home, {
						pid: i + 1,
						alive: () => false,
						beforePidfileWrite: async () => {
							reachedPidfileWrite++;
							await Bun.sleep(1250);
						},
					}),
				),
			);
			const winners = results.filter((result) => result.status === "fulfilled");
			expect(winners).toHaveLength(1);
			// An election refreshed by its live owner is never reclaimable, so no
			// second contender is ever elected into the pidfile write.
			expect(reachedPidfileWrite).toBe(1);
			for (const result of results)
				if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
			const winner = winners[0]!.value;
			expect(await readFile(path, "utf8")).toBe(`${winner.pid}\n`);
			expect((await readdir(home)).filter((name) => name.includes(".reclaim.d") || name.endsWith(".dead"))).toEqual([]);
			await winner.release();
		});
	}, 10000);

	test("a displaced winner never overwrites the pidfile of the contender that replaced it", async () => {
		await withHome(async (home) => {
			const path = join(home, "adapter-discord.pid");
			const election = `${path}.reclaim.d`;
			await writeFile(path, "99999\n");
			let backdated = false;
			const results = await Promise.allSettled(
				Array.from({ length: 20 }, (_, i) =>
					AdapterLock.acquire(home, {
						pid: i + 1,
						alive: () => false,
						beforePidfileWrite: async () => {
							if (backdated) return;
							backdated = true;
							// Outrun this winner's own heartbeat so a waiting contender really
							// does reclaim the election mid-critical-section.
							for (let tick = 0; tick < 80; tick++) {
								const aged = new Date(Date.now() - 5000);
								await utimes(election, aged, aged).catch(() => {});
								await Bun.sleep(50);
							}
						},
					}),
				),
			);
			const winners = results.filter((result) => result.status === "fulfilled");
			expect(winners).toHaveLength(1);
			for (const result of results)
				if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
			const winner = winners[0]!.value;
			expect(await readFile(path, "utf8")).toBe(`${winner.pid}\n`);
			await winner.release();
		});
	}, 30000);

	test("an unparseable pidfile is stale, not a live holder", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "adapter-discord.pid"), "");
			const lock = await AdapterLock.acquire(home, {
				pid: 5353,
				alive: () => {
					throw new Error("liveness must not be consulted for an unreadable holder");
				},
			});
			expect((await readFile(lock.path, "utf8")).trim()).toBe("5353");
		});
	});

	test("release drops the lock, and only when this process still holds it", async () => {
		await withHome(async (home) => {
			const lock = await AdapterLock.acquire(home, { pid: 4242, alive: () => true });
			await lock.release();
			expect(await Bun.file(lock.path).exists()).toBe(false);
			const next = await AdapterLock.acquire(home, { pid: 5353, alive: () => true });
			await lock.release();
			expect(await Bun.file(next.path).exists()).toBe(true);
		});
	});

	test("liveness of this very process is observable, an absurd pid is not", () => {
		expect(processIsAlive(process.pid)).toBe(true);
		expect(processIsAlive(0x7ff_ffff)).toBe(false);
	});
});
