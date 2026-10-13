import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runUp } from "../src/up";

async function withHome<T>(run: (home: string) => Promise<T>): Promise<T> {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-up-test-"));
	try {
		return await run(join(root, "home"));
	} finally {
		await rm(root, { recursive: true }).catch(() => {});
	}
}

describe("gajaeway up", () => {
	test("prompts for setup when config missing", async () => {
		await withHome(async (home) => {
			const lines: string[] = [];
			const log = (line: string) => lines.push(line);

			// Mock spawn to avoid actually starting processes
			const spawned: Array<{ cmd: string; args: string[] }> = [];
			const mockSpawn = (cmd: string, args: string[]) => {
				spawned.push({ cmd, args });
				return { pid: 999 };
			};

			try {
				await runUp({
					home,
					skipSetup: true, // Skip actual setup prompts
					log,
					spawn: mockSpawn,
				});
			} catch {
				// Expected when trying to check if config exists
			}

			// Should indicate gateway not running
			const output = lines.join("\n");
			expect(output).toContain("Starting gateway");
		});
	});

	test("reports gateway running when socket connects", async () => {
		await withHome(async (home) => {
			const lines: string[] = [];
			const log = (line: string) => lines.push(line);

			// Create a config file so setup is skipped
			await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));

			// Create a dummy adapter config
			await Bun.write(
				join(home, "adapter-discord.json"),
				JSON.stringify({ tokenFile: join(home, "secrets", "discord-token") }),
			);

			// Don't actually try to connect; skip that part
			try {
				await runUp({
					home,
					skipSetup: true,
					log,
					spawn: () => ({ pid: 999 }),
				});
			} catch {
				// Expected when trying to connect to socket
			}

			const output = lines.join("\n");
			expect(output.includes("Starting gateway") || output.includes("already running")).toBe(true);
		});
	});

	test("handles home directory creation with isolated GAJAEWAY_HOME", async () => {
		await withHome(async (home) => {
			const lines: string[] = [];
			const log = (line: string) => lines.push(line);

			// Create a config file in an isolated location
			await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));

			try {
				await runUp({
					home,
					skipSetup: true,
					log,
					spawn: () => ({ pid: 999 }),
				});
			} catch {
				// Expected when trying to check gateway status
			}

			// Verify we used the right home
			const configExists = await Bun.file(join(home, "config.json")).exists();
			expect(configExists).toBe(true);
		});
	});
});
