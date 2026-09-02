import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const root = join(import.meta.dir, "../../..");
const homes: string[] = [];
const children: ReturnType<typeof Bun.spawn>[] = [];

afterEach(async () => {
	for (const child of children.splice(0)) {
		child.kill("SIGKILL");
		await child.exited.catch(() => {});
	}
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function temporaryHome(): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-daemon-e2e-"));
	homes.push(home);
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	return home;
}

async function command(
	args: string[],
	home: string,
): Promise<{ readonly code: number; readonly stdout: string; readonly stderr: string }> {
	const child = Bun.spawn(["bun", ...args], {
		cwd: root,
		stdout: "pipe",
		stderr: "pipe",
		env: { ...process.env, GAJAEWAY_HOME: home },
	});
	const [stdout, stderr, code] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	return { code, stdout, stderr };
}

async function start(
	home: string,
): Promise<{ readonly child: ReturnType<typeof Bun.spawn>; readonly adminUrl: string }> {
	const child = Bun.spawn(["bun", "packages/gajaeway/test/daemon-entry.ts"], {
		cwd: root,
		stdout: "pipe",
		stderr: "pipe",
		env: { ...process.env, GAJAEWAY_HOME: home },
	});
	children.push(child);
	let stderr = "";
	const ready = new Promise<string>((resolve, reject) => {
		const reader = child.stderr.getReader();
		const decoder = new TextDecoder();
		void (async () => {
			try {
				while (true) {
					const { value, done } = await reader.read();
					if (done) break;
					stderr += decoder.decode(value, { stream: true });
					const match = stderr.match(/gajaeway daemon ready[^\n]* admin=(http:\/\/[^\s]+)/);
					if (match?.[1]) {
						resolve(match[1]);
						return;
					}
				}
				stderr += decoder.decode();
				reject(new Error(`daemon exited before ready: ${stderr}`));
			} catch (error) {
				reject(error);
			}
		})();
	});
	return { child, adminUrl: await within(ready, 10_000) };
}

async function within<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}

test("the daemon owns its lock, exposes status and admin, and shutdown cleans socket plus pidfile", async () => {
	const home = await temporaryHome();
	const daemon = await start(home);
	const socket = join(home, "gateway.sock");

	const status = await command(["packages/gajaeway/src/main.ts", "--socket", socket, "status"], home);
	expect(status.code).toBe(0);
	expect(JSON.parse(status.stdout)).toMatchObject({ pid: daemon.child.pid });

	// A second managed start would wait for the first to exit; --only-new is the
	// fail-closed form and must refuse at once without disturbing the resident.
	const second = await command(["packages/gajaeway/test/daemon-entry.ts", "--only-new"], home);
	expect(second.code).toBe(2);
	expect(second.stderr).toContain("Another gajaeway daemon is already running");
	const stillRunning = await command(["packages/gajaeway/src/main.ts", "--socket", socket, "status"], home);
	expect(stillRunning.code).toBe(0);

	const admin = await fetch(`${daemon.adminUrl}/api/status`);
	expect(admin.status).toBe(200);
	expect(await admin.json()).toMatchObject({ ok: true });

	const shutdown = await command(["packages/gajaeway/src/main.ts", "--socket", socket, "shutdown"], home);
	expect(shutdown.code).toBe(0);
	expect(await within(daemon.child.exited, 10_000)).toBe(0);
	expect(await Bun.file(socket).exists()).toBe(false);
	expect(await Bun.file(join(home, "gajaeway.pid")).exists()).toBe(false);
});

test("SIGTERM shuts the composite daemon down cleanly", async () => {
	const home = await temporaryHome();
	const daemon = await start(home);
	daemon.child.kill("SIGTERM");
	expect(await within(daemon.child.exited, 10_000)).toBe(0);
	expect(await Bun.file(join(home, "gateway.sock")).exists()).toBe(false);
	expect(await Bun.file(join(home, "gajaeway.pid")).exists()).toBe(false);
});
