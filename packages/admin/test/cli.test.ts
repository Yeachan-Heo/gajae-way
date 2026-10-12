import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type AddressInfo, type Server } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { encodeFrame, PROFILE_VERSION } from "@gajae-gateway/protocol";
import { ADMIN_USAGE, USAGE_EXIT_CODE, usageFor } from "../src/cli";



describe("admin usage guard", () => {
	test("an empty argv is a usage error, not an implicit boot", () => {
		expect(usageFor([])).toBe(ADMIN_USAGE);
	});

	test("an unknown subcommand is a usage error", () => {
		for (const args of [["bogus"], ["--help"], ["-h"], ["serve", "extra"], ["Serve"]])
			expect(usageFor(args)).toBe(ADMIN_USAGE);
	});

	test("serve is the one argv that boots the console", () => {
		expect(usageFor(["serve"])).toBeUndefined();
	});
});

describe("the binary exits on a usage error instead of blocking", () => {
	async function run(args: string[]): Promise<{ code: number; stderr: string }> {
		const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
			stdout: "pipe",
			stderr: "pipe",
			// A home with no socket: reaching the connect at all would be the bug.
			env: { ...process.env, GAJAEWAY_HOME: join(tmpdir(), "gajaeway-admin-usage-nonexistent") },
		});
		const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);
		return { code, stderr };
	}

	test("no arguments prints usage on stderr and exits non-zero", async () => {
		const result = await run([]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(ADMIN_USAGE);
		// The old entry reported the unreachable socket, i.e. it had already tried to connect.
		expect(result.stderr).not.toContain("Unable to connect to gateway socket");
	}, 30_000);

	test("an unknown subcommand prints usage on stderr and exits non-zero", async () => {
		const result = await run(["bogus"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(ADMIN_USAGE);
	}, 30_000);
});

test("serve waits for a gateway socket that appears after startup", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-admin-late-socket-"));
	const socketPath = join(home, "gateway.sock");
	let child: ReturnType<typeof Bun.spawn> | undefined;
	let socketServer: Server | undefined;
	let stderr = "";
	let childFinished = false;
	let stderrDone = Promise.resolve();

	try {
		const probe = createServer();
		await new Promise<void>((resolve, reject) => {
			probe.once("error", reject);
			probe.listen(0, "127.0.0.1", resolve);
		});
		const port = (probe.address() as AddressInfo).port;
		await new Promise<void>((resolve, reject) => probe.close((error) => (error ? reject(error) : resolve())));

		child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), "serve"], {
			stdout: "ignore",
			stderr: "pipe",
			env: { ...process.env, GAJAEWAY_HOME: home, GAJAEWAY_ADMIN_PORT: String(port) },
		});
		const stderrOutput = child.stderr;
		if (!(stderrOutput instanceof ReadableStream)) throw new Error("Expected the admin stderr pipe.");
		const stderrReader = stderrOutput.getReader();
		stderrDone = (async () => {
			for (;;) {
				const { value, done } = await stderrReader.read();
				if (done) return;
				stderr += new TextDecoder().decode(value, { stream: true });
			}
		})();


		const waitFor = async (predicate: () => boolean): Promise<void> => {
			const deadline = Date.now() + 5_000;
			while (!predicate() && Date.now() < deadline) await Bun.sleep(10);
			expect(predicate()).toBe(true);
		};

		await waitFor(() => stderr.includes("retry 1 in"));
		socketServer = createServer((socket) => {
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
			socketServer!.once("error", reject);
			socketServer!.listen(socketPath, resolve);
		});
		await waitFor(() => stderr.includes("gajaeway console on"));

		child.kill("SIGTERM");
		const code = await child.exited;
		childFinished = true;
		expect(code).toBe(0);
		await stderrDone;
		expect(stderr).toContain("Gateway socket");
		expect(stderr).toContain("gajaeway console on");
	} finally {
		if (child && !childFinished) {
			child.kill("SIGKILL");
			await child.exited;
		}
		await stderrDone;
		if (socketServer) await new Promise<void>((resolve) => socketServer!.close(() => resolve()));
		await rm(home, { recursive: true, force: true });
	}
}, 30_000);
