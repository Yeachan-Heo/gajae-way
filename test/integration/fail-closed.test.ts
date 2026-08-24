import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ManagedProcessRegistry } from "../helpers/managed-process";
import { RpcClient } from "../helpers/rpc-client";

const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// Listener creation and socket permission setup are separate operations.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error("RPC socket did not become available.");
}

test("failed-closed linger serves unhealthy way.health then exits 78", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-failed-closed-"));
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profile = path.join(root, "profile.toml");
	fs.writeFileSync(
		profile,
		`[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
session_kind = "main"
`,
	);
	const child = managedProcesses.spawnDaemon({
		cmd: [
			"bun",
			"src/main.ts",
			"serve",
			"--state-dir",
			stateDirectory,
			"--profile",
			profile,
			"--fail-closed-linger-ms",
			"1000",
		],
		cwd: process.cwd(),
		stderr: "pipe",
	});
	let client: RpcClient | undefined;
	try {
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		let health: unknown;
		for (let attempt = 0; attempt < 50; attempt += 1) {
			const response = await client.request("way.health");
			health = response.result;
			if ((health as { state?: string } | undefined)?.state === "failed_closed") break;
			await Bun.sleep(10);
		}
		expect(health).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "bootstrap_required" });
		const exitCode = await child.exited;
		expect(exitCode).toBe(78);
		expect(JSON.parse(fs.readFileSync(path.join(stateDirectory, "health.json"), "utf8"))).toMatchObject({
			status: "unhealthy",
			state: "failed_closed",
			reason: "bootstrap_required",
		});
	} finally {
		client?.close();
		await managedProcesses.stopDaemon(child);
		fs.rmSync(root, { recursive: true, force: true });
	}
}, 10_000);
