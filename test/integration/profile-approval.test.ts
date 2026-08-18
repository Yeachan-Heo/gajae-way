import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { loadWayCore } from "../../src/native-loader";
import { FileSdkDouble } from "../helpers/main-session";
import { RpcClient } from "../helpers/rpc-client";

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The listener is still becoming ready.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error("RPC socket did not become available.");
}

function profileContents(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`;
}

async function bootstrapNativeState(root: string): Promise<{
	stateDirectory: string;
	profilePath: string;
	state: GatewayStateStore;
	sdk: FileSdkDouble;
}> {
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	const state = new GatewayStateStore(loadWayCore().WayCore.open(stateDirectory));
	const sdk = new FileSdkDouble();
	await bootstrapMainSession({ confirm: true, profile: loadWayProfile(profilePath), state, sdk });
	return { stateDirectory, profilePath, state, sdk };
}

test("way profile approve --confirm updates a stopped daemon's bound projection", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-direct-"));
	try {
		const setup = await bootstrapNativeState(root);
		fs.writeFileSync(setup.profilePath, fs.readFileSync(setup.profilePath, "utf8").replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"'));
		const command = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "profile", "approve", "--confirm", "--state-dir", setup.stateDirectory, "--profile", setup.profilePath],
			cwd: process.cwd(),
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(command.exitCode).toBe(0);
		expect(new TextDecoder().decode(command.stdout)).toContain("receipt_id");
		expect(setup.state.read().bootstrapState).toBe("COMMITTED");
		expect(setup.state.read().profileDigest).toBe(loadWayProfile(setup.profilePath).digest.sha256);
		expect(loadWayCore().WayCore.open(setup.stateDirectory).journalRead(undefined, 10).events.map(event => event.kind)).toContain("profile_approved");
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
});

test("owner RPC approves profile drift during failed-closed linger", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-rpc-"));
	let server: ReturnType<typeof Bun.spawn> | undefined;
	let client: RpcClient | undefined;
	try {
		const setup = await bootstrapNativeState(root);
		fs.writeFileSync(setup.profilePath, fs.readFileSync(setup.profilePath, "utf8").replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"'));
		server = Bun.spawn(
			[
				"bun",
				"src/main.ts",
				"serve",
				"--state-dir",
				setup.stateDirectory,
				"--profile",
				setup.profilePath,
				"--fail-closed-linger-ms",
				"2000",
			],
			{ cwd: process.cwd(), stdout: "ignore", stderr: "pipe" },
		);
		client = await connectEventually(path.join(setup.stateDirectory, "rpc.sock"));
		for (let attempt = 0; attempt < 50; attempt += 1) {
			const health = await client.request("way.health");
			if ((health.result as { state?: string } | undefined)?.state === "failed_closed") break;
			await Bun.sleep(10);
		}
		const approval = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "profile", "approve", "--confirm", "--state-dir", setup.stateDirectory, "--profile", setup.profilePath],
			cwd: process.cwd(),
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(approval.exitCode).toBe(0);
		expect(new TextDecoder().decode(approval.stdout)).toContain("receipt_id");
		expect(await server.exited).toBe(78);
		expect(setup.state.read().bootstrapState).toBe("COMMITTED");
		const resumed = await strictResumeMainSession({ profile: loadWayProfile(setup.profilePath), state: setup.state, sdk: setup.sdk });
		await resumed.session.dispose();
	} finally {
		client?.close();
		if (server?.exitCode === null) server.kill("SIGTERM");
		if (server) await server.exited;
		fs.rmSync(root, { recursive: true, force: true });
	}
}, 10_000);
