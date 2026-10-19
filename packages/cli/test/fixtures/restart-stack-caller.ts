// A persona turn that asks for a stack restart and then keeps running, so the
// gateway restart is what ends it. Run in its own process group by the test.
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { launchRestartStack } from "../../src/restart-stack";

const home = process.env.GAJAEWAY_HOME;
if (!home) throw new Error("fixture needs GAJAEWAY_HOME");

// Fail loudly if GAJAEWAY_HOME is not a temporary test directory.
// This prevents the fixture from accidentally touching real system files like ~/Library/LaunchAgents.
// Test directories are created by mkdtemp and will contain "gajaeway-restart" in their path.
if (!home.includes("gajaeway-restart")) {
	throw new Error(
		`fixture GAJAEWAY_HOME must be a test temp directory (should contain 'gajaeway-restart'), got: ${home}`,
	);
}

const launchAgentsDir = await mkdtemp(join(tmpdir(), "gajaeway-test-agents-"));
try {
	await mkdir(launchAgentsDir, { recursive: true });

	await launchRestartStack({
		home,
		platform: "darwin",
		uid: 501,
		launchAgentsDir,
		worker: [process.execPath, join(import.meta.dir, "restart-stack-worker.ts"), "ops", "restart-stack"],
		env: { ...process.env, FIXTURE_CALLER_PGID: String(process.pid) },
	});
	console.log("launched");
	await Bun.sleep(60_000);
} finally {
	await rm(launchAgentsDir, { recursive: true, force: true });
}
