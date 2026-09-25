// A persona turn that asks for a stack restart and then keeps running, so the
// gateway restart is what ends it. Run in its own process group by the test.
import { join } from "node:path";
import { launchRestartStack } from "../../src/restart-stack";

const home = process.env.GAJAEWAY_HOME;
if (!home) throw new Error("fixture needs GAJAEWAY_HOME");
await launchRestartStack({
	home,
	platform: "darwin",
	uid: 501,
	worker: [process.execPath, join(import.meta.dir, "restart-stack-worker.ts"), "ops", "restart-stack"],
	env: { ...process.env, FIXTURE_CALLER_PGID: String(process.pid) },
});
console.log("launched");
await Bun.sleep(60_000);
