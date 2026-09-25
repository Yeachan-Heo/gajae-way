// Supervisor stand-in for restart-stack.test.ts: the real runRestartStack with a
// service manager whose gateway restart kills the process group of the turn that
// asked for it, as a gateway restart kills its persona child.
import { appendFile } from "node:fs/promises";
import { join } from "node:path";
import { runRestartStack } from "../../src/restart-stack";

const home = process.env.GAJAEWAY_HOME;
const callerGroup = Number(process.env.FIXTURE_CALLER_PGID);
const id = process.argv[process.argv.indexOf("--run") + 1];
if (!home || !id || !Number.isInteger(callerGroup))
	throw new Error("fixture needs GAJAEWAY_HOME, caller pgid, --run id");

const started = new Map<string, number>();
await runRestartStack({
	home,
	id,
	platform: "darwin",
	uid: 501,
	pollMs: 10,
	runner: async (command) => {
		const label = command.at(-1)?.split("/").at(-1) ?? "";
		await appendFile(join(home, "ran.log"), `${label}\n`);
		if (label === "dev.gajaeway.gateway") {
			try {
				process.kill(-callerGroup, "SIGKILL");
			} catch {}
		}
		started.set(label, Date.now() + 1_000);
		return 0;
	},
	probe: async (label) => {
		const startedAt = started.get(label);
		return startedAt === undefined ? undefined : { pid: 4242, startedAt, binary: `/opt/bin/${label}` };
	},
	binaryModifiedAt: async () => 0,
});
