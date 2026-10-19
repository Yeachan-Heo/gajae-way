// Supervisor stand-in for restart-stack.test.ts: the real runRestartStack with a
// service manager whose gateway restart kills the process group of the turn that
// asked for it, as a gateway restart kills its persona child.
import { appendFile } from "node:fs/promises";
import { join } from "node:path";
import { acquireLease, type Lease, type LeaseName } from "@gajae-gateway/home";
import { runRestartStack } from "../../src/restart-stack";
import { serviceSpecs } from "../../src/services";

const home = process.env.GAJAEWAY_HOME;
const launchAgentsDir = process.env.GAJAEWAY_LAUNCH_AGENTS_DIR;
const callerGroup = Number(process.env.FIXTURE_CALLER_PGID);
const id = process.argv[process.argv.indexOf("--run") + 1];
if (!home || !id || !Number.isInteger(callerGroup))
	throw new Error("fixture needs GAJAEWAY_HOME, caller pgid, --run id");
if (!launchAgentsDir)
	throw new Error("fixture needs GAJAEWAY_LAUNCH_AGENTS_DIR to prevent touching real system files");

const leases = new Map<LeaseName, Lease>();
let adminPid = process.pid + 1;
await runRestartStack({
	home,
	id,
	platform: "darwin",
	uid: 501,
	binDir: process.execPath.slice(0, process.execPath.lastIndexOf("/")),
	launchAgentsDir,
	pollMs: 10,
	runner: async (command) => {
		const label = command.at(-1)?.split("/").at(-1) ?? "";
		await appendFile(join(home, "ran.log"), `${label}\n`);
		if (label === "dev.gajaeway.gateway") {
			try {
				process.kill(-callerGroup, "SIGKILL");
			} catch {}
		}
		const spec = serviceSpecs().find((candidate) => candidate.label === label);
		if (spec?.id === "admin") adminPid++;
		const leaseName =
			spec?.id === "broker" || spec?.id === "gateway" || spec?.id.startsWith("adapter-")
				? (spec.id as LeaseName)
				: undefined;
		if (leaseName !== undefined) {
			await leases.get(leaseName)?.release();
			leases.set(leaseName, await acquireLease(home, leaseName));
		}
		return 0;
	},
	probe: async (label) => ({ pid: label === "dev.gajaeway.admin" ? adminPid : process.pid }),
	binaryModifiedAt: async () => 0,
	isServiceInstalled: async () => true,
});
