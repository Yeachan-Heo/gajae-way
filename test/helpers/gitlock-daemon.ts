import * as fs from "node:fs";
import {
	type ClosureHookContext,
	type ClosureRequest,
	type ClosureStep,
	createClosureExecutor,
} from "../../src/main-session/closure";
import { loadWayCore } from "../../src/native-loader";

interface DaemonFixture {
	readonly stateDir: string;
	readonly markerPath: string;
	readonly request: ClosureRequest;
	readonly stopAfter: ClosureStep;
}

const encoded = process.env.GITLOCK_DAEMON_FIXTURE;
if (!encoded) throw new Error("GITLOCK_DAEMON_FIXTURE is required");
const fixture = JSON.parse(encoded) as DaemonFixture;
const core = loadWayCore().WayCore.open(fixture.stateDir);
const executor = createClosureExecutor({
	core,
	hooks: {
		[fixture.stopAfter]: async (context: ClosureHookContext) => {
			fs.writeFileSync(
				fixture.markerPath,
				JSON.stringify({ childPid: context.childPid, childPgid: context.childPgid, leaseId: context.leaseId }),
			);
			await new Promise<void>(() => {
				// The parent drill SIGKILLs this daemon process at the durable boundary.
			});
		},
	},
});

try {
	await executor.execute(fixture.request);
} finally {
	await executor.shutdown();
}
