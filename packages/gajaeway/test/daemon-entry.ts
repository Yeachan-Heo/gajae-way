import { testOnlyBrokerDependencies } from "@gajaeway/gateway/test-broker";
import { isDaemonLockRefusal, runDaemon } from "../src/daemon";

try {
	const daemon = await runDaemon({
		home: process.env.GAJAEWAY_HOME,
		broker: testOnlyBrokerDependencies(),
		adminPort: 0,
	});
	await daemon.stopped;
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exitCode = isDaemonLockRefusal(error) ? 2 : 1;
}
