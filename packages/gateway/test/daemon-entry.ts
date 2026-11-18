import { join } from "node:path";
import { installStructuredLogging } from "@gajae-gateway/log";
import { bootGateway } from "../src/boot";
import { gatewayHome } from "../src/config";
import { testOnlyBrokerDependencies } from "../src/orchestrator/test-broker";

const stdio = process.argv.includes("--stdio");
const disposeLogging = installStructuredLogging({
	path: join(gatewayHome(), "gateway.log"),
	stdio,
	heartbeatIntervalMs: 0, // Disable heartbeat for tests
});

try {
	const server = await bootGateway({
		stdio,
		broker: testOnlyBrokerDependencies(),
	});
	const shutdown = () => void server.stop("signal received");
	process.once("SIGINT", shutdown);
	process.once("SIGTERM", shutdown);
} finally {
	disposeLogging();
}
