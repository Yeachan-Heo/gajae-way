import { join } from "node:path";
import { installStructuredLogging } from "@gajae-gateway/log";
import { bootGateway } from "../src/boot";
import { gatewayHome } from "../src/config";
import { testOnlyBrokerDependencies } from "../src/orchestrator/test-broker";

const stdio = process.argv.includes("--stdio");
const disposeLogging = installStructuredLogging({
	path: join(gatewayHome(), "gateway.log"),
	stdio,
	heartbeatIntervalMs: 0,
});

let server: Awaited<ReturnType<typeof bootGateway>>;
try {
	server = await bootGateway({
		stdio,
		broker: testOnlyBrokerDependencies(),
	});
} catch (error) {
	disposeLogging();
	throw error;
}
// The daemon keeps running after boot returns; the logger must stay installed so
// later info lines keep going to stderr instead of the stdout protocol stream.
const shutdown = () => void server.stop("signal received");
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
