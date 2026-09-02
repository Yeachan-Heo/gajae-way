import { bootGateway } from "../src/boot";
import { testOnlyBrokerDependencies } from "../src/orchestrator/test-broker";

const server = await bootGateway({
	stdio: process.argv.includes("--stdio"),
	broker: testOnlyBrokerDependencies(),
});
const shutdown = () => void server.stop("signal received");
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
