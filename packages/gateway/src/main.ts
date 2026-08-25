import { bootGateway } from "./boot";

const [command, ...args] = process.argv.slice(2);
if (command !== "daemon") {
	console.error("usage: gajaeway-gateway daemon [--stdio]");
	process.exitCode = 2;
} else {
	const server = await bootGateway({ stdio: args.includes("--stdio") });
	const shutdown = () => void server.stop("signal received");
	process.once("SIGINT", shutdown);
	process.once("SIGTERM", shutdown);
}
