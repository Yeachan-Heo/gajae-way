import { expect, test } from "bun:test";
import { BrokerCli, SESSION_ROWS_VERSION } from "../../src/broker/cli";

const runBrokerIntegration = Bun.env.GAJAEWAY_BROKER_CLI_INTEGRATION === "1" && Bun.which("gjc") !== null;

(runBrokerIntegration ? test : test.skip)("published gjc sdk session list satisfies the pinned row DTO", async () => {
	const rows = await new BrokerCli().listSessions();
	expect(rows.version).toBe(SESSION_ROWS_VERSION);
	expect(Array.isArray(rows.sessions)).toBe(true);
}, 20_000);
