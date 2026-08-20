import { expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";

const runIntegration = Bun.env.GAJAEWAY_SUPERVISOR_INTEGRATION === "1" && Bun.which("gjc") !== null;
const sessionId = Bun.env.GAJAEWAY_SUPERVISOR_SESSION_ID;
const workspace = Bun.env.GAJAEWAY_SUPERVISOR_WORKSPACE;

/**
 * Opt-in operator-run smoke test. It only discovers/verifies an existing
 * broker session; it deliberately sends no prompt and never creates/resumes a
 * session so it is safe against accidental rebirth.
 */
(runIntegration ? test : test.skip)("external HostSupervisor verifies an operator-run gjc session through the broker CLI", async () => {
	if (!sessionId || !workspace) throw new Error("GAJAEWAY_SUPERVISOR_SESSION_ID and GAJAEWAY_SUPERVISOR_WORKSPACE are required.");
	const supervisor = createExternalHostSupervisor({ broker: new BrokerCli(), workspace });
	try {
		const discovered = await supervisor.discover(sessionId);
		const verified = await supervisor.verify(discovered.identity);
		expect(verified.identity.sessionId).toBe(sessionId);
		expect(verified.identity.locator.repo).toBe(workspace);
	} finally {
		await supervisor.dispose();
	}
}, 180_000);
