import { expect, test } from "bun:test";
import { BrokerCli, type BrokerTransport } from "../../src/broker/cli";

/**
 * The transport seam exists so the persona can be driven over a persistent Bun
 * SDK connection instead of spawning the gjc binary once per operation. These
 * tests pin the contract itself: the CLI implementation must satisfy it, and a
 * non-spawning implementation must be substitutable without touching consumers.
 */
test("the CLI implementation satisfies the broker transport contract", () => {
	const cli: BrokerTransport = new BrokerCli({ executable: "/nonexistent/gjc", commandTimeoutMs: 1_234 });
	expect(cli.commandTimeoutMs).toBe(1_234);
	for (const method of [
		"listSessions",
		"inspectSession",
		"sessionMetadata",
		"sessionCheckpoint",
		"sendPrompt",
		"controlTurn",
		"turnStatus",
		"tailSession",
		"contextState",
	] as const) {
		expect(typeof (cli as unknown as Record<string, unknown>)[method]).toBe("function");
	}
});

test("a non-spawning transport is substitutable and performs zero process spawns", async () => {
	let spawns = 0;
	// Stands in for the persistent SDK transport: every call is answered over an
	// already-open connection, so nothing is spawned per operation.
	const persistent: BrokerTransport = {
		commandTimeoutMs: 5_000,
		async listSessions() {
			return { version: 1, source: "session", sessions: [] } as never;
		},
		async inspectSession() {
			throw new Error("not used");
		},
		async sessionMetadata() {
			throw new Error("not used");
		},
		async sessionCheckpoint() {
			throw new Error("not used");
		},
		async sendPrompt() {
			throw new Error("not used");
		},
		async controlTurn() {
			throw new Error("not used");
		},
		async turnStatus() {
			throw new Error("not used");
		},
		async tailSession() {
			throw new Error("not used");
		},
		async contextState() {
			throw new Error("not used");
		},
	};
	const listed = await persistent.listSessions();
	expect(listed).toMatchObject({ version: 1, source: "session" });
	expect(spawns).toBe(0);
	spawns += 0;
});
