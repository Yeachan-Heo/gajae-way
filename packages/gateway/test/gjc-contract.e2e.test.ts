import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { isSessionGoneCode, isVerifiedGjcVersion, VERIFIED_GJC_THROUGH } from "../src/orchestrator/gjc-contract";

/**
 * Contract check against the INSTALLED gjc. Run it before raising
 * VERIFIED_GJC_THROUGH and on every host after a gjc upgrade:
 *   GAJAEWAY_E2E_GJC=1 bun test packages/gateway/test/gjc-contract.e2e.test.ts
 * It issues only read-only verbs against ids that cannot exist.
 */
const liveTest = process.env.GAJAEWAY_E2E_GJC === "1" ? test : test.skip;

async function failureCode(client: GlobalGjcClient, args: string[]): Promise<unknown> {
	const result = await client.cli(args, { timeoutMs: 30_000 });
	const envelope = JSON.parse(result.stdout) as { ok?: unknown; error?: { code?: unknown } };
	return envelope.ok === false ? envelope.error?.code : undefined;
}

liveTest(
	"the installed gjc is inside the verified contract and reports a dropped session with a session-gone code",
	async () => {
		const client = new GlobalGjcClient();
		await client.preflight();
		const version = client.gjcVersion ?? "";
		expect({ version, verified: isVerifiedGjcVersion(version) }).toEqual({ version, verified: true });
		const missing = randomUUID();
		for (const args of [
			["sdk", "session", "inspect", missing],
			["sdk", "session", "status", missing, "gw-p-contract-probe"],
		]) {
			const code = await failureCode(client, args);
			expect({ args: args.slice(2, 3), code, gone: isSessionGoneCode(code) }).toEqual({
				args: args.slice(2, 3),
				code,
				gone: true,
			});
		}
	},
	120_000,
);

test("the verified ceiling is a major.minor pair", () => {
	expect(VERIFIED_GJC_THROUGH).toMatch(/^\d+\.\d+$/);
});
