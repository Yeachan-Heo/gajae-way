import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

const runbook = fs.readFileSync(path.join(import.meta.dir, "..", "..", "ops", "runbooks", "operations.md"), "utf8");

/**
 * The metrics endpoint's compensating control is operational, not technical:
 * loopback-only plus default-off plus an informed operator. If the disclosure is
 * missing, the control is missing, so the slice is incomplete rather than merely
 * undocumented.
 */
test("the operations runbook discloses the metrics endpoint exposure", () => {
	expect(runbook).toContain("[tunables.metrics].http_enabled");
	// The exposure must be stated plainly, not implied.
	expect(runbook).toMatch(/any local process running\s*\n?\s*as any user/u);
	expect(runbook).toContain("disabled by default");
	expect(runbook).toContain("unauthenticated");
});

test("the runbook records what the exposition withholds and the missing approval trail", () => {
	expect(runbook).toContain("holder identity");
	expect(runbook).toContain("no");
	// Enablement is an unsigned tunable: the runbook must not imply a ceremony.
	expect(runbook).toContain("profile approve");
	expect(runbook).toContain("multi-tenant");
});

test("the runbook documents the bind and surface refusals", () => {
	expect(runbook).toContain("non-loopback address");
	expect(runbook).toContain("refused");
	expect(runbook).toContain("GET /metrics");
	expect(runbook).toContain("404");
});
