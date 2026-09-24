import { expect, test } from "bun:test";
import { violatesSessionArgvContract } from "../src/orchestrator/test-broker";

// The test broker must reject what the gjc 0.17.4 registry rejects (#258), so
// the e2e suites fail on the incompatible argv instead of pinning it.
test("the test broker refuses the argv gjc 0.17.4 rejects and accepts the leaf-bound spelling", () => {
	const rejected = [
		["sdk", "session", "--agent-dir", "/a", "list", "--scope", "all"],
		["sdk", "session", "--agent-dir=/a", "inspect", "s-1"],
		["sdk", "session", "inspect", "s-1", "--repo", "/r"],
		["sdk", "session", "send", "s-1", "--repo=/r", "--text", "hi"],
		["sdk", "session", "status", "s-1", "op-1", "--repo", "/r"],
		["sdk", "session", "raw", "query", "s-1", "--query", "turn.result", "--repo", "/r"],
	];
	const accepted = [
		["sdk", "session", "list", "--scope", "all", "--agent-dir", "/a"],
		["sdk", "session", "list", "--repo", "/r", "--agent-dir", "/a"],
		["sdk", "session", "tail", "s-1", "--repo", "/r", "--agent-dir", "/a"],
		["sdk", "session", "inspect", "s-1", "--agent-dir", "/a"],
		["sdk", "session", "raw", "query", "s-1", "--query", "turn.result", "--agent-dir", "/a"],
		["sdk", "serve", "--stdio", "--session", "s-1"],
	];
	for (const args of rejected) expect(violatesSessionArgvContract(args)).toBe(true);
	for (const args of accepted) expect(violatesSessionArgvContract(args)).toBe(false);
});
