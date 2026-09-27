import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { autolinkCorpus } from "../src/memory/autolink";
import { MemoryClosureQueue } from "../src/memory/closure";
import { GatewayDatabase } from "../src/store/db";
import { initializeMemory, memoryGit } from "../src/memory/doctrine";

let home = "";

test("issue #341: autolink + intent through MemoryClosureQueue don't race", async () => {
	// #341 regression: autolinkCorpus writes/commits INSIDE lock to prevent races.
	// With fix: autolink and intent commits are serialized, each contains only its changes.
	// Without fix: intent's add --all picks up autolink's uncommitted changes, misattributing them.
	home = await mkdtemp(join(tmpdir(), "issue-341-"));
	const root = await initializeMemory(home);
	const dbPath = join(home, "gateway.db");
	const database = new GatewayDatabase(dbPath);
	const closure = new MemoryClosureQueue(database, home);

	// Set up: entity for autolink to link (use exact mention for autolink to find)
	await mkdir(join(root, "entities"), { recursive: true });
	await writeFile(join(root, "entities/myentity.md"), "# MyEntity\n\nReference.");
	await mkdir(join(root, "ops/rules"), { recursive: true });
	await writeFile(join(root, "ops/rules/rule.md"), "# Rule\n\nMyEntity is here.");
	await memoryGit(root, ["add", "-A"]);
	await memoryGit(root, ["commit", "-m", "setup"]);

	const intentId = "test-" + Date.now();
	const intentText = "Captured text";

	// Mark dirty files in parallel
	const beforeCommits = await memoryGit(root, ["log", "--format=%H"])
		.then((out) => out.split("\n").filter(Boolean));

	// Concurrent work: autolink links MyEntity in ops/rules/rule.md
	const autolinkPromise = autolinkCorpus(root, closure);

	const intentPromise = (async () => {
		const date = new Date().toISOString().slice(0, 10);
		await mkdir(join(root, "daily"), { recursive: true });
		await writeFile(
			join(root, `daily/${date}.md`),
			`\n## ${new Date().toISOString()}\n\n- intent-id: ${intentId}\n- user: ${intentText}\n- reply: resp\n`,
		);
		// Process through closure's coordinator
		return await closure.coordinateCommit(root, async () => {
			const writer = closure.getWriter(root);
			await memoryGit(root, ["add", "--all", "."]);
			return await writer.commit(`Memory mutation`, `Gajaeway-Mutation-Id: ${intentId}`);
		});
	})();

	await Promise.all([autolinkPromise, intentPromise]);

	const afterCommits = await memoryGit(root, ["log", "--format=%H"])
		.then((out) => out.split("\n").filter(Boolean));

	// Should have 3+ commits: setup, autolink, intent (plus maybe sync)
	expect(afterCommits.length).toBeGreaterThan(beforeCommits.length);

	// Find intent commit
	const intentCommits = await memoryGit(root, ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${intentId}`])
		.then((out) => out.split("\n").filter(Boolean));
	expect(intentCommits.length).toBe(1);

	// Intent commit should contain only daily file changes, not autolink's links
	const show = await memoryGit(root, ["show", intentCommits[0]]);
	expect(show).toContain(intentText); // intent content is there
	expect(show).not.toContain("[MyEntity]"); // autolink links NOT there (key test for #341)
});
