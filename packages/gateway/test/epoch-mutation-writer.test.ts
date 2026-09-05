import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import ts from "typescript";
import { GatewayDatabase } from "../src/store/db";
import { EPOCH_MUTATION_REASONS, type EpochMutationInput } from "../src/store/epoch-mutation";

test("every reason is recorded, reset semantics preserved, and caller rollback is atomic", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-epoch-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		database.putSession("origin", "session-before");
		database.incrementTurnCount("origin", "original-payload");
		database.updateActivity("origin", "original-payload");
		database.metaSet("broker_generation", "generation-7");
		for (const [index, reason] of EPOCH_MUTATION_REASONS.entries()) {
			expect(
				database.mutateEpoch("origin", {
					scope: "persona",
					reason,
					cause: { kind: "retirement", ref: "cause" },
					opRef: "op",
					actor: "actor",
				}),
			).toEqual({ fromEpoch: index, toEpoch: index + 1 });
		}
		const rows = database.listEpochMutations({ sinceMs: 0 });
		expect(rows.map((row) => row.reason)).toEqual([...EPOCH_MUTATION_REASONS]);
		expect(rows[0]).toMatchObject({
			fromSessionId: "session-before",
			brokerGeneration: "generation-7",
			causeRef: "cause",
			actor: "actor",
			opRef: "op",
		});
		expect(database.getSessionRecord("origin")).toEqual({ sessionId: "", epoch: 7 });
		expect(database.sessionIdentityRows()[0]!.origin_ref_json).toBe("original-payload");
		expect(database.incrementTurnCount("origin")).toBe(1);
		expect(() =>
			database.withTransaction(() => {
				database.mutateEpoch("origin", {
					scope: "persona",
					reason: "operator_new",
					cause: { kind: "operator" },
					originRefJson: "replacement",
				});
				throw new Error("rollback");
			}),
		).toThrow("rollback");
		expect(database.getSessionRecord("origin")!.epoch).toBe(7);
		expect(database.listEpochMutations({ sinceMs: 0 })).toHaveLength(7);
		expect(() =>
			database.mutateEpoch("origin", {
				scope: "persona",
				reason: "invalid",
				cause: { kind: "policy" },
			} as unknown as EpochMutationInput),
		).toThrow("unknown epoch mutation reason");
		database.mutateEpoch("origin", {
			scope: "persona",
			reason: "operator_new",
			cause: { kind: "operator" },
			originRefJson: "replacement",
		});
		expect(database.sessionIdentityRows()[0]!.origin_ref_json).toBe("replacement");
		expect(database.epochRotationSummary(0)).toMatchObject({
			last24h: 8,
			byScope: { persona: 8 },
			byReason: { operator_new: 2 },
		});
	} finally {
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("AST: mutateEpoch owns every epoch update and pruneTurnAttempts owns every attempt delete", async () => {
	const root = resolve(import.meta.dir, "../src");
	const writers: string[] = [];
	const deleters: string[] = [];
	for await (const path of new Bun.Glob("**/*.ts").scan({ cwd: root, absolute: true })) {
		const source = await Bun.file(path).text();
		expect(source).not.toMatch(/\b(?:bumpEpoch|rebindEpoch)\b/);
		const file = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true);
		const visit = (node: ts.Node, owner = "") => {
			if (ts.isMethodDeclaration(node)) owner = node.name.getText(file);
			if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node) || ts.isTemplateExpression(node)) {
				const sql = ts.isTemplateExpression(node)
					? node.head.text + node.templateSpans.map((span) => `?${span.literal.text}`).join("")
					: node.text;
				if (
					/(?:UPDATE\s+sessions\s+SET|INSERT\s+INTO\s+sessions\b[\s\S]*?DO\s+UPDATE\s+SET)[\s\S]*?\bepoch\s*=/i.test(
						sql.split(/\bWHERE\b/i)[0]!,
					)
				)
					writers.push(owner);
				if (/DELETE\s+FROM\s+turn_attempts\b/i.test(sql)) deleters.push(owner);
			}
			ts.forEachChild(node, (child) => visit(child, owner));
		};
		visit(file);
	}
	expect(writers).toEqual(["mutateEpoch"]);
	expect(deleters).toEqual(["pruneTurnAttempts"]);
});
