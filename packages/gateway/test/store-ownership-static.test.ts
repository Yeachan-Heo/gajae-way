import { expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import * as ts from "typescript";
import { OWNER_FILE, type OwnedTable, TABLE_OWNER } from "../src/store/ownership";
import { classifySql } from "../src/store/sql-classify";

const sourceRoot = fileURLToPath(new URL("../src/", import.meta.url));
const rawMethods = new Set(["query", "exec", "prepare"]);

type SqlWrite = { readonly file: string; readonly table: string; readonly ownerFile?: string };

async function sourceFiles(directory: string): Promise<string[]> {
	const result: string[] = [];
	for (const entry of await readdir(directory, { withFileTypes: true })) {
		const path = resolve(directory, entry.name);
		if (entry.isDirectory()) result.push(...(await sourceFiles(path)));
		else if (entry.isFile() && path.endsWith(".ts")) result.push(path);
	}
	return result.sort();
}

function sourceSql(node: ts.Node): string | undefined {
	if (ts.isStringLiteralLike(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text;
	if (ts.isTemplateExpression(node))
		return node.head.text + node.templateSpans.map((span) => `__DYNAMIC__${span.literal.text}`).join("");
	return undefined;
}

function writeOwnerViolation(file: string, sql: string): SqlWrite | undefined {
	const fileRelative = file.replaceAll("\\", "/");
	if (fileRelative === "store/migrations.ts") return undefined;
	// broker-agent/agent-db.ts owns auth_credentials in the separate agent.db store, not the gateway store.
	// This exception allows the provisioning and re-provisioning flows to manage rotating OAuth credentials.
	if (fileRelative === "broker-agent/agent-db.ts" && sql.includes("auth_credentials")) return undefined;
	const classification = classifySql(sql);
	if (classification?.kind !== "write") {
		const writePrefix =
			/^\s*(?:INSERT\s|REPLACE\s|UPDATE\s|DELETE\s+FROM\b|WITH\s+(?:RECURSIVE\s+)?[A-Za-z_$][\w$]*\s+AS\b)/i;
		if (writePrefix.test(sql)) return { file: fileRelative, table: "<unclassified SQL write>" };
		return undefined;
	}
	const owner = TABLE_OWNER[classification.table as OwnedTable];
	const ownerFile = owner === undefined ? undefined : OWNER_FILE[owner];
	return ownerFile === fileRelative ? undefined : { file: fileRelative, table: classification.table, ownerFile };
}

function rawCallViolation(file: string, receiver: string, method: string): boolean {
	if (!rawMethods.has(method)) return false;
	if (file === "store/core.ts" || file === "broker-agent/agent-db.ts") return false;
	if (file.startsWith("store/groups/") && receiver === "this.#db" && method !== "exec") return false;
	if (receiver.startsWith("/")) return false;
	if (file === "orchestrator/rebind.ts" && receiver === "open") return false;
	if (file === "orchestrator/session-port.ts" && (receiver === "relay" || receiver === "input.relay")) return false;
	if (file === "server/server.ts" && receiver === "runtime.delivery") return false;
	return true;
}

function collectSourceFacts(file: string, text: string): { writes: SqlWrite[]; rawCalls: string[] } {
	const relativePath = relative(sourceRoot, file).replaceAll("\\", "/");
	const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
	const writes: SqlWrite[] = [];
	const rawCalls: string[] = [];
	const visit = (node: ts.Node): void => {
		const sql = sourceSql(node);
		if (sql !== undefined) {
			const violation = writeOwnerViolation(relativePath, sql);
			if (violation) writes.push(violation);
		}
		if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)) {
			const method = node.expression.name.text;
			const receiver = node.expression.expression.getText(source);
			if (rawCallViolation(relativePath, receiver, method))
				rawCalls.push(`${relativePath}: ${receiver}.${method}(...)`);
		}
		ts.forEachChild(node, visit);
	};
	visit(source);
	return { writes, rawCalls };
}

test("static SQL write literals stay in their table owner's file", async () => {
	const files = await sourceFiles(sourceRoot);
	const violations: SqlWrite[] = [];
	for (const file of files) {
		const text = await readFile(file, "utf8");
		violations.push(...collectSourceFacts(file, text).writes);
	}
	expect(violations).toEqual([]);
});

test("raw SQLite calls stay in core or broker-agent DB code; non-database APIs are exempt", async () => {
	const files = await sourceFiles(sourceRoot);
	const violations: string[] = [];
	for (const file of files) {
		const text = await readFile(file, "utf8");
		violations.push(...collectSourceFacts(file, text).rawCalls);
	}
	expect(violations).toEqual([]);
});

test("positive fixtures identify misplaced writes and raw SQLite access", () => {
	expect(writeOwnerViolation("store/groups/inbox.ts", "UPDATE deliveries SET state = 'confirmed'")).toEqual({
		file: "store/groups/inbox.ts",
		table: "deliveries",
		ownerFile: OWNER_FILE[TABLE_OWNER.deliveries],
	});
	expect(writeOwnerViolation("store/groups/inbox.ts", "DELETE FROM ")).toEqual({
		file: "store/groups/inbox.ts",
		table: "<unclassified SQL write>",
	});
	expect(rawCallViolation("ops/report.ts", "database", "query")).toBe(true);
	expect(rawCallViolation("store/core.ts", "this.#database", "query")).toBe(false);
	expect(rawCallViolation("store/groups/inbox.ts", "this.#db", "prepare")).toBe(false);
	expect(rawCallViolation("ops/report.ts", "connection", "exec")).toBe(true);
	expect(rawCallViolation("server/server.ts", "runtime.delivery", "prepare")).toBe(false);
	expect(rawCallViolation("orchestrator/session-port.ts", "input.relay", "query")).toBe(false);
	expect(rawCallViolation("orchestrator/rebind.ts", "open", "exec")).toBe(false);
});
