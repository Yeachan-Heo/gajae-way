import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LATEST_SCHEMA_VERSION, Store, TABLE_OWNER } from "../src/store";
import { classifySql } from "../src/store/sql-classify";

type SchemaState = { readonly name: string; readonly open: (path: string) => Promise<Store> };

// L4h will append its post-migration-35 state here after the host cutover makes that schema real.
const schemaStates: readonly SchemaState[] = [{ name: "fully migrated schema", open: (path) => Store.open(path) }];

type TriggerRow = { readonly name: string; readonly tbl_name: string; readonly sql: string };
type ForeignKeyRow = {
	readonly table: string;
	readonly on_delete: string;
	readonly on_update: string;
};

function splitTriggerBody(sql: string): string[] {
	const begin = sql.search(/\bBEGIN\b/i);
	const end = sql.toUpperCase().lastIndexOf("END");
	if (begin < 0 || end < begin + 5) throw new Error("trigger body is not bounded by BEGIN/END");
	const body = sql.slice(begin + 5, end);
	const statements: string[] = [];
	let start = 0;
	let quote = "";
	for (let index = 0; index < body.length; index++) {
		const char = body[index]!;
		if (quote) {
			if (char === quote) {
				if (body[index + 1] === quote) index++;
				else quote = "";
			}
			continue;
		}
		if (char === "'" || char === '"' || char === "`") {
			quote = char;
			continue;
		}
		if (char === "[") {
			const close = body.indexOf("]", index + 1);
			if (close < 0) throw new Error("unterminated trigger identifier");
			index = close;
			continue;
		}
		if (char === ";") {
			const statement = body.slice(start, index).trim();
			if (statement) statements.push(statement);
			start = index + 1;
		}
	}
	const trailing = body.slice(start).trim();
	if (trailing) statements.push(trailing);
	return statements;
}

for (const schema of schemaStates) {
	test(`${schema.name}: triggers and foreign keys preserve owner boundaries`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-store-schema-effects-"));
		const path = join(directory, "gateway.db");
		const store = await schema.open(path);
		const raw = new Database(path, { readonly: true });
		try {
			expect(store.schemaVersion).toBe(LATEST_SCHEMA_VERSION);
			const triggers = raw
				.query("SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")
				.all() as TriggerRow[];
			for (const trigger of triggers) {
				expect(trigger.sql).not.toMatch(/\bINSTEAD\s+OF\b/i);
				const owner = TABLE_OWNER[trigger.tbl_name as keyof typeof TABLE_OWNER];
				expect(owner).toBeDefined();
				const statements = splitTriggerBody(trigger.sql);
				for (const statement of statements) {
					const classification = classifySql(statement);
					expect(classification).toBeDefined();
					if (classification?.kind === "write") {
						expect(TABLE_OWNER[classification.table as keyof typeof TABLE_OWNER]).toBe(owner);
					} else {
						expect(statement).toMatch(/\bRAISE\s*\(/i);
					}
				}
			}

			for (const [table, owner] of Object.entries(TABLE_OWNER)) {
				const foreignKeys = raw.query(`PRAGMA foreign_key_list("${table}")`).all() as ForeignKeyRow[];
				for (const foreignKey of foreignKeys) {
					const targetOwner = TABLE_OWNER[foreignKey.table as keyof typeof TABLE_OWNER];
					expect(targetOwner).toBeDefined();
					if (owner === targetOwner) continue;
					for (const action of [foreignKey.on_delete, foreignKey.on_update])
						expect(["CASCADE", "SET NULL"]).not.toContain(action.toUpperCase());
				}
			}
		} finally {
			raw.close();
			store.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}
