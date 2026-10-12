import { expect, test } from "bun:test";
import { classifySql } from "../src/store/sql-classify";

test("classifies reads and target-table writes after stripping SQL comments", () => {
	expect(classifySql("-- leading comment\n SELECT '/* not a comment */' /* trailing comment */")).toEqual({ kind: "read" });
	expect(classifySql("VALUES (1), (2); -- trailing comment\n")).toEqual({ kind: "read" });
	expect(classifySql("INSERT INTO sessions VALUES (1) -- ignored tail text\n")).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("INSERT OR IGNORE INTO sessions VALUES (1)")).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("REPLACE INTO deliveries VALUES (1)")).toEqual({ kind: "write", table: "deliveries" });
	expect(classifySql("UPDATE OR FAIL sessions SET gjc_session_id = 'x'")).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("DELETE FROM sessions WHERE origin_key = 'x'")).toEqual({ kind: "write", table: "sessions" });
});

test("skips balanced WITH prefixes and retains the final statement's write target", () => {
	expect(
		classifySql(
			"WITH RECURSIVE seed(x) AS (VALUES (1)), next(x) AS (SELECT x + 1 FROM seed) INSERT OR REPLACE INTO main.sessions SELECT x, 's', 'now' FROM next ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id RETURNING origin_key",
		),
	).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("WITH rows AS (SELECT 1) DELETE FROM `deliveries` WHERE delivery_id IN (SELECT * FROM rows)")).toEqual({
		kind: "write",
		table: "deliveries",
	});
	expect(classifySql("WITH c AS (SELECT 1) SELECT * FROM c")).toEqual({ kind: "read" });
});

test("strips identifier quotes and accepts only the main schema qualifier", () => {
	expect(classifySql('UPDATE main."sessions" SET gjc_session_id = ?')).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("INSERT INTO [deliveries] VALUES (1)")).toEqual({ kind: "write", table: "deliveries" });
	expect(classifySql("DELETE FROM `sessions` WHERE origin_key = ?")).toEqual({ kind: "write", table: "sessions" });
	expect(classifySql("UPDATE aux.sessions SET gjc_session_id = ?")).toBeUndefined();
	expect(classifySql("INSERT INTO main.aux.sessions VALUES (1)")).toBeUndefined();
});

test("rejects statement tails and every command outside the allowlisted grammar", () => {
	for (const sql of [
		"SELECT 1; SELECT 2",
		"INSERT INTO sessions VALUES (1); UPDATE sessions SET gjc_session_id = 'x'",
		"PRAGMA integrity_check",
		"ATTACH DATABASE 'x' AS aux",
		"DETACH DATABASE aux",
		"CREATE TABLE t (id INTEGER)",
		"DROP TABLE sessions",
		"ALTER TABLE sessions ADD COLUMN x TEXT",
		"VACUUM",
		"BEGIN IMMEDIATE",
		"COMMIT",
		"SAVEPOINT nested",
		"REINDEX",
		"ANALYZE",
		"WITH c AS SELECT 1 SELECT * FROM c",
		"INSERT INTO",
		"/* unterminated",
	])
		expect(classifySql(sql)).toBeUndefined();
	expect(classifySql("SELECT 1; /* trailing comment */")).toEqual({ kind: "read" });
});
