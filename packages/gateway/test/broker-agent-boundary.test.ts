import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const AGENT_DB_SOURCE = resolve(import.meta.dir, "../src/broker-agent/agent-db.ts");

describe("broker-agent SQLite boundary", () => {
	test("agent-db is the only broker-agent SQLite module and has no store/config or gateway.db dependency", () => {
		const source = readFileSync(AGENT_DB_SOURCE, "utf8");
		expect(source).toContain('from "bun:sqlite"');
		expect(source).not.toMatch(/from ["'][^"']*(?:store|config)(?:\/|["'])/);
		expect(source).not.toContain("gateway.db");
	});
});
