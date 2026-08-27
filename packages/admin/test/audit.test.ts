import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { jsonlAuditLog, memoryAuditLog } from "../src/audit";
import type { AuditEntry } from "../src/gate";

const directories: string[] = [];

async function temp(): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), "admin-audit-"));
	directories.push(directory);
	return directory;
}

afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

function entry(overrides: Partial<AuditEntry> = {}): AuditEntry {
	return {
		at: "2026-08-27T14:02:31.000Z",
		operationId: "ops.backup",
		actor: "형님",
		decision: "allowed",
		...overrides,
	};
}

describe("memoryAuditLog", () => {
	test("returns the tail newest first", async () => {
		const log = memoryAuditLog();
		await log.append(entry({ actor: "a" }));
		await log.append(entry({ actor: "b" }));
		expect((await log.tail(10)).map((row) => row.actor)).toEqual(["b", "a"]);
	});

	test("honours the limit", async () => {
		const log = memoryAuditLog();
		for (const actor of ["a", "b", "c"]) await log.append(entry({ actor }));
		expect((await log.tail(2)).map((row) => row.actor)).toEqual(["c", "b"]);
	});
});

describe("jsonlAuditLog", () => {
	test("an absent file is an empty trail, not an error", async () => {
		const log = jsonlAuditLog(join(await temp(), "nested", "admin-audit.jsonl"));
		expect(await log.tail(10)).toEqual([]);
	});

	test("creates its directory and round-trips an entry", async () => {
		const path = join(await temp(), "nested", "admin-audit.jsonl");
		const log = jsonlAuditLog(path);
		await log.append(entry({ params: { path: "/tmp/backup" } }));
		expect(await log.tail(10)).toEqual([entry({ params: { path: "/tmp/backup" } })]);
	});

	test("concurrent appends never interleave partial lines", async () => {
		const path = join(await temp(), "admin-audit.jsonl");
		const log = jsonlAuditLog(path);
		await Promise.all(Array.from({ length: 25 }, (_, index) => log.append(entry({ actor: `actor-${index}` }))));
		const tail = await log.tail(100);
		expect(tail).toHaveLength(25);
		expect(new Set(tail.map((row) => row.actor)).size).toBe(25);
	});

	test("a truncated final line does not hide the rest of the trail", async () => {
		const path = join(await temp(), "admin-audit.jsonl");
		await writeFile(path, `${JSON.stringify(entry({ actor: "good" }))}\n{"at":"broken`, "utf8");
		const log = jsonlAuditLog(path);
		expect((await log.tail(10)).map((row) => row.actor)).toEqual(["good"]);
	});

	test("a rejected attempt is recorded with its reason: the trail is the point", async () => {
		const path = join(await temp(), "admin-audit.jsonl");
		const log = jsonlAuditLog(path);
		await log.append(entry({ decision: "rejected", reason: "confirmation did not match" }));
		expect((await log.tail(1))[0]).toMatchObject({ decision: "rejected", reason: "confirmation did not match" });
	});
});
