import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { loadConfig } from "../src/config";
import { runCutover } from "../src/ops/cutover";
import { Store } from "../src/store";
import { StoreCore } from "../src/store/core";

const homes: string[] = [];

afterEach(async () => {
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function fixture() {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-cutover-"));
	homes.push(root);
	const home = join(root, "home");
	const oldAgent = join(root, "old-agent");
	const targetAgent = join(root, "target-agent");
	await Promise.all([mkdir(home), mkdir(oldAgent), mkdir(targetAgent)]);
	await writeFile(
		join(home, "config.json"),
		JSON.stringify({ schemaVersion: 1, gjc: { agentDir: await realpath(targetAgent) } }),
	);
	const config = await loadConfig({ home });
	const layout = homeLayout(home);
	const database = await Store.open(layout.db, { canonicalAgentDir: await realpath(oldAgent) });
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "open-before-cutover",
			originKey: "discord:channel:cutover",
			originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "cutover" }),
			body: "pending inbound",
		}),
	);
	database.close();
	return { root, home, oldAgent, targetAgent, config, layout };
}

async function readSnapshot(path: string): Promise<Buffer> {
	return readFile(path);
}

test("cutover backs up, exports open work, then quarantines under one lease", async () => {
	const f = await fixture();
	const code = await runCutover({ config: f.config, agentDir: f.targetAgent });
	expect(code).toBe(0);
	const backups = await readdir(f.layout.backupsDir);
	const backupName = backups.find((name) => /^pre-cutover-.*\.db$/.test(name));
	expect(backupName).toBeDefined();
	const backup = join(f.layout.backupsDir, backupName as string);
	const exportPath = `${backup.slice(0, -3)}.open-work.json`;
	expect((await lstat(backup)).mode & 0o777).toBe(0o600);
	expect((await lstat(exportPath)).mode & 0o777).toBe(0o600);
	expect(await Bun.file(`${backup}-wal`).exists()).toBe(false);
	const backupDb = await Store.openReadonly(backup);
	try {
		expect(backupDb.core.quickCheckDetail()).toBe("ok");
		expect(backupDb.schemaVersion).toBe(32);
	} finally {
		backupDb.close();
	}
	const exported = JSON.parse(await readFile(exportPath, "utf8"));
	expect(exported).toMatchObject({
		kind: "gajaeway.open-work-export",
		version: 1,
		reason: "cutover",
		counts: { inbound: 1 },
	});
	expect(exported.inbound.map((row: { messageId: string }) => row.messageId)).toEqual(["open-before-cutover"]);
	const database = await Store.openReadonly(f.layout.db);
	try {
		expect(database.bindings.authority).toEqual({
			canonicalAgentDir: await realpath(f.targetAgent),
			identity: `gjc:${await realpath(f.targetAgent)}`,
		});
		expect(database.inspectBrokerAuthority().openInbound).toBe(0);
		expect(database.bindings.isQuarantined("inbound", "open-before-cutover")).toBe(true);
	} finally {
		database.close();
	}
	const second = await runCutover({ config: f.config, agentDir: f.targetAgent });
	expect(second).toBe(1);
});

test("cutover refuses config/argument mismatch and held gateway lease before backup", async () => {
	const f = await fixture();
	const before = await readSnapshot(f.layout.db);
	expect(await runCutover({ config: f.config, agentDir: f.oldAgent })).toBe(1);
	expect(await readSnapshot(f.layout.db)).toEqual(before);
	const lease = await acquireLease(f.home, "gateway");
	try {
		expect(await runCutover({ config: f.config, agentDir: f.targetAgent })).toBe(1);
	} finally {
		await lease.release();
	}
	expect(await readSnapshot(f.layout.db)).toEqual(before);
	expect(await Bun.file(f.layout.backupsDir).exists()).toBe(false);
});

test("export failure leaves the live database bytes and authority unchanged", async () => {
	const f = await fixture();
	const before = await readSnapshot(f.layout.db);
	const collect = StoreCore.prototype.collectOpenWork;
	StoreCore.prototype.collectOpenWork = () => {
		throw new Error("injected export failure");
	};
	try {
		expect(await runCutover({ config: f.config, agentDir: f.targetAgent })).toBe(1);
	} finally {
		StoreCore.prototype.collectOpenWork = collect;
	}
	expect(await readSnapshot(f.layout.db)).toEqual(before);
	const check = await Store.openReadonly(f.layout.db);
	try {
		expect(check.bindings.authority?.canonicalAgentDir).toBe(await realpath(f.oldAgent));
	} finally {
		check.close();
	}
});

test("a corrupt record is exported as invalid before settlement refuses it", async () => {
	const f = await fixture();
	const raw = new Database(f.layout.db);
	raw
		.query(
			"INSERT INTO lane_jobs(job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		)
		.run(
			"lanejob-cutover-invalid",
			"invalid-lane",
			"main",
			f.home,
			"running",
			"{broken",
			new Date().toISOString(),
			new Date().toISOString(),
		);
	raw.close();
	const beforeDatabase = await Store.openReadonly(f.layout.db);
	const beforeAuthority = beforeDatabase.bindings.authority;
	beforeDatabase.close();
	expect(await runCutover({ config: f.config, agentDir: f.targetAgent })).toBe(1);
	const files = await readdir(f.layout.backupsDir);
	const exportName = files.find((name) => name.endsWith(".open-work.json"));
	expect(exportName).toBeDefined();
	const exported = JSON.parse(await readFile(join(f.layout.backupsDir, exportName as string), "utf8"));
	expect(exported.work.invalid).toHaveLength(1);
	expect(exported.work.invalid[0]).toMatchObject({ jobId: "lanejob-cutover-invalid", recordJson: "{broken" });
	const check = await Store.openReadonly(f.layout.db);
	try {
		expect(check.bindings.authority).toEqual(beforeAuthority);
		expect(check.bindings.isQuarantined("work", "lanejob-cutover-invalid")).toBe(false);
	} finally {
		check.close();
	}
});

test("administrative cutover refuses a non-latest schema without migrating it", async () => {
	const f = await fixture();
	const raw = new Database(f.layout.db);
	raw.exec("DELETE FROM schema_migrations WHERE version = 32");
	raw.close();
	const before = await readSnapshot(f.layout.db);
	expect(await runCutover({ config: f.config, agentDir: f.targetAgent })).toBe(1);
	expect(await readSnapshot(f.layout.db)).toEqual(before);
	const readonly = await Store.openReadonly(f.layout.db);
	try {
		expect(readonly.schemaVersion).toBe(31);
	} finally {
		readonly.close();
	}
});
