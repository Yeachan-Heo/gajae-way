import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout } from "@gajae-gateway/home";
import { assertAgentDb, credentialFingerprint } from "../src/broker-agent/agent-db";
import {
	BROKER_AGENT_ALLOWLIST,
	resolveBrokerAgentDataSource,
	resolveBrokerAgentSource,
} from "../src/broker-agent/allowlist";
import { brokerAgentDrift } from "../src/broker-agent/drift";
import { provisionBrokerAgent, recoverProvision } from "../src/broker-agent/provision";

interface Fixture {
	readonly root: string;
	readonly home: string;
	readonly source: string;
	readonly agentDir: string;
}

const roots: string[] = [];
const processes: Array<ReturnType<typeof Bun.spawn>> = [];
afterEach(async () => {
	for (const process of processes.splice(0)) {
		if (process.exitCode === null) process.kill("SIGKILL");
		await process.exited;
	}
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture(): Promise<Fixture> {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-agent-provision-")));
	roots.push(root);
	const home = join(root, "home");
	const source = join(root, "operator-agent");
	const agentDir = homeLayout(home).brokerAgentDir;
	await mkdir(home, { recursive: true, mode: 0o700 });
	await mkdir(source, { recursive: true });
	const database = new Database(join(source, "agent.db"));
	database.exec(
		"CREATE TABLE auth_credentials (provider TEXT NOT NULL, token TEXT NOT NULL); CREATE TABLE model_usage (model TEXT NOT NULL, count INTEGER NOT NULL); INSERT INTO auth_credentials VALUES ('provider-a', 'source-token'); INSERT INTO model_usage VALUES ('model-a', 1);",
	);
	database.close();
	await writeFile(join(source, "models.yml"), "model: source-model\n");
	await writeFile(join(source, "config.yml"), "model: source-config\n");
	const presets = join(source, "model-presets");
	await mkdir(presets);
	await writeFile(join(presets, "state.json"), '{"source":true}\n');
	await writeFile(join(presets, "state.backup.json"), '{"backup":true}\n');
	await writeFile(join(presets, "control.json"), '{"control":true}\n');
	return { root, home, source, agentDir };
}

async function provision(f: Fixture, mode: "force" | "update" = "force", source = f.source) {
	const lease = await acquireLease(f.home, "broker");
	try {
		return await provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source, mode, lease });
	} finally {
		await lease.release();
	}
}

async function putCredential(path: string, token: string): Promise<void> {
	const database = new Database(path);
	database.query("UPDATE auth_credentials SET token = ? WHERE provider = 'provider-a'").run(token);
	database.close();
}

async function insertModelUsage(path: string, model: string): Promise<void> {
	const database = new Database(path);
	database.query("INSERT INTO model_usage VALUES (?, 1)").run(model);
	database.close();
}

async function treeDigest(root: string, excluded = new Set<string>()): Promise<string> {
	const files: Array<[string, string]> = [];
	const walk = async (directory: string, prefix = "") => {
		for (const name of (await readdir(directory)).sort()) {
			const path = join(directory, name);
			const relative = prefix ? `${prefix}/${name}` : name;
			const info = await lstat(path);
			if (info.isDirectory()) await walk(path, relative);
			else if (!excluded.has(relative))
				files.push([
					relative,
					createHash("sha256")
						.update(await readFile(path))
						.digest("hex"),
				]);
		}
	};
	await walk(root);
	return JSON.stringify(files);
}

async function fileDigest(path: string): Promise<string | undefined> {
	try {
		return createHash("sha256")
			.update(await readFile(path))
			.digest("hex");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

async function fileSize(path: string): Promise<number | undefined> {
	try {
		return (await lstat(path)).size;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

async function waitForFile(path: string): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		if (await Bun.file(path).exists()) return;
		await Bun.sleep(10);
	}
	throw new Error(`timed out waiting for ${path}`);
}

async function runProvisionCrash(f: Fixture, marker: string, point: string): Promise<ReturnType<typeof Bun.spawn>> {
	const provisionUrl = new URL("../src/broker-agent/provision.ts", import.meta.url).href;
	const homeUrl = new URL("../../home/src/index.ts", import.meta.url).href;
	const script = `
		import { writeFileSync } from "node:fs";
		import { provisionBrokerAgent } from ${JSON.stringify(provisionUrl)};
		import { acquireLease } from ${JSON.stringify(homeUrl)};
		const lease = await acquireLease(${JSON.stringify(f.home)}, "broker");
		await provisionBrokerAgent({
			home: ${JSON.stringify(f.home)},
			agentDir: ${JSON.stringify(f.agentDir)},
			source: ${JSON.stringify(f.source)},
			mode: "force",
			lease,
			fault: (at) => {
				if (at === ${JSON.stringify(point)}) {
					writeFileSync(${JSON.stringify(marker)}, at);
					Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
				}
			},
		});
		await lease.release();
	`;
	const child = Bun.spawn([process.execPath, "--eval", script], {
		cwd: join(import.meta.dir, "../../.."),
		stdout: "pipe",
		stderr: "pipe",
		stdin: "ignore",
	});
	processes.push(child);
	await waitForFile(marker);
	return child;
}

describe("broker agent allowlist provisioning", () => {
	test("the exact allowlist is copied without modifying source or broker-local files", async () => {
		const f = await fixture();
		expect(BROKER_AGENT_ALLOWLIST.map(({ name, kind, required, form }) => ({ name, kind, required, form }))).toEqual([
			{ name: "agent.db", kind: "credentials", required: true, form: "sqlite" },
			{ name: "models.yml", kind: "model-config", required: false, form: "file" },
			{ name: "config.yml", kind: "model-config", required: false, form: "file" },
			{ name: "model-presets", kind: "model-presets", required: false, form: "group" },
		]);
		for (const [relative, contents] of [
			["sdk/broker.json", "broker-local-discovery"],
			["sessions/session.json", "broker-local-session"],
			["history.db", "broker-local-history"],
			["models.db", "broker-local-models"],
			["blobs/blob", "broker-local-blob"],
			["memories/note", "broker-local-memory"],
			["logs/run.log", "broker-local-log"],
			["secrets.yml", "broker-local-secret"],
			["not-allowlisted.yml", "source-only"],
		] as const) {
			const path = join(f.source, relative);
			await mkdir(join(path, ".."), { recursive: true });
			await writeFile(path, contents);
		}
		await mkdir(f.agentDir, { recursive: true, mode: 0o700 });
		await mkdir(join(f.agentDir, "sdk"), { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "sdk", "broker.json"), "broker-local-discovery");
		await mkdir(join(f.agentDir, "sessions"), { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "sessions", "session.json"), "broker-local-session");
		await writeFile(join(f.agentDir, "history.db"), "broker-local-history");
		await writeFile(join(f.agentDir, "models.db"), "broker-local-models");
		await mkdir(join(f.agentDir, "blobs"), { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "blobs", "blob"), "broker-local-blob");
		await mkdir(join(f.agentDir, "memories"), { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "memories", "note"), "broker-local-memory");
		await mkdir(join(f.agentDir, "logs"), { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "logs", "run.log"), "broker-local-log");
		await writeFile(join(f.agentDir, "secrets.yml"), "broker-local-secret");
		const sourceBefore = await treeDigest(f.source);
		const sourceDbBefore = await lstat(join(f.source, "agent.db"));
		const result = await provision(f);
		const sourceAfter = await treeDigest(f.source);
		const sourceDbAfter = await lstat(join(f.source, "agent.db"));
		expect(result.status).toBe("ok");
		expect(sourceAfter).toBe(sourceBefore);
		expect(sourceDbAfter.ino).toBe(sourceDbBefore.ino);
		expect(sourceDbAfter.mode & 0o777).toBe(sourceDbBefore.mode & 0o777);
		expect(await readFile(join(f.agentDir, "sdk", "broker.json"), "utf8")).toBe("broker-local-discovery");
		expect(await readFile(join(f.agentDir, "sessions", "session.json"), "utf8")).toBe("broker-local-session");
		expect(await readFile(join(f.agentDir, "history.db"), "utf8")).toBe("broker-local-history");
		expect(await readFile(join(f.agentDir, "models.db"), "utf8")).toBe("broker-local-models");
		expect(await readFile(join(f.agentDir, "secrets.yml"), "utf8")).toBe("broker-local-secret");
		expect(await Bun.file(join(f.agentDir, "not-allowlisted.yml")).exists()).toBe(false);
		const dirInfo = await lstat(f.agentDir);
		expect(dirInfo.isSymbolicLink()).toBe(false);
		expect(dirInfo.mode & 0o777).toBe(0o700);
		for (const path of [
			join(f.agentDir, "agent.db"),
			join(f.agentDir, "models.yml"),
			join(f.agentDir, "config.yml"),
			join(f.agentDir, "model-presets", "state.json"),
		]) {
			const [copy, source] = await Promise.all([lstat(path), lstat(path.replace(f.agentDir, f.source))]);
			expect(copy.isSymbolicLink()).toBe(false);
			expect(copy.mode & 0o777).toBe(0o600);
			expect(copy.nlink).toBe(1);
			expect(copy.ino).not.toBe(source.ino);
		}
		expect((await lstat(join(f.agentDir, "model-presets"))).mode & 0o777).toBe(0o700);
		await assertAgentDb(join(f.agentDir, "agent.db"));
		expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(
			await credentialFingerprint(join(f.source, "agent.db")),
		);
	});

	test("VACUUM INTO snapshots committed WAL credentials without changing the live source tree", async () => {
		const f = await fixture();
		const writerReady = join(f.root, "writer.ready");
		const writerScript = `
			import { writeFileSync } from "node:fs";
			import { Database } from "bun:sqlite";
			const db = new Database(${JSON.stringify(join(f.source, "agent.db"))});
			db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
			db.query("INSERT INTO auth_credentials VALUES ('provider-b', 'committed-wal-token')").run();
			writeFileSync(${JSON.stringify(writerReady)}, "ready");
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
		`;
		const writer = Bun.spawn([process.execPath, "--eval", writerScript], {
			stdout: "pipe",
			stderr: "pipe",
			stdin: "ignore",
		});
		processes.push(writer);
		await waitForFile(writerReady);
		const sidecars = new Set(["agent.db", "agent.db-wal", "agent.db-shm"]);
		const before = await treeDigest(f.source, sidecars);
		const dbBefore = await fileDigest(join(f.source, "agent.db"));
		const walBefore = await fileDigest(join(f.source, "agent.db-wal"));
		const shmBefore = await fileSize(join(f.source, "agent.db-shm"));
		const result = await provision(f);
		const after = await treeDigest(f.source, sidecars);
		expect(result.status).toBe("ok");
		expect(after).toBe(before);
		expect(await fileDigest(join(f.source, "agent.db"))).toBe(dbBefore);
		expect(await fileDigest(join(f.source, "agent.db-wal"))).toBe(walBefore);
		// SQLite readers may update ephemeral WAL read-marks in an existing shm file.
		expect(shmBefore).toBeDefined();
		expect(await fileSize(join(f.source, "agent.db-shm"))).toBe(shmBefore);
		const copied = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			expect(copied.query("SELECT token FROM auth_credentials WHERE provider = 'provider-b'").get()).toEqual({
				token: "committed-wal-token",
			});
			expect(copied.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
		} finally {
			copied.close();
		}
	});
	test("a WAL-only source does not acquire a new shared-memory sidecar", async () => {
		const f = await fixture();
		const writerReady = join(f.root, "wal-writer.ready");
		const writerScript = `
			import { writeFileSync } from "node:fs";
			import { Database } from "bun:sqlite";
			const db = new Database(${JSON.stringify(join(f.source, "agent.db"))});
			db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
			db.query("INSERT INTO auth_credentials VALUES ('provider-b', 'committed-wal-token')").run();
			writeFileSync(${JSON.stringify(writerReady)}, "ready");
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
		`;
		const writer = Bun.spawn([process.execPath, "--eval", writerScript], {
			stdout: "pipe",
			stderr: "pipe",
			stdin: "ignore",
		});
		processes.push(writer);
		await waitForFile(writerReady);
		writer.kill("SIGKILL");
		await writer.exited;
		await rm(join(f.source, "agent.db-shm"), { force: true });
		const sidecars = new Set(["agent.db", "agent.db-wal", "agent.db-shm"]);
		expect(await fileDigest(join(f.source, "agent.db-wal"))).toBeDefined();
		const dbBefore = await fileDigest(join(f.source, "agent.db"));
		const walBefore = await fileDigest(join(f.source, "agent.db-wal"));
		const restBefore = await treeDigest(f.source, sidecars);
		expect(await fileSize(join(f.source, "agent.db-shm"))).toBeUndefined();
		await provision(f);
		expect(await fileDigest(join(f.source, "agent.db"))).toBe(dbBefore);
		expect(await fileDigest(join(f.source, "agent.db-wal"))).toBe(walBefore);
		expect(await treeDigest(f.source, sidecars)).toBe(restBefore);
		expect(await fileSize(join(f.source, "agent.db-shm"))).toBeDefined();
		const copied = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			expect(copied.query("SELECT token FROM auth_credentials WHERE provider = 'provider-b'").get()).toEqual({
				token: "committed-wal-token",
			});
		} finally {
			copied.close();
		}
	});

	test("update detects committed WAL-only credential changes and snapshots them without checkpointing", async () => {
		const f = await fixture();
		await provision(f);
		const writerReady = join(f.root, "update-writer.ready");
		const writerScript = `
			import { writeFileSync } from "node:fs";
			import { Database } from "bun:sqlite";
			const db = new Database(${JSON.stringify(join(f.source, "agent.db"))});
			db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
			db.query("UPDATE auth_credentials SET token = 'wal-only-source-token' WHERE provider = 'provider-a'").run();
			writeFileSync(${JSON.stringify(writerReady)}, "ready");
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
		`;
		const writer = Bun.spawn([process.execPath, "--eval", writerScript], {
			stdout: "pipe",
			stderr: "pipe",
			stdin: "ignore",
		});
		processes.push(writer);
		await waitForFile(writerReady);
		const databasePath = join(f.source, "agent.db");
		const walPath = `${databasePath}-wal`;
		const databaseBefore = await fileDigest(databasePath);
		const walBefore = await fileDigest(walPath);
		const shmSizeBefore = await fileSize(`${databasePath}-shm`);
		const drift = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(drift.entries.find((entry) => entry.name === "agent.db")?.state).toBe("source_changed");
		expect((await provision(f, "update")).status).toBe("ok");
		const copy = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			expect(copy.query("SELECT token FROM auth_credentials WHERE provider = 'provider-a'").get()).toEqual({
				token: "wal-only-source-token",
			});
			expect(copy.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
		} finally {
			copy.close();
		}
		expect(await fileDigest(databasePath)).toBe(databaseBefore);
		expect(await fileDigest(walPath)).toBe(walBefore);
		expect(await fileSize(`${databasePath}-shm`)).toBe(shmSizeBefore);
	});
	test("update preserves broker-refreshed credentials and only copies changed or missing entries", async () => {
		const f = await fixture();
		await provision(f);
		const sourceFingerprint = await credentialFingerprint(join(f.source, "agent.db"));
		await insertModelUsage(join(f.source, "agent.db"), "model-churn-only");
		expect(await credentialFingerprint(join(f.source, "agent.db"))).toBe(sourceFingerprint);
		expect((await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source })).entries[0]?.state).toBe(
			"in_sync",
		);
		await putCredential(join(f.agentDir, "agent.db"), "broker-refreshed-token");
		await writeFile(join(f.source, "models.yml"), "model: changed-source\n");
		const drift = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(drift.entries.find((entry) => entry.name === "agent.db")?.state).toBe("copy_changed");
		expect(drift.entries.find((entry) => entry.name === "models.yml")?.state).toBe("source_changed");
		const result = await provision(f, "update");
		expect(result.status).toBe("ok");
		const refreshed = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			expect(refreshed.query("SELECT token FROM auth_credentials WHERE provider = 'provider-a'").get()).toEqual({
				token: "broker-refreshed-token",
			});
		} finally {
			refreshed.close();
		}
		expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: changed-source\n");
		await rm(join(f.agentDir, "config.yml"));
		const missing = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(missing.entries.find((entry) => entry.name === "config.yml")?.state).toBe("missing_copy");
		await provision(f, "update");
		expect(await readFile(join(f.agentDir, "config.yml"), "utf8")).toBe("model: source-config\n");
	});

	test("force replaces changed copies while update keeps diverged or source-absent optional files", async () => {
		const f = await fixture();
		await provision(f);
		await writeFile(join(f.source, "models.yml"), "model: source-new\n");
		await writeFile(join(f.agentDir, "models.yml"), "model: broker-new\n");
		const diverged = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(diverged.entries.find((entry) => entry.name === "models.yml")?.state).toBe("diverged");
		await provision(f, "update");
		expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: broker-new\n");
		await provision(f, "force");
		expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: source-new\n");
		await rm(join(f.source, "config.yml"));
		const missingSource = await brokerAgentDrift({ home: f.home, agentDir: f.agentDir, source: f.source });
		expect(missingSource.entries.find((entry) => entry.name === "config.yml")?.state).toBe("missing_source");
		await provision(f, "update");
		expect(await readFile(join(f.agentDir, "config.yml"), "utf8")).toBe("model: source-config\n");
	});

	test("refuses unsafe inputs, live brokers, and unheld leases before replacing target content", async () => {
		const f = await fixture();
		const missing = join(f.root, "missing-source");
		await mkdir(missing);
		const lease = await acquireLease(f.home, "broker");
		try {
			await expect(
				provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source: missing, mode: "force", lease }),
			).rejects.toMatchObject({
				code: "agent_source_missing",
			});
			expect(await Bun.file(f.agentDir).exists()).toBe(false);
			await expect(
				provisionBrokerAgent({
					home: f.home,
					agentDir: f.agentDir,
					source: f.source,
					mode: "force",
					lease: {} as never,
				}),
			).rejects.toMatchObject({ code: "broker_lease_required" });
		} finally {
			await lease.release();
		}
		await writeFile(join(f.source, "config.yml"), "auth:\n  broker:\n    url: ws://127.0.0.1:9000\n");
		await expect(provision(f)).rejects.toMatchObject({ code: "agent_source_live_link" });
		expect(await Bun.file(f.agentDir).exists()).toBe(false);
	});
	test("a malformed provision manifest refuses before replacing the committed target", async () => {
		const f = await fixture();
		await provision(f);
		const oldFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
		await putCredential(join(f.source, "agent.db"), "new-source-token");
		await writeFile(homeLayout(f.home).brokerProvisionManifest, "{}\n", { mode: 0o600 });
		const lease = await acquireLease(f.home, "broker");
		try {
			await expect(
				provisionBrokerAgent({ home: f.home, agentDir: f.agentDir, source: f.source, mode: "force", lease }),
			).rejects.toMatchObject({ code: "broker_agent_unrecoverable" });
		} finally {
			await lease.release();
		}
		expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(oldFingerprint);
	});
	test("refuses a source that is the target or is nested inside the gateway home without changing it", async () => {
		const same = await fixture();
		const sameBefore = await treeDigest(same.source);
		const sameLease = await acquireLease(same.home, "broker");
		try {
			await expect(
				provisionBrokerAgent({
					home: same.home,
					agentDir: same.source,
					source: same.source,
					mode: "force",
					lease: sameLease,
				}),
			).rejects.toMatchObject({ code: "agent_source_live_link" });
		} finally {
			await sameLease.release();
		}
		expect(await treeDigest(same.source)).toBe(sameBefore);

		const nested = await fixture();
		const nestedSource = join(nested.home, "operator-agent");
		await mkdir(nestedSource);
		const nestedLease = await acquireLease(nested.home, "broker");
		try {
			await expect(
				provisionBrokerAgent({
					home: nested.home,
					agentDir: nested.agentDir,
					source: nestedSource,
					mode: "force",
					lease: nestedLease,
				}),
			).rejects.toMatchObject({ code: "agent_source_live_link" });
		} finally {
			await nestedLease.release();
		}
		expect(await Bun.file(nested.agentDir).exists()).toBe(false);
	});

	test("refuses symlinked targets or entries and a live discovery owner", async () => {
		const f = await fixture();
		await mkdir(f.agentDir, { recursive: true, mode: 0o700 });
		await writeFile(join(f.agentDir, "models.yml"), "old\n");
		await rm(join(f.agentDir, "models.yml"));
		await symlink(join(f.source, "models.yml"), join(f.agentDir, "models.yml"));
		await expect(provision(f)).rejects.toMatchObject({ code: "broker_agent_symlink" });
		await rm(join(f.agentDir, "models.yml"));
		const otherTarget = join(f.root, "other-agent");
		await mkdir(otherTarget);
		const linkedAgent = join(f.root, "linked-agent");
		await symlink(otherTarget, linkedAgent);
		const lease = await acquireLease(f.home, "broker");
		try {
			await expect(
				provisionBrokerAgent({ home: f.home, agentDir: linkedAgent, source: f.source, mode: "force", lease }),
			).rejects.toMatchObject({ code: "broker_agent_invalid_target" });
		} finally {
			await lease.release();
		}
		const sdk = join(f.agentDir, "sdk");
		await mkdir(sdk, { recursive: true });
		await writeFile(
			join(sdk, "broker.json"),
			JSON.stringify({
				protocolVersion: 3,
				host: "127.0.0.1",
				url: "ws://127.0.0.1:7777",
				token: "not-a-secret-fixture",
				pid: process.pid,
				heartbeatAt: Date.now(),
			}),
		);
		await expect(provision(f)).rejects.toMatchObject({ code: "broker_still_running" });
	});

	test("default and XDG source resolution preserve explicit --from paths", async () => {
		const root = await mkdtemp(join(tmpdir(), "gajaeway-agent-xdg-"));
		roots.push(root);
		const home = join(root, "user");
		const xdg = join(root, "xdg");
		const source = resolveBrokerAgentSource(undefined, home);
		await mkdir(join(xdg, "gjc"), { recursive: true });
		expect(source).toBe(join(home, ".gjc", "agent"));
		expect(resolveBrokerAgentDataSource(source, { homeDirectory: home, xdgDataHome: xdg, platform: "linux" })).toBe(
			join(xdg, "gjc"),
		);
		expect(resolveBrokerAgentDataSource(source, { homeDirectory: home, xdgDataHome: xdg, platform: "win32" })).toBe(
			source,
		);
		// Without XDG_DATA_HOME gjc keeps agent.db in ~/.gjc/agent, even when
		// ~/.local/share/gjc exists for its installer releases.
		await mkdir(join(home, ".local", "share", "gjc", "releases"), { recursive: true });
		expect(resolveBrokerAgentDataSource(source, { homeDirectory: home, xdgDataHome: "", platform: "linux" })).toBe(
			source,
		);
		expect(resolveBrokerAgentSource(join(root, "custom"), home)).toBe(join(root, "custom"));
	});

	test("a process crash after staging leaves the current generation intact and removes the orphan", async () => {
		const f = await fixture();
		await provision(f);
		const oldManifest = JSON.parse(await readFile(homeLayout(f.home).brokerProvisionManifest, "utf8")) as {
			genId: string;
		};
		const oldFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
		await putCredential(join(f.source, "agent.db"), "uncommitted-source-token");
		const marker = join(f.root, "staging-crash.marker");
		const child = await runProvisionCrash(f, marker, "after_staging");
		child.kill("SIGKILL");
		await child.exited;
		expect(child.signalCode).toBe("SIGKILL");
		const provisionDir = homeLayout(f.home).brokerProvisionDir;
		expect((await readdir(provisionDir)).some((name) => name.startsWith("gen-"))).toBe(true);
		const lease = await acquireLease(f.home, "broker");
		try {
			expect(await recoverProvision(f.home, f.agentDir, lease)).toBe("none");
		} finally {
			await lease.release();
		}
		expect((await readdir(provisionDir)).some((name) => name.startsWith("gen-"))).toBe(false);
		expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(oldFingerprint);
		expect(JSON.parse(await readFile(homeLayout(f.home).brokerProvisionManifest, "utf8")).genId).toBe(
			oldManifest.genId,
		);
	});
	test("process crashes at swap boundaries restore exactly one committed generation", async () => {
		const points = [
			"after_journal",
			"after_old_agent.db",
			"after_new_agent.db",
			"after_old_models.yml",
			"after_new_models.yml",
			"after_old_config.yml",
			"after_new_config.yml",
			"after_old_model-presets",
			"after_new_model-presets",
			"before_manifest",
			"after_manifest",
		] as const;
		for (const point of points) {
			const f = await fixture();
			await provision(f);
			const manifestPath = homeLayout(f.home).brokerProvisionManifest;
			const oldManifest = JSON.parse(await readFile(manifestPath, "utf8")) as { genId: string };
			const oldCredentialFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
			await putCredential(join(f.source, "agent.db"), `token-${point}`);
			await writeFile(join(f.source, "models.yml"), `model: ${point}\n`);
			const child = await runProvisionCrash(f, join(f.root, "crash.marker"), point);
			child.kill("SIGKILL");
			await child.exited;
			expect(child.signalCode).toBe("SIGKILL");
			const lease = await acquireLease(f.home, "broker");
			try {
				const recovery = await recoverProvision(f.home, f.agentDir, lease);
				if (point === "after_manifest") {
					expect(recovery).toBe("finished");
					expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).not.toBe(oldCredentialFingerprint);
					expect(JSON.parse(await readFile(manifestPath, "utf8")).genId).not.toBe(oldManifest.genId);
					expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe(`model: ${point}\n`);
				} else {
					expect(recovery).toBe("rolled_back");
					expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(oldCredentialFingerprint);
					expect(JSON.parse(await readFile(manifestPath, "utf8")).genId).toBe(oldManifest.genId);
					expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: source-model\n");
				}
				expect(await Bun.file(join(f.home, "broker", "provision", "journal.json")).exists()).toBe(false);
			} finally {
				await lease.release();
			}
		}
	});
	test("moving the old WAL sidecar and recovering restores its original database pair", async () => {
		const f = await fixture();
		await provision(f);
		const marker = join(f.root, "target-writer.ready");
		const writerScript = `
			import { writeFileSync } from "node:fs";
			import { Database } from "bun:sqlite";
			const db = new Database(${JSON.stringify(join(f.agentDir, "agent.db"))});
			db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
			db.query("UPDATE auth_credentials SET token = 'broker-side-wal-token'").run();
			writeFileSync(${JSON.stringify(marker)}, "ready");
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
		`;
		const writer = Bun.spawn([process.execPath, "--eval", writerScript], {
			stdout: "pipe",
			stderr: "pipe",
			stdin: "ignore",
		});
		processes.push(writer);
		await waitForFile(marker);
		writer.kill("SIGKILL");
		await writer.exited;
		const walPath = join(f.agentDir, "agent.db-wal");
		const walBefore = await fileDigest(walPath);
		const walInfo = await lstat(walPath);
		expect(walBefore).toBeDefined();
		const child = await runProvisionCrash(f, join(f.root, "provision-crash.marker"), "after_old_agent.db-wal");
		child.kill("SIGKILL");
		await child.exited;
		expect(child.signalCode).toBe("SIGKILL");
		const lease = await acquireLease(f.home, "broker");
		try {
			expect(await recoverProvision(f.home, f.agentDir, lease)).toBe("rolled_back");
		} finally {
			await lease.release();
		}
		const restored = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			expect(restored.query("SELECT token FROM auth_credentials WHERE provider = 'provider-a'").get()).toEqual({
				token: "broker-side-wal-token",
			});
			expect(restored.query("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
		} finally {
			restored.close();
		}
		expect(await fileDigest(walPath)).toBe(walBefore);
		expect((await lstat(walPath)).ino).toBe(walInfo.ino);
	});
	test("rollback validates the whole journal before changing a target with an anomalous entry", async () => {
		const f = await fixture();
		await provision(f);
		const marker = join(f.root, "anomaly-crash.marker");
		const child = await runProvisionCrash(f, marker, "after_old_agent.db");
		child.kill("SIGKILL");
		await child.exited;
		const journalPath = join(f.home, "broker", "provision", "journal.json");
		const journal = JSON.parse(await readFile(journalPath, "utf8")) as { prevDir: string };
		const previousDb = join(journal.prevDir, "agent.db");
		expect(await Bun.file(join(f.agentDir, "agent.db")).exists()).toBe(false);
		expect(await Bun.file(previousDb).exists()).toBe(true);
		await writeFile(join(f.agentDir, "models.yml"), "model: tampered\n");
		const lease = await acquireLease(f.home, "broker");
		try {
			await expect(recoverProvision(f.home, f.agentDir, lease)).rejects.toMatchObject({
				code: "broker_agent_unrecoverable",
			});
		} finally {
			await lease.release();
		}
		expect(await Bun.file(join(f.agentDir, "agent.db")).exists()).toBe(false);
		expect(await Bun.file(previousDb).exists()).toBe(true);
		expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: tampered\n");
		expect(await Bun.file(journalPath).exists()).toBe(true);
	});

	test("thrown faults recover before and after manifest publication at every phase boundary", async () => {
		for (const point of [
			"after_staging",
			"after_journal",
			"after_old_agent.db",
			"after_new_agent.db",
			"after_new_model-presets",
			"before_manifest",
			"after_manifest",
		] as const) {
			const f = await fixture();
			await provision(f);
			const oldManifest = JSON.parse(await readFile(homeLayout(f.home).brokerProvisionManifest, "utf8")) as {
				genId: string;
			};
			const oldFingerprint = await credentialFingerprint(join(f.agentDir, "agent.db"));
			await putCredential(join(f.source, "agent.db"), `token-${point}`);
			await writeFile(join(f.source, "models.yml"), `model: ${point}\n`);
			const lease = await acquireLease(f.home, "broker");
			try {
				const run = () =>
					provisionBrokerAgent({
						home: f.home,
						agentDir: f.agentDir,
						source: f.source,
						mode: "force",
						lease,
						fault: (at) => {
							if (at === point) throw new Error(`injected ${point}`);
						},
					});
				if (point === "after_manifest") {
					const result = await run();
					expect(result.status).toBe("ok");
					expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).not.toBe(oldFingerprint);
					expect(JSON.parse(await readFile(homeLayout(f.home).brokerProvisionManifest, "utf8")).genId).not.toBe(
						oldManifest.genId,
					);
					expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe(`model: ${point}\n`);
				} else {
					await expect(run()).rejects.toMatchObject({ code: "broker_agent_failed_restored" });
					expect(await credentialFingerprint(join(f.agentDir, "agent.db"))).toBe(oldFingerprint);
					expect(JSON.parse(await readFile(homeLayout(f.home).brokerProvisionManifest, "utf8")).genId).toBe(
						oldManifest.genId,
					);
					expect(await readFile(join(f.agentDir, "models.yml"), "utf8")).toBe("model: source-model\n");
				}
				expect(await Bun.file(join(f.home, "broker", "provision", "journal.json")).exists()).toBe(false);
			} finally {
				await lease.release();
			}
		}
	});

	test("rotating OAuth credentials are filtered out during provision", async () => {
		const f = await fixture();
		// Create a source agent.db with rotating OAuth credentials
		const sourceDb = new Database(join(f.source, "agent.db"));
		sourceDb.query("DELETE FROM auth_credentials").run();
		// Add a rotating OAuth provider credential (anthropic)
		sourceDb.query("INSERT INTO auth_credentials (provider, token) VALUES (?, ?)").run("anthropic", "anthropic-token");
		// Add another rotating OAuth provider credential (openai-codex)
		sourceDb.query("INSERT INTO auth_credentials (provider, token) VALUES (?, ?)").run("openai-codex", "openai-codex-token");
		// Add a non-rotating credential (should be kept)
		sourceDb.query("INSERT INTO auth_credentials (provider, token) VALUES (?, ?)").run("github", "github-token");
		sourceDb.close();

		const filtered: string[] = [];
		const lease = await acquireLease(f.home, "broker");
		try {
			await provisionBrokerAgent({
				home: f.home,
				agentDir: f.agentDir,
				source: f.source,
				mode: "force",
				lease,
				onFilteredCredentials: (providers) => filtered.push(...providers),
			});
		} finally {
			await lease.release();
		}

		// Verify that rotating OAuth credentials were filtered
		expect(filtered.sort()).toEqual(["anthropic", "openai-codex"]);

		// Verify the broker agent.db does not contain the filtered credentials
		const brokerDb = new Database(join(f.agentDir, "agent.db"), { readonly: true });
		try {
			const anthropic = brokerDb.query("SELECT token FROM auth_credentials WHERE provider = 'anthropic'").get() as
				| { token: unknown }
				| undefined;
			const openaiCodex = brokerDb.query("SELECT token FROM auth_credentials WHERE provider = 'openai-codex'").get() as
				| { token: unknown }
				| undefined;
			const github = brokerDb.query("SELECT token FROM auth_credentials WHERE provider = 'github'").get() as
				| { token: unknown }
				| undefined;

			expect(anthropic).toBeNull();
			expect(openaiCodex).toBeNull();
			expect(github?.token).toBe("github-token");
		} finally {
			brokerDb.close();
		}
	});
});
