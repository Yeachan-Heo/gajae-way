import { Database } from "bun:sqlite";
import { constants } from "node:fs";
import { link, lstat, mkdtemp, open, readFile, realpath, rmdir, stat, unlink } from "node:fs/promises";
import { createConnection } from "node:net";
import { dirname, isAbsolute, join, normalize, resolve } from "node:path";
import { loadConfig } from "../packages/gateway/src/config";
import { type BrokerAuthority, GatewayDatabase } from "../packages/gateway/src/store/db";
import { acquireGatewayHome } from "../packages/gateway/src/takeover";

export class CutoverCommandError extends Error {
	constructor(readonly code: string) {
		super(code);
	}
}

function refuse(code: string): never {
	throw new CutoverCommandError(code);
}

function authority(value: unknown): BrokerAuthority | null {
	if (value === null) return null;
	if (typeof value !== "object" || Array.isArray(value)) return refuse("invalid_authority");
	const record = value as Record<string, unknown>;
	if (
		typeof record.canonicalAgentDir !== "string" ||
		!isAbsolute(record.canonicalAgentDir) ||
		normalize(record.canonicalAgentDir) !== record.canonicalAgentDir ||
		record.canonicalAgentDir.includes("\0") ||
		typeof record.identity !== "string" ||
		!record.identity.trim() ||
		record.identity.includes("\0")
	)
		return refuse("invalid_authority");
	return { canonicalAgentDir: record.canonicalAgentDir, identity: record.identity };
}

function parse(args: readonly string[]) {
	const mode = args[0];
	if (mode !== "inspect" && mode !== "apply") return refuse("usage");
	const values = new Map<string, string>();
	for (let i = 1; i < args.length; i++) {
		const key = args[i]!;
		if (values.has(key)) return refuse("usage");
		if (key === "--quarantine") values.set(key, "true");
		else if (["--home", "--agent-dir", "--expected-authority", "--evidence", "--backup"].includes(key)) {
			const value = args[++i];
			if (value === undefined || value.startsWith("--")) return refuse("usage");
			values.set(key, value);
		} else return refuse("usage");
	}
	const home = values.get("--home");
	const agentDir = values.get("--agent-dir");
	if (!home || !agentDir || !isAbsolute(home) || !isAbsolute(agentDir)) return refuse("absolute_paths_required");
	if (mode === "inspect" && values.size !== 2) return refuse("usage");
	let expected: BrokerAuthority | null = null;
	const backup = values.get("--backup");
	if (mode === "apply") {
		if (
			!values.has("--expected-authority") ||
			!values.has("--quarantine") ||
			!values.get("--evidence")?.trim() ||
			!backup ||
			!isAbsolute(backup)
		)
			return refuse("explicit_apply_arguments_required");
		try {
			expected = authority(JSON.parse(values.get("--expected-authority")!));
		} catch {
			return refuse("invalid_authority");
		}
	}
	return { mode, home, agentDir, expected, backup, evidence: values.get("--evidence") };
}

/** No process controls: only signal zero, and every result except ESRCH refuses. */
export async function assertGatewayStopped(home: string, socketPath: string): Promise<void> {
	let raw: string | undefined;
	try {
		raw = await readFile(join(home, "daemon.pid"), "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") return refuse("pid_indeterminate");
		try {
			await lstat(join(home, "daemon.pid"));
			return refuse("pid_indeterminate");
		} catch (entryError) {
			if ((entryError as NodeJS.ErrnoException).code !== "ENOENT") throw entryError;
		}
	}
	if (raw !== undefined) {
		let record: Record<string, unknown>;
		try {
			record = JSON.parse(raw);
		} catch {
			return refuse("pid_indeterminate");
		}
		if (
			!record ||
			typeof record.pid !== "number" ||
			!Number.isSafeInteger(record.pid) ||
			record.pid <= 0 ||
			typeof record.home !== "string" ||
			typeof record.startedAt !== "string"
		)
			return refuse("pid_indeterminate");
		try {
			process.kill(record.pid, 0);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") return checkSocket(socketPath);
			return refuse("pid_indeterminate");
		}
		return refuse("gateway_live_pid");
	}
	await checkSocket(socketPath);
}

async function checkSocket(path: string): Promise<void> {
	await new Promise<void>((resolvePromise, reject) => {
		const socket = createConnection({ path });
		const finish = (code?: string) => {
			socket.destroy();
			if (code) reject(new CutoverCommandError(code));
			else resolvePromise();
		};
		socket.setTimeout(1000, () => finish("socket_indeterminate"));
		socket.once("connect", () => finish("gateway_live_socket"));
		socket.once("error", (error: NodeJS.ErrnoException) => {
			finish(error.code === "ENOENT" || error.code === "ECONNREFUSED" ? undefined : "socket_indeterminate");
		});
	});
}

/** Read-only, point-in-time table census; deliberately not a claim of replay quiescence. */
function inspect(path: string) {
	const database = new Database(path, { readonly: true, create: false });
	try {
		database.exec("BEGIN");
		const tables = database
			.query<{ name: string }, []>(
				"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
			)
			.all();
		if (tables.length > 128) return refuse("unsupported_schema");
		const rowCounts: Record<string, number> = {};
		for (const { name } of tables) {
			if (!/^[a-z][a-z0-9_]{0,63}$/.test(name)) return refuse("unsupported_schema");
			rowCounts[name] = database.query<{ n: number }, []>(`SELECT COUNT(*) AS n FROM "${name}"`).get()!.n;
		}
		let current: BrokerAuthority | null = null;
		if (Object.hasOwn(rowCounts, "broker_authority")) {
			const row = database
				.query<{ authority_key: string }, []>("SELECT authority_key FROM broker_authority WHERE singleton = 1")
				.get();
			if (row) {
				const parsed: unknown = JSON.parse(row.authority_key);
				if (!Array.isArray(parsed) || parsed.length !== 2) return refuse("invalid_authority");
				current = authority({ canonicalAgentDir: parsed[0], identity: parsed[1] });
			}
		}
		let schema = 0;
		if (Object.hasOwn(rowCounts, "schema_migrations")) {
			const versions = database
				.query<{ version: unknown }, []>("SELECT version FROM schema_migrations ORDER BY version")
				.all();
			for (const row of versions) {
				if (typeof row.version !== "number" || !Number.isSafeInteger(row.version) || row.version !== schema + 1)
					return refuse("invalid_migration_ledger");
				schema = row.version;
			}
		}
		database.exec("COMMIT");
		return { authority: current, census: { schema, rowCounts, semanticOpenWorkCensus: "not_evaluated" } };
	} finally {
		database.close();
	}
}

/** Safe to import. Inspection is read-only; apply fences boot before any database access. */
export async function main(args: readonly string[]) {
	const options = parse(args);
	const home = await realpath(options.home);
	const canonicalAgentDir = await realpath(options.agentDir);
	if (canonicalAgentDir !== options.agentDir) return refuse("canonical_agent_dir_required");
	if (!(await stat(home)).isDirectory() || !(await stat(canonicalAgentDir)).isDirectory())
		return refuse("directory_required");
	const targetAuthority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	const config = await loadConfig({ home, env: {} });
	const databasePath = await realpath(resolve(home, config.dbPath));
	if (options.mode === "inspect") {
		await assertGatewayStopped(home, resolve(home, config.socketPath));
		const before = inspect(databasePath);
		return {
			mode: "inspect" as const,
			oldAuthority: before.authority,
			targetAuthority,
			census: before.census,
			consistentRead: true,
			exclusiveOwnership: false,
			applyAvailable: true,
		};
	}
	const lease = await acquireGatewayHome(home);
	try {
		await assertGatewayStopped(home, resolve(home, config.socketPath));
		const backup = options.backup!;
		if (normalize(backup) !== backup || backup.includes("\0") || (await realpath(dirname(backup))) !== dirname(backup))
			return refuse("canonical_backup_required");
		if ([databasePath, `${databasePath}-wal`, `${databasePath}-shm`, `${databasePath}-journal`].includes(backup))
			return refuse("backup_conflicts_with_database");
		const before = inspect(databasePath);
		if (
			JSON.stringify(before.authority) !== JSON.stringify(options.expected) ||
			JSON.stringify(before.authority) === JSON.stringify(targetAuthority)
		)
			return refuse("authority_mismatch");
		// VACUUM INTO requires a nonexistent output. A private, exclusively created
		// directory reserves its namespace without precreating the SQLite file.
		// Publication uses link(), never rename(): EEXIST cannot overwrite a backup.
		try {
			await lstat(backup);
			return refuse("backup_exists");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const parent = dirname(backup);
		const temporaryDirectory = await mkdtemp(join(parent, ".gjc-cutover-"));
		const directoryIdentity = await lstat(temporaryDirectory);
		const temporary = join(temporaryDirectory, "backup.db");
		const cleanupTemporaryBackup = async (): Promise<void> => {
			// Never remove the published destination, even after a later failure.
			// Cleanup is limited to the namespace this invocation exclusively owns.
			const current = await lstat(temporaryDirectory);
			if (current.dev !== directoryIdentity.dev || current.ino !== directoryIdentity.ino)
				throw new CutoverCommandError("backup_temporary_identity_changed");
			try {
				await unlink(temporary);
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			}
			await rmdir(temporaryDirectory);
		};
		const backupErrors: unknown[] = [];
		try {
			const source = new Database(databasePath, { readonly: true, create: false });
			try {
				source.query("VACUUM INTO ?").run(temporary);
			} finally {
				source.close();
			}
			const file = await open(temporary, constants.O_RDWR | constants.O_NOFOLLOW);
			try {
				const identity = await file.stat();
				if (!identity.isFile() || identity.nlink !== 1) return refuse("backup_identity_changed");
				await file.chmod(0o600);
				const copy = new Database(temporary, { readonly: true, create: false });
				try {
					const result = copy.query<{ integrity_check: string }, []>("PRAGMA integrity_check").all();
					if (result.length !== 1 || result[0]?.integrity_check !== "ok") return refuse("backup_integrity_failed");
				} finally {
					copy.close();
				}
				await file.sync();
				const checked = await lstat(temporary);
				if (checked.dev !== identity.dev || checked.ino !== identity.ino) return refuse("backup_identity_changed");
				await link(temporary, backup).catch((error: NodeJS.ErrnoException) => {
					if (error.code === "EEXIST") return refuse("backup_exists");
					throw error;
				});
				const published = await lstat(backup);
				if (
					!published.isFile() ||
					published.dev !== identity.dev ||
					published.ino !== identity.ino ||
					published.size !== identity.size
				)
					return refuse("backup_identity_changed");
				const backupDirectory = await open(parent, "r");
				try {
					await backupDirectory.sync();
				} finally {
					await backupDirectory.close();
				}
			} finally {
				await file.close();
			}
		} catch (error) {
			backupErrors.push(error);
		}
		try {
			await cleanupTemporaryBackup();
		} catch (error) {
			backupErrors.push(error);
		}
		if (backupErrors.length === 1) throw backupErrors[0];
		if (backupErrors.length > 1)
			throw new AggregateError(backupErrors, "Backup creation failed and temporary backup cleanup also failed", {
				cause: backupErrors[0],
			});
		const database = await GatewayDatabase.open(databasePath);
		try {
			const semanticCensus = database.inspectBrokerAuthority();
			const snapshotId = database.cutoverBrokerAuthority({
				expectedAuthority: options.expected,
				targetAuthority,
				evidence: options.evidence!,
				disposition: "quarantine",
			});
			return {
				mode: "apply" as const,
				oldAuthority: before.authority,
				targetAuthority,
				backup: { path: backup, integrity: "ok", premigration: true, schema: before.census.schema },
				snapshotId,
				targetSchema: database.schemaVersion,
				census: before.census,
				semanticCensus,
				consistentRead: true,
				exclusiveOwnership: true,
				applyAvailable: true,
			};
		} finally {
			database.close();
		}
	} finally {
		await lease.release();
	}
}

if (import.meta.main) {
	try {
		console.log(JSON.stringify(await main(Bun.argv.slice(2))));
	} catch (error) {
		console.error(
			JSON.stringify({ error: error instanceof CutoverCommandError ? error.code : "cutover_command_failed" }),
		);
		process.exitCode = 1;
	}
}
