import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";

/** Providers that use rotating OAuth refresh tokens and should not be copied to the broker agent. */
const ROTATING_OAUTH_PROVIDERS = new Set(["anthropic", "openai-codex"]);

export class AgentDatabaseError extends Error {
	readonly code = "agent_database_invalid";
	constructor(message: string) {
		super(message);
		this.name = "AgentDatabaseError";
	}
}

// SQLite's read-only WAL reader may rebuild or update the transient -shm index.
// It leaves agent.db and the WAL frames untouched; deleting -shm would invalidate concurrent readers.
export function snapshotAgentDb(source: string, destination: string): void {
	const sourceDb = new Database(source, { readonly: true });
	try {
		const quotedDestination = destination.replaceAll("'", "''");
		sourceDb.exec(`VACUUM INTO '${quotedDestination}'`);
	} finally {
		sourceDb.close();
	}
	assertAgentDb(destination);
}

/**
 * Copy the agent database to the broker agent directory, filtering out OAuth credentials
 * for providers with rotating refresh tokens to avoid sharing tokens between stores.
 * Returns the list of filtered provider names.
 */
export function snapshotAgentDbWithoutRotatingCredentials(source: string, destination: string): string[] {
	const sourceDb = new Database(source, { readonly: true });
	try {
		const quotedDestination = destination.replaceAll("'", "''");
		sourceDb.exec(`VACUUM INTO '${quotedDestination}'`);
	} finally {
		sourceDb.close();
	}

	// Filter out rotating OAuth credentials from the destination database
	const destDb = new Database(destination);
	const filtered: string[] = [];
	try {
		const rows = destDb.query("SELECT provider FROM auth_credentials").all() as Array<{ provider: unknown }>;
		for (const row of rows) {
			const provider = typeof row.provider === "string" ? row.provider : "";
			if (ROTATING_OAUTH_PROVIDERS.has(provider)) {
				filtered.push(provider);
				destDb.query("DELETE FROM auth_credentials WHERE provider = ?").run(provider);
			}
		}
	} finally {
		destDb.close();
	}

	assertAgentDb(destination);
	return filtered;
}

export function assertAgentDb(path: string): void {
	const database = new Database(path, { readonly: true });
	try {
		const integrity = database.query("PRAGMA integrity_check").get() as Record<string, unknown> | null;
		if (!integrity || Object.values(integrity)[0] !== "ok")
			throw new AgentDatabaseError("agent.db failed integrity_check");
		const credentials = database
			.query("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'auth_credentials'")
			.get() as Record<string, unknown> | null;
		if (!credentials) throw new AgentDatabaseError("agent.db is missing auth_credentials");
	} catch (error) {
		if (error instanceof AgentDatabaseError) throw error;
		throw new AgentDatabaseError(
			`agent.db could not be validated: ${error instanceof Error ? error.message : String(error)}`,
		);
	} finally {
		database.close();
	}
}

export function readAuthCredentialData(path: string): string[] {
	const database = new Database(path, { readonly: true });
	try {
		const rows = database.query("SELECT data FROM auth_credentials").all() as Array<{ data: unknown }>;
		return rows.flatMap((row) => (typeof row.data === "string" ? [row.data] : []));
	} finally {
		database.close();
	}
}

export function credentialFingerprint(path: string): string {
	const database = new Database(path, { readonly: true });
	try {
		const columns = database.query("PRAGMA table_info(auth_credentials)").all() as Array<{ name: string }>;
		if (columns.length === 0) throw new AgentDatabaseError("agent.db is missing auth_credentials");
		const columnNames = columns.map((column) => column.name);
		const quotedColumns = columnNames.map((name) => `"${name.replaceAll('"', '""')}"`);
		const rows = database.query(`SELECT * FROM auth_credentials ORDER BY ${quotedColumns.join(", ")}`).all() as Array<
			Record<string, unknown>
		>;
		const canonical = JSON.stringify({
			columns: columnNames,
			rows: rows.map((row) => columnNames.map((name) => canonicalSqlValue(row[name]))),
		});
		return createHash("sha256").update(canonical).digest("hex");
	} catch (error) {
		if (error instanceof AgentDatabaseError) throw error;
		throw new AgentDatabaseError(
			`auth_credentials could not be fingerprinted: ${error instanceof Error ? error.message : String(error)}`,
		);
	} finally {
		database.close();
	}
}

function canonicalSqlValue(value: unknown): unknown {
	if (typeof value === "bigint") return { integer: value.toString() };
	if (value instanceof Uint8Array) return { blob: Buffer.from(value).toString("base64") };
	if (typeof value === "number" && !Number.isFinite(value)) return { number: String(value) };
	return value;
}
